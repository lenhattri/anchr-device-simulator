package simulator

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type Pump struct {
	index          int
	identity       DeviceIdentity
	cfg            Config
	client         mqtt.Client
	registry       *TxRegistry
	seqLogger      *SeqLogger
	rng            *rand.Rand
	mu             sync.RWMutex
	state          PumpState
	shiftNo        int
	unitPrice      int
	currency       string
	fuelGrade      string
	sessionID      string
	startTime      time.Time
	currentVolume  float64
	currentAmount  float64
	displayVolume  float64
	displayAmount  float64
	pumpRate       float64
	lastTXSeq      int64
	totalizer      float64
	startTotalizer float64
	msgSeqs        map[string]int64
	cancel         context.CancelFunc
	done           chan struct{}
}

func NewPump(index int, cfg Config, client mqtt.Client, registry *TxRegistry, seqLogger *SeqLogger) *Pump {
	identity := DeviceIdentityForIndex(index)
	source := rand.NewSource(time.Now().UnixNano() + int64(index)*7919)
	rng := rand.New(source)
	state := registry.EnsureDevice(identity.DeviceID)
	return &Pump{
		index:     index,
		identity:  identity,
		cfg:       cfg,
		client:    client,
		registry:  registry,
		seqLogger: seqLogger,
		rng:       rng,
		state:     StateHook,
		shiftNo:   1,
		unitPrice: 20000 + ((((index / PumpsPerStation) % NumStations) + 1) * 50),
		currency:  "VND",
		fuelGrade: "RON95",
		pumpRate:  math.Round((0.5+rng.Float64()*1.5)*100) / 100,
		lastTXSeq: state.LastIssuedSeq,
		totalizer: 10000.0 + float64(index*10),
		msgSeqs:   map[string]int64{"telemetry": 0, "tx": 0, "ack": 0},
		done:      make(chan struct{}),
	}
}

func (p *Pump) State() PumpState {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.state
}

func (p *Pump) resetSessionLocked() {
	p.state = StateHook
	p.sessionID = ""
	p.currentVolume = 0
	p.currentAmount = 0
}

func (p *Pump) Run(ctx context.Context) {
	defer close(p.done)
	jitter := time.Duration(p.rng.Int63n(int64(DefaultSessionStartJitter)))
	if !sleepWithContext(ctx, jitter) {
		return
	}

	for {
		switch p.State() {
		case StateHook:
			if ctx.Err() != nil {
				return
			}
			idleSecs := p.rng.Intn(241) + 60
			idleTicks := maxInt(1, idleSecs/maxInt(1, p.cfg.Speed*5))
			for i := 0; i < idleTicks; i++ {
				if ctx.Err() != nil {
					return
				}
				p.publishTelemetry()
				if !sleepWithContext(ctx, 5*time.Second/time.Duration(maxInt(1, p.cfg.Speed))) {
					return
				}
			}
			if ctx.Err() != nil {
				return
			}
			p.mu.Lock()
			p.state = StateLift
			p.sessionID = randomUUID(p.rng)
			p.startTotalizer = p.totalizer
			p.currentVolume = 0
			p.currentAmount = 0
			p.displayVolume = 0
			p.displayAmount = 0
			p.mu.Unlock()

		case StateLift:
			p.publishTelemetry()
			liftDelay := time.Duration(math.Max(1, 3+p.rng.Float64()*5)) * time.Second / time.Duration(maxInt(1, p.cfg.Speed))
			if !sleepWithContext(ctx, liftDelay) {
				p.mu.Lock()
				p.resetSessionLocked()
				p.mu.Unlock()
				return
			}
			if ctx.Err() != nil {
				p.mu.Lock()
				p.resetSessionLocked()
				p.mu.Unlock()
				return
			}
			p.mu.Lock()
			p.state = StatePump
			p.startTime = time.Now().UTC()
			p.mu.Unlock()

		case StatePump:
			fillSecs := p.rng.Intn(91) + 30
			pumpTicks := maxInt(1, fillSecs/maxInt(1, p.cfg.Speed))
			for i := 0; i < pumpTicks; i++ {
				p.mu.Lock()
				p.currentVolume += p.pumpRate * float64(p.cfg.Speed)
				p.currentAmount = p.currentVolume * float64(p.unitPrice)
				p.mu.Unlock()
				p.publishTelemetry()
				if !sleepWithContext(ctx, time.Second) {
					p.mu.Lock()
					p.resetSessionLocked()
					p.mu.Unlock()
					return
				}
			}
			p.mu.Lock()
			p.totalizer += p.currentVolume
			p.displayVolume = p.currentVolume
			p.displayAmount = p.currentAmount
			p.mu.Unlock()

			if err := p.publishTransactionGuaranteed(); err != nil {
				log.Printf("ERROR tx publish failed device=%s err=%v", p.identity.DeviceID, err)
			}
			p.mu.Lock()
			p.resetSessionLocked()
			p.mu.Unlock()
			if ctx.Err() != nil {
				return
			}
		default:
			return
		}
	}
}

func (p *Pump) Stop() {
	if p.cancel != nil {
		p.cancel()
	}
}

func (p *Pump) Done() <-chan struct{} { return p.done }

func (p *Pump) attachCancel(cancel context.CancelFunc) {
	p.cancel = cancel
}

func (p *Pump) handleCommand(envelope map[string]any) {
	data, _ := envelope["data"].(map[string]any)
	cmdWrap, _ := data["command"].(map[string]any)
	cmdType, _ := cmdWrap["type"].(string)
	cmdID, _ := cmdWrap["cmd_id"].(string)

	ackStatus := "REJECTED"
	ackCode := 1
	ackMsg := "Unknown command"
	ackPayload := map[string]any{}

	p.mu.Lock()
	switch cmdType {
	case "set_price":
		if p.state == StateHook {
			if payload, ok := cmdWrap["payload"].(map[string]any); ok {
				if price, ok := asInt(payload["unit_price"]); ok {
					p.unitPrice = price
					ackStatus = "APPLIED"
					ackCode = 0
					ackMsg = "Price updated"
					ackPayload = map[string]any{"unit_price": p.unitPrice}
				} else {
					ackMsg = "Invalid price payload"
				}
			}
		} else {
			ackMsg = "Cannot change price while pumping"
		}
	case "close_shift":
		if p.state == StateHook {
			old := p.shiftNo
			p.shiftNo++
			ackStatus = "APPLIED"
			ackCode = 0
			ackMsg = "Shift closed"
			ackPayload = map[string]any{"old_shift_no": old, "new_shift_no": p.shiftNo}
		} else {
			ackMsg = "Cannot close shift while pumping"
		}
	}
	p.msgSeqs["ack"]++
	seq := p.msgSeqs["ack"]
	stationID := p.identity.StationID
	pumpID := p.identity.PumpID
	deviceID := p.identity.DeviceID
	p.mu.Unlock()

	ack := map[string]any{
		"schema":         "anchr.ack.v1",
		"schema_version": 1,
		"message_id":     randomUUID(p.rng),
		"type":           "ack",
		"tenant_id":      p.cfg.TenantID,
		"station_id":     stationID,
		"pump_id":        pumpID,
		"device_id":      deviceID,
		"event_time":     nowRFC3339Millis(),
		"seq":            seq,
		"correlation_id": cmdID,
		"data": map[string]any{
			"ack": map[string]any{
				"cmd_id":     cmdID,
				"type":       cmdType,
				"status":     ackStatus,
				"code":       ackCode,
				"message":    ackMsg,
				"applied_at": nowRFC3339Millis(),
				"payload":    ackPayload,
			},
		},
	}
	p.publish("ack", ack, 1, false)
}

func (p *Pump) publishTelemetry() {
	p.mu.Lock()
	vol := p.displayVolume
	amt := p.displayAmount
	if p.state == StatePump {
		vol = p.currentVolume
		amt = p.currentAmount
	}
	meter := p.totalizer
	if p.state == StatePump {
		meter += p.currentVolume
	}
	payload := map[string]any{
		"state_code":             int(p.state),
		"display_amount":         int(math.Round(amt)),
		"display_volume_liters":  round3(vol),
		"pump_rate_lpm":          round1(p.pumpRate * 60),
		"unit_price":             p.unitPrice,
		"shift_no":               p.shiftNo,
		"fuel_grade":             p.fuelGrade,
		"currency":               p.currency,
		"session_id":             p.sessionID,
		"meter_totalizer_liters": round3(meter),
		"fw_version":             "sim-go-1.0",
		"health":                 map[string]any{"ok": true},
	}
	p.msgSeqs["telemetry"]++
	seq := p.msgSeqs["telemetry"]
	p.mu.Unlock()

	envelope := p.baseEnvelope("telemetry", seq, payload)
	p.publish("telemetry", envelope, 0, true)
}

func (p *Pump) publishTransactionGuaranteed() error {
	p.mu.Lock()
	startTime := p.startTime
	currentVolume := p.currentVolume
	currentAmount := p.currentAmount
	shiftNo := p.shiftNo
	unitPrice := p.unitPrice
	fuelGrade := p.fuelGrade
	currency := p.currency
	startTotalizer := p.startTotalizer
	endTotalizer := p.totalizer
	p.mu.Unlock()

	endTime := time.Now().UTC()
	durationMS := endTime.Sub(startTime).Milliseconds()
	if startTime.IsZero() {
		durationMS = 0
	}

	p.mu.Lock()
	p.msgSeqs["tx"]++
	msgSeq := p.msgSeqs["tx"]
	p.mu.Unlock()

	pending, err := p.registry.ReservePending(p.cfg.TXStateFile, p.identity.DeviceID, func(nextSeq int64) (PendingTransaction, error) {
		txID := fmt.Sprintf("%s:%d:%d", p.identity.DeviceID, shiftNo, nextSeq)
		envelope := p.baseEnvelope("tx", msgSeq, map[string]any{
			"tx_id":                        txID,
			"tx_seq":                       nextSeq,
			"shift_no":                     shiftNo,
			"start_time":                   startTime.UTC().Format(time.RFC3339Nano),
			"end_time":                     endTime.Format(time.RFC3339Nano),
			"duration_ms":                  durationMS,
			"fuel_grade":                   fuelGrade,
			"unit_price":                   unitPrice,
			"total_volume_liters":          round3(currentVolume),
			"total_amount":                 int(math.Round(currentAmount)),
			"currency":                     currency,
			"stop_reason":                  "manual_stop",
			"meter_start_totalizer_liters": round3(startTotalizer),
			"meter_end_totalizer_liters":   round3(endTotalizer),
			"status":                       "COMPLETED",
		})
		payload, err := json.Marshal(envelope)
		if err != nil {
			return PendingTransaction{}, err
		}
		return PendingTransaction{
			DeviceID:  p.identity.DeviceID,
			StationID: p.identity.StationID,
			PumpID:    p.identity.PumpID,
			Topic:     p.topicFor("tx"),
			TxSeq:     nextSeq,
			MessageID: fmt.Sprint(envelope["message_id"]),
			CreatedAt: endTime.Format(time.RFC3339Nano),
			Payload:   payload,
		}, nil
	})
	if err != nil {
		p.mu.Lock()
		p.msgSeqs["tx"]--
		p.mu.Unlock()
		return err
	}

	if err := publishPendingTransaction(nil, p.client, p.cfg, pending); err != nil {
		p.mu.Lock()
		p.msgSeqs["tx"]--
		p.mu.Unlock()
		return err
	}
	if err := p.registry.ConfirmPending(p.cfg.TXStateFile, p.identity.DeviceID, pending.TxSeq); err != nil {
		return err
	}

	p.mu.Lock()
	p.lastTXSeq = pending.TxSeq
	p.mu.Unlock()
	p.seqLogger.Write(map[string]any{
		"device_id":  p.identity.DeviceID,
		"seq":        msgSeq,
		"type":       "tx",
		"message_id": pending.MessageID,
		"ts":         pending.CreatedAt,
		"tx_seq":     pending.TxSeq,
	})
	log.Printf("INFO tx confirmed device=%s tx_seq=%d liters=%.3f", p.identity.DeviceID, pending.TxSeq, currentVolume)
	return nil
}

func (p *Pump) publish(subtopic string, envelope map[string]any, qos byte, writeSeqLog bool) {
	payload, err := json.Marshal(envelope)
	if err != nil {
		log.Printf("ERROR marshal publish payload device=%s subtopic=%s err=%v", p.identity.DeviceID, subtopic, err)
		return
	}
	messageID := envelope["message_id"]
	eventTime := envelope["event_time"]
	topic := p.topicFor(subtopic)
	token := p.client.Publish(topic, qos, false, payload)
	if qos == 0 {
		if writeSeqLog {
			p.seqLogger.Write(map[string]any{
				"device_id":  p.identity.DeviceID,
				"seq":        envelope["seq"],
				"type":       subtopic,
				"message_id": messageID,
				"ts":         eventTime,
			})
		}
		return
	}

	go func(deviceID string) {
		_ = token.WaitTimeout(p.cfg.TXPublishTimeout)
		if err := token.Error(); err != nil {
			log.Printf("WARN publish error device=%s subtopic=%s err=%v", deviceID, subtopic, err)
		}
	}(p.identity.DeviceID)
}

func (p *Pump) topicFor(subtopic string) string {
	return fmt.Sprintf("anchr/v1/%s/%s/%s/%s", p.cfg.TenantID, p.identity.StationID, p.identity.PumpID, subtopic)
}

func (p *Pump) baseEnvelope(msgType string, seq int64, data map[string]any) map[string]any {
	return map[string]any{
		"schema":         fmt.Sprintf("anchr.%s.v1", msgType),
		"schema_version": 1,
		"message_id":     randomUUID(p.rng),
		"type":           msgType,
		"tenant_id":      p.cfg.TenantID,
		"station_id":     p.identity.StationID,
		"pump_id":        p.identity.PumpID,
		"device_id":      p.identity.DeviceID,
		"event_time":     nowRFC3339Millis(),
		"seq":            seq,
		"data":           data,
	}
}
