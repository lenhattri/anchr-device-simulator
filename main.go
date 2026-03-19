package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"gopkg.in/yaml.v3"
)

const (
	numStations               = 10
	pumpsPerStation           = 3000
	defaultTXStateFile        = "/tmp/sim-tx-state.json"
	defaultHTTPAddr           = ":8080"
	defaultPersistInterval    = 30 * time.Second
	defaultConnectTimeout     = 10 * time.Second
	defaultPublishTimeout     = 10 * time.Second
	defaultShutdownTimeout    = 60 * time.Second
	defaultSessionStartJitter = 10 * time.Second
)

type Config struct {
	MQTTHost           string
	MQTTPort           int
	MQTTTLS            bool
	TenantID           string
	Speed              int
	InitialPumps       int
	MinPumps           int
	MaxPumps           int
	StepSize           int
	ScaleInterval      time.Duration
	PoolSize           int
	HTTPAddr           string
	TXStateFile        string
	TXPublishTimeout   time.Duration
	MaxTXRetries       int
	PersistInterval    time.Duration
	ShutdownTimeout    time.Duration
	SeqLogFile         string
	MQTTUsername       string
	MQTTPassword       string
	MQTTClientIDPrefix string
}

type rawConfig struct {
	MQTT struct {
		Host     string `yaml:"host"`
		Port     int    `yaml:"port"`
		TLS      bool   `yaml:"tls"`
		Username string `yaml:"username"`
		Password string `yaml:"password"`
	} `yaml:"mqtt"`
	Simulation struct {
		TenantID         string `yaml:"tenant_id"`
		Speed            int    `yaml:"speed"`
		InitialPumps     int    `yaml:"initial_pumps"`
		MinPumps         int    `yaml:"min_pumps"`
		MaxPumps         int    `yaml:"max_pumps"`
		StepSize         int    `yaml:"step_size"`
		ScaleIntervalSec int    `yaml:"scale_interval"`
	} `yaml:"simulation"`
	Pool struct {
		Size int `yaml:"size"`
	} `yaml:"pool"`
	HTTP struct {
		Addr string `yaml:"addr"`
	} `yaml:"http"`
	Persistence struct {
		TXStateFile        string `yaml:"tx_state_file"`
		PersistIntervalSec int    `yaml:"persist_interval"`
		ShutdownTimeoutSec int    `yaml:"shutdown_timeout"`
	} `yaml:"persistence"`
	Transaction struct {
		PublishTimeoutSec int `yaml:"publish_timeout"`
		MaxRetries        int `yaml:"max_retries"`
	} `yaml:"transaction"`
}

type PumpState int

const (
	StateHook  PumpState = 0
	StateLift  PumpState = 1
	StatePump  PumpState = 2
	StateError PumpState = 9
)

func (s PumpState) String() string {
	switch s {
	case StateHook:
		return "HOOK"
	case StateLift:
		return "LIFT"
	case StatePump:
		return "PUMP"
	default:
		return "ERROR"
	}
}

type DeviceIdentity struct {
	StationID string
	PumpID    string
	DeviceID  string
}

func deviceIdentityForIndex(idx int) DeviceIdentity {
	stationIdx := idx / pumpsPerStation
	stationNum := (stationIdx % numStations) + 1
	pumpNum := (idx % pumpsPerStation) + 1
	stationID := fmt.Sprintf("st-%04d", stationNum)
	pumpID := fmt.Sprintf("p-%04d", pumpNum)
	return DeviceIdentity{
		StationID: stationID,
		PumpID:    pumpID,
		DeviceID:  stationID + ":" + pumpID,
	}
}

type DeviceTXState struct {
	LastIssuedSeq int64 `json:"last_issued_seq"`
	LastAckedSeq  int64 `json:"last_acked_seq"`
}

type PendingTransaction struct {
	DeviceID  string          `json:"device_id"`
	StationID string          `json:"station_id"`
	PumpID    string          `json:"pump_id"`
	Topic     string          `json:"topic"`
	TxSeq     int64           `json:"tx_seq"`
	MessageID string          `json:"message_id"`
	CreatedAt string          `json:"created_at"`
	Payload   json.RawMessage `json:"payload"`
}

type persistedTXState struct {
	Devices map[string]DeviceTXState      `json:"devices"`
	Pending map[string]PendingTransaction `json:"pending"`
}

type TxRegistry struct {
	mu      sync.RWMutex
	devices map[string]DeviceTXState
	pending map[string]PendingTransaction
}

func NewTxRegistry() *TxRegistry {
	return &TxRegistry{
		devices: make(map[string]DeviceTXState),
		pending: make(map[string]PendingTransaction),
	}
}

func (r *TxRegistry) Load(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}

	var persisted persistedTXState
	if err := json.Unmarshal(data, &persisted); err == nil && (persisted.Devices != nil || persisted.Pending != nil) {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.devices = persisted.Devices
		if r.devices == nil {
			r.devices = make(map[string]DeviceTXState)
		}
		r.pending = persisted.Pending
		if r.pending == nil {
			r.pending = make(map[string]PendingTransaction)
		}
		return nil
	}

	var legacy map[string]int64
	if err := json.Unmarshal(data, &legacy); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.devices = make(map[string]DeviceTXState, len(legacy))
	r.pending = make(map[string]PendingTransaction)
	for deviceID, seq := range legacy {
		r.devices[deviceID] = DeviceTXState{
			LastIssuedSeq: seq,
			LastAckedSeq:  seq,
		}
	}
	return nil
}

func (r *TxRegistry) Save(path string) error {
	r.mu.RLock()
	snapshot := r.snapshotLocked()
	r.mu.RUnlock()
	return saveTXSnapshot(path, snapshot)
}

func (r *TxRegistry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.devices)
}

func (r *TxRegistry) PendingCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.pending)
}

func (r *TxRegistry) EnsureDevice(deviceID string) DeviceTXState {
	r.mu.Lock()
	defer r.mu.Unlock()
	state := r.devices[deviceID]
	r.devices[deviceID] = state
	return state
}

func (r *TxRegistry) LastIssued(deviceID string) int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.devices[deviceID].LastIssuedSeq
}

func (r *TxRegistry) ReservePending(path, deviceID string, build func(nextSeq int64) (PendingTransaction, error)) (PendingTransaction, error) {
	r.mu.Lock()
	state := r.devices[deviceID]
	if existing, ok := r.pending[deviceID]; ok {
		r.mu.Unlock()
		return existing, fmt.Errorf("device %s still has pending tx_seq=%d awaiting confirmation", deviceID, existing.TxSeq)
	}

	nextSeq := maxInt64(state.LastIssuedSeq, state.LastAckedSeq) + 1
	pending, err := build(nextSeq)
	if err != nil {
		r.mu.Unlock()
		return PendingTransaction{}, err
	}
	state.LastIssuedSeq = nextSeq
	r.devices[deviceID] = state
	r.pending[deviceID] = pending
	snapshot := r.snapshotLocked()
	r.mu.Unlock()

	if err := saveTXSnapshot(path, snapshot); err != nil {
		return PendingTransaction{}, err
	}
	return pending, nil
}

func (r *TxRegistry) ConfirmPending(path, deviceID string, txSeq int64) error {
	r.mu.Lock()
	state := r.devices[deviceID]
	if txSeq > state.LastIssuedSeq {
		state.LastIssuedSeq = txSeq
	}
	if txSeq > state.LastAckedSeq {
		state.LastAckedSeq = txSeq
	}
	r.devices[deviceID] = state
	if pending, ok := r.pending[deviceID]; ok && pending.TxSeq <= txSeq {
		delete(r.pending, deviceID)
	}
	snapshot := r.snapshotLocked()
	r.mu.Unlock()
	return saveTXSnapshot(path, snapshot)
}

func (r *TxRegistry) PendingTransactions() []PendingTransaction {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]PendingTransaction, 0, len(r.pending))
	for _, pending := range r.pending {
		payloadCopy := make([]byte, len(pending.Payload))
		copy(payloadCopy, pending.Payload)
		pending.Payload = payloadCopy
		result = append(result, pending)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].DeviceID == result[j].DeviceID {
			return result[i].TxSeq < result[j].TxSeq
		}
		return result[i].DeviceID < result[j].DeviceID
	})
	return result
}

func (r *TxRegistry) snapshotLocked() persistedTXState {
	devices := make(map[string]DeviceTXState, len(r.devices))
	for k, v := range r.devices {
		devices[k] = v
	}
	pending := make(map[string]PendingTransaction, len(r.pending))
	for k, v := range r.pending {
		payloadCopy := make([]byte, len(v.Payload))
		copy(payloadCopy, v.Payload)
		v.Payload = payloadCopy
		pending[k] = v
	}
	return persistedTXState{
		Devices: devices,
		Pending: pending,
	}
}

func saveTXSnapshot(path string, snapshot persistedTXState) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	payload, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, payload, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

type SeqLogger struct {
	mu sync.Mutex
	f  *os.File
}

func NewSeqLogger(path string) (*SeqLogger, error) {
	if path == "" {
		return &SeqLogger{}, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return &SeqLogger{f: f}, nil
}

func (l *SeqLogger) Write(entry map[string]any) {
	if l == nil || l.f == nil {
		return
	}
	line, err := json.Marshal(entry)
	if err != nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	_, _ = l.f.Write(append(line, '\n'))
}

func (l *SeqLogger) Close() error {
	if l == nil || l.f == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.f.Close()
}

type MQTTClientPool struct {
	clients []mqtt.Client
}

func NewMQTTClientPool(cfg Config, onCommand mqtt.MessageHandler) (*MQTTClientPool, error) {
	pool := &MQTTClientPool{clients: make([]mqtt.Client, 0, cfg.PoolSize)}
	brokerURL := fmt.Sprintf("tcp://%s:%d", cfg.MQTTHost, cfg.MQTTPort)
	if cfg.MQTTTLS {
		brokerURL = fmt.Sprintf("tls://%s:%d", cfg.MQTTHost, cfg.MQTTPort)
	}
	for i := 0; i < cfg.PoolSize; i++ {
		clientID := fmt.Sprintf("%s-%d-%d", cfg.MQTTClientIDPrefix, i, time.Now().UnixNano())
		opts := mqtt.NewClientOptions().
			AddBroker(brokerURL).
			SetClientID(clientID).
			SetOrderMatters(false).
			SetAutoReconnect(true).
			SetConnectRetry(true).
			SetConnectRetryInterval(2 * time.Second).
			SetKeepAlive(60 * time.Second).
			SetPingTimeout(10 * time.Second).
			SetWriteTimeout(defaultPublishTimeout).
			SetConnectTimeout(defaultConnectTimeout).
			SetDefaultPublishHandler(onCommand)
		if cfg.MQTTUsername != "" {
			opts.SetUsername(cfg.MQTTUsername)
			opts.SetPassword(cfg.MQTTPassword)
		}
		if cfg.MQTTTLS {
			opts.SetTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: true})
		}
		opts.OnConnect = func(c mqtt.Client) {
			topic := fmt.Sprintf("anchr/v1/%s/+/+/cmd", cfg.TenantID)
			token := c.Subscribe(topic, 1, onCommand)
			if ok := token.WaitTimeout(10 * time.Second); !ok || token.Error() != nil {
				log.Printf("WARN mqtt subscribe failed client=%s topic=%s err=%v", clientID, topic, token.Error())
				return
			}
			log.Printf("INFO mqtt client connected and subscribed client=%s topic=%s", clientID, topic)
		}
		opts.OnConnectionLost = func(c mqtt.Client, err error) {
			log.Printf("WARN mqtt connection lost client=%s err=%v", clientID, err)
		}
		client := mqtt.NewClient(opts)
		token := client.Connect()
		if ok := token.WaitTimeout(defaultConnectTimeout); !ok {
			return nil, fmt.Errorf("mqtt connect timeout for client %d", i)
		}
		if err := token.Error(); err != nil {
			return nil, fmt.Errorf("mqtt connect error for client %d: %w", i, err)
		}
		pool.clients = append(pool.clients, client)
	}
	return pool, nil
}

func (p *MQTTClientPool) Get(idx int) mqtt.Client {
	return p.clients[idx%len(p.clients)]
}

func (p *MQTTClientPool) GetByKey(key string) mqtt.Client {
	sum := 0
	for _, b := range []byte(key) {
		sum += int(b)
	}
	return p.Get(sum)
}

func (p *MQTTClientPool) Close() {
	for _, client := range p.clients {
		if client.IsConnected() {
			client.Disconnect(250)
		}
	}
}

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
	identity := deviceIdentityForIndex(index)
	source := rand.NewSource(time.Now().UnixNano() + int64(index)*7919)
	rng := rand.New(source)
	state := registry.EnsureDevice(identity.DeviceID)
	pump := &Pump{
		index:     index,
		identity:  identity,
		cfg:       cfg,
		client:    client,
		registry:  registry,
		seqLogger: seqLogger,
		rng:       rng,
		state:     StateHook,
		shiftNo:   1,
		unitPrice: 20000 + ((((index / pumpsPerStation) % numStations) + 1) * 50),
		currency:  "VND",
		fuelGrade: "RON95",
		pumpRate:  math.Round((0.5+rng.Float64()*1.5)*100) / 100,
		lastTXSeq: state.LastIssuedSeq,
		totalizer: 10000.0 + float64(index*10),
		msgSeqs:   map[string]int64{"telemetry": 0, "tx": 0, "ack": 0},
		done:      make(chan struct{}),
	}
	return pump
}

func (p *Pump) State() PumpState {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.state
}

func (p *Pump) setState(state PumpState) {
	p.mu.Lock()
	p.state = state
	p.mu.Unlock()
}

func (p *Pump) Run(ctx context.Context) {
	defer close(p.done)
	jitter := time.Duration(p.rng.Int63n(int64(defaultSessionStartJitter)))
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
				p.state = StateHook
				p.sessionID = ""
				p.mu.Unlock()
				return
			}
			if ctx.Err() != nil {
				p.mu.Lock()
				p.state = StateHook
				p.sessionID = ""
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
				time.Sleep(1 * time.Second)
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
			p.state = StateHook
			p.sessionID = ""
			p.currentVolume = 0
			p.currentAmount = 0
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
	token := p.client.Publish(p.topicFor(subtopic), qos, false, payload)
	if qos == 0 {
		if writeSeqLog {
			p.seqLogger.Write(map[string]any{
				"device_id":  p.identity.DeviceID,
				"seq":        envelope["seq"],
				"type":       subtopic,
				"message_id": envelope["message_id"],
				"ts":         envelope["event_time"],
			})
		}
		return
	}
	go func() {
		_ = token.WaitTimeout(p.cfg.TXPublishTimeout)
		if err := token.Error(); err != nil {
			log.Printf("WARN publish error device=%s subtopic=%s err=%v", p.identity.DeviceID, subtopic, err)
		}
	}()
}

func publishPendingTransaction(ctx context.Context, client mqtt.Client, cfg Config, pending PendingTransaction) error {
	for attempt := 1; ; attempt++ {
		token := client.Publish(pending.Topic, 1, false, pending.Payload)
		ok := token.WaitTimeout(cfg.TXPublishTimeout)
		if ok && token.Error() == nil {
			return nil
		}

		err := token.Error()
		if !ok && err == nil {
			err = fmt.Errorf("publish timeout waiting for PUBACK")
		}
		log.Printf("WARN tx publish retry device=%s attempt=%d tx_seq=%d err=%v", pending.DeviceID, attempt, pending.TxSeq, err)
		if cfg.MaxTXRetries > 0 && attempt == cfg.MaxTXRetries {
			log.Printf("WARN tx publish reached configured retry threshold device=%s tx_seq=%d threshold=%d; continuing until confirmed to avoid sequence gaps", pending.DeviceID, pending.TxSeq, cfg.MaxTXRetries)
		}

		backoff := time.Duration(minInt(1<<uint(minInt(attempt, 5)), 30)) * time.Second
		if ctx != nil {
			if !sleepWithContext(ctx, backoff) {
				return ctx.Err()
			}
			continue
		}
		time.Sleep(backoff)
	}
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

type ScaleManager struct {
	cfg       Config
	pool      *MQTTClientPool
	registry  *TxRegistry
	seqLogger *SeqLogger
	mu        sync.RWMutex
	pumps     map[int]*Pump
	devices   map[string]*Pump
	target    int
	scalingMu sync.Mutex
}

func NewScaleManager(cfg Config, pool *MQTTClientPool, registry *TxRegistry, seqLogger *SeqLogger) *ScaleManager {
	return &ScaleManager{
		cfg:       cfg,
		pool:      pool,
		registry:  registry,
		seqLogger: seqLogger,
		pumps:     make(map[int]*Pump),
		devices:   make(map[string]*Pump),
		target:    cfg.InitialPumps,
	}
}

func (m *ScaleManager) Run(ctx context.Context) error {
	if err := m.ScaleTo(ctx, m.target); err != nil {
		return err
	}
	persistTicker := time.NewTicker(m.cfg.PersistInterval)
	defer persistTicker.Stop()

	var scaleTicker *time.Ticker
	if m.cfg.ScaleInterval > 0 && m.cfg.StepSize > 0 {
		scaleTicker = time.NewTicker(m.cfg.ScaleInterval)
		defer scaleTicker.Stop()
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-persistTicker.C:
			if err := m.registry.Save(m.cfg.TXStateFile); err != nil {
				log.Printf("WARN persist tx state failed err=%v", err)
			}
		case <-tickerChan(scaleTicker):
			next := m.nextAutoTarget()
			if err := m.ScaleTo(ctx, next); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("WARN auto scale failed target=%d err=%v", next, err)
			}
		}
	}
}

func (m *ScaleManager) nextAutoTarget() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	next := m.target + m.cfg.StepSize
	if next > m.cfg.MaxPumps {
		next = m.cfg.MinPumps
	}
	m.target = next
	return next
}

func (m *ScaleManager) CurrentCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.pumps)
}

func (m *ScaleManager) Status() map[string]any {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return map[string]any{
		"current_pumps": len(m.pumps),
		"target":        m.target,
		"min_pumps":     m.cfg.MinPumps,
		"max_pumps":     m.cfg.MaxPumps,
		"mqtt_host":     m.cfg.MQTTHost,
		"mqtt_port":     m.cfg.MQTTPort,
		"pool_size":     m.cfg.PoolSize,
		"tx_registry":   m.registry.Len(),
		"pending_tx":    m.registry.PendingCount(),
	}
}

func (m *ScaleManager) ScaleTo(ctx context.Context, count int) error {
	m.scalingMu.Lock()
	defer m.scalingMu.Unlock()
	if count < m.cfg.MinPumps || count > m.cfg.MaxPumps {
		return fmt.Errorf("target_pumps must be between %d and %d", m.cfg.MinPumps, m.cfg.MaxPumps)
	}

	current := m.CurrentCount()
	if count == current {
		m.mu.Lock()
		m.target = count
		m.mu.Unlock()
		log.Printf("INFO scale unchanged pumps=%d", count)
		return nil
	}

	if count > current {
		for idx := current; idx < count; idx++ {
			pump := NewPump(idx, m.cfg, m.pool.Get(idx), m.registry, m.seqLogger)
			pumpCtx, cancel := context.WithCancel(ctx)
			pump.attachCancel(cancel)
			m.mu.Lock()
			m.pumps[idx] = pump
			m.devices[pump.identity.DeviceID] = pump
			m.target = count
			m.mu.Unlock()
			go pump.Run(pumpCtx)
		}
		log.Printf("INFO scaled up previous=%d current=%d", current, count)
		return nil
	}

	toRemove := make([]*Pump, 0, current-count)
	m.mu.Lock()
	for idx := current - 1; idx >= count; idx-- {
		if pump, ok := m.pumps[idx]; ok {
			toRemove = append(toRemove, pump)
			delete(m.pumps, idx)
			delete(m.devices, pump.identity.DeviceID)
		}
	}
	m.target = count
	m.mu.Unlock()

	for _, pump := range toRemove {
		pump.Stop()
	}
	for _, pump := range toRemove {
		select {
		case <-pump.Done():
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := m.registry.Save(m.cfg.TXStateFile); err != nil {
		log.Printf("WARN persist tx state after scale down failed err=%v", err)
	}
	log.Printf("INFO scaled down previous=%d current=%d", current, count)
	return nil
}

func (m *ScaleManager) Shutdown(ctx context.Context) error {
	m.scalingMu.Lock()
	pumps := make([]*Pump, 0, len(m.pumps))
	m.mu.RLock()
	for _, pump := range m.pumps {
		pumps = append(pumps, pump)
	}
	m.mu.RUnlock()
	m.scalingMu.Unlock()

	for _, pump := range pumps {
		pump.Stop()
	}
	for _, pump := range pumps {
		select {
		case <-pump.Done():
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return m.registry.Save(m.cfg.TXStateFile)
}

func (m *ScaleManager) PumpByTopic(topic string) (*Pump, bool) {
	parts := strings.Split(topic, "/")
	if len(parts) != 6 {
		return nil, false
	}
	deviceID := parts[3] + ":" + parts[4]
	m.mu.RLock()
	defer m.mu.RUnlock()
	pump, ok := m.devices[deviceID]
	return pump, ok
}

type HTTPAPI struct {
	manager *ScaleManager
}

func NewHTTPAPI(manager *ScaleManager) *HTTPAPI { return &HTTPAPI{manager: manager} }

func (a *HTTPAPI) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", a.handleHealth)
	mux.HandleFunc("/api/status", a.handleStatus)
	mux.HandleFunc("/api/scale", a.handleScale)
	return mux
}

func (a *HTTPAPI) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (a *HTTPAPI) handleStatus(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, a.manager.Status())
}

func (a *HTTPAPI) handleScale(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	var req struct {
		TargetPumps int `json:"target_pumps"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
		return
	}
	prev := a.manager.CurrentCount()
	if err := a.manager.ScaleTo(context.Background(), req.TargetPumps); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":         "ok",
		"previous_pumps": prev,
		"target_pumps":   req.TargetPumps,
		"current_pumps":  a.manager.CurrentCount(),
	})
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	registry := NewTxRegistry()
	if err := registry.Load(cfg.TXStateFile); err != nil {
		log.Printf("WARN failed to load tx state file=%s err=%v", cfg.TXStateFile, err)
	} else {
		log.Printf("INFO loaded tx state devices=%d pending=%d file=%s", registry.Len(), registry.PendingCount(), cfg.TXStateFile)
	}
	seqLogger, err := NewSeqLogger(cfg.SeqLogFile)
	if err != nil {
		log.Fatalf("failed to open seq log: %v", err)
	}
	defer func() {
		if err := seqLogger.Close(); err != nil {
			log.Printf("WARN closing seq log failed err=%v", err)
		}
	}()

	var manager *ScaleManager
	pool, err := NewMQTTClientPool(cfg, func(_ mqtt.Client, msg mqtt.Message) {
		if manager == nil {
			return
		}
		pump, ok := manager.PumpByTopic(msg.Topic())
		if !ok {
			return
		}
		var envelope map[string]any
		if err := json.Unmarshal(msg.Payload(), &envelope); err != nil {
			log.Printf("WARN invalid command payload topic=%s err=%v", msg.Topic(), err)
			return
		}
		pump.handleCommand(envelope)
	})
	if err != nil {
		log.Fatalf("failed to create mqtt pool: %v", err)
	}
	defer pool.Close()

	if err := replayPendingTransactions(ctx, cfg, pool, registry, seqLogger); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("failed to replay pending transactions: %v", err)
	}

	manager = NewScaleManager(cfg, pool, registry, seqLogger)

	api := NewHTTPAPI(manager)
	httpServer := &http.Server{Addr: cfg.HTTPAddr, Handler: api.Routes()}
	go func() {
		log.Printf("INFO http api listening addr=%s", cfg.HTTPAddr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server failed: %v", err)
		}
	}()

	go func() {
		if err := manager.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("WARN manager exited err=%v", err)
		}
	}()

	<-ctx.Done()
	log.Printf("INFO shutdown signal received")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("WARN http shutdown failed err=%v", err)
	}
	if err := manager.Shutdown(context.Background()); err != nil {
		log.Printf("WARN manager shutdown failed err=%v", err)
	}
	if err := registry.Save(cfg.TXStateFile); err != nil {
		log.Printf("WARN final tx state persist failed err=%v", err)
	}
	log.Printf("INFO simulator shut down cleanly")
}

func replayPendingTransactions(ctx context.Context, cfg Config, pool *MQTTClientPool, registry *TxRegistry, seqLogger *SeqLogger) error {
	pendingTransactions := registry.PendingTransactions()
	if len(pendingTransactions) == 0 {
		return nil
	}

	log.Printf("INFO replaying pending transactions count=%d", len(pendingTransactions))
	for _, pending := range pendingTransactions {
		if err := publishPendingTransaction(ctx, pool.GetByKey(pending.DeviceID), cfg, pending); err != nil {
			return err
		}
		if err := registry.ConfirmPending(cfg.TXStateFile, pending.DeviceID, pending.TxSeq); err != nil {
			return err
		}
		seqLogger.Write(map[string]any{
			"device_id":  pending.DeviceID,
			"type":       "tx-replay",
			"message_id": pending.MessageID,
			"tx_seq":     pending.TxSeq,
			"ts":         pending.CreatedAt,
		})
		log.Printf("INFO replayed pending tx device=%s tx_seq=%d", pending.DeviceID, pending.TxSeq)
	}
	return nil
}

func loadConfig() (Config, error) {
	var configPath string
	flag.StringVar(&configPath, "config", "config.json", "path to config file (.json or .yaml)")
	flag.Parse()

	cfg := Config{
		MQTTHost:           "localhost",
		MQTTPort:           1883,
		MQTTTLS:            false,
		TenantID:           "default",
		Speed:              10,
		InitialPumps:       1500,
		MinPumps:           1000,
		MaxPumps:           5000,
		StepSize:           1000,
		ScaleInterval:      1800 * time.Second,
		PoolSize:           50,
		HTTPAddr:           defaultHTTPAddr,
		TXStateFile:        getEnv("TX_STATE_FILE", defaultTXStateFile),
		TXPublishTimeout:   defaultPublishTimeout,
		MaxTXRetries:       0,
		PersistInterval:    defaultPersistInterval,
		ShutdownTimeout:    defaultShutdownTimeout,
		SeqLogFile:         os.Getenv("SEQ_LOG_FILE"),
		MQTTUsername:       os.Getenv("MQTT_USERNAME"),
		MQTTPassword:       os.Getenv("MQTT_PASSWORD"),
		MQTTClientIDPrefix: getEnv("MQTT_CLIENT_ID_PREFIX", "sim-pool"),
	}

	if configPath != "" {
		if raw, err := os.ReadFile(configPath); err == nil {
			var rc rawConfig
			switch strings.ToLower(filepath.Ext(configPath)) {
			case ".json":
				if err := json.Unmarshal(raw, &rc); err != nil {
					return Config{}, fmt.Errorf("parse config json: %w", err)
				}
			default:
				if err := yaml.Unmarshal(raw, &rc); err != nil {
					return Config{}, fmt.Errorf("parse config yaml: %w", err)
				}
			}
			if rc.MQTT.Host != "" {
				cfg.MQTTHost = rc.MQTT.Host
			}
			if rc.MQTT.Port != 0 {
				cfg.MQTTPort = rc.MQTT.Port
			}
			cfg.MQTTTLS = rc.MQTT.TLS
			if rc.MQTT.Username != "" {
				cfg.MQTTUsername = rc.MQTT.Username
			}
			if rc.MQTT.Password != "" {
				cfg.MQTTPassword = rc.MQTT.Password
			}
			if rc.Simulation.TenantID != "" {
				cfg.TenantID = rc.Simulation.TenantID
			}
			if rc.Simulation.Speed != 0 {
				cfg.Speed = rc.Simulation.Speed
			}
			if rc.Simulation.InitialPumps != 0 {
				cfg.InitialPumps = rc.Simulation.InitialPumps
			}
			if rc.Simulation.MinPumps != 0 {
				cfg.MinPumps = rc.Simulation.MinPumps
			}
			if rc.Simulation.MaxPumps != 0 {
				cfg.MaxPumps = rc.Simulation.MaxPumps
			}
			if rc.Simulation.StepSize != 0 {
				cfg.StepSize = rc.Simulation.StepSize
			}
			if rc.Simulation.ScaleIntervalSec != 0 {
				cfg.ScaleInterval = time.Duration(rc.Simulation.ScaleIntervalSec) * time.Second
			}
			if rc.Pool.Size != 0 {
				cfg.PoolSize = rc.Pool.Size
			}
			if rc.HTTP.Addr != "" {
				cfg.HTTPAddr = rc.HTTP.Addr
			}
			if rc.Persistence.TXStateFile != "" {
				cfg.TXStateFile = rc.Persistence.TXStateFile
			}
			if rc.Persistence.PersistIntervalSec != 0 {
				cfg.PersistInterval = time.Duration(rc.Persistence.PersistIntervalSec) * time.Second
			}
			if rc.Persistence.ShutdownTimeoutSec != 0 {
				cfg.ShutdownTimeout = time.Duration(rc.Persistence.ShutdownTimeoutSec) * time.Second
			}
			if rc.Transaction.PublishTimeoutSec != 0 {
				cfg.TXPublishTimeout = time.Duration(rc.Transaction.PublishTimeoutSec) * time.Second
			}
			if rc.Transaction.MaxRetries != 0 {
				cfg.MaxTXRetries = rc.Transaction.MaxRetries
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return Config{}, err
		}
	}

	cfg.MQTTHost = getEnv("MQTT_HOST", cfg.MQTTHost)
	cfg.MQTTPort = getEnvInt("MQTT_PORT", cfg.MQTTPort)
	cfg.MQTTTLS = getEnvBool("MQTT_TLS", cfg.MQTTTLS)
	cfg.TenantID = getEnv("TENANT_ID", cfg.TenantID)
	cfg.Speed = maxInt(1, getEnvInt("SPEED", cfg.Speed))
	cfg.InitialPumps = getEnvInt("INITIAL_PUMPS", cfg.InitialPumps)
	cfg.MinPumps = getEnvInt("MIN_PUMPS", cfg.MinPumps)
	cfg.MaxPumps = getEnvInt("MAX_PUMPS", cfg.MaxPumps)
	cfg.StepSize = getEnvInt("STEP_SIZE", cfg.StepSize)
	cfg.ScaleInterval = time.Duration(getEnvInt("SCALE_INTERVAL", int(cfg.ScaleInterval.Seconds()))) * time.Second
	cfg.PoolSize = maxInt(1, getEnvInt("POOL_SIZE", cfg.PoolSize))
	cfg.HTTPAddr = getEnv("HTTP_ADDR", cfg.HTTPAddr)
	cfg.TXStateFile = getEnv("TX_STATE_FILE", cfg.TXStateFile)
	cfg.TXPublishTimeout = time.Duration(getEnvInt("TX_PUBLISH_TIMEOUT", int(cfg.TXPublishTimeout.Seconds()))) * time.Second
	cfg.MaxTXRetries = getEnvInt("MAX_TX_RETRIES", cfg.MaxTXRetries)
	cfg.PersistInterval = time.Duration(getEnvInt("PERSIST_INTERVAL", int(cfg.PersistInterval.Seconds()))) * time.Second
	cfg.ShutdownTimeout = time.Duration(getEnvInt("GRACEFUL_SHUTDOWN_TIMEOUT", int(cfg.ShutdownTimeout.Seconds()))) * time.Second
	cfg.MQTTUsername = getEnv("MQTT_USERNAME", cfg.MQTTUsername)
	cfg.MQTTPassword = getEnv("MQTT_PASSWORD", cfg.MQTTPassword)
	cfg.MQTTClientIDPrefix = getEnv("MQTT_CLIENT_ID_PREFIX", cfg.MQTTClientIDPrefix)

	if cfg.MinPumps < 0 || cfg.MaxPumps < cfg.MinPumps || cfg.InitialPumps < cfg.MinPumps || cfg.InitialPumps > cfg.MaxPumps {
		return Config{}, fmt.Errorf("invalid pump bounds min=%d initial=%d max=%d", cfg.MinPumps, cfg.InitialPumps, cfg.MaxPumps)
	}
	return cfg, nil
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func nowRFC3339Millis() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}

func randomUUID(r *rand.Rand) string {
	const hex = "0123456789abcdef"
	b := make([]byte, 36)
	for i := range b {
		b[i] = hex[r.Intn(len(hex))]
	}
	b[8], b[13], b[18], b[23] = '-', '-', '-', '-'
	b[14] = '4'
	b[19] = "89ab"[r.Intn(4)]
	return string(b)
}

func sleepWithContext(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func tickerChan(t *time.Ticker) <-chan time.Time {
	if t == nil {
		return nil
	}
	return t.C
}

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func getEnvBool(key string, fallback bool) bool {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	switch strings.ToLower(value) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return fallback
	}
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func round1(v float64) float64 { return math.Round(v*10) / 10 }
func round3(v float64) float64 { return math.Round(v*1000) / 1000 }

func asInt(v any) (int, bool) {
	switch value := v.(type) {
	case int:
		return value, true
	case int64:
		return int(value), true
	case float64:
		return int(value), true
	default:
		return 0, false
	}
}
