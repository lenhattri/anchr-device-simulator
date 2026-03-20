package simulator

import (
	"encoding/json"
	"path/filepath"
	"sync"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type fakeToken struct {
	err  error
	done chan struct{}
}

func newFakeToken(err error) *fakeToken {
	done := make(chan struct{})
	close(done)
	return &fakeToken{err: err, done: done}
}

func (t *fakeToken) Wait() bool                     { return true }
func (t *fakeToken) WaitTimeout(time.Duration) bool { return true }
func (t *fakeToken) Done() <-chan struct{}          { return t.done }
func (t *fakeToken) Error() error                   { return t.err }

type publishedMessage struct {
	topic   string
	qos     byte
	payload []byte
}

type fakeMQTTClient struct {
	mu       sync.Mutex
	messages []publishedMessage
}

func (c *fakeMQTTClient) IsConnected() bool      { return true }
func (c *fakeMQTTClient) IsConnectionOpen() bool { return true }
func (c *fakeMQTTClient) Connect() mqtt.Token    { return newFakeToken(nil) }
func (c *fakeMQTTClient) Disconnect(uint)        {}
func (c *fakeMQTTClient) Subscribe(string, byte, mqtt.MessageHandler) mqtt.Token {
	return newFakeToken(nil)
}
func (c *fakeMQTTClient) SubscribeMultiple(map[string]byte, mqtt.MessageHandler) mqtt.Token {
	return newFakeToken(nil)
}
func (c *fakeMQTTClient) Unsubscribe(...string) mqtt.Token     { return newFakeToken(nil) }
func (c *fakeMQTTClient) AddRoute(string, mqtt.MessageHandler) {}
func (c *fakeMQTTClient) OptionsReader() mqtt.ClientOptionsReader {
	return mqtt.ClientOptionsReader{}
}
func (c *fakeMQTTClient) Publish(topic string, qos byte, retained bool, payload interface{}) mqtt.Token {
	var bytes []byte
	switch v := payload.(type) {
	case []byte:
		bytes = v
	case json.RawMessage:
		bytes = []byte(v)
	case string:
		bytes = []byte(v)
	default:
		encoded, _ := json.Marshal(v)
		bytes = encoded
	}
	copied := append([]byte(nil), bytes...)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.messages = append(c.messages, publishedMessage{topic: topic, qos: qos, payload: copied})
	return newFakeToken(nil)
}

func (c *fakeMQTTClient) publishedPayloads(t *testing.T) []map[string]any {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]map[string]any, 0, len(c.messages))
	for _, message := range c.messages {
		var envelope map[string]any
		if err := json.Unmarshal(message.payload, &envelope); err != nil {
			t.Fatalf("unmarshal published payload: %v", err)
		}
		result = append(result, envelope)
	}
	return result
}

func TestPumpUsesIndependentSequencesPerMessageType(t *testing.T) {
	client := &fakeMQTTClient{}
	cfg := Config{TenantID: "tenant-a", TXPublishTimeout: time.Second}
	pump := NewPump(0, cfg, client, NewTxRegistry(), &SeqLogger{})

	if err := pump.publishGuaranteed(nil, "telemetry", "telemetry", map[string]any{"state_code": 0}, false); err != nil {
		t.Fatalf("publish first telemetry: %v", err)
	}
	if err := pump.publishGuaranteed(nil, "telemetry", "telemetry", map[string]any{"state_code": 1}, false); err != nil {
		t.Fatalf("publish second telemetry: %v", err)
	}
	pump.handleCommand(map[string]any{
		"data": map[string]any{
			"command": map[string]any{
				"type":    "set_price",
				"cmd_id":  "cmd-1",
				"payload": map[string]any{"unit_price": 21000},
			},
		},
	})

	payloads := client.publishedPayloads(t)
	if len(payloads) != 3 {
		t.Fatalf("expected 3 published messages got %d", len(payloads))
	}
	if got := int64(payloads[0]["seq"].(float64)); got != 1 {
		t.Fatalf("first telemetry expected seq 1 got %d", got)
	}
	if got := int64(payloads[1]["seq"].(float64)); got != 2 {
		t.Fatalf("second telemetry expected seq 2 got %d", got)
	}
	if payloads[2]["type"] != "ack" {
		t.Fatalf("expected third message to be ack got %v", payloads[2]["type"])
	}
	if got := int64(payloads[2]["seq"].(float64)); got != 1 {
		t.Fatalf("ack expected seq 1 got %d", got)
	}
	if payloads[2]["correlation_id"] != "cmd-1" {
		t.Fatalf("expected ack correlation_id cmd-1 got %v", payloads[2]["correlation_id"])
	}
}

func TestPumpTransactionSequenceIsIndependentFromTelemetry(t *testing.T) {
	client := &fakeMQTTClient{}
	cfg := Config{
		TenantID:         "tenant-a",
		TXPublishTimeout: time.Second,
		TXStateFile:      filepath.Join(t.TempDir(), "tx-state.json"),
	}
	pump := NewPump(0, cfg, client, NewTxRegistry(), &SeqLogger{})

	if err := pump.publishGuaranteed(nil, "telemetry", "telemetry", map[string]any{"state_code": 0}, false); err != nil {
		t.Fatalf("publish telemetry: %v", err)
	}

	pump.mu.Lock()
	pump.startTime = time.Now().Add(-2 * time.Minute).UTC()
	pump.currentVolume = 12.345
	pump.currentAmount = 246900
	pump.shiftNo = 7
	pump.unitPrice = 20000
	pump.fuelGrade = "RON95"
	pump.currency = "VND"
	pump.startTotalizer = 100
	pump.totalizer = 112.345
	pump.mu.Unlock()

	if err := pump.publishTransactionGuaranteed(); err != nil {
		t.Fatalf("publish transaction: %v", err)
	}

	payloads := client.publishedPayloads(t)
	if len(payloads) != 2 {
		t.Fatalf("expected 2 published messages got %d", len(payloads))
	}
	if got := int64(payloads[0]["seq"].(float64)); got != 1 {
		t.Fatalf("expected telemetry seq 1 got %d", got)
	}
	if got := int64(payloads[1]["seq"].(float64)); got != 1 {
		t.Fatalf("expected tx message seq 1 got %d", got)
	}
	data := payloads[1]["data"].(map[string]any)
	if got := int64(data["tx_seq"].(float64)); got != 1 {
		t.Fatalf("expected tx_seq 1 got %d", got)
	}
}
