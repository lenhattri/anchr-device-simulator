package simulator

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type mqttTestToken struct {
	err  error
	done chan struct{}
}

func newMQTTTestToken(err error) *mqttTestToken {
	done := make(chan struct{})
	close(done)
	return &mqttTestToken{err: err, done: done}
}

func (t *mqttTestToken) Wait() bool                     { return true }
func (t *mqttTestToken) WaitTimeout(time.Duration) bool { return true }
func (t *mqttTestToken) Done() <-chan struct{}          { return t.done }
func (t *mqttTestToken) Error() error                   { return t.err }

type mqttPayloadCapturingClient struct {
	mu           sync.Mutex
	payloads     [][]byte
	payloadTypes []string
}

func (c *mqttPayloadCapturingClient) IsConnected() bool      { return true }
func (c *mqttPayloadCapturingClient) IsConnectionOpen() bool { return true }
func (c *mqttPayloadCapturingClient) Connect() mqtt.Token    { return newMQTTTestToken(nil) }
func (c *mqttPayloadCapturingClient) Disconnect(uint)        {}
func (c *mqttPayloadCapturingClient) Subscribe(string, byte, mqtt.MessageHandler) mqtt.Token {
	return newMQTTTestToken(nil)
}
func (c *mqttPayloadCapturingClient) SubscribeMultiple(map[string]byte, mqtt.MessageHandler) mqtt.Token {
	return newMQTTTestToken(nil)
}
func (c *mqttPayloadCapturingClient) Unsubscribe(...string) mqtt.Token     { return newMQTTTestToken(nil) }
func (c *mqttPayloadCapturingClient) AddRoute(string, mqtt.MessageHandler) {}
func (c *mqttPayloadCapturingClient) OptionsReader() mqtt.ClientOptionsReader {
	return mqtt.ClientOptionsReader{}
}
func (c *mqttPayloadCapturingClient) Publish(topic string, qos byte, retained bool, payload interface{}) mqtt.Token {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch v := payload.(type) {
	case []byte:
		c.payloadTypes = append(c.payloadTypes, "[]byte")
		c.payloads = append(c.payloads, append([]byte(nil), v...))
		return newMQTTTestToken(nil)
	case string:
		c.payloadTypes = append(c.payloadTypes, "string")
		c.payloads = append(c.payloads, []byte(v))
		return newMQTTTestToken(nil)
	default:
		c.payloadTypes = append(c.payloadTypes, "other")
		return newMQTTTestToken(errors.New("unknown payload type"))
	}
}

func TestPublishPendingTransactionNormalizesRawMessagePayload(t *testing.T) {
	client := &mqttPayloadCapturingClient{}
	cfg := Config{TXPublishTimeout: time.Second}
	pending := PendingTransaction{
		DeviceID: "st-0001:p-0001",
		Topic:    "anchr/v1/default/st-0001/p-0001/tx",
		Payload:  json.RawMessage(`{"tx_seq":1}`),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := publishPendingTransaction(ctx, client, cfg, pending); err != nil {
		t.Fatalf("publish pending transaction: %v", err)
	}

	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.payloadTypes) != 1 {
		t.Fatalf("expected 1 publish attempt got %d", len(client.payloadTypes))
	}
	if client.payloadTypes[0] != "[]byte" {
		t.Fatalf("expected payload type []byte got %s", client.payloadTypes[0])
	}
	if string(client.payloads[0]) != `{"tx_seq":1}` {
		t.Fatalf("unexpected payload body %s", string(client.payloads[0]))
	}
}
