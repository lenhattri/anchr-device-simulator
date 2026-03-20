package simulator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"strings"
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

type mqttFactoryTestClient struct {
	mu             sync.Mutex
	opts           *mqtt.ClientOptions
	subscribeCalls int
	connectRelease <-chan struct{}
}

func (c *mqttFactoryTestClient) IsConnected() bool      { return true }
func (c *mqttFactoryTestClient) IsConnectionOpen() bool { return true }
func (c *mqttFactoryTestClient) Connect() mqtt.Token {
	if c.connectRelease != nil {
		<-c.connectRelease
	}
	if c.opts != nil && c.opts.OnConnect != nil {
		c.opts.OnConnect(c)
	}
	return newMQTTTestToken(nil)
}
func (c *mqttFactoryTestClient) Disconnect(uint) {}
func (c *mqttFactoryTestClient) Subscribe(string, byte, mqtt.MessageHandler) mqtt.Token {
	c.mu.Lock()
	c.subscribeCalls++
	c.mu.Unlock()
	return newMQTTTestToken(nil)
}
func (c *mqttFactoryTestClient) SubscribeMultiple(map[string]byte, mqtt.MessageHandler) mqtt.Token {
	return newMQTTTestToken(nil)
}
func (c *mqttFactoryTestClient) Unsubscribe(...string) mqtt.Token     { return newMQTTTestToken(nil) }
func (c *mqttFactoryTestClient) AddRoute(string, mqtt.MessageHandler) {}
func (c *mqttFactoryTestClient) OptionsReader() mqtt.ClientOptionsReader {
	return mqtt.ClientOptionsReader{}
}
func (c *mqttFactoryTestClient) Publish(string, byte, bool, interface{}) mqtt.Token {
	return newMQTTTestToken(nil)
}

func TestNewMQTTClientPoolSubscribesCommandsOnce(t *testing.T) {
	previousFactory := mqttClientFactory
	defer func() { mqttClientFactory = previousFactory }()

	var mu sync.Mutex
	clients := make([]*mqttFactoryTestClient, 0, 3)
	mqttClientFactory = func(opts *mqtt.ClientOptions) mqtt.Client {
		client := &mqttFactoryTestClient{opts: opts}
		mu.Lock()
		clients = append(clients, client)
		mu.Unlock()
		return client
	}

	pool, err := NewMQTTClientPool(Config{
		MQTTHost:           "broker",
		MQTTPort:           1883,
		TenantID:           "default",
		PoolSize:           3,
		MQTTClientIDPrefix: "sim-pool",
	}, func(mqtt.Client, mqtt.Message) {})
	if err != nil {
		t.Fatalf("new mqtt client pool: %v", err)
	}
	if got := len(pool.clients); got != 3 {
		t.Fatalf("expected 3 clients in pool got %d", got)
	}

	totalSubscriptions := 0
	for _, client := range clients {
		client.mu.Lock()
		totalSubscriptions += client.subscribeCalls
		client.mu.Unlock()
	}
	if totalSubscriptions != 1 {
		t.Fatalf("expected exactly one command subscription got %d", totalSubscriptions)
	}
}

func TestNewMQTTClientPoolConnectsClientsConcurrently(t *testing.T) {
	previousFactory := mqttClientFactory
	defer func() { mqttClientFactory = previousFactory }()

	const poolSize = 3
	release := make(chan struct{})
	var started sync.WaitGroup
	started.Add(poolSize)
	mqttClientFactory = func(opts *mqtt.ClientOptions) mqtt.Client {
		return &mqttFactoryTestClient{
			opts: opts,
			connectRelease: func() <-chan struct{} {
				started.Done()
				return release
			}(),
		}
	}

	done := make(chan error, 1)
	go func() {
		_, err := NewMQTTClientPool(Config{
			MQTTHost:           "broker",
			MQTTPort:           1883,
			TenantID:           "default",
			PoolSize:           poolSize,
			MQTTClientIDPrefix: "sim-pool",
		}, func(mqtt.Client, mqtt.Message) {})
		done <- err
	}()

	ready := make(chan struct{})
	go func() {
		started.Wait()
		close(ready)
	}()

	select {
	case <-ready:
		close(release)
	case <-time.After(time.Second):
		t.Fatal("expected all mqtt clients to begin connecting concurrently")
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("new mqtt client pool: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("expected mqtt client pool creation to finish after releasing connects")
	}
}

func TestLogPublishRetryOmitsZeroTxSeqForNonTransactionMessages(t *testing.T) {
	var logBuffer bytes.Buffer
	previousWriter := log.Writer()
	previousFlags := log.Flags()
	log.SetOutput(&logBuffer)
	log.SetFlags(0)
	defer func() {
		log.SetOutput(previousWriter)
		log.SetFlags(previousFlags)
	}()

	logPublishRetry(PendingTransaction{
		DeviceID: "st-0001:p-0001",
		Topic:    "anchr/v1/default/st-0001/p-0001/telemetry",
	}, 1, errors.New("publish timeout waiting for PUBACK"))

	output := logBuffer.String()
	if !strings.Contains(output, "subtopic=telemetry") {
		t.Fatalf("expected telemetry subtopic in log got %q", output)
	}
	if strings.Contains(output, "tx_seq=0") {
		t.Fatalf("did not expect zero tx_seq in non-transaction retry log got %q", output)
	}
}

func TestLogPublishRetryIncludesTxSeqForTransactions(t *testing.T) {
	var logBuffer bytes.Buffer
	previousWriter := log.Writer()
	previousFlags := log.Flags()
	log.SetOutput(&logBuffer)
	log.SetFlags(0)
	defer func() {
		log.SetOutput(previousWriter)
		log.SetFlags(previousFlags)
	}()

	logPublishRetry(PendingTransaction{
		DeviceID: "st-0001:p-0001",
		Topic:    "anchr/v1/default/st-0001/p-0001/tx",
		TxSeq:    19,
	}, 1, errors.New("publish timeout waiting for PUBACK"))

	output := logBuffer.String()
	if !strings.Contains(output, "subtopic=tx") || !strings.Contains(output, "tx_seq=19") {
		t.Fatalf("expected tx subtopic and tx_seq in log got %q", output)
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

func TestPublishRetryBackoff(t *testing.T) {
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: 0, want: 200 * time.Millisecond},
		{attempt: 1, want: 200 * time.Millisecond},
		{attempt: 2, want: 400 * time.Millisecond},
		{attempt: 3, want: 800 * time.Millisecond},
		{attempt: 4, want: 1600 * time.Millisecond},
		{attempt: 5, want: 2 * time.Second},
		{attempt: 9, want: 2 * time.Second},
	}
	for _, tc := range cases {
		if got := publishRetryBackoff(tc.attempt); got != tc.want {
			t.Fatalf("attempt=%d expected backoff %s got %s", tc.attempt, tc.want, got)
		}
	}
}
