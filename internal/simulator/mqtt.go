package simulator

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type MQTTClientPool struct {
	clients []mqtt.Client
}

var mqttClientFactory = mqtt.NewClient

func NewMQTTClientPool(cfg Config, onCommand mqtt.MessageHandler) (*MQTTClientPool, error) {
	pool := &MQTTClientPool{clients: make([]mqtt.Client, cfg.PoolSize)}
	brokerURL := fmt.Sprintf("tcp://%s:%d", cfg.MQTTHost, cfg.MQTTPort)
	if cfg.MQTTTLS {
		brokerURL = fmt.Sprintf("tls://%s:%d", cfg.MQTTHost, cfg.MQTTPort)
	}

	topic := fmt.Sprintf("anchr/v1/%s/+/+/cmd", cfg.TenantID)
	type connectResult struct {
		idx    int
		client mqtt.Client
		err    error
	}
	results := make(chan connectResult, cfg.PoolSize)
	var wg sync.WaitGroup
	for i := 0; i < cfg.PoolSize; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			clientID := fmt.Sprintf("%s-%d-%d", cfg.MQTTClientIDPrefix, idx, time.Now().UnixNano())
			opts := mqtt.NewClientOptions().
				AddBroker(brokerURL).
				SetClientID(clientID).
				SetOrderMatters(false).
				SetAutoReconnect(true).
				SetConnectRetry(true).
				SetConnectRetryInterval(2 * time.Second).
				SetKeepAlive(60 * time.Second).
				SetPingTimeout(10 * time.Second).
				SetWriteTimeout(DefaultPublishTimeout).
				SetConnectTimeout(DefaultConnectTimeout).
				SetDefaultPublishHandler(onCommand)
			if cfg.MQTTUsername != "" {
				opts.SetUsername(cfg.MQTTUsername)
				opts.SetPassword(cfg.MQTTPassword)
			}
			if cfg.MQTTTLS {
				opts.SetTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: true})
			}
			if idx == 0 {
				opts.OnConnect = func(c mqtt.Client) {
					token := c.Subscribe(topic, 1, onCommand)
					if ok := token.WaitTimeout(10 * time.Second); !ok || token.Error() != nil {
						log.Printf("WARN mqtt subscribe failed client=%s topic=%s err=%v", clientID, topic, token.Error())
						return
					}
					log.Printf("INFO mqtt client connected and subscribed client=%s topic=%s", clientID, topic)
				}
			} else {
				opts.OnConnect = func(mqtt.Client) {
					log.Printf("INFO mqtt client connected client=%s", clientID)
				}
			}
			opts.OnConnectionLost = func(c mqtt.Client, err error) {
				log.Printf("WARN mqtt connection lost client=%s err=%v", clientID, err)
			}

			client := mqttClientFactory(opts)
			token := client.Connect()
			if ok := token.WaitTimeout(DefaultConnectTimeout); !ok {
				results <- connectResult{idx: idx, err: fmt.Errorf("mqtt connect timeout for client %d", idx)}
				return
			}
			if err := token.Error(); err != nil {
				results <- connectResult{idx: idx, err: fmt.Errorf("mqtt connect error for client %d: %w", idx, err)}
				return
			}
			results <- connectResult{idx: idx, client: client}
		}(i)
	}
	wg.Wait()
	close(results)

	for result := range results {
		if result.err != nil {
			pool.Close()
			return nil, result.err
		}
		pool.clients[result.idx] = result.client
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

func publishPendingTransaction(ctx context.Context, client mqtt.Client, cfg Config, pending PendingTransaction) error {
	for attempt := 1; ; attempt++ {
		token := client.Publish(pending.Topic, 1, false, []byte(pending.Payload))
		ok := token.WaitTimeout(cfg.TXPublishTimeout)
		if ok && token.Error() == nil {
			return nil
		}

		err := token.Error()
		if !ok && err == nil {
			err = fmt.Errorf("publish timeout waiting for PUBACK")
		}
		logPublishRetry(pending, attempt, err)
		if cfg.MaxTXRetries > 0 && attempt == cfg.MaxTXRetries {
			logPublishRetryThreshold(pending, cfg.MaxTXRetries)
		}

		backoff := publishRetryBackoff(attempt)
		if ctx != nil {
			if !sleepWithContext(ctx, backoff) {
				return ctx.Err()
			}
			continue
		}
		time.Sleep(backoff)
	}
}

func publishRetryBackoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	shift := minInt(attempt-1, 4)
	backoff := 200 * time.Millisecond * time.Duration(1<<uint(shift))
	if backoff > 2*time.Second {
		return 2 * time.Second
	}
	return backoff
}

func logPublishRetry(pending PendingTransaction, attempt int, err error) {
	subtopic := topicTail(pending.Topic)
	if pending.TxSeq > 0 {
		log.Printf("WARN mqtt publish retry device=%s subtopic=%s attempt=%d tx_seq=%d err=%v", pending.DeviceID, subtopic, attempt, pending.TxSeq, err)
		return
	}
	log.Printf("WARN mqtt publish retry device=%s subtopic=%s attempt=%d err=%v", pending.DeviceID, subtopic, attempt, err)
}

func logPublishRetryThreshold(pending PendingTransaction, threshold int) {
	subtopic := topicTail(pending.Topic)
	if pending.TxSeq > 0 {
		log.Printf("WARN mqtt publish reached configured retry threshold device=%s subtopic=%s tx_seq=%d threshold=%d; continuing until confirmed to avoid sequence gaps", pending.DeviceID, subtopic, pending.TxSeq, threshold)
		return
	}
	log.Printf("WARN mqtt publish reached configured retry threshold device=%s subtopic=%s threshold=%d", pending.DeviceID, subtopic, threshold)
}

func topicTail(topic string) string {
	for i := len(topic) - 1; i >= 0; i-- {
		if topic[i] == '/' {
			return topic[i+1:]
		}
	}
	return topic
}
