package simulator

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

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
		topic := fmt.Sprintf("anchr/v1/%s/+/+/cmd", cfg.TenantID)
		opts.OnConnect = func(c mqtt.Client) {
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
		if ok := token.WaitTimeout(DefaultConnectTimeout); !ok {
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
		log.Printf("WARN tx publish retry device=%s attempt=%d tx_seq=%d err=%v", pending.DeviceID, attempt, pending.TxSeq, err)
		if cfg.MaxTXRetries > 0 && attempt == cfg.MaxTXRetries {
			log.Printf("WARN tx publish reached configured retry threshold device=%s tx_seq=%d threshold=%d; continuing until confirmed to avoid sequence gaps", pending.DeviceID, pending.TxSeq, cfg.MaxTXRetries)
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
