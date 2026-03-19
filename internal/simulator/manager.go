package simulator

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

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
