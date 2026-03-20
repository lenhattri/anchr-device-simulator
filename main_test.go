package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"anchr-device-simulator/internal/simulator"
)

func TestDeviceIdentityForIndex(t *testing.T) {
	cases := []struct {
		idx       int
		stationID string
		pumpID    string
	}{
		{0, "st-0001", "p-0001"},
		{2999, "st-0001", "p-3000"},
		{3000, "st-0002", "p-0001"},
		{30000, "st-0001", "p-0001"},
	}
	for _, tc := range cases {
		identity := simulator.DeviceIdentityForIndex(tc.idx)
		if identity.StationID != tc.stationID || identity.PumpID != tc.pumpID {
			t.Fatalf("idx=%d got %s/%s", tc.idx, identity.StationID, identity.PumpID)
		}
	}
}

func TestTxRegistrySetAndGet(t *testing.T) {
	registry := simulator.NewTxRegistry()
	statePath := filepath.Join(t.TempDir(), "state.json")
	registry.EnsureDevice("st-0001:p-0001")
	pending, err := registry.ReservePending(statePath, "st-0001:p-0001", func(nextSeq int64) (simulator.PendingTransaction, error) {
		return simulator.PendingTransaction{
			DeviceID:  "st-0001:p-0001",
			StationID: "st-0001",
			PumpID:    "p-0001",
			Topic:     "anchr/v1/default/st-0001/p-0001/tx",
			TxSeq:     nextSeq,
			MessageID: fmt.Sprintf("msg-%d", nextSeq),
			Payload:   []byte(`{"tx_seq":1}`),
		}, nil
	})
	if err != nil {
		t.Fatalf("reserve pending tx: %v", err)
	}
	if pending.TxSeq != 1 {
		t.Fatalf("expected tx_seq 1 got %d", pending.TxSeq)
	}
	if err := registry.ConfirmPending(statePath, "st-0001:p-0001", pending.TxSeq); err != nil {
		t.Fatalf("confirm pending tx: %v", err)
	}
	if got := registry.LastIssued("st-0001:p-0001"); got != 1 {
		t.Fatalf("expected last issued 1 got %d", got)
	}
}

func TestTxRegistryReloadsPendingState(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "state.json")
	registry := simulator.NewTxRegistry()
	_, err := registry.ReservePending(statePath, "st-0001:p-0002", func(nextSeq int64) (simulator.PendingTransaction, error) {
		return simulator.PendingTransaction{
			DeviceID:  "st-0001:p-0002",
			StationID: "st-0001",
			PumpID:    "p-0002",
			Topic:     "anchr/v1/default/st-0001/p-0002/tx",
			TxSeq:     nextSeq,
			MessageID: "msg-persisted",
			Payload:   []byte(`{"tx_seq":1}`),
		}, nil
	})
	if err != nil {
		t.Fatalf("reserve pending tx: %v", err)
	}

	reloaded := simulator.NewTxRegistry()
	if err := reloaded.Load(statePath); err != nil {
		t.Fatalf("load persisted tx state: %v", err)
	}
	if got := reloaded.PendingCount(); got != 1 {
		t.Fatalf("expected 1 pending tx got %d", got)
	}
	if got := reloaded.LastIssued("st-0001:p-0002"); got != 1 {
		t.Fatalf("expected last issued 1 got %d", got)
	}
	if _, err := os.Stat(statePath); err != nil {
		t.Fatalf("expected persisted state file: %v", err)
	}
}

func TestLoadConfigUsesDefaultTXPublishTimeout(t *testing.T) {
	cfg, err := simulator.LoadConfigFromArgs([]string{"-config", filepath.Join(t.TempDir(), "missing.json")})
	if err != nil {
		t.Fatalf("load config defaults: %v", err)
	}
	if cfg.TXPublishTimeout != 10*time.Second {
		t.Fatalf("expected default tx publish timeout 10s got %s", cfg.TXPublishTimeout)
	}
}

func TestLoadConfigEnablesDebugTXTimingFromEnv(t *testing.T) {
	if err := os.Setenv("DEBUG_TX_TIMING", "true"); err != nil {
		t.Fatalf("set DEBUG_TX_TIMING: %v", err)
	}
	defer os.Unsetenv("DEBUG_TX_TIMING")

	cfg, err := simulator.LoadConfigFromArgs([]string{"-config", filepath.Join(t.TempDir(), "missing.json")})
	if err != nil {
		t.Fatalf("load config defaults: %v", err)
	}
	if !cfg.DebugTXTiming {
		t.Fatal("expected debug tx timing to be enabled from env")
	}
}

func TestLoadConfigSupportsJSON(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.json")
	raw := simulator.RawConfig{}
	raw.MQTT.Host = "broker"
	raw.MQTT.Port = 2883
	raw.Simulation.TenantID = "tenant-a"
	raw.Simulation.InitialPumps = 1200
	raw.Simulation.MinPumps = 1000
	raw.Simulation.MaxPumps = 2000
	raw.Pool.Size = 25
	raw.HTTP.Addr = ":9090"
	raw.Persistence.TXStateFile = "/data/state.json"

	payload, err := json.Marshal(raw)
	if err != nil {
		t.Fatalf("marshal json config: %v", err)
	}
	if err := os.WriteFile(configPath, payload, 0o644); err != nil {
		t.Fatalf("write json config: %v", err)
	}

	cfg, err := simulator.LoadConfigFromArgs([]string{"-config", configPath})
	if err != nil {
		t.Fatalf("load json config: %v", err)
	}
	if cfg.MQTTHost != "broker" || cfg.MQTTPort != 2883 {
		t.Fatalf("unexpected mqtt config: %+v", cfg)
	}
	if cfg.TenantID != "tenant-a" || cfg.InitialPumps != 1200 || cfg.MaxPumps != 2000 {
		t.Fatalf("unexpected simulation config: %+v", cfg)
	}
	if cfg.PoolSize != 25 || cfg.HTTPAddr != ":9090" || cfg.TXStateFile != "/data/state.json" {
		t.Fatalf("unexpected runtime config: %+v", cfg)
	}
}
