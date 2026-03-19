package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
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
		identity := deviceIdentityForIndex(tc.idx)
		if identity.StationID != tc.stationID || identity.PumpID != tc.pumpID {
			t.Fatalf("idx=%d got %s/%s", tc.idx, identity.StationID, identity.PumpID)
		}
	}
}

func TestTxRegistrySetAndGet(t *testing.T) {
	registry := NewTxRegistry()
	statePath := filepath.Join(t.TempDir(), "state.json")
	registry.EnsureDevice("st-0001:p-0001")
	pending, err := registry.ReservePending(statePath, "st-0001:p-0001", func(nextSeq int64) (PendingTransaction, error) {
		return PendingTransaction{
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
	registry := NewTxRegistry()
	_, err := registry.ReservePending(statePath, "st-0001:p-0002", func(nextSeq int64) (PendingTransaction, error) {
		return PendingTransaction{
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

	reloaded := NewTxRegistry()
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
