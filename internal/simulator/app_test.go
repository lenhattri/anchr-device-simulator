package simulator

import (
	"bytes"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func TestReplayPendingTransactionsLogsEachDeviceForSmallBacklog(t *testing.T) {
	cfg := Config{TXStateFile: filepath.Join(t.TempDir(), "tx-state.json"), TXPublishTimeout: time.Second}
	registry := NewTxRegistry()
	client := &fakeMQTTClient{}
	pool := &MQTTClientPool{clients: []mqtt.Client{client}}

	for i := 1; i <= replayPendingTxDetailThreshold; i++ {
		deviceID := fmt.Sprintf("st-0001:p-%04d", i)
		_, err := registry.ReservePending(cfg.TXStateFile, deviceID, func(nextSeq int64) (PendingTransaction, error) {
			return PendingTransaction{
				DeviceID:  deviceID,
				StationID: "st-0001",
				PumpID:    fmt.Sprintf("p-%04d", i),
				Topic:     fmt.Sprintf("anchr/v1/default/st-0001/p-%04d/tx", i),
				TxSeq:     nextSeq,
				MessageID: fmt.Sprintf("msg-%d", i),
				CreatedAt: "2026-03-20T20:25:37Z",
				Payload:   []byte(fmt.Sprintf(`{"tx_seq":%d}`, nextSeq)),
			}, nil
		})
		if err != nil {
			t.Fatalf("reserve pending tx %d: %v", i, err)
		}
	}

	var logBuffer bytes.Buffer
	previousWriter := log.Writer()
	previousFlags := log.Flags()
	log.SetOutput(&logBuffer)
	log.SetFlags(0)
	defer func() {
		log.SetOutput(previousWriter)
		log.SetFlags(previousFlags)
	}()

	if err := replayPendingTransactions(t.Context(), cfg, pool, registry, &SeqLogger{}); err != nil {
		t.Fatalf("replay pending transactions: %v", err)
	}
	if got := registry.PendingCount(); got != 0 {
		t.Fatalf("expected replay to clear pending transactions got %d", got)
	}

	output := logBuffer.String()
	if !strings.Contains(output, "INFO replaying pending transactions count=10 detailed_logging=true") {
		t.Fatalf("expected detailed replay start log, got %q", output)
	}
	if !strings.Contains(output, "INFO replayed pending tx device=st-0001:p-0001 tx_seq=1") {
		t.Fatalf("expected per-device replay log, got %q", output)
	}
	if strings.Contains(output, "INFO replayed pending tx progress") {
		t.Fatalf("did not expect progress replay log for small backlog, got %q", output)
	}
}

func TestReplayPendingTransactionsSummarizesLargeBacklog(t *testing.T) {
	cfg := Config{TXStateFile: filepath.Join(t.TempDir(), "tx-state.json"), TXPublishTimeout: time.Second}
	registry := NewTxRegistry()
	client := &fakeMQTTClient{}
	pool := &MQTTClientPool{clients: []mqtt.Client{client}}

	for i := 1; i <= replayPendingTxDetailThreshold+1; i++ {
		deviceID := fmt.Sprintf("st-0001:p-%04d", i)
		_, err := registry.ReservePending(cfg.TXStateFile, deviceID, func(nextSeq int64) (PendingTransaction, error) {
			return PendingTransaction{
				DeviceID:  deviceID,
				StationID: "st-0001",
				PumpID:    fmt.Sprintf("p-%04d", i),
				Topic:     fmt.Sprintf("anchr/v1/default/st-0001/p-%04d/tx", i),
				TxSeq:     nextSeq,
				MessageID: fmt.Sprintf("msg-%d", i),
				CreatedAt: "2026-03-20T20:25:37Z",
				Payload:   []byte(fmt.Sprintf(`{"tx_seq":%d}`, nextSeq)),
			}, nil
		})
		if err != nil {
			t.Fatalf("reserve pending tx %d: %v", i, err)
		}
	}

	var logBuffer bytes.Buffer
	previousWriter := log.Writer()
	previousFlags := log.Flags()
	log.SetOutput(&logBuffer)
	log.SetFlags(0)
	defer func() {
		log.SetOutput(previousWriter)
		log.SetFlags(previousFlags)
	}()

	if err := replayPendingTransactions(t.Context(), cfg, pool, registry, &SeqLogger{}); err != nil {
		t.Fatalf("replay pending transactions: %v", err)
	}
	if got := registry.PendingCount(); got != 0 {
		t.Fatalf("expected replay to clear pending transactions got %d", got)
	}

	output := logBuffer.String()
	if !strings.Contains(output, "INFO replaying pending transactions count=11 detailed_logging=false") {
		t.Fatalf("expected summarized replay start log, got %q", output)
	}
	if strings.Contains(output, "INFO replayed pending tx device=") {
		t.Fatalf("did not expect per-device replay logs for large backlog, got %q", output)
	}
	if !strings.Contains(output, "INFO replayed pending tx progress completed=11 total=11 last_device=st-0001:p-0011 last_tx_seq=1") {
		t.Fatalf("expected replay progress summary log, got %q", output)
	}
}
