package simulator

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

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
		r.devices[deviceID] = DeviceTXState{LastIssuedSeq: seq, LastAckedSeq: seq}
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
	return persistedTXState{Devices: devices, Pending: pending}
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
