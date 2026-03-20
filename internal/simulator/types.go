package simulator

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

const (
	NumStations               = 10
	PumpsPerStation           = 3000
	DefaultTXStateFile        = "/tmp/sim-tx-state.json"
	DefaultHTTPAddr           = ":8080"
	DefaultPersistInterval    = 30 * time.Second
	DefaultConnectTimeout     = 10 * time.Second
	DefaultPublishTimeout     = 10 * time.Second
	DefaultShutdownTimeout    = 60 * time.Second
	DefaultSessionStartJitter = 10 * time.Second
)

type Config struct {
	MQTTHost           string
	MQTTPort           int
	MQTTTLS            bool
	TenantID           string
	Speed              int
	InitialPumps       int
	MinPumps           int
	MaxPumps           int
	StepSize           int
	ScaleInterval      time.Duration
	PoolSize           int
	HTTPAddr           string
	TXStateFile        string
	TXPublishTimeout   time.Duration
	MaxTXRetries       int
	PersistInterval    time.Duration
	ShutdownTimeout    time.Duration
	SeqLogFile         string
	MQTTUsername       string
	MQTTPassword       string
	MQTTClientIDPrefix string
	DebugTXTiming      bool
}

type RawConfig struct {
	MQTT struct {
		Host     string `yaml:"host"`
		Port     int    `yaml:"port"`
		TLS      bool   `yaml:"tls"`
		Username string `yaml:"username"`
		Password string `yaml:"password"`
	} `yaml:"mqtt"`
	Simulation struct {
		TenantID         string `yaml:"tenant_id"`
		Speed            int    `yaml:"speed"`
		InitialPumps     int    `yaml:"initial_pumps"`
		MinPumps         int    `yaml:"min_pumps"`
		MaxPumps         int    `yaml:"max_pumps"`
		StepSize         int    `yaml:"step_size"`
		ScaleIntervalSec int    `yaml:"scale_interval"`
	} `yaml:"simulation"`
	Pool struct {
		Size int `yaml:"size"`
	} `yaml:"pool"`
	HTTP struct {
		Addr string `yaml:"addr"`
	} `yaml:"http"`
	Persistence struct {
		TXStateFile        string `yaml:"tx_state_file"`
		PersistIntervalSec int    `yaml:"persist_interval"`
		ShutdownTimeoutSec int    `yaml:"shutdown_timeout"`
	} `yaml:"persistence"`
	Transaction struct {
		PublishTimeoutSec int `yaml:"publish_timeout"`
		MaxRetries        int `yaml:"max_retries"`
	} `yaml:"transaction"`
}

type PumpState int

const (
	StateHook  PumpState = 0
	StateLift  PumpState = 1
	StatePump  PumpState = 2
	StateError PumpState = 9
)

func (s PumpState) String() string {
	switch s {
	case StateHook:
		return "HOOK"
	case StateLift:
		return "LIFT"
	case StatePump:
		return "PUMP"
	default:
		return "ERROR"
	}
}

type DeviceIdentity struct {
	StationID string
	PumpID    string
	DeviceID  string
}

func DeviceIdentityForIndex(idx int) DeviceIdentity {
	stationIdx := idx / PumpsPerStation
	stationNum := (stationIdx % NumStations) + 1
	pumpNum := (idx % PumpsPerStation) + 1
	stationID := fmt.Sprintf("st-%04d", stationNum)
	pumpID := fmt.Sprintf("p-%04d", pumpNum)
	return DeviceIdentity{
		StationID: stationID,
		PumpID:    pumpID,
		DeviceID:  stationID + ":" + pumpID,
	}
}

type DeviceTXState struct {
	LastIssuedSeq int64 `json:"last_issued_seq"`
	LastAckedSeq  int64 `json:"last_acked_seq"`
}

type PendingTransaction struct {
	DeviceID  string          `json:"device_id"`
	StationID string          `json:"station_id"`
	PumpID    string          `json:"pump_id"`
	Topic     string          `json:"topic"`
	TxSeq     int64           `json:"tx_seq"`
	MessageID string          `json:"message_id"`
	CreatedAt string          `json:"created_at"`
	Payload   json.RawMessage `json:"payload"`
}

type persistedTXState struct {
	Devices map[string]DeviceTXState      `json:"devices"`
	Pending map[string]PendingTransaction `json:"pending"`
}

type TxRegistry struct {
	mu               sync.RWMutex
	persistMu        sync.Mutex
	devices          map[string]DeviceTXState
	pending          map[string]PendingTransaction
	snapshotVersion  uint64
	persistedVersion uint64
}
