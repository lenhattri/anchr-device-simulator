package simulator

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

func LoadConfigFromArgs(args []string) (Config, error) {
	fs := flag.NewFlagSet("anchr-device-simulator", flag.ContinueOnError)
	fs.SetOutput(ioDiscard{})
	var configPath string
	fs.StringVar(&configPath, "config", "config.json", "path to config file (.json or .yaml)")
	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}

	cfg := Config{
		MQTTHost:           "localhost",
		MQTTPort:           1883,
		MQTTTLS:            false,
		TenantID:           "default",
		Speed:              10,
		InitialPumps:       1500,
		MinPumps:           1000,
		MaxPumps:           5000,
		StepSize:           1000,
		ScaleInterval:      1800 * time.Second,
		PoolSize:           50,
		HTTPAddr:           DefaultHTTPAddr,
		TXStateFile:        getEnv("TX_STATE_FILE", DefaultTXStateFile),
		TXPublishTimeout:   DefaultPublishTimeout,
		MaxTXRetries:       0,
		PersistInterval:    DefaultPersistInterval,
		ShutdownTimeout:    DefaultShutdownTimeout,
		SeqLogFile:         os.Getenv("SEQ_LOG_FILE"),
		MQTTUsername:       os.Getenv("MQTT_USERNAME"),
		MQTTPassword:       os.Getenv("MQTT_PASSWORD"),
		MQTTClientIDPrefix: getEnv("MQTT_CLIENT_ID_PREFIX", "sim-pool"),
		DebugTXTiming:      getEnvBool("DEBUG_TX_TIMING", false),
	}

	if configPath != "" {
		if raw, err := os.ReadFile(configPath); err == nil {
			var rc RawConfig
			switch strings.ToLower(filepath.Ext(configPath)) {
			case ".json":
				if err := json.Unmarshal(raw, &rc); err != nil {
					return Config{}, fmt.Errorf("parse config json: %w", err)
				}
			default:
				if err := yaml.Unmarshal(raw, &rc); err != nil {
					return Config{}, fmt.Errorf("parse config yaml: %w", err)
				}
			}
			applyRawConfig(&cfg, rc)
		} else if !errors.Is(err, os.ErrNotExist) {
			return Config{}, err
		}
	}

	cfg.MQTTHost = getEnv("MQTT_HOST", cfg.MQTTHost)
	cfg.MQTTPort = getEnvInt("MQTT_PORT", cfg.MQTTPort)
	cfg.MQTTTLS = getEnvBool("MQTT_TLS", cfg.MQTTTLS)
	cfg.TenantID = getEnv("TENANT_ID", cfg.TenantID)
	cfg.Speed = maxInt(1, getEnvInt("SPEED", cfg.Speed))
	cfg.InitialPumps = getEnvInt("INITIAL_PUMPS", cfg.InitialPumps)
	cfg.MinPumps = getEnvInt("MIN_PUMPS", cfg.MinPumps)
	cfg.MaxPumps = getEnvInt("MAX_PUMPS", cfg.MaxPumps)
	cfg.StepSize = getEnvInt("STEP_SIZE", cfg.StepSize)
	cfg.ScaleInterval = time.Duration(getEnvInt("SCALE_INTERVAL", int(cfg.ScaleInterval.Seconds()))) * time.Second
	cfg.PoolSize = maxInt(1, getEnvInt("POOL_SIZE", cfg.PoolSize))
	cfg.HTTPAddr = getEnv("HTTP_ADDR", cfg.HTTPAddr)
	cfg.TXStateFile = getEnv("TX_STATE_FILE", cfg.TXStateFile)
	cfg.TXPublishTimeout = time.Duration(getEnvInt("TX_PUBLISH_TIMEOUT", int(cfg.TXPublishTimeout.Seconds()))) * time.Second
	cfg.MaxTXRetries = getEnvInt("MAX_TX_RETRIES", cfg.MaxTXRetries)
	cfg.PersistInterval = time.Duration(getEnvInt("PERSIST_INTERVAL", int(cfg.PersistInterval.Seconds()))) * time.Second
	cfg.ShutdownTimeout = time.Duration(getEnvInt("GRACEFUL_SHUTDOWN_TIMEOUT", int(cfg.ShutdownTimeout.Seconds()))) * time.Second
	cfg.MQTTUsername = getEnv("MQTT_USERNAME", cfg.MQTTUsername)
	cfg.MQTTPassword = getEnv("MQTT_PASSWORD", cfg.MQTTPassword)
	cfg.MQTTClientIDPrefix = getEnv("MQTT_CLIENT_ID_PREFIX", cfg.MQTTClientIDPrefix)
	cfg.DebugTXTiming = getEnvBool("DEBUG_TX_TIMING", cfg.DebugTXTiming)

	if cfg.MinPumps < 0 || cfg.MaxPumps < cfg.MinPumps || cfg.InitialPumps < cfg.MinPumps || cfg.InitialPumps > cfg.MaxPumps {
		return Config{}, fmt.Errorf("invalid pump bounds min=%d initial=%d max=%d", cfg.MinPumps, cfg.InitialPumps, cfg.MaxPumps)
	}
	return cfg, nil
}

func applyRawConfig(cfg *Config, rc RawConfig) {
	if rc.MQTT.Host != "" {
		cfg.MQTTHost = rc.MQTT.Host
	}
	if rc.MQTT.Port != 0 {
		cfg.MQTTPort = rc.MQTT.Port
	}
	cfg.MQTTTLS = rc.MQTT.TLS
	if rc.MQTT.Username != "" {
		cfg.MQTTUsername = rc.MQTT.Username
	}
	if rc.MQTT.Password != "" {
		cfg.MQTTPassword = rc.MQTT.Password
	}
	if rc.Simulation.TenantID != "" {
		cfg.TenantID = rc.Simulation.TenantID
	}
	if rc.Simulation.Speed != 0 {
		cfg.Speed = rc.Simulation.Speed
	}
	if rc.Simulation.InitialPumps != 0 {
		cfg.InitialPumps = rc.Simulation.InitialPumps
	}
	if rc.Simulation.MinPumps != 0 {
		cfg.MinPumps = rc.Simulation.MinPumps
	}
	if rc.Simulation.MaxPumps != 0 {
		cfg.MaxPumps = rc.Simulation.MaxPumps
	}
	if rc.Simulation.StepSize != 0 {
		cfg.StepSize = rc.Simulation.StepSize
	}
	if rc.Simulation.ScaleIntervalSec != 0 {
		cfg.ScaleInterval = time.Duration(rc.Simulation.ScaleIntervalSec) * time.Second
	}
	if rc.Pool.Size != 0 {
		cfg.PoolSize = rc.Pool.Size
	}
	if rc.HTTP.Addr != "" {
		cfg.HTTPAddr = rc.HTTP.Addr
	}
	if rc.Persistence.TXStateFile != "" {
		cfg.TXStateFile = rc.Persistence.TXStateFile
	}
	if rc.Persistence.PersistIntervalSec != 0 {
		cfg.PersistInterval = time.Duration(rc.Persistence.PersistIntervalSec) * time.Second
	}
	if rc.Persistence.ShutdownTimeoutSec != 0 {
		cfg.ShutdownTimeout = time.Duration(rc.Persistence.ShutdownTimeoutSec) * time.Second
	}
	if rc.Transaction.PublishTimeoutSec != 0 {
		cfg.TXPublishTimeout = time.Duration(rc.Transaction.PublishTimeoutSec) * time.Second
	}
	if rc.Transaction.MaxRetries != 0 {
		cfg.MaxTXRetries = rc.Transaction.MaxRetries
	}
}

type ioDiscard struct{}

func (ioDiscard) Write(p []byte) (int, error) { return len(p), nil }
