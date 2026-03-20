package simulator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type App struct {
	cfg        Config
	registry   *TxRegistry
	seqLogger  *SeqLogger
	pool       *MQTTClientPool
	manager    *ScaleManager
	httpServer *http.Server
}

func NewApp(ctx context.Context, cfg Config) (*App, error) {
	app := &App{cfg: cfg, registry: NewTxRegistry()}

	if err := app.registry.Load(cfg.TXStateFile); err != nil {
		log.Printf("WARN failed to load tx state file=%s err=%v", cfg.TXStateFile, err)
	} else {
		log.Printf("INFO loaded tx state devices=%d pending=%d file=%s", app.registry.Len(), app.registry.PendingCount(), cfg.TXStateFile)
	}

	seqLogger, err := NewSeqLogger(cfg.SeqLogFile)
	if err != nil {
		return nil, fmt.Errorf("open seq log: %w", err)
	}
	app.seqLogger = seqLogger

	var manager *ScaleManager
	pool, err := NewMQTTClientPool(cfg, func(_ mqtt.Client, msg mqtt.Message) {
		if manager == nil {
			return
		}
		pump, ok := manager.PumpByTopic(msg.Topic())
		if !ok {
			return
		}
		var envelope map[string]any
		if err := json.Unmarshal(msg.Payload(), &envelope); err != nil {
			log.Printf("WARN invalid command payload topic=%s err=%v", msg.Topic(), err)
			return
		}
		pump.handleCommand(envelope)
	})
	if err != nil {
		_ = seqLogger.Close()
		return nil, fmt.Errorf("create mqtt pool: %w", err)
	}
	app.pool = pool

	if err := replayPendingTransactions(ctx, cfg, pool, app.registry, seqLogger); err != nil {
		app.Close()
		return nil, fmt.Errorf("replay pending transactions: %w", err)
	}

	manager = NewScaleManager(cfg, pool, app.registry, seqLogger)
	app.manager = manager
	app.httpServer = &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: NewHTTPAPI(ctx, manager).Routes(),
	}
	return app, nil
}

func (a *App) Run(ctx context.Context) error {
	errCh := make(chan error, 2)

	go func() {
		log.Printf("INFO http api listening addr=%s", a.cfg.HTTPAddr)
		if err := a.httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("http server: %w", err)
		}
	}()

	go func() {
		if err := a.manager.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- fmt.Errorf("manager: %w", err)
		}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

func (a *App) Shutdown(ctx context.Context) {
	if err := a.httpServer.Shutdown(ctx); err != nil {
		log.Printf("WARN http shutdown failed err=%v", err)
	}
	if err := a.manager.Shutdown(ctx); err != nil {
		log.Printf("WARN manager shutdown failed err=%v", err)
	}
	if err := a.registry.Save(a.cfg.TXStateFile); err != nil {
		log.Printf("WARN final tx state persist failed err=%v", err)
	}
}

func (a *App) Close() {
	if a.pool != nil {
		a.pool.Close()
	}
	if a.seqLogger != nil {
		if err := a.seqLogger.Close(); err != nil {
			log.Printf("WARN closing seq log failed err=%v", err)
		}
	}
}

const (
	replayPendingTxDetailThreshold = 10
	replayPendingTxProgressEvery   = 100
)

func replayPendingTransactions(ctx context.Context, cfg Config, pool *MQTTClientPool, registry *TxRegistry, seqLogger *SeqLogger) error {
	pendingTransactions := registry.PendingTransactions()
	if len(pendingTransactions) == 0 {
		return nil
	}

	detailedLogging := len(pendingTransactions) <= replayPendingTxDetailThreshold
	log.Printf("INFO replaying pending transactions count=%d detailed_logging=%t", len(pendingTransactions), detailedLogging)
	for idx, pending := range pendingTransactions {
		if err := publishPendingTransaction(ctx, pool.GetByKey(pending.DeviceID), cfg, pending); err != nil {
			return err
		}
		if err := registry.ConfirmPending(cfg.TXStateFile, pending.DeviceID, pending.TxSeq); err != nil {
			return err
		}
		seqLogger.Write(map[string]any{
			"device_id":  pending.DeviceID,
			"type":       "tx-replay",
			"message_id": pending.MessageID,
			"tx_seq":     pending.TxSeq,
			"ts":         pending.CreatedAt,
		})

		completed := idx + 1
		if detailedLogging {
			log.Printf("INFO replayed pending tx device=%s tx_seq=%d", pending.DeviceID, pending.TxSeq)
			continue
		}
		if completed%replayPendingTxProgressEvery == 0 || completed == len(pendingTransactions) {
			log.Printf("INFO replayed pending tx progress completed=%d total=%d last_device=%s last_tx_seq=%d", completed, len(pendingTransactions), pending.DeviceID, pending.TxSeq)
		}
	}
	return nil
}
