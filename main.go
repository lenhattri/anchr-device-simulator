package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"

	"anchr-device-simulator/internal/simulator"
)

func main() {
	cfg, err := simulator.LoadConfigFromArgs(os.Args[1:])
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	runCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	app, err := simulator.NewApp(runCtx, cfg)
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("failed to initialize simulator: %v", err)
	}
	if app == nil {
		return
	}
	defer app.Close()

	if err := app.Run(runCtx); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("WARN simulator runtime exited err=%v", err)
		stop()
	}

	log.Printf("INFO shutdown requested")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	app.Shutdown(shutdownCtx)
	log.Printf("INFO simulator shut down cleanly")
}
