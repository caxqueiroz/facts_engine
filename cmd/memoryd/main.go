// memoryd composes the ledger API with verified workload identity. Migrations,
// provisioning and workers remain explicit deployment operations.
package main

import (
	"context"
	"flag"
	"github.com/caxqueiroz/facts_engine/internal/daemon"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	configPath := flag.String("config", "", "path to service configuration JSON")
	flag.Parse()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if *configPath == "" || flag.NArg() != 0 {
		logger.Error("usage: memoryd -config PATH")
		os.Exit(2)
	}
	config, err := daemon.LoadConfig(*configPath)
	if err != nil {
		logger.Error("configuration rejected")
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := daemon.Run(ctx, config, logger); err != nil {
		logger.Error("service failed", "reason", err.Error())
		os.Exit(1)
	}
}
