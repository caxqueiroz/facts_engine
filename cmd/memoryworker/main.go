package main

import (
	"context"
	"flag"
	"github.com/caxqueiroz/facts_engine/internal/workerdaemon"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	path := flag.String("config", "", "path to worker configuration JSON")
	flag.Parse()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if *path == "" || flag.NArg() != 0 {
		logger.Error("usage: memoryworker -config PATH")
		os.Exit(2)
	}
	config, err := workerdaemon.LoadConfig(*path)
	if err != nil {
		logger.Error("worker configuration rejected")
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := workerdaemon.Run(ctx, config, logger); err != nil {
		logger.Error("worker failed", "reason", err.Error())
		os.Exit(1)
	}
}
