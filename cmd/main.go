package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/shreyas/dronc/api/routes"
	"github.com/shreyas/dronc/lib/httpserver"
	"github.com/shreyas/dronc/lib/logger"
	redisClient "github.com/shreyas/dronc/lib/redis"
	"github.com/shreyas/dronc/scheduler"
)

var (
	ctx    context.Context
	cancel context.CancelFunc
)

func init() {
	// Initialize logger
	if err := logger.Initialize(); err != nil {
		panic("failed to initialize logger: " + err.Error())
	}

	// Initialize context
	ctx, cancel = context.WithCancel(context.Background())

	// Initialize Redis client
	if err := redisClient.Initialize(ctx); err != nil {
		logger.Fatal("failed to initialize Redis client", "error", err)
	}
	logger.Info("successfully connected to Redis")
}

func main() {
	defer func() { _ = logger.Sync() }()
	defer cancel()
	defer func() { _ = redisClient.Close() }()

	// Create JobsManager and start the processing goroutines
	jobsManager := scheduler.NewJobsManager(nil, nil)
	jobsManager.Run(ctx)
	logger.Info("jobs manager started")

	// Create HTTP server for API routes
	server := httpserver.New(routes.Setup())

	// Start server in a goroutine
	go func() {
		logger.Info("starting dronc server", "addr", server.Addr())
		if err := server.Start(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatal("server failed to start", "error", err)
		}
	}()

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	logger.Info("shutdown signal received, gracefully shutting down")

	// Cancel context to signal all components (including due jobs finder)
	cancel()

	// Graceful shutdown with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("server shutdown error", "error", err)
	}

	logger.Info("server stopped")
}
