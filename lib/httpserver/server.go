package httpserver

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/shreyas/dronc/lib/env"
)

type Server struct {
	httpServer *http.Server
}

// New creates a new HTTP server instance
func New(handler http.Handler) *Server {
	port := env.HTTPPort()

	return &Server{
		httpServer: &http.Server{
			Addr:           fmt.Sprintf(":%s", port),
			Handler:        handler,
			ReadTimeout:    15 * time.Second,
			WriteTimeout:   15 * time.Second,
			IdleTimeout:    60 * time.Second,
			MaxHeaderBytes: 1 << 20, // 1 MB
		},
	}
}

// Start starts the HTTP server
func (s *Server) Start() error {
	return s.httpServer.ListenAndServe()
}

// Shutdown gracefully shuts down the server
func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

// Addr returns the server address
func (s *Server) Addr() string {
	return s.httpServer.Addr
}
