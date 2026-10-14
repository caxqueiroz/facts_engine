package daemon

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"sync/atomic"
	"time"
)

// Serve owns the supplied listener until shutdown. The caller owns dependencies
// and closes them after Serve returns. Probe never returns sensitive diagnostics.
func Serve(ctx context.Context, listener net.Listener, tlsConfig *tls.Config, handler http.Handler, wrap func(http.Handler) http.Handler, probe func(context.Context) map[string]bool, timeout, grace time.Duration, logger *slog.Logger) error {
	return serve(ctx, listener, tlsConfig, handler, wrap, probe, nil, timeout, grace, logger)
}

// Optional component checks do not participate in core readiness.
func serve(ctx context.Context, listener net.Listener, tlsConfig *tls.Config, handler http.Handler, wrap func(http.Handler) http.Handler, probe, componentsProbe func(context.Context) map[string]bool, timeout, grace time.Duration, logger *slog.Logger) error {
	if listener == nil || tlsConfig == nil || tlsConfig.ClientAuth != tls.RequireAndVerifyClientCert || tlsConfig.ClientCAs == nil || handler == nil || wrap == nil || probe == nil || timeout <= 0 || grace <= 0 || logger == nil {
		return errors.New("invalid server configuration")
	}
	var draining atomic.Bool
	var checking atomic.Bool
	mux := http.NewServeMux()
	mux.Handle("/", handler)
	mux.HandleFunc("GET /livez", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = io.WriteString(w, "{\"alive\":true}\n")
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		if draining.Load() || !checking.CompareAndSwap(false, true) {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, "{\"ready\":false}\n")
			return
		}
		defer checking.Store(false)
		checkCtx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		components := probe(checkCtx)
		ready := len(components) > 0 && checkCtx.Err() == nil && !draining.Load()
		for _, ok := range components {
			ready = ready && ok
		}
		if !ready {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(struct {
			Ready      bool            `json:"ready"`
			Components map[string]bool `json:"components"`
		}{ready, components})
	})
	if componentsProbe != nil {
		var checkingComponents atomic.Bool
		mux.HandleFunc("GET /componentsz", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Cache-Control", "no-store")
			if draining.Load() || !checkingComponents.CompareAndSwap(false, true) {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = io.WriteString(w, "{\"healthy\":false}\n")
				return
			}
			defer checkingComponents.Store(false)
			checkCtx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
			defer cancel()
			components := componentsProbe(checkCtx)
			healthy := checkCtx.Err() == nil && !draining.Load()
			for _, ok := range components {
				healthy = healthy && ok
			}
			if !healthy {
				w.WriteHeader(http.StatusServiceUnavailable)
			}
			_ = json.NewEncoder(w).Encode(struct {
				Healthy    bool            `json:"healthy"`
				Components map[string]bool `json:"components"`
			}{healthy, components})
		})
	}
	server := &http.Server{Handler: wrap(mux), TLSConfig: tlsConfig.Clone(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: timeout + 5*time.Second, WriteTimeout: timeout + 5*time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 32 << 10, ErrorLog: log.New(io.Discard, "", 0)}
	done := make(chan error, 1)
	go func() { done <- server.ServeTLS(listener, "", "") }()
	logger.Info("service started")
	select {
	case err := <-done:
		_ = server.Close()
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return errors.New("HTTP service stopped unexpectedly")
	case <-ctx.Done():
		draining.Store(true)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), grace)
		err := server.Shutdown(shutdownCtx)
		cancel()
		if err != nil {
			_ = server.Close()
		}
		serveErr := <-done
		logger.Info("service stopped", "forced", err != nil)
		if err != nil {
			return errors.New("graceful shutdown deadline exceeded")
		}
		if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			return errors.New("HTTP service stopped unexpectedly")
		}
		return nil
	}
}
