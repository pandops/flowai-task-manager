// Package health serves the probes on the one HTTP listener: /v1/livez and
// /v1/readyz. Every other path answers 404; task-manager has no other HTTP
// endpoint.
package health

import (
	"context"
	"net/http"
	"time"
)

// Handler returns the probe handler; ready reports readiness.
func Handler(ready func() error) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/livez", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("GET /v1/readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := ready(); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"status":"unavailable"}`))
			return
		}
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	return mux
}

// Broker is the broker side of readiness: the connection is up and every
// durable consumer is bound.
type Broker interface{ Ready() error }

// Database is the store side of readiness.
type Database interface {
	Ping(ctx context.Context) error
}

// Ready is task-manager's readiness: the broker first, then a database ping
// bounded by two seconds.
func Ready(bus Broker, db Database) func() error {
	return func() error {
		if err := bus.Ready(); err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		return db.Ping(ctx)
	}
}
