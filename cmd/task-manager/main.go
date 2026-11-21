// Command task-manager runs the FlowAI task-manager service: intake and
// fan-out, FIFO task queues, claims, retries, cancels and the task archive.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pandops/flowai-task-manager/internal/broker"
	"github.com/pandops/flowai-task-manager/internal/claimtoken"
	"github.com/pandops/flowai-task-manager/internal/config"
	"github.com/pandops/flowai-task-manager/internal/health"
	"github.com/pandops/flowai-task-manager/internal/outbox"
	"github.com/pandops/flowai-task-manager/internal/pgstore"
	"github.com/pandops/flowai-task-manager/internal/service"
	"github.com/pandops/flowai-task-manager/internal/wire"
)

func main() {
	path := flag.String("config", "/etc/flowai/config.yaml", "configuration file")
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", wire.Service)
	if err := run(*path, log); err != nil {
		log.Error("task-manager stopped", "error", err)
		os.Exit(1)
	}
}

func run(path string, log *slog.Logger) error {
	cfg, err := config.Load(path)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	openCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	st, err := pgstore.Open(openCtx, cfg.DSN(), cfg.DB.Schema)
	if err != nil {
		return err
	}
	defer st.Close()
	if err := st.Migrate(openCtx); err != nil {
		return err
	}

	signer, err := claimtoken.NewSigner(cfg.ClaimToken.SigningKey)
	if err != nil {
		return err
	}
	codec, err := cfg.CursorCodec()
	if err != nil {
		return err
	}

	bus, err := broker.Dial(broker.DialConfig{URL: cfg.NATS.URL, CAFile: cfg.NATS.CAFile,
		CredentialsFile: cfg.NATS.CredentialsFile, InboxPrefix: cfg.NATS.InboxPrefix}, log)
	if err != nil {
		return err
	}
	defer bus.Close()

	wake := make(chan struct{}, 1)
	notify := func() {
		select {
		case wake <- struct{}{}:
		default:
		}
	}
	svc := service.New(service.Config{Store: st, Flows: timeoutRequester{bus, cfg.NATS.RequestTimeout}, Signer: signer,
		Cursors: codec, Log: log, Committed: notify})
	relay := &outbox.Relay{Store: st, Pub: bus}

	if err := bus.Serve(wire.RequestSubjects(), svc.HandleRequest); err != nil {
		return err
	}
	go bus.Consume(ctx, wire.StreamIntake, wire.IntakeDurable, svc.HandleIntake)
	go bus.Consume(ctx, wire.StreamEvent, wire.EventDurable, svc.HandleEvent)
	go loop(ctx, time.Second, wake, func() {
		if _, err := relay.Flush(ctx); err != nil && ctx.Err() == nil {
			log.Warn("outbox relay", "error", err)
		}
	})
	go loop(ctx, time.Second, nil, func() {
		n, err := svc.RunDue(ctx)
		if err != nil && ctx.Err() == nil {
			log.Warn("republication", "error", err)
		}
		if n > 0 {
			notify()
		}
	})

	srv := &http.Server{Addr: cfg.HTTP.Listen, ReadHeaderTimeout: 5 * time.Second,
		Handler: health.Handler(health.Ready(bus, st))}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	log.Info("task-manager started", "listen", cfg.HTTP.Listen)
	select {
	case <-ctx.Done():
	case err := <-errc:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	}
	sctx, scancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer scancel()
	return srv.Shutdown(sctx)
}

// loop runs fn every interval and whenever wake fires, until ctx ends.
func loop(ctx context.Context, interval time.Duration, wake <-chan struct{}, fn func()) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		fn()
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-wake:
		}
	}
}

// timeoutRequester bounds every outgoing request with the configured
// timeout.
type timeoutRequester struct {
	bus     *broker.Conn
	timeout time.Duration
}

func (r timeoutRequester) Request(ctx context.Context, subject string, header map[string]string, data []byte) (wire.Reply, error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	return r.bus.Request(ctx, subject, header, data)
}
