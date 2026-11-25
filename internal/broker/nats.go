// Package broker is task-manager's NATS JetStream adapter: core NATS
// requests and served subjects, JetStream publication, task-consumer
// creation and the two durable consumers the deployment creates.
package broker

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/pandops/flowai-task-manager/internal/wire"
)

// ErrDisconnected is returned while the connection is down.
var ErrDisconnected = errors.New("broker: disconnected")

// queueGroup spreads requests over the replicas of the service.
const queueGroup = "task-manager"

// DialConfig is the broker connection: a tls:// URL, the stand CA, the user
// JWT credentials file and the inbox prefix.
type DialConfig struct {
	URL             string
	CAFile          string
	CredentialsFile string
	InboxPrefix     string
}

// Conn is one broker connection.
type Conn struct {
	nc  *nats.Conn
	js  jetstream.JetStream
	log *slog.Logger

	mu    sync.Mutex
	subs  []*nats.Subscription
	ctxs  []jetstream.ConsumeContext
	bound map[string]bool
}

// Dial validates the trust configuration and starts a connection that keeps
// retrying in the background. It never connects anonymously or without TLS.
func Dial(c DialConfig, log *slog.Logger) (*Conn, error) {
	if !strings.HasPrefix(c.URL, "tls://") {
		return nil, errors.New("broker: url must use tls://")
	}
	pem, err := os.ReadFile(c.CAFile)
	if c.CAFile == "" || err != nil || !x509.NewCertPool().AppendCertsFromPEM(pem) {
		return nil, errors.New("broker: a readable PEM CA file is required")
	}
	creds, err := os.ReadFile(c.CredentialsFile)
	if c.CredentialsFile == "" || err != nil || !strings.Contains(string(creds), "BEGIN NATS USER JWT") || !strings.Contains(string(creds), "BEGIN USER NKEY SEED") {
		return nil, errors.New("broker: a user credentials file with JWT and seed is required")
	}
	if !strings.HasPrefix(c.InboxPrefix, "_INBOX.") {
		return nil, errors.New("broker: inbox prefix must start with _INBOX")
	}
	nc, err := nats.Connect(c.URL,
		nats.Name(wire.Service),
		nats.RootCAs(c.CAFile),
		nats.UserCredentials(c.CredentialsFile),
		nats.CustomInboxPrefix(c.InboxPrefix),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("broker: connect: %w", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("broker: jetstream: %w", err)
	}
	return &Conn{nc: nc, js: js, log: log, bound: map[string]bool{}}, nil
}

func toHeader(h map[string]string) nats.Header {
	out := nats.Header{}
	for k, v := range h {
		out[k] = []string{v}
	}
	return out
}

func fromHeader(h nats.Header) map[string]string {
	out := make(map[string]string, len(h))
	for k, v := range h {
		if len(v) > 0 {
			out[k] = v[0]
		}
	}
	return out
}

// Ready reports an error while the connection is down or a durable
// consumer is not bound.
func (c *Conn) Ready() error {
	if !c.nc.IsConnected() {
		return ErrDisconnected
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, d := range []string{wire.IntakeDurable, wire.EventDurable} {
		if !c.bound[d] {
			return fmt.Errorf("broker: durable consumer %s is not bound", d)
		}
	}
	return nil
}

// Request sends one core NATS request and waits for the reply.
func (c *Conn) Request(ctx context.Context, subject string, header map[string]string, data []byte) (wire.Reply, error) {
	if !c.nc.IsConnected() {
		return wire.Reply{}, ErrDisconnected
	}
	r, err := c.nc.RequestMsgWithContext(ctx, &nats.Msg{Subject: subject, Header: toHeader(header), Data: data})
	if err != nil {
		return wire.Reply{}, fmt.Errorf("broker: request %s: %w", subject, err)
	}
	h := fromHeader(r.Header)
	status, err := strconv.Atoi(h[wire.HeaderStatus])
	if err != nil {
		return wire.Reply{}, fmt.Errorf("broker: reply of %s has no status", subject)
	}
	return wire.Reply{Status: status, Header: h, Body: r.Data}, nil
}

// Publish sends one JetStream message and waits for its acknowledgement.
func (c *Conn) Publish(ctx context.Context, subject string, header map[string]string, data []byte) error {
	if !c.nc.IsConnected() {
		return ErrDisconnected
	}
	_, err := c.js.PublishMsg(ctx, &nats.Msg{Subject: subject, Header: toHeader(header), Data: data})
	return err
}

// EnsureTaskConsumer idempotently creates tasks-<team>-<tag_hash> with the
// configuration executor-registry uses.
func (c *Conn) EnsureTaskConsumer(ctx context.Context, team int64, tag string) error {
	_, err := c.js.CreateConsumer(ctx, wire.StreamTask, jetstream.ConsumerConfig{
		Durable:       wire.TaskConsumer(team, tag),
		FilterSubject: wire.TaskFilter(team, tag),
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckWait:       30 * time.Second,
		MaxDeliver:    -1,
	})
	if err == nil || errors.Is(err, jetstream.ErrConsumerExists) {
		return nil
	}
	return err
}

// Serve answers requests on subjects in the service's queue group, each on
// its own goroutine.
func (c *Conn) Serve(subjects []string, h func(context.Context, wire.Msg) wire.Reply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, s := range subjects {
		sub, err := c.nc.QueueSubscribe(s, queueGroup, func(m *nats.Msg) {
			go func() {
				r := h(context.Background(), wire.Msg{Subject: m.Subject, Header: fromHeader(m.Header), Data: m.Data})
				if err := m.RespondMsg(&nats.Msg{Header: toHeader(r.Header), Data: r.Body}); err != nil {
					c.log.Warn("reply failed", "subject", m.Subject, "error", err)
				}
			}()
		})
		if err != nil {
			return fmt.Errorf("broker: subscribe %s: %w", s, err)
		}
		c.subs = append(c.subs, sub)
	}
	return nil
}

// Consume binds a durable consumer the deployment created and handles its
// messages one at a time until ctx ends. It retries binding while the
// consumer is missing.
func (c *Conn) Consume(ctx context.Context, stream, durable string, h func(context.Context, wire.Msg) wire.Ack) {
	for {
		cons, err := c.js.Consumer(ctx, stream, durable)
		if err == nil {
			cc, err := cons.Consume(func(m jetstream.Msg) { c.handle(ctx, m, h) })
			if err == nil {
				c.mu.Lock()
				c.ctxs = append(c.ctxs, cc)
				c.bound[durable] = true
				c.mu.Unlock()
				return
			}
		}
		c.log.Warn("durable consumer not bound yet", "stream", stream, "durable", durable, "error", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}

func (c *Conn) handle(ctx context.Context, m jetstream.Msg, h func(context.Context, wire.Msg) wire.Ack) {
	ack := h(ctx, wire.Msg{Subject: m.Subject(), Header: fromHeader(m.Headers()), Data: m.Data()})
	var err error
	switch ack.Kind {
	case wire.AckOK:
		err = m.Ack()
	case wire.AckTerm:
		err = m.Term()
	default:
		if ack.Delay > 0 {
			err = m.NakWithDelay(ack.Delay)
		} else {
			err = m.Nak()
		}
	}
	if err != nil {
		c.log.Warn("acknowledgement failed", "subject", m.Subject(), "error", err)
	}
}

// Close stops every subscription and closes the connection.
func (c *Conn) Close() {
	c.mu.Lock()
	for _, s := range c.subs {
		_ = s.Unsubscribe()
	}
	for _, cc := range c.ctxs {
		cc.Stop()
	}
	c.subs, c.ctxs = nil, nil
	c.mu.Unlock()
	c.nc.Close()
}
