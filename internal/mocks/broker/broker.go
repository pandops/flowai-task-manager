// Package broker is the in-process fake of task-manager's broker sides for
// unit tests: the JetStream publisher of the outbox relay and the requester
// that calls flow-registry.
package broker

import (
	"context"
	"errors"
	"sync"

	"github.com/pandops/flowai-task-manager/internal/wire"
)

// ErrNoResponder is returned when no responder is set.
var ErrNoResponder = errors.New("fake broker: no responder")

// Fake records every publication and request.
type Fake struct {
	mu        sync.Mutex
	Published []wire.Msg
	Ensured   []string // "<team>/<tag>" in call order
	// Events is the ordered log of ensures and stored publications:
	// "ensure <consumer>" and "publish <subject>".
	Events   []string
	Requests []wire.Msg
	// Respond answers requests; nil answers 503.
	Respond func(wire.Msg) (wire.Reply, error)
	msgIDs  map[string]bool
}

// New returns a fake broker.
func New() *Fake { return &Fake{msgIDs: map[string]bool{}} }

// EnsureTaskConsumer implements outbox.Publisher.
func (f *Fake) EnsureTaskConsumer(_ context.Context, team int64, tag string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Ensured = append(f.Ensured, wire.TaskConsumer(team, tag))
	f.Events = append(f.Events, "ensure "+wire.TaskConsumer(team, tag))
	return nil
}

// Publish implements outbox.Publisher with Nats-Msg-Id deduplication.
func (f *Fake) Publish(_ context.Context, subject string, header map[string]string, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := header[wire.HeaderMsgID]
	if id != "" && f.msgIDs[id] {
		return nil
	}
	if id != "" {
		f.msgIDs[id] = true
	}
	h := map[string]string{}
	for k, v := range header {
		h[k] = v
	}
	f.Published = append(f.Published, wire.Msg{Subject: subject, Header: h, Data: append([]byte(nil), data...)})
	f.Events = append(f.Events, "publish "+subject)
	return nil
}

// Request implements service.Requester.
func (f *Fake) Request(_ context.Context, subject string, header map[string]string, data []byte) (wire.Reply, error) {
	f.mu.Lock()
	respond := f.Respond
	f.Requests = append(f.Requests, wire.Msg{Subject: subject, Header: header, Data: append([]byte(nil), data...)})
	f.mu.Unlock()
	if respond == nil {
		return wire.Reply{}, ErrNoResponder
	}
	return respond(wire.Msg{Subject: subject, Header: header, Data: data})
}

// Messages returns a copy of the published messages.
func (f *Fake) Messages() []wire.Msg {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]wire.Msg(nil), f.Published...)
}
