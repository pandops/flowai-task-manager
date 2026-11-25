// Package outbox publishes committed outbox rows in publisher_seq order and
// marks each row sent only after JetStream confirms it. A failed publish
// stops the pass; the next pass retries the same row with the same
// publisher_seq and Nats-Msg-Id.
package outbox

import (
	"context"
	"sync"

	"github.com/pandops/flowai-task-manager/internal/store"
)

// Publisher is the broker side of the relay.
type Publisher interface {
	// EnsureTaskConsumer idempotently ensures tasks-<team>-<tag_hash>.
	EnsureTaskConsumer(ctx context.Context, teamID int64, tag string) error
	// Publish sends one JetStream message and waits for its acknowledgement.
	Publish(ctx context.Context, subject string, header map[string]string, data []byte) error
}

// Relay moves outbox rows to the broker.
type Relay struct {
	Store store.Store
	Pub   Publisher
	Batch int

	mu sync.Mutex
}

// Flush publishes every pending row in order and returns how many it sent.
func (r *Relay) Flush(ctx context.Context) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	batch := r.Batch
	if batch <= 0 {
		batch = 100
	}
	sent := 0
	for {
		rows, err := r.Store.PendingOutbox(ctx, batch)
		if err != nil || len(rows) == 0 {
			return sent, err
		}
		for _, m := range rows {
			if m.EnsureTag != "" {
				if err := r.Pub.EnsureTaskConsumer(ctx, m.EnsureTeam, m.EnsureTag); err != nil {
					return sent, err
				}
			}
			if err := r.Pub.Publish(ctx, m.Subject, m.Header, m.Data); err != nil {
				return sent, err
			}
			if err := r.Store.MarkSent(ctx, m.PublisherSeq); err != nil {
				return sent, err
			}
			sent++
		}
	}
}
