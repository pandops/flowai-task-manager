package test

import (
	"testing"

	"github.com/pandops/flowai-task-manager/internal/wire"
)

// Spec: message-broker — consumers acknowledge explicitly.
func TestV0048MessageBrokerConsumersAcknowledgeExplicitly(t *testing.T) {
	run(t, "Redelivery does not start a new attempt", func(t *testing.T) {
		for _, c := range loadConsumers(t) {
			if c.AckPolicy != "explicit" {
				t.Fatalf("consumer %s ack policy %q", c.Durable, c.AckPolicy)
			}
		}
		h := newHarness(t)
		task, _ := h.claimed(3, 7, "A")
		// The queue message was redelivered after its ack wait and a second
		// Executor sends created for the same claim.
		h.ensureExecutor(3, 8)
		ack, _ := h.exec(3, 8, "created", task, 1, 1, nil)
		cur := h.current(3, task)
		if ack.Kind != wire.AckOK || cur.Attempt != 1 || len(cur.Attempts) != 1 || len(h.taskMsgsFor(3, task)) != 1 {
			t.Fatalf("redelivery started a new attempt: %+v", cur)
		}
	})
}
