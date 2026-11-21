package test

import (
	"testing"
)

// Spec: service-platform — service publishes changes through an outbox.
func TestV0048ServicePlatformServicePublishesChangesThroughAnOutbox(t *testing.T) {
	run(t, "Rolled-back change publishes nothing", func(t *testing.T) {
		h := newHarness(t)
		h.teamCreated(3, imageTeam3)
		h.bind(3, 1, flow(2))
		h.Store.Hook = func(step string) error {
			if step == "save_task" {
				return errTest
			}
			return nil
		}
		h.intake(3, 1, entry(1, "A"))
		h.flush()
		if len(h.Bus.Messages()) != 0 || h.Store.OutboxLen() != 0 {
			t.Fatalf("rolled-back change published %d messages", len(h.Bus.Messages()))
		}
	})

	run(t, "Synchronous publications without an outbox", func(t *testing.T) {
		// task-manager's form: replies are the only synchronous answers; every
		// stream message waits for the relay.
		h := newHarness(t)
		task := h.seedTask(3, 1, "A")
		before := len(h.Bus.Messages())
		h.cancel(3, 11, task)
		if len(h.Bus.Messages()) != before {
			t.Fatalf("cancel published synchronously")
		}
		h.flush()
		if len(h.Bus.Messages()) == before {
			t.Fatalf("cancel produced no stream message")
		}
	})
}
