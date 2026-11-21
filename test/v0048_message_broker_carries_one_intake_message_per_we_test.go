package test

import (
	"testing"

	"github.com/pandops/flowai-task-manager/internal/wire"
)

// Spec: message-broker — carries one intake message per webhook request or polled task.
func TestV0048MessageBrokerCarriesOneIntakeMessagePerRequest(t *testing.T) {
	setup := func(t *testing.T) *harness {
		h := newHarness(t)
		h.teamCreated(3, imageTeam3)
		h.teamCreated(4, imageTeam4)
		h.bind(3, 1, flow(2))
		h.bind(4, 1, flow(2))
		return h
	}

	run(t, "Envelope disagrees with the team header", func(t *testing.T) {
		h := setup(t)
		ack := h.intakeRaw("source-controller.task.3.sub", map[string]string{"team": "3"}, h.intakeBody(4, 1, 3, entry(1, "A")))
		h.flush()
		if ack.Kind != wire.AckTerm || len(h.Store.CurrentTasks()) != 0 {
			t.Fatalf("ack %v", ack.Kind)
		}
		expectRejected(t, h, 3, 1, "invalid_source_task")
	})

	run(t, "Subject team differs from the team header", func(t *testing.T) {
		h := setup(t)
		for _, hdr := range []string{"3", "03", "", "3 "} {
			ack := h.intakeRaw("source-controller.task.4.sub", map[string]string{"team": hdr}, h.intakeBody(3, 1, 4, entry(1, "A")))
			if ack.Kind != wire.AckTerm || len(h.Store.CurrentTasks()) != 0 {
				t.Fatalf("header %q: ack %v", hdr, ack.Kind)
			}
		}
	})

	run(t, "One consumer reads every team's intake", func(t *testing.T) {
		h := setup(t)
		h.intake(3, 1, entry(1, "A"))
		h.intake(4, 1, entry(1, "B"))
		h.flush()
		if len(h.currentOf(3)) != 1 || len(h.currentOf(4)) != 1 {
			t.Fatalf("intake of both teams not processed by the one handler")
		}
		n := 0
		for _, c := range loadConsumers(t) {
			if c.Stream == wire.StreamIntake {
				n++
			}
		}
		if n != 1 {
			t.Fatalf("intake consumers = %d", n)
		}
	})
}
