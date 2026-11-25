package test

import (
	"testing"
)

// Spec: message-broker — admits new teams, Executors, Flows and Sources without a restart.
func TestV0048MessageBrokerAdmitsNewTeamsWithoutARestart(t *testing.T) {
	run(t, "Team is created without a restart", func(t *testing.T) {
		h := newHarness(t)
		h.seedTask(3, 1, "A")
		ensured := len(h.Bus.Ensured)
		h.teamCreated(12, imageTeam3)
		if len(h.Bus.Ensured) != ensured {
			t.Fatalf("team_created created a consumer")
		}
		h.bind(12, 1, flow(2))
		h.intake(12, 1, entry(1, "NEW"))
		h.flush()
		if len(h.currentOf(12)) != 1 {
			t.Fatalf("new team's intake not processed by the running service")
		}
	})

	run(t, "Flow with a new execution tag gets its queue live", func(t *testing.T) {
		h := newHarness(t)
		h.seedTask(3, 1, "A")
		f := flow(8)
		f.Tag = "gpu"
		h.bind(3, 1, f)
		h.intake(3, 1, entry(2, "B"))
		h.flush()
		found := false
		for _, e := range h.Bus.Ensured {
			if e == consumerName(3, "gpu") {
				found = true
			}
		}
		if !found || len(h.published("task-manager.task.3.gpu.8.pub")) != 1 {
			t.Fatalf("new tag queue not ensured live: %v", h.Bus.Ensured)
		}
		// The queue is ensured before the publish, so the message is stored
		// by a live consumer: a relay that ensures afterwards fails here.
		ensureAt, publishAt := -1, -1
		for i, e := range h.Bus.Events {
			if e == "ensure "+consumerName(3, "gpu") && ensureAt < 0 {
				ensureAt = i
			}
			if e == "publish task-manager.task.3.gpu.8.pub" && publishAt < 0 {
				publishAt = i
			}
		}
		if ensureAt < 0 || publishAt < 0 || ensureAt > publishAt {
			t.Fatalf("ensure at %d, publish at %d, want ensure first: %v", ensureAt, publishAt, h.Bus.Events)
		}
	})
}
