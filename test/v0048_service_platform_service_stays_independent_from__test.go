package test

import (
	"testing"

	"github.com/pandops/flowai-task-manager/internal/wire"
)

// Spec: service-platform — service stays independent from runtime details.
func TestV0048ServicePlatformServiceStaysIndependent(t *testing.T) {
	run(t, "Executor capacity is not decided centrally", func(t *testing.T) {
		h := newHarness(t)
		for i := 0; i < 5; i++ {
			h.seedTask(3, 1, "T"+tok(int64(i)))
		}
		if len(h.taskMsgs()) != 5 {
			t.Fatalf("publication gated: %d of 5", len(h.taskMsgs()))
		}
		h.ensureExecutor(3, 7)
		for _, v := range h.currentOf(3) {
			h.created(3, 7, v.TaskID, 1, 1) // one Executor claims all; no capacity check
		}
		for _, v := range h.currentOf(3) {
			if v.State != "created" {
				t.Fatalf("claim gated by capacity: %s", v.State)
			}
		}
	})

	run(t, "Image equality across teams grants no authority", func(t *testing.T) {
		h := newHarness(t)
		h.teamCreated(3, imageTeam3)
		h.teamCreated(4, imageTeam3) // same default image
		h.teams[3], h.teams[4] = true, true
		task := h.seedTask(3, 1, "A")
		h.ensureExecutor(4, 9)
		h.created(4, 9, task, 1, 1)
		if h.current(3, task).State != "queued" {
			t.Fatalf("equal image let team 4 claim")
		}
	})

	run(t, "Flow cannot be created from a Source path", func(t *testing.T) {
		h := newHarness(t)
		h.teamCreated(3, imageTeam3)
		e := entry(1, "A")
		e["flow_id"] = 99
		ack := h.intakeRaw("source-controller.task.3.sub", map[string]string{"team": "3"}, h.intakeBody(3, 1, 1, e))
		if ack.Kind != wire.AckOK || len(h.Store.CurrentTasks()) != 0 {
			t.Fatalf("source-supplied flow accepted")
		}
	})
}
