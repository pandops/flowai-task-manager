package test

import (
	"testing"

	"github.com/pandops/flowai-task-manager/internal/wire"
)

// Spec: task-manager — task-manager validates Executor lifecycle events before changing state.
func TestV0048TaskManagerValidatesExecutorLifecycleEvents(t *testing.T) {
	run(t, "Finished after finished", func(t *testing.T) {
		h := newHarness(t)
		task := h.runningTask(3, 7, "A")
		h.finished(3, 7, task, 1, 1)
		ack, _ := h.exec(3, 7, "finished", task, 1, 1, nil)
		h.flush()
		rec := h.archived(3, task)
		count := 0
		for _, k := range historyKinds(rec) {
			if k == "finished" {
				count++
			}
		}
		if ack.Kind != wire.AckOK || count != 1 || len(h.events("archived")) != 1 {
			t.Fatalf("ack %v finished entries %d archived events %d", ack.Kind, count, len(h.events("archived")))
		}
	})

	run(t, "Running before created", func(t *testing.T) {
		h := newHarness(t)
		task := h.seedTask(3, 1, "A")
		h.ensureExecutor(3, 7)
		ack, _ := h.exec(3, 7, "running", task, 1, 1, nil)
		if cur := h.current(3, task); ack.Kind != wire.AckOK || cur.State != "queued" {
			t.Fatalf("ack %v state %s", ack.Kind, cur.State)
		}
	})

	run(t, "Event from a non-winning Executor", func(t *testing.T) {
		h := newHarness(t)
		task, _ := h.claimed(3, 7, "A")
		h.ensureExecutor(3, 8)
		h.created(3, 8, task, 1, 1)
		h.running(3, 8, task, 1, 1)
		if cur := h.current(3, task); cur.State != "created" || cur.CurrentClaim().ExecutorID != 7 {
			t.Fatalf("state %s holder %d", cur.State, cur.CurrentClaim().ExecutorID)
		}
	})

	run(t, "Out-of-order event", func(t *testing.T) {
		h := newHarness(t)
		task, created := h.claimed(3, 7, "A")
		h.execSeq(3, 7, "running", task, 1, 1, created, nil) // equal publisher_seq, different kind
		if cur := h.current(3, task); cur.State != "created" {
			t.Fatalf("state %s", cur.State)
		}
		h.running(3, 7, task, 1, 1)
		h.execSeq(3, 7, "finished", task, 1, 1, created, nil) // lower than the running event
		if cur := h.current(3, task); cur == nil || cur.State != "running" {
			t.Fatalf("out-of-order finished accepted: %+v", cur)
		}
	})

	run(t, "Event redelivered by the broker", func(t *testing.T) {
		h := newHarness(t)
		task, _ := h.claimed(3, 7, "A")
		ack, seq := h.exec(3, 7, "running", task, 1, 1, nil)
		before := h.current(3, task)
		again := h.execSeq(3, 7, "running", task, 1, 1, seq, nil)
		after := h.current(3, task)
		if ack.Kind != wire.AckOK || again.Kind != wire.AckOK || after.State != "running" || len(after.History) != len(before.History) {
			t.Fatalf("redelivery changed history %d->%d", len(before.History), len(after.History))
		}
	})

	run(t, "Unknown and archived tasks are ignored", func(t *testing.T) {
		h := newHarness(t)
		h.ensureTeam(3)
		h.ensureExecutor(3, 7)
		if ack, _ := h.exec(3, 7, "running", 999, 1, 1, nil); ack.Kind != wire.AckOK || len(h.Store.CurrentTasks()) != 0 {
			t.Fatalf("unknown task: %v", ack.Kind)
		}
	})
}
