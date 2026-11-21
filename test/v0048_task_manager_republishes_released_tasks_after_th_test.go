package test

import (
	"testing"
	"time"

	"github.com/pandops/flowai-task-manager/internal/outbox"
	"github.com/pandops/flowai-task-manager/internal/service"
)

// Spec: task-manager — task-manager republishes released tasks after the release delay.
func TestV0048TaskManagerRepublishesReleasedTasksAfterTheReleaseDelay(t *testing.T) {
	run(t, "Pending timeout release", func(t *testing.T) {
		h := newHarness(t)
		f := flow(2)
		f.ReleaseDelay = 600
		task, _ := h.claimed(3, 7, "A", f)
		h.released(3, 7, task, 1, 1, "pending_timeout")
		h.flush()
		if n := h.due(599 * time.Second); n != 0 || len(h.taskMsgsFor(3, task)) != 1 {
			t.Fatalf("republished before m: %d", n)
		}
		h.due(time.Second)
		msgs := h.taskMsgsFor(3, task)
		if len(msgs) != 2 || num(msgs[1]["attempt"]) != 1 || num(msgs[1]["claim_seq"]) != 2 {
			t.Fatalf("republication = %v", msgs)
		}
	})

	run(t, "Restart and shutdown releases wait for m", func(t *testing.T) {
		h := newHarness(t)
		a := h.runningTask(3, 7, "A")
		b := h.runningTask(3, 7, "B")
		h.released(3, 7, a, 1, 1, "executor_restarted")
		h.released(3, 7, b, 1, 1, "executor_shutdown")
		h.flush()
		h.due(299 * time.Second)
		if len(h.taskMsgsFor(3, a)) != 1 || len(h.taskMsgsFor(3, b)) != 1 {
			t.Fatalf("republished before m")
		}
		h.due(time.Second)
		for _, task := range []int64{a, b} {
			msgs := h.taskMsgsFor(3, task)
			if len(msgs) != 2 || num(msgs[1]["claim_seq"]) != 2 || num(msgs[1]["attempt"]) != 1 {
				t.Fatalf("task %d republication = %v", task, msgs)
			}
		}
	})

	run(t, "Restart during the release delay", func(t *testing.T) {
		h := newHarness(t)
		task, _ := h.claimed(3, 7, "A")
		h.released(3, 7, task, 1, 1, "pending_timeout")
		h.flush()
		h.advance(5 * time.Minute)
		// A new process on the same store and broker.
		restarted := service.New(service.Config{Store: h.Store, Flows: h.Bus, Signer: h.Signer, Now: func() time.Time { return h.Now }})
		relay := &outbox.Relay{Store: h.Store, Pub: h.Bus, Batch: 100}
		if _, err := restarted.RunDue(t.Context()); err != nil {
			t.Fatalf("run due after restart: %v", err)
		}
		relay.Flush(t.Context())
		restarted.RunDue(t.Context())
		relay.Flush(t.Context())
		if msgs := h.taskMsgsFor(3, task); len(msgs) != 2 {
			t.Fatalf("republications after restart = %d, want exactly one more", len(msgs)-1)
		}
	})
}
