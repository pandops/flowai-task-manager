package test

import (
	"testing"
	"time"
)

// Spec: task-manager — task-manager defines the task lifecycle with attempts and claims.
func TestV0048TaskManagerDefinesTheTaskLifecycle(t *testing.T) {
	run(t, "Claim, pending, release and second claim", func(t *testing.T) {
		h := newHarness(t)
		task, _ := h.claimed(3, 7, "A")
		h.ensureExecutor(3, 8)
		h.exec(3, 7, "pending", task, 1, 1, map[string]any{"reason": "budget_exhausted"})
		h.released(3, 7, task, 1, 1, "pending_timeout")
		h.flush()
		if cur := h.current(3, task); cur.State != "queued" || cur.Attempt != 1 {
			t.Fatalf("after release: %s attempt %d", cur.State, cur.Attempt)
		}
		h.due(5 * time.Minute)
		h.created(3, 8, task, 1, 2)
		h.running(3, 8, task, 1, 2)
		h.finished(3, 8, task, 1, 2)
		h.flush()
		rec := h.archived(3, task)
		if rec == nil || rec.State != "finished" || len(rec.Attempts) != 1 || len(rec.Attempts[0].Claims) != 2 {
			t.Fatalf("record = %+v", rec)
		}
		c1, c2 := rec.Attempts[0].Claims[0], rec.Attempts[0].Claims[1]
		if c1.ExecutorID != 7 || c2.ExecutorID != 8 || len(c1.PendingReasons) != 1 || c1.PendingReasons[0] != "budget_exhausted" || c1.ReleasedReason == nil || *c1.ReleasedReason != "pending_timeout" {
			t.Fatalf("claims = %+v %+v", c1, c2)
		}
		want := []string{"queued", "created", "pending", "released", "queued", "created", "running", "finished"}
		if got := historyKinds(rec); !sameStrings(got, want) {
			t.Fatalf("history = %v, want %v", got, want)
		}
	})

	run(t, "Configuration error after claim", func(t *testing.T) {
		h := newHarness(t)
		task, _ := h.claimed(3, 7, "A")
		h.failed(3, 7, task, 1, 1, "image_missing", false)
		h.flush()
		rec := h.archived(3, task)
		if rec == nil || rec.State != "failed" || len(h.events("retry_scheduled")) != 0 || rec.Attempts[0].FailureReason == nil || *rec.Attempts[0].FailureReason != "image_missing" {
			t.Fatalf("record = %+v", rec)
		}
	})

	run(t, "Running claim released by an Executor restart", func(t *testing.T) {
		h := newHarness(t)
		task := h.runningTask(3, 7, "A")
		h.released(3, 7, task, 1, 1, "executor_restarted")
		h.flush()
		if cur := h.current(3, task); cur.State != "queued" || cur.Attempt != 1 {
			t.Fatalf("state %s attempt %d", cur.State, cur.Attempt)
		}
		h.due(5 * time.Minute)
		msgs := h.taskMsgsFor(3, task)
		if len(msgs) != 2 || num(msgs[1]["attempt"]) != 1 || num(msgs[1]["claim_seq"]) != 2 {
			t.Fatalf("republication = %v", msgs)
		}
	})

	run(t, "Shutdown releases claims in every held state", func(t *testing.T) {
		h := newHarness(t)
		a, _ := h.claimed(3, 7, "CREATED")
		b, _ := h.claimed(3, 7, "PENDING")
		h.exec(3, 7, "pending", b, 1, 1, map[string]any{"reason": "no_active_model"})
		c := h.runningTask(3, 7, "RUNNING")
		for _, task := range []int64{a, b, c} {
			h.released(3, 7, task, 1, 1, "executor_shutdown")
		}
		h.flush()
		for _, task := range []int64{a, b, c} {
			if cur := h.current(3, task); cur.State != "queued" || cur.Attempt != 1 {
				t.Fatalf("task %d: %s attempt %d", task, cur.State, cur.Attempt)
			}
		}
		h.due(5 * time.Minute)
		for _, task := range []int64{a, b, c} {
			msgs := h.taskMsgsFor(3, task)
			if len(msgs) != 2 || num(msgs[1]["claim_seq"]) != 2 || num(msgs[1]["attempt"]) != 1 {
				t.Fatalf("task %d republication = %v", task, msgs)
			}
		}
	})

	run(t, "Pending timeout for a running claim is ignored", func(t *testing.T) {
		h := newHarness(t)
		task := h.runningTask(3, 7, "A")
		before := len(h.current(3, task).History)
		h.released(3, 7, task, 1, 1, "pending_timeout")
		h.flush()
		if cur := h.current(3, task); cur.State != "running" || len(cur.History) != before {
			t.Fatalf("state %s history %d->%d", cur.State, before, len(cur.History))
		}
	})

	run(t, "Executor publishes executor_lost", func(t *testing.T) {
		for _, reason := range []string{"executor_lost", "claim_missing", "made_up"} {
			h := newHarness(t)
			task := h.runningTask(3, 7, "A")
			h.released(3, 7, task, 1, 1, reason)
			h.flush()
			if cur := h.current(3, task); cur.State != "running" || cur.ClaimSeq != 1 {
				t.Fatalf("%s from an Executor changed state to %s", reason, cur.State)
			}
		}
	})
}
