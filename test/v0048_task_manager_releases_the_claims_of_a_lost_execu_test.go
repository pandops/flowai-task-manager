package test

import (
	"testing"
	"time"
)

// Spec: task-manager — task-manager releases the claims of a lost Executor.
func TestV0048TaskManagerReleasesTheClaimsOfALostExecutor(t *testing.T) {
	run(t, "executor_lost releases every held claim", func(t *testing.T) {
		h := newHarness(t)
		a := h.runningTask(3, 7, "A")
		b := h.runningTask(3, 7, "B")
		h.lost(3, 7, 900)
		h.flush()
		rel := h.events("released")
		if len(rel) != 2 {
			t.Fatalf("released = %v", rel)
		}
		for _, e := range rel {
			if str(e["reason"]) != "executor_lost" || num(e["executor_id"]) != 7 {
				t.Fatalf("released = %v", e)
			}
		}
		h.due(5 * time.Minute)
		for _, task := range []int64{a, b} {
			msgs := h.taskMsgsFor(3, task)
			if len(msgs) != 2 || num(msgs[1]["attempt"]) != 1 || num(msgs[1]["claim_seq"]) != 2 {
				t.Fatalf("task %d republication = %v", task, msgs)
			}
		}
	})

	run(t, "Lost Executor reports finished late", func(t *testing.T) {
		h := newHarness(t)
		task := h.runningTask(3, 7, "A")
		h.lost(3, 7, 900)
		h.due(5 * time.Minute)
		h.ensureExecutor(3, 8)
		h.created(3, 8, task, 1, 2)
		h.finished(3, 7, task, 1, 1)
		if cur := h.current(3, task); cur == nil || cur.State != "created" || cur.CurrentClaim().ExecutorID != 8 {
			t.Fatalf("late finished applied: %+v", cur)
		}
	})

	run(t, "Late restore after the release", func(t *testing.T) {
		h := newHarness(t)
		a := h.runningTask(3, 7, "A")
		b := h.runningTask(3, 7, "B")
		h.lost(3, 7, 900)
		h.flush()
		r := h.executorClaims(3, 7, 900, claimRef{Task: a, Attempt: 1, Claim: 1}, claimRef{Task: b, Attempt: 1, Claim: 1})
		h.flush()
		if len(heldClaims(r)) != 0 || len(h.events("released")) != 2 {
			t.Fatalf("held %v released %d", heldClaims(r), len(h.events("released")))
		}
		for _, task := range []int64{a, b} {
			if c := h.claimCurrent(3, task, 1, 1, 7); c.Body["current"] != false || str(c.Body["reason"]) != "claim_ended" {
				t.Fatalf("claim-current = %v", c.Body)
			}
		}
	})

	run(t, "Redelivered executor_lost releases nothing twice", func(t *testing.T) {
		h := newHarness(t)
		task := h.runningTask(3, 7, "A")
		h.lost(3, 7, 900)
		h.due(5 * time.Minute)
		h.created(3, 7, task, 1, 2)
		h.lost(3, 7, 900)
		h.flush()
		if cur := h.current(3, task); len(h.events("released")) != 1 || cur.State != "created" || cur.ClaimSeq != 2 {
			t.Fatalf("redelivery released again: %d %s", len(h.events("released")), cur.State)
		}
	})
}
