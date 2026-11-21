package test

import (
	"testing"
	"time"
)

// Spec: task-manager — task-manager releases held claims that the Executor no longer reports.
func TestV0048TaskManagerReleasesHeldClaimsTheExecutorNoLongerReports(t *testing.T) {
	run(t, "Claim missing from three heartbeats", func(t *testing.T) {
		h := newHarness(t)
		task := h.runningTask(3, 7, "A")
		h.advance(2 * time.Minute)
		for i := 0; i < 3; i++ {
			h.executorClaims(3, 7, 0)
			h.advance(10 * time.Second)
		}
		h.flush()
		rel := h.eventsFor("released", 3, task)
		if len(rel) != 1 || str(rel[0]["reason"]) != "claim_missing" {
			t.Fatalf("released = %v", rel)
		}
		if cur := h.current(3, task); cur.State != "queued" || cur.Attempt != 1 {
			t.Fatalf("state %s attempt %d", cur.State, cur.Attempt)
		}
		h.due(5 * time.Minute)
		msgs := h.taskMsgsFor(3, task)
		if len(msgs) != 2 || num(msgs[1]["claim_seq"]) != 2 {
			t.Fatalf("republication = %v", msgs)
		}
	})

	run(t, "Newly created claim is not yet reported", func(t *testing.T) {
		h := newHarness(t)
		task := h.seedTask(3, 1, "A")
		h.ensureExecutor(3, 7)
		h.executorClaims(3, 7, 0) // before the created
		h.advance(time.Second)
		h.created(3, 7, task, 1, 1)
		h.advance(time.Second)
		h.executorClaims(3, 7, 0)
		h.executorClaims(3, 7, 0)
		h.flush()
		if len(h.events("released")) != 0 || h.current(3, task).State != "created" {
			t.Fatalf("new claim released")
		}
	})

	run(t, "Claim reported again resets the count", func(t *testing.T) {
		h := newHarness(t)
		task := h.runningTask(3, 7, "A")
		h.advance(time.Minute)
		h.executorClaims(3, 7, 0)
		h.executorClaims(3, 7, 0)
		h.executorClaims(3, 7, 0, claimRef{Task: task, Attempt: 1, Claim: 1})
		h.executorClaims(3, 7, 0)
		h.executorClaims(3, 7, 0)
		h.flush()
		if len(h.events("released")) != 0 || h.current(3, task).State != "running" {
			t.Fatalf("count did not restart")
		}
		h.executorClaims(3, 7, 0)
		h.flush()
		if len(h.events("released")) != 1 {
			t.Fatalf("third consecutive omission after the reset did not release")
		}
	})
}
