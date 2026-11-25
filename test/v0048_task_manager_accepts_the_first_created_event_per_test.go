package test

import (
	"testing"

	"github.com/pandops/flowai-task-manager/internal/wire"
)

// Spec: task-manager — task-manager accepts the first created event per claim as the claim.
func TestV0048TaskManagerAcceptsTheFirstCreatedEventPerClaim(t *testing.T) {
	run(t, "Two Executors claim the same message", func(t *testing.T) {
		h := newHarness(t)
		task := h.seedTask(3, 1, "A")
		h.ensureExecutor(3, 7)
		h.ensureExecutor(3, 8)
		s7 := h.created(3, 7, task, 1, 1)
		s8 := h.created(3, 8, task, 1, 1)
		h.flush()
		cur := h.current(3, task)
		if cur.State != "created" || cur.CurrentClaim() == nil || cur.CurrentClaim().ExecutorID != 7 || cur.CurrentClaim().CreatedPublisherSeq != s7 {
			t.Fatalf("winner = %+v", cur.CurrentClaim())
		}
		if r := h.claimToken(8, task, 1, 1, s8); r.Status != 409 || str(r.Body["code"]) != "claim_rejected" {
			t.Fatalf("loser token = %d %v", r.Status, r.Body)
		}
	})

	run(t, "Accepted claim publishes claim_accepted", func(t *testing.T) {
		h := newHarness(t)
		task := h.seedTask(3, 1, "A")
		h.ensureExecutor(3, 7)
		h.ensureExecutor(3, 8)
		failNext := false
		h.Store.Hook = func(step string) error {
			if failNext && step == "append_outbox" {
				failNext = false
				return errTest
			}
			return nil
		}
		failNext = true
		ack, s := h.exec(3, 7, "created", task, 1, 1, nil)
		if cur := h.current(3, task); cur.State != "queued" || ack.Kind != wire.AckNak {
			t.Fatalf("a failed claim_accepted outbox write must roll the claim back and leave the event unacknowledged: %s %v", cur.State, ack.Kind)
		}
		h.execSeq(3, 7, "created", task, 1, 1, s, nil) // redelivery of the same created
		h.created(3, 8, task, 1, 1)
		h.flush()
		got := h.events("claim_accepted")
		if len(got) != 1 || num(got[0]["executor_id"]) != 7 || num(got[0]["attempt"]) != 1 || num(got[0]["claim_seq"]) != 1 || num(got[0]["task_id"]) != task {
			t.Fatalf("claim_accepted = %v", got)
		}
	})

	run(t, "Claim-token request arrives before its created event", func(t *testing.T) {
		h := newHarness(t)
		task := h.seedTask(3, 1, "A")
		h.ensureExecutor(3, 7)
		h.ensureExecutor(3, 8)
		if r := h.claimToken(8, task, 1, 1, 1); r.Status != 409 || str(r.Body["code"]) != "claim_not_processed" {
			t.Fatalf("early request = %d %v", r.Status, r.Body)
		}
		if cur := h.current(3, task); cur.State != "queued" {
			t.Fatalf("early token request changed state: %s", cur.State)
		}
		h.execSeq(3, 7, "created", task, 1, 1, 1, nil)
		h.execSeq(3, 8, "created", task, 1, 1, 1, nil)
		h.flush()
		if r := h.claimToken(7, task, 1, 1, 1); r.Status != 200 || str(r.Body["claim_token"]) == "" {
			t.Fatalf("winner = %d %v", r.Status, r.Body)
		}
		if r := h.claimToken(8, task, 1, 1, 1); r.Status != 409 || str(r.Body["code"]) != "claim_rejected" {
			t.Fatalf("loser = %d %v", r.Status, r.Body)
		}
	})

	run(t, "Created from a foreign team's Executor", func(t *testing.T) {
		h := newHarness(t)
		task := h.seedTask(3, 1, "A")
		h.ensureTeam(4)
		h.ensureExecutor(4, 9)
		_, s := h.exec(3, 9, "created", task, 1, 1, nil) // header team: 3, envelope team 3
		h.flush()
		if cur := h.current(3, task); cur.State != "queued" || len(h.events("claim_accepted")) != 0 {
			t.Fatalf("foreign Executor claimed: %s", cur.State)
		}
		if r := h.claimToken(9, task, 1, 1, s); r.Status != 409 || str(r.Body["code"]) != "claim_rejected" {
			t.Fatalf("token = %d %v", r.Status, r.Body)
		}
	})

	run(t, "Created for a cancelled task", func(t *testing.T) {
		h := newHarness(t)
		task := h.seedTask(3, 1, "A")
		h.ensureExecutor(3, 7)
		if r := h.cancel(3, 11, task); r.Status != 200 {
			t.Fatalf("cancel = %d %v", r.Status, r.Body)
		}
		s := h.created(3, 7, task, 1, 1)
		h.flush()
		if rec := h.archived(3, task); rec == nil || rec.State != "cancelled" || len(h.events("claim_accepted")) != 0 {
			t.Fatalf("cancelled task claimed: %+v", rec)
		}
		if r := h.claimToken(7, task, 1, 1, s); r.Status != 409 || str(r.Body["code"]) != "claim_rejected" {
			t.Fatalf("token = %d %v", r.Status, r.Body)
		}
	})
}
