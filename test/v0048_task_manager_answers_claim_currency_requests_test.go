package test

import (
	"strings"
	"testing"
)

// Spec: task-manager — task-manager answers claim currency requests.
// "Caller outside the allowed list" is enforced by NATS permissions
// (infrastructure, deliberately not covered).
func TestV0048TaskManagerAnswersClaimCurrencyRequests(t *testing.T) {
	run(t, "env-registry checks a running claim", func(t *testing.T) {
		h := newHarness(t)
		task := h.runningTask(3, 7, "A")
		r := h.claimCurrent(3, task, 1, 1, 7)
		if r.Status != 200 || r.Body["current"] != true || num(r.Body["team_id"]) != 3 || num(r.Body["flow_id"]) != 2 || num(r.Body["execution_revision_id"]) != 12 {
			t.Fatalf("reply = %d %v", r.Status, r.Body)
		}
		if strings.Contains(string(r.Raw), "Fix A") || strings.Contains(string(r.Raw), "payload") {
			t.Fatalf("reply carries the snapshot: %s", r.Raw)
		}
		for _, bad := range []struct {
			team, attempt, claim, exec int64
			reason                     string
		}{{4, 1, 1, 7, "unknown_task"}, {3, 1, 2, 7, "claim_ended"}, {3, 1, 1, 8, "executor_mismatch"}} {
			got := h.claimCurrent(bad.team, task, bad.attempt, bad.claim, bad.exec)
			if got.Body["current"] != false || str(got.Body["reason"]) != bad.reason {
				t.Fatalf("%+v -> %v", bad, got.Body)
			}
		}
	})

	run(t, "Token of a released claim", func(t *testing.T) {
		h := newHarness(t)
		task := h.runningTask(3, 7, "A")
		h.lost(3, 7, 5)
		if r := h.claimCurrent(3, task, 1, 1, 7); r.Body["current"] != false || str(r.Body["reason"]) != "claim_ended" {
			t.Fatalf("reply = %v", r.Body)
		}
	})

	run(t, "Token of an archived task", func(t *testing.T) {
		h := newHarness(t)
		task := h.runningTask(3, 7, "A")
		h.finished(3, 7, task, 1, 1)
		if r := h.claimCurrent(3, task, 1, 1, 7); r.Body["current"] != false || str(r.Body["reason"]) != "unknown_task" {
			t.Fatalf("reply = %v", r.Body)
		}
	})
}
