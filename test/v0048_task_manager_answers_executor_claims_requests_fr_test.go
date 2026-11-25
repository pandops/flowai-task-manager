package test

import "testing"

// Spec: task-manager — task-manager answers executor-claims requests from executor-registry.
// "Caller outside the allowed list" is enforced by NATS permissions
// (infrastructure, deliberately not covered).
func TestV0048TaskManagerAnswersExecutorClaimsRequests(t *testing.T) {
	run(t, "Reported created claim is listed as held", func(t *testing.T) {
		h := newHarness(t)
		task, _ := h.claimed(3, 7, "A")
		r := h.executorClaims(3, 7, 0, claimRef{Task: task, Attempt: 1, Claim: 1, State: "created"})
		got := heldClaims(r)
		if r.Status != 200 || len(got) != 1 || got[0] != (claimRef{Task: task, Attempt: 1, Claim: 1}) || num(r.Body["executor_id"]) != 7 {
			t.Fatalf("reply = %d %v", r.Status, r.Body)
		}
	})

	run(t, "Reply omits a released claim", func(t *testing.T) {
		h := newHarness(t)
		a := h.runningTask(3, 7, "A")
		b := h.runningTask(3, 7, "B")
		h.released(3, 7, b, 1, 1, "executor_restarted")
		r := h.executorClaims(3, 7, 0, claimRef{Task: a, Attempt: 1, Claim: 1}, claimRef{Task: b, Attempt: 1, Claim: 1})
		got := heldClaims(r)
		if len(got) != 1 || got[0].Task != a {
			t.Fatalf("held = %v", got)
		}
		if cur := h.current(3, b); cur.State != "queued" || cur.ClaimSeq != 1 || cur.Held() {
			t.Fatalf("reported released claim restored: %+v", cur)
		}
	})

	run(t, "Request with an unapplied lost_seq releases first", func(t *testing.T) {
		h := newHarness(t)
		a := h.runningTask(3, 7, "A")
		b := h.runningTask(3, 7, "B")
		r := h.executorClaims(3, 7, 900, claimRef{Task: a, Attempt: 1, Claim: 1}, claimRef{Task: b, Attempt: 1, Claim: 1})
		h.flush()
		rel := h.events("released")
		if len(heldClaims(r)) != 0 || len(rel) != 2 {
			t.Fatalf("held %v released %d", heldClaims(r), len(rel))
		}
		for _, e := range rel {
			if str(e["reason"]) != "executor_lost" || num(e["lost_publisher_seq"]) != 900 {
				t.Fatalf("released = %v", e)
			}
		}
		h.lost(3, 7, 900)
		h.flush()
		if len(h.events("released")) != 2 {
			t.Fatalf("the executor_lost event released again")
		}
	})
}
