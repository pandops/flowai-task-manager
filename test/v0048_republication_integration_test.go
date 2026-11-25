//go:build integration

package test

import (
	"context"
	"sync"
	"testing"
	"time"
)

// FIFO republication, the due-time scheduler and claim_seq > 1 on PostgreSQL
// (critique-1 finding 3).
func TestV0048RepublicationIntegration(t *testing.T) {
	run(t, "Released task is republished with claim_seq 2 behind newer tasks", func(t *testing.T) {
		env := newPGEnv(t, true)
		h := newHarnessOn(t, openPG(t, env))
		a := h.runningTask(3, 7, "A")
		h.released(3, 7, a, 1, 1, "executor_restarted")
		h.flush()
		h.advance(time.Second)
		b := h.seedTask(3, 1, "B") // newer task of the same team and tag
		h.due(298 * time.Second)   // 299 s after the release
		if n := len(h.taskMsgsFor(3, a)); n != 1 {
			t.Fatalf("republished before the release delay: %d publications", n)
		}
		h.due(time.Second)
		msgs := h.taskMsgs()
		last := msgs[len(msgs)-1]
		if num(last["task_id"]) != a || num(last["attempt"]) != 1 || num(last["claim_seq"]) != 2 {
			t.Fatalf("last task message = %v", last)
		}
		bAt, aAt := -1, -1
		for i, m := range msgs {
			if num(m["task_id"]) == b {
				bAt = i
			}
			if num(m["task_id"]) == a && num(m["claim_seq"]) == 2 {
				aAt = i
			}
		}
		if bAt < 0 || aAt < bAt {
			t.Fatalf("republication not behind the newer task: b at %d, a#2 at %d", bAt, aAt)
		}
		h.ensureExecutor(3, 8)
		h.created(3, 8, a, 1, 1) // stale claim 1
		h.flush()
		if cur := h.current(3, a); cur == nil || cur.State != "queued" || len(h.eventsFor("claim_accepted", 3, a)) != 1 {
			t.Fatalf("stale created accepted: %+v", cur)
		}
		seq := h.created(3, 8, a, 1, 2)
		h.flush()
		if cur := h.current(3, a); cur == nil || cur.State != "created" || cur.ClaimSeq != 2 || len(h.eventsFor("claim_accepted", 3, a)) != 2 {
			t.Fatalf("claim 2 not accepted: %+v", cur)
		}
		if r := h.claimToken(8, a, 1, 2, seq); r.Status != 200 {
			t.Fatalf("claim 2 token = %d %v", r.Status, r.Body)
		}
	})

	run(t, "Second process starts during the release delay", func(t *testing.T) {
		env := newPGEnv(t, true)
		h := newHarnessOn(t, openPG(t, env))
		a := h.runningTask(3, 7, "A")
		h.released(3, 7, a, 1, 1, "executor_shutdown")
		h.flush()
		h.advance(100 * time.Second)
		other := h.peer(openPG(t, env)) // started during the delay
		h.advance(200 * time.Second)
		var wg sync.WaitGroup
		for _, run := range []func(context.Context) (int, error){h.Svc.RunDue, other.RunDue} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := run(context.Background()); err != nil {
					t.Errorf("run due: %v", err)
				}
			}()
		}
		wg.Wait()
		h.flush()
		var republished int
		for _, m := range h.taskMsgsFor(3, a) {
			if num(m["claim_seq"]) == 2 {
				republished++
			}
		}
		if republished != 1 {
			t.Fatalf("republications = %d, want 1", republished)
		}
	})
}
