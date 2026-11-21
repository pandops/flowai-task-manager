//go:build integration

package test

import (
	"testing"
	"time"

	"github.com/pandops/flowai-task-manager/internal/wire"
)

// Consumer deduplication by (publisher, publisher_seq) on PostgreSQL
// (critique-1 finding 2): a redelivered message, also when a second process
// on the same schema receives it, changes nothing.
func TestV0048ConsumerDedupeIntegration(t *testing.T) {
	run(t, "Committed intake redelivered to another process", func(t *testing.T) {
		env := newPGEnv(t, true)
		h := newHarnessOn(t, openPG(t, env))
		other := h.peer(openPG(t, env))
		h.teamCreated(3, imageTeam3)
		h.bind(3, 1, flow(2))
		ack, seq := h.intake(3, 1, entry(1, "A"))
		h.flush()
		if ack.Kind != wire.AckOK || len(h.currentOf(3)) != 1 {
			t.Fatalf("first delivery: %v %d", ack.Kind, len(h.currentOf(3)))
		}
		for _, svc := range [](func(func())){func(f func()) { h.via(other, f) }, func(f func()) { f() }} {
			svc(func() {
				if again := h.intakeSeq(3, 1, seq, entry(1, "A")); again.Kind != wire.AckOK {
					t.Fatalf("redelivery not acknowledged: %v", again.Kind)
				}
			})
		}
		h.flush()
		if len(h.currentOf(3)) != 1 || len(h.taskMsgs()) != 1 || len(h.events("queued")) != 1 || len(intakeOutcomes(h, seq)) != 1 {
			t.Fatalf("redelivery changed state: tasks %d msgs %d queued %d processed %d",
				len(h.currentOf(3)), len(h.taskMsgs()), len(h.events("queued")), len(intakeOutcomes(h, seq)))
		}
	})

	run(t, "Executor event redelivered to another process", func(t *testing.T) {
		env := newPGEnv(t, true)
		h := newHarnessOn(t, openPG(t, env))
		other := h.peer(openPG(t, env))
		task := h.runningTask(3, 7, "A")
		_, seq := h.exec(3, 7, "failed", task, 1, 1, map[string]any{"failure_reason": "agent_error", "retryable": true})
		h.flush()
		if cur := h.current(3, task); cur == nil || cur.Attempt != 2 {
			t.Fatalf("after failed: %+v", cur)
		}
		h.via(other, func() {
			h.execSeq(3, 7, "failed", task, 1, 1, seq, map[string]any{"failure_reason": "agent_error", "retryable": true})
		})
		h.flush()
		if cur := h.current(3, task); cur == nil || cur.Attempt != 2 || len(h.eventsFor("retry_scheduled", 3, task)) != 1 {
			t.Fatalf("redelivered failed changed the task: %+v, retry_scheduled %d", cur, len(h.eventsFor("retry_scheduled", 3, task)))
		}
	})

	run(t, "Redelivered executor_lost releases nothing twice", func(t *testing.T) {
		env := newPGEnv(t, true)
		h := newHarnessOn(t, openPG(t, env))
		other := h.peer(openPG(t, env))
		task := h.runningTask(3, 7, "A")
		h.lost(3, 7, 40)
		h.flush()
		if len(h.eventsFor("released", 3, task)) != 1 {
			t.Fatalf("executor_lost did not release the claim")
		}
		h.due(300 * time.Second)
		h.created(3, 7, task, 1, 2) // the Executor wins the republication
		h.flush()
		if cur := h.current(3, task); cur == nil || !cur.Held() || cur.ClaimSeq != 2 {
			t.Fatalf("claim 2 not held: %+v", cur)
		}
		h.via(other, func() { h.lost(3, 7, 40) })
		h.flush()
		if cur := h.current(3, task); cur == nil || !cur.Held() || cur.ClaimSeq != 2 || len(h.eventsFor("released", 3, task)) != 1 {
			t.Fatalf("redelivered executor_lost released again: %+v", cur)
		}
	})
}
