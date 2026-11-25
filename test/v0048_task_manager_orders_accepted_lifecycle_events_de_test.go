package test

import "testing"

// Spec: task-manager — task-manager orders accepted lifecycle events deterministically.
func TestV0048TaskManagerOrdersAcceptedLifecycleEvents(t *testing.T) {
	run(t, "Two events share occurred_at", func(t *testing.T) {
		h := newHarness(t)
		task, _ := h.claimed(3, 7, "A")
		h.exec(3, 7, "pending", task, 1, 1, map[string]any{"reason": "no_active_model"})
		h.running(3, 7, task, 1, 1) // same h.Now, greater publisher_seq
		hist := h.current(3, task).History
		n := len(hist)
		if n < 2 || hist[n-2].Kind != "pending" || hist[n-1].Kind != "running" || hist[n-1].Seq != hist[n-2].Seq+1 || !hist[n-1].OccurredAt.Equal(hist[n-2].OccurredAt) {
			t.Fatalf("history = %+v", hist)
		}
		for i, e := range hist {
			if e.Seq != int64(i+1) {
				t.Fatalf("history_seq not 1..n: %+v", hist)
			}
		}
		card := h.getTask(3, task)
		items := list(card.Body["history"])
		for i, e := range items {
			if num(obj(e)["history_seq"]) != int64(i+1) {
				t.Fatalf("card history not ordered by history_seq: %v", items)
			}
		}
	})

	run(t, "Released by task-manager follows the last Executor event", func(t *testing.T) {
		h := newHarness(t)
		task := h.runningTask(3, 7, "A")
		h.lost(3, 7, 900)
		hist := h.current(3, task).History
		n := len(hist)
		if n < 2 || hist[n-2].Kind != "running" || hist[n-1].Kind != "released" || hist[n-1].Publisher != "task-manager" {
			t.Fatalf("history = %+v", hist)
		}
	})
}
