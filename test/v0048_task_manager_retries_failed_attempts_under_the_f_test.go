package test

import (
	"testing"
	"time"
)

// Spec: task-manager — task-manager retries failed attempts under the frozen retry policy.
func TestV0048TaskManagerRetriesFailedAttempts(t *testing.T) {
	run(t, "Retry count 3", func(t *testing.T) {
		h := newHarness(t)
		task, _ := h.claimed(3, 7, "A")
		for attempt := int64(1); attempt <= 4; attempt++ {
			if attempt > 1 {
				h.created(3, 7, task, attempt, 1)
			}
			h.running(3, 7, task, attempt, 1)
			h.failed(3, 7, task, attempt, 1, "agent_error", true)
			h.flush()
			if attempt < 4 {
				if cur := h.current(3, task); cur == nil || cur.State != "queued" || cur.Attempt != attempt+1 {
					t.Fatalf("after attempt %d: %+v", attempt, cur)
				}
				before := len(h.taskMsgsFor(3, task))
				h.due(59 * time.Second)
				if got := len(h.taskMsgsFor(3, task)); got != before {
					t.Fatalf("attempt %d republished before the 60 s retry wait (%d -> %d)", attempt+1, before, got)
				}
				h.due(time.Second)
				msgs := h.taskMsgsFor(3, task)
				if len(msgs) != before+1 {
					t.Fatalf("attempt %d: %d publications after the retry wait, want %d", attempt+1, len(msgs), before+1)
				}
				last := msgs[len(msgs)-1]
				if num(last["attempt"]) != attempt+1 || num(last["claim_seq"]) != 1 {
					t.Fatalf("attempt %d publication = %v", attempt+1, last)
				}
			}
		}
		if len(h.eventsFor("retry_scheduled", 3, task)) != 3 {
			t.Fatalf("retry_scheduled = %d", len(h.eventsFor("retry_scheduled", 3, task)))
		}
		rec := h.archived(3, task)
		if rec == nil || rec.State != "failed" || len(rec.Attempts) != 4 || rec.FinalReason == nil || *rec.FinalReason != "agent_error" {
			t.Fatalf("final record = %+v", rec)
		}
	})

	run(t, "Retry count 0", func(t *testing.T) {
		h := newHarness(t)
		f := flow(2)
		f.RetryCount = 0
		task := h.runningTask(3, 7, "A", f)
		h.failed(3, 7, task, 1, 1, "agent_error", true)
		h.flush()
		if rec := h.archived(3, task); rec == nil || rec.State != "failed" || len(h.events("retry_scheduled")) != 0 {
			t.Fatalf("record = %+v", rec)
		}
	})

	run(t, "Non-retryable failure", func(t *testing.T) {
		h := newHarness(t)
		task := h.runningTask(3, 7, "A")
		h.failed(3, 7, task, 1, 1, "invalid_parameters", false)
		h.flush()
		if rec := h.archived(3, task); rec == nil || rec.State != "failed" || len(rec.Attempts) != 1 || len(h.events("retry_scheduled")) != 0 {
			t.Fatalf("record = %+v", rec)
		}
	})
}
