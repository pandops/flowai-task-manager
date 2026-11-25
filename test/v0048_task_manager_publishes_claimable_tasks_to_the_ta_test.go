package test

import (
	"crypto/sha256"
	"testing"
	"time"

	"github.com/pandops/flowai-task-manager/internal/wire"
)

// Spec: task-manager — task-manager publishes claimable tasks to the task queue.
func TestV0048TaskManagerPublishesClaimableTasksToTheTaskQueue(t *testing.T) {
	run(t, "New task is published", func(t *testing.T) {
		h := newHarness(t)
		task := h.seedTask(3, 1, "A")
		all := h.Bus.Messages()
		var taskAt, queuedAt = -1, -1
		for i, m := range all {
			if m.Subject == "task-manager.task.3.openhands.2.pub" {
				taskAt = i
			}
			if m.Subject == "task-manager.event.queued.pub" {
				queuedAt = i
			}
		}
		if taskAt < 0 || queuedAt < taskAt {
			t.Fatalf("task message at %d, queued at %d", taskAt, queuedAt)
		}
		m := decode(all[taskAt].Data)
		if num(m["task_id"]) != task || num(m["attempt"]) != 1 || num(m["claim_seq"]) != 1 || num(m["pending_limit_seconds"]) != 900 || all[taskAt].Header["team"] != "3" {
			t.Fatalf("task message = %v", m)
		}
	})

	run(t, "Task consumer name is derived from the team and tag", func(t *testing.T) {
		sum := sha256.Sum256([]byte("GPU_large-1"))
		want := "tasks-3-" + hexSHA(sum[:])[:16]
		if got := wire.TaskConsumer(3, "GPU_large-1"); got != want {
			t.Fatalf("consumer = %q, want %q", got, want)
		}
		if wire.TaskFilter(3, "GPU_large-1") != "task-manager.task.3.GPU_large-1.*.sub" {
			t.Fatalf("filter = %q", wire.TaskFilter(3, "GPU_large-1"))
		}
		h := newHarness(t)
		f := flow(2)
		f.Tag = "GPU_large-1"
		h.seedTask(3, 1, "A", f)
		if len(h.Bus.Ensured) == 0 || h.Bus.Ensured[0] != want {
			t.Fatalf("ensured = %v", h.Bus.Ensured)
		}
		if len(h.published("task-manager.task.3.GPU_large-1.2.pub")) != 1 {
			t.Fatalf("task not published on its tag subject")
		}
	})

	run(t, "Queued events carry the intake ID", func(t *testing.T) {
		h := newHarness(t)
		h.teamCreated(3, imageTeam3)
		h.registered(3, 7, "openhands")
		h.execs[7] = true
		h.bind(3, 1, flow(2), flow(5))
		h.intake(3, 1, entry(41, "ISSUE-41"))
		h.flush()
		var flow2 int64
		for _, v := range h.currentOf(3) {
			if v.FlowID == 2 {
				flow2 = v.TaskID
			}
		}
		h.created(3, 7, flow2, 1, 1)
		h.released(3, 7, flow2, 1, 1, "pending_timeout")
		h.flush()
		h.due(5 * time.Minute)
		q := h.events("queued")
		if len(q) != 3 {
			t.Fatalf("queued events = %d, want 3", len(q))
		}
		for _, e := range q {
			if num(e["intake_id"]) != 41 {
				t.Fatalf("queued without the intake id: %v", e)
			}
		}
	})

	run(t, "Republished task goes to the back", func(t *testing.T) {
		h := newHarness(t)
		old, _ := h.claimed(3, 7, "OLD", flow(2), flow(5))
		h.released(3, 7, old, 1, 1, "pending_timeout")
		h.flush()
		h.bind(3, 1, flow(2), flow(5))
		h.intake(3, 1, entry(500, "NEW"))
		h.flush()
		h.due(5 * time.Minute)
		var order []int64
		for _, m := range h.taskMsgs() {
			order = append(order, num(m["task_id"]))
		}
		if len(order) == 0 || order[len(order)-1] != old || len(order) != 5 {
			t.Fatalf("publication order = %v, republished task %d must be last", order, old)
		}
	})
}
