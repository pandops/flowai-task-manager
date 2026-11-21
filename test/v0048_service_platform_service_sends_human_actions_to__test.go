package test

import (
	"strings"
	"testing"
)

// Spec: service-platform — service sends human actions to the audit stream.
func TestV0048ServicePlatformServiceSendsHumanActionsToAudit(t *testing.T) {
	run(t, "Operator cancels a task", func(t *testing.T) {
		h := newHarness(t)
		task := h.seedTask(3, 1, "A")
		r := h.cancel(3, 11, task)
		h.flush()
		a := h.audits("cancel")
		if len(a) != 1 || str(a[0]["actor_type"]) != "operator" || num(a[0]["actor_id"]) != 11 || num(a[0]["request_id"]) <= 0 || num(obj(a[0]["changes"])["control_id"]) != num(r.Body["control_id"]) {
			t.Fatalf("audit = %v", a)
		}
	})

	run(t, "Forwarded administrator action is audited by its owner", func(t *testing.T) {
		h := newHarness(t)
		h.seedTask(3, 1, "A")
		h.admin(map[string]any{"team_id": 3}, nil)
		h.flush()
		msgs := h.published("task-manager.audit.admin-list-tasks.pub")
		if len(msgs) != 1 || msgs[0].Header["team"] != "admin" || num(obj(decode(msgs[0].Data)["changes"])["team_id"]) != 3 {
			t.Fatalf("admin audit = %v", msgs)
		}
	})

	run(t, "Rejected human action is audited", func(t *testing.T) {
		h := newHarness(t)
		task := h.runningTask(3, 7, "A")
		h.finished(3, 7, task, 1, 1)
		h.cancel(3, 11, task) // task_terminal
		h.admin(map[string]any{"flow_id": 2}, nil)
		h.flush()
		c := h.audits("cancel")
		a := h.audits("admin-list-tasks")
		if len(c) != 1 || str(c[0]["outcome"]) != "rejected" || len(a) != 1 || str(a[0]["outcome"]) != "rejected" {
			t.Fatalf("rejected actions audit = %v / %v", c, a)
		}
	})

	run(t, "Executor event goes to journal", func(t *testing.T) {
		h := newHarness(t)
		task := h.runningTask(3, 7, "A")
		h.finished(3, 7, task, 1, 1)
		h.flush()
		for _, m := range h.Bus.Messages() {
			if strings.Contains(m.Subject, ".audit.") {
				t.Fatalf("Executor events produced audit %s", m.Subject)
			}
		}
	})
}
