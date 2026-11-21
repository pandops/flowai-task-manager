package test

import (
	"testing"
	"time"
)

func auditMsgs(h *harness, action string) []map[string]any { return h.audits(action) }

// Spec: task-manager — task-manager cancels tasks on operator request.
func TestV0048TaskManagerCancelsTasksOnOperatorRequest(t *testing.T) {
	run(t, "Cancel a queued task", func(t *testing.T) {
		h := newHarness(t)
		task := h.seedTask(3, 1, "A")
		r := h.cancel(3, 11, task)
		h.flush()
		if r.Status != 200 || num(r.Body["control_id"]) != 1 || str(r.Body["status"]) != "cancelled" {
			t.Fatalf("reply = %d %v", r.Status, r.Body)
		}
		c := h.eventsFor("cancelled", 3, task)
		if len(c) != 1 || num(c[0]["control_id"]) != 1 || len(h.eventsFor("archived", 3, task)) != 1 || h.archived(3, task).State != "cancelled" {
			t.Fatalf("cancelled %v archived %v", c, h.eventsFor("archived", 3, task))
		}
		a := auditMsgs(h, "cancel")
		if len(a) != 1 || str(a[0]["outcome"]) != "succeeded" || num(a[0]["actor_id"]) != 11 || num(a[0]["resource_id"]) != task || num(obj(a[0]["changes"])["control_id"]) != 1 || str(obj(a[0]["changes"])["status"]) != "cancelled" {
			t.Fatalf("audit = %v", a)
		}
		if again := h.cancel(3, 11, task); again.Status != 409 || str(again.Body["code"]) != "task_terminal" {
			t.Fatalf("cancel of a terminal task = %d %v", again.Status, again.Body)
		}
	})

	run(t, "Cancel a running task", func(t *testing.T) {
		h := newHarness(t)
		task := h.runningTask(3, 7, "A")
		r := h.cancel(3, 11, task)
		h.flush()
		cmds := h.commands()
		if r.Status != 202 || len(cmds) != 1 || cmds[0].Subject != "task-manager.command.7.pub" {
			t.Fatalf("reply %d commands %v", r.Status, cmds)
		}
		cmd := decode(cmds[0].Data)
		req := h.eventsFor("control_requested", 3, task)
		id := num(r.Body["control_id"])
		if num(cmd["control_id"]) != id || str(cmd["control"]) != "cancel" || num(cmd["claim_seq"]) != 1 || len(req) != 1 || num(req[0]["control_id"]) != id || str(req[0]["control"]) != "cancel" {
			t.Fatalf("command %v control_requested %v", cmd, req)
		}
		if again := h.cancel(3, 11, task); again.Status != 202 || num(again.Body["control_id"]) != id {
			t.Fatalf("repeated cancel not idempotent: %v", again.Body)
		}
		h.flush()
		if len(h.commands()) != 1 {
			t.Fatalf("repeated cancel sent another command")
		}
		if cur := h.current(3, task); cur.State != "running" {
			t.Fatalf("state %s before the Executor confirms", cur.State)
		}
		h.exec(3, 7, "control_acknowledged", task, 1, 1, map[string]any{"control_id": id, "control": "cancel"})
		h.exec(3, 7, "cancelled", task, 1, 1, map[string]any{"control_id": id})
		h.flush()
		if rec := h.archived(3, task); rec == nil || rec.State != "cancelled" {
			t.Fatalf("not cancelled after the Executor's cancelled: %+v", rec)
		}
		if len(h.eventsFor("cancelled", 3, task)) != 0 {
			t.Fatalf("task-manager published cancelled for a claim-holder cancel")
		}
	})

	run(t, "Executor answers with another control ID", func(t *testing.T) {
		h := newHarness(t)
		task := h.runningTask(3, 7, "A")
		r := h.cancel(3, 11, task)
		h.exec(3, 7, "cancelled", task, 1, 1, map[string]any{"control_id": num(r.Body["control_id"]) + 5})
		if cur := h.current(3, task); cur == nil || cur.State != "running" {
			t.Fatalf("foreign control id changed state: %+v", cur)
		}
	})

	run(t, "Cancel a task of a removed Executor", func(t *testing.T) {
		h := newHarness(t)
		task := h.runningTask(3, 7, "A")
		h.removed(3, 7, "openhands")
		r := h.cancel(3, 11, task)
		h.flush()
		if r.Status != 200 || len(h.commands()) != 0 || len(h.eventsFor("cancelled", 3, task)) != 1 || len(h.eventsFor("archived", 3, task)) != 1 {
			t.Fatalf("reply %d commands %d", r.Status, len(h.commands()))
		}
	})

	run(t, "Cancel a foreign task", func(t *testing.T) {
		h := newHarness(t)
		task := h.seedTask(4, 1, "A")
		h.ensureTeam(3)
		r := h.cancel(3, 11, task)
		unknown := h.cancel(3, 11, 999)
		h.flush()
		if r.Status != 404 || str(r.Body["code"]) != "not_found" || unknown.Status != 404 || str(unknown.Body["message"]) != str(r.Body["message"]) || len(h.commands()) != 0 {
			t.Fatalf("foreign = %d %v unknown = %v", r.Status, r.Body, unknown.Body)
		}
		a := auditMsgs(h, "cancel")
		if len(a) != 2 || str(a[0]["outcome"]) != "rejected" || str(a[0]["reason"]) != "not_found" || num(a[0]["team_id"]) != 3 {
			t.Fatalf("audit = %v", a)
		}
		for _, m := range h.published("task-manager.audit.cancel.pub") {
			if m.Header["team"] != "3" {
				t.Fatalf("audit header team = %q", m.Header["team"])
			}
		}
		if cur := h.current(4, task); cur.State != "queued" {
			t.Fatalf("foreign task changed")
		}
	})

	run(t, "Cancel races with release", func(t *testing.T) {
		h := newHarness(t)
		task := h.runningTask(3, 7, "A")
		h.cancel(3, 11, task)
		h.lost(3, 7, 900)
		h.flush()
		h.due(10 * time.Minute)
		if rec := h.archived(3, task); rec == nil || rec.State != "cancelled" || len(h.taskMsgsFor(3, task)) != 1 || len(h.eventsFor("cancelled", 3, task)) != 1 {
			t.Fatalf("record %+v republications %d", rec, len(h.taskMsgsFor(3, task))-1)
		}
	})
}
