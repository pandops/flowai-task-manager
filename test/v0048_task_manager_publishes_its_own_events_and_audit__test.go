package test

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pandops/flowai-task-manager/internal/wire"
)

// Spec: task-manager — task-manager publishes its own events and audit messages.
func TestV0048TaskManagerPublishesItsOwnEventsAndAuditMessages(t *testing.T) {
	run(t, "Retry is scheduled", func(t *testing.T) {
		h := newHarness(t)
		task := h.runningTask(3, 7, "A")
		h.failed(3, 7, task, 1, 1, "agent_error", true)
		failedAt := h.Now
		h.flush()
		ev := h.published("task-manager.event.retry_scheduled.pub")
		if len(ev) != 1 || ev[0].Header["team"] != "3" {
			t.Fatalf("retry_scheduled = %v", ev)
		}
		e := decode(ev[0].Data)
		if num(e["attempt"]) != 1 || num(e["next_attempt"]) != 2 || str(e["due_at"]) != ts(failedAt.Add(60*time.Second)) || str(e["failure_reason"]) != "agent_error" {
			t.Fatalf("event = %v", e)
		}
		if got, want := str(e["task_name"]), h.taskName(3, task); got != want { // v0050
			t.Errorf("retry_scheduled task_name = %q, want %q", got, want)
		}
	})

	// v0050: every event that carries a task_id also carries that task's
	// task_name, and each event schema requires it.
	run(t, "Every task event carries the task name", func(t *testing.T) {
		h := newHarness(t)
		h.nameTeam(3, "ops")
		a := h.runningTask(3, 7, "A")                // queued, claim_accepted
		h.failed(3, 7, a, 1, 1, "agent_error", true) // retry_scheduled
		b := h.runningTask(3, 7, "B")
		h.cancel(3, 11, b) // control_requested
		c := h.seedTask(3, 1, "C")
		h.cancel(3, 11, c) // cancelled + archived
		h.lost(3, 7, 900)  // released (executor_lost)
		h.flush()
		kinds := []string{"queued", "claim_accepted", "retry_scheduled", "released", "cancelled", "control_requested", "archived"}
		for _, kind := range kinds {
			msgs := h.published("task-manager.event." + kind + ".pub")
			if len(msgs) == 0 {
				t.Errorf("no %s event published", kind)
				continue
			}
			for _, m := range msgs {
				e := decode(m.Data)
				team, task := num(e["team_id"]), num(e["task_id"])
				want := "ops-" + tok(task)
				if team != 3 || task <= 0 {
					t.Errorf("%s: unexpected team/task %d/%d", kind, team, task)
					continue
				}
				if got := str(e["task_name"]); got != want {
					t.Errorf("%s of task %d: task_name = %q, want %q", kind, task, got, want)
				}
				// The schema must require task_name: the same event without it
				// is invalid (the harness validates the event as published).
				delete(e, "task_name")
				if err := validateAgainst(t, "svc/task-manager/schemas/"+schemaForSubject(m.Subject), mustJSON(t, e)); err == nil {
					t.Errorf("%s schema accepts an event without task_name", kind)
				}
			}
		}
		// intake_processed carries task_ids, not one task, and no task_name.
		for _, e := range h.events("intake_processed") {
			if _, ok := e["task_name"]; ok {
				t.Errorf("intake_processed carries task_name: %v", e)
			}
		}
	})

	run(t, "Published message matches its schema", func(t *testing.T) {
		h := newHarness(t)
		a := h.runningTask(3, 7, "A")
		h.failed(3, 7, a, 1, 1, "agent_error", true) // retry_scheduled
		b := h.runningTask(3, 7, "B")
		h.cancel(3, 11, b) // control_requested + command + audit
		c := h.seedTask(3, 1, "C")
		h.cancel(3, 11, c) // cancelled + archived
		h.lost(3, 7, 900)  // released
		h.admin(map[string]any{}, nil)
		h.flush()
		seen := map[string]bool{}
		for _, m := range h.Bus.Messages() {
			seen[schemaForSubject(m.Subject)] = true
		}
		entries, _ := os.ReadDir("../schemas")
		for _, e := range entries {
			name := e.Name()
			if strings.HasPrefix(name, "event.") || strings.HasPrefix(name, "audit.") || name == "task.schema.json" || name == "command.schema.json" {
				if !seen[name] {
					t.Errorf("no published message exercised %s", name)
				}
			}
		}
		// The harness cleanup validates each message against its schema; a
		// message that breaks its schema must be detected.
		bad := []byte(`{"publisher":"task-manager","publisher_seq":1,"team_id":3,"occurred_at":"2026-11-02T10:00:00.000Z","kind":"queued","task_id":0}`)
		if err := validateAgainst(t, "svc/task-manager/schemas/event.queued.schema.json", bad); err == nil {
			t.Fatalf("schema accepted an invalid queued event")
		}
	})

	run(t, "Consumer contract reads the publisher's schema", func(t *testing.T) {
		h := newHarness(t)
		check := func(schema string, body map[string]any) {
			t.Helper()
			if err := validateAgainst(t, schema, mustJSON(t, body)); err != nil {
				t.Errorf("fixture violates %s: %v", schema, err)
			}
		}
		check("svc/source-controller/schemas/task.schema.json", h.intakeBody(3, 1, 5, entry(1, "A"), entry(2, "B")))
		poll := h.intakeBody(3, 1, 6, entry(3, "C"))
		poll["origin"], poll["poll_id"], poll["page"] = "poll", 4, 1
		check("svc/source-controller/schemas/task.schema.json", poll)
		team := map[string]any{"publisher": "team-registry", "publisher_seq": 1, "kind": "team_created", "occurred_at": ts(h.Now), "team_id": 3,
			"team_name": "team-3", "default_image": imageTeam3, "ingested_at": ts(h.Now), "archived_at": nil}
		check("svc/team-registry/schemas/event.team_created.schema.json", team)
		for _, k := range []string{"executor_registered", "executor_removed"} {
			check("svc/executor-registry/schemas/event."+k+".schema.json", map[string]any{"publisher": "executor-registry", "publisher_seq": 2, "team_id": 3,
				"occurred_at": ts(h.Now), "kind": k, "executor_id": 7, "execution_tag": "openhands", "executor_type": "executor_docker_openhands"})
		}
		check("svc/executor-registry/schemas/event.executor_lost.schema.json", map[string]any{"publisher": "executor-registry", "publisher_seq": 3, "team_id": 3,
			"occurred_at": ts(h.Now), "kind": "executor_lost", "executor_id": 7, "last_heartbeat_at": ts(h.Now)})
		ev := func(kind string, extra map[string]any) map[string]any {
			m := map[string]any{"publisher": "executor", "publisher_seq": 9, "team_id": 3, "executor_id": 7, "kind": kind, "occurred_at": ts(h.Now), "task_id": 1, "attempt": 1, "claim_seq": 1}
			for k, v := range extra {
				m[k] = v
			}
			return m
		}
		for _, e := range []map[string]any{ev("created", nil), ev("pending", map[string]any{"reason": "budget_exhausted"}),
			ev("released", map[string]any{"reason": "pending_timeout"}), ev("running", map[string]any{"image_source": "team_default"}), ev("finished", nil),
			ev("failed", map[string]any{"failure_reason": "image_missing", "retryable": false}), ev("cancelled", map[string]any{"control_id": 1})} {
			check("executor/docker_openhands/schemas/executor-event.schema.json", e)
		}
		h.bind(3, 1, flow(2))
		rep, _ := h.respond(wire.Msg{Subject: "flow-registry.service.targets.pub", Data: mustJSON(t, map[string]any{"params": map[string]any{"team_id": 3, "source_id": 1}})})
		if err := validateAgainst(t, "svc/flow-registry/schemas/service.targets.reply.schema.json", rep.Body); err != nil {
			t.Errorf("targets reply fixture: %v", err)
		}
		check("executor/docker_openhands/schemas/claim-token-request.schema.json", map[string]any{"params": map[string]any{}, "body": map[string]any{"task_id": 1, "attempt": 1, "claim_seq": 1, "created_publisher_seq": 9}})
		for _, f := range []string{"svc/source-controller/schemas/task.schema.json", "executor/docker_openhands/schemas/executor-event.schema.json"} {
			if _, err := os.Stat(contractFixturePath(t, f)); err != nil {
				t.Fatalf("declared shared input %s missing: %v", f, err)
			}
		}
		inputs, err := os.ReadFile(contractFixturePath(t, "qa-e2e/scripts/shared-test-inputs.json"))
		if err != nil || !strings.Contains(string(inputs), "svc/source-controller/schemas/task.schema.json") {
			t.Fatalf("source-controller's intake schema is not a declared shared test input: %v", err)
		}
	})

	run(t, "Outbox sequence is allocated in the state transaction", func(t *testing.T) {
		h := newHarness(t)
		a := h.runningTask(3, 7, "A")
		h.runningTask(3, 8, "B")
		h.Store.Hook = func(step string) error {
			if step == "append_outbox" {
				return errTest
			}
			return nil
		}
		h.lost(3, 7, 900) // rolls back after allocating its publisher_seq
		h.Store.Hook = nil
		if cur := h.current(3, a); cur.State != "running" {
			t.Fatalf("rolled-back release changed state")
		}
		h.lost(3, 8, 901)
		h.lost(3, 7, 900) // redelivery now commits
		h.flush()
		var last int64
		for _, m := range h.Bus.Messages() {
			seq := num(decode(m.Data)["publisher_seq"])
			if seq <= last {
				t.Fatalf("publisher_seq not increasing: %d after %d", seq, last)
			}
			last = seq
		}
		if rel := h.events("released"); len(rel) != 2 {
			t.Fatalf("released events = %d", len(rel))
		}
		if all := h.Bus.Messages(); last <= int64(len(all)) {
			t.Fatalf("no gap: the rolled-back transaction's publisher_seq was reused (max %d, %d messages)", last, len(all))
		}
	})
}
