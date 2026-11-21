package test

import (
	"encoding/json"
	"os"
	"testing"
)

// Spec: message-broker — message payloads follow publisher-owned JSON Schemas.
func TestV0048MessageBrokerMessagePayloadsFollowPublisherSchemas(t *testing.T) {
	run(t, "Publisher message violates its own schema", func(t *testing.T) {
		bad := map[string]string{
			"task.schema.json":           `{"publisher":"task-manager","publisher_seq":1,"team_id":3,"occurred_at":"2026-11-02T10:00:00.000Z","task_id":1}`,
			"command.schema.json":        `{"publisher":"task-manager","publisher_seq":1,"team_id":3,"occurred_at":"2026-11-02T10:00:00Z","control":"cancel"}`,
			"event.released.schema.json": `{"publisher":"task-manager","publisher_seq":1,"team_id":3,"occurred_at":"2026-11-02T10:00:00.000Z","kind":"released","task_id":1,"attempt":1,"claim_seq":1,"executor_id":7,"reason":"pending_timeout","republish_at":null,"lost_publisher_seq":null}`,
			"audit.cancel.schema.json":   `{"publisher":"task-manager","publisher_seq":1,"occurred_at":"2026-11-02T10:00:00.000Z","claim_token":"x"}`,
			"error.reply.schema.json":    `{"code":"Not Found","message":"x","request_id":null}`,
		}
		for schema, doc := range bad {
			if err := validateAgainst(t, "svc/task-manager/schemas/"+schema, []byte(doc)); err == nil {
				t.Fatalf("%s accepted an invalid message", schema)
			}
		}
		h := newHarness(t)
		h.seedTask(3, 1, "A") // the harness cleanup validates every published message
		// The scenario's input: a queued event that is valid except that it
		// lacks intake_id. Start from the event task-manager really published.
		h.flush()
		var queued map[string]any
		for _, m := range h.Bus.Messages() {
			if m.Subject == "task-manager.event.queued.pub" {
				if err := json.Unmarshal(m.Data, &queued); err != nil {
					t.Fatalf("queued event: %v", err)
				}
			}
		}
		if queued == nil || queued["intake_id"] == nil {
			t.Fatalf("no published queued event with intake_id: %v", queued)
		}
		if err := validateAgainst(t, "svc/task-manager/schemas/event.queued.schema.json", mustJSON(t, queued)); err != nil {
			t.Fatalf("the published queued event is invalid: %v", err)
		}
		delete(queued, "intake_id")
		if err := validateAgainst(t, "svc/task-manager/schemas/event.queued.schema.json", mustJSON(t, queued)); err == nil {
			t.Fatalf("event.queued.schema.json accepted a queued event without intake_id: %v", queued)
		}
	})

	run(t, "Consumer contract test reads the publisher schema", func(t *testing.T) {
		h := newHarness(t)
		if err := validateAgainst(t, "svc/source-controller/schemas/task.schema.json", mustJSON(t, h.intakeBody(3, 1, 1, entry(1, "A")))); err != nil {
			t.Fatalf("intake fixture: %v", err)
		}
		bad := h.intakeBody(3, 1, 1, entry(1, "A"))
		bad["tasks"] = []any{}
		if err := validateAgainst(t, "svc/source-controller/schemas/task.schema.json", mustJSON(t, bad)); err == nil {
			t.Fatalf("publisher schema accepts an empty batch; the contract test would be meaningless")
		}
	})

	run(t, "Schema change selects its consumers", func(t *testing.T) {
		raw, err := os.ReadFile(contractFixturePath(t, "qa-e2e/scripts/shared-test-inputs.json"))
		if err != nil {
			t.Fatalf("shared-test-inputs.json: %v", err)
		}
		var doc map[string]any
		if err := json.Unmarshal(raw, &doc); err != nil {
			t.Fatalf("shared-test-inputs.json: %v", err)
		}
		consumed := []string{
			"svc/source-controller/schemas/task.schema.json",
			"executor/docker_openhands/schemas/executor-event.schema.json",
			"svc/team-registry/schemas/event.team_created.schema.json",
			"svc/executor-registry/schemas/event.executor_lost.schema.json",
			"svc/flow-registry/schemas/service.targets.reply.schema.json",
		}
		found := map[string]bool{}
		var walk func(v any)
		walk = func(v any) {
			switch x := v.(type) {
			case map[string]any:
				for k, vv := range x {
					for _, c := range consumed {
						if k == c {
							for _, s := range list(vv) {
								if s == "task-manager" {
									found[c] = true
								}
							}
						}
					}
					walk(vv)
				}
			case []any:
				for _, vv := range x {
					walk(vv)
				}
			}
		}
		walk(doc)
		for _, c := range consumed {
			if !found[c] {
				t.Errorf("%s does not select task-manager's tests", c)
			}
		}
	})

	// task-manager's form for the requests it answers: requesters build their
	// contract tests on its request schemas, so an envelope the schema rejects
	// is rejected by task-manager too (the harness checks the converse: every
	// accepted request matches its schema).
	run(t, "Request violating the responder schema is rejected", func(t *testing.T) {
		h := newHarness(t)
		task, createdSeq := h.claimed(3, 7, "A")
		hdr := func(team string) map[string]string {
			return map[string]string{"team": team, "request_id": tok(h.nextRequestID())}
		}
		admin := hdr("admin")
		admin["admin_subject"] = "admin-alice"
		cases := []struct {
			name, subject, schema string
			header                map[string]string
			envelope              map[string]any
		}{
			{"claim-current without executor_id", "task-manager.service.claim-current.sub", "service.claim-current.request.schema.json", hdr("3"),
				map[string]any{"params": map[string]any{"team_id": 3, "task_id": task, "attempt": 1, "claim_seq": 1}}},
			{"executor-claims with a renamed lost_seq", "task-manager.service.executor-claims.sub", "service.executor-claims.request.schema.json", hdr("3"),
				map[string]any{"params": map[string]any{}, "body": map[string]any{"team_id": 3, "executor_id": 7, "claims": []any{}, "lost_sequence": 0}}},
			{"admin list-tasks with after in params", "task-manager.admin.list-tasks.sub", "admin.list-tasks.request.schema.json", admin,
				map[string]any{"params": map[string]any{"after": map[string]any{"ingested_at": ts(t0), "team_id": 3, "task_id": task}}}},
			{"claim-token without created_publisher_seq", "task-manager.executor.7.claim-token.sub", "executor.claim-token.request.schema.json", hdr("3"),
				map[string]any{"params": map[string]any{}, "body": map[string]any{"task_id": task, "attempt": 1, "claim_seq": 1}}},
			{"ui get-task with a string task_id", "task-manager.ui.get-task.sub", "ui.get-task.request.schema.json", h.uiHeader(3, 1),
				map[string]any{"params": map[string]any{"task_id": tok(task)}}},
		}
		for _, c := range cases {
			if err := validateAgainst(t, "svc/task-manager/schemas/"+c.schema, mustJSON(t, c.envelope)); err == nil {
				t.Fatalf("%s: %s accepts the envelope", c.name, c.schema)
			}
			if r := h.request(c.subject, c.header, c.envelope, ""); r.Status != 400 {
				t.Fatalf("%s: task-manager answered %d %v, want 400", c.name, r.Status, r.Body)
			}
		}
		// The valid form of the claim-token request is accepted.
		if r := h.claimToken(7, task, 1, 1, createdSeq); r.Status != 200 {
			t.Fatalf("valid claim-token request -> %d %v", r.Status, r.Body)
		}
	})
}
