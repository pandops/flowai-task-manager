package test

import "testing"

// Spec: service-platform — service trusts team context only from verified headers.
func TestV0048ServicePlatformServiceTrustsTeamContextOnlyFromHeaders(t *testing.T) {
	run(t, "Gateway forwards verified operator context", func(t *testing.T) {
		h := newHarness(t)
		task := h.seedTask(3, 1, "A")
		r := h.request("task-manager.ui.get-task.sub", map[string]string{"team": "3", "operator_id": "11", "request_id": "5", "team_name": "anything"},
			map[string]any{"params": map[string]any{"task_id": task}}, "ui.get-task.reply.schema.json")
		if r.Status != 200 || num(r.Body["team_id"]) != 3 {
			t.Fatalf("verified context = %d %v", r.Status, r.Body)
		}
	})

	run(t, "Payload names another team", func(t *testing.T) {
		h := newHarness(t)
		task := h.seedTask(4, 1, "A")
		h.ensureTeam(3)
		reads := h.Store.Reads
		r := h.ui("get-task", 3, 1, map[string]any{"task_id": task, "team_id": 4})
		if r.Status != 400 || str(r.Body["code"]) != "invalid_request" || h.Store.Reads != reads {
			t.Fatalf("params team 4 under header team 3 = %d %v", r.Status, r.Body)
		}
		same := h.ui("list-tasks", 4, 1, map[string]any{"team_id": 4})
		if same.Status != 200 {
			t.Fatalf("matching params team rejected: %d", same.Status)
		}
	})

	run(t, "Context is missing", func(t *testing.T) {
		h := newHarness(t)
		task := h.seedTask(3, 1, "A")
		env := map[string]any{"params": map[string]any{"task_id": task}}
		for name, hdr := range map[string]map[string]string{
			"no team":       {"operator_id": "1", "request_id": "1"},
			"no operator":   {"team": "3", "request_id": "1"},
			"no request id": {"team": "3", "operator_id": "1"},
			"admin team":    {"team": "admin", "operator_id": "1", "request_id": "1"},
			"bad team":      {"team": "03", "operator_id": "1", "request_id": "1"},
			"bad operator":  {"team": "3", "operator_id": "x", "request_id": "1"},
		} {
			r := h.request("task-manager.ui.get-task.sub", hdr, env, "")
			if r.Status != 400 || str(r.Body["code"]) != "invalid_request" {
				t.Fatalf("%s -> %d %v", name, r.Status, r.Body)
			}
			// D12 owner decision 2026-11-09: no valid request_id, no
			// request_id field; every other error keeps it.
			rid, has := r.Body["request_id"]
			if _, sent := hdr["request_id"]; sent != has || (sent && num(rid) != 1) {
				t.Fatalf("%s -> %v, want request_id present = %v", name, r.Body, sent)
			}
		}
		for _, bad := range []string{"0", "-1", "01", "r-1", "9223372036854775808"} {
			r := h.request("task-manager.ui.get-task.sub", map[string]string{"team": "3", "operator_id": "1", "request_id": bad}, env, "")
			if _, has := r.Body["request_id"]; r.Status != 400 || str(r.Body["code"]) != "invalid_request" || has {
				t.Fatalf("request_id %q -> %d %v, want 400 invalid_request without request_id", bad, r.Status, r.Body)
			}
		}
		if r := h.request("task-manager.admin.list-tasks.sub", map[string]string{"team": "3", "admin_subject": "a", "request_id": "1"}, map[string]any{"params": map[string]any{}}, ""); r.Status != 400 || str(r.Body["code"]) != "invalid_request" {
			t.Fatalf("admin request with a decimal team -> %d", r.Status)
		}
		if r := h.request("task-manager.admin.list-tasks.sub", map[string]string{"team": "admin", "request_id": "1"}, map[string]any{"params": map[string]any{}}, ""); r.Status != 400 {
			t.Fatalf("admin request without admin_subject -> %d", r.Status)
		}
		if r := h.request("task-manager.service.claim-current.sub", map[string]string{"team": "admin", "request_id": "1"},
			map[string]any{"params": map[string]any{"team_id": 3, "task_id": task, "attempt": 1, "claim_seq": 1, "executor_id": 7}}, ""); r.Status != 400 {
			t.Fatalf("service request with team admin -> %d", r.Status)
		}
	})
}
