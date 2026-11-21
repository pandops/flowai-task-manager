package test

import (
	"testing"

	"github.com/pandops/flowai-task-manager/internal/ids"
)

// Spec: service-platform — services use integer identifiers.
func TestV0048ServicePlatformServicesUseIntegerIdentifiers(t *testing.T) {
	run(t, "Invalid identifier is rejected", func(t *testing.T) {
		for _, bad := range []string{"", "0", "-1", "+1", "01", "1.5", "1e3", "abc", "9223372036854775808", "3f2c9a1e-7b44-4c1d-9e2a-5d6f7a8b9c0d", " 1"} {
			if _, err := ids.ParseToken(bad); err == nil {
				t.Fatalf("token %q accepted", bad)
			}
		}
		for _, bad := range []string{`"1"`, `0`, `-1`, `1.0`, `1e0`, `null`, `true`, `9223372036854775808`} {
			if _, err := ids.ParseJSON([]byte(bad)); err == nil {
				t.Fatalf("JSON %s accepted", bad)
			}
		}
		if v, err := ids.ParseJSON([]byte("9223372036854775807")); err != nil || v != 9223372036854775807 {
			t.Fatalf("max int64 rejected")
		}
		h := newHarness(t)
		h.seedTask(3, 1, "A")
		h.ensureExecutor(3, 7)
		if r := h.claimCurrent(3, 1, 1, 1, 7); r.Status != 200 {
			t.Fatalf("valid claim-current = %d", r.Status)
		}
		reads := h.Store.Reads
		r := h.request("task-manager.service.claim-current.sub", map[string]string{"team": "3", "request_id": "1"},
			map[string]any{"params": map[string]any{"team_id": 3, "task_id": "1", "attempt": 1, "claim_seq": 1, "executor_id": 7}}, "")
		if r.Status != 400 || str(r.Body["code"]) != "invalid_request" || h.Store.Reads != reads {
			t.Fatalf("string task_id = %d %v", r.Status, r.Body)
		}
		if r := h.request("task-manager.executor.07.claim-token.sub", map[string]string{"team": "3"}, map[string]any{"params": map[string]any{},
			"body": map[string]any{"task_id": 1, "attempt": 1, "claim_seq": 1, "created_publisher_seq": 1}}, ""); r.Status != 400 {
			t.Fatalf("subject executor token 07 = %d", r.Status)
		}
	})

	run(t, "First record in each team is 1", func(t *testing.T) {
		h := newHarness(t)
		a := h.seedTask(3, 1, "A")
		b := h.seedTask(4, 1, "B")
		ca := h.cancel(3, 11, a)
		cb := h.cancel(4, 12, b)
		if a != 1 || b != 1 || num(ca.Body["control_id"]) != 1 || num(cb.Body["control_id"]) != 1 {
			t.Fatalf("tasks %d %d controls %v %v", a, b, ca.Body, cb.Body)
		}
	})

	run(t, "Equal numbers in two teams grant nothing", func(t *testing.T) {
		h := newHarness(t)
		h.seedTask(3, 1, "A")
		b := h.seedTask(4, 1, "B")
		if r := h.cancel(3, 11, b); r.Status != 200 || h.current(4, b) == nil {
			t.Fatalf("cancel (3,1) touched (4,1): %d", r.Status)
		}
		if r := h.claimCurrent(4, 1, 1, 1, 7); r.Body["current"] != false {
			t.Fatalf("claim-current crossed teams")
		}
	})

	run(t, "Concurrent creates get unique increasing numbers", func(t *testing.T) {
		h := newHarness(t)
		var last int64
		for i := 0; i < 5; i++ {
			id := h.seedTask(3, 1, "C"+tok(int64(i)))
			if id <= last {
				t.Fatalf("id %d after %d", id, last)
			}
			last = id
		}
	})

	run(t, "Rolled-back transaction neither publishes nor reuses its sequence", func(t *testing.T) {
		h := newHarness(t)
		h.teamCreated(3, imageTeam3)
		h.bind(3, 1, flow(2))
		h.Store.Hook = func(step string) error {
			if step == "append_outbox" {
				return errTest
			}
			return nil
		}
		h.intake(3, 1, entry(1, "A"))
		h.Store.Hook = nil
		h.flush()
		if len(h.Bus.Messages()) != 0 {
			t.Fatalf("rolled-back transaction published")
		}
		h.intake(3, 1, entry(2, "B"))
		h.flush()
		task := h.currentOf(3)[0].TaskID
		if task == 1 {
			t.Fatalf("rolled-back transaction's task id reused")
		}
		if first := num(decode(h.Bus.Messages()[0].Data)["publisher_seq"]); first == 1 {
			t.Fatalf("rolled-back publisher_seq reused")
		}
	})

	run(t, "Redelivered message is deduplicated by publisher sequence", func(t *testing.T) {
		h := newHarness(t)
		h.teamCreated(3, imageTeam3)
		h.bind(3, 1, flow(2))
		h.intakeSeq(3, 1, 50, entry(1, "A"))
		h.intakeSeq(3, 1, 50, entry(1, "A"))
		if len(h.currentOf(3)) != 1 {
			t.Fatalf("redelivery created a task")
		}
	})
}
