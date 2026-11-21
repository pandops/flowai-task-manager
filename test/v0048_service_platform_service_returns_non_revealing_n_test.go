package test

import "testing"

// Spec: service-platform — service returns non-revealing not-found.
// "Operator writes a foreign environment" has task-manager's form
// "operator cancels a foreign task" (the environment is env-registry's).
func TestV0048ServicePlatformServiceReturnsNonRevealingNotFound(t *testing.T) {
	run(t, "Operator reads a foreign task", func(t *testing.T) {
		h := newHarness(t)
		foreign := h.seedTask(4, 1, "A")
		h.ensureTeam(3)
		done := h.runningTask(4, 9, "DONE")
		h.finished(4, 9, done, 1, 1)
		for _, op := range []string{"get-task", "list-attempts", "get-archived-task"} {
			for _, id := range []int64{foreign, done} {
				f := h.ui(op, 3, 1, map[string]any{"task_id": id})
				u := h.ui(op, 3, 1, map[string]any{"task_id": 999})
				if f.Status != 404 || u.Status != 404 || str(f.Body["code"]) != "not_found" || str(f.Body["message"]) != str(u.Body["message"]) {
					t.Fatalf("%s %d: %d %v vs %v", op, id, f.Status, f.Body, u.Body)
				}
			}
		}
	})

	run(t, "Operator writes a foreign environment", func(t *testing.T) {
		h := newHarness(t)
		foreign := h.seedTask(4, 1, "A")
		h.ensureTeam(3)
		f := h.cancel(3, 11, foreign)
		u := h.cancel(3, 11, 999)
		if f.Status != 404 || u.Status != 404 || str(f.Body["message"]) != str(u.Body["message"]) || h.current(4, foreign).State != "queued" {
			t.Fatalf("foreign cancel = %d %v", f.Status, f.Body)
		}
	})
}
