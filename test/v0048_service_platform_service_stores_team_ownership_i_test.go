package test

import "testing"

// Spec: service-platform — service stores team ownership in every record.
func TestV0048ServicePlatformServiceStoresTeamOwnership(t *testing.T) {
	run(t, "Team-owned parent keeps its team", func(t *testing.T) {
		h := newHarness(t)
		task := h.runningTask(3, 7, "A")
		h.finished(3, 7, task, 1, 1)
		rec := h.archived(3, task)
		if rec == nil || rec.TeamID != 3 {
			t.Fatalf("archive lost its team: %+v", rec)
		}
		for _, e := range h.eventsFor("archived", 3, task) {
			if num(e["team_id"]) != 3 {
				t.Fatalf("event team %v", e["team_id"])
			}
		}

		// Writes that would move a stored task to team 4 change nothing.
		// The team of an Executor event comes from executor_id (design:
		// header and envelope are ignored), so a team-3 Executor's event
		// that names team 4 applies to team 3's task and keeps team 3.
		q := h.seedTask(3, 1, "Q")
		h.ensureTeam(4)
		h.ensureExecutor(4, 9)
		h.created(4, 9, q, 1, 1)                                             // team-4 Executor claims it
		h.exec(4, 9, "created", q, 1, 1, map[string]any{"header_team": "3"}) // header names team 3
		if r := h.cancel(4, 1, q); r.Status != 404 {
			t.Fatalf("team-4 cancel of a team-3 task: %d", r.Status)
		}
		h.exec(3, 7, "created", q, 1, 1, map[string]any{"team_id": 4}) // body names team 4
		h.flush()
		cur := h.current(3, q)
		if cur == nil || cur.TeamID != 3 {
			t.Fatalf("team-3 task changed team: %+v", cur)
		}
		for _, c := range cur.CurrentAttempt().Claims {
			if c.ExecutorID != 7 {
				t.Fatalf("team-4 Executor %d claimed a team-3 task", c.ExecutorID)
			}
		}
		if n := len(h.currentOf(4)); n != 0 || len(h.taskMsgsFor(4, q)) != 0 {
			t.Fatalf("team 4 gained %d tasks, %d task messages", n, len(h.taskMsgsFor(4, q)))
		}
		for _, e := range h.events("claim_accepted") {
			if num(e["team_id"]) != 3 || num(e["executor_id"]) == 9 {
				t.Fatalf("claim_accepted %v", e)
			}
		}
	})

	run(t, "Child references a foreign-team parent", func(t *testing.T) {
		h := newHarness(t)
		task := h.seedTask(4, 1, "A")
		h.ensureTeam(3)
		h.ensureExecutor(3, 7)
		h.created(3, 7, task, 1, 1) // team-3 Executor names a team-4 task
		if cur := h.current(4, task); cur.State != "queued" || len(cur.Attempts[0].Claims) != 0 {
			t.Fatalf("foreign-team claim stored: %+v", cur.Attempts)
		}
	})

	run(t, "Display metadata never authorizes", func(t *testing.T) {
		h := newHarness(t)
		task := h.seedTask(4, 1, "A")
		h.ensureTeam(3)
		hdr := h.uiHeader(3, 1)
		hdr["team_name"] = "team-4"
		if r := h.request("task-manager.ui.get-task.sub", hdr, map[string]any{"params": map[string]any{"task_id": task}}, ""); r.Status != 404 {
			t.Fatalf("team_name granted access: %d", r.Status)
		}
	})
}
