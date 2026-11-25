package test

import (
	"context"
	"reflect"
	"testing"

	"github.com/pandops/flowai-task-manager/internal/store"
)

// fieldString reads the string field name of a struct (or pointer to one)
// by reflection, so these tests compile before the field exists and fail at
// run time while it is missing. ok is false when the field is absent.
func fieldString(v any, name string) (string, bool) {
	rv := reflect.ValueOf(v)
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return "", false
		}
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		return "", false
	}
	f := rv.FieldByName(name)
	if !f.IsValid() || f.Kind() != reflect.String {
		return "", false
	}
	return f.String(), true
}

// skipTo consumes per-team task numbers so the next task of team gets
// task_id = next. Numbering gaps are allowed (a rolled-back transaction
// consumes a number too), so this only reaches realistic ids such as 42.
func (h *harness) skipTo(team, next int64) {
	h.t.Helper()
	err := h.St.Write(context.Background(), func(tx store.Tx) error {
		for {
			n, err := tx.NextTeamID(team, store.KindTask)
			if err != nil {
				return err
			}
			if n >= next-1 {
				if n != next-1 {
					h.t.Errorf("task counter of team %d already at %d, cannot reach %d", team, n, next)
				}
				return nil
			}
		}
	})
	if err != nil {
		h.t.Fatalf("skip task counter of team %d to %d: %v", team, next, err)
	}
}

// seedTaskAt creates the task of team with task_id = id.
func (h *harness) seedTaskAt(team, id int64, external string, flows ...flowDef) int64 {
	h.t.Helper()
	h.ensureTeam(team)
	h.skipTo(team, id)
	got := h.seedTask(team, 1, external, flows...)
	if got != id {
		h.t.Fatalf("seeded %s of team %d as task %d, want %d", external, team, got, id)
	}
	return got
}

// projectedTeam returns the team projection row of team (nil when absent).
func (h *harness) projectedTeam(team int64) *store.Team {
	h.t.Helper()
	v, err := h.St.Team(context.Background(), team)
	if err != nil {
		h.t.Fatalf("read team %d: %v", team, err)
	}
	return v
}

// Spec: task-manager — task-manager owns current tasks in its own schema
// (v0050: one immutable task name per task, the team projection keeps
// team_name). Behaviour matrix M4 on the in-process store; the PostgreSQL
// constraints (NOT NULL, CHECK, unique indexes, trigger) are checked in
// TestV0048ReadsIntegration.
func TestV0050TaskManagerStoresTheTaskName(t *testing.T) {
	run(t, "Task gets its name at creation", func(t *testing.T) {
		// M4 "creation, team known" and "archive".
		h := newHarness(t)
		h.nameTeam(3, "ops")
		task := h.seedTaskAt(3, 42, "PROJ-7")
		cur := h.current(3, task)
		if name, ok := fieldString(cur, "TaskName"); !ok || name != "ops-42" {
			t.Errorf("stored current task name = %q (field present %v), want ops-42", name, ok)
		}
		if r := h.getTask(3, task); r.Status != 200 || str(r.Body["task_name"]) != "ops-42" {
			t.Errorf("get-task task_name = %d %v, want ops-42", r.Status, r.Body["task_name"])
		}
		h.ensureExecutor(3, 7)
		h.created(3, 7, task, 1, 1)
		h.running(3, 7, task, 1, 1)
		h.finished(3, 7, task, 1, 1)
		h.flush()
		rec := h.archived(3, task)
		if rec == nil || h.current(3, task) != nil {
			t.Fatalf("precondition: task %d is not archived", task)
		}
		if name, ok := fieldString(rec, "TaskName"); !ok || name != "ops-42" {
			t.Errorf("archived task name = %q (field present %v), want ops-42", name, ok)
		}
		if r := h.ui("get-archived-task", 3, 1, map[string]any{"task_id": task}); r.Status != 200 || str(r.Body["task_name"]) != "ops-42" {
			t.Errorf("get-archived-task task_name = %d %v, want ops-42", r.Status, r.Body["task_name"])
		}
	})

	run(t, "Name is built from the projection, not from the intake message", func(t *testing.T) {
		// Two teams with the same task_id get distinct names from their own
		// projection rows; the intake message carries no team name at all.
		h := newHarness(t)
		h.nameTeam(3, "ops")
		h.nameTeam(4, "qa")
		a := h.seedTask(3, 1, "A")
		b := h.seedTask(4, 1, "B")
		if a != 1 || b != 1 {
			t.Fatalf("precondition: first tasks are %d and %d, want 1 and 1", a, b)
		}
		for _, c := range []struct {
			team int64
			want string
		}{{3, "ops-1"}, {4, "qa-1"}} {
			if name, ok := fieldString(h.current(c.team, 1), "TaskName"); !ok || name != c.want {
				t.Errorf("team %d task 1 name = %q (field present %v), want %s", c.team, name, ok, c.want)
			}
		}
		// A later task of the same team gets the same prefix and its own id.
		next := h.seedTask(3, 1, "C")
		if name, _ := fieldString(h.current(3, next), "TaskName"); name != "ops-"+tok(next) {
			t.Errorf("second task of team 3 name = %q, want ops-%d", name, next)
		}
	})

	run(t, "Every team event sets team_name, never empty", func(t *testing.T) {
		// M4 "team event upsert (created, updated, archived)": each kind,
		// also as the first event a team's projection sees, stores the
		// event's team_name; a later event keeps it.
		h := newHarness(t)
		cases := []struct {
			team int64
			name string
			send func(int64, string)
		}{
			{21, "alpha", h.teamCreated},
			{22, "beta", h.teamUpdated},
			{23, "gamma", h.teamArchived},
		}
		for _, c := range cases {
			h.nameTeam(c.team, c.name)
			h.teams[c.team] = true
			c.send(c.team, imageTeam3)
			got := h.projectedTeam(c.team)
			if got == nil {
				t.Errorf("team %d: no projection row", c.team)
				continue
			}
			if name, ok := fieldString(got, "TeamName"); !ok || name != c.name {
				t.Errorf("team %d: projected team_name = %q (field present %v), want %s", c.team, name, ok, c.name)
			}
		}
		// team_created, then team_updated (image change), then team_archived:
		// the name stays set on every upsert.
		h.nameTeam(24, "delta")
		h.teams[24] = true
		h.teamCreated(24, imageTeam3)
		h.teamUpdated(24, imageNew)
		if name, _ := fieldString(h.projectedTeam(24), "TeamName"); name != "delta" {
			t.Errorf("after team_updated: team_name = %q, want delta", name)
		}
		if img := h.projectedTeam(24).DefaultImage; img != imageNew {
			t.Errorf("after team_updated: default_image = %q", img)
		}
		h.teamArchived(24, imageNew)
		if name, _ := fieldString(h.projectedTeam(24), "TeamName"); name != "delta" {
			t.Errorf("after team_archived: team_name = %q, want delta", name)
		}
		// A task created after team_updated is named from the kept name.
		h.nameTeam(25, "eps")
		h.teams[25] = true
		h.teamCreated(25, imageTeam3)
		h.teamUpdated(25, imageNew)
		task := h.seedTask(25, 1, "AFTER-UPDATE")
		if r := h.getTask(25, task); str(r.Body["task_name"]) != "eps-"+tok(task) {
			t.Errorf("task after team_updated: task_name = %v, want eps-%d", r.Body["task_name"], task)
		}
	})

	run(t, "Reads carry the task name", func(t *testing.T) {
		// Spec scenario with team 3 named ops and task 42 across all five
		// reads: list-tasks, get-task, list-executor-tasks (current, held)
		// and list-archive, get-archived-task (archived).
		h := newHarness(t)
		h.nameTeam(3, "ops")
		held := h.seedTaskAt(3, 42, "HELD")
		h.ensureExecutor(3, 7)
		h.created(3, 7, held, 1, 1)
		h.running(3, 7, held, 1, 1)
		h.flush()
		done := h.seedTask(3, 1, "DONE")
		h.cancel(3, 11, done)
		h.flush()
		if h.archived(3, done) == nil {
			t.Fatalf("precondition: task %d is not archived", done)
		}
		names := map[int64]string{held: "ops-42", done: "ops-" + tok(done)}
		rowNames := func(op string, params map[string]any, want ...int64) {
			t.Helper()
			r := h.ui(op, 3, 1, params)
			if r.Status != 200 || !sameIDs(itemIDs(r), want) {
				t.Errorf("%s = %d %v, want %v", op, r.Status, itemIDs(r), want)
				return
			}
			for _, it := range list(r.Body["items"]) {
				row := obj(it)
				if got := str(row["task_name"]); got != names[num(row["task_id"])] {
					t.Errorf("%s row %d task_name = %q, want %q", op, num(row["task_id"]), got, names[num(row["task_id"])])
				}
			}
		}
		rowNames("list-tasks", nil, done, held)
		rowNames("list-executor-tasks", map[string]any{"executor_id": 7}, held)
		rowNames("list-archive", nil, done)
		if r := h.getTask(3, held); str(r.Body["task_name"]) != "ops-42" {
			t.Errorf("get-task card task_name = %v", r.Body["task_name"])
		}
		if r := h.getTask(3, done); str(r.Body["task_name"]) != names[done] {
			t.Errorf("get-task of an archived task: task_name = %v", r.Body["task_name"])
		}
		if r := h.ui("get-archived-task", 3, 1, map[string]any{"task_id": done}); str(r.Body["task_name"]) != names[done] {
			t.Errorf("get-archived-task card task_name = %v", r.Body["task_name"])
		}
	})
}
