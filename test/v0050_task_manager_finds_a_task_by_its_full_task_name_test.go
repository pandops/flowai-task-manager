package test

import (
	"testing"
)

// nameSearchFixture is team 3 named ops, Executor 7, and team 5 named qa:
//
//	ops 42  finished (archived), external ID PROJ-7
//	ops 142 running, held by Executor 7, external ID OPS-42
//	ops 420 queued, external ID X-420
//	ops 990 queued, external ID OPS-990
//	ops 991 queued, external ID OPS-042-X
//	ops 992 queued, external ID OPS-9223372036854775808
//	ops 993 queued, Flow 12, external ID F-FLOW
//	qa  42  queued, external ID QA-ONLY
//
// No team 3 field contains "qa-42" and no team 3 task_id, flow_id,
// source_id or Executor ID contains "12" except Flow 12.
type nameSearchFixture struct {
	h                                            *harness
	t42, t142, t420, t990, t991, t992, t993, q42 int64
}

func newNameSearchFixture(t *testing.T) nameSearchFixture {
	t.Helper()
	h := newHarness(t)
	h.nameTeam(3, "ops")
	h.nameTeam(5, "qa")
	f := nameSearchFixture{h: h}
	h.ensureExecutor(3, 7)
	f.t42 = h.seedTaskAt(3, 42, "PROJ-7")
	h.created(3, 7, f.t42, 1, 1)
	h.running(3, 7, f.t42, 1, 1)
	h.finished(3, 7, f.t42, 1, 1)
	f.t142 = h.seedTaskAt(3, 142, "OPS-42")
	h.created(3, 7, f.t142, 1, 1)
	h.running(3, 7, f.t142, 1, 1)
	f.t420 = h.seedTaskAt(3, 420, "X-420")
	f.t990 = h.seedTaskAt(3, 990, "OPS-990")
	f.t991 = h.seedTask(3, 1, "OPS-042-X")
	f.t992 = h.seedTask(3, 1, "OPS-9223372036854775808")
	f.t993 = h.seedTask(3, 1, "F-FLOW", flow(12))
	f.q42 = h.seedTaskAt(5, 42, "QA-ONLY")
	h.flush()
	if h.archived(3, f.t42) == nil || h.current(3, f.t142) == nil || h.current(3, f.t142).State != "running" {
		t.Fatalf("precondition: ops-42 archived and ops-142 running")
	}
	if f.t991 != 991 || f.t992 != 992 || f.t993 != 993 {
		t.Fatalf("precondition: tasks %d %d %d, want 991 992 993", f.t991, f.t992, f.t993)
	}
	if c := h.current(3, f.t993); c == nil || c.FlowID != 12 {
		t.Fatalf("precondition: task 993 belongs to Flow 12")
	}
	return f
}

// search lists tasks of team with params plus search=term and returns the
// reply after checking it never holds a foreign row.
func (f nameSearchFixture) search(t *testing.T, op string, team int64, term string, params map[string]any) reply {
	t.Helper()
	p := map[string]any{"search": term}
	for k, v := range params {
		p[k] = v
	}
	r := f.h.ui(op, team, 1, p)
	if r.Status != 200 {
		t.Fatalf("%s search=%q %v -> %d %v", op, term, params, r.Status, r.Body)
	}
	for _, it := range list(r.Body["items"]) {
		if num(obj(it)["team_id"]) != team {
			t.Fatalf("%s search=%q for team %d returned a foreign row %v", op, term, team, it)
		}
	}
	return r
}

func (f nameSearchFixture) expect(t *testing.T, op string, team int64, term string, params map[string]any, want ...int64) reply {
	t.Helper()
	r := f.search(t, op, team, term, params)
	if want == nil {
		want = []int64{}
	}
	got := itemIDs(r)
	if got == nil {
		got = []int64{}
	}
	if !sameIDs(got, want) {
		t.Errorf("%s team %d search=%q %v = %v, want %v", op, team, term, params, got, want)
	}
	return r
}

// Spec: task-manager — task-manager finds a task by its full task name.
// Behaviour matrix M2 on the in-process store; the PostgreSQL query path is
// checked in TestV0048ReadsIntegration.
func TestV0050TaskManagerFindsATaskByItsFullTaskName(t *testing.T) {
	run(t, "Full task name finds only that task", func(t *testing.T) {
		f := newNameSearchFixture(t)
		// ops-42 is a substring of task 142's external ID OPS-42; the full
		// name matches only by name.
		r := f.expect(t, "list-tasks", 3, "ops-42", nil, f.t42)
		if r.Body["next_cursor"] != nil {
			t.Errorf("exact name page has a next cursor: %v", r.Body["next_cursor"])
		}
		if items := list(r.Body["items"]); len(items) == 1 && str(obj(items[0])["task_name"]) != "ops-42" {
			t.Errorf("row task_name = %v, want ops-42", obj(items[0])["task_name"])
		}
		// A current (running) task found by name.
		f.expect(t, "list-tasks", 3, "ops-142", nil, f.t142)
		// Team 5's own name finds its own task 42.
		f.expect(t, "list-tasks", 5, "qa-42", nil, f.q42)
	})

	run(t, "Task name in another case", func(t *testing.T) {
		f := newNameSearchFixture(t)
		for _, term := range []string{"OPS-42", "Ops-42", " ops-42 ", "\tOPS-42\n"} {
			r := f.expect(t, "list-tasks", 3, term, nil, f.t42)
			if r.Body["next_cursor"] != nil {
				t.Errorf("search=%q has a next cursor", term)
			}
		}
	})

	run(t, "Task name combined with a filter", func(t *testing.T) {
		f := newNameSearchFixture(t)
		// Task 42 is finished; running holds task 142 (external ID OPS-42),
		// which must not appear either.
		f.expect(t, "list-tasks", 3, "ops-42", map[string]any{"status": "running"})
		f.expect(t, "list-tasks", 3, "ops-42", map[string]any{"status": "queued"})
		f.expect(t, "list-tasks", 3, "ops-42", map[string]any{"flow_id": 12})
		// Matching filters keep the task.
		f.expect(t, "list-tasks", 3, "ops-42", map[string]any{"status": "finished"}, f.t42)
		f.expect(t, "list-tasks", 3, "ops-42", map[string]any{"status": "finished", "flow_id": 2, "source_id": 1, "tag": "openhands",
			"period": "day", "date": "2026-11-02", "time_zone": "UTC", "limit": 25}, f.t42)
		f.expect(t, "list-tasks", 3, "OPS-142", map[string]any{"status": "running"}, f.t142)
		f.expect(t, "list-tasks", 3, "ops-142", map[string]any{"date": "2026-11-01"})
	})

	run(t, "Partial name or another team's task name does not match by name", func(t *testing.T) {
		// Scenario wording: no field of a team 3 task contains the text.
		h := newHarness(t)
		h.nameTeam(3, "ops")
		h.nameTeam(5, "qa")
		own := h.seedTaskAt(3, 42, "PROJ-7")
		h.seedTaskAt(5, 42, "QA-ONLY")
		f := nameSearchFixture{h: h}
		f.expect(t, "list-tasks", 3, "ops-4", nil)
		f.expect(t, "list-tasks", 3, "qa-42", nil)
		f.expect(t, "list-tasks", 3, "QA-42", nil)
		f.expect(t, "list-tasks", 3, "ops-42", nil, own)
	})

	run(t, "Non-canonical names keep the broad search", func(t *testing.T) {
		f := newNameSearchFixture(t)
		// Not <team_name>-<n> with a canonical positive int64 n: the broad
		// substring search over task_id, flow_id, source_id, external_id and
		// Executor ID applies, so these never return task 42 by name.
		f.expect(t, "list-tasks", 3, "ops-042", nil, f.t991)                 // leading zero: OPS-042-X only
		f.expect(t, "list-tasks", 3, "ops-0", nil, f.t991)                   // zero: OPS-042-X only
		f.expect(t, "list-tasks", 3, "ops-9223372036854775808", nil, f.t992) // int64 overflow: its external ID only
		f.expect(t, "list-tasks", 3, "qa-42", nil)                           // another team's task name
		f.expect(t, "list-tasks", 3, "ops-+42", nil)                         // sign
		f.expect(t, "list-tasks", 3, "ops 42", nil)                          // no dash
		f.expect(t, "list-tasks", 3, "OPS-", nil, f.t992, f.t991, f.t990, f.t142)
		f.expect(t, "list-tasks", 3, "PS-4", nil, f.t142)
	})

	run(t, "Name of a task that does not exist", func(t *testing.T) {
		f := newNameSearchFixture(t)
		// Task 990's external ID OPS-990 contains ops-99 and task 142's
		// external ID OPS-42 contains ops-4, but ops-99 and ops-4 are full
		// names of the team whose tasks 99 and 4 do not exist. The decision
		// depends only on the term's form and the projection (design.md
		// "Tasks search by name"), so both pages are empty.
		f.expect(t, "list-tasks", 3, "ops-99", nil)
		f.expect(t, "list-tasks", 3, "OPS-99", map[string]any{"status": "queued"})
		f.expect(t, "list-tasks", 3, "ops-4", nil)
		f.expect(t, "list-tasks", 3, "ops-9223372036854775807", nil) // largest int64, no such task
	})

	run(t, "Other search terms keep the broad search", func(t *testing.T) {
		f := newNameSearchFixture(t)
		// The Flow page searches its full flow_id.
		f.expect(t, "list-tasks", 3, "12", nil, f.t993)
		f.expect(t, "list-tasks", 3, "12", map[string]any{"flow_id": 12}, f.t993)
		// External-ID text still finds the tasks by other parts of it.
		f.expect(t, "list-tasks", 3, "PROJ", nil, f.t42)
		f.expect(t, "list-tasks", 3, "ps-9", nil, f.t992, f.t990)
		// Executor ID and task_id substrings as before.
		f.expect(t, "list-tasks", 3, "7", nil, f.t992, f.t142, f.t42) // Executor 7, PROJ-7, the overflow external ID
		f.expect(t, "list-tasks", 3, "420", nil, f.t420)
	})

	run(t, "Task-name search leaves list-archive and list-executor-tasks alone", func(t *testing.T) {
		f := newNameSearchFixture(t)
		// list-archive holds task 42 (ops-42) but none of its fields
		// contains "ops-42": the broad search finds nothing.
		f.expect(t, "list-archive", 3, "ops-42", nil)
		f.expect(t, "list-archive", 3, "PROJ", nil, f.t42)
		// Executor 7 holds task 142 (ops-142, external ID OPS-42): the
		// broad search for ops-42 finds it, ops-142 finds nothing.
		f.expect(t, "list-executor-tasks", 3, "ops-42", map[string]any{"executor_id": 7}, f.t142)
		f.expect(t, "list-executor-tasks", 3, "ops-142", map[string]any{"executor_id": 7})
	})

	run(t, "The team_name header plays no part", func(t *testing.T) {
		f := newNameSearchFixture(t)
		h := f.h
		for _, name := range []string{"qa", "", "ops"} {
			hdr := h.uiHeader(3, 1)
			if name == "" {
				delete(hdr, "team_name")
			} else {
				hdr["team_name"] = name
			}
			for term, want := range map[string][]int64{"ops-42": {f.t42}, "qa-42": {}} {
				r := h.request("task-manager.ui.list-tasks.sub", hdr, map[string]any{"params": map[string]any{"search": term}}, "ui.list-tasks.reply.schema.json")
				got := itemIDs(r)
				if got == nil {
					got = []int64{}
				}
				if r.Status != 200 || !sameIDs(got, want) {
					t.Errorf("team_name header %q search=%s = %d %v, want %v", name, term, r.Status, got, want)
				}
				if items := list(r.Body["items"]); term == "ops-42" && len(items) == 1 && str(obj(items[0])["task_name"]) != "ops-42" {
					t.Errorf("team_name header %q search=ops-42 row task_name = %v, want ops-42", name, obj(items[0])["task_name"])
				}
			}
			// The card's stored name ignores the header too.
			g := h.request("task-manager.ui.get-task.sub", hdr, map[string]any{"params": map[string]any{"task_id": f.t42}}, "ui.get-task.reply.schema.json")
			if g.Status != 200 || str(g.Body["task_name"]) != "ops-42" {
				t.Errorf("team_name header %q get-task 42 = %d task_name %v, want ops-42", name, g.Status, g.Body["task_name"])
			}
		}
	})

	run(t, "Cursor of a broad search is not valid for a name search", func(t *testing.T) {
		f := newNameSearchFixture(t)
		for i := 0; i < 11; i++ {
			f.h.seedTask(3, 1, "PAD-"+tok(int64(i)))
		}
		first := f.search(t, "list-tasks", 3, "PAD", nil)
		cur := str(first.Body["next_cursor"])
		if len(itemIDs(first)) != 10 || cur == "" {
			t.Fatalf("precondition: broad first page = %v", first.Body)
		}
		r := f.h.ui("list-tasks", 3, 1, map[string]any{"search": "ops-42", "cursor": cur})
		if r.Status != 400 || str(r.Body["code"]) != "invalid_cursor" {
			t.Errorf("broad cursor reused with a name search = %d %v", r.Status, r.Body)
		}
	})
}
