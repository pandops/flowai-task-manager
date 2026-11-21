package test

import (
	"testing"
	"time"
)

// Bugfix 6: ui.get-task, ui.list-attempts and ui.list-tasks serve current
// and archived tasks of the header team; ui.list-executor-tasks stays
// current-only and ui.list-archive / ui.get-archived-task stay archive-only.
// The cases run on the in-process store here and on PostgreSQL in
// v0048_reads_archived_tasks_integration_test.go.
func TestV0048ReadsArchivedTasks(t *testing.T) {
	archivedReadCases(t, newHarness)
}

func archivedReadCases(t *testing.T, mk func(*testing.T) *harness) {
	run(t, "get-task reads a finished and archived task", func(t *testing.T) {
		h := mk(t)
		task := h.runningTask(3, 7, "DONE")
		h.finished(3, 7, task, 1, 1)
		h.flush()
		if h.current(3, task) != nil || h.archived(3, task) == nil {
			t.Fatalf("precondition: task %d is not archived", task)
		}
		r := h.getTask(3, task)
		atts := list(r.Body["attempts"])
		if r.Status != 200 || num(r.Body["task_id"]) != task || str(r.Body["current_state"]) != "finished" || r.Body["archived"] != true ||
			r.Body["completed_at"] == nil || num(r.Body["executor_id"]) != 7 || len(atts) != 1 {
			t.Fatalf("get-task of an archived task = %d %v", r.Status, r.Body)
		}
		a1 := obj(atts[0])
		claims := list(a1["claims"])
		if str(a1["state"]) != "finished" || len(claims) != 1 || num(obj(claims[0])["executor_id"]) != 7 || str(obj(claims[0])["resolved_image"]) != imageTeam3 {
			t.Fatalf("archived attempts = %v", atts)
		}
		la := h.ui("list-attempts", 3, 1, map[string]any{"task_id": task})
		if la.Status != 200 || len(list(la.Body["items"])) != 1 {
			t.Fatalf("list-attempts of an archived task = %d %v", la.Status, la.Body)
		}
		ga := h.ui("get-archived-task", 3, 1, map[string]any{"task_id": task})
		if ga.Status != 200 || str(ga.Body["current_state"]) != "finished" || ga.Body["archived"] != true {
			t.Fatalf("get-archived-task = %d %v", ga.Status, ga.Body)
		}
		// v0050: the archive keeps the task name in both cards.
		for op, card := range map[string]reply{"get-task": r, "get-archived-task": ga} {
			if got, want := str(card.Body["task_name"]), h.taskName(3, task); got != want {
				t.Errorf("%s of an archived task: task_name = %q, want %q", op, got, want)
			}
		}
	})

	run(t, "get-task carries the final reason of an archived task", func(t *testing.T) {
		h := mk(t)
		failed := h.runningTask(3, 7, "FAIL")
		h.failed(3, 7, failed, 1, 1, "image_missing", false)
		cancelled := h.seedTask(3, 1, "CANCEL")
		if c := h.cancel(3, 11, cancelled); c.Status != 200 {
			t.Fatalf("cancel = %d %v", c.Status, c.Body)
		}
		h.flush()
		if h.archived(3, failed) == nil || h.archived(3, cancelled) == nil {
			t.Fatalf("precondition: failed and cancelled tasks are not archived")
		}
		f := h.getTask(3, failed)
		if f.Status != 200 || str(f.Body["current_state"]) != "failed" || str(f.Body["final_reason"]) != "image_missing" || f.Body["archived"] != true {
			t.Fatalf("get-task of a failed archived task = %d %v", f.Status, f.Body)
		}
		if c := h.getTask(3, cancelled); c.Status != 200 || str(c.Body["current_state"]) != "cancelled" || c.Body["archived"] != true {
			t.Fatalf("get-task of a cancelled archived task = %d %v", c.Status, c.Body)
		}
		if cur := h.getTask(3, h.runningTask(3, 7, "LIVE")); cur.Status != 200 || cur.Body["archived"] != false {
			t.Fatalf("get-task of a current task = %d %v", cur.Status, cur.Body)
		}
	})

	run(t, "list-tasks shows current and archived tasks together", func(t *testing.T) {
		h := mk(t)
		fin := h.runningTask(3, 7, "FIN")
		h.finished(3, 7, fin, 1, 1)
		h.advance(time.Second)
		queued := h.seedTask(3, 1, "QUEUED")
		h.advance(time.Second)
		failed := h.runningTask(3, 7, "FAILED")
		h.failed(3, 7, failed, 1, 1, "image_missing", false)
		h.advance(time.Second)
		running := h.runningTask(3, 7, "RUNNING")
		h.advance(time.Second)
		cancelled := h.seedTask(3, 1, "CANCELLED")
		h.cancel(3, 11, cancelled)
		h.flush()
		for _, id := range []int64{fin, failed, cancelled} {
			if h.archived(3, id) == nil || h.current(3, id) != nil {
				t.Fatalf("precondition: task %d is not archived", id)
			}
		}
		all := h.ui("list-tasks", 3, 1, nil)
		if want := []int64{cancelled, running, failed, queued, fin}; all.Status != 200 || !sameIDs(itemIDs(all), want) || all.Body["next_cursor"] != nil {
			t.Fatalf("list-tasks = %v, want %v", itemIDs(all), want)
		}
		rowTaskNames(t, h, "list-tasks", all) // v0050: current and archived rows
		states := map[int64]string{}
		for _, it := range list(all.Body["items"]) {
			states[num(obj(it)["task_id"])] = str(obj(it)["current_state"])
		}
		if states[fin] != "finished" || states[failed] != "failed" || states[cancelled] != "cancelled" || states[queued] != "queued" || states[running] != "running" {
			t.Fatalf("list states = %v", states)
		}
		for status, want := range map[string][]int64{"finished": {fin}, "failed": {failed}, "cancelled": {cancelled}, "queued": {queued}, "running": {running}} {
			if r := h.ui("list-tasks", 3, 1, map[string]any{"status": status}); r.Status != 200 || !sameIDs(itemIDs(r), want) {
				t.Fatalf("status=%s -> %v, want %v", status, itemIDs(r), want)
			}
		}
		filtered := map[string]any{"status": "finished", "flow_id": 2, "source_id": 1, "tag": "openhands", "search": "fin", "period": "day",
			"date": "2026-11-02", "time_zone": "UTC"}
		if r := h.ui("list-tasks", 3, 1, filtered); r.Status != 200 || !sameIDs(itemIDs(r), []int64{fin}) {
			t.Fatalf("filtered archived = %d %v", r.Status, r.Body)
		}
		if r := h.ui("list-tasks", 3, 1, map[string]any{"status": "finished", "flow_id": 999}); r.Status != 200 || len(itemIDs(r)) != 0 {
			t.Fatalf("flow filter over the archive = %v", itemIDs(r))
		}
		if r := h.ui("list-tasks", 3, 1, map[string]any{"status": "finished", "date": "2026-11-01"}); r.Status != 200 || len(itemIDs(r)) != 0 {
			t.Fatalf("date filter over the archive = %v", itemIDs(r))
		}
		if r := h.ui("list-tasks", 3, 1, map[string]any{"search": "7"}); r.Status != 200 || !sameIDs(itemIDs(r), []int64{running, failed, fin}) {
			t.Fatalf("Executor search over current and archive = %v", itemIDs(r))
		}
	})

	run(t, "list-tasks pages the union without skips or duplicates", func(t *testing.T) {
		h := mk(t)
		var want []int64 // descending order
		for i := 0; i < 12; i++ {
			h.advance(time.Second)
			want = append([]int64{h.runningTask(3, 7, "P"+tok(int64(i)))}, want...)
		}
		// Before the traversal: tasks of both future pages are archived.
		for _, id := range []int64{want[1], want[4], want[11]} {
			h.finished(3, 7, id, 1, 1)
		}
		h.flush()
		p1 := h.ui("list-tasks", 3, 1, nil)
		if p1.Status != 200 || !sameIDs(itemIDs(p1), want[:10]) || p1.Body["next_cursor"] == nil {
			t.Fatalf("page 1 = %v, want %v", itemIDs(p1), want[:10])
		}
		// Between pages: one task already served and one still ahead move
		// from current to the archive.
		h.finished(3, 7, want[2], 1, 1)
		h.failed(3, 7, want[10], 1, 1, "image_missing", false)
		h.flush()
		if h.archived(3, want[10]) == nil || h.archived(3, want[2]) == nil {
			t.Fatalf("precondition: tasks did not move to the archive")
		}
		p2 := h.ui("list-tasks", 3, 1, map[string]any{"cursor": str(p1.Body["next_cursor"])})
		if p2.Status != 200 || !sameIDs(itemIDs(p2), want[10:]) || p2.Body["next_cursor"] != nil {
			t.Fatalf("page 2 = %v, want %v", itemIDs(p2), want[10:])
		}
		fresh := h.ui("list-tasks", 3, 1, map[string]any{"limit": 25})
		if !sameIDs(itemIDs(fresh), want) {
			t.Fatalf("one page = %v, want %v", itemIDs(fresh), want)
		}
		fin := h.ui("list-tasks", 3, 1, map[string]any{"status": "finished"})
		if wantFin := []int64{want[1], want[2], want[4], want[11]}; !sameIDs(itemIDs(fin), wantFin) {
			t.Fatalf("finished = %v, want %v", itemIDs(fin), wantFin)
		}
	})

	run(t, "foreign archived task stays invisible", func(t *testing.T) {
		h := mk(t)
		own := h.runningTask(3, 7, "OWN")
		h.finished(3, 7, own, 1, 1)
		h.ensureTeam(4)
		// Task IDs are per team: two padding tasks give the foreign task an
		// ID that team 3 does not have.
		h.seedTask(4, 1, "PAD-1")
		h.seedTask(4, 1, "PAD-2")
		foreign := h.runningTask(4, 9, "FOREIGN")
		h.finished(4, 9, foreign, 1, 1)
		h.flush()
		if h.archived(4, foreign) == nil || h.archived(3, foreign) != nil || h.current(3, foreign) != nil {
			t.Fatalf("precondition: foreign task %d is not archived or its ID exists in team 3", foreign)
		}
		for _, params := range []map[string]any{nil, {"status": "finished"}, {"search": "FOREIGN"}, {"search": "9"}} {
			r := h.ui("list-tasks", 3, 1, params)
			for _, it := range list(r.Body["items"]) {
				if num(obj(it)["team_id"]) != 3 {
					t.Fatalf("list-tasks %v leaked a foreign row: %v", params, it)
				}
			}
		}
		if r := h.ui("list-tasks", 3, 1, map[string]any{"status": "finished"}); !sameIDs(itemIDs(r), []int64{own}) {
			t.Fatalf("team 3 finished = %v", itemIDs(r))
		}
		for _, op := range []string{"get-task", "list-attempts", "get-archived-task"} {
			f := h.ui(op, 3, 1, map[string]any{"task_id": foreign})
			u := h.ui(op, 3, 1, map[string]any{"task_id": 999})
			if f.Status != 404 || u.Status != 404 || str(f.Body["code"]) != "not_found" || str(f.Body["message"]) != str(u.Body["message"]) {
				t.Fatalf("%s foreign %d %v vs unknown %d %v", op, f.Status, f.Body, u.Status, u.Body)
			}
		}
	})

	run(t, "Executor tasks and the archive list keep their scope", func(t *testing.T) {
		h := mk(t)
		held := h.runningTask(3, 7, "HELD")
		done := h.runningTask(3, 7, "DONE")
		h.finished(3, 7, done, 1, 1)
		h.flush()
		if r := h.ui("list-executor-tasks", 3, 1, map[string]any{"executor_id": 7}); r.Status != 200 || !sameIDs(itemIDs(r), []int64{held}) {
			t.Fatalf("list-executor-tasks = %v, want only %d", itemIDs(r), held)
		} else {
			rowTaskNames(t, h, "list-executor-tasks", r) // v0050
		}
		if r := h.ui("list-executor-tasks", 3, 1, map[string]any{"executor_id": 7, "status": "finished"}); r.Status != 200 || len(itemIDs(r)) != 0 {
			t.Fatalf("list-executor-tasks finished = %v", itemIDs(r))
		}
		if r := h.ui("list-archive", 3, 1, nil); r.Status != 200 || !sameIDs(itemIDs(r), []int64{done}) {
			t.Fatalf("list-archive = %v, want only %d", itemIDs(r), done)
		} else {
			rowTaskNames(t, h, "list-archive", r) // v0050
		}
		// v0050: list-archive and list-executor-tasks keep the broad search;
		// a full task name is not a name match there.
		if r := h.ui("list-archive", 3, 1, map[string]any{"search": h.taskName(3, done)}); r.Status != 200 || len(itemIDs(r)) != 0 {
			t.Fatalf("list-archive search=%s = %v, want the broad search result (none)", h.taskName(3, done), itemIDs(r))
		}
		if r := h.ui("list-executor-tasks", 3, 1, map[string]any{"executor_id": 7, "search": h.taskName(3, held)}); r.Status != 200 || len(itemIDs(r)) != 0 {
			t.Fatalf("list-executor-tasks search=%s = %v, want the broad search result (none)", h.taskName(3, held), itemIDs(r))
		}
		if r := h.ui("get-archived-task", 3, 1, map[string]any{"task_id": held}); r.Status != 404 {
			t.Fatalf("get-archived-task of a current task = %d", r.Status)
		}
	})
}
