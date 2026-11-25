package test

import (
	"strings"
	"testing"
	"time"
)

func mustTime(t *testing.T, s string) time.Time {
	t.Helper()
	v, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatalf("time %q: %v", s, err)
	}
	return v
}

// datedTasks seeds one team-3 task per instant and returns their ids.
func datedTasks(t *testing.T, h *harness, instants ...string) []int64 {
	t.Helper()
	var out []int64
	for i, s := range instants {
		h.at(mustTime(t, s))
		out = append(out, h.seedTask(3, 1, "D"+tok(int64(i))))
	}
	h.at(t0)
	return out
}

// rowTaskNames checks that every row of a task list reply carries the
// stored task name <team_name>-<task_id> of its task (v0050).
func rowTaskNames(t *testing.T, h *harness, op string, r reply) {
	t.Helper()
	for _, it := range list(r.Body["items"]) {
		row := obj(it)
		if got, want := str(row["task_name"]), h.taskName(num(row["team_id"]), num(row["task_id"])); got != want {
			t.Errorf("%s row %d task_name = %q, want %q", op, num(row["task_id"]), got, want)
		}
	}
}

// schemaRequiresTaskName removes task_name from a successful reply (from
// its first row when rows is true, otherwise from the card) and checks that
// the reply schema then rejects it: the contract requires the field (v0050).
func schemaRequiresTaskName(t *testing.T, schema string, r reply, rows bool) {
	t.Helper()
	body := decode(r.Raw)
	target := body
	if rows {
		items := list(body["items"])
		if len(items) == 0 {
			t.Fatalf("%s: no row to strip task_name from", schema)
		}
		target = obj(items[0])
	}
	if _, ok := target["task_name"]; !ok {
		t.Errorf("%s: reply has no task_name to remove: %v", schema, target)
		return
	}
	delete(target, "task_name")
	if err := validateAgainst(t, "svc/task-manager/schemas/"+schema, mustJSON(t, body)); err == nil {
		t.Errorf("%s accepts a reply without task_name", schema)
	}
}

// Spec: task-manager — task-manager serves team task reads to API Gateway.
func TestV0048TaskManagerServesTeamTaskReads(t *testing.T) {
	run(t, "Teams share filter values", func(t *testing.T) {
		h := newHarness(t)
		a := h.runningTask(3, 7, "A")
		h.seedTask(3, 1, "Q")
		h.ensureTeam(4)
		h.ensureExecutor(4, 9)
		b := h.seedTask(4, 1, "B")
		h.created(4, 9, b, 1, 1)
		h.running(4, 9, b, 1, 1)
		r := h.ui("list-tasks", 3, 1, map[string]any{"status": "running"})
		if r.Status != 200 || !sameIDs(itemIDs(r), []int64{a}) || r.Body["next_cursor"] != nil {
			t.Fatalf("team 3 running = %d %v", r.Status, r.Body)
		}
		for _, it := range list(r.Body["items"]) {
			if num(obj(it)["team_id"]) != 3 {
				t.Fatalf("foreign row in page")
			}
		}
		// v0050: every row carries the stored task name.
		all := h.ui("list-tasks", 3, 1, nil)
		rowTaskNames(t, h, "list-tasks", all)
		schemaRequiresTaskName(t, "ui.list-tasks.reply.schema.json", all, true)
	})

	run(t, "Non-integer task ID is rejected", func(t *testing.T) {
		h := newHarness(t)
		h.seedTask(3, 1, "A")
		reads := h.Store.Reads
		for _, bad := range []any{"abc", 0, -5, "3f2c9a1e-7b44-4c1d-9e2a-5d6f7a8b9c0d", "1", 1.5, "+1", "01", 9223372036854775808.0} {
			for _, op := range []string{"get-task", "list-attempts", "get-archived-task", "cancel"} {
				r := h.ui(op, 3, 1, map[string]any{"task_id": bad})
				if r.Status != 400 || str(r.Body["code"]) != "invalid_request" {
					t.Fatalf("%s task_id=%v -> %d %v", op, bad, r.Status, r.Body)
				}
			}
			for _, field := range []string{"flow_id", "source_id"} {
				if r := h.ui("list-tasks", 3, 1, map[string]any{field: bad}); r.Status != 400 || str(r.Body["code"]) != "invalid_request" {
					t.Fatalf("%s=%v -> %d", field, bad, r.Status)
				}
			}
			if r := h.ui("list-executor-tasks", 3, 1, map[string]any{"executor_id": bad}); r.Status != 400 {
				t.Fatalf("executor_id=%v -> %d", bad, r.Status)
			}
		}
		if h.Store.Reads != reads {
			t.Fatalf("invalid identifiers read %d task rows", h.Store.Reads-reads)
		}
	})

	run(t, "Task card with two attempts", func(t *testing.T) {
		h := newHarness(t)
		task := h.runningTask(3, 7, "A")
		h.failed(3, 7, task, 1, 1, "agent_error", true)
		h.due(time.Minute)
		h.ensureExecutor(3, 8)
		h.created(3, 8, task, 2, 1)
		h.running(3, 8, task, 2, 1)
		r := h.getTask(3, task)
		atts := list(r.Body["attempts"])
		if r.Status != 200 || len(atts) != 2 || str(r.Body["current_state"]) != "running" || num(r.Body["executor_id"]) != 8 {
			t.Fatalf("card = %d %v", r.Status, r.Body)
		}
		a1, a2 := obj(atts[0]), obj(atts[1])
		c1, c2 := obj(list(a1["claims"])[0]), obj(list(a2["claims"])[0])
		if str(a1["failure_reason"]) != "agent_error" || a1["retryable"] != true || num(c1["executor_id"]) != 7 || num(c2["executor_id"]) != 8 ||
			str(c1["resolved_image"]) != imageTeam3 || str(c2["image_source"]) != "team_default" || str(a2["state"]) != "running" {
			t.Fatalf("attempts = %v", atts)
		}
		la := h.ui("list-attempts", 3, 1, map[string]any{"task_id": task})
		if la.Status != 200 || len(list(la.Body["items"])) != 2 {
			t.Fatalf("list-attempts = %d %v", la.Status, la.Body)
		}
		// v0050: the task card carries the stored task name.
		if got, want := str(r.Body["task_name"]), h.taskName(3, task); got != want {
			t.Errorf("get-task task_name = %q, want %q", got, want)
		}
		schemaRequiresTaskName(t, "ui.get-task.reply.schema.json", r, false)
	})

	run(t, "Archived task is read", func(t *testing.T) {
		h := newHarness(t)
		task := h.runningTask(3, 7, "A")
		h.failed(3, 7, task, 1, 1, "image_missing", false)
		h.flush()
		r := h.ui("get-archived-task", 3, 1, map[string]any{"task_id": task})
		if r.Status != 200 || str(r.Body["current_state"]) != "failed" || str(r.Body["final_reason"]) != "image_missing" {
			t.Fatalf("archive = %d %v", r.Status, r.Body)
		}
		if got, want := str(r.Body["task_name"]), h.taskName(3, task); got != want { // v0050
			t.Errorf("get-archived-task task_name = %q, want %q", got, want)
		}
		if f := h.ui("get-archived-task", 4, 1, map[string]any{"task_id": task}); f.Status != 404 {
			t.Fatalf("foreign archive read = %d", f.Status)
		}
		l := h.ui("list-archive", 3, 1, nil)
		if !sameIDs(itemIDs(l), []int64{task}) {
			t.Fatalf("list-archive = %v", l.Body)
		}
		rowTaskNames(t, h, "list-archive", l) // v0050
		// Bugfix 6: list-tasks serves current and archived tasks.
		if cur := h.ui("list-tasks", 3, 1, nil); !sameIDs(itemIDs(cur), []int64{task}) || str(obj(list(cur.Body["items"])[0])["current_state"]) != "failed" {
			t.Fatalf("archived task missing from list-tasks: %v", cur.Body)
		}
	})

	run(t, "Tasks of one Executor", func(t *testing.T) {
		h := newHarness(t)
		held := h.runningTask(3, 7, "HELD")
		gone := h.runningTask(3, 7, "GONE")
		h.released(3, 7, gone, 1, 1, "executor_restarted")
		r := h.ui("list-executor-tasks", 3, 1, map[string]any{"executor_id": 7})
		if !sameIDs(itemIDs(r), []int64{held}) {
			t.Fatalf("executor tasks = %v", itemIDs(r))
		}
		rowTaskNames(t, h, "list-executor-tasks", r) // v0050
		h.ensureTeam(4)
		h.ensureExecutor(4, 9)
		f := h.ui("list-executor-tasks", 3, 1, map[string]any{"executor_id": 9})
		u := h.ui("list-executor-tasks", 3, 1, map[string]any{"executor_id": 999})
		if f.Status != 200 || len(itemIDs(f)) != 0 || string(f.Raw) != string(u.Raw) {
			t.Fatalf("foreign Executor = %d %s vs unknown %s", f.Status, f.Raw, u.Raw)
		}
	})

	run(t, "Tampered or cross-scope cursor", func(t *testing.T) {
		h := newHarness(t)
		for i := 0; i < 11; i++ {
			h.advance(time.Second)
			h.seedTask(3, 1, "T"+tok(int64(i)))
		}
		params := map[string]any{"time_zone": "Europe/Moscow", "date": "2026-11-02", "search": "T", "status": "queued"}
		first := h.ui("list-tasks", 3, 1, params)
		cur := str(first.Body["next_cursor"])
		if len(itemIDs(first)) != 10 || cur == "" {
			t.Fatalf("first page = %v", first.Body)
		}
		with := func(k string, v any) map[string]any {
			m := map[string]any{"cursor": cur}
			for kk, vv := range params {
				m[kk] = vv
			}
			if k != "" {
				m[k] = v
			}
			return m
		}
		if ok := h.ui("list-tasks", 3, 1, with("", nil)); ok.Status != 200 || len(itemIDs(ok)) != 1 {
			t.Fatalf("valid cursor = %d %v", ok.Status, ok.Body)
		}
		flipped := []byte(cur)
		flipped[len(flipped)/2] ^= 1
		reads := h.Store.Reads
		cases := []struct {
			name      string
			op        string
			team, op_ int64
			params    map[string]any
		}{
			{"bit", "list-tasks", 3, 1, with("cursor", string(flipped))},
			{"team", "list-tasks", 4, 1, with("", nil)},
			{"operator", "list-tasks", 3, 2, with("", nil)},
			{"time_zone", "list-tasks", 3, 1, with("time_zone", "UTC")},
			{"date", "list-tasks", 3, 1, with("date", "2026-11-03")},
			{"filter", "list-tasks", 3, 1, with("status", "running")},
			{"search", "list-tasks", 3, 1, with("search", "X")},
			{"operation", "list-archive", 3, 1, with("", nil)},
			{"empty", "list-tasks", 3, 1, with("cursor", "")},
			{"legacy", "list-tasks", 3, 1, with("cursor", "eyJ2IjowfQ")},
		}
		for _, c := range cases {
			r := h.ui(c.op, c.team, c.op_, c.params)
			if r.Status != 400 || str(r.Body["code"]) != "invalid_cursor" {
				t.Fatalf("%s: %d %v", c.name, r.Status, r.Body)
			}
		}
		if h.Store.Reads != reads {
			t.Fatalf("rejected cursors read task rows")
		}
	})

	run(t, "Unsupported limit", func(t *testing.T) {
		h := newHarness(t)
		h.seedTask(3, 1, "A")
		reads := h.Store.Reads
		for _, l := range []any{20, 0, 101, "abc", 10.5} {
			if r := h.ui("list-tasks", 3, 1, map[string]any{"limit": l}); r.Status != 400 || str(r.Body["code"]) != "invalid_pagination" {
				t.Fatalf("limit %v -> %d %v", l, r.Status, r.Body)
			}
		}
		if h.Store.Reads != reads {
			t.Fatalf("invalid limit read task rows")
		}
		for _, l := range []any{10, 25, 50, 100} {
			if r := h.ui("list-tasks", 3, 1, map[string]any{"limit": l}); r.Status != 200 {
				t.Fatalf("limit %v rejected", l)
			}
		}
	})

	run(t, "One task-history day", func(t *testing.T) {
		h := newHarness(t)
		ids := datedTasks(t, h, "2026-08-14T23:59:59Z", "2026-08-15T00:00:00Z", "2026-08-15T23:59:59Z", "2026-08-16T00:00:00Z")
		r := h.ui("list-tasks", 3, 1, map[string]any{"date": "2026-08-15"})
		if !sameIDs(itemIDs(r), []int64{ids[2], ids[1]}) {
			t.Fatalf("UTC day = %v (ids %v)", itemIDs(r), ids)
		}
		if bad := h.ui("list-tasks", 3, 1, map[string]any{"date": "2026-02-30"}); bad.Status != 400 || str(bad.Body["code"]) != "invalid_request" {
			t.Fatalf("invalid civil date = %d %v", bad.Status, bad.Body)
		}
	})

	run(t, "Moscow day excludes neighboring UTC hours", func(t *testing.T) {
		h := newHarness(t)
		ids := datedTasks(t, h, "2026-10-17T20:59:59Z", "2026-10-17T21:00:00Z", "2026-10-18T20:59:59Z", "2026-10-18T21:00:00Z")
		r := h.ui("list-tasks", 3, 1, map[string]any{"date": "2026-10-18", "time_zone": "Europe/Moscow"})
		if !sameIDs(itemIDs(r), []int64{ids[2], ids[1]}) {
			t.Fatalf("Moscow day = %v (ids %v)", itemIDs(r), ids)
		}
	})

	run(t, "DST, non-hour offsets and skipped dates", func(t *testing.T) {
		h := newHarness(t)
		spring := datedTasks(t, h, "2026-03-08T04:59:59Z", "2026-03-08T05:00:00Z", "2026-03-09T03:59:59Z", "2026-03-09T04:00:00Z")
		if r := h.ui("list-tasks", 3, 1, map[string]any{"date": "2026-03-08", "time_zone": "America/New_York"}); !sameIDs(itemIDs(r), []int64{spring[2], spring[1]}) {
			t.Fatalf("NY spring = %v (%v)", itemIDs(r), spring)
		}
		h2 := newHarness(t)
		autumn := datedTasks(t, h2, "2026-11-01T03:59:59Z", "2026-11-01T05:30:00Z", "2026-11-01T06:30:00Z", "2026-11-02T04:59:59Z", "2026-11-02T05:00:00Z")
		if r := h2.ui("list-tasks", 3, 1, map[string]any{"date": "2026-11-01", "time_zone": "America/New_York"}); !sameIDs(itemIDs(r), []int64{autumn[3], autumn[2], autumn[1]}) {
			t.Fatalf("NY autumn (both repeated hours) = %v (%v)", itemIDs(r), autumn)
		}
		h3 := newHarness(t)
		kolkata := datedTasks(t, h3, "2026-08-14T18:29:59Z", "2026-08-14T18:30:00Z", "2026-08-15T18:29:59Z", "2026-08-15T18:30:00Z")
		if r := h3.ui("list-tasks", 3, 1, map[string]any{"date": "2026-08-15", "time_zone": "Asia/Kolkata"}); !sameIDs(itemIDs(r), []int64{kolkata[2], kolkata[1]}) {
			t.Fatalf("Kolkata = %v (%v)", itemIDs(r), kolkata)
		}
		h4 := newHarness(t)
		datedTasks(t, h4, "2011-12-29T09:59:59Z", "2011-12-30T10:00:00Z", "2011-12-30T12:00:00Z")
		if r := h4.ui("list-tasks", 3, 1, map[string]any{"date": "2011-12-30", "time_zone": "Pacific/Apia"}); r.Status != 200 || len(itemIDs(r)) != 0 {
			t.Fatalf("skipped Apia date = %d %v", r.Status, itemIDs(r))
		}
	})

	run(t, "Unsupported time zone", func(t *testing.T) {
		h := newHarness(t)
		h.seedTask(3, 1, "A")
		reads := h.Store.Reads
		for _, tz := range []any{"+03:00", "Local", "", []string{"UTC", "UTC"}, "../etc/passwd", "Mars/Olympus"} {
			if r := h.ui("list-tasks", 3, 1, map[string]any{"time_zone": tz}); r.Status != 400 || str(r.Body["code"]) != "invalid_time_zone" {
				t.Fatalf("time_zone %v -> %d %v", tz, r.Status, r.Body)
			}
		}
		if h.Store.Reads != reads {
			t.Fatalf("invalid time zones read task rows")
		}
		for _, tz := range []string{"UTC", "Europe/Moscow", "America/New_York", "Asia/Kolkata", "Pacific/Apia"} {
			if r := h.ui("list-tasks", 3, 1, map[string]any{"time_zone": tz}); r.Status != 200 {
				t.Fatalf("supported zone %s rejected", tz)
			}
		}
	})

	run(t, "Rolling cutoff stays frozen across pages", func(t *testing.T) {
		h := newHarness(t)
		h.at(t0.Add(-23*time.Hour - 30*time.Minute))
		oldest := h.seedTask(3, 1, "OLDEST")
		for i := 0; i < 10; i++ {
			h.at(t0.Add(-time.Duration(10-i) * time.Minute))
			h.seedTask(3, 1, "N"+tok(int64(i)))
		}
		h.at(t0)
		first := h.ui("list-tasks", 3, 1, map[string]any{"period": "day"})
		cur := str(first.Body["next_cursor"])
		if len(itemIDs(first)) != 10 || cur == "" {
			t.Fatalf("first page = %v", first.Body)
		}
		h.at(t0.Add(time.Hour)) // a fresh cutoff would now exclude OLDEST
		h.seedTask(3, 1, "NEWER")
		second := h.ui("list-tasks", 3, 1, map[string]any{"period": "day", "cursor": cur})
		if !sameIDs(itemIDs(second), []int64{oldest}) {
			t.Fatalf("second page = %v, want only %d", itemIDs(second), oldest)
		}
	})

	run(t, "Period month is one calendar month back", func(t *testing.T) {
		// Baseline State Registry (UITaskCutoff): day = 24 h, week = 7 days,
		// month = now.AddDate(0, -1, 0) in UTC, so its length follows the
		// calendar and normalizes like Go's AddDate.
		for _, c := range []struct {
			name, now, cutoff string
		}{
			// 31-day July: 31 days back, not 30.
			{"after a 31-day month", "2026-08-15T12:00:00Z", "2026-07-15T12:00:00Z"},
			// 28-day February: 28 days back, not 30.
			{"after a 28-day month", "2026-03-15T12:00:00Z", "2026-02-15T12:00:00Z"},
			// February 31 normalizes to March 3.
			{"day missing in the previous month", "2026-03-31T12:00:00Z", "2026-03-03T12:00:00Z"},
		} {
			run(t, c.name, func(t *testing.T) {
				h := newHarness(t)
				now, cutoff := mustTime(t, c.now), mustTime(t, c.cutoff)
				if want := now.AddDate(0, -1, 0); !want.Equal(cutoff) {
					t.Fatalf("fixture cutoff %s, baseline gives %s", cutoff, want)
				}
				ids := datedTasks(t, h,
					cutoff.Add(-time.Minute).Format(time.RFC3339),
					cutoff.Format(time.RFC3339),
					cutoff.Add(time.Minute).Format(time.RFC3339),
					now.Add(-time.Minute).Format(time.RFC3339))
				h.at(now)
				r := h.ui("list-tasks", 3, 1, map[string]any{"period": "month"})
				want := []int64{ids[3], ids[2], ids[1]} // newest first; the task before the cutoff is out
				if r.Status != 200 || !sameIDs(itemIDs(r), want) {
					t.Fatalf("month at %s = %d %v, want %v", c.now, r.Status, itemIDs(r), want)
				}
			})
		}
	})

	run(t, "Search, Source and tag filters", func(t *testing.T) {
		h := newHarness(t)
		a := h.seedTask(3, 1, "ISSUE-42")
		h.seedTask(3, 2, "OTHER")
		if r := h.ui("list-tasks", 3, 1, map[string]any{"search": "issue-4"}); !sameIDs(itemIDs(r), []int64{a}) {
			t.Fatalf("search = %v", itemIDs(r))
		}
		if r := h.ui("list-tasks", 3, 1, map[string]any{"source_id": 1}); !sameIDs(itemIDs(r), []int64{a}) {
			t.Fatalf("source filter = %v", itemIDs(r))
		}
		if r := h.ui("list-tasks", 3, 1, map[string]any{"tag": "gpu"}); len(itemIDs(r)) != 0 {
			t.Fatalf("tag filter = %v", itemIDs(r))
		}
		if r := h.ui("list-tasks", 3, 1, nil); strings.Contains(string(r.Raw), "rendered_prompt") || strings.Contains(string(r.Raw), "payload") {
			t.Fatalf("list exposes snapshot data")
		}
	})
}
