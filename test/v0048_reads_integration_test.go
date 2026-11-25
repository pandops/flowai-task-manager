//go:build integration

package test

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// pgConstraint returns the constraint name of a PostgreSQL error.
func pgConstraint(err error) string {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return pe.ConstraintName
	}
	return ""
}

// pgWhere returns the PL/pgSQL context of a PostgreSQL error, which names
// the trigger function that raised it.
func pgWhere(err error) string {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return pe.Where
	}
	return ""
}

// tmExec runs one statement as the database administrator against
// task_manager_schema (triggers and constraints apply to it as well).
func tmExec(t *testing.T, env pgEnv, stmt string, args ...any) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err := env.Admin.ExecContext(ctx, stmt, args...)
	return err
}

// tmString reads one text value as the administrator.
func tmString(t *testing.T, env pgEnv, query string, args ...any) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var v sql.NullString
	err := env.Admin.QueryRowContext(ctx, query, args...).Scan(&v)
	return v.String, err
}

// Team isolation and the read queries on PostgreSQL (critique-1 finding 1):
// the team_id predicate runs before the limit, foreign reads look unknown,
// keyset paging, the time-zone day window and the admin projection.
func TestV0048ReadsIntegration(t *testing.T) {
	pg := func(t *testing.T) *harness {
		env := newPGEnv(t, true)
		return newHarnessOn(t, openPG(t, env))
	}
	pgWithEnv := func(t *testing.T) (*harness, pgEnv) {
		env := newPGEnv(t, true)
		return newHarnessOn(t, openPG(t, env)), env
	}

	// v0050 — migration 0003 on an empty schema: the name columns are
	// NOT NULL text, both tables have a unique index on task_name and
	// tasks has the immutability trigger.
	run(t, "Migration 0003 adds the task name columns, indexes and trigger", func(t *testing.T) {
		_, env := pgWithEnv(t)
		for _, c := range [][2]string{{"teams", "team_name"}, {"tasks", "task_name"}, {"task_archive", "task_name"}} {
			nullable, err := tmString(t, env, `SELECT is_nullable FROM information_schema.columns
				WHERE table_schema = $1 AND table_name = $2 AND column_name = $3 AND data_type = 'text'`, tmSchema, c[0], c[1])
			if err != nil || nullable != "NO" {
				t.Errorf("%s.%s: text NOT NULL column missing (is_nullable %q, %v)", c[0], c[1], nullable, err)
			}
		}
		for _, ix := range [][2]string{{"tasks", "tasks_task_name"}, {"task_archive", "task_archive_task_name"}} {
			def, err := tmString(t, env, `SELECT indexdef FROM pg_indexes WHERE schemaname = $1 AND tablename = $2 AND indexname = $3`, tmSchema, ix[0], ix[1])
			if err != nil || !strings.Contains(def, "UNIQUE") || !strings.Contains(def, "(task_name)") {
				t.Errorf("unique index %s on %s(task_name) missing: %q %v", ix[1], ix[0], def, err)
			}
		}
		trg, err := tmString(t, env, `SELECT tg.tgname FROM pg_trigger tg JOIN pg_class c ON c.oid = tg.tgrelid
			JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1 AND c.relname = 'tasks' AND tg.tgname = 'tasks_task_name_immutable'`, tmSchema)
		if err != nil || trg != "tasks_task_name_immutable" {
			t.Errorf("trigger tasks_task_name_immutable on tasks missing: %v", err)
		}
	})

	// v0050 — SaveTask writes <team_name>-<task_id> from the projected
	// team_name, ArchiveTask keeps it, and a full task name is matched on
	// the stored column for current and archived tasks (the LIKE path, which
	// would also return the task whose external ID is OPS-42, is not used).
	run(t, "Task name is stored, archived and found by full name", func(t *testing.T) {
		h, env := pgWithEnv(t)
		h.nameTeam(3, "ops")
		h.nameTeam(4, "qa")
		h.ensureExecutor(3, 7)
		t42 := h.seedTaskAt(3, 42, "PROJ-7")
		if got, err := tmString(t, env, `SELECT team_name FROM `+tmSchema+`.teams WHERE team_id = 3`); err != nil || got != "ops" {
			t.Errorf("teams.team_name of team 3 = %q (%v), want ops", got, err)
		}
		if got, err := tmString(t, env, `SELECT task_name FROM `+tmSchema+`.tasks WHERE team_id = 3 AND task_id = $1`, t42); err != nil || got != "ops-42" {
			t.Errorf("tasks.task_name of (3, 42) = %q (%v), want ops-42", got, err)
		}
		h.created(3, 7, t42, 1, 1)
		h.running(3, 7, t42, 1, 1)
		h.finished(3, 7, t42, 1, 1)
		h.flush()
		if got, err := tmString(t, env, `SELECT task_name FROM `+tmSchema+`.task_archive WHERE team_id = 3 AND task_id = $1`, t42); err != nil || got != "ops-42" {
			t.Errorf("task_archive.task_name of (3, 42) = %q (%v), want ops-42", got, err)
		}
		t142 := h.seedTaskAt(3, 142, "OPS-42")
		q42 := h.seedTaskAt(4, 42, "QA-ONLY")
		h.flush()
		for _, c := range []struct {
			team int64
			term string
			want []int64
		}{
			{3, "ops-42", []int64{t42}},     // archived task, by name only
			{3, " OPS-142 ", []int64{t142}}, // current task, case and padding
			{3, "ops-4", []int64{}},         // name of a missing task
			{3, "qa-42", []int64{}},         // another team's name
			{3, "PS-4", []int64{t142}},      // broad search unchanged
			{4, "qa-42", []int64{q42}},
		} {
			r := h.ui("list-tasks", c.team, 1, map[string]any{"search": c.term})
			got := itemIDs(r)
			if got == nil {
				got = []int64{}
			}
			if r.Status != 200 || !sameIDs(got, c.want) || r.Body["next_cursor"] != nil {
				t.Errorf("team %d search=%q = %d %v, want %v", c.team, c.term, r.Status, got, c.want)
			}
			for _, it := range list(r.Body["items"]) {
				if num(obj(it)["team_id"]) != c.team {
					t.Errorf("team %d search=%q returned a foreign row %v", c.team, c.term, it)
				}
			}
		}
		if r := h.ui("list-tasks", 3, 1, map[string]any{"search": "ops-42", "status": "running"}); r.Status != 200 || len(itemIDs(r)) != 0 {
			t.Errorf("name + non-matching status = %v, want empty", itemIDs(r))
		}
		if r := h.ui("list-tasks", 3, 1, map[string]any{"search": "ops-42", "status": "finished"}); !sameIDs(itemIDs(r), []int64{t42}) {
			t.Errorf("name + matching status = %v, want [%d]", itemIDs(r), t42)
		}
	})

	// v0050 — the database itself keeps names unique, immutable and
	// non-empty.
	run(t, "Task name constraints reject duplicates, updates and empty names", func(t *testing.T) {
		h, env := pgWithEnv(t)
		h.nameTeam(3, "ops")
		task := h.seedTask(3, 1, "A")
		name := "ops-" + tok(task)
		copyTask := `INSERT INTO ` + tmSchema + `.tasks (team_id, task_id, source_id, external_id, flow_id, required_tag, state, ingested_at, record, task_name)
			SELECT team_id, $2, source_id, $3, flow_id, required_tag, state, ingested_at, record, $4 FROM ` + tmSchema + `.tasks WHERE team_id = 3 AND task_id = $1`
		err := tmExec(t, env, copyTask, task, 2000, "DUP", name)
		if pgCode(err) != "23505" || pgConstraint(err) != "tasks_task_name" {
			t.Errorf("duplicate tasks.task_name: %v (code %s, constraint %q), want unique violation of tasks_task_name", err, pgCode(err), pgConstraint(err))
		}
		err = tmExec(t, env, copyTask, task, 2001, "EMPTY", "")
		if pgCode(err) != "23514" || pgConstraint(err) != "tasks_task_name_check" {
			t.Errorf("empty tasks.task_name: %v (code %s, constraint %q), want check violation of tasks_task_name_check", err, pgCode(err), pgConstraint(err))
		}
		err = tmExec(t, env, `UPDATE `+tmSchema+`.tasks SET task_name = 'ops-999' WHERE team_id = 3 AND task_id = $1`, task)
		// The rejection must come from the trigger's function
		// tasks_task_name_immutable (PL/pgSQL context in Where) with its
		// integrity_constraint_violation, not from any other error.
		if pgCode(err) != "23000" || !strings.Contains(pgWhere(err), "tasks_task_name_immutable") {
			t.Errorf("UPDATE of tasks.task_name: %v (code %s, where %q), want 23000 raised by tasks_task_name_immutable", err, pgCode(err), pgWhere(err))
		}
		if got, err := tmString(t, env, `SELECT task_name FROM `+tmSchema+`.tasks WHERE team_id = 3 AND task_id = $1`, task); err != nil || got != name {
			t.Errorf("task_name after the rejected update = %q (%v), want %s", got, err, name)
		}
		if r := h.getTask(3, task); str(r.Body["task_name"]) != name {
			t.Errorf("get-task after the rejected update: task_name = %v, want %s", r.Body["task_name"], name)
		}
		err = tmExec(t, env, `UPDATE `+tmSchema+`.teams SET team_name = '' WHERE team_id = 3`)
		if pgCode(err) != "23514" || pgConstraint(err) != "teams_team_name_check" {
			t.Errorf("empty teams.team_name: %v (code %s, constraint %q), want check violation of teams_team_name_check", err, pgCode(err), pgConstraint(err))
		}
		archive := `INSERT INTO ` + tmSchema + `.task_archive (team_id, task_id, source_id, external_id, flow_id, required_tag, state, ingested_at, archived_at, record, task_name)
			VALUES (3, $1, 1, $2, 2, 'openhands', 'finished', now(), now(), '{}'::jsonb, $3)`
		if err := tmExec(t, env, archive, 900, "ARCH-900", "ops-900"); err != nil {
			t.Errorf("valid archive row: %v", err)
		}
		err = tmExec(t, env, archive, 901, "ARCH-901", "ops-900")
		if pgCode(err) != "23505" || pgConstraint(err) != "task_archive_task_name" {
			t.Errorf("duplicate task_archive.task_name: %v (code %s, constraint %q), want unique violation of task_archive_task_name", err, pgCode(err), pgConstraint(err))
		}
		err = tmExec(t, env, archive, 902, "ARCH-902", "")
		if pgCode(err) != "23514" || pgConstraint(err) != "task_archive_task_name_check" {
			t.Errorf("empty task_archive.task_name: %v (code %s, constraint %q), want check violation of task_archive_task_name_check", err, pgCode(err), pgConstraint(err))
		}
	})

	run(t, "Team predicate applies before the page limit", func(t *testing.T) {
		h := pg(t)
		for i := 0; i < 12; i++ {
			h.seedTask(4, 1, "SHARED-"+tok(int64(i)))
		}
		own := h.seedTask(3, 1, "SHARED-X")
		r := h.ui("list-tasks", 3, 1, map[string]any{"status": "queued", "search": "SHARED", "flow_id": 2, "source_id": 1, "tag": "openhands"})
		if r.Status != 200 || !sameIDs(itemIDs(r), []int64{own}) || r.Body["next_cursor"] != nil {
			t.Fatalf("team 3 page = %d %v", r.Status, r.Body)
		}
		p1 := h.ui("list-tasks", 4, 1, map[string]any{"status": "queued"})
		p2 := h.ui("list-tasks", 4, 1, map[string]any{"status": "queued", "cursor": str(p1.Body["next_cursor"])})
		if len(itemIDs(p1)) != 10 || len(itemIDs(p2)) != 2 || p2.Body["next_cursor"] != nil {
			t.Fatalf("team 4 pages %v %v", itemIDs(p1), itemIDs(p2))
		}
		for _, it := range append(list(p1.Body["items"]), list(p2.Body["items"])...) {
			if num(obj(it)["team_id"]) != 4 {
				t.Fatalf("foreign row in team 4 page: %v", it)
			}
		}
	})

	run(t, "Foreign reads are indistinguishable from unknown ones", func(t *testing.T) {
		h := pg(t)
		held := h.runningTask(3, 7, "HELD")
		done := h.runningTask(3, 7, "DONE")
		h.finished(3, 7, done, 1, 1)
		h.flush()
		h.ensureTeam(4)
		for _, c := range []struct {
			op, field string
			id        int64
		}{{"get-task", "task_id", held}, {"get-archived-task", "task_id", done}, {"list-attempts", "task_id", held}} {
			f := h.ui(c.op, 4, 1, map[string]any{c.field: c.id})
			u := h.ui(c.op, 4, 1, map[string]any{c.field: 999})
			if f.Status != 404 || u.Status != 404 || str(f.Body["code"]) != str(u.Body["code"]) || str(f.Body["message"]) != str(u.Body["message"]) {
				t.Fatalf("%s foreign %d %v vs unknown %d %v", c.op, f.Status, f.Body, u.Status, u.Body)
			}
		}
		if r := h.ui("get-task", 3, 1, map[string]any{"task_id": held}); r.Status != 200 || num(r.Body["task_id"]) != held {
			t.Fatalf("own get-task = %d %v", r.Status, r.Body)
		}
		f := h.ui("list-executor-tasks", 4, 1, map[string]any{"executor_id": 7})
		u := h.ui("list-executor-tasks", 4, 1, map[string]any{"executor_id": 999})
		if f.Status != 200 || len(itemIDs(f)) != 0 || string(f.Raw) != string(u.Raw) {
			t.Fatalf("foreign Executor %d %s vs unknown %s", f.Status, f.Raw, u.Raw)
		}
		if own := h.ui("list-executor-tasks", 3, 1, map[string]any{"executor_id": 7}); !sameIDs(itemIDs(own), []int64{held}) {
			t.Fatalf("own Executor tasks = %v", itemIDs(own))
		}
		if arch := h.ui("list-archive", 4, 1, nil); len(itemIDs(arch)) != 0 {
			t.Fatalf("team 4 sees team 3's archive: %v", arch.Body)
		}
	})

	run(t, "Keyset paging across equal ingested_at", func(t *testing.T) {
		h := pg(t)
		var want []int64
		for i := 0; i < 15; i++ { // one ingested_at for all
			want = append([]int64{h.seedTask(3, 1, "K"+tok(int64(i)))}, want...)
		}
		p1 := h.ui("list-tasks", 3, 1, nil)
		p2 := h.ui("list-tasks", 3, 1, map[string]any{"cursor": str(p1.Body["next_cursor"])})
		if got := append(itemIDs(p1), itemIDs(p2)...); !sameIDs(got, want) || p2.Body["next_cursor"] != nil {
			t.Fatalf("pages %v + %v, want %v", itemIDs(p1), itemIDs(p2), want)
		}
	})

	run(t, "Moscow day excludes neighboring UTC hours", func(t *testing.T) {
		h := pg(t)
		ids := datedTasks(t, h, "2026-10-17T20:59:59Z", "2026-10-17T21:00:00Z", "2026-10-18T20:59:59Z", "2026-10-18T21:00:00Z")
		if r := h.ui("list-tasks", 3, 1, map[string]any{"date": "2026-10-18", "time_zone": "Europe/Moscow"}); !sameIDs(itemIDs(r), []int64{ids[2], ids[1]}) {
			t.Fatalf("Moscow day = %v (ids %v)", itemIDs(r), ids)
		}
	})

	run(t, "Admin projection orders teams and archive and pages", func(t *testing.T) {
		h := pg(t)
		done := h.runningTask(3, 7, "DONE")
		h.finished(3, 7, done, 1, 1)
		h.advance(time.Second)
		q3 := h.seedTask(3, 1, "Q3")
		q4 := h.seedTask(4, 1, "Q4") // same ingested_at as Q3
		h.flush()
		first := h.admin(map[string]any{"limit": 2}, nil)
		items := list(first.Body["items"])
		next := obj(first.Body["next"])
		if first.Status != 200 || len(items) != 2 || next == nil ||
			num(obj(items[0])["team_id"]) != 4 || num(obj(items[0])["task_id"]) != q4 || num(obj(items[1])["team_id"]) != 3 || num(obj(items[1])["task_id"]) != q3 {
			t.Fatalf("first page = %d %v", first.Status, first.Body)
		}
		second := h.admin(map[string]any{"limit": 2}, map[string]any{"after": next})
		items = list(second.Body["items"])
		if len(items) != 1 || num(obj(items[0])["task_id"]) != done || str(obj(items[0])["current_state"]) != "finished" || second.Body["next"] != nil {
			t.Fatalf("second page = %v", second.Body)
		}
		only3 := h.admin(map[string]any{"team_id": 3}, nil)
		if !sameIDs(itemIDs(only3), []int64{q3, done}) {
			t.Fatalf("team_id filter = %v", only3.Body)
		}
		for _, it := range list(only3.Body["items"]) {
			if num(obj(it)["team_id"]) != 3 {
				t.Fatalf("team filter leaked %v", it)
			}
		}
	})
}
