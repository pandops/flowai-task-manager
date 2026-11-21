//go:build integration

package test

import (
	"database/sql"
	"testing"
)

// Spec: service-platform — "Team-owned parent keeps its team": the database
// refuses a write that changes a stored task's team_id, for the service's
// own role and for an administrator alike.
func TestV0048TeamOwnedParentKeepsItsTeamInTheDatabase(t *testing.T) {
	env := newPGEnv(t, true)
	st := openPG(t, env)
	h := newHarnessOn(t, st)
	queued := h.seedTask(3, 1, "Q")
	done := h.runningTask(3, 7, "D")
	h.finished(3, 7, done, 1, 1)
	h.flush()
	if h.archived(3, done) == nil {
		t.Fatalf("finished task %d not archived", done)
	}

	role, err := sql.Open("pgx", env.roleDSN())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { role.Close() })
	for name, db := range map[string]*sql.DB{"service role": role, "administrator": env.Admin} {
		for table, task := range map[string]int64{"tasks": queued, "task_archive": done} {
			if _, err := db.Exec(`UPDATE `+tmSchema+`.`+table+` SET team_id = 4 WHERE team_id = 3 AND task_id = $1`, task); err == nil {
				t.Errorf("%s changed team_id of %s row %d", name, table, task)
			}
		}
	}
	var moved int
	if err := env.Admin.QueryRow(`SELECT (SELECT count(*) FROM ` + tmSchema + `.tasks WHERE team_id = 4) +
		(SELECT count(*) FROM ` + tmSchema + `.task_archive WHERE team_id = 4)`).Scan(&moved); err != nil {
		t.Fatal(err)
	}
	if moved != 0 {
		t.Fatalf("%d rows moved to team 4", moved)
	}
	if cur := h.current(3, queued); cur == nil || cur.TeamID != 3 || cur.State != "queued" {
		t.Fatalf("queued task changed: %+v", cur)
	}
	if rec := h.archived(3, done); rec == nil || rec.TeamID != 3 || rec.State != "finished" {
		t.Fatalf("archived task changed: %+v", rec)
	}

	// The service's own write path cannot move it either.
	h.ensureTeam(4)
	h.ensureExecutor(4, 9)
	h.created(4, 9, queued, 1, 1)
	if cur := h.current(3, queued); cur == nil || cur.TeamID != 3 || len(cur.CurrentAttempt().Claims) != 0 || len(h.currentOf(4)) != 0 {
		t.Fatalf("a foreign-team write changed the task: %+v", cur)
	}
}
