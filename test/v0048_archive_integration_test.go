//go:build integration

package test

import "testing"

// Coverage: "Archive write then delete from current tasks is atomic; a later
// identical source task creates a new task" (1.7; task-manager archive).
func TestV0048ArchiveIntegration(t *testing.T) {
	env := newPGEnv(t, true)
	st := openPG(t, env)
	h := newHarnessOn(t, st)
	task := h.runningTask(3, 7, "ISSUE-42")

	// The failure hits the first outbox append after ArchiveTask (the
	// archived event) in the same transaction, so ArchiveTask's INSERT and
	// DELETE have run and must roll back with it.
	archivedInTx := false
	st.Hook = func(step string) error {
		switch {
		case step == "archive_task":
			archivedInTx = true
		case step == "append_outbox" && archivedInTx:
			return errTest
		}
		return nil
	}
	h.finished(3, 7, task, 1, 1)
	st.Hook = nil
	if !archivedInTx {
		t.Fatalf("finished did not reach ArchiveTask")
	}
	var archived, current int
	env.Admin.QueryRow("SELECT count(*) FROM "+tmSchema+".task_archive WHERE team_id=3 AND task_id=$1", task).Scan(&archived)
	env.Admin.QueryRow("SELECT count(*) FROM "+tmSchema+".tasks WHERE team_id=3 AND task_id=$1", task).Scan(&current)
	if archived != 0 || current != 1 {
		t.Fatalf("failed archive: archive rows %d current rows %d", archived, current)
	}

	h.finished(3, 7, task, 1, 1) // redelivered: now commits
	h.flush()
	env.Admin.QueryRow("SELECT count(*) FROM "+tmSchema+".task_archive WHERE team_id=3 AND task_id=$1", task).Scan(&archived)
	env.Admin.QueryRow("SELECT count(*) FROM "+tmSchema+".tasks WHERE team_id=3 AND task_id=$1", task).Scan(&current)
	if archived != 1 || current != 0 || len(h.eventsFor("archived", 3, task)) != 1 {
		t.Fatalf("archive rows %d current rows %d", archived, current)
	}
	h.intake(3, 1, entry(99, "ISSUE-42"))
	h.flush()
	cur := st.CurrentTasks()
	if len(cur) != 1 || cur[0].TaskID == task {
		t.Fatalf("identical source task after archive: %v", cur)
	}
}
