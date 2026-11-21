package test

import (
	"testing"

	"github.com/pandops/flowai-task-manager/internal/outbox"
)

// Spec: task-manager — task-manager archives every finished task before deleting it.
func TestV0048TaskManagerArchivesEveryFinishedTask(t *testing.T) {
	run(t, "Finished task is archived", func(t *testing.T) {
		h := newHarness(t)
		task := h.runningTask(3, 7, "A")
		h.finished(3, 7, task, 1, 1)
		h.flush()
		rec := h.archived(3, task)
		if rec == nil || h.current(3, task) != nil || rec.State != "finished" || len(rec.Attempts) != 1 || rec.Attempts[0].Claims[0].ExecutorID != 7 || rec.CompletedAt == nil {
			t.Fatalf("archive = %+v current = %+v", rec, h.current(3, task))
		}
		ev := h.eventsFor("archived", 3, task)
		if len(ev) != 1 || str(ev[0]["final_state"]) != "finished" {
			t.Fatalf("archived events = %v", ev)
		}
		if r := h.ui("get-archived-task", 3, 1, map[string]any{"task_id": task}); r.Status != 200 || str(r.Body["current_state"]) != "finished" || r.Body["archived"] != true {
			t.Fatalf("archive read = %d %v", r.Status, r.Body)
		}
		// Bugfix 6: get-task serves the archived task with its final state.
		if r := h.getTask(3, task); r.Status != 200 || str(r.Body["current_state"]) != "finished" || r.Body["archived"] != true {
			t.Fatalf("get-task of the archived task = %d %v", r.Status, r.Body)
		}
	})

	run(t, "Crash between archive and publication", func(t *testing.T) {
		h := newHarness(t)
		task := h.runningTask(3, 7, "A")
		h.finished(3, 7, task, 1, 1) // committed, not yet published
		restarted := &outbox.Relay{Store: h.Store, Pub: h.Bus, Batch: 100}
		if _, err := restarted.Flush(t.Context()); err != nil {
			t.Fatalf("flush after restart: %v", err)
		}
		restarted.Flush(t.Context())
		if len(h.eventsFor("archived", 3, task)) != 1 || h.current(3, task) != nil || h.archived(3, task) == nil {
			t.Fatalf("archived events %d", len(h.eventsFor("archived", 3, task)))
		}
	})

	run(t, "Archive write failure deletes nothing", func(t *testing.T) {
		h := newHarness(t)
		task := h.runningTask(3, 7, "A")
		// The failure hits the first outbox append after ArchiveTask (the
		// archived event) in the same transaction, so ArchiveTask's INSERT and
		// DELETE have run and must roll back with it.
		archivedInTx := false
		h.Store.Hook = func(step string) error {
			switch {
			case step == "archive_task":
				archivedInTx = true
			case step == "append_outbox" && archivedInTx:
				return errTest
			}
			return nil
		}
		h.finished(3, 7, task, 1, 1)
		h.Store.Hook = nil
		if !archivedInTx {
			t.Fatalf("finished did not reach ArchiveTask")
		}
		if cur := h.current(3, task); cur == nil || cur.State != "running" || h.archived(3, task) != nil {
			t.Fatalf("task deleted without its archive row: %+v", cur)
		}
	})
}
