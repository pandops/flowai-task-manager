package test

import (
	"testing"
	"time"

	"github.com/pandops/flowai-task-manager/internal/wire"
)

// Spec: task-manager — task-manager rejects source tasks of archived teams.
func TestV0048TaskManagerRejectsSourceTasksOfArchivedTeams(t *testing.T) {
	run(t, "Archived team's source task arrives", func(t *testing.T) {
		h := newHarness(t)
		h.teamCreated(3, imageTeam3)
		h.bind(3, 1, flow(2))
		h.teamArchived(3, imageTeam3)
		ack, seq := h.intake(3, 1, entry(1, "A"), entry(2, "B"))
		h.flush()
		if ack.Kind != wire.AckOK || len(h.currentOf(3)) != 0 {
			t.Fatalf("ack=%v tasks=%d", ack.Kind, len(h.currentOf(3)))
		}
		expectRejected(t, h, seq, 2, "team_archived")
	})

	run(t, "Team event has not arrived yet", func(t *testing.T) {
		h := newHarness(t)
		h.bind(3, 1, flow(2))
		ack, seq := h.intake(3, 1, entry(1, "A"))
		h.flush()
		if ack.Kind != wire.AckNak || ack.Delay <= 0 || len(h.Store.CurrentTasks()) != 0 || len(h.events("intake_processed")) != 0 {
			t.Fatalf("unknown team: ack=%+v tasks=%d", ack, len(h.Store.CurrentTasks()))
		}
		h.teamCreated(3, imageTeam3)
		if again := h.intakeSeq(3, 1, seq, entry(1, "A")); again.Kind != wire.AckOK || len(h.currentOf(3)) != 1 {
			t.Fatalf("after the team event: ack=%v tasks=%d", again.Kind, len(h.currentOf(3)))
		}
	})

	run(t, "Running task of an archived team", func(t *testing.T) {
		h := newHarness(t)
		task := h.runningTask(3, 7, "RUN")
		h.teamArchived(3, imageTeam3)
		if cur := h.current(3, task); cur == nil || cur.State != "running" || cur.CurrentClaim() == nil || cur.CurrentClaim().ExecutorID != 7 {
			t.Fatalf("archive changed the running task: %+v", cur)
		}
		h.finished(3, 7, task, 1, 1)
		h.flush()
		rec, _, ok := h.Store.Archived(3, task)
		if !ok || rec.State != "finished" {
			t.Fatalf("task did not end normally: %+v", rec)
		}
		if r := h.ui("get-archived-task", 3, 1, map[string]any{"task_id": task}); r.Status != 200 {
			t.Fatalf("archived team's task not readable: %d", r.Status)
		}
	})
}

// Spec: team-registry — "Existing tasks continue after archive"; the task
// part belongs to task-manager: a queued task stays claimable, a claimed task
// continues, a terminal task stays readable and is not duplicated, reset or
// reopened.
func TestV0048ExistingTasksContinueAfterArchive(t *testing.T) {
	h := newHarness(t)
	claimed := h.runningTask(3, 7, "RUN")
	done := h.runningTask(3, 7, "DONE")
	h.finished(3, 7, done, 1, 1)
	h.flush()
	queued := h.seedTask(3, 1, "WAIT")
	h.ensureExecutor(3, 8)
	before, _, ok := h.St.Archived(3, done)
	if !ok || before.State != "finished" {
		t.Fatalf("terminal task not archived: %+v", before)
	}
	doneMsgs := len(h.taskMsgsFor(3, done))

	h.teamArchived(3, imageTeam3)
	h.flush()

	// The queued task remains claimable.
	if cur := h.current(3, queued); cur == nil || cur.State != "queued" {
		t.Fatalf("queued task changed by the archive: %+v", cur)
	}
	seq := h.created(3, 8, queued, 1, 1)
	h.flush()
	if acc := h.eventsFor("claim_accepted", 3, queued); len(acc) != 1 || num(acc[0]["executor_id"]) != 8 {
		t.Fatalf("queued task of an archived team was not claimable: %v", acc)
	}
	if r := h.claimToken(8, queued, 1, 1, seq); r.Status != 200 {
		t.Fatalf("claim token for the queued task: %d %s", r.Status, r.Raw)
	}

	// The claimed task continues through its lifecycle.
	h.finished(3, 7, claimed, 1, 1)
	h.flush()
	if rec, _, ok := h.St.Archived(3, claimed); !ok || rec.State != "finished" {
		t.Fatalf("claimed task did not finish: %+v", rec)
	}

	// The terminal task: readable, once, unchanged, never queued again.
	h.due(time.Hour)
	if h.current(3, done) != nil {
		t.Fatalf("terminal task reopened as a current task")
	}
	after, _, ok := h.St.Archived(3, done)
	if !ok || after.State != "finished" || after.Attempt != before.Attempt || after.ClaimSeq != before.ClaimSeq || len(after.Attempts) != len(before.Attempts) {
		t.Fatalf("terminal task reset: before %+v after %+v", before, after)
	}
	if n := len(h.taskMsgsFor(3, done)); n != doneMsgs {
		t.Fatalf("terminal task republished: %d task messages, was %d", n, doneMsgs)
	}
	for _, kind := range []string{"queued", "archived"} {
		if n := len(h.eventsFor(kind, 3, done)); n != 1 {
			t.Fatalf("terminal task has %d %s events, want 1", n, kind)
		}
	}
	r := h.ui("get-archived-task", 3, 1, map[string]any{"task_id": done})
	if r.Status != 200 || num(r.Body["task_id"]) != done || str(r.Body["current_state"]) != "finished" || r.Body["archived"] != true {
		t.Fatalf("terminal task not readable: %d %s", r.Status, r.Raw)
	}
	page := h.ui("list-archive", 3, 1, nil)
	n := 0
	for _, id := range itemIDs(page) {
		if id == done {
			n++
		}
	}
	if page.Status != 200 || n != 1 {
		t.Fatalf("archive lists the terminal task %d times: %s", n, page.Raw)
	}
}
