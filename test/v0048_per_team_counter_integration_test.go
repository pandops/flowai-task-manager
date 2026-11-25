//go:build integration

package test

import (
	"context"
	"sync"
	"testing"

	"github.com/pandops/flowai-task-manager/internal/wire"
)

// Coverage: "Per-team counters under concurrent creates of one kind in one
// team give unique, increasing numbers; gaps after a rollback are allowed and
// no number is reused; the first record in a new team is 1" (1.7).
func TestV0048PerTeamCounterIntegration(t *testing.T) {
	env := newPGEnv(t, true)
	st := openPG(t, env)
	h := newHarnessOn(t, st)
	other := h.peer(openPG(t, env))
	h.teamCreated(3, imageTeam3)
	h.bind(3, 1, flow(2))

	// One rolled-back creation consumes a number.
	st.Hook = func(step string) error {
		if step == "save_task" {
			return errTest
		}
		return nil
	}
	h.intakeSeq(3, 1, 1, entry(1, "ROLLED-BACK"))
	st.Hook = nil

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		svc := h.Svc
		if i%2 == 1 {
			svc = other
		}
		seq := int64(10 + i)
		msg := wire.Msg{Subject: "source-controller.task.3.sub", Header: map[string]string{"team": "3"}, Data: mustJSON(t, h.intakeBody(3, 1, seq, entry(seq, "C"+tok(seq))))}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := 0; k < 5; k++ {
				if svc.HandleIntake(context.Background(), msg).Kind == wire.AckOK {
					return
				}
			}
		}()
	}
	wg.Wait()
	h.flush()
	seen := map[int64]bool{}
	for _, v := range st.CurrentTasks() {
		if v.TeamID == 3 {
			if seen[v.TaskID] || v.TaskID == 1 {
				t.Fatalf("task id %d reused (1 belongs to the rolled-back transaction)", v.TaskID)
			}
			seen[v.TaskID] = true
		}
	}
	if len(seen) != 10 {
		t.Fatalf("committed %d tasks, want 10", len(seen))
	}
	// Commit order: queued events are published in publisher_seq order and
	// their task ids increase.
	var last int64
	for _, e := range h.events("queued") {
		id := num(e["task_id"])
		if id <= last {
			t.Fatalf("task id %d committed after %d", id, last)
		}
		last = id
	}
	first := h.seedTask(8, 1, "FIRST")
	if first != 1 {
		t.Fatalf("first task of a new team = %d", first)
	}
}
