package test

import (
	"sync"
	"testing"

	"github.com/pandops/flowai-task-manager/internal/wire"
)

// Spec: task-manager — task-manager owns current tasks in its own schema.
func TestV0048TaskManagerOwnsCurrentTasksInItsOwnSchema(t *testing.T) {

	run(t, "First tasks of two teams are numbered from 1", func(t *testing.T) {
		h := newHarness(t)
		a := h.seedTask(3, 1, "ISSUE-1")
		b := h.seedTask(3, 1, "ISSUE-2")
		c := h.seedTask(4, 1, "ISSUE-9")
		if a != 1 || b != 2 || c != 1 {
			t.Fatalf("task ids team3=%d,%d team4=%d; want 1,2 and 1", a, b, c)
		}
		own := h.getTask(4, 1)
		if own.Status != 200 || num(own.Body["team_id"]) != 4 || str(own.Body["external_id"]) != "ISSUE-9" {
			t.Fatalf("read (4,1) = %d %v", own.Status, own.Body)
		}
		foreign := h.getTask(4, 2)
		unknown := h.getTask(4, 77)
		if foreign.Status != 404 || str(foreign.Body["code"]) != "not_found" || unknown.Status != 404 || str(foreign.Body["message"]) != str(unknown.Body["message"]) {
			t.Fatalf("(4,2) must look like an unknown task: %d %v / %d %v", foreign.Status, foreign.Body, unknown.Status, unknown.Body)
		}
	})

	run(t, "Concurrent task creation in one team", func(t *testing.T) {
		h := newHarness(t)
		h.teamCreated(3, imageTeam3)
		h.bind(3, 1, flow(2))
		failed := false
		var mu sync.Mutex
		h.Store.Hook = func(step string) error {
			mu.Lock()
			defer mu.Unlock()
			if step == "save_task" && !failed {
				failed = true
				return errTest
			}
			return nil
		}
		var wg sync.WaitGroup
		acks := make([]wire.Ack, 2)
		seqs := []int64{h.nextSeq("source-controller"), h.nextSeq("source-controller")}
		for i := range seqs {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				acks[i] = h.intakeSeq(3, 1, seqs[i], entry(int64(100+i), "ISSUE-C"+tok(int64(i))))
			}(i)
		}
		wg.Wait()
		for i, a := range acks {
			if a.Kind != wire.AckOK {
				if again := h.intakeSeq(3, 1, seqs[i], entry(int64(100+i), "ISSUE-C"+tok(int64(i)))); again.Kind != wire.AckOK {
					t.Fatalf("redelivery %d not acknowledged", i)
				}
			}
		}
		got := map[int64]bool{}
		for _, v := range h.currentOf(3) {
			if got[v.TaskID] {
				t.Fatalf("task id %d reused", v.TaskID)
			}
			got[v.TaskID] = true
		}
		if len(got) != 2 {
			t.Fatalf("committed tasks = %v, want 2", got)
		}
		// A number consumed by the rolled-back transaction is never reused.
		if got[1] && got[2] {
			t.Fatalf("ids 1 and 2 both committed although one transaction rolled back after allocating: %v", got)
		}
		next := h.seedTask(3, 1, "ISSUE-LATER")
		for id := range got {
			if next <= id {
				t.Fatalf("later task id %d not greater than %d", next, id)
			}
		}
	})

	run(t, "Another service's data is needed", func(t *testing.T) {
		h := newHarness(t)
		h.teamCreated(3, imageTeam3)
		h.bind(3, 1, flow(2))
		h.intake(3, 1, entry(1, "ISSUE-42"))
		h.flush()
		for _, r := range h.Bus.Requests {
			if r.Subject != "flow-registry.service.targets.pub" {
				t.Fatalf("unexpected request %s", r.Subject)
			}
		}
		if len(h.targetRequests()) != 1 {
			t.Fatalf("targets requests = %d, want 1", len(h.targetRequests()))
		}
		msgs := h.taskMsgs()
		if len(msgs) != 1 || str(msgs[0]["resolved_image"]) != imageTeam3 || str(msgs[0]["image_source"]) != "team_default" {
			t.Fatalf("team default image must come from the team-registry projection: %v", msgs)
		}
	})
}
