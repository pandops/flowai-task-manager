package test

import (
	"sync"
	"testing"
)

func outcomeOf(h *harness, team int64, external string) []map[string]any {
	var out []map[string]any
	for _, e := range h.events("intake_processed") {
		if num(e["team_id"]) == team && str(e["external_id"]) == external {
			out = append(out, e)
		}
	}
	return out
}

// Spec: task-manager — task-manager deduplicates source tasks over current tasks.
func TestV0048TaskManagerDeduplicatesSourceTasksOverCurrentTasks(t *testing.T) {
	run(t, "Same source repeats a current external ID", func(t *testing.T) {
		h := newHarness(t)
		task := h.runningTask(3, 7, "ISSUE-42")
		before := h.current(3, task)
		queued := len(h.events("queued"))
		h.intake(3, 1, entry(55, "ISSUE-42"))
		h.flush()
		after := h.current(3, task)
		if len(h.currentOf(3)) != 1 || after.State != "running" || len(after.History) != len(before.History) || len(h.events("queued")) != queued {
			t.Fatalf("duplicate changed the running task or created one")
		}
		got := outcomeOf(h, 3, "ISSUE-42")
		last := got[len(got)-1]
		if str(last["outcome"]) != "deduplicated" || num(last["intake_id"]) != 55 || len(ids64(last["deduplicated_flow_ids"])) != 1 {
			t.Fatalf("outcome = %v", last)
		}
	})

	run(t, "Batch repeats an external ID", func(t *testing.T) {
		h := newHarness(t)
		h.teamCreated(3, imageTeam3)
		h.bind(3, 1, flow(2))
		before := h.Store.Writes
		h.intake(3, 1, entry(1, "ISSUE-42"), entry(2, "ISSUE-42"))
		h.flush()
		got := outcomeOf(h, 3, "ISSUE-42")
		if len(h.currentOf(3)) != 1 || h.Store.Writes != before+1 || len(got) != 2 ||
			str(got[0]["outcome"]) != "accepted" || str(got[1]["outcome"]) != "deduplicated" || num(got[1]["intake_id"]) != 2 {
			t.Fatalf("tasks=%d commits=%d outcomes=%v", len(h.currentOf(3)), h.Store.Writes-before, got)
		}
	})

	run(t, "Same external ID after archive", func(t *testing.T) {
		h := newHarness(t)
		first := h.runningTask(3, 7, "ISSUE-42")
		h.finished(3, 7, first, 1, 1)
		h.flush()
		h.intake(3, 1, entry(9, "ISSUE-42"))
		h.flush()
		cur := h.currentOf(3)
		if len(cur) != 1 || cur[0].TaskID == first || cur[0].FlowID != 2 {
			t.Fatalf("after archive: %v (first %d)", cur, first)
		}
	})

	run(t, "Two sources use the same external ID", func(t *testing.T) {
		h := newHarness(t)
		h.teamCreated(3, imageTeam3)
		h.bind(3, 1, flow(2))
		h.bind(3, 2, flow(2))
		h.intake(3, 1, entry(1, "ISSUE-42"))
		h.intake(3, 2, entry(2, "ISSUE-42"))
		h.flush()
		if cur := h.currentOf(3); len(cur) != 2 || cur[0].SourceID == cur[1].SourceID {
			t.Fatalf("two sources: %v", cur)
		}
	})

	run(t, "Concurrent duplicates race", func(t *testing.T) {
		h := newHarness(t)
		h.teamCreated(3, imageTeam3)
		h.bind(3, 1, flow(2), flow(5))
		var wg sync.WaitGroup
		for i := 0; i < 2; i++ {
			seq := h.nextSeq("source-controller")
			wg.Add(1)
			go func(seq int64) { defer wg.Done(); h.intakeSeq(3, 1, seq, entry(seq, "ISSUE-42")) }(seq)
		}
		wg.Wait()
		h.flush()
		perFlow := map[int64]int{}
		for _, v := range h.currentOf(3) {
			perFlow[v.FlowID]++
		}
		got := outcomeOf(h, 3, "ISSUE-42")
		outcomes := map[string]int{}
		for _, e := range got {
			outcomes[str(e["outcome"])]++
		}
		if perFlow[2] != 1 || perFlow[5] != 1 || outcomes["accepted"] != 1 || outcomes["deduplicated"] != 1 {
			t.Fatalf("per flow %v outcomes %v", perFlow, outcomes)
		}
	})
}
