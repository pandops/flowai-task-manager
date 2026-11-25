package test

import (
	"strings"
	"testing"

	"github.com/pandops/flowai-task-manager/internal/wire"
)

// Spec: task-manager — task-manager fans one source task out to every bound Flow.
func TestV0048TaskManagerFansOneSourceTaskOutToEveryBoundFlow(t *testing.T) {
	run(t, "Source bound to two Flows", func(t *testing.T) {
		h := newHarness(t)
		h.teamCreated(3, imageTeam3)
		h.bind(3, 1, flow(2), flow(5))
		before := h.Store.Writes
		h.intake(3, 1, entry(1, "ISSUE-42"))
		h.flush()
		cur := h.currentOf(3)
		if len(cur) != 2 || cur[0].TaskID == cur[1].TaskID || h.Store.Writes != before+1 {
			t.Fatalf("tasks=%v commits=%d", cur, h.Store.Writes-before)
		}
		flows := map[int64]int64{}
		for _, v := range cur {
			flows[v.FlowID] = v.Snapshot.ExecutionRevision
		}
		if flows[2] != 12 || flows[5] != 15 {
			t.Fatalf("frozen revisions = %v", flows)
		}
		if len(h.taskMsgs()) != 2 || len(h.targetRequests()) != 1 {
			t.Fatalf("queue messages=%d targets requests=%d", len(h.taskMsgs()), len(h.targetRequests()))
		}
		req := decode(h.targetRequests()[0].Data)
		params := obj(req["params"])
		if num(params["team_id"]) != 3 || num(params["source_id"]) != 1 || h.targetRequests()[0].Header["team"] != "3" {
			t.Fatalf("targets request %v %v", req, h.targetRequests()[0].Header)
		}
		if err := validateAgainst(t, "svc/flow-registry/schemas/service.targets.request.schema.json", h.targetRequests()[0].Data); err != nil {
			t.Fatalf("targets request violates flow-registry's schema: %v", err)
		}
	})

	run(t, "One Flow already has a current task", func(t *testing.T) {
		h := newHarness(t)
		h.seedTask(3, 1, "ISSUE-42", flow(2))
		h.bind(3, 1, flow(2), flow(5))
		h.intake(3, 1, entry(77, "ISSUE-42"))
		h.flush()
		perFlow := map[int64]int{}
		for _, v := range h.currentOf(3) {
			perFlow[v.FlowID]++
		}
		got := outcomeOf(h, 3, "ISSUE-42")
		last := got[len(got)-1]
		if perFlow[2] != 1 || perFlow[5] != 1 || str(last["outcome"]) != "accepted" || len(taskIDs(last)) != 1 {
			t.Fatalf("per flow %v last %v", perFlow, last)
		}
		if d := ids64(last["deduplicated_flow_ids"]); len(d) != 1 || d[0] != 2 {
			t.Fatalf("deduplicated flows = %v", d)
		}
	})

	run(t, "Binding removed after creation", func(t *testing.T) {
		h := newHarness(t)
		h.seedTask(3, 1, "ISSUE-42", flow(2), flow(5))
		h.bind(3, 1, flow(2))
		// task-manager acts after the unbind: the same external ID arrives again.
		ack, seq := h.intake(3, 1, entry(99, "ISSUE-42"))
		h.flush()
		got := intakeOutcomes(h, seq)
		if ack.Kind != wire.AckOK || len(got) != 1 || str(got[0]["outcome"]) != "deduplicated" {
			t.Fatalf("repeat after unbind: %v %v", ack.Kind, got)
		}
		for _, v := range h.currentOf(3) {
			if v.State != "queued" {
				t.Fatalf("task %d state %s", v.TaskID, v.State)
			}
		}
		if len(h.currentOf(3)) != 2 {
			t.Fatalf("unbinding deleted a task")
		}
	})

	run(t, "No bound Flow", func(t *testing.T) {
		h := newHarness(t)
		h.teamCreated(3, imageTeam3)
		ack, seq := h.intake(3, 1, entry(1, "A"))
		h.flush()
		got := intakeOutcomes(h, seq)
		if ack.Kind != wire.AckOK || len(got) != 1 || str(got[0]["outcome"]) != "no_target" {
			t.Fatalf("no binding: %v %v", ack.Kind, got)
		}
	})

	run(t, "Fan-out limits", func(t *testing.T) {
		h := newHarness(t)
		h.teamCreated(3, imageTeam3)
		var many []flowDef
		for i := int64(1); i <= 33; i++ {
			many = append(many, flow(i))
		}
		h.bind(3, 1, many...)
		_, seq := h.intake(3, 1, entry(1, "A"), entry(2, "B"))
		big := flow(2)
		big.Prompt = strings.Repeat("x", 1<<20) + "{{external_id}}"
		h.bind(3, 2, big)
		_, seq2 := h.intake(3, 2, entry(3, "C"))
		h.flush()
		if len(h.Store.CurrentTasks()) != 0 {
			t.Fatalf("oversized fan-out created tasks")
		}
		expectRejected(t, h, seq, 2, "fanout_too_large")
		expectRejected(t, h, seq2, 1, "fanout_too_large")
	})
}
