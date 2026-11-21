package test

import (
	"testing"

	"github.com/pandops/flowai-task-manager/internal/wire"
)

func intakeOutcomes(h *harness, intakeSeq int64) []map[string]any {
	var out []map[string]any
	for _, e := range h.events("intake_processed") {
		if num(e["intake_publisher_seq"]) == intakeSeq {
			out = append(out, e)
		}
	}
	return out
}

func expectRejected(t *testing.T, h *harness, seq int64, entries int, reason string) {
	t.Helper()
	got := intakeOutcomes(h, seq)
	if len(got) != entries {
		t.Fatalf("intake_processed for %d = %d events, want %d", seq, len(got), entries)
	}
	for _, e := range got {
		if str(e["outcome"]) != "rejected" || str(e["reason"]) != reason || len(list(e["task_ids"])) != 0 {
			t.Fatalf("outcome %v, want rejected/%s", e, reason)
		}
	}
}

// Spec: task-manager — task-manager accepts source tasks only from the SOURCE_CONTROLLER_TASK stream.
func TestV0048TaskManagerAcceptsSourceTasksOnlyFromTheStream(t *testing.T) {
	run(t, "Source task creates team-owned tasks", func(t *testing.T) {
		h := newHarness(t)
		h.teamCreated(3, imageTeam3)
		h.bind(3, 1, flow(2))
		var ackedAfterCommit bool
		h.Store.Hook = nil
		ack, _ := h.intake(3, 1, entry(1, "ISSUE-42"))
		ackedAfterCommit = h.Store.Writes > 0 && ack.Kind == wire.AckOK
		tasks := h.currentOf(3)
		if !ackedAfterCommit || len(tasks) != 1 || tasks[0].SourceID != 1 || tasks[0].ExternalID != "ISSUE-42" || tasks[0].TeamID != 3 {
			t.Fatalf("ack=%v writes=%d tasks=%v", ack, h.Store.Writes, tasks)
		}
	})

	run(t, "Envelope matches the team header", func(t *testing.T) {
		h := newHarness(t)
		h.teamCreated(3, imageTeam3)
		h.bind(3, 1, flow(2))
		ack := h.intakeRaw("source-controller.task.3.sub", map[string]string{"team": "3"}, h.intakeBody(3, 1, 5, entry(1, "ISSUE-1")))
		if ack.Kind != wire.AckOK || len(h.currentOf(3)) != 1 {
			t.Fatalf("ack=%v tasks=%d", ack, len(h.currentOf(3)))
		}
	})

	run(t, "Envelope differs from the team header", func(t *testing.T) {
		for name, header := range map[string]string{"envelope team 4": "3", "header admin": "admin"} {
			h := newHarness(t)
			h.teamCreated(3, imageTeam3)
			h.teamCreated(4, imageTeam4)
			h.bind(3, 1, flow(2))
			h.bind(4, 1, flow(2))
			envTeam := int64(4)
			if header == "admin" {
				envTeam = 3
			}
			ack := h.intakeRaw("source-controller.task.3.sub", map[string]string{"team": header}, h.intakeBody(envTeam, 1, 7, entry(1, "A"), entry(2, "B")))
			h.flush()
			if ack.Kind != wire.AckTerm || len(h.Store.CurrentTasks()) != 0 {
				t.Fatalf("%s: ack=%v tasks=%d, want +TERM and no task", name, ack.Kind, len(h.Store.CurrentTasks()))
			}
			expectRejected(t, h, 7, 2, "invalid_source_task")
		}
	})

	run(t, "Subject team token differs from the team header", func(t *testing.T) {
		h := newHarness(t)
		h.teamCreated(3, imageTeam3)
		h.teamCreated(4, imageTeam4)
		h.bind(3, 1, flow(2))
		h.bind(4, 1, flow(2))
		ack := h.intakeRaw("source-controller.task.4.sub", map[string]string{"team": "3"}, h.intakeBody(3, 1, 8, entry(1, "A")))
		h.flush()
		if ack.Kind != wire.AckTerm || len(h.Store.CurrentTasks()) != 0 {
			t.Fatalf("ack=%v tasks=%d", ack.Kind, len(h.Store.CurrentTasks()))
		}
		expectRejected(t, h, 8, 1, "invalid_source_task")
	})

	run(t, "One intake consumer serves every team", func(t *testing.T) {
		h := newHarness(t)
		h.teamCreated(3, imageTeam3)
		h.bind(3, 1, flow(2))
		h.intake(3, 1, entry(1, "FIRST"))
		h.teamCreated(9, imageTeam3) // after start: no restart, no reconfiguration
		h.bind(9, 1, flow(2))
		h.intake(9, 1, entry(1, "NINE"))
		h.intake(3, 1, entry(2, "SECOND"))
		h.flush()
		var order []string
		for _, e := range h.events("intake_processed") {
			order = append(order, tok(num(e["team_id"]))+"/"+str(e["external_id"]))
		}
		if len(order) != 3 || order[0] != "3/FIRST" || order[1] != "9/NINE" || order[2] != "3/SECOND" {
			t.Fatalf("processing order = %v", order)
		}
		if len(h.currentOf(9)) != 1 {
			t.Fatalf("team 9 task not created")
		}
		if subs := wire.RequestSubjects(); len(subs) == 0 {
			t.Fatalf("served subjects missing")
		}
		consumers := loadConsumers(t)
		intake := 0
		for _, c := range consumers {
			if c.Stream == "SOURCE_CONTROLLER_TASK" {
				intake++
				if c.Durable != "task-manager-intake" || len(c.Filters) != 1 || c.Filters[0] != "source-controller.task.*.sub" {
					t.Fatalf("intake consumer %+v", c)
				}
			}
		}
		if intake != 1 {
			t.Fatalf("intake consumers = %d, want one for every team", intake)
		}
		for _, e := range h.Bus.Ensured {
			if e != "" && e[:6] != "tasks-" {
				t.Fatalf("created a consumer other than a task consumer: %s", e)
			}
		}
	})

	run(t, "Source task asserts ownership or an image", func(t *testing.T) {
		for _, field := range []string{"flow_id", "required_tag", "image"} {
			h := newHarness(t)
			h.teamCreated(3, imageTeam3)
			h.bind(3, 1, flow(2))
			bad := entry(2, "B")
			bad[field] = "x"
			ack := h.intakeRaw("source-controller.task.3.sub", map[string]string{"team": "3"}, h.intakeBody(3, 1, 11, entry(1, "A"), bad, entry(3, "C")))
			h.flush()
			if ack.Kind != wire.AckOK || len(h.Store.CurrentTasks()) != 0 {
				t.Fatalf("%s: ack=%v tasks=%d, want ack and no task", field, ack.Kind, len(h.Store.CurrentTasks()))
			}
			expectRejected(t, h, 11, 3, "invalid_source_task")
			if len(h.targetRequests()) != 0 {
				t.Fatalf("%s: rejected message caused a targets request", field)
			}
		}
	})

	run(t, "Payload contains a domain field named image", func(t *testing.T) {
		h := newHarness(t)
		h.teamCreated(3, imageTeam3)
		h.bind(3, 1, flow(2))
		e := entry(1, "IMG")
		e["payload"] = map[string]any{"image": "evil/image:latest", "title": "x"}
		h.intake(3, 1, e)
		h.flush()
		msgs := h.taskMsgs()
		if len(msgs) != 1 || str(msgs[0]["resolved_image"]) != imageTeam3 || str(obj(msgs[0]["payload"])["image"]) != "evil/image:latest" {
			t.Fatalf("payload image must stay payload data: %v", msgs)
		}
	})

	run(t, "Webhook batch is processed as a whole", func(t *testing.T) {
		h := newHarness(t)
		h.teamCreated(3, imageTeam3)
		h.bind(3, 1, flow(2))
		saves := 0
		h.Store.Hook = func(step string) error {
			if step == "save_task" {
				saves++
				if saves == 2 {
					return errTest
				}
			}
			return nil
		}
		seq := h.nextSeq("source-controller")
		ack := h.intakeSeq(3, 1, seq, entry(1, "A"), entry(2, "B"), entry(3, "C"))
		if ack.Kind == wire.AckOK || ack.Kind == wire.AckTerm || len(h.Store.CurrentTasks()) != 0 || h.Store.OutboxLen() != 0 {
			t.Fatalf("failed batch: ack=%v tasks=%d outbox=%d", ack.Kind, len(h.Store.CurrentTasks()), h.Store.OutboxLen())
		}
		h.Store.Hook = nil
		before := h.Store.Writes
		if ack := h.intakeSeq(3, 1, seq, entry(1, "A"), entry(2, "B"), entry(3, "C")); ack.Kind != wire.AckOK {
			t.Fatalf("redelivery ack=%v", ack.Kind)
		}
		if len(h.currentOf(3)) != 3 || h.Store.Writes != before+1 {
			t.Fatalf("redelivery: tasks=%d commits=%d, want 3 tasks in one commit", len(h.currentOf(3)), h.Store.Writes-before)
		}
	})

	run(t, "Message is redelivered after a crash", func(t *testing.T) {
		h := newHarness(t)
		h.teamCreated(3, imageTeam3)
		h.bind(3, 1, flow(2))
		ack, seq := h.intake(3, 1, entry(1, "A"))
		h.flush()
		queued, processed := len(h.events("queued")), len(h.events("intake_processed"))
		again := h.intakeSeq(3, 1, seq, entry(1, "A"))
		h.flush()
		if ack.Kind != wire.AckOK || again.Kind != wire.AckOK || len(h.currentOf(3)) != 1 ||
			len(h.events("queued")) != queued || len(h.events("intake_processed")) != processed {
			t.Fatalf("redelivery created work: tasks=%d queued=%d processed=%d", len(h.currentOf(3)), len(h.events("queued")), len(h.events("intake_processed")))
		}
	})

	run(t, "Redelivered intake message is deduplicated by publisher sequence", func(t *testing.T) {
		h := newHarness(t)
		h.teamCreated(3, imageTeam3)
		h.bind(3, 1, flow(2))
		h.intakeSeq(3, 1, 120, entry(1, "A"))
		second := h.intakeSeq(3, 1, 120, entry(1, "A"))
		h.intakeSeq(3, 1, 121, entry(2, "B"))
		h.flush()
		if second.Kind != wire.AckOK || len(h.currentOf(3)) != 2 || len(intakeOutcomes(h, 120)) != 1 || len(intakeOutcomes(h, 121)) != 1 {
			t.Fatalf("tasks=%d outcomes120=%d outcomes121=%d", len(h.currentOf(3)), len(intakeOutcomes(h, 120)), len(intakeOutcomes(h, 121)))
		}
	})
}
