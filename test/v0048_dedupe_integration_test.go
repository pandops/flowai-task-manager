//go:build integration

package test

import (
	"context"
	"sync"
	"testing"

	"github.com/pandops/flowai-task-manager/internal/wire"
)

// Coverage: "Concurrent intake of the same (team_id, source_id,
// external_id, flow_id) creates one current task; a failing
// SOURCE_CONTROLLER_TASK batch commits no task" (1.7; task-manager dedupe).
func TestV0048DedupeIntegration(t *testing.T) {
	run(t, "Concurrent duplicates create one current task per Flow", func(t *testing.T) {
		env := newPGEnv(t, true)
		st := openPG(t, env)
		h := newHarnessOn(t, st)
		other := h.peer(openPG(t, env)) // a second process on the same schema
		h.teamCreated(3, imageTeam3)
		h.bind(3, 1, flow(2), flow(5))
		var wg sync.WaitGroup
		for i, svc := range []interface {
			HandleIntake(context.Context, wire.Msg) wire.Ack
		}{h.Svc, other} {
			seq := int64(200 + i)
			msg := wire.Msg{Subject: "source-controller.task.3.sub", Header: map[string]string{"team": "3"}, Data: mustJSON(t, h.intakeBody(3, 1, seq, entry(seq, "ISSUE-42")))}
			wg.Add(1)
			go func() {
				defer wg.Done()
				for k := 0; k < 3; k++ { // a serialization failure is redelivered
					if svc.HandleIntake(context.Background(), msg).Kind == wire.AckOK {
						return
					}
				}
			}()
		}
		wg.Wait()
		h.flush()
		perFlow := map[int64]int{}
		for _, v := range st.CurrentTasks() {
			perFlow[v.FlowID]++
		}
		outcomes := map[string]int{}
		for _, e := range h.events("intake_processed") {
			outcomes[str(e["outcome"])]++
		}
		if perFlow[2] != 1 || perFlow[5] != 1 || outcomes["accepted"] != 1 || outcomes["deduplicated"] != 1 {
			t.Fatalf("per flow %v outcomes %v", perFlow, outcomes)
		}
	})

	run(t, "A failing batch commits no task", func(t *testing.T) {
		env := newPGEnv(t, true)
		st := openPG(t, env)
		h := newHarnessOn(t, st)
		h.teamCreated(3, imageTeam3)
		h.bind(3, 1, flow(2))
		saves := 0
		st.Hook = func(step string) error {
			if step == "save_task" {
				saves++
				if saves == 2 {
					return errTest
				}
			}
			return nil
		}
		ack := h.intakeSeq(3, 1, 300, entry(1, "A"), entry(2, "B"), entry(3, "C"))
		var rows int
		env.Admin.QueryRow("SELECT count(*) FROM " + tmSchema + ".tasks").Scan(&rows)
		if ack.Kind == wire.AckOK || rows != 0 || st.OutboxLen() != 0 {
			t.Fatalf("failed batch: ack %v rows %d outbox %d", ack.Kind, rows, st.OutboxLen())
		}
		st.Hook = nil
		if again := h.intakeSeq(3, 1, 300, entry(1, "A"), entry(2, "B"), entry(3, "C")); again.Kind != wire.AckOK || len(st.CurrentTasks()) != 3 {
			t.Fatalf("redelivery: %v tasks %d", again.Kind, len(st.CurrentTasks()))
		}
	})
}
