//go:build integration

package test

// PostgreSQL survivors of deleted State Registry cases (ledger
// docs/testing/v0048-test-decisions.csv, rows named per subtest).

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/pandops/flowai-task-manager/internal/wire"
)

func pgCode(err error) string {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return pe.Code
	}
	return ""
}

// maxFanout binds 32 Flows to Source 1 of team 3 and returns the harness.
func maxFanout(t *testing.T) *harness {
	t.Helper()
	h := newHarnessOn(t, openPG(t, newPGEnv(t, true)))
	h.teamCreated(3, imageTeam3)
	var flows []flowDef
	for i := int64(1); i <= 32; i++ {
		flows = append(flows, flow(i))
	}
	h.bind(3, 1, flows...)
	return h
}

// assertMaxFanout checks every source task fanned out to all 32 Flows once.
func assertMaxFanout(t *testing.T, h *harness, sourceTasks int) {
	t.Helper()
	perExternal := map[string]map[int64]bool{}
	seen := map[int64]bool{}
	for _, v := range h.St.CurrentTasks() {
		if seen[v.TaskID] {
			t.Fatalf("task_id %d stored twice", v.TaskID)
		}
		seen[v.TaskID] = true
		if perExternal[v.ExternalID] == nil {
			perExternal[v.ExternalID] = map[int64]bool{}
		}
		perExternal[v.ExternalID][v.FlowID] = true
	}
	if len(seen) != sourceTasks*32 || len(perExternal) != sourceTasks {
		t.Fatalf("tasks = %d over %d source tasks, want %d over %d", len(seen), len(perExternal), sourceTasks*32, sourceTasks)
	}
	for ext, flows := range perExternal {
		if len(flows) != 32 {
			t.Fatalf("source task %s reached %d Flows, want 32", ext, len(flows))
		}
	}
	if n := len(h.taskMsgs()); n != sourceTasks*32 {
		t.Fatalf("queue messages = %d, want %d", n, sourceTasks*32)
	}
	outcomes := h.events("intake_processed")
	if len(outcomes) != sourceTasks {
		t.Fatalf("intake_processed = %d, want %d", len(outcomes), sourceTasks)
	}
	for _, e := range outcomes {
		if str(e["outcome"]) != "accepted" || len(list(e["task_ids"])) != 32 {
			t.Fatalf("outcome %v, want accepted with 32 task_ids", e)
		}
	}
	if h.St.OutboxLen() != 0 {
		t.Fatalf("outbox keeps %d unsent rows", h.St.OutboxLen())
	}
}

func maxEntries(first, n int) []map[string]any {
	out := make([]map[string]any, n)
	for i := range out {
		out[i] = map[string]any{"intake_id": int64(first + i), "external_id": fmt.Sprintf("maximum-%d", first+i),
			"payload": map[string]any{"marker": "payload-stored-once", "index": first + i}}
	}
	return out
}

func TestV0048TaskManagerRehomedRestCasesIntegration(t *testing.T) {
	// M398 (TestMigrations): the migrations pin their applied versions and
	// a second start applies nothing again.
	run(t, "M398 migration versions are pinned", func(t *testing.T) {
		env := newPGEnv(t, true)
		openPG(t, env)
		openPG(t, env)
		rows, err := env.Admin.Query(`SELECT version, count(*) FROM ` + tmSchema + `.schema_migrations GROUP BY version ORDER BY version`)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		defer rows.Close()
		var got []string
		for rows.Next() {
			var v string
			var n int
			if err := rows.Scan(&v, &n); err != nil || n != 1 {
				t.Fatalf("version %q recorded %d times (%v)", v, n, err)
			}
			got = append(got, v)
		}
		want := []string{"0001_init.sql", "0002_team_id_immutable.sql", "0003_task_name.sql"}
		if !sameStrings(got, want) {
			t.Fatalf("applied migrations = %v, want %v", got, want)
		}
	})

	// M400 (TestPersistenceConstraints): the dedupe key, the per-publisher
	// event identity and the tag and owner columns are enforced by the
	// database; the pre-claim Executor columns stay nullable.
	run(t, "M400 persistence constraints", func(t *testing.T) {
		env := newPGEnv(t, true)
		openPG(t, env)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		exec := func(q string) error { _, err := env.Admin.ExecContext(ctx, q); return err }
		mustOK := func(q string) {
			t.Helper()
			if err := exec(q); err != nil {
				t.Fatalf("%s: %v", q, err)
			}
		}
		expect := func(code, q string) {
			t.Helper()
			if err := exec(q); pgCode(err) != code {
				t.Fatalf("%s: error %v, want SQLSTATE %s", q, err, code)
			}
		}
		const s = tmSchema + "."
		task := func(id int, ext, tag string) string {
			return fmt.Sprintf(`INSERT INTO %stasks (team_id, task_id, source_id, external_id, flow_id, required_tag, state, ingested_at, record, task_name)
				VALUES (3, %d, 1, '%s', 2, %s, 'queued', now(), '{}', 'ops-%d')`, s, id, ext, tag, id)
		}
		// Pre-claim: held_executor, last_executor and next_due_at are NULL.
		mustOK(task(1, "A", "'openhands'"))
		var nulls bool
		if err := env.Admin.QueryRowContext(ctx, `SELECT held_executor IS NULL AND last_executor IS NULL AND next_due_at IS NULL FROM `+s+`tasks WHERE task_id = 1`).Scan(&nulls); err != nil || !nulls {
			t.Fatalf("pre-claim columns are not NULL: %v %v", nulls, err)
		}
		expect("23505", task(2, "A", "'openhands'")) // same (team, source, external, flow)
		expect("23505", task(1, "B", "'openhands'")) // same (team, task)
		expect("23502", task(3, "C", "NULL"))        // required_tag NOT NULL
		expect("23514", fmt.Sprintf(`INSERT INTO %stasks (team_id, task_id, source_id, external_id, flow_id, required_tag, state, ingested_at, record, task_name)
			VALUES (0, 4, 1, 'D', 2, 'openhands', 'queued', now(), '{}', 'ops-4')`, s))
		mustOK(`INSERT INTO ` + s + `consumed (publisher_key, seq) VALUES ('executor-3-7', 1)`)
		expect("23505", `INSERT INTO `+s+`consumed (publisher_key, seq) VALUES ('executor-3-7', 1)`)
		expect("23502", `INSERT INTO `+s+`consumed (publisher_key, seq) VALUES (NULL, 2)`)
		mustOK(`INSERT INTO ` + s + `executors (executor_id, team_id, tag) VALUES (7, 3, 'openhands')`)
		expect("23502", `INSERT INTO `+s+`executors (executor_id, team_id, tag) VALUES (8, 3, NULL)`)
		expect("23502", `INSERT INTO `+s+`executors (executor_id, team_id, tag) VALUES (9, NULL, 'openhands')`)
		expect("23505", `INSERT INTO `+s+`executors (executor_id, team_id, tag) VALUES (7, 4, 'gpu')`)
		expect("23502", `INSERT INTO `+s+`task_archive (team_id, task_id, source_id, external_id, flow_id, required_tag, state, ingested_at, archived_at, record, task_name)
			VALUES (3, 1, 1, 'A', 2, NULL, 'finished', now(), now(), '{}', 'ops-1')`)
	})

	// M260 (TestV0013_10_WebhookMaximumFanoutAnd33rdBindingRejection): one
	// webhook message with the maximum 100 source tasks fans each out to
	// the maximum 32 Flows in one transaction.
	run(t, "M260 100 source tasks by 32 Flows", func(t *testing.T) {
		h := maxFanout(t)
		ack, _ := h.intake(3, 1, maxEntries(1, 100)...)
		if ack.Kind != wire.AckOK {
			t.Fatalf("maximum batch ack = %v", ack.Kind)
		}
		h.flush()
		assertMaxFanout(t, h, 100)
	})

	// M261 (TestV0013_10_PollerMaximumFanoutAnd33rdBindingRejection): a
	// maximum 1000-task poll (one intake message per polled task) fans every
	// task out to 32 Flows.
	run(t, "M261 1000 polled tasks by 32 Flows", func(t *testing.T) {
		h := maxFanout(t)
		for i := 1; i <= 1000; i++ {
			seq := h.nextSeq("source-controller")
			body := h.intakeBody(3, 1, seq, maxEntries(i, 1)...)
			body["origin"], body["poll_id"], body["page"] = "poll", 1, (i-1)/100+1 // 10 pages of 100
			ack := h.intakeRaw("source-controller.task.3.sub", map[string]string{"team": "3", "Nats-Msg-Id": "source-controller-" + tok(seq)}, body)
			if ack.Kind != wire.AckOK {
				t.Fatalf("polled task %d ack = %v", i, ack.Kind)
			}
			if i%100 == 0 {
				h.flush()
			}
		}
		h.flush()
		assertMaxFanout(t, h, 1000)
	})
}
