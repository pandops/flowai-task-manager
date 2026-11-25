//go:build integration

package test

import (
	"context"
	"database/sql"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/pandops/flowai-task-manager/internal/domain"
	"github.com/pandops/flowai-task-manager/internal/service"
	"github.com/pandops/flowai-task-manager/internal/wire"
)

func createdMsg(t *testing.T, h *harness, execID, task, seq int64) wire.Msg {
	body := map[string]any{"publisher": "executor", "publisher_seq": seq, "team_id": 3, "executor_id": execID, "kind": "created",
		"occurred_at": ts(h.Now), "task_id": task, "attempt": 1, "claim_seq": 1}
	return wire.Msg{Subject: "executor.event." + tok(execID) + ".created.sub", Header: map[string]string{"team": "3"}, Data: mustJSON(t, body)}
}

func claimSetup(t *testing.T) (*harness, *service.Service, int64) {
	env := newPGEnv(t, true)
	st := openPG(t, env)
	h := newHarnessOn(t, st)
	other := h.peer(openPG(t, env))
	task := h.seedTask(3, 1, "A")
	h.ensureExecutor(3, 7)
	h.ensureExecutor(3, 8)
	return h, other, task
}

// Coverage: "Two concurrent created for one task: one claim wins, the other
// Executor is told to drop it" (1.7) and developer-tooling "Race case runs
// both serial orders".
func TestV0048ClaimIntegration(t *testing.T) {
	run(t, "Two concurrent created for one task", func(t *testing.T) {
		h, other, task := claimSetup(t)
		var wg sync.WaitGroup
		for i, p := range []struct {
			svc interface {
				HandleEvent(context.Context, wire.Msg) wire.Ack
			}
			exec int64
		}{{h.Svc, 7}, {other, 8}} {
			msg := createdMsg(t, h, p.exec, task, int64(50+i))
			wg.Add(1)
			go func(svc interface {
				HandleEvent(context.Context, wire.Msg) wire.Ack
			}) {
				defer wg.Done()
				for k := 0; k < 5; k++ {
					if svc.HandleEvent(context.Background(), msg).Kind == wire.AckOK {
						return
					}
				}
			}(p.svc)
		}
		wg.Wait()
		h.flush()
		acc := h.events("claim_accepted")
		if len(acc) != 1 {
			t.Fatalf("claim_accepted = %d", len(acc))
		}
		winner := num(acc[0]["executor_id"])
		loser := map[int64]int64{7: 8, 8: 7}[winner]
		loserSeq := map[int64]int64{7: 50, 8: 51}[loser]
		if r := h.claimToken(loser, task, 1, 1, loserSeq); r.Status != 409 || str(r.Body["code"]) != "claim_rejected" {
			t.Fatalf("loser token = %d %v", r.Status, r.Body)
		}
		if r := h.claimToken(winner, task, 1, 1, map[int64]int64{7: 50, 8: 51}[winner]); r.Status != 200 {
			t.Fatalf("winner token = %d", r.Status)
		}
	})

	run(t, "Race case runs both serial orders", func(t *testing.T) {
		for _, order := range [][2]int64{{7, 8}, {8, 7}} {
			env := newPGEnv(t, true)
			first := openPG(t, env)
			h := newHarnessOn(t, first)
			competitor := h.peer(openPG(t, env))
			task := h.seedTask(3, 1, "A")
			h.ensureExecutor(3, 7)
			h.ensureExecutor(3, 8)

			// The first process's write transaction stops at a barrier
			// after it saved the claim and before it commits.
			paused, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			first.Hook = func(step string) error {
				if step == "save_task" {
					once.Do(func() { close(paused); <-release })
				}
				return nil
			}
			firstDone, secondDone := make(chan wire.Ack, 1), make(chan wire.Ack, 1)
			go func() { firstDone <- h.Svc.HandleEvent(context.Background(), createdMsg(t, h, order[0], task, 60)) }()
			waitClosed(t, paused, "first transaction at the barrier")
			go func() {
				secondDone <- competitor.HandleEvent(context.Background(), createdMsg(t, h, order[1], task, 61))
			}()
			waitLockWaiter(t, env)
			select {
			case <-secondDone:
				t.Fatalf("order %v: the competitor finished while the first transaction was open", order)
			default:
			}
			close(release)
			for _, done := range []chan wire.Ack{firstDone, secondDone} {
				select {
				case <-done:
				case <-time.After(30 * time.Second):
					t.Fatalf("order %v: a claim transaction did not finish", order)
				}
			}
			first.Hook = nil

			// Committed rows: one claim, held by the first Executor.
			var held sql.NullInt64
			var state string
			var record []byte
			if err := env.Admin.QueryRow(`SELECT held_executor, state, record FROM `+tmSchema+`.tasks WHERE team_id = 3 AND task_id = $1`, task).
				Scan(&held, &state, &record); err != nil {
				t.Fatalf("order %v: read committed task: %v", order, err)
			}
			var stored domain.Task
			if err := json.Unmarshal(record, &stored); err != nil {
				t.Fatalf("order %v: task record: %v", order, err)
			}
			claims := stored.CurrentAttempt().Claims
			if !held.Valid || held.Int64 != order[0] || state != domain.StateCreated || len(claims) != 1 || claims[0].ExecutorID != order[0] {
				t.Fatalf("order %v: committed held_executor %v state %q claims %+v", order, held, state, claims)
			}
			h.flush()
			acc := h.events("claim_accepted")
			if len(acc) != 1 || num(acc[0]["executor_id"]) != order[0] {
				t.Fatalf("order %v: claim_accepted %v", order, acc)
			}
		}
	})
}

func waitClosed(t *testing.T, c <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-c:
	case <-time.After(30 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// waitLockWaiter waits until a session of env's database waits for the
// write lock, so the competitor runs while the first transaction is open.
func waitLockWaiter(t *testing.T, env pgEnv) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var n int
		if err := env.Admin.QueryRow(`SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted
			AND database = (SELECT oid FROM pg_database WHERE datname = current_database())`).Scan(&n); err != nil {
			t.Fatalf("pg_locks: %v", err)
		}
		if n > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("the competitor never waited for the first transaction")
}
