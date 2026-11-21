package service

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/pandops/flowai-task-manager/internal/domain"
	"github.com/pandops/flowai-task-manager/internal/ids"
	"github.com/pandops/flowai-task-manager/internal/store"
	"github.com/pandops/flowai-task-manager/internal/wire"
)

// executorEvent is the flat envelope of an Executor lifecycle event.
type executorEvent struct {
	PublisherSeq  json.RawMessage `json:"publisher_seq"`
	Kind          string          `json:"kind"`
	OccurredAt    string          `json:"occurred_at"`
	TaskID        json.RawMessage `json:"task_id"`
	Attempt       json.RawMessage `json:"attempt"`
	ClaimSeq      json.RawMessage `json:"claim_seq"`
	Reason        string          `json:"reason"`
	FailureReason string          `json:"failure_reason"`
	Retryable     *bool           `json:"retryable"`
	ControlID     json.RawMessage `json:"control_id"`
	ImageSource   string          `json:"image_source"`
}

// HandleEvent processes one ALL_EVENT message.
func (s *Service) HandleEvent(ctx context.Context, m wire.Msg) wire.Ack {
	p := strings.Split(m.Subject, ".")
	switch {
	case len(p) == 4 && p[0] == "team-registry" && p[1] == "event" && p[3] == "sub":
		return s.teamEvent(ctx, p[2], m)
	case len(p) == 4 && p[0] == "executor-registry" && p[1] == "event" && p[3] == "sub":
		return s.registryEvent(ctx, p[2], m)
	case len(p) == 5 && p[0] == "executor" && p[1] == "event" && p[4] == "sub":
		exec, err := ids.ParseToken(p[2])
		if err != nil {
			return wire.Ack{Kind: wire.AckTerm}
		}
		return s.executorEvent(ctx, exec, p[3], m)
	}
	// Other services' events (and task-manager's own) are not consumed.
	return wire.Ack{Kind: wire.AckOK}
}

func (s *Service) teamEvent(ctx context.Context, kind string, m wire.Msg) wire.Ack {
	var ev struct {
		PublisherSeq json.RawMessage `json:"publisher_seq"`
		TeamID       json.RawMessage `json:"team_id"`
		DefaultImage *string         `json:"default_image"`
		ArchivedAt   *string         `json:"archived_at"`
		TeamName     *string         `json:"team_name"`
	}
	if err := json.Unmarshal(m.Data, &ev); err != nil {
		return wire.Ack{Kind: wire.AckTerm}
	}
	seq, err1 := ids.ParseJSON(ev.PublisherSeq)
	team, err2 := ids.ParseJSON(ev.TeamID)
	if err1 != nil || err2 != nil {
		return wire.Ack{Kind: wire.AckTerm}
	}
	if kind != "team_created" && kind != "team_updated" && kind != "team_archived" {
		return wire.Ack{Kind: wire.AckOK}
	}
	if ev.TeamName == nil || *ev.TeamName == "" {
		// team-registry's schema requires a non-empty team_name.
		return wire.Ack{Kind: wire.AckTerm}
	}
	err := s.writeOrSkip(ctx, func(tx store.Tx) error {
		if done, err := tx.Consumed("team-registry", seq); err != nil {
			return err
		} else if done {
			return errSkip
		}
		if err := tx.MarkConsumed("team-registry", seq); err != nil {
			return err
		}
		cur, err := tx.Team(team)
		if err != nil {
			return err
		}
		next := store.Team{TeamID: team}
		if cur != nil {
			next = *cur
		}
		if ev.DefaultImage != nil {
			next.DefaultImage = *ev.DefaultImage
		}
		// Every team event carries the team's immutable name.
		next.TeamName = *ev.TeamName
		if kind == "team_archived" || ev.ArchivedAt != nil {
			next.Archived = true
		}
		return tx.PutTeam(next)
	})
	if err != nil {
		return s.nak(err)
	}
	return wire.Ack{Kind: wire.AckOK}
}

func (s *Service) registryEvent(ctx context.Context, kind string, m wire.Msg) wire.Ack {
	var ev struct {
		PublisherSeq json.RawMessage `json:"publisher_seq"`
		TeamID       json.RawMessage `json:"team_id"`
		ExecutorID   json.RawMessage `json:"executor_id"`
		ExecutionTag string          `json:"execution_tag"`
	}
	if err := json.Unmarshal(m.Data, &ev); err != nil {
		return wire.Ack{Kind: wire.AckTerm}
	}
	seq, err1 := ids.ParseJSON(ev.PublisherSeq)
	team, err2 := ids.ParseJSON(ev.TeamID)
	exec, err3 := ids.ParseJSON(ev.ExecutorID)
	if err1 != nil || err2 != nil || err3 != nil {
		return wire.Ack{Kind: wire.AckTerm}
	}
	if kind != "executor_registered" && kind != "executor_removed" && kind != "executor_lost" {
		return wire.Ack{Kind: wire.AckOK}
	}
	const key = "executor-registry"
	err := s.writeOrSkip(ctx, func(tx store.Tx) error {
		if done, err := tx.Consumed(key, seq); err != nil {
			return err
		} else if done {
			return errSkip
		}
		if err := tx.MarkConsumed(key, seq); err != nil {
			return err
		}
		cur, err := tx.Executor(exec)
		if err != nil {
			return err
		}
		switch kind {
		case "executor_registered":
			next := store.Executor{ExecutorID: exec, TeamID: team, Tag: ev.ExecutionTag}
			if cur != nil {
				next.LostSeqApplied = cur.LostSeqApplied
			}
			return tx.PutExecutor(next)
		case "executor_removed":
			next := store.Executor{ExecutorID: exec, TeamID: team, Tag: ev.ExecutionTag, Removed: true}
			if cur != nil {
				next.LostSeqApplied = cur.LostSeqApplied
				if next.Tag == "" {
					next.Tag = cur.Tag
				}
			}
			return tx.PutExecutor(next)
		default: // executor_lost
			if cur == nil {
				return nil
			}
			return s.applyLost(tx, cur, seq)
		}
	})
	if err != nil {
		return s.nak(err)
	}
	return wire.Ack{Kind: wire.AckOK}
}

// applyLost releases every claim the Executor holds once per executor_lost
// publisher_seq, whether it arrives as the event or through executor-claims.
func (s *Service) applyLost(tx store.Tx, exec *store.Executor, lostSeq int64) error {
	if lostSeq <= exec.LostSeqApplied {
		return nil
	}
	held, err := tx.TasksHeldBy(exec.ExecutorID)
	if err != nil {
		return err
	}
	for _, t := range held {
		if t.TeamID != exec.TeamID {
			continue
		}
		if err := s.releaseByTaskManager(tx, t, domain.ReasonExecutorLost, ptr(lostSeq)); err != nil {
			return err
		}
	}
	exec.LostSeqApplied = lostSeq
	return tx.PutExecutor(*exec)
}

func (s *Service) executorEvent(ctx context.Context, execID int64, kind string, m wire.Msg) wire.Ack {
	var ev executorEvent
	if err := json.Unmarshal(m.Data, &ev); err != nil {
		return wire.Ack{Kind: wire.AckTerm}
	}
	seq, err := ids.ParseJSON(ev.PublisherSeq)
	if err != nil || ev.Kind != kind {
		return wire.Ack{Kind: wire.AckTerm}
	}
	exec, err := s.cfg.Store.Executor(ctx, execID)
	if err != nil {
		return s.nak(err)
	}
	if exec == nil {
		// executor-registry's registration has not been projected yet.
		return wire.Ack{Kind: wire.AckNak, Delay: s.cfg.UnknownTeamDelay}
	}
	// The team is the Executor's own team; header and envelope grant nothing.
	key := wire.PublisherKey("executor", exec.TeamID, execID)
	taskID, errT := ids.ParseJSON(ev.TaskID)
	attempt, errA := ids.ParseJSON(ev.Attempt)
	claim, errC := ids.ParseJSON(ev.ClaimSeq)
	occurred, errO := time.Parse(time.RFC3339Nano, ev.OccurredAt)
	if errO != nil {
		occurred = s.now()
	}
	malformed := errT != nil || errA != nil || errC != nil
	err = s.writeOrSkip(ctx, func(tx store.Tx) error {
		if done, err := tx.Consumed(key, seq); err != nil {
			return err
		} else if done {
			return errSkip
		}
		if err := tx.MarkConsumed(key, seq); err != nil {
			return err
		}
		if malformed {
			return nil
		}
		t, err := tx.Task(exec.TeamID, taskID)
		if err != nil || t == nil {
			return err
		}
		return s.applyExecutorEvent(tx, t, exec, ev, seq, attempt, claim, occurred.UTC())
	})
	if err != nil {
		return s.nak(err)
	}
	return wire.Ack{Kind: wire.AckOK}
}

// applyExecutorEvent changes state only for an event of the current claim
// from its holder in a permitted order (design rules 19-23).
func (s *Service) applyExecutorEvent(tx store.Tx, t *domain.Task, exec *store.Executor, ev executorEvent, seq, attempt, claimSeq int64, occurred time.Time) error {
	hist := domain.History{Kind: ev.Kind, Publisher: "executor", PublisherSeq: seq, OccurredAt: occurred,
		Attempt: attempt, ClaimSeq: ptr(claimSeq), ExecutorID: ptr(exec.ExecutorID)}
	if ev.Kind == "created" {
		if t.State != domain.StateQueued || t.NextDueAt != nil || attempt != t.Attempt || claimSeq != t.ClaimSeq || exec.Removed ||
			t.CurrentClaim() != nil {
			return nil
		}
		now := s.now()
		a := t.CurrentAttempt()
		a.Claims = append(a.Claims, domain.Claim{Attempt: attempt, ClaimSeq: claimSeq, ExecutorID: exec.ExecutorID,
			CreatedPublisherSeq: seq, ClaimedAt: now, ResolvedImage: t.QueuedImage, ImageSource: t.QueuedImageSource,
			PendingReasons: []string{}, LastEventSeq: seq})
		a.State = domain.StateCreated
		t.State = domain.StateCreated
		t.AddHistory(hist)
		if _, err := s.event(tx, "claim_accepted", t.TeamID, map[string]any{"task_id": t.TaskID, "task_name": t.TaskName, "attempt": attempt,
			"claim_seq": claimSeq, "executor_id": exec.ExecutorID, "created_publisher_seq": seq}); err != nil {
			return err
		}
		return tx.SaveTask(t)
	}
	c := t.CurrentClaim()
	if c == nil || !t.Held() || attempt != t.Attempt || claimSeq != t.ClaimSeq || c.ExecutorID != exec.ExecutorID || seq <= c.LastEventSeq {
		return nil
	}
	a := t.CurrentAttempt()
	now := s.now()
	switch ev.Kind {
	case "pending":
		if t.State != domain.StateCreated && t.State != domain.StatePending {
			return nil
		}
		if ev.Reason != "" {
			c.PendingReasons = append(c.PendingReasons, ev.Reason)
			hist.Reason = ptr(ev.Reason)
		}
		t.State, a.State = domain.StatePending, domain.StatePending
	case "running":
		if t.State != domain.StateCreated && t.State != domain.StatePending {
			return nil
		}
		t.State, a.State = domain.StateRunning, domain.StateRunning
	case "control_acknowledged":
		c.LastEventSeq = seq
		return tx.SaveTask(t)
	case "released":
		switch ev.Reason {
		case domain.ReasonPendingTimeout:
			if t.State == domain.StateRunning {
				return nil
			}
		case domain.ReasonExecutorRestarted, domain.ReasonExecutorShutdown:
		default: // executor_lost and claim_missing are task-manager's own reasons
			return nil
		}
		c.LastEventSeq = seq
		c.ReleasedReason, c.EndedAt = ptr(ev.Reason), &now
		hist.Reason = ptr(ev.Reason)
		t.AddHistory(hist)
		return s.afterRelease(tx, t)
	case "finished":
		if t.State != domain.StateRunning {
			return nil
		}
		c.LastEventSeq, c.EndedAt = seq, &now
		t.AddHistory(hist)
		return s.finalize(tx, t, domain.StateFinished, nil)
	case "failed":
		c.LastEventSeq, c.EndedAt = seq, &now
		reason := ev.FailureReason
		a.FailureReason, a.Retryable = ptr(reason), ev.Retryable
		hist.Reason = ptr(reason)
		t.AddHistory(hist)
		if t.PendingControl != nil {
			return s.directCancel(tx, t, *t.PendingControl)
		}
		retryable := ev.Retryable != nil && *ev.Retryable
		if retryable && int(t.Attempt-1) < t.Snapshot.FailureRetryCount {
			a.State = domain.StateFailed
			wait := time.Duration(t.Snapshot.RetryWaitSeconds) * time.Second
			due := now.Add(wait)
			if _, err := s.event(tx, "retry_scheduled", t.TeamID, map[string]any{"task_id": t.TaskID, "task_name": t.TaskName, "attempt": t.Attempt,
				"next_attempt": t.Attempt + 1, "due_at": wire.FormatTime(due), "failure_reason": reason}); err != nil {
				return err
			}
			t.Attempt++
			t.ClaimSeq = 0
			s.schedule(t, domain.DueRetry, wait)
			return tx.SaveTask(t)
		}
		return s.finalize(tx, t, domain.StateFailed, ptr(reason))
	case "cancelled":
		control, perr := ids.ParseJSON(ev.ControlID)
		if valid := perr == nil; !valid || t.PendingControl == nil || control != *t.PendingControl {
			return nil
		}
		c.LastEventSeq, c.EndedAt = seq, &now
		t.AddHistory(hist)
		if err := s.markControl(tx, t.TeamID, control, "cancelled"); err != nil {
			return err
		}
		return s.finalize(tx, t, domain.StateCancelled, ptr("cancelled"))
	default:
		return nil
	}
	c.LastEventSeq = seq
	t.AddHistory(hist)
	return tx.SaveTask(t)
}
