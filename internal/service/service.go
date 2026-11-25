// Package service is task-manager's behavior: intake and fan-out, the claim
// lifecycle from Executor events, releases, retries, cancels, archive, the
// claim-token, claim-current and executor-claims answers and the team and
// admin reads. Every state change commits with its outbox rows in one
// store transaction; the outbox relay publishes them after commit.
package service

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"time"

	"github.com/pandops/flowai-task-manager/internal/claimtoken"
	"github.com/pandops/flowai-task-manager/internal/cursor"
	"github.com/pandops/flowai-task-manager/internal/domain"
	"github.com/pandops/flowai-task-manager/internal/ids"
	"github.com/pandops/flowai-task-manager/internal/store"
	"github.com/pandops/flowai-task-manager/internal/wire"
)

// Requester sends core NATS requests (flow-registry.service.targets).
type Requester interface {
	Request(ctx context.Context, subject string, header map[string]string, data []byte) (wire.Reply, error)
}

// Config wires a Service.
type Config struct {
	Store   store.Store
	Flows   Requester
	Signer  *claimtoken.Signer
	Cursors *cursor.Codec
	Now     func() time.Time
	Log     *slog.Logger
	// Committed is called after a transaction that wrote outbox rows commits
	// (the relay's wake-up). Optional.
	Committed func()
	// UnknownTeamDelay is the Nak delay of an intake message whose team is
	// not yet projected.
	UnknownTeamDelay time.Duration
}

// Service is task-manager.
type Service struct {
	cfg Config
	log *slog.Logger
}

// Limits of one intake message (design rules 3 and 4).
const (
	maxFlows          = 32
	maxRenderedBytes  = 1 << 20
	maxIntakeEntries  = 100
	missingHeartbeats = 3
	dueBatch          = 100
	retryNakDelay     = time.Second
)

// New returns a Service.
func New(cfg Config) *Service {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.UnknownTeamDelay <= 0 {
		cfg.UnknownTeamDelay = 5 * time.Second
	}
	log := cfg.Log
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Service{cfg: cfg, log: log}
}

// now is the service clock in UTC with millisecond precision, the precision
// of every published timestamp.
func (s *Service) now() time.Time { return s.cfg.Now().UTC().Truncate(time.Millisecond) }

// write runs fn in one transaction and wakes the relay after a commit.
func (s *Service) write(ctx context.Context, fn func(store.Tx) error) error {
	if err := s.cfg.Store.Write(ctx, fn); err != nil {
		return err
	}
	if s.cfg.Committed != nil {
		s.cfg.Committed()
	}
	return nil
}

// errSkip ends a transaction without committing it.
var errSkip = errors.New("service: nothing to commit")

func (s *Service) writeOrSkip(ctx context.Context, fn func(store.Tx) error) error {
	err := s.write(ctx, fn)
	if errors.Is(err, errSkip) {
		return nil
	}
	return err
}

// emit appends one stream message to the outbox with the next
// publisher_seq and returns that sequence.
func (s *Service) emit(tx store.Tx, subject string, team string, body map[string]any, ensureTeam int64, ensureTag string) (int64, error) {
	seq, err := tx.NextPublisherSeq()
	if err != nil {
		return 0, err
	}
	body["publisher"] = wire.Service
	body["publisher_seq"] = seq
	if _, ok := body["occurred_at"]; !ok {
		body["occurred_at"] = wire.FormatTime(s.now())
	}
	data, err := json.Marshal(body)
	if err != nil {
		return 0, err
	}
	err = tx.AppendOutbox(store.OutboxMessage{PublisherSeq: seq, Subject: subject,
		Header: map[string]string{wire.HeaderTeam: team, wire.HeaderMsgID: wire.MsgID(seq)},
		Data:   data, EnsureTeam: ensureTeam, EnsureTag: ensureTag})
	return seq, err
}

// event emits task-manager.event.<kind>.pub for a team.
func (s *Service) event(tx store.Tx, kind string, team int64, body map[string]any) (int64, error) {
	body["kind"] = kind
	body["team_id"] = team
	return s.emit(tx, wire.EventSubject(kind), ids.Token(team), body, 0, "")
}

func nullable[T any](v *T) any {
	if v == nil {
		return nil
	}
	return *v
}

func ptr[T any](v T) *T { return &v }

// ---- shared lifecycle transitions ---------------------------------------

// resolveImage picks the Flow image override or the team default.
func resolveImage(t *domain.Task, team *store.Team) (string, string) {
	if t.Snapshot.ImageOverride != nil && *t.Snapshot.ImageOverride != "" {
		return *t.Snapshot.ImageOverride, domain.ImageFlowDefault
	}
	image := ""
	if team != nil {
		image = team.DefaultImage
	}
	return image, domain.ImageTeamDefault
}

// publishTask makes the next claim_seq of the current attempt claimable:
// the task message, then the queued event, both in this transaction.
func (s *Service) publishTask(tx store.Tx, t *domain.Task) error {
	team, err := tx.Team(t.TeamID)
	if err != nil {
		return err
	}
	image, source := resolveImage(t, team)
	t.ClaimSeq++
	t.State = domain.StateQueued
	t.NextDueAt, t.NextDueKind = nil, ""
	t.QueuedImage, t.QueuedImageSource = image, source
	a := t.CurrentAttempt()
	a.State = domain.StateQueued
	params := t.Snapshot.ExecutionParameters
	if len(params) == 0 {
		params = json.RawMessage(`{}`)
	}
	payload := t.Payload
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	msg := map[string]any{
		"team_id": t.TeamID, "task_id": t.TaskID, "flow_id": t.FlowID, "source_id": t.SourceID, "external_id": t.ExternalID,
		"intake_id": t.IntakeID, "attempt": t.Attempt, "claim_seq": t.ClaimSeq, "required_tag": t.RequiredTag,
		"payload": payload, "project_id": nullable(t.ProjectID), "environment_id": nullable(t.EnvironmentID),
		"execution": map[string]any{"execution_revision_id": t.Snapshot.ExecutionRevision, "required_tag": t.RequiredTag,
			"rendered_prompt": t.Snapshot.RenderedPrompt, "execution_parameters": params},
		"resolved_image": image, "image_source": source, "pending_limit_seconds": t.Snapshot.PendingLimitSeconds,
	}
	taskSeq, err := s.emit(tx, wire.TaskSubject(t.TeamID, t.RequiredTag, t.FlowID), ids.Token(t.TeamID), msg, t.TeamID, t.RequiredTag)
	if err != nil {
		return err
	}
	seq, err := s.event(tx, "queued", t.TeamID, map[string]any{"task_id": t.TaskID, "task_name": t.TaskName, "attempt": t.Attempt, "claim_seq": t.ClaimSeq,
		"flow_id": t.FlowID, "source_id": t.SourceID, "required_tag": t.RequiredTag, "intake_id": t.IntakeID, "task_publisher_seq": taskSeq})
	if err != nil {
		return err
	}
	t.AddHistory(domain.History{Kind: "queued", Publisher: wire.Service, PublisherSeq: seq, OccurredAt: s.now(),
		Attempt: t.Attempt, ClaimSeq: ptr(t.ClaimSeq)})
	return nil
}

// schedule makes a released or failed task wait until it is republished.
func (s *Service) schedule(t *domain.Task, kind string, after time.Duration) time.Time {
	due := s.now().Add(after)
	t.State = domain.StateQueued
	t.NextDueAt, t.NextDueKind = &due, kind
	t.CurrentAttempt().State = domain.StateQueued
	return due
}

// afterRelease ends the claim holder's part: a pending cancel completes
// directly, otherwise the task waits the release delay.
func (s *Service) afterRelease(tx store.Tx, t *domain.Task) error {
	if t.PendingControl != nil {
		return s.directCancel(tx, t, *t.PendingControl)
	}
	s.schedule(t, domain.DueRelease, time.Duration(t.Snapshot.ReleaseDelaySeconds)*time.Second)
	return tx.SaveTask(t)
}

// releaseByTaskManager ends the current claim for executor_lost or
// claim_missing and publishes released.
func (s *Service) releaseByTaskManager(tx store.Tx, t *domain.Task, reason string, lostSeq *int64) error {
	c := t.CurrentClaim()
	if c == nil || !t.Held() {
		return nil
	}
	now := s.now()
	c.ReleasedReason, c.EndedAt = ptr(reason), &now
	var republishAt any
	if t.PendingControl == nil {
		republishAt = wire.FormatTime(now.Add(time.Duration(t.Snapshot.ReleaseDelaySeconds) * time.Second))
	}
	seq, err := s.event(tx, "released", t.TeamID, map[string]any{"task_id": t.TaskID, "task_name": t.TaskName, "attempt": t.Attempt, "claim_seq": t.ClaimSeq,
		"executor_id": c.ExecutorID, "reason": reason, "republish_at": republishAt, "lost_publisher_seq": nullable(lostSeq)})
	if err != nil {
		return err
	}
	t.AddHistory(domain.History{Kind: "released", Publisher: wire.Service, PublisherSeq: seq, OccurredAt: now,
		Attempt: t.Attempt, ClaimSeq: ptr(t.ClaimSeq), ExecutorID: ptr(c.ExecutorID), Reason: ptr(reason)})
	return s.afterRelease(tx, t)
}

// directCancel cancels a task that no Executor holds and archives it.
func (s *Service) directCancel(tx store.Tx, t *domain.Task, controlID int64) error {
	var claimSeq, executor any
	if c := t.CurrentClaim(); c != nil && t.Held() {
		claimSeq, executor = c.ClaimSeq, c.ExecutorID
		now := s.now()
		if c.EndedAt == nil {
			c.EndedAt = &now
		}
	}
	seq, err := s.event(tx, "cancelled", t.TeamID, map[string]any{"task_id": t.TaskID, "task_name": t.TaskName, "attempt": t.Attempt,
		"claim_seq": claimSeq, "executor_id": executor, "control_id": controlID})
	if err != nil {
		return err
	}
	t.AddHistory(domain.History{Kind: "cancelled", Publisher: wire.Service, PublisherSeq: seq, OccurredAt: s.now(), Attempt: t.Attempt})
	if err := s.markControl(tx, t.TeamID, controlID, "cancelled"); err != nil {
		return err
	}
	return s.finalize(tx, t, domain.StateCancelled, ptr("cancelled"))
}

func (s *Service) markControl(tx store.Tx, team, controlID int64, status string) error {
	c, err := tx.ControlByID(team, controlID)
	if err != nil || c == nil {
		return err
	}
	c.Status = status
	return tx.PutControl(*c)
}

// finalize moves the task into its terminal state, archives it and then
// publishes archived in the same transaction.
func (s *Service) finalize(tx store.Tx, t *domain.Task, state string, reason *string) error {
	now := s.now()
	t.State = state
	t.CompletedAt, t.FinalReason = &now, reason
	t.NextDueAt, t.NextDueKind = nil, ""
	t.CurrentAttempt().State = state
	if err := tx.ArchiveTask(t, now); err != nil {
		return err
	}
	_, err := s.event(tx, "archived", t.TeamID, map[string]any{"task_id": t.TaskID, "task_name": t.TaskName, "flow_id": t.FlowID, "source_id": t.SourceID,
		"final_state": state, "final_reason": nullable(reason), "attempts": int64(len(t.Attempts))})
	return err
}
