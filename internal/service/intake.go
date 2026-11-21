package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pandops/flowai-task-manager/internal/domain"
	"github.com/pandops/flowai-task-manager/internal/ids"
	"github.com/pandops/flowai-task-manager/internal/store"
	"github.com/pandops/flowai-task-manager/internal/wire"
)

const sourceController = "source-controller"

// intakeEntry is one validated task entry of an intake message.
type intakeEntry struct {
	IntakeID      int64
	ExternalID    string
	Payload       json.RawMessage
	ProjectID     *string
	EnvironmentID *int64
}

// rawEntry keeps the identifiers of an entry for its intake_processed
// event, also when the entry is invalid.
type rawEntry struct {
	IntakeID   *int64
	ExternalID *string
}

var allowedEntryKeys = map[string]bool{"intake_id": true, "external_id": true, "payload": true, "project_id": true, "environment_id": true}

// targetFlow is one bound Flow of flow-registry's targets reply.
type targetFlow struct {
	FlowID            int64 `json:"flow_id"`
	ExecutionRevision struct {
		Revision            int64           `json:"revision"`
		ExecutionTag        string          `json:"execution_tag"`
		PromptTemplate      string          `json:"prompt_template"`
		ExecutionParameters json.RawMessage `json:"execution_parameters"`
		DefaultImage        *struct {
			Repository string `json:"repository"`
			Digest     string `json:"digest"`
		} `json:"default_image"`
		FailureRetryCount       *int `json:"failure_retry_count"`
		FailureRetryWaitSeconds int  `json:"failure_retry_wait_seconds"`
		PendingLimitSeconds     int  `json:"pending_limit_seconds"`
		ReleaseDelaySeconds     int  `json:"release_delay_seconds"`
	} `json:"execution_revision"`
}

// defaultRetryCount applies when a revision carries no retry count.
const defaultRetryCount = 3

// HandleIntake processes one SOURCE_CONTROLLER_TASK message.
func (s *Service) HandleIntake(ctx context.Context, m wire.Msg) wire.Ack {
	var env map[string]json.RawMessage
	if err := json.Unmarshal(m.Data, &env); err != nil {
		return wire.Ack{Kind: wire.AckTerm}
	}
	seq, err := ids.ParseJSON(env["publisher_seq"])
	var publisher string
	_ = json.Unmarshal(env["publisher"], &publisher)
	if err != nil || publisher != sourceController {
		return wire.Ack{Kind: wire.AckTerm}
	}
	sourceID, srcErr := ids.ParseJSON(env["source_id"])
	raws, entries, entriesOK := parseEntries(env["tasks"])

	subjectTeam, subjErr := intakeSubjectTeam(m.Subject)
	headerTeam, hdrErr := ids.ParseToken(m.Header[wire.HeaderTeam])
	envTeam, envErr := ids.ParseJSON(env["team_id"])
	if subjErr != nil || hdrErr != nil || envErr != nil || subjectTeam != headerTeam || envTeam != headerTeam {
		// A forged or misrouted message: every entry is rejected and the
		// message is never redelivered.
		eventTeam := envTeam
		if envErr != nil {
			eventTeam = subjectTeam
		}
		if eventTeam == 0 || srcErr != nil {
			return wire.Ack{Kind: wire.AckTerm}
		}
		if err := s.rejectIntake(ctx, eventTeam, sourceID, seq, raws, "invalid_source_task"); err != nil {
			return s.nak(err)
		}
		return wire.Ack{Kind: wire.AckTerm}
	}
	team := headerTeam
	if srcErr != nil {
		return wire.Ack{Kind: wire.AckTerm}
	}
	key := wire.PublisherKey(sourceController, team, 0)
	if done, err := s.cfg.Store.Consumed(ctx, key, seq); err != nil {
		return s.nak(err)
	} else if done {
		return wire.Ack{Kind: wire.AckOK}
	}
	if !entriesOK {
		if err := s.rejectIntake(ctx, team, sourceID, seq, raws, "invalid_source_task"); err != nil {
			return s.nak(err)
		}
		return wire.Ack{Kind: wire.AckOK}
	}
	teamRec, err := s.cfg.Store.Team(ctx, team)
	if err != nil {
		return s.nak(err)
	}
	if teamRec == nil {
		return wire.Ack{Kind: wire.AckNak, Delay: s.cfg.UnknownTeamDelay}
	}
	if teamRec.Archived {
		if err := s.rejectIntake(ctx, team, sourceID, seq, raws, "team_archived"); err != nil {
			return s.nak(err)
		}
		return wire.Ack{Kind: wire.AckOK}
	}
	flows, found, err := s.targets(ctx, team, sourceID)
	if err != nil {
		s.log.Warn("flow-registry targets unavailable", "team_id", team, "source_id", sourceID, "error", err)
		return wire.Ack{Kind: wire.AckNak, Delay: retryNakDelay}
	}
	if !found || len(flows) == 0 {
		if err := s.outcomeOnly(ctx, team, sourceID, seq, raws, "no_target", nil); err != nil {
			return s.nak(err)
		}
		return wire.Ack{Kind: wire.AckOK}
	}
	prompts, ok := renderAll(flows, entries)
	if !ok {
		if err := s.rejectIntake(ctx, team, sourceID, seq, raws, "fanout_too_large"); err != nil {
			return s.nak(err)
		}
		return wire.Ack{Kind: wire.AckOK}
	}
	if err := s.writeOrSkip(ctx, func(tx store.Tx) error {
		return s.createTasks(tx, team, sourceID, seq, entries, flows, prompts)
	}); err != nil {
		return s.nak(err)
	}
	return wire.Ack{Kind: wire.AckOK}
}

func (s *Service) nak(err error) wire.Ack {
	s.log.Warn("message not processed; redelivery requested", "error", err)
	return wire.Ack{Kind: wire.AckNak, Delay: retryNakDelay}
}

func intakeSubjectTeam(subject string) (int64, error) {
	p := strings.Split(subject, ".")
	if len(p) != 4 || p[0] != sourceController || p[1] != "task" || p[3] != "sub" {
		return 0, ids.ErrInvalid
	}
	return ids.ParseToken(p[2])
}

// parseEntries validates the task entries. raws always lists one item per
// entry (or one empty item when the list itself is unusable) for the
// rejection events.
func parseEntries(raw json.RawMessage) ([]rawEntry, []intakeEntry, bool) {
	var list []json.RawMessage
	if err := json.Unmarshal(raw, &list); err != nil || list == nil {
		return []rawEntry{{}}, nil, false
	}
	ok := len(list) >= 1 && len(list) <= maxIntakeEntries
	raws := make([]rawEntry, 0, len(list))
	entries := make([]intakeEntry, 0, len(list))
	for _, item := range list {
		var fields map[string]json.RawMessage
		var r rawEntry
		var e intakeEntry
		if err := json.Unmarshal(item, &fields); err != nil || fields == nil {
			raws = append(raws, r)
			ok = false
			continue
		}
		if v, err := ids.ParseJSON(fields["intake_id"]); err == nil {
			r.IntakeID, e.IntakeID = &v, v
		} else {
			ok = false
		}
		var ext string
		if json.Unmarshal(fields["external_id"], &ext) == nil && ext != "" && len(ext) <= 512 {
			r.ExternalID, e.ExternalID = &ext, ext
		} else {
			ok = false
		}
		raws = append(raws, r)
		for k := range fields {
			if !allowedEntryKeys[k] {
				ok = false
			}
		}
		payload := bytes.TrimSpace(fields["payload"])
		if len(payload) == 0 || payload[0] != '{' {
			ok = false
		}
		e.Payload = json.RawMessage(payload)
		if v, present := fields["project_id"]; present && string(bytes.TrimSpace(v)) != "null" {
			var p string
			if json.Unmarshal(v, &p) != nil {
				ok = false
			}
			e.ProjectID = &p
		}
		if v, present := fields["environment_id"]; present && string(bytes.TrimSpace(v)) != "null" {
			id, err := ids.ParseJSON(v)
			if err != nil {
				ok = false
			}
			e.EnvironmentID = &id
		}
		entries = append(entries, e)
	}
	return raws, entries, ok
}

// targets asks flow-registry for the Flows bound to the Source. found is
// false when there is no binding (404).
func (s *Service) targets(ctx context.Context, team, source int64) ([]targetFlow, bool, error) {
	if s.cfg.Flows == nil {
		return nil, false, errors.New("no flow-registry client")
	}
	data, _ := json.Marshal(map[string]any{"params": map[string]any{"team_id": team, "source_id": source}})
	rep, err := s.cfg.Flows.Request(ctx, "flow-registry.service.targets.pub", map[string]string{wire.HeaderTeam: ids.Token(team),
		wire.HeaderRequestID: ids.Token(s.now().UnixNano()/1000 + 1)}, data)
	if err != nil {
		return nil, false, err
	}
	switch status := replyStatus(rep); status {
	case 200:
	case 404:
		return nil, false, nil
	default:
		return nil, false, errors.New("flow-registry targets status " + ids.Token(int64(status)))
	}
	var body struct {
		TeamID   int64        `json:"team_id"`
		SourceID int64        `json:"source_id"`
		Flows    []targetFlow `json:"flows"`
	}
	if err := json.Unmarshal(rep.Body, &body); err != nil {
		return nil, false, err
	}
	if body.TeamID != team || body.SourceID != source {
		return nil, false, errors.New("flow-registry targets reply names another team or Source")
	}
	return body.Flows, true, nil
}

func replyStatus(r wire.Reply) int {
	if v, ok := r.Header[wire.HeaderStatus]; ok {
		if n, err := ids.ParseToken(v); err == nil {
			return int(n)
		}
	}
	return r.Status
}

// render substitutes {{external_id}}; {{{{ and }}}} are literal braces.
func render(template, externalID string) string {
	var b strings.Builder
	for i := 0; i < len(template); {
		switch {
		case strings.HasPrefix(template[i:], "{{{{"):
			b.WriteString("{{")
			i += 4
		case strings.HasPrefix(template[i:], "}}}}"):
			b.WriteString("}}")
			i += 4
		case strings.HasPrefix(template[i:], "{{external_id}}"):
			b.WriteString(externalID)
			i += len("{{external_id}}")
		default:
			b.WriteByte(template[i])
			i++
		}
	}
	return b.String()
}

// renderAll renders every (entry, Flow) prompt; false when the fan-out is
// too large.
func renderAll(flows []targetFlow, entries []intakeEntry) ([][]string, bool) {
	if len(flows) > maxFlows {
		return nil, false
	}
	total := 0
	out := make([][]string, len(entries))
	for i, e := range entries {
		out[i] = make([]string, len(flows))
		for j, f := range flows {
			p := render(f.ExecutionRevision.PromptTemplate, e.ExternalID)
			total += len(p)
			if total > maxRenderedBytes {
				return nil, false
			}
			out[i][j] = p
		}
	}
	return out, true
}

func (s *Service) processed(tx store.Tx, team, source, seq int64, r rawEntry, outcome string, reason *string, taskIDs, dedup []int64) error {
	if taskIDs == nil {
		taskIDs = []int64{}
	}
	if dedup == nil {
		dedup = []int64{}
	}
	_, err := s.event(tx, "intake_processed", team, map[string]any{"intake_publisher_seq": seq, "intake_id": nullable(r.IntakeID),
		"source_id": source, "external_id": nullable(r.ExternalID), "outcome": outcome, "reason": nullable(reason),
		"task_ids": taskIDs, "deduplicated_flow_ids": dedup})
	return err
}

// outcomeOnly records one outcome for every entry without creating tasks.
func (s *Service) outcomeOnly(ctx context.Context, team, source, seq int64, raws []rawEntry, outcome string, reason *string) error {
	key := wire.PublisherKey(sourceController, team, 0)
	return s.writeOrSkip(ctx, func(tx store.Tx) error {
		if done, err := tx.Consumed(key, seq); err != nil {
			return err
		} else if done {
			return errSkip
		}
		if err := tx.MarkConsumed(key, seq); err != nil {
			return err
		}
		for _, r := range raws {
			if err := s.processed(tx, team, source, seq, r, outcome, reason, nil, nil); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Service) rejectIntake(ctx context.Context, team, source, seq int64, raws []rawEntry, reason string) error {
	return s.outcomeOnly(ctx, team, source, seq, raws, "rejected", &reason)
}

// createTasks is the intake transaction: dedupe over current tasks, one
// new task per (entry, Flow) and one intake_processed per entry.
func (s *Service) createTasks(tx store.Tx, team, source, seq int64, entries []intakeEntry, flows []targetFlow, prompts [][]string) error {
	key := wire.PublisherKey(sourceController, team, 0)
	if done, err := tx.Consumed(key, seq); err != nil {
		return err
	} else if done {
		return errSkip
	}
	if err := tx.MarkConsumed(key, seq); err != nil {
		return err
	}
	teamRec, err := tx.Team(team)
	if err != nil {
		return err
	}
	if teamRec == nil || teamRec.TeamName == "" {
		// The first read found the row and rows are never deleted: a miss
		// here is an internal error, retried by redelivery.
		return fmt.Errorf("service: team %d has no projected team_name", team)
	}
	if teamRec.Archived {
		reason := "team_archived"
		for _, e := range entries {
			r := rawEntry{IntakeID: ptr(e.IntakeID), ExternalID: ptr(e.ExternalID)}
			if err := s.processed(tx, team, source, seq, r, "rejected", &reason, nil, nil); err != nil {
				return err
			}
		}
		return nil
	}
	now := s.now()
	for i, e := range entries {
		var created, dedup []int64
		for j, f := range flows {
			if _, found, err := tx.FindCurrent(team, source, e.ExternalID, f.FlowID); err != nil {
				return err
			} else if found {
				dedup = append(dedup, f.FlowID)
				continue
			}
			id, err := tx.NextTeamID(team, store.KindTask)
			if err != nil {
				return err
			}
			t := newTask(team, id, source, seq, now, e, f, prompts[i][j])
			t.TaskName = teamRec.TeamName + "-" + strconv.FormatInt(id, 10)
			if err := s.publishTask(tx, t); err != nil {
				return err
			}
			if err := tx.SaveTask(t); err != nil {
				return err
			}
			created = append(created, id)
		}
		outcome := "accepted"
		if len(created) == 0 {
			outcome = "deduplicated"
		}
		r := rawEntry{IntakeID: ptr(e.IntakeID), ExternalID: ptr(e.ExternalID)}
		if err := s.processed(tx, team, source, seq, r, outcome, nil, created, dedup); err != nil {
			return err
		}
	}
	return nil
}

func newTask(team, id, source, seq int64, now time.Time, e intakeEntry, f targetFlow, prompt string) *domain.Task {
	rev := f.ExecutionRevision
	retry := defaultRetryCount
	if rev.FailureRetryCount != nil {
		retry = *rev.FailureRetryCount
	}
	var override *string
	if rev.DefaultImage != nil && rev.DefaultImage.Repository != "" && rev.DefaultImage.Digest != "" {
		override = ptr(rev.DefaultImage.Repository + "@" + rev.DefaultImage.Digest)
	}
	params := rev.ExecutionParameters
	if len(params) == 0 || string(params) == "null" {
		params = json.RawMessage(`{}`)
	}
	return &domain.Task{
		TeamID: team, TaskID: id, SourceID: source, ExternalID: e.ExternalID, FlowID: f.FlowID, RequiredTag: rev.ExecutionTag,
		IntakeID: e.IntakeID, IntakePublisherSeq: seq, IngestedAt: now, Payload: e.Payload, ProjectID: e.ProjectID,
		EnvironmentID: e.EnvironmentID, Attempt: 1,
		Snapshot: domain.Snapshot{ExecutionRevision: rev.Revision, RenderedPrompt: prompt, ExecutionParameters: params,
			ImageOverride: override, FailureRetryCount: retry, RetryWaitSeconds: rev.FailureRetryWaitSeconds,
			PendingLimitSeconds: rev.PendingLimitSeconds, ReleaseDelaySeconds: rev.ReleaseDelaySeconds},
		Attempts: []domain.Attempt{{Attempt: 1, State: domain.StateQueued, Claims: []domain.Claim{}}},
		History:  []domain.History{},
	}
}
