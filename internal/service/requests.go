package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/pandops/flowai-task-manager/internal/claimtoken"
	"github.com/pandops/flowai-task-manager/internal/domain"
	"github.com/pandops/flowai-task-manager/internal/ids"
	"github.com/pandops/flowai-task-manager/internal/store"
	"github.com/pandops/flowai-task-manager/internal/wire"
)

// errReply carries an error reply out of a transaction.
type errReply struct{ reply wire.Reply }

func (e errReply) Error() string { return "reply " + ids.Token(int64(e.reply.Status)) }

// HandleRequest answers one core NATS request on a served subject.
func (s *Service) HandleRequest(ctx context.Context, m wire.Msg) wire.Reply {
	r := s.route(ctx, m)
	if r.Header == nil {
		r.Header = map[string]string{}
	}
	r.Header[wire.HeaderStatus] = ids.Token(int64(r.Status))
	return r
}

func badRequest(code, msg string, requestID *int64) wire.Reply {
	return wire.Error(400, code, msg, requestID)
}

func internalError(requestID *int64) wire.Reply {
	return wire.Error(500, "internal_error", "the request could not be completed", requestID)
}

func (s *Service) route(ctx context.Context, m wire.Msg) wire.Reply {
	p := strings.Split(m.Subject, ".")
	if len(p) < 4 || p[0] != wire.Service || p[len(p)-1] != "sub" {
		return wire.Error(404, "not_found", "unknown operation", nil)
	}
	req, err := wire.DecodeRequest(m.Data)
	requestID := headerID(m.Header, wire.HeaderRequestID)
	if err != nil {
		return badRequest("invalid_request", "the request is not a valid envelope", requestID)
	}
	switch {
	case len(p) == 4 && p[1] == "ui":
		return s.ui(ctx, p[2], m.Header, req)
	case len(p) == 4 && p[1] == "admin" && p[2] == "list-tasks":
		return s.adminList(ctx, m.Header, req)
	case len(p) == 4 && p[1] == "service" && p[2] == "claim-current":
		return s.claimCurrent(ctx, m.Header, req)
	case len(p) == 4 && p[1] == "service" && p[2] == "executor-claims":
		return s.executorClaims(ctx, m.Header, req)
	case len(p) == 5 && p[1] == "executor" && p[3] == "claim-token":
		exec, err := ids.ParseToken(p[2])
		if err != nil {
			return badRequest("invalid_request", "invalid Executor in the subject", requestID)
		}
		return s.claimToken(ctx, exec, requestID, req)
	}
	return wire.Error(404, "not_found", "unknown operation", requestID)
}

func headerID(h map[string]string, name string) *int64 {
	v, err := ids.ParseToken(h[name])
	if err != nil {
		return nil
	}
	return &v
}

// serviceTeam validates the header team of a service request: a decimal
// team, never admin.
func serviceTeam(h map[string]string) (int64, *int64, bool) {
	team, err := ids.ParseToken(h[wire.HeaderTeam])
	requestID := headerID(h, wire.HeaderRequestID)
	return team, requestID, err == nil && requestID != nil
}

// strictBody decodes a request body into v, rejecting unknown fields.
func strictBody(raw json.RawMessage, v any) error {
	if len(bytes.TrimSpace(raw)) == 0 {
		return errors.New("missing body")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

func onlyParams(req wire.Request, allowed ...string) bool {
	for k := range req.Params {
		ok := false
		for _, a := range allowed {
			if k == a {
				ok = true
			}
		}
		if !ok {
			return false
		}
	}
	return true
}

// ---- claim token -------------------------------------------------------

func (s *Service) claimToken(ctx context.Context, execID int64, requestID *int64, req wire.Request) wire.Reply {
	var body struct {
		TaskID              json.RawMessage `json:"task_id"`
		Attempt             json.RawMessage `json:"attempt"`
		ClaimSeq            json.RawMessage `json:"claim_seq"`
		CreatedPublisherSeq json.RawMessage `json:"created_publisher_seq"`
	}
	if len(req.Params) != 0 || strictBody(req.Body, &body) != nil {
		return badRequest("invalid_request", "invalid claim-token request", requestID)
	}
	task, e1 := ids.ParseJSON(body.TaskID)
	attempt, e2 := ids.ParseJSON(body.Attempt)
	claim, e3 := ids.ParseJSON(body.ClaimSeq)
	created, e4 := ids.ParseJSON(body.CreatedPublisherSeq)
	if e1 != nil || e2 != nil || e3 != nil || e4 != nil {
		return badRequest("invalid_request", "invalid claim-token request", requestID)
	}
	exec, err := s.cfg.Store.Executor(ctx, execID)
	if err != nil {
		return internalError(requestID)
	}
	if exec == nil {
		return wire.Error(503, "service_unavailable", "the Executor is not known yet", requestID)
	}
	done, err := s.cfg.Store.Consumed(ctx, wire.PublisherKey("executor", exec.TeamID, execID), created)
	if err != nil {
		return internalError(requestID)
	}
	if !done {
		return wire.Error(409, "claim_not_processed", "the created event has not been processed yet", requestID)
	}
	rejected := wire.Error(409, "claim_rejected", "the claim is not held by this Executor", requestID)
	t, err := s.cfg.Store.GetTask(ctx, exec.TeamID, task)
	if err != nil {
		return internalError(requestID)
	}
	if t == nil || t.Attempt != attempt || t.ClaimSeq != claim || !t.Held() {
		return rejected
	}
	c := t.CurrentClaim()
	if c.ExecutorID != execID || c.CreatedPublisherSeq != created {
		return rejected
	}
	if s.cfg.Signer == nil {
		return internalError(requestID)
	}
	token, err := s.cfg.Signer.Sign(claimtoken.Payload{TeamID: t.TeamID, TaskID: t.TaskID, Attempt: attempt, ClaimSeq: claim, ExecutorID: execID})
	if err != nil {
		return internalError(requestID)
	}
	return wire.JSON(200, map[string]any{"claim_token": token, "task_id": t.TaskID, "attempt": attempt, "claim_seq": claim})
}

// ---- claim currency ----------------------------------------------------

func (s *Service) claimCurrent(ctx context.Context, h map[string]string, req wire.Request) wire.Reply {
	team, requestID, ok := serviceTeam(h)
	if !ok {
		return badRequest("invalid_request", "missing or invalid request context", requestID)
	}
	if !onlyParams(req, "team_id", "task_id", "attempt", "claim_seq", "executor_id") {
		return badRequest("invalid_request", "unknown parameter", requestID)
	}
	var v [5]int64
	for i, name := range []string{"team_id", "task_id", "attempt", "claim_seq", "executor_id"} {
		id, present, err := req.ID(name)
		if !present || err != nil {
			return badRequest("invalid_request", "invalid "+name, requestID)
		}
		v[i] = id
	}
	if v[0] != team {
		return badRequest("invalid_request", "team_id differs from the request team", requestID)
	}
	t, err := s.cfg.Store.GetTask(ctx, team, v[1])
	if err != nil {
		return internalError(requestID)
	}
	notCurrent := func(reason string) wire.Reply {
		return wire.JSON(200, map[string]any{"current": false, "reason": reason})
	}
	if t == nil {
		return notCurrent("unknown_task")
	}
	if domain.Terminal(t.State) {
		return notCurrent("task_terminal")
	}
	if t.Attempt != v[2] || t.ClaimSeq != v[3] || !t.Held() {
		return notCurrent("claim_ended")
	}
	if t.CurrentClaim().ExecutorID != v[4] {
		return notCurrent("executor_mismatch")
	}
	return wire.JSON(200, map[string]any{"current": true, "team_id": t.TeamID, "flow_id": t.FlowID,
		"execution_revision_id": t.Snapshot.ExecutionRevision})
}

// ---- executor-claims ---------------------------------------------------

func (s *Service) executorClaims(ctx context.Context, h map[string]string, req wire.Request) wire.Reply {
	team, requestID, ok := serviceTeam(h)
	if !ok {
		return badRequest("invalid_request", "missing or invalid request context", requestID)
	}
	type reported struct {
		TaskID   int64  `json:"task_id"`
		Attempt  int64  `json:"attempt"`
		ClaimSeq int64  `json:"claim_seq"`
		State    string `json:"state"`
	}
	var body struct {
		TeamID     *int64     `json:"team_id"`
		ExecutorID *int64     `json:"executor_id"`
		Claims     []reported `json:"claims"`
		LostSeq    *int64     `json:"lost_seq"`
	}
	if len(req.Params) != 0 || strictBody(req.Body, &body) != nil || body.TeamID == nil || body.ExecutorID == nil ||
		body.Claims == nil || body.LostSeq == nil || *body.TeamID != team || *body.ExecutorID < 1 || *body.LostSeq < 0 {
		return badRequest("invalid_request", "invalid executor-claims request", requestID)
	}
	for _, c := range body.Claims {
		if c.TaskID < 1 || c.Attempt < 1 || c.ClaimSeq < 1 {
			return badRequest("invalid_request", "invalid claim", requestID)
		}
	}
	execID := *body.ExecutorID
	requestAt := s.now()
	var held []map[string]any
	err := s.write(ctx, func(tx store.Tx) error {
		held = []map[string]any{}
		exec, err := tx.Executor(execID)
		if err != nil {
			return err
		}
		if exec == nil || exec.TeamID != team {
			return nil
		}
		if err := s.applyLost(tx, exec, *body.LostSeq); err != nil {
			return err
		}
		tasks, err := tx.TasksHeldBy(execID)
		if err != nil {
			return err
		}
		for _, t := range tasks {
			if t.TeamID != team {
				continue
			}
			c := t.CurrentClaim()
			listed := false
			for _, r := range body.Claims {
				if r.TaskID == t.TaskID && r.Attempt == c.Attempt && r.ClaimSeq == c.ClaimSeq {
					listed = true
				}
			}
			switch {
			case listed:
				if c.MissingCount != 0 {
					c.MissingCount = 0
					if err := tx.SaveTask(t); err != nil {
						return err
					}
				}
			case c.ClaimedAt.Before(requestAt):
				c.MissingCount++
				if c.MissingCount >= missingHeartbeats {
					if err := s.releaseByTaskManager(tx, t, domain.ReasonClaimMissing, nil); err != nil {
						return err
					}
					continue
				}
				if err := tx.SaveTask(t); err != nil {
					return err
				}
			}
			held = append(held, map[string]any{"task_id": t.TaskID, "attempt": c.Attempt, "claim_seq": c.ClaimSeq})
		}
		return nil
	})
	if err != nil {
		return internalError(requestID)
	}
	return wire.JSON(200, map[string]any{"executor_id": execID, "claims": held})
}

// ---- cancel ------------------------------------------------------------

func (s *Service) cancel(ctx context.Context, op uiContext, req wire.Request) wire.Reply {
	rid := &op.requestID
	audit := func(tx store.Tx, task *int64, outcome, reason string, changes map[string]any) error {
		body := map[string]any{"source_service": wire.Service, "actor_type": "operator", "action": "cancel", "resource_type": "task",
			"resource_id": nullable(task), "request_id": op.requestID, "outcome": outcome, "team_id": op.team, "actor_id": op.operator}
		if reason != "" {
			body["reason"] = reason
		}
		if changes != nil {
			body["changes"] = changes
		}
		seq, err := tx.NextPublisherSeq()
		if err != nil {
			return err
		}
		body["publisher"], body["publisher_seq"], body["occurred_at"] = wire.Service, seq, wire.FormatTime(s.now())
		data, err := json.Marshal(body)
		if err != nil {
			return err
		}
		return tx.AppendOutbox(store.OutboxMessage{PublisherSeq: seq, Subject: wire.AuditSubject("cancel"),
			Header: map[string]string{wire.HeaderTeam: ids.Token(op.team), wire.HeaderRequestID: ids.Token(op.requestID),
				wire.HeaderMsgID: wire.MsgID(seq)}, Data: data})
	}
	rejectWith := func(task *int64, reply wire.Reply, reason string) wire.Reply {
		if err := s.write(ctx, func(tx store.Tx) error { return audit(tx, task, "rejected", reason, nil) }); err != nil {
			return internalError(rid)
		}
		return reply
	}
	if !onlyParams(req, "task_id", "team_id") {
		return rejectWith(nil, badRequest("invalid_request", "unknown parameter", rid), "invalid_request")
	}
	if bad := op.checkTeamParam(req); bad != nil {
		return rejectWith(nil, *bad, "invalid_request")
	}
	taskID, present, err := req.ID("task_id")
	if !present || err != nil {
		return rejectWith(nil, badRequest("invalid_request", "invalid task_id", rid), "invalid_request")
	}
	notFound := wire.Error(404, "not_found", "task not found", rid)
	terminal := wire.Error(409, "task_terminal", "the task has already ended", rid)
	cur, err := s.cfg.Store.GetTask(ctx, op.team, taskID)
	if err != nil {
		return internalError(rid)
	}
	if cur == nil {
		arch, err := s.cfg.Store.GetArchived(ctx, op.team, taskID)
		if err != nil {
			return internalError(rid)
		}
		if arch != nil {
			return rejectWith(&taskID, terminal, "task_terminal")
		}
		return rejectWith(&taskID, notFound, "not_found")
	}
	var reply wire.Reply
	err = s.write(ctx, func(tx store.Tx) error {
		t, err := tx.Task(op.team, taskID)
		if err != nil {
			return err
		}
		if t == nil {
			reply = terminal
			return audit(tx, &taskID, "rejected", "task_terminal", nil)
		}
		existing, err := tx.Control(op.team, taskID, op.operator, "cancel")
		if err != nil {
			return err
		}
		if existing == nil && t.PendingControl != nil {
			existing, err = tx.ControlByID(op.team, *t.PendingControl)
			if err != nil {
				return err
			}
		}
		if existing != nil {
			reply = cancelReply(taskID, existing.ControlID, existing.Status)
			return audit(tx, &taskID, "succeeded", "", map[string]any{"control_id": existing.ControlID, "status": existing.Status})
		}
		controlID, err := tx.NextTeamID(op.team, store.KindControl)
		if err != nil {
			return err
		}
		control := store.Control{TeamID: op.team, ControlID: controlID, TaskID: taskID, OperatorID: op.operator, Action: "cancel",
			Status: "requested", CreatedAt: s.now()}
		var holder *store.Executor
		if c := t.CurrentClaim(); c != nil && t.Held() {
			holder, err = tx.Executor(c.ExecutorID)
			if err != nil {
				return err
			}
			if holder == nil {
				holder = &store.Executor{ExecutorID: c.ExecutorID, TeamID: t.TeamID, Removed: true}
			}
		}
		requested := map[string]any{"task_id": taskID, "task_name": t.TaskName, "control": "cancel", "control_id": controlID, "attempt": t.Attempt,
			"claim_seq": nil, "executor_id": nil}
		if holder != nil {
			requested["claim_seq"], requested["executor_id"] = t.ClaimSeq, holder.ExecutorID
		}
		if holder == nil || holder.Removed {
			requested["delivery"] = "direct"
			if err := tx.PutControl(control); err != nil {
				return err
			}
			if _, err := s.event(tx, "control_requested", op.team, requested); err != nil {
				return err
			}
			if err := s.directCancel(tx, t, controlID); err != nil {
				return err
			}
			reply = cancelReply(taskID, controlID, "cancelled")
			return audit(tx, &taskID, "succeeded", "", map[string]any{"control_id": controlID, "status": "cancelled"})
		}
		requested["delivery"] = "command"
		if err := tx.PutControl(control); err != nil {
			return err
		}
		if _, err := s.emit(tx, wire.CommandSubject(holder.ExecutorID), ids.Token(op.team), map[string]any{"team_id": op.team,
			"control": "cancel", "control_id": controlID, "task_id": taskID, "attempt": t.Attempt, "claim_seq": t.ClaimSeq,
			"executor_id": holder.ExecutorID}, 0, ""); err != nil {
			return err
		}
		if _, err := s.event(tx, "control_requested", op.team, requested); err != nil {
			return err
		}
		t.PendingControl = &controlID
		if err := tx.SaveTask(t); err != nil {
			return err
		}
		reply = cancelReply(taskID, controlID, "requested")
		return audit(tx, &taskID, "succeeded", "", map[string]any{"control_id": controlID, "status": "requested"})
	})
	if err != nil {
		return internalError(rid)
	}
	return reply
}

func cancelReply(task, control int64, status string) wire.Reply {
	code := 202
	if status == "cancelled" {
		code = 200
	}
	return wire.JSON(code, map[string]any{"task_id": task, "control_id": control, "control": "cancel", "status": status})
}

// ---- admin projection --------------------------------------------------

func (s *Service) adminList(ctx context.Context, h map[string]string, req wire.Request) wire.Reply {
	requestID := headerID(h, wire.HeaderRequestID)
	subject := h[wire.HeaderAdminSubject]
	if h[wire.HeaderTeam] != wire.TeamAdmin || subject == "" || requestID == nil {
		return badRequest("invalid_request", "missing or invalid administrator context", requestID)
	}
	changes := map[string]any{}
	for k, v := range req.Params {
		changes[k] = json.RawMessage(v)
	}
	audit := func(tx store.Tx, outcome, reason string) error {
		body := map[string]any{"source_service": wire.Service, "actor_type": "administrator", "action": "admin-list-tasks",
			"resource_type": "task", "resource_id": nil, "request_id": *requestID, "outcome": outcome, "changes": changes,
			"actor_subject": subject}
		if reason != "" {
			body["reason"] = reason
		}
		seq, err := tx.NextPublisherSeq()
		if err != nil {
			return err
		}
		body["publisher"], body["publisher_seq"], body["occurred_at"] = wire.Service, seq, wire.FormatTime(s.now())
		data, err := json.Marshal(body)
		if err != nil {
			return err
		}
		return tx.AppendOutbox(store.OutboxMessage{PublisherSeq: seq, Subject: wire.AuditSubject("admin-list-tasks"),
			Header: map[string]string{wire.HeaderTeam: wire.TeamAdmin, wire.HeaderRequestID: ids.Token(*requestID),
				wire.HeaderMsgID: wire.MsgID(seq)}, Data: data})
	}
	reject := func(code, msg string) wire.Reply {
		if err := s.write(ctx, func(tx store.Tx) error { return audit(tx, "rejected", code) }); err != nil {
			return internalError(requestID)
		}
		return badRequest(code, msg, requestID)
	}
	q, code, msg := parseAdminQuery(req)
	if code != "" {
		return reject(code, msg)
	}
	rows, err := s.cfg.Store.AdminList(ctx, store.AdminQuery{TeamID: q.TeamID, State: q.State, Tag: q.Tag, FlowID: q.FlowID,
		After: q.After, Limit: q.Limit + 1})
	if err != nil {
		return internalError(requestID)
	}
	var next any
	if len(rows) > q.Limit {
		rows = rows[:q.Limit]
		last := rows[len(rows)-1]
		next = map[string]any{"ingested_at": wire.FormatTime(last.IngestedAt), "team_id": last.TeamID, "task_id": last.TaskID}
	}
	items := make([]map[string]any, 0, len(rows))
	for _, t := range rows {
		var executor, claimed any
		if c := t.LatestClaim(); c != nil {
			executor, claimed = c.ExecutorID, wire.FormatTime(c.ClaimedAt)
		}
		items = append(items, map[string]any{"task_id": t.TaskID, "team_id": t.TeamID, "flow_id": t.FlowID, "source_id": t.SourceID,
			"external_id": t.ExternalID, "required_tag": t.RequiredTag, "current_state": t.State, "owner_command_id": nil,
			"executor_id": executor, "ingested_at": wire.FormatTime(t.IngestedAt), "claimed_at": claimed})
	}
	if err := s.write(ctx, func(tx store.Tx) error { return audit(tx, "succeeded", "") }); err != nil {
		return internalError(requestID)
	}
	return wire.JSON(200, map[string]any{"items": items, "next": next})
}

func parseAdminQuery(req wire.Request) (store.AdminQuery, string, string) {
	q := store.AdminQuery{Limit: 50}
	if !onlyParams(req, "team_id", "state", "tag", "flow_id", "limit") {
		return q, "invalid_request", "unknown parameter"
	}
	if id, present, err := req.ID("team_id"); present {
		if err != nil {
			return q, "invalid_request", "invalid team_id"
		}
		q.TeamID = &id
	}
	if id, present, err := req.ID("flow_id"); present {
		if err != nil {
			return q, "invalid_request", "invalid flow_id"
		}
		if q.TeamID == nil {
			return q, "invalid_request", "flow_id requires team_id"
		}
		q.FlowID = &id
	}
	if v, present, valid := req.Strings("state"); present {
		if !valid || len(v) != 1 || !validState(v[0]) {
			return q, "invalid_request", "invalid state"
		}
		q.State = v[0]
	}
	if v, present, valid := req.Strings("tag"); present {
		if !valid || len(v) != 1 || !validTag(v[0]) {
			return q, "invalid_request", "invalid tag"
		}
		q.Tag = v[0]
	}
	if n, present, valid := req.Int("limit"); present {
		if !valid || n < 1 || n > 200 {
			return q, "invalid_pagination", "limit must be 1 to 200"
		}
		q.Limit = n
	}
	if len(bytes.TrimSpace(req.Body)) > 0 && string(bytes.TrimSpace(req.Body)) != "null" {
		var body struct {
			After *struct {
				IngestedAt string          `json:"ingested_at"`
				TeamID     json.RawMessage `json:"team_id"`
				TaskID     json.RawMessage `json:"task_id"`
			} `json:"after"`
		}
		if strictBody(req.Body, &body) != nil || body.After == nil {
			return q, "invalid_pagination", "invalid continuation position"
		}
		at, err := time.Parse(time.RFC3339Nano, body.After.IngestedAt)
		team, e1 := ids.ParseJSON(body.After.TeamID)
		task, e2 := ids.ParseJSON(body.After.TaskID)
		if err != nil || e1 != nil || e2 != nil {
			return q, "invalid_pagination", "invalid continuation position"
		}
		q.After = &store.Position{IngestedAt: at.UTC(), TeamID: team, TaskID: task}
	}
	return q, "", ""
}

func validState(v string) bool {
	switch v {
	case domain.StateQueued, domain.StateCreated, domain.StatePending, domain.StateRunning,
		domain.StateFinished, domain.StateFailed, domain.StateCancelled:
		return true
	}
	return false
}

func validTag(v string) bool {
	if v == "" || len(v) > 128 {
		return false
	}
	for _, r := range v {
		alnum := r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9'
		if !alnum && r != '_' && r != '-' {
			return false
		}
	}
	return true
}
