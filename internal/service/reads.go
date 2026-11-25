package service

import (
	"context"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/pandops/flowai-task-manager/internal/civil"
	"github.com/pandops/flowai-task-manager/internal/cursor"
	"github.com/pandops/flowai-task-manager/internal/domain"
	"github.com/pandops/flowai-task-manager/internal/ids"
	"github.com/pandops/flowai-task-manager/internal/store"
	"github.com/pandops/flowai-task-manager/internal/wire"
)

// uiContext is the verified operator context API Gateway forwards.
type uiContext struct {
	team, operator, requestID int64
}

// checkTeamParam rejects a params team_id that differs from the header
// team; nil means the parameter is absent or equal.
func (op uiContext) checkTeamParam(req wire.Request) *wire.Reply {
	id, present, err := req.ID("team_id")
	if present && (err != nil || id != op.team) {
		r := badRequest("invalid_request", "team_id differs from the request team", &op.requestID)
		return &r
	}
	return nil
}

func (s *Service) ui(ctx context.Context, operation string, h map[string]string, req wire.Request) wire.Reply {
	requestID := headerID(h, wire.HeaderRequestID)
	team, e1 := ids.ParseToken(h[wire.HeaderTeam])
	operator, e2 := ids.ParseToken(h[wire.HeaderOperatorID])
	if e1 != nil || e2 != nil || requestID == nil {
		return badRequest("invalid_request", "missing or invalid request context", requestID)
	}
	op := uiContext{team: team, operator: operator, requestID: *requestID}
	switch operation {
	case "cancel":
		return s.cancel(ctx, op, req)
	case "list-tasks", "list-archive", "list-executor-tasks":
		return s.list(ctx, operation, op, req)
	case "get-task", "get-archived-task", "list-attempts":
		return s.card(ctx, operation, op, req)
	}
	return wire.Error(404, "not_found", "unknown operation", requestID)
}

var listParams = []string{"team_id", "status", "period", "search", "date", "time_zone", "flow_id", "source_id", "tag", "limit", "cursor"}

const maxSearch = 256

// one returns the single string value of a parameter; ok is false when it
// is repeated or not a string.
func one(req wire.Request, name string) (string, bool, bool) {
	v, present, valid := req.Strings(name)
	if !present {
		return "", false, true
	}
	if !valid || len(v) != 1 {
		return "", true, false
	}
	return v[0], true, true
}

func (s *Service) list(ctx context.Context, operation string, op uiContext, req wire.Request) wire.Reply {
	rid := &op.requestID
	allowed := listParams
	if operation == "list-executor-tasks" {
		allowed = append(append([]string{}, listParams...), "executor_id")
	}
	invalid := func(msg string) wire.Reply { return badRequest("invalid_request", msg, rid) }
	if !onlyParams(req, allowed...) {
		return invalid("unknown parameter")
	}
	if bad := op.checkTeamParam(req); bad != nil {
		return *bad
	}
	// list-tasks serves current and archived tasks; list-executor-tasks
	// only current claims; list-archive only the archive.
	q := store.TaskQuery{TeamID: op.team, Scope: store.ScopeAll}
	switch operation {
	case "list-archive":
		q.Scope = store.ScopeArchive
	case "list-executor-tasks":
		q.Scope = store.ScopeCurrent
	}
	scope := []string{operation, ids.Token(op.team), ids.Token(op.operator)}
	idFilter := func(name string) (*int64, bool) {
		id, present, err := req.ID(name)
		if !present {
			scope = append(scope, name+"=")
			return nil, true
		}
		if err != nil {
			return nil, false
		}
		scope = append(scope, name+"="+ids.Token(id))
		return &id, true
	}
	var ok bool
	if q.FlowID, ok = idFilter("flow_id"); !ok {
		return invalid("invalid flow_id")
	}
	if q.SourceID, ok = idFilter("source_id"); !ok {
		return invalid("invalid source_id")
	}
	if operation == "list-executor-tasks" {
		if q.ExecutorID, ok = idFilter("executor_id"); !ok || q.ExecutorID == nil {
			return invalid("invalid executor_id")
		}
	}
	status, present, valid := one(req, "status")
	if !valid || present && !validState(status) {
		return invalid("invalid status")
	}
	q.State = status
	tag, present, valid := one(req, "tag")
	if !valid || present && !validTag(tag) {
		return invalid("invalid tag")
	}
	q.Tag = tag
	search, _, valid := one(req, "search")
	if !valid || !utf8.ValidString(search) || utf8.RuneCountInString(search) > maxSearch {
		return invalid("invalid search")
	}
	q.Search = search
	period, present, valid := one(req, "period")
	// Rolling periods as in the baseline: day = 24 h, week = 7 days,
	// month = one calendar month back (UTC AddDate).
	var since func(time.Time) time.Time
	switch {
	case !valid:
		return invalid("invalid period")
	case !present:
	case period == "day":
		since = func(now time.Time) time.Time { return now.Add(-24 * time.Hour) }
	case period == "week":
		since = func(now time.Time) time.Time { return now.Add(-7 * 24 * time.Hour) }
	case period == "month":
		since = func(now time.Time) time.Time { return now.UTC().AddDate(0, -1, 0) }
	default:
		return invalid("invalid period")
	}
	zones, zonePresent, zoneValid := req.Strings("time_zone")
	if !zoneValid {
		return badRequest("invalid_time_zone", "unsupported time zone", rid)
	}
	loc, zoneName, err := civil.Zone(zones, zonePresent)
	if err != nil {
		return badRequest("invalid_time_zone", "unsupported time zone", rid)
	}
	date, datePresent, valid := one(req, "date")
	if !valid {
		return invalid("invalid date")
	}
	skipped := false
	if datePresent {
		start, end, exists, err := civil.DateBounds(date, loc)
		if err != nil {
			return invalid("invalid date")
		}
		q.DateStart, q.DateEnd, skipped = &start, &end, !exists
	}
	limit := 10
	if n, present, valid := req.Int("limit"); present {
		if !valid || (n != 10 && n != 25 && n != 50 && n != 100) {
			return badRequest("invalid_pagination", "limit must be 10, 25, 50 or 100", rid)
		}
		limit = n
	}
	scope = append(scope, "status="+status, "tag="+tag, "search="+search, "period="+period, "tz="+zoneName,
		"date="+date, "limit="+ids.Token(int64(limit)))
	scopeKey := strings.Join(scope, "\x1f")

	var cutoff *time.Time
	if raw, present := req.Params["cursor"]; present {
		token, _, valid := one(req, "cursor")
		if !valid || raw == nil {
			return badRequest("invalid_cursor", "invalid cursor", rid)
		}
		if s.cfg.Cursors == nil || token == "" || len(token) > cursor.MaxLength {
			return badRequest("invalid_cursor", "invalid cursor", rid)
		}
		c, err := s.cfg.Cursors.Decode(token)
		if err != nil || c.Scope != scopeKey || c.Pos.TeamID != op.team || (since != nil) != (c.Cutoff != nil) {
			return badRequest("invalid_cursor", "invalid cursor", rid)
		}
		pos := c.Pos
		q.After, cutoff = &pos, c.Cutoff
	} else if since != nil {
		at := since(s.now())
		cutoff = &at
	}
	q.Since = cutoff
	empty := wire.JSON(200, map[string]any{"items": []any{}, "next_cursor": nil})
	if skipped {
		return empty
	}
	if q.ExecutorID != nil {
		exec, err := s.cfg.Store.Executor(ctx, *q.ExecutorID)
		if err != nil {
			return internalError(rid)
		}
		if exec == nil || exec.TeamID != op.team {
			return empty
		}
	}
	if operation == "list-tasks" && search != "" {
		// A full task name of the header team selects that task alone.
		// The decision reads only the team projection, never whether a
		// task exists; list-archive and list-executor-tasks keep the broad
		// search.
		team, err := s.cfg.Store.Team(ctx, op.team)
		if err != nil {
			return internalError(rid)
		}
		if name, ok := fullTaskName(search, team); ok {
			q.TaskName, q.Search = name, ""
		}
	}
	q.Limit = limit + 1
	rows, err := s.cfg.Store.ListTasks(ctx, q)
	if err != nil {
		return internalError(rid)
	}
	var next any
	if len(rows) > limit {
		rows = rows[:limit]
		last := rows[len(rows)-1]
		if s.cfg.Cursors == nil {
			return internalError(rid)
		}
		next = s.cfg.Cursors.Encode(cursor.Cursor{Scope: scopeKey, Cutoff: cutoff,
			Pos: store.Position{IngestedAt: last.IngestedAt, TeamID: last.TeamID, TaskID: last.TaskID}})
	}
	items := make([]map[string]any, 0, len(rows))
	for _, t := range rows {
		items = append(items, summary(t))
	}
	return wire.JSON(200, map[string]any{"items": items, "next_cursor": next})
}

// fullTaskName reports whether the trimmed term is, ignoring case,
// <team_name>-<n> with the projected team_name of the header team and a
// canonical positive int64 n, and returns the lowercase stored form.
func fullTaskName(term string, team *store.Team) (string, bool) {
	if team == nil || team.TeamName == "" {
		return "", false
	}
	name := strings.ToLower(strings.TrimSpace(term))
	prefix := strings.ToLower(team.TeamName) + "-"
	if !strings.HasPrefix(name, prefix) {
		return "", false
	}
	n := name[len(prefix):]
	if n == "" || n[0] < '1' || n[0] > '9' {
		return "", false
	}
	for i := 0; i < len(n); i++ {
		if n[i] < '0' || n[i] > '9' {
			return "", false
		}
	}
	if _, err := strconv.ParseInt(n, 10, 64); err != nil {
		return "", false
	}
	return name, true
}

func timeOrNil(t *time.Time) any {
	if t == nil {
		return nil
	}
	return wire.FormatTime(*t)
}

// holder is the Executor shown for a task: the current holder, or the last
// claim of a terminal task.
func holder(t *domain.Task) *domain.Claim {
	if t.Held() {
		return t.CurrentClaim()
	}
	if domain.Terminal(t.State) {
		return t.LatestClaim()
	}
	return nil
}

func summary(t *domain.Task) map[string]any {
	var executor, claimed any
	if c := holder(t); c != nil {
		executor, claimed = c.ExecutorID, wire.FormatTime(c.ClaimedAt)
	}
	return map[string]any{"task_id": t.TaskID, "task_name": t.TaskName, "team_id": t.TeamID, "flow_id": t.FlowID, "source_id": t.SourceID,
		"external_id": t.ExternalID, "required_tag": t.RequiredTag, "current_state": t.State, "attempt": t.Attempt,
		"executor_id": executor, "ingested_at": wire.FormatTime(t.IngestedAt), "claimed_at": claimed,
		"completed_at": timeOrNil(t.CompletedAt)}
}

func attempts(t *domain.Task) []map[string]any {
	out := make([]map[string]any, 0, len(t.Attempts))
	for _, a := range t.Attempts {
		claims := make([]map[string]any, 0, len(a.Claims))
		for _, c := range a.Claims {
			reasons := c.PendingReasons
			if reasons == nil {
				reasons = []string{}
			}
			claims = append(claims, map[string]any{"attempt": c.Attempt, "claim_seq": c.ClaimSeq, "executor_id": c.ExecutorID,
				"claimed_at": wire.FormatTime(c.ClaimedAt), "resolved_image": c.ResolvedImage, "image_source": c.ImageSource,
				"pending_reasons": reasons, "released_reason": nullable(c.ReleasedReason), "ended_at": timeOrNil(c.EndedAt)})
		}
		out = append(out, map[string]any{"attempt": a.Attempt, "state": a.State, "failure_reason": nullable(a.FailureReason),
			"retryable": nullable(a.Retryable), "claims": claims})
	}
	return out
}

func card(t *domain.Task, archived bool) map[string]any {
	m := summary(t)
	m["intake_id"] = t.IntakeID
	m["claim_seq"] = max(t.ClaimSeq, 1)
	var image, source any
	if c := holder(t); c != nil {
		image, source = c.ResolvedImage, c.ImageSource
	} else if t.QueuedImage != "" {
		image, source = t.QueuedImage, t.QueuedImageSource
	}
	m["resolved_image"], m["image_source"] = image, source
	m["next_due_at"] = timeOrNil(t.NextDueAt)
	var kind any
	if t.NextDueKind != "" {
		kind = t.NextDueKind
	}
	m["next_due_kind"] = kind
	m["execution_revision_id"] = t.Snapshot.ExecutionRevision
	m["failure_retry_count"] = t.Snapshot.FailureRetryCount
	m["failure_retry_wait_seconds"] = t.Snapshot.RetryWaitSeconds
	m["pending_limit_seconds"] = t.Snapshot.PendingLimitSeconds
	m["release_delay_seconds"] = t.Snapshot.ReleaseDelaySeconds
	m["final_reason"] = nullable(t.FinalReason)
	m["archived"] = archived
	m["attempts"] = attempts(t)
	history := make([]map[string]any, 0, len(t.History))
	for _, h := range t.History {
		history = append(history, map[string]any{"history_seq": h.Seq, "kind": h.Kind, "publisher": h.Publisher,
			"publisher_seq": h.PublisherSeq, "occurred_at": wire.FormatTime(h.OccurredAt), "attempt": h.Attempt,
			"claim_seq": nullable(h.ClaimSeq), "executor_id": nullable(h.ExecutorID), "reason": nullable(h.Reason)})
	}
	m["history"] = history
	return m
}

func (s *Service) card(ctx context.Context, operation string, op uiContext, req wire.Request) wire.Reply {
	rid := &op.requestID
	if !onlyParams(req, "team_id", "task_id") {
		return badRequest("invalid_request", "unknown parameter", rid)
	}
	if bad := op.checkTeamParam(req); bad != nil {
		return *bad
	}
	taskID, present, err := req.ID("task_id")
	if !present || err != nil {
		return badRequest("invalid_request", "invalid task_id", rid)
	}
	notFound := wire.Error(404, "not_found", "task not found", rid)
	archived := operation == "get-archived-task"
	var t *domain.Task
	if archived {
		t, err = s.cfg.Store.GetArchived(ctx, op.team, taskID)
	} else {
		// get-task and list-attempts serve current and archived tasks. A
		// task only moves from current to the archive, so a miss here
		// followed by an archive read cannot lose a task.
		t, err = s.cfg.Store.GetTask(ctx, op.team, taskID)
		if err == nil && t == nil {
			t, err = s.cfg.Store.GetArchived(ctx, op.team, taskID)
			archived = t != nil
		}
	}
	if err != nil {
		return internalError(rid)
	}
	if t == nil || t.TeamID != op.team {
		return notFound
	}
	if operation == "list-attempts" {
		return wire.JSON(200, map[string]any{"task_id": t.TaskID, "items": attempts(t)})
	}
	return wire.JSON(200, card(t, archived))
}
