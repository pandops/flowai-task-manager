package test

// Shared unit-test harness: task-manager's service on the in-memory store
// and the fake broker. Every published message and every reply is validated
// against task-manager's own schema at cleanup, so each test also checks the
// published contract.

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/pandops/flowai-task-manager/internal/claimtoken"
	"github.com/pandops/flowai-task-manager/internal/cursor"
	"github.com/pandops/flowai-task-manager/internal/domain"
	"github.com/pandops/flowai-task-manager/internal/mocks/broker"
	"github.com/pandops/flowai-task-manager/internal/mocks/memstore"
	"github.com/pandops/flowai-task-manager/internal/outbox"
	"github.com/pandops/flowai-task-manager/internal/service"
	"github.com/pandops/flowai-task-manager/internal/store"
	"github.com/pandops/flowai-task-manager/internal/wire"
)

var t0 = time.Date(2026, 11, 2, 10, 0, 0, 0, time.UTC)

// testSeed is a fixed Ed25519 seed (base64 standard) for claim tokens.
var testSeed = base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32))

const (
	imageTeam3 = "registry.example/agent@sha256:" + "3333333333333333333333333333333333333333333333333333333333333333"
	imageTeam4 = "registry.example/agent@sha256:" + "4444444444444444444444444444444444444444444444444444444444444444"
	imageNew   = "registry.example/agent@sha256:" + "5555555555555555555555555555555555555555555555555555555555555555"
	flowDigest = "sha256:" + "abababababababababababababababababababababababababababababababab"
)

// flowDef is one bound Flow as flow-registry's targets reply returns it.
type flowDef struct {
	FlowID       int64
	Revision     int64
	Tag          string
	Prompt       string
	Params       map[string]any
	Image        map[string]any // nil = explicit absence marker
	RetryCount   int
	OmitRetry    bool
	RetryWait    int
	PendingLimit int
	ReleaseDelay int
}

func flow(id int64) flowDef {
	return flowDef{FlowID: id, Revision: 10 + id, Tag: "openhands", Prompt: "Fix {{external_id}} now", Params: map[string]any{},
		RetryCount: 3, RetryWait: 60, PendingLimit: 900, ReleaseDelay: 300}
}

type reply struct {
	Status int
	Body   map[string]any
	Raw    []byte
	Header map[string]string
}

type claimRef struct {
	Task, Attempt, Claim int64
	State                string
}

// testStore is the store surface the harness needs: memstore in unit tests,
// pgstore in integration tests.
type testStore interface {
	store.Store
	CurrentTasks() []*domain.Task
	Archived(teamID, taskID int64) (*domain.Task, time.Time, bool)
	OutboxLen() int
}

type harness struct {
	t         *testing.T
	St        testStore
	Store     *memstore.Store // nil when St is not the in-memory fake
	Bus       *broker.Fake
	Svc       *service.Service
	Relay     *outbox.Relay
	Signer    *claimtoken.Signer
	Codec     *cursor.Codec
	Now       time.Time
	mu        sync.Mutex
	bindings  map[[2]int64][]flowDef
	seq       map[string]int64
	requestID int64
	replies   []checkedReply
	requests  []checkedReply
	teams     map[int64]bool
	execs     map[int64]bool
	names     map[int64]string // team_name per team; default team-<id>
}

type checkedReply struct {
	schema string
	raw    []byte
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	m := memstore.New()
	h := newHarnessOn(t, m)
	h.Store = m
	return h
}

func newHarnessOn(t *testing.T, st testStore) *harness {
	t.Helper()
	h := &harness{t: t, St: st, Bus: broker.New(), Now: t0, bindings: map[[2]int64][]flowDef{}, seq: map[string]int64{},
		teams: map[int64]bool{}, execs: map[int64]bool{}, names: map[int64]string{}}
	signer, err := claimtoken.NewSigner(testSeed)
	if err != nil {
		t.Logf("claim token signer: %v", err)
	}
	h.Signer = signer
	codec, err := cursor.New([]cursor.Key{{ID: 1, Key: bytes.Repeat([]byte{9}, 32)}})
	if err != nil {
		t.Logf("cursor codec: %v", err)
	}
	h.Codec = codec
	h.Bus.Respond = h.respond
	h.Svc = h.peer(st)
	h.Relay = &outbox.Relay{Store: st, Pub: h.Bus, Batch: 100}
	t.Cleanup(h.checkContracts)
	return h
}

// peer returns another task-manager process on st that shares the clock,
// keys and fake broker (a second replica or a restarted process).
func (h *harness) peer(st store.Store) *service.Service {
	return service.New(service.Config{Store: st, Flows: h.Bus, Signer: h.Signer, Cursors: h.Codec,
		Now: func() time.Time { h.mu.Lock(); defer h.mu.Unlock(); return h.Now }, UnknownTeamDelay: 5 * time.Second})
}

func (h *harness) advance(d time.Duration) {
	h.mu.Lock()
	h.Now = h.Now.Add(d)
	h.mu.Unlock()
}

func (h *harness) nextSeq(publisher string) int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.seq[publisher]++
	return h.seq[publisher]
}

func ts(t time.Time) string { return wire.FormatTime(t) }

func tok(v int64) string { return strconv.FormatInt(v, 10) }

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

func decode(raw []byte) map[string]any {
	var m map[string]any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&m); err != nil {
		return nil
	}
	return m
}

// num converts a decoded JSON number to int64 (-1 when absent or not a number).
func num(v any) int64 {
	switch x := v.(type) {
	case json.Number:
		n, err := x.Int64()
		if err != nil {
			return -1
		}
		return n
	case float64:
		return int64(x)
	case int64:
		return x
	case int:
		return int64(x)
	}
	return -1
}

func str(v any) string {
	s, _ := v.(string)
	return s
}

func list(v any) []any {
	l, _ := v.([]any)
	return l
}

func obj(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

// ---- flow-registry fake -------------------------------------------------

func (h *harness) bind(team, source int64, flows ...flowDef) {
	h.mu.Lock()
	h.bindings[[2]int64{team, source}] = flows
	h.mu.Unlock()
}

func (h *harness) respond(m wire.Msg) (wire.Reply, error) {
	if m.Subject != "flow-registry.service.targets.pub" {
		return wire.Error(404, "not_found", "no such operation", nil), nil
	}
	req := decode(m.Data)
	params := obj(req["params"])
	team, source := num(params["team_id"]), num(params["source_id"])
	h.mu.Lock()
	flows, ok := h.bindings[[2]int64{team, source}]
	h.mu.Unlock()
	if !ok || len(flows) == 0 {
		return wire.Error(404, "not_found", "not found", nil), nil
	}
	var out []map[string]any
	for _, f := range flows {
		rev := map[string]any{"revision": f.Revision, "execution_tag": f.Tag, "prompt_template": f.Prompt, "execution_parameters": f.Params,
			"default_image": f.Image, "failure_retry_wait_seconds": f.RetryWait, "pending_limit_seconds": f.PendingLimit,
			"release_delay_seconds": f.ReleaseDelay, "created_at": ts(t0.Add(-time.Hour))}
		if !f.OmitRetry {
			rev["failure_retry_count"] = f.RetryCount
		}
		out = append(out, map[string]any{"flow_id": f.FlowID, "execution_revision": rev})
	}
	return wire.JSON(200, map[string]any{"team_id": team, "source_id": source, "flows": out}), nil
}

// targetRequests returns the flow-registry requests sent so far.
func (h *harness) targetRequests() []wire.Msg {
	var out []wire.Msg
	for _, m := range h.Bus.Requests {
		if strings.HasPrefix(m.Subject, "flow-registry.") {
			out = append(out, m)
		}
	}
	return out
}

// ---- consumed events ------------------------------------------------------

func (h *harness) event(subject string, header map[string]string, body map[string]any) wire.Ack {
	h.t.Helper()
	return h.Svc.HandleEvent(context.Background(), wire.Msg{Subject: subject, Header: header, Data: mustJSON(h.t, body)})
}

func (h *harness) teamEvent(kind string, team int64, image string, archived bool) wire.Ack {
	seq := h.nextSeq("team-registry")
	var archivedAt any
	if archived {
		archivedAt = ts(h.Now)
	}
	body := map[string]any{"publisher": "team-registry", "publisher_seq": seq, "kind": kind, "occurred_at": ts(h.Now), "team_id": team,
		"team_name": h.teamName(team), "default_image": image, "ingested_at": ts(t0.Add(-24 * time.Hour)), "archived_at": archivedAt}
	return h.event("team-registry.event."+kind+".sub", map[string]string{"team": "admin", "Nats-Msg-Id": "team-registry-" + tok(seq)}, body)
}

// teamName is the team_name the harness's team-registry events carry for
// team: a name set by nameTeam, otherwise team-<id> (a valid team name).
func (h *harness) teamName(team int64) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	if n, ok := h.names[team]; ok {
		return n
	}
	return "team-" + tok(team)
}

// nameTeam sets the team_name of team for every later team event.
func (h *harness) nameTeam(team int64, name string) {
	h.mu.Lock()
	h.names[team] = name
	h.mu.Unlock()
}

// taskName is the task name task-manager must store: <team_name>-<task_id>.
func (h *harness) taskName(team, task int64) string {
	return h.teamName(team) + "-" + tok(task)
}

func (h *harness) teamCreated(team int64, image string) {
	h.teamEvent("team_created", team, image, false)
}
func (h *harness) teamUpdated(team int64, image string) {
	h.teamEvent("team_updated", team, image, false)
}
func (h *harness) teamArchived(team int64, image string) {
	h.teamEvent("team_archived", team, image, true)
}

func (h *harness) registryEvent(kind string, team, exec int64, tag string, seq int64) wire.Ack {
	body := map[string]any{"publisher": "executor-registry", "publisher_seq": seq, "team_id": team, "occurred_at": ts(h.Now), "kind": kind, "executor_id": exec}
	switch kind {
	case "executor_lost":
		body["last_heartbeat_at"] = ts(h.Now.Add(-time.Minute))
	default:
		body["execution_tag"] = tag
		body["executor_type"] = "executor_docker_openhands"
	}
	return h.event("executor-registry.event."+kind+".sub", map[string]string{"team": tok(team), "Nats-Msg-Id": "executor-registry-" + tok(seq)}, body)
}

func (h *harness) registered(team, exec int64, tag string) {
	h.registryEvent("executor_registered", team, exec, tag, h.nextSeq("executor-registry"))
}
func (h *harness) removed(team, exec int64, tag string) {
	h.registryEvent("executor_removed", team, exec, tag, h.nextSeq("executor-registry"))
}

// lost publishes executor_lost with an explicit publisher_seq.
func (h *harness) lost(team, exec, seq int64) wire.Ack {
	h.mu.Lock()
	if seq > h.seq["executor-registry"] {
		h.seq["executor-registry"] = seq
	}
	h.mu.Unlock()
	return h.registryEvent("executor_lost", team, exec, "", seq)
}

// exec publishes one Executor event on executor.event.<exec>.<kind>.sub with
// the Executor's next publisher_seq; header team and envelope team_id are
// the given team unless overridden through extra["team_id"] / headerTeam.
func (h *harness) exec(team, execID int64, kind string, task, attempt, claim int64, extra map[string]any) (wire.Ack, int64) {
	h.t.Helper()
	seq := h.nextSeq("executor-" + tok(execID))
	body := map[string]any{"publisher": "executor", "publisher_seq": seq, "team_id": team, "executor_id": execID, "kind": kind, "occurred_at": ts(h.Now),
		"task_id": task, "attempt": attempt, "claim_seq": claim}
	headerTeam := tok(team)
	for k, v := range extra {
		if k == "header_team" {
			headerTeam = v.(string)
			continue
		}
		body[k] = v
	}
	ack := h.event("executor.event."+tok(execID)+"."+kind+".sub", map[string]string{"team": headerTeam, "Nats-Msg-Id": fmt.Sprintf("executor-%d-%d-%d", team, execID, seq)}, body)
	return ack, seq
}

// execSeq re-sends an Executor event with an explicit publisher_seq.
func (h *harness) execSeq(team, execID int64, kind string, task, attempt, claim, seq int64, extra map[string]any) wire.Ack {
	body := map[string]any{"publisher": "executor", "publisher_seq": seq, "team_id": team, "executor_id": execID, "kind": kind, "occurred_at": ts(h.Now),
		"task_id": task, "attempt": attempt, "claim_seq": claim}
	for k, v := range extra {
		body[k] = v
	}
	return h.event("executor.event."+tok(execID)+"."+kind+".sub", map[string]string{"team": tok(team)}, body)
}

func (h *harness) created(team, execID, task, attempt, claim int64) int64 {
	_, seq := h.exec(team, execID, "created", task, attempt, claim, nil)
	return seq
}

func (h *harness) running(team, execID, task, attempt, claim int64) {
	h.exec(team, execID, "running", task, attempt, claim, map[string]any{"image_source": "team_default"})
}

func (h *harness) finished(team, execID, task, attempt, claim int64) {
	h.exec(team, execID, "finished", task, attempt, claim, nil)
}

func (h *harness) failed(team, execID, task, attempt, claim int64, reason string, retryable bool) {
	h.exec(team, execID, "failed", task, attempt, claim, map[string]any{"failure_reason": reason, "retryable": retryable})
}

func (h *harness) released(team, execID, task, attempt, claim int64, reason string) wire.Ack {
	ack, _ := h.exec(team, execID, "released", task, attempt, claim, map[string]any{"reason": reason})
	return ack
}

// ---- intake ----------------------------------------------------------------

func entry(intakeID int64, external string) map[string]any {
	return map[string]any{"intake_id": intakeID, "external_id": external, "payload": map[string]any{"title": "issue " + external}}
}

func (h *harness) intakeBody(team, source, seq int64, entries ...map[string]any) map[string]any {
	return map[string]any{"publisher": "source-controller", "publisher_seq": seq, "team_id": team, "occurred_at": ts(h.Now), "source_id": source,
		"origin": "webhook", "implementation_version": "1", "accepted_at": ts(h.Now), "tasks": entries}
}

func (h *harness) intakeRaw(subject string, header map[string]string, body map[string]any) wire.Ack {
	h.t.Helper()
	return h.Svc.HandleIntake(context.Background(), wire.Msg{Subject: subject, Header: header, Data: mustJSON(h.t, body)})
}

// intake publishes one valid intake message of team on its own subject and
// returns the decision and the message's publisher_seq.
func (h *harness) intake(team, source int64, entries ...map[string]any) (wire.Ack, int64) {
	h.t.Helper()
	seq := h.nextSeq("source-controller")
	return h.intakeSeq(team, source, seq, entries...), seq
}

func (h *harness) intakeSeq(team, source, seq int64, entries ...map[string]any) wire.Ack {
	return h.intakeRaw("source-controller.task."+tok(team)+".sub",
		map[string]string{"team": tok(team), "Nats-Msg-Id": "source-controller-" + tok(seq)}, h.intakeBody(team, source, seq, entries...))
}

// ---- requests --------------------------------------------------------------

func (h *harness) request(subject string, header map[string]string, envelope map[string]any, replySchema string) reply {
	h.t.Helper()
	r := h.Svc.HandleRequest(context.Background(), wire.Msg{Subject: subject, Header: header, Data: mustJSON(h.t, envelope)})
	status := r.Status
	if s, ok := r.Header[wire.HeaderStatus]; ok {
		status, _ = strconv.Atoi(s)
	}
	out := reply{Status: status, Raw: r.Body, Body: decode(r.Body), Header: r.Header}
	schema := "error.reply.schema.json"
	if status >= 200 && status < 300 {
		schema = replySchema
	}
	if schema != "" {
		h.replies = append(h.replies, checkedReply{schema: schema, raw: r.Body})
	}
	if status >= 200 && status < 300 {
		// An accepted request must match task-manager's own request schema,
		// the file requesters build their contract tests on.
		h.requests = append(h.requests, checkedReply{schema: requestSchema(subject), raw: mustJSON(h.t, envelope)})
	}
	return out
}

// requestSchema maps a served subject to its request schema file:
// task-manager.<class>.<op>.sub -> <class>.<op>.request.schema.json and
// task-manager.executor.<id>.claim-token.sub -> executor.claim-token.request.schema.json.
func requestSchema(subject string) string {
	parts := strings.Split(subject, ".")
	if len(parts) == 5 && parts[1] == "executor" {
		return "executor." + parts[3] + ".request.schema.json"
	}
	if len(parts) == 4 {
		return parts[1] + "." + parts[2] + ".request.schema.json"
	}
	return "unknown-request-subject:" + subject
}

func (h *harness) nextRequestID() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.requestID++
	return 9000 + h.requestID
}

func (h *harness) uiHeader(team, operator int64) map[string]string {
	return map[string]string{"team": tok(team), "operator_id": tok(operator), "request_id": tok(h.nextRequestID()), "team_name": h.teamName(team)}
}

// ui sends task-manager.ui.<op>.sub with params; body is omitted when nil.
func (h *harness) ui(op string, team, operator int64, params map[string]any) reply {
	h.t.Helper()
	if params == nil {
		params = map[string]any{}
	}
	return h.request("task-manager.ui."+op+".sub", h.uiHeader(team, operator), map[string]any{"params": params}, "ui."+op+".reply.schema.json")
}

func (h *harness) getTask(team, task int64) reply {
	return h.ui("get-task", team, 1, map[string]any{"task_id": task})
}

func (h *harness) cancel(team, operator, task int64) reply {
	h.t.Helper()
	return h.request("task-manager.ui.cancel.sub", h.uiHeader(team, operator), map[string]any{"params": map[string]any{"task_id": task}}, "ui.cancel.reply.schema.json")
}

func (h *harness) admin(params map[string]any, body any) reply {
	h.t.Helper()
	env := map[string]any{"params": params}
	if body != nil {
		env["body"] = body
	}
	return h.request("task-manager.admin.list-tasks.sub", map[string]string{"team": "admin", "admin_subject": "admin-alice", "request_id": tok(h.nextRequestID())}, env, "admin.list-tasks.reply.schema.json")
}

func (h *harness) claimToken(execID, task, attempt, claim, createdSeq int64) reply {
	h.t.Helper()
	return h.request("task-manager.executor."+tok(execID)+".claim-token.sub", map[string]string{"team": "1", "request_id": tok(h.nextRequestID())},
		map[string]any{"params": map[string]any{}, "body": map[string]any{"task_id": task, "attempt": attempt, "claim_seq": claim, "created_publisher_seq": createdSeq}},
		"executor.claim-token.reply.schema.json")
}

func (h *harness) claimCurrent(team, task, attempt, claim, execID int64) reply {
	h.t.Helper()
	return h.request("task-manager.service.claim-current.sub", map[string]string{"team": tok(team), "request_id": tok(h.nextRequestID())},
		map[string]any{"params": map[string]any{"team_id": team, "task_id": task, "attempt": attempt, "claim_seq": claim, "executor_id": execID}},
		"service.claim-current.reply.schema.json")
}

func (h *harness) executorClaims(team, execID int64, lostSeq int64, claims ...claimRef) reply {
	h.t.Helper()
	cl := []map[string]any{}
	for _, c := range claims {
		state := c.State
		if state == "" {
			state = "running"
		}
		cl = append(cl, map[string]any{"task_id": c.Task, "attempt": c.Attempt, "claim_seq": c.Claim, "state": state})
	}
	return h.request("task-manager.service.executor-claims.sub", map[string]string{"team": tok(team), "request_id": tok(h.nextRequestID())},
		map[string]any{"params": map[string]any{}, "body": map[string]any{"team_id": team, "executor_id": execID, "claims": cl, "lost_seq": lostSeq}},
		"service.executor-claims.reply.schema.json")
}

// heldClaims returns the claims listed in an executor-claims reply.
func heldClaims(r reply) []claimRef {
	var out []claimRef
	for _, c := range list(r.Body["claims"]) {
		m := obj(c)
		out = append(out, claimRef{Task: num(m["task_id"]), Attempt: num(m["attempt"]), Claim: num(m["claim_seq"])})
	}
	return out
}

// ---- outbox and published messages ------------------------------------------

func (h *harness) flush() {
	h.t.Helper()
	if _, err := h.Relay.Flush(context.Background()); err != nil {
		h.t.Errorf("outbox flush: %v", err)
	}
}

// due advances the clock and runs the scheduler and the relay.
func (h *harness) due(d time.Duration) int {
	h.t.Helper()
	h.advance(d)
	n, err := h.Svc.RunDue(context.Background())
	if err != nil {
		h.t.Errorf("run due: %v", err)
	}
	h.flush()
	return n
}

func (h *harness) published(prefix string) []wire.Msg {
	var out []wire.Msg
	for _, m := range h.Bus.Messages() {
		if strings.HasPrefix(m.Subject, prefix) {
			out = append(out, m)
		}
	}
	return out
}

func (h *harness) events(kind string) []map[string]any {
	var out []map[string]any
	for _, m := range h.published("task-manager.event." + kind + ".pub") {
		out = append(out, decode(m.Data))
	}
	return out
}

// eventsFor returns the events of one kind for one task.
func (h *harness) eventsFor(kind string, team, task int64) []map[string]any {
	var out []map[string]any
	for _, e := range h.events(kind) {
		if num(e["team_id"]) == team && num(e["task_id"]) == task {
			out = append(out, e)
		}
	}
	return out
}

func (h *harness) taskMsgs() []map[string]any {
	var out []map[string]any
	for _, m := range h.published("task-manager.task.") {
		out = append(out, decode(m.Data))
	}
	return out
}

func (h *harness) taskMsgsFor(team, task int64) []map[string]any {
	var out []map[string]any
	for _, m := range h.taskMsgs() {
		if num(m["team_id"]) == team && num(m["task_id"]) == task {
			out = append(out, m)
		}
	}
	return out
}

func (h *harness) commands() []wire.Msg { return h.published("task-manager.command.") }

func (h *harness) audits(action string) []map[string]any {
	var out []map[string]any
	for _, m := range h.published("task-manager.audit." + action + ".pub") {
		out = append(out, decode(m.Data))
	}
	return out
}

// current returns the current task (nil when absent or archived).
func (h *harness) current(team, task int64) *domain.Task {
	for _, v := range h.St.CurrentTasks() {
		if v.TeamID == team && v.TaskID == task {
			return v
		}
	}
	return nil
}

func (h *harness) currentOf(team int64) []*domain.Task {
	var out []*domain.Task
	for _, v := range h.St.CurrentTasks() {
		if v.TeamID == team {
			out = append(out, v)
		}
	}
	return out
}

// taskIDs returns the task_ids of an intake_processed event.
func taskIDs(e map[string]any) []int64 {
	var out []int64
	for _, v := range list(e["task_ids"]) {
		out = append(out, num(v))
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func ids64(v any) []int64 {
	var out []int64
	for _, x := range list(v) {
		out = append(out, num(x))
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// seedTask creates one task of team for Flow flowID through intake and
// returns its task_id. Team and binding are set up when missing.
func (h *harness) seedTask(team, source int64, external string, flows ...flowDef) int64 {
	h.t.Helper()
	if len(flows) == 0 {
		flows = []flowDef{flow(2)}
	}
	h.ensureTeam(team)
	h.bind(team, source, flows...)
	before := len(h.St.CurrentTasks())
	ack, _ := h.intake(team, source, entry(h.nextSeq("intake-"+tok(team)), external))
	if ack.Kind != wire.AckOK {
		h.t.Errorf("seed intake for %s: ack %v", external, ack.Kind)
	}
	h.flush()
	var newest int64
	for _, v := range h.currentOf(team) {
		if v.ExternalID == external && v.TaskID > newest {
			newest = v.TaskID
		}
	}
	if newest == 0 || len(h.St.CurrentTasks()) <= before {
		h.t.Errorf("seed task %s of team %d was not created", external, team)
	}
	return newest
}

func (h *harness) ensureTeam(team int64) {
	if h.teams[team] {
		return
	}
	h.teams[team] = true
	image := imageTeam3
	if team == 4 {
		image = imageTeam4
	}
	h.teamCreated(team, image)
}

// claimed seeds a task and lets execID win claim (1,1); it returns the task
// and the created publisher_seq.
func (h *harness) claimed(team, execID int64, external string, flows ...flowDef) (int64, int64) {
	h.t.Helper()
	task := h.seedTask(team, 1, external, flows...)
	h.ensureExecutor(team, execID)
	seq := h.created(team, execID, task, 1, 1)
	h.flush()
	return task, seq
}

func (h *harness) runningTask(team, execID int64, external string, flows ...flowDef) int64 {
	task, _ := h.claimed(team, execID, external, flows...)
	h.running(team, execID, task, 1, 1)
	h.flush()
	return task
}

func (h *harness) ensureExecutor(team, execID int64) {
	if h.execs[execID] {
		return
	}
	h.execs[execID] = true
	h.registered(team, execID, "openhands")
}

// ---- schemas -------------------------------------------------------------------

var (
	schemaMu    sync.Mutex
	schemaCache = map[string]*jsonschema.Schema{}
)

// contractFixturePath resolves a baseline-relative test input in the supplied fixture tree.
func contractFixturePath(t *testing.T, rel string) string {
	t.Helper()
	dir := os.Getenv("FLOWAI_CONTRACT_FIXTURE_DIR")
	if !filepath.IsAbs(dir) {
		t.Fatal("set FLOWAI_CONTRACT_FIXTURE_DIR to an absolute fixture directory")
	}
	return filepath.Join(dir, rel)
}

// compileSchema resolves owned schemas locally and publisher inputs in the fixture tree.
func compileSchema(t *testing.T, rel string) *jsonschema.Schema {
	t.Helper()
	var abs string
	if name, owned := strings.CutPrefix(rel, "svc/task-manager/schemas/"); owned {
		var err error
		abs, err = filepath.Abs(filepath.Join("..", "schemas", name))
		if err != nil {
			t.Fatalf("schema path: %v", err)
		}
	} else {
		abs = contractFixturePath(t, rel)
	}

	schemaMu.Lock()
	defer schemaMu.Unlock()
	if s, ok := schemaCache[abs]; ok {
		return s
	}
	if _, err := os.Stat(abs); err != nil {
		t.Fatalf("schema %s: %v", rel, err)
	}
	c := jsonschema.NewCompiler()
	s, err := c.Compile(abs)
	if err != nil {
		t.Fatalf("compile %s: %v", rel, err)
	}
	schemaCache[abs] = s
	return s
}

func validateAgainst(t *testing.T, rel string, raw []byte) error {
	t.Helper()
	inst, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	return compileSchema(t, rel).Validate(inst)
}

// schemaForSubject maps a published subject to task-manager's schema file.
func schemaForSubject(subject string) string {
	parts := strings.Split(subject, ".")
	switch {
	case strings.HasPrefix(subject, "task-manager.event.") && len(parts) == 4:
		return "event." + parts[2] + ".schema.json"
	case strings.HasPrefix(subject, "task-manager.audit.") && len(parts) == 4:
		return "audit." + parts[2] + ".schema.json"
	case strings.HasPrefix(subject, "task-manager.task."):
		return "task.schema.json"
	case strings.HasPrefix(subject, "task-manager.command."):
		return "command.schema.json"
	}
	return ""
}

// checkContracts validates every message published and every reply given
// during the test, and the publication invariants every message keeps.
func (h *harness) checkContracts() {
	t := h.t
	for _, m := range h.Bus.Messages() {
		schema := schemaForSubject(m.Subject)
		if schema == "" {
			t.Errorf("published on an unexpected subject %q", m.Subject)
			continue
		}
		if err := validateAgainst(t, "svc/task-manager/schemas/"+schema, m.Data); err != nil {
			t.Errorf("%s violates %s: %v\n%s", m.Subject, schema, err, m.Data)
		}
		body := decode(m.Data)
		seq := num(body["publisher_seq"])
		if got, want := m.Header["Nats-Msg-Id"], "task-manager-"+tok(seq); got != want {
			t.Errorf("%s: Nats-Msg-Id %q, want %q", m.Subject, got, want)
		}
		if m.Header["team"] == "" {
			t.Errorf("%s: no team header", m.Subject)
		}
		if strings.Contains(string(m.Data), "claim_token") {
			t.Errorf("%s carries a claim token field", m.Subject)
		}
	}
	for _, r := range h.requests {
		if err := validateAgainst(t, "svc/task-manager/schemas/"+r.schema, r.raw); err != nil {
			t.Errorf("accepted request violates %s: %v\n%s", r.schema, err, r.raw)
		}
	}
	for _, r := range h.replies {
		if err := validateAgainst(t, "svc/task-manager/schemas/"+r.schema, r.raw); err != nil {
			t.Errorf("reply violates %s: %v\n%s", r.schema, err, r.raw)
		}
	}
}

func hexSHA(b []byte) string { return hex.EncodeToString(b) }
