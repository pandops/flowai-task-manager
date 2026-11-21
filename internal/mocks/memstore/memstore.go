// Package memstore is the in-process fake of store.Store for unit tests.
// Every Write works on a copy of the state and replaces it only when the
// callback succeeds, so a failed transaction leaves nothing behind. Per-team
// counters and the publisher sequence live outside the copy: like
// PostgreSQL sequences they are never rolled back.
package memstore

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pandops/flowai-task-manager/internal/domain"
	"github.com/pandops/flowai-task-manager/internal/store"
)

type archived struct {
	task       *domain.Task
	archivedAt time.Time
}

type state struct {
	consumed map[string]bool
	teams    map[int64]store.Team
	execs    map[int64]store.Executor
	tasks    map[[2]int64]*domain.Task
	archive  map[[2]int64]archived
	controls map[[2]int64]store.Control
	outbox   []store.OutboxMessage
}

func (s *state) clone() *state {
	c := &state{
		consumed: map[string]bool{}, teams: map[int64]store.Team{}, execs: map[int64]store.Executor{},
		tasks: map[[2]int64]*domain.Task{}, archive: map[[2]int64]archived{}, controls: map[[2]int64]store.Control{},
		outbox: append([]store.OutboxMessage(nil), s.outbox...),
	}
	for k, v := range s.consumed {
		c.consumed[k] = v
	}
	for k, v := range s.teams {
		c.teams[k] = v
	}
	for k, v := range s.execs {
		c.execs[k] = v
	}
	for k, v := range s.tasks {
		c.tasks[k] = v.Clone()
	}
	for k, v := range s.archive {
		c.archive[k] = v
	}
	for k, v := range s.controls {
		c.controls[k] = v
	}
	return c
}

// Store is the fake.
type Store struct {
	mu       sync.Mutex
	st       *state
	counters map[string]int64
	pubSeq   int64
	// Hook, when set, is called at every write step ("save_task",
	// "append_outbox", "archive_task") and fails the transaction when it
	// returns an error.
	Hook func(step string) error
	// Reads counts every read of task rows (reads and transactions).
	Reads int
	// Writes counts committed transactions.
	Writes int
}

// New returns an empty fake store.
func New() *Store {
	return &Store{st: (&state{}).cloneEmpty(), counters: map[string]int64{}}
}

func (s *state) cloneEmpty() *state {
	return &state{consumed: map[string]bool{}, teams: map[int64]store.Team{}, execs: map[int64]store.Executor{},
		tasks: map[[2]int64]*domain.Task{}, archive: map[[2]int64]archived{}, controls: map[[2]int64]store.Control{}}
}

func consumedKey(p string, seq int64) string { return p + "#" + strconv.FormatInt(seq, 10) }

type tx struct {
	s  *Store
	st *state
}

// Write implements store.Store.
func (s *Store) Write(ctx context.Context, fn func(store.Tx) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	work := s.st.clone()
	if err := fn(&tx{s: s, st: work}); err != nil {
		return err
	}
	s.st = work
	s.Writes++
	return nil
}

func (t *tx) hook(step string) error {
	if t.s.Hook != nil {
		return t.s.Hook(step)
	}
	return nil
}

func (t *tx) Consumed(p string, seq int64) (bool, error) {
	return t.st.consumed[consumedKey(p, seq)], nil
}
func (t *tx) MarkConsumed(p string, seq int64) error {
	t.st.consumed[consumedKey(p, seq)] = true
	return nil
}
func (t *tx) Team(id int64) (*store.Team, error) {
	v, ok := t.st.teams[id]
	if !ok {
		return nil, nil
	}
	return &v, nil
}
func (t *tx) PutTeam(v store.Team) error { t.st.teams[v.TeamID] = v; return nil }
func (t *tx) Executor(id int64) (*store.Executor, error) {
	v, ok := t.st.execs[id]
	if !ok {
		return nil, nil
	}
	return &v, nil
}
func (t *tx) PutExecutor(v store.Executor) error { t.st.execs[v.ExecutorID] = v; return nil }

func (t *tx) NextTeamID(team int64, kind string) (int64, error) {
	key := strconv.FormatInt(team, 10) + "/" + kind
	t.s.counters[key]++
	return t.s.counters[key], nil
}
func (t *tx) NextPublisherSeq() (int64, error) { t.s.pubSeq++; return t.s.pubSeq, nil }
func (t *tx) AppendOutbox(m store.OutboxMessage) error {
	if err := t.hook("append_outbox"); err != nil {
		return err
	}
	t.st.outbox = append(t.st.outbox, m)
	return nil
}

func (t *tx) Task(team, id int64) (*domain.Task, error) {
	t.s.Reads++
	v, ok := t.st.tasks[[2]int64{team, id}]
	if !ok {
		return nil, nil
	}
	return v.Clone(), nil
}
func (t *tx) FindCurrent(team, source int64, ext string, flow int64) (int64, bool, error) {
	for _, v := range t.st.tasks {
		if v.TeamID == team && v.SourceID == source && v.ExternalID == ext && v.FlowID == flow {
			return v.TaskID, true, nil
		}
	}
	return 0, false, nil
}
func (t *tx) SaveTask(v *domain.Task) error {
	if err := t.hook("save_task"); err != nil {
		return err
	}
	key := [2]int64{v.TeamID, v.TaskID}
	if _, exists := t.st.tasks[key]; !exists {
		if _, ok, _ := t.FindCurrent(v.TeamID, v.SourceID, v.ExternalID, v.FlowID); ok {
			return store.ErrConflict
		}
	}
	t.st.tasks[key] = v.Clone()
	return nil
}
func (t *tx) ArchiveTask(v *domain.Task, at time.Time) error {
	if err := t.hook("archive_task"); err != nil {
		return err
	}
	key := [2]int64{v.TeamID, v.TaskID}
	if _, ok := t.st.archive[key]; ok {
		return errors.New("memstore: archive row exists")
	}
	t.st.archive[key] = archived{task: v.Clone(), archivedAt: at}
	delete(t.st.tasks, key)
	return nil
}
func (t *tx) TasksHeldBy(exec int64) ([]*domain.Task, error) {
	var out []*domain.Task
	for _, v := range t.st.tasks {
		if c := v.CurrentClaim(); c != nil && v.Held() && c.ExecutorID == exec {
			out = append(out, v.Clone())
		}
	}
	sortTasks(out)
	return out, nil
}
func (t *tx) DueTasks(now time.Time, limit int) ([]*domain.Task, error) {
	var out []*domain.Task
	for _, v := range t.st.tasks {
		if v.NextDueAt != nil && !v.NextDueAt.After(now) {
			out = append(out, v.Clone())
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].NextDueAt.Equal(*out[j].NextDueAt) {
			return out[i].NextDueAt.Before(*out[j].NextDueAt)
		}
		if out[i].TeamID != out[j].TeamID {
			return out[i].TeamID < out[j].TeamID
		}
		return out[i].TaskID < out[j].TaskID
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
func (t *tx) Control(team, task, op int64, action string) (*store.Control, error) {
	for _, c := range t.st.controls {
		if c.TeamID == team && c.TaskID == task && c.OperatorID == op && c.Action == action {
			v := c
			return &v, nil
		}
	}
	return nil, nil
}
func (t *tx) ControlByID(team, id int64) (*store.Control, error) {
	c, ok := t.st.controls[[2]int64{team, id}]
	if !ok {
		return nil, nil
	}
	return &c, nil
}
func (t *tx) PutControl(c store.Control) error {
	t.st.controls[[2]int64{c.TeamID, c.ControlID}] = c
	return nil
}

func sortTasks(out []*domain.Task) {
	sort.Slice(out, func(i, j int) bool {
		if out[i].TeamID != out[j].TeamID {
			return out[i].TeamID < out[j].TeamID
		}
		return out[i].TaskID < out[j].TaskID
	})
}

// Consumed implements store.Store.
func (s *Store) Consumed(_ context.Context, p string, seq int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.st.consumed[consumedKey(p, seq)], nil
}

// Team implements store.Store.
func (s *Store) Team(_ context.Context, id int64) (*store.Team, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.st.teams[id]
	if !ok {
		return nil, nil
	}
	return &v, nil
}

// Executor implements store.Store.
func (s *Store) Executor(_ context.Context, id int64) (*store.Executor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.st.execs[id]
	if !ok {
		return nil, nil
	}
	return &v, nil
}

// GetTask implements store.Store.
func (s *Store) GetTask(_ context.Context, team, id int64) (*domain.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Reads++
	v, ok := s.st.tasks[[2]int64{team, id}]
	if !ok {
		return nil, nil
	}
	return v.Clone(), nil
}

// GetArchived implements store.Store.
func (s *Store) GetArchived(_ context.Context, team, id int64) (*domain.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Reads++
	v, ok := s.st.archive[[2]int64{team, id}]
	if !ok {
		return nil, nil
	}
	return v.task.Clone(), nil
}

func less(a, b *domain.Task, withTeam bool) bool {
	if !a.IngestedAt.Equal(b.IngestedAt) {
		return a.IngestedAt.After(b.IngestedAt)
	}
	if withTeam && a.TeamID != b.TeamID {
		return a.TeamID > b.TeamID
	}
	return a.TaskID > b.TaskID
}

func afterPos(t *domain.Task, p *store.Position, withTeam bool) bool {
	if p == nil {
		return true
	}
	if !t.IngestedAt.Equal(p.IngestedAt) {
		return t.IngestedAt.Before(p.IngestedAt)
	}
	if withTeam && t.TeamID != p.TeamID {
		return t.TeamID < p.TeamID
	}
	return t.TaskID < p.TaskID
}

// ListTasks implements store.Store.
func (s *Store) ListTasks(_ context.Context, q store.TaskQuery) ([]*domain.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Reads++
	var src []*domain.Task
	if q.Scope != store.ScopeArchive {
		for _, v := range s.st.tasks {
			src = append(src, v)
		}
	}
	if q.Scope != store.ScopeCurrent {
		for _, a := range s.st.archive {
			src = append(src, a.task)
		}
	}
	var out []*domain.Task
	for _, v := range src {
		if q.ExecutorID != nil && q.Scope != store.ScopeCurrent {
			continue
		}
		if v.TeamID != q.TeamID || !match(v, q) || !afterPos(v, q.After, false) {
			continue
		}
		out = append(out, v.Clone())
	}
	sort.Slice(out, func(i, j int) bool { return less(out[i], out[j], false) })
	if q.Limit > 0 && len(out) > q.Limit {
		out = out[:q.Limit]
	}
	return out, nil
}

func match(v *domain.Task, q store.TaskQuery) bool {
	if q.ExecutorID != nil {
		c := v.CurrentClaim()
		if c == nil || !v.Held() || c.ExecutorID != *q.ExecutorID {
			return false
		}
	}
	if q.State != "" && v.State != q.State {
		return false
	}
	if q.FlowID != nil && v.FlowID != *q.FlowID {
		return false
	}
	if q.SourceID != nil && v.SourceID != *q.SourceID {
		return false
	}
	if q.Tag != "" && v.RequiredTag != q.Tag {
		return false
	}
	if q.Since != nil && v.IngestedAt.Before(*q.Since) {
		return false
	}
	if q.DateStart != nil && (v.IngestedAt.Before(*q.DateStart) || !v.IngestedAt.Before(*q.DateEnd)) {
		return false
	}
	if q.TaskName != "" {
		if v.TaskName != q.TaskName {
			return false
		}
	} else if q.Search != "" {
		needle := strings.ToLower(q.Search)
		hay := []string{strconv.FormatInt(v.TaskID, 10), strconv.FormatInt(v.FlowID, 10), strconv.FormatInt(v.SourceID, 10), strings.ToLower(v.ExternalID)}
		if c := v.LatestClaim(); c != nil {
			hay = append(hay, strconv.FormatInt(c.ExecutorID, 10))
		}
		found := false
		for _, h := range hay {
			if strings.Contains(h, needle) {
				found = true
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// AdminList implements store.Store.
func (s *Store) AdminList(_ context.Context, q store.AdminQuery) ([]*domain.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Reads++
	var src []*domain.Task
	for _, v := range s.st.tasks {
		src = append(src, v)
	}
	for _, a := range s.st.archive {
		src = append(src, a.task)
	}
	var out []*domain.Task
	for _, v := range src {
		if q.TeamID != nil && v.TeamID != *q.TeamID {
			continue
		}
		if q.State != "" && v.State != q.State {
			continue
		}
		if q.Tag != "" && v.RequiredTag != q.Tag {
			continue
		}
		if q.FlowID != nil && v.FlowID != *q.FlowID {
			continue
		}
		if !afterPos(v, q.After, true) {
			continue
		}
		out = append(out, v.Clone())
	}
	sort.Slice(out, func(i, j int) bool { return less(out[i], out[j], true) })
	if q.Limit > 0 && len(out) > q.Limit {
		out = out[:q.Limit]
	}
	return out, nil
}

// PendingOutbox implements store.Store.
func (s *Store) PendingOutbox(_ context.Context, limit int) ([]store.OutboxMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := append([]store.OutboxMessage(nil), s.st.outbox...)
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// MarkSent implements store.Store.
func (s *Store) MarkSent(_ context.Context, seq int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, m := range s.st.outbox {
		if m.PublisherSeq == seq {
			s.st.outbox = append(s.st.outbox[:i:i], s.st.outbox[i+1:]...)
			return nil
		}
	}
	return nil
}

// Archived returns a copy of an archive row and its time (test helper).
func (s *Store) Archived(team, id int64) (*domain.Task, time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.st.archive[[2]int64{team, id}]
	if !ok {
		return nil, time.Time{}, false
	}
	return a.task.Clone(), a.archivedAt, true
}

// CurrentTasks returns copies of every current task (test helper).
func (s *Store) CurrentTasks() []*domain.Task {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*domain.Task
	for _, v := range s.st.tasks {
		out = append(out, v.Clone())
	}
	sortTasks(out)
	return out
}

// OutboxLen returns the number of unsent outbox rows (test helper).
func (s *Store) OutboxLen() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.st.outbox)
}

var _ store.Store = (*Store)(nil)
