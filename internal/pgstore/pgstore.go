// Package pgstore is the PostgreSQL implementation of store.Store. Every
// object lives in the configured schema; write transactions serialize on one
// advisory lock so commit order equals publisher_seq order.
package pgstore

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pandops/flowai-task-manager/internal/domain"
	"github.com/pandops/flowai-task-manager/internal/store"
)

//go:embed sql/*.sql
var migrations embed.FS

// writeLock is the advisory lock key every write transaction takes.
const writeLock int64 = 0x746d5f7772697465 // "tm_write"

// migrateLock is the advisory lock key of Migrate.
const migrateLock int64 = 0x746d5f6d69677261 // "tm_migra"

// Store is the PostgreSQL store.
type Store struct {
	// Hook, when set, is called at every write step ("save_task",
	// "append_outbox", "archive_task") and fails the transaction when it
	// returns an error. Only tests set it.
	Hook func(step string) error

	schema string
	pool   *pgxpool.Pool
	// counters allocates per-team numbers outside the write transaction so
	// a rolled-back number is never reused. It has its own connections:
	// the lock holder must never wait for a connection held by a writer
	// that waits for the lock.
	counters *pgxpool.Pool
}

// Open connects with dsn; schema names the service's schema.
func Open(ctx context.Context, dsn, schema string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("pgstore: parse dsn: %w", err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("pgstore: connect: %w", err)
	}
	ccfg := cfg.Copy()
	ccfg.MaxConns, ccfg.MinConns = 2, 0
	counters, err := pgxpool.NewWithConfig(ctx, ccfg)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("pgstore: connect: %w", err)
	}
	s := &Store{schema: schema, pool: pool, counters: counters}
	if err := pool.Ping(ctx); err != nil {
		s.Close()
		return nil, fmt.Errorf("pgstore: ping: %w", err)
	}
	return s, nil
}

// Migrate creates the schema when missing and applies every migration.
// The schema is looked up in pg_namespace first, because the deployment's
// role has no CREATE on the database and the schema exists already.
func (s *Store) Migrate(ctx context.Context) error {
	var exists bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)`, s.schema).Scan(&exists); err != nil {
		return fmt.Errorf("pgstore: look up schema: %w", err)
	}
	if !exists {
		if _, err := s.pool.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+pgx.Identifier{s.schema}.Sanitize()); err != nil {
			return fmt.Errorf("pgstore: create schema: %w", err)
		}
	}
	files, err := migrations.ReadDir("sql")
	if err != nil {
		return err
	}
	names := make([]string, 0, len(files))
	for _, f := range files {
		names = append(names, f.Name())
	}
	sort.Strings(names)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, migrateLock); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return fmt.Errorf("pgstore: migrations table: %w", err)
	}
	for _, name := range names {
		var done bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)`, name).Scan(&done); err != nil {
			return err
		}
		if done {
			continue
		}
		body, err := migrations.ReadFile("sql/" + name)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, string(body)); err != nil {
			return fmt.Errorf("pgstore: migration %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, name); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// Close releases the pools.
func (s *Store) Close() {
	s.pool.Close()
	s.counters.Close()
}

// Ping checks the connection (readiness).
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// Write runs fn in one transaction under the global write lock.
func (s *Store) Write(ctx context.Context, fn func(store.Tx) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, writeLock); err != nil {
		return err
	}
	if err := fn(&wtx{s: s, ctx: ctx, tx: tx}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

type wtx struct {
	s   *Store
	ctx context.Context
	tx  pgx.Tx
}

func (t *wtx) hook(step string) error {
	if t.s.Hook != nil {
		return t.s.Hook(step)
	}
	return nil
}

func (t *wtx) Consumed(p string, seq int64) (bool, error) {
	var ok bool
	err := t.tx.QueryRow(t.ctx, `SELECT EXISTS (SELECT 1 FROM consumed WHERE publisher_key = $1 AND seq = $2)`, p, seq).Scan(&ok)
	return ok, err
}

func (t *wtx) MarkConsumed(p string, seq int64) error {
	_, err := t.tx.Exec(t.ctx, `INSERT INTO consumed (publisher_key, seq) VALUES ($1, $2) ON CONFLICT DO NOTHING`, p, seq)
	return err
}

func scanTeam(row pgx.Row) (*store.Team, error) {
	var v store.Team
	err := row.Scan(&v.TeamID, &v.Archived, &v.DefaultImage, &v.TeamName)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &v, nil
}

const teamCols = `team_id, archived, default_image, team_name`

func (t *wtx) Team(id int64) (*store.Team, error) {
	return scanTeam(t.tx.QueryRow(t.ctx, `SELECT `+teamCols+` FROM teams WHERE team_id = $1`, id))
}

func (t *wtx) PutTeam(v store.Team) error {
	_, err := t.tx.Exec(t.ctx, `INSERT INTO teams (team_id, archived, default_image, team_name) VALUES ($1, $2, $3, $4)
		ON CONFLICT (team_id) DO UPDATE SET archived = EXCLUDED.archived, default_image = EXCLUDED.default_image,
		team_name = EXCLUDED.team_name`,
		v.TeamID, v.Archived, v.DefaultImage, v.TeamName)
	return err
}

const executorCols = `executor_id, team_id, tag, removed, lost_seq_applied`

func scanExecutor(row pgx.Row) (*store.Executor, error) {
	var v store.Executor
	err := row.Scan(&v.ExecutorID, &v.TeamID, &v.Tag, &v.Removed, &v.LostSeqApplied)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &v, nil
}

func (t *wtx) Executor(id int64) (*store.Executor, error) {
	return scanExecutor(t.tx.QueryRow(t.ctx, `SELECT `+executorCols+` FROM executors WHERE executor_id = $1`, id))
}

func (t *wtx) PutExecutor(v store.Executor) error {
	_, err := t.tx.Exec(t.ctx, `INSERT INTO executors (executor_id, team_id, tag, removed, lost_seq_applied) VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (executor_id) DO UPDATE SET team_id = EXCLUDED.team_id, tag = EXCLUDED.tag, removed = EXCLUDED.removed,
		lost_seq_applied = EXCLUDED.lost_seq_applied`, v.ExecutorID, v.TeamID, v.Tag, v.Removed, v.LostSeqApplied)
	return err
}

// NextTeamID commits the counter on its own connection, so the number
// stays used when the write transaction rolls back.
func (t *wtx) NextTeamID(team int64, kind string) (int64, error) {
	var v int64
	err := t.s.counters.QueryRow(t.ctx, `INSERT INTO team_counters (team_id, kind, last) VALUES ($1, $2, 1)
		ON CONFLICT (team_id, kind) DO UPDATE SET last = team_counters.last + 1 RETURNING last`, team, kind).Scan(&v)
	return v, err
}

// NextPublisherSeq draws from the publisher_seq sequence; a sequence value
// is never handed out twice, also after a rollback.
func (t *wtx) NextPublisherSeq() (int64, error) {
	var v int64
	err := t.tx.QueryRow(t.ctx, `SELECT nextval('publisher_seq')`).Scan(&v)
	return v, err
}

func (t *wtx) AppendOutbox(m store.OutboxMessage) error {
	if err := t.hook("append_outbox"); err != nil {
		return err
	}
	header, err := json.Marshal(m.Header)
	if err != nil {
		return err
	}
	_, err = t.tx.Exec(t.ctx, `INSERT INTO outbox (publisher_seq, subject, header, data, ensure_team, ensure_tag) VALUES ($1, $2, $3, $4, $5, $6)`,
		m.PublisherSeq, m.Subject, header, m.Data, m.EnsureTeam, m.EnsureTag)
	return err
}

func decodeTask(raw []byte) (*domain.Task, error) {
	var v domain.Task
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, err
	}
	return &v, nil
}

func oneTask(row pgx.Row) (*domain.Task, error) {
	var raw []byte
	err := row.Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return decodeTask(raw)
}

func manyTasks(rows pgx.Rows, err error) ([]*domain.Task, error) {
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.Task
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		v, err := decodeTask(raw)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (t *wtx) Task(team, id int64) (*domain.Task, error) {
	return oneTask(t.tx.QueryRow(t.ctx, `SELECT record FROM tasks WHERE team_id = $1 AND task_id = $2`, team, id))
}

func (t *wtx) FindCurrent(team, source int64, external string, flow int64) (int64, bool, error) {
	var id int64
	err := t.tx.QueryRow(t.ctx, `SELECT task_id FROM tasks WHERE team_id = $1 AND source_id = $2 AND external_id = $3 AND flow_id = $4`,
		team, source, external, flow).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	return id, err == nil, err
}

func held(v *domain.Task) *int64 {
	if v.Held() {
		id := v.CurrentClaim().ExecutorID
		return &id
	}
	return nil
}

func latest(v *domain.Task) *int64 {
	if c := v.LatestClaim(); c != nil {
		id := c.ExecutorID
		return &id
	}
	return nil
}

func (t *wtx) SaveTask(v *domain.Task) error {
	if err := t.hook("save_task"); err != nil {
		return err
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = t.tx.Exec(t.ctx, `INSERT INTO tasks (team_id, task_id, source_id, external_id, flow_id, required_tag, state, ingested_at,
		next_due_at, held_executor, last_executor, record, task_name) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		ON CONFLICT (team_id, task_id) DO UPDATE SET state = EXCLUDED.state, next_due_at = EXCLUDED.next_due_at,
		held_executor = EXCLUDED.held_executor, last_executor = EXCLUDED.last_executor, record = EXCLUDED.record`,
		v.TeamID, v.TaskID, v.SourceID, v.ExternalID, v.FlowID, v.RequiredTag, v.State, v.IngestedAt, v.NextDueAt, held(v), latest(v), raw, v.TaskName)
	return err
}

func (t *wtx) ArchiveTask(v *domain.Task, at time.Time) error {
	if err := t.hook("archive_task"); err != nil {
		return err
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if _, err := t.tx.Exec(t.ctx, `INSERT INTO task_archive (team_id, task_id, source_id, external_id, flow_id, required_tag, state,
		ingested_at, last_executor, archived_at, record, task_name) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
		v.TeamID, v.TaskID, v.SourceID, v.ExternalID, v.FlowID, v.RequiredTag, v.State, v.IngestedAt, latest(v), at, raw, v.TaskName); err != nil {
		return err
	}
	_, err = t.tx.Exec(t.ctx, `DELETE FROM tasks WHERE team_id = $1 AND task_id = $2`, v.TeamID, v.TaskID)
	return err
}

func (t *wtx) TasksHeldBy(exec int64) ([]*domain.Task, error) {
	return manyTasks(t.tx.Query(t.ctx, `SELECT record FROM tasks WHERE held_executor = $1 ORDER BY team_id, task_id`, exec))
}

func (t *wtx) DueTasks(now time.Time, limit int) ([]*domain.Task, error) {
	return manyTasks(t.tx.Query(t.ctx, `SELECT record FROM tasks WHERE next_due_at <= $1 ORDER BY next_due_at, team_id, task_id LIMIT $2`, now, limit))
}

const controlCols = `team_id, control_id, task_id, operator_id, action, status, created_at`

func scanControl(row pgx.Row) (*store.Control, error) {
	var v store.Control
	err := row.Scan(&v.TeamID, &v.ControlID, &v.TaskID, &v.OperatorID, &v.Action, &v.Status, &v.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	v.CreatedAt = v.CreatedAt.UTC()
	return &v, nil
}

func (t *wtx) Control(team, task, operator int64, action string) (*store.Control, error) {
	return scanControl(t.tx.QueryRow(t.ctx, `SELECT `+controlCols+` FROM controls WHERE team_id = $1 AND task_id = $2 AND operator_id = $3 AND action = $4`,
		team, task, operator, action))
}

func (t *wtx) ControlByID(team, id int64) (*store.Control, error) {
	return scanControl(t.tx.QueryRow(t.ctx, `SELECT `+controlCols+` FROM controls WHERE team_id = $1 AND control_id = $2`, team, id))
}

func (t *wtx) PutControl(v store.Control) error {
	_, err := t.tx.Exec(t.ctx, `INSERT INTO controls (`+controlCols+`) VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (team_id, control_id) DO UPDATE SET status = EXCLUDED.status`,
		v.TeamID, v.ControlID, v.TaskID, v.OperatorID, v.Action, v.Status, v.CreatedAt)
	return err
}

// ---- reads -------------------------------------------------------------

func (s *Store) Consumed(ctx context.Context, p string, seq int64) (bool, error) {
	var ok bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM consumed WHERE publisher_key = $1 AND seq = $2)`, p, seq).Scan(&ok)
	return ok, err
}

func (s *Store) Team(ctx context.Context, id int64) (*store.Team, error) {
	return scanTeam(s.pool.QueryRow(ctx, `SELECT `+teamCols+` FROM teams WHERE team_id = $1`, id))
}

func (s *Store) Executor(ctx context.Context, id int64) (*store.Executor, error) {
	return scanExecutor(s.pool.QueryRow(ctx, `SELECT `+executorCols+` FROM executors WHERE executor_id = $1`, id))
}

func (s *Store) GetTask(ctx context.Context, team, id int64) (*domain.Task, error) {
	return oneNamedTask(s.pool.QueryRow(ctx, `SELECT record, task_name FROM tasks WHERE team_id = $1 AND task_id = $2`, team, id))
}

func (s *Store) GetArchived(ctx context.Context, team, id int64) (*domain.Task, error) {
	return oneNamedTask(s.pool.QueryRow(ctx, `SELECT record, task_name FROM task_archive WHERE team_id = $1 AND task_id = $2`, team, id))
}

// where collects SQL predicates with numbered arguments.
type where struct {
	parts []string
	args  []any
}

func (w *where) add(pred string, args ...any) {
	for _, a := range args {
		w.args = append(w.args, a)
		pred = strings.Replace(pred, "?", "$"+strconv.Itoa(len(w.args)), 1)
	}
	w.parts = append(w.parts, pred)
}

func (w *where) sql() string {
	if len(w.parts) == 0 {
		return ""
	}
	return " WHERE " + strings.Join(w.parts, " AND ")
}

func likeEscape(s string) string {
	return strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(s)
}

// ListTasks applies the team predicate first, then every filter, the
// keyset position and the limit, all in SQL.
func (s *Store) ListTasks(ctx context.Context, q store.TaskQuery) ([]*domain.Task, error) {
	table := "tasks"
	switch q.Scope {
	case store.ScopeArchive:
		table = "task_archive"
	case store.ScopeAll:
		// The team and filter predicates below are pushed into both
		// branches, which keep their (team_id, ingested_at, task_id) order
		// indexes.
		table = `(SELECT team_id, task_id, source_id, external_id, flow_id, required_tag, state, ingested_at, last_executor, record,
			task_name FROM tasks UNION ALL SELECT team_id, task_id, source_id, external_id, flow_id, required_tag, state, ingested_at,
			last_executor, record, task_name FROM task_archive) AS all_tasks`
	}
	w := &where{}
	w.add("team_id = ?", q.TeamID)
	if q.ExecutorID != nil {
		if q.Scope != store.ScopeCurrent {
			return nil, nil
		}
		w.add("held_executor = ?", *q.ExecutorID)
	}
	if q.State != "" {
		w.add("state = ?", q.State)
	}
	if q.FlowID != nil {
		w.add("flow_id = ?", *q.FlowID)
	}
	if q.SourceID != nil {
		w.add("source_id = ?", *q.SourceID)
	}
	if q.Tag != "" {
		w.add("required_tag = ?", q.Tag)
	}
	if q.Since != nil {
		w.add("ingested_at >= ?", *q.Since)
	}
	if q.DateStart != nil && q.DateEnd != nil {
		w.add("ingested_at >= ? AND ingested_at < ?", *q.DateStart, *q.DateEnd)
	}
	if q.TaskName != "" {
		// Stored names are lowercase, as team names are; the service passes
		// the lowercase term.
		w.add("task_name = ?", q.TaskName)
	} else if q.Search != "" {
		w.add(`(task_id::text LIKE ? OR flow_id::text LIKE ? OR source_id::text LIKE ? OR lower(external_id) LIKE ? OR
			coalesce(last_executor::text, '') LIKE ?)`, repeat("%"+likeEscape(strings.ToLower(q.Search))+"%", 5)...)
	}
	if q.After != nil {
		w.add("(ingested_at, task_id) < (?, ?)", q.After.IngestedAt, q.After.TaskID)
	}
	query := `SELECT record, task_name FROM ` + table + w.sql() + ` ORDER BY ingested_at DESC, task_id DESC`
	if q.Limit > 0 {
		w.args = append(w.args, q.Limit)
		query += " LIMIT $" + strconv.Itoa(len(w.args))
	}
	return namedTasks(s.pool.Query(ctx, query, w.args...))
}

// namedTasks reads (record, task_name) rows; the task name comes from its
// column.
func namedTasks(rows pgx.Rows, err error) ([]*domain.Task, error) {
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.Task
	for rows.Next() {
		var raw []byte
		var name string
		if err := rows.Scan(&raw, &name); err != nil {
			return nil, err
		}
		v, err := decodeTask(raw)
		if err != nil {
			return nil, err
		}
		v.TaskName = name
		out = append(out, v)
	}
	return out, rows.Err()
}

// oneNamedTask reads one (record, task_name) row.
func oneNamedTask(row pgx.Row) (*domain.Task, error) {
	var raw []byte
	var name string
	err := row.Scan(&raw, &name)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	v, err := decodeTask(raw)
	if err != nil {
		return nil, err
	}
	v.TaskName = name
	return v, nil
}

func repeat(v any, n int) []any {
	out := make([]any, n)
	for i := range out {
		out[i] = v
	}
	return out
}

// AdminList reads the global projection over current and archived tasks.
func (s *Store) AdminList(ctx context.Context, q store.AdminQuery) ([]*domain.Task, error) {
	w := &where{}
	if q.TeamID != nil {
		w.add("team_id = ?", *q.TeamID)
	}
	if q.State != "" {
		w.add("state = ?", q.State)
	}
	if q.Tag != "" {
		w.add("required_tag = ?", q.Tag)
	}
	if q.FlowID != nil {
		w.add("flow_id = ?", *q.FlowID)
	}
	if q.After != nil {
		w.add("(ingested_at, team_id, task_id) < (?, ?, ?)", q.After.IngestedAt, q.After.TeamID, q.After.TaskID)
	}
	query := `SELECT record FROM (SELECT team_id, task_id, flow_id, required_tag, state, ingested_at, record FROM tasks
		UNION ALL SELECT team_id, task_id, flow_id, required_tag, state, ingested_at, record FROM task_archive) AS projection` +
		w.sql() + ` ORDER BY ingested_at DESC, team_id DESC, task_id DESC`
	if q.Limit > 0 {
		w.args = append(w.args, q.Limit)
		query += " LIMIT $" + strconv.Itoa(len(w.args))
	}
	return manyTasks(s.pool.Query(ctx, query, w.args...))
}

// PendingOutbox returns unsent rows in publisher_seq order.
func (s *Store) PendingOutbox(ctx context.Context, limit int) ([]store.OutboxMessage, error) {
	rows, err := s.pool.Query(ctx, `SELECT publisher_seq, subject, header, data, ensure_team, ensure_tag FROM outbox ORDER BY publisher_seq LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []store.OutboxMessage
	for rows.Next() {
		var m store.OutboxMessage
		var header []byte
		if err := rows.Scan(&m.PublisherSeq, &m.Subject, &header, &m.Data, &m.EnsureTeam, &m.EnsureTag); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(header, &m.Header); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// MarkSent deletes an acknowledged outbox row.
func (s *Store) MarkSent(ctx context.Context, seq int64) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM outbox WHERE publisher_seq = $1`, seq)
	return err
}

// ---- diagnostics (tests) -----------------------------------------------

// CurrentTasks returns every current task (tests and diagnostics).
func (s *Store) CurrentTasks() []*domain.Task {
	out, err := manyTasks(s.pool.Query(context.Background(), `SELECT record FROM tasks ORDER BY team_id, task_id`))
	if err != nil {
		return nil
	}
	return out
}

// Archived returns one archive row and its time (tests and diagnostics).
func (s *Store) Archived(team, id int64) (*domain.Task, time.Time, bool) {
	var raw []byte
	var at time.Time
	err := s.pool.QueryRow(context.Background(), `SELECT record, archived_at FROM task_archive WHERE team_id = $1 AND task_id = $2`, team, id).Scan(&raw, &at)
	if err != nil {
		return nil, time.Time{}, false
	}
	v, err := decodeTask(raw)
	if err != nil {
		return nil, time.Time{}, false
	}
	return v, at.UTC(), true
}

// OutboxLen returns the number of unsent outbox rows (tests and diagnostics).
func (s *Store) OutboxLen() int {
	var n int
	if err := s.pool.QueryRow(context.Background(), `SELECT count(*) FROM outbox`).Scan(&n); err != nil {
		return -1
	}
	return n
}

var _ store.Store = (*Store)(nil)
