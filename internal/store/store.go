// Package store defines task-manager's persistence boundary. The
// PostgreSQL implementation lives in internal/pgstore; unit tests use the
// in-process fake in internal/mocks/memstore.
package store

import (
	"context"
	"errors"
	"time"

	"github.com/pandops/flowai-task-manager/internal/domain"
)

// ErrConflict is returned by SaveTask when a new current task would break
// the (team_id, source_id, external_id, flow_id) uniqueness.
var ErrConflict = errors.New("store: current task key conflict")

// Counter kinds of the per-team counters.
const (
	KindTask    = "task"
	KindControl = "control"
)

// Team is the projection of team-registry's team events.
type Team struct {
	TeamID       int64
	Archived     bool
	DefaultImage string
	// TeamName is the team's immutable name from every team event; tasks
	// are named <TeamName>-<task_id>.
	TeamName string
}

// Executor is the projection of executor-registry's events plus the
// latest applied executor_lost sequence.
type Executor struct {
	ExecutorID     int64
	TeamID         int64
	Tag            string
	Removed        bool
	LostSeqApplied int64
}

// Control is one operator control request.
type Control struct {
	TeamID     int64
	ControlID  int64
	TaskID     int64
	OperatorID int64
	Action     string
	Status     string // requested | cancelled | failed
	CreatedAt  time.Time
}

// OutboxMessage is one stream message waiting for its JetStream
// acknowledgement.
type OutboxMessage struct {
	PublisherSeq int64
	Subject      string
	Header       map[string]string
	Data         []byte
	// EnsureTeam/EnsureTag name the task consumer that must exist before a
	// TASK_MANAGER_TASK message is published.
	EnsureTeam int64
	EnsureTag  string
}

// Position is a keyset position of the descending task order.
type Position struct {
	IngestedAt time.Time
	TeamID     int64
	TaskID     int64
}

// Scope selects the tasks a TaskQuery reads.
type Scope int

const (
	// ScopeCurrent reads current tasks only.
	ScopeCurrent Scope = iota
	// ScopeArchive reads archived tasks only.
	ScopeArchive
	// ScopeAll reads current and archived tasks. A task moves from current
	// to the archive in one transaction and keeps its (ingested_at,
	// task_id), so one query sees it exactly once and a keyset traversal
	// neither skips nor repeats it.
	ScopeAll
)

// TaskQuery selects team tasks; TeamID is always applied first.
type TaskQuery struct {
	TeamID     int64
	Scope      Scope
	ExecutorID *int64
	State      string
	Search     string
	// TaskName, when set, replaces Search: only the task with exactly this
	// stored task_name matches.
	TaskName  string
	FlowID    *int64
	SourceID  *int64
	Tag       string
	Since     *time.Time
	DateStart *time.Time
	DateEnd   *time.Time
	After     *Position
	Limit     int
}

// AdminQuery selects the global projection over current and archived
// tasks.
type AdminQuery struct {
	TeamID *int64
	State  string
	Tag    string
	FlowID *int64
	After  *Position
	Limit  int
}

// Tx is one serialized write transaction.
type Tx interface {
	Consumed(publisherKey string, seq int64) (bool, error)
	MarkConsumed(publisherKey string, seq int64) error

	Team(teamID int64) (*Team, error)
	PutTeam(Team) error
	Executor(executorID int64) (*Executor, error)
	PutExecutor(Executor) error

	// NextTeamID allocates the next per-team number of a kind. A number is
	// never reused, also after a rollback.
	NextTeamID(teamID int64, kind string) (int64, error)
	// NextPublisherSeq allocates the next publisher_seq.
	NextPublisherSeq() (int64, error)
	AppendOutbox(OutboxMessage) error

	Task(teamID, taskID int64) (*domain.Task, error)
	FindCurrent(teamID, sourceID int64, externalID string, flowID int64) (int64, bool, error)
	SaveTask(*domain.Task) error
	// ArchiveTask inserts the final record into task_archive and deletes
	// the current task.
	ArchiveTask(t *domain.Task, archivedAt time.Time) error
	TasksHeldBy(executorID int64) ([]*domain.Task, error)
	DueTasks(now time.Time, limit int) ([]*domain.Task, error)

	Control(teamID, taskID, operatorID int64, action string) (*Control, error)
	ControlByID(teamID, controlID int64) (*Control, error)
	PutControl(Control) error
}

// Store is the complete persistence of task-manager.
type Store interface {
	// Write runs fn in one transaction; fn's error rolls it back.
	Write(ctx context.Context, fn func(Tx) error) error

	Consumed(ctx context.Context, publisherKey string, seq int64) (bool, error)
	Team(ctx context.Context, teamID int64) (*Team, error)
	Executor(ctx context.Context, executorID int64) (*Executor, error)
	GetTask(ctx context.Context, teamID, taskID int64) (*domain.Task, error)
	GetArchived(ctx context.Context, teamID, taskID int64) (*domain.Task, error)
	ListTasks(ctx context.Context, q TaskQuery) ([]*domain.Task, error)
	AdminList(ctx context.Context, q AdminQuery) ([]*domain.Task, error)

	PendingOutbox(ctx context.Context, limit int) ([]OutboxMessage, error)
	MarkSent(ctx context.Context, publisherSeq int64) error
}
