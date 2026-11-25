// Package domain holds task-manager's task aggregate: one current task with
// its attempts, claims and accepted history.
package domain

import (
	"encoding/json"
	"time"
)

// Task states (design rule 5).
const (
	StateQueued    = "queued"
	StateCreated   = "created"
	StatePending   = "pending"
	StateRunning   = "running"
	StateFinished  = "finished"
	StateFailed    = "failed"
	StateCancelled = "cancelled"
)

// Release reasons (design rule 14).
const (
	ReasonPendingTimeout    = "pending_timeout"
	ReasonExecutorLost      = "executor_lost"
	ReasonExecutorRestarted = "executor_restarted"
	ReasonExecutorShutdown  = "executor_shutdown"
	ReasonClaimMissing      = "claim_missing"
)

// Due kinds of a scheduled republication.
const (
	DueRelease = "release"
	DueRetry   = "retry"
)

// Image sources.
const (
	ImageFlowDefault = "flow_default"
	ImageTeamDefault = "team_default"
)

// Snapshot is the frozen, non-secret execution snapshot of a task.
type Snapshot struct {
	ExecutionRevision   int64           `json:"execution_revision"`
	RenderedPrompt      string          `json:"rendered_prompt"`
	ExecutionParameters json.RawMessage `json:"execution_parameters"`
	ImageOverride       *string         `json:"image_override"`
	FailureRetryCount   int             `json:"failure_retry_count"`
	RetryWaitSeconds    int             `json:"failure_retry_wait_seconds"`
	PendingLimitSeconds int             `json:"pending_limit_seconds"`
	ReleaseDelaySeconds int             `json:"release_delay_seconds"`
}

// Claim is one claim (task_id, attempt, claim_seq).
type Claim struct {
	Attempt             int64      `json:"attempt"`
	ClaimSeq            int64      `json:"claim_seq"`
	ExecutorID          int64      `json:"executor_id"`
	CreatedPublisherSeq int64      `json:"created_publisher_seq"`
	ClaimedAt           time.Time  `json:"claimed_at"`
	ResolvedImage       string     `json:"resolved_image"`
	ImageSource         string     `json:"image_source"`
	PendingReasons      []string   `json:"pending_reasons"`
	ReleasedReason      *string    `json:"released_reason"`
	EndedAt             *time.Time `json:"ended_at"`
	MissingCount        int        `json:"missing_count"`
	LastEventSeq        int64      `json:"last_event_seq"`
}

// Attempt is one attempt of a task.
type Attempt struct {
	Attempt       int64   `json:"attempt"`
	State         string  `json:"state"`
	FailureReason *string `json:"failure_reason"`
	Retryable     *bool   `json:"retryable"`
	Claims        []Claim `json:"claims"`
}

// History is one accepted or self-published lifecycle event.
type History struct {
	Seq          int64     `json:"history_seq"`
	Kind         string    `json:"kind"`
	Publisher    string    `json:"publisher"`
	PublisherSeq int64     `json:"publisher_seq"`
	OccurredAt   time.Time `json:"occurred_at"`
	Attempt      int64     `json:"attempt"`
	ClaimSeq     *int64    `json:"claim_seq"`
	ExecutorID   *int64    `json:"executor_id"`
	Reason       *string   `json:"reason"`
}

// Task is the current-task aggregate.
type Task struct {
	TeamID      int64  `json:"team_id"`
	TaskID      int64  `json:"task_id"`
	SourceID    int64  `json:"source_id"`
	ExternalID  string `json:"external_id"`
	FlowID      int64  `json:"flow_id"`
	RequiredTag string `json:"required_tag"`
	// TaskName is <team_name>-<task_id>, set once at creation.
	TaskName           string          `json:"task_name"`
	IntakeID           int64           `json:"intake_id"`
	IntakePublisherSeq int64           `json:"intake_publisher_seq"`
	IngestedAt         time.Time       `json:"ingested_at"`
	Payload            json.RawMessage `json:"payload"`
	ProjectID          *string         `json:"project_id"`
	EnvironmentID      *int64          `json:"environment_id"`
	Snapshot           Snapshot        `json:"snapshot"`

	State    string `json:"state"`
	Attempt  int64  `json:"attempt"`
	ClaimSeq int64  `json:"claim_seq"`

	// QueuedImage is the image resolved for the latest publication; the
	// claim that wins it keeps it.
	QueuedImage       string `json:"queued_image"`
	QueuedImageSource string `json:"queued_image_source"`

	NextDueAt   *time.Time `json:"next_due_at"`
	NextDueKind string     `json:"next_due_kind"`

	// PendingControl is the control_id of a cancel sent to the claim holder.
	PendingControl *int64 `json:"pending_control"`

	FinalReason *string    `json:"final_reason"`
	CompletedAt *time.Time `json:"completed_at"`

	Attempts []Attempt `json:"attempts"`
	History  []History `json:"history"`
}

// Clone returns a deep copy.
func (t *Task) Clone() *Task {
	raw, err := json.Marshal(t)
	if err != nil {
		panic(err)
	}
	var c Task
	if err := json.Unmarshal(raw, &c); err != nil {
		panic(err)
	}
	return &c
}

// CurrentAttempt returns the attempt record of the current attempt,
// creating it when missing.
func (t *Task) CurrentAttempt() *Attempt {
	for i := range t.Attempts {
		if t.Attempts[i].Attempt == t.Attempt {
			return &t.Attempts[i]
		}
	}
	t.Attempts = append(t.Attempts, Attempt{Attempt: t.Attempt, State: StateQueued, Claims: []Claim{}})
	return &t.Attempts[len(t.Attempts)-1]
}

// CurrentClaim returns the claim with the task's current (attempt,
// claim_seq) when one exists.
func (t *Task) CurrentClaim() *Claim {
	a := t.CurrentAttempt()
	for i := range a.Claims {
		if a.Claims[i].ClaimSeq == t.ClaimSeq {
			return &a.Claims[i]
		}
	}
	return nil
}

// LatestClaim returns the most recent claim of any attempt.
func (t *Task) LatestClaim() *Claim {
	var latest *Claim
	for i := range t.Attempts {
		for j := range t.Attempts[i].Claims {
			c := &t.Attempts[i].Claims[j]
			if latest == nil || c.Attempt > latest.Attempt || (c.Attempt == latest.Attempt && c.ClaimSeq > latest.ClaimSeq) {
				latest = c
			}
		}
	}
	return latest
}

// Held reports whether the task's current claim is held (created,
// pending or running).
func (t *Task) Held() bool {
	return (t.State == StateCreated || t.State == StatePending || t.State == StateRunning) && t.CurrentClaim() != nil
}

// Terminal reports a terminal state.
func Terminal(state string) bool {
	return state == StateFinished || state == StateFailed || state == StateCancelled
}

// AddHistory appends an entry with the next history_seq.
func (t *Task) AddHistory(h History) {
	h.Seq = int64(len(t.History)) + 1
	t.History = append(t.History, h)
}
