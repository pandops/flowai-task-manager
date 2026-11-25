package wire

import (
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/pandops/flowai-task-manager/internal/ids"
)

// AckKind is the JetStream acknowledgement decision for a consumed message.
type AckKind int

// Acknowledgement decisions.
const (
	AckOK   AckKind = iota // +ACK
	AckNak                 // -NAK with Delay
	AckTerm                // +TERM, never redelivered
)

// Ack is the decision of a stream message handler.
type Ack struct {
	Kind  AckKind
	Delay time.Duration
}

// Stream names.
const (
	StreamTask    = "TASK_MANAGER_TASK"
	StreamIntake  = "SOURCE_CONTROLLER_TASK"
	StreamEvent   = "ALL_EVENT"
	IntakeDurable = "task-manager-intake"
	EventDurable  = "task-manager"
)

// TaskSubject is task-manager.task.<team>.<tag>.<flow>.pub.
func TaskSubject(team int64, tag string, flow int64) string {
	return "task-manager.task." + ids.Token(team) + "." + tag + "." + ids.Token(flow) + ".pub"
}

// TaskFilter is the filter of the (team, tag) task consumer.
func TaskFilter(team int64, tag string) string {
	return "task-manager.task." + ids.Token(team) + "." + tag + ".*.sub"
}

// TaskConsumer is tasks-<team_id>-<tag_hash>, tag_hash being the first 16
// lowercase hex characters of SHA-256 of the tag.
func TaskConsumer(team int64, tag string) string {
	sum := sha256.Sum256([]byte(tag))
	return "tasks-" + ids.Token(team) + "-" + hex.EncodeToString(sum[:])[:16]
}

// EventSubject is task-manager.event.<kind>.pub.
func EventSubject(kind string) string { return "task-manager.event." + kind + ".pub" }

// AuditSubject is task-manager.audit.<action>.pub.
func AuditSubject(action string) string { return "task-manager.audit." + action + ".pub" }

// CommandSubject is task-manager.command.<executor>.pub.
func CommandSubject(executor int64) string {
	return "task-manager.command." + ids.Token(executor) + ".pub"
}

// UI operations served on task-manager.ui.<op>.sub.
var UIOperations = []string{"list-tasks", "get-task", "list-attempts", "list-executor-tasks", "list-archive", "get-archived-task", "cancel"}

// RequestSubjects lists the subjects task-manager serves (the .sub side).
func RequestSubjects() []string {
	out := []string{}
	for _, op := range UIOperations {
		out = append(out, "task-manager.ui."+op+".sub")
	}
	return append(out,
		"task-manager.admin.list-tasks.sub",
		"task-manager.service.claim-current.sub",
		"task-manager.service.executor-claims.sub",
		"task-manager.executor.*.claim-token.sub",
	)
}
