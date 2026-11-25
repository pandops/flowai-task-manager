package test

import (
	"regexp"
	"strings"
	"testing"

	"github.com/pandops/flowai-task-manager/internal/wire"
)

// Spec: message-broker — names subjects from immutable identifiers.
func TestV0048MessageBrokerNamesSubjectsFromImmutableIdentifiers(t *testing.T) {
	run(t, "Team appears as a subject token only in the task queue and Source intake", func(t *testing.T) {
		h := newHarness(t)
		task := h.runningTask(3, 7, "A")
		h.cancel(3, 11, task)
		h.failed(3, 7, task, 1, 1, "x", false)
		h.admin(map[string]any{}, nil)
		h.flush()
		taskSubject := regexp.MustCompile(`^task-manager\.task\.3\.openhands\.2\.pub$`)
		for _, m := range h.Bus.Messages() {
			switch {
			case strings.HasPrefix(m.Subject, "task-manager.task."):
				if !taskSubject.MatchString(m.Subject) {
					t.Fatalf("task subject %s", m.Subject)
				}
			case strings.HasPrefix(m.Subject, "task-manager.command."):
				if m.Subject != "task-manager.command.7.pub" {
					t.Fatalf("command subject %s", m.Subject)
				}
			default:
				if regexp.MustCompile(`\.\d+\.`).MatchString(m.Subject) {
					t.Fatalf("subject %s carries an identifier token", m.Subject)
				}
			}
		}
		for _, s := range wire.RequestSubjects() {
			if strings.Contains(s, ".3.") || strings.Contains(s, "team-") {
				t.Fatalf("served subject %s names a team", s)
			}
		}
		if wire.TaskSubject(3, "openhands", 2) != "task-manager.task.3.openhands.2.pub" || wire.CommandSubject(7) != "task-manager.command.7.pub" ||
			wire.EventSubject("queued") != "task-manager.event.queued.pub" || wire.AuditSubject("cancel") != "task-manager.audit.cancel.pub" {
			t.Fatalf("subject builders")
		}
	})
}
