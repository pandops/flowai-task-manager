package test

import (
	"github.com/pandops/flowai-task-manager/internal/domain"
	"github.com/pandops/flowai-task-manager/internal/service"
)

func historyKinds(task *domain.Task) []string {
	var out []string
	for _, e := range task.History {
		out = append(out, e.Kind)
	}
	return out
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// archived returns the archive record of a task (nil when not archived).
func (h *harness) archived(team, task int64) *domain.Task {
	rec, _, ok := h.St.Archived(team, task)
	if !ok {
		return nil
	}
	return rec
}

// via runs fn with svc as the task-manager process the harness talks to (a
// second replica or a restarted process on the same store).
func (h *harness) via(svc *service.Service, fn func()) {
	saved := h.Svc
	h.Svc = svc
	defer func() { h.Svc = saved }()
	fn()
}
