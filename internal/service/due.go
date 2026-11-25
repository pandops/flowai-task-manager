package service

import (
	"context"

	"github.com/pandops/flowai-task-manager/internal/store"
)

// RunDue republishes every released or retried task whose wait has ended
// and returns how many were republished. The store serializes writers, and
// each task is re-read inside the transaction, so two processes running at
// once republish a task once.
func (s *Service) RunDue(ctx context.Context) (int, error) {
	n := 0
	err := s.writeOrSkip(ctx, func(tx store.Tx) error {
		now := s.now()
		due, err := tx.DueTasks(now, dueBatch)
		if err != nil {
			return err
		}
		for _, d := range due {
			t, err := tx.Task(d.TeamID, d.TaskID)
			if err != nil {
				return err
			}
			if t == nil || t.NextDueAt == nil || t.NextDueAt.After(now) || t.PendingControl != nil {
				continue
			}
			if err := s.publishTask(tx, t); err != nil {
				return err
			}
			if err := tx.SaveTask(t); err != nil {
				return err
			}
			n++
		}
		if n == 0 {
			return errSkip
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return n, nil
}
