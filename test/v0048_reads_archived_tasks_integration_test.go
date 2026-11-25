//go:build integration

package test

import "testing"

// Bugfix 6 on PostgreSQL: the same cases as TestV0048ReadsArchivedTasks
// against the union of tasks and task_archive in SQL.
func TestV0048ReadsArchivedTasksIntegration(t *testing.T) {
	archivedReadCases(t, func(t *testing.T) *harness {
		env := newPGEnv(t, true)
		return newHarnessOn(t, openPG(t, env))
	})
}
