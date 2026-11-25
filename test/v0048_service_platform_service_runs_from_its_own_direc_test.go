package test

import (
	"strings"
	"testing"
)

// Spec: service-platform — service runs from its own directory.
// "E2E uses the deployed services" is not applicable to task-manager (E2E suite).
func TestV0048ServicePlatformServiceRunsFromItsOwnDirectory(t *testing.T) {
	run(t, "Change to one service selects only its tests", func(t *testing.T) {
		for p, files := range serviceImports(t) {
			if p == "github.com/flowai/platform" || strings.HasPrefix(p, "github.com/flowai/platform/") ||
				strings.HasPrefix(p, "github.com/pandops/flowai-") && !strings.HasPrefix(p, "github.com/pandops/flowai-task-manager/") {
				t.Fatalf("%v import %s: another service's change would select task-manager's tests", files, p)
			}
		}
	})
}
