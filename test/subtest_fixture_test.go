package test

import (
	"runtime/debug"
	"testing"
)

// run is t.Run with a panic turned into a failure of that subtest, so one
// panicking subtest (for example a nil task before GREEN) neither aborts the
// test binary nor skips the remaining tests and their cleanups.
func run(t *testing.T, name string, f func(t *testing.T)) bool {
	t.Helper()
	return t.Run(name, func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panic: %v\n%s", r, debug.Stack())
			}
		}()
		f(t)
	})
}
