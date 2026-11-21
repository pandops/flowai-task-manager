package test

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// Spec: task-manager — task-manager freezes the Flow execution snapshot into each task.
func TestV0048TaskManagerFreezesTheFlowExecutionSnapshot(t *testing.T) {
	run(t, "External ID is rendered into the prompt", func(t *testing.T) {
		h := newHarness(t)
		f := flow(2)
		f.Params = map[string]any{"max_iterations": float64(30), "workspace": "repo-main", "verbose": true}
		task := h.seedTask(3, 1, "ISSUE-42", f)
		msgs := h.taskMsgs()
		if len(msgs) != 1 {
			t.Fatalf("queue messages = %d", len(msgs))
		}
		exec := obj(msgs[0]["execution"])
		if str(exec["rendered_prompt"]) != "Fix ISSUE-42 now" || exec["execution_parameters"] == nil {
			t.Fatalf("execution = %v", exec)
		}
		// The snapshot carries exactly the revision's parameters: no key
		// dropped, added or changed, in the queue message and in the store.
		if got := obj(exec["execution_parameters"]); !sameJSON(got, f.Params) {
			t.Fatalf("queue execution_parameters = %v, want %v", got, f.Params)
		}
		var stored map[string]any
		if err := json.Unmarshal(h.current(3, task).Snapshot.ExecutionParameters, &stored); err != nil || !reflect.DeepEqual(stored, f.Params) {
			t.Fatalf("stored execution_parameters = %s (%v), want %v", h.current(3, task).Snapshot.ExecutionParameters, err, f.Params)
		}
		raw, _ := json.Marshal(msgs[0])
		for _, k := range []string{"agent", "model", "secret", "api_key", "litellm"} {
			if strings.Contains(string(raw), "\""+k) {
				t.Fatalf("snapshot carries %q: %s", k, raw)
			}
		}
		lit := flow(2)
		lit.Prompt = "No placeholder {here} {{{{literal}}}}"
		h.bind(3, 9, lit)
		h.intake(3, 9, entry(5, "ISSUE-7"))
		h.flush()
		for _, v := range h.currentOf(3) {
			if v.SourceID == 9 && v.Snapshot.RenderedPrompt != "No placeholder {here} {{literal}}" {
				t.Fatalf("template without placeholder changed: %q", v.Snapshot.RenderedPrompt)
			}
		}
		listed := h.ui("list-tasks", 3, 1, nil)
		if strings.Contains(string(listed.Raw), "Fix ISSUE-42") || strings.Contains(string(listed.Raw), "execution_parameters") ||
			strings.Contains(string(listed.Raw), "repo-main") || strings.Contains(string(listed.Raw), "max_iterations") {
			t.Fatalf("list exposes the prompt or parameters: %s", listed.Raw)
		}
	})

	run(t, "Flow changes after task creation", func(t *testing.T) {
		h := newHarness(t)
		orig := flow(2)
		orig.Params = map[string]any{"max_iterations": float64(30)}
		task := h.seedTask(3, 1, "ISSUE-1", orig)
		changed := flow(2)
		changed.Revision = 99
		changed.Prompt = "New {{external_id}}"
		changed.Params = map[string]any{"max_iterations": float64(5), "workspace": "other"}
		changed.RetryCount = 0
		changed.ReleaseDelay = 900
		h.bind(3, 1, changed)
		later := h.seedTask(3, 1, "ISSUE-2", changed)
		old := h.current(3, task).Snapshot
		if old.ExecutionRevision != 12 || old.RenderedPrompt != "Fix ISSUE-1 now" || old.FailureRetryCount != 3 || old.ReleaseDelaySeconds != 300 {
			t.Fatalf("existing snapshot changed: %+v", old)
		}
		var oldParams map[string]any
		if err := json.Unmarshal(old.ExecutionParameters, &oldParams); err != nil || !reflect.DeepEqual(oldParams, orig.Params) {
			t.Fatalf("existing snapshot parameters changed: %s", old.ExecutionParameters)
		}
		s := h.current(3, later).Snapshot
		if s.ExecutionRevision != 99 || s.FailureRetryCount != 0 || s.RenderedPrompt != "New ISSUE-2" {
			t.Fatalf("later task did not use the new revision: %+v", s)
		}
		var newParams map[string]any
		if err := json.Unmarshal(s.ExecutionParameters, &newParams); err != nil || !reflect.DeepEqual(newParams, changed.Params) {
			t.Fatalf("later task parameters = %s, want %v", s.ExecutionParameters, changed.Params)
		}
	})

	run(t, "Retry policy defaults", func(t *testing.T) {
		h := newHarness(t)
		f := flow(2)
		f.OmitRetry = true
		task := h.seedTask(3, 1, "ISSUE-1", f)
		if got := h.current(3, task).Snapshot.FailureRetryCount; got != 3 {
			t.Fatalf("default retry count = %d", got)
		}
	})
}

// sameJSON reports whether a and b encode to the same JSON value, so a
// json.Number from the harness decoder equals a float64 fixture value.
func sameJSON(a, b any) bool {
	norm := func(v any) any {
		raw, err := json.Marshal(v)
		if err != nil {
			return err
		}
		var out any
		if err := json.Unmarshal(raw, &out); err != nil {
			return err
		}
		return out
	}
	return reflect.DeepEqual(norm(a), norm(b))
}
