package test

import (
	"sort"
	"testing"
	"time"
)

// Spec: task-manager — task-manager serves the global task projection to admin-panel.
func TestV0048TaskManagerServesTheGlobalTaskProjection(t *testing.T) {
	run(t, "Administrator lists tasks of every team", func(t *testing.T) {
		h := newHarness(t)
		done := h.runningTask(3, 7, "DONE")
		h.finished(3, 7, done, 1, 1)
		h.advance(time.Second)
		h.seedTask(3, 1, "Q3")
		h.seedTask(4, 1, "Q4") // same ingested_at as Q3: team_id DESC breaks the tie
		h.flush()
		before := len(h.Bus.Messages())
		r := h.admin(map[string]any{}, nil)
		h.flush()
		items := list(r.Body["items"])
		if r.Status != 200 || len(items) != 3 || r.Body["next"] != nil {
			t.Fatalf("admin = %d %v", r.Status, r.Body)
		}
		want := []string{"task_id", "team_id", "flow_id", "source_id", "external_id", "required_tag", "current_state", "owner_command_id", "executor_id", "ingested_at", "claimed_at"}
		for _, it := range items {
			var keys []string
			for k := range obj(it) {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			w := append([]string(nil), want...)
			sort.Strings(w)
			if !sameStrings(keys, w) || obj(it)["owner_command_id"] != nil {
				t.Fatalf("entry keys = %v", keys)
			}
		}
		if num(obj(items[0])["team_id"]) != 4 || num(obj(items[1])["team_id"]) != 3 || str(obj(items[2])["current_state"]) != "finished" || num(obj(items[2])["executor_id"]) != 7 {
			t.Fatalf("order = %v", items)
		}
		msgs := h.Bus.Messages()[before:]
		if len(msgs) != 1 || msgs[0].Subject != "task-manager.audit.admin-list-tasks.pub" || msgs[0].Header["team"] != "admin" {
			t.Fatalf("read published %v, want exactly one audit message", msgs)
		}
		a := decode(msgs[0].Data)
		if str(a["actor_subject"]) != "admin-alice" || str(a["actor_type"]) != "administrator" || str(a["outcome"]) != "succeeded" {
			t.Fatalf("audit = %v", a)
		}
		if len(h.events("archived")) != 1 {
			t.Fatalf("read changed tasks")
		}
	})

	run(t, "Filters narrow the projection", func(t *testing.T) {
		h := newHarness(t)
		want := h.seedTask(3, 1, "A")
		h.seedTask(3, 1, "B", flow(5))
		gpu := flow(2)
		gpu.Tag = "gpu"
		h.bind(3, 2, gpu)
		h.intake(3, 2, entry(70, "C"))
		h.runningTask(3, 7, "RUN")
		h.seedTask(4, 1, "D")
		r := h.admin(map[string]any{"team_id": 3, "state": "queued", "tag": "openhands", "flow_id": 2}, nil)
		if r.Status != 200 || !sameIDs(itemIDs(r), []int64{want}) {
			t.Fatalf("filtered = %d %v (want %d)", r.Status, itemIDs(r), want)
		}
	})

	run(t, "Paging uses the continuation position", func(t *testing.T) {
		h := newHarness(t)
		for i := 0; i < 3; i++ {
			h.advance(time.Second)
			h.seedTask(3, 1, "P"+tok(int64(i)))
		}
		first := h.admin(map[string]any{"limit": 2}, nil)
		next := obj(first.Body["next"])
		if len(itemIDs(first)) != 2 || next == nil {
			t.Fatalf("first = %v", first.Body)
		}
		second := h.admin(map[string]any{"limit": 2}, map[string]any{"after": next})
		if len(itemIDs(second)) != 1 || second.Body["next"] != nil {
			t.Fatalf("second = %v", second.Body)
		}
		if bad := h.admin(map[string]any{}, map[string]any{"after": map[string]any{"team_id": "x"}}); bad.Status != 400 || str(bad.Body["code"]) != "invalid_pagination" {
			t.Fatalf("bad position = %d %v", bad.Status, bad.Body)
		}
	})

	run(t, "Flow filter without a team", func(t *testing.T) {
		h := newHarness(t)
		h.seedTask(3, 1, "A")
		reads := h.Store.Reads
		for _, p := range []map[string]any{{"flow_id": 2}, {"team_id": 0}, {"team_id": "3"}, {"team_id": 3, "flow_id": -1}} {
			if r := h.admin(p, nil); r.Status != 400 || str(r.Body["code"]) != "invalid_request" {
				t.Fatalf("%v -> %d %v", p, r.Status, r.Body)
			}
		}
		if h.Store.Reads != reads {
			t.Fatalf("rejected request read rows")
		}
	})

	run(t, "Limit above the maximum", func(t *testing.T) {
		h := newHarness(t)
		h.seedTask(3, 1, "A")
		reads := h.Store.Reads
		for _, l := range []any{201, 0, "x"} {
			if r := h.admin(map[string]any{"limit": l}, nil); r.Status != 400 || str(r.Body["code"]) != "invalid_pagination" {
				t.Fatalf("limit %v -> %d %v", l, r.Status, r.Body)
			}
		}
		if h.Store.Reads != reads {
			t.Fatalf("rejected limit read rows")
		}
		if r := h.admin(map[string]any{"limit": 200}, nil); r.Status != 200 {
			t.Fatalf("limit 200 rejected")
		}
	})
}
