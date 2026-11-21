package test

import (
	"bytes"
	"testing"
	"time"

	"github.com/pandops/flowai-task-manager/internal/cursor"
	"github.com/pandops/flowai-task-manager/internal/store"
)

// Spec: service-platform — service binds opaque cursors to their scope.
func TestV0048ServicePlatformServiceBindsOpaqueCursors(t *testing.T) {
	seed := func(t *testing.T) *harness {
		h := newHarness(t)
		for i := 0; i < 12; i++ {
			h.advance(time.Second)
			h.seedTask(3, 1, "C"+tok(int64(i)))
		}
		return h
	}

	run(t, "Forward page advances", func(t *testing.T) {
		h := seed(t)
		p1 := h.ui("list-tasks", 3, 1, nil)
		p2 := h.ui("list-tasks", 3, 1, map[string]any{"cursor": str(p1.Body["next_cursor"])})
		ids := append(itemIDs(p1), itemIDs(p2)...)
		if len(itemIDs(p1)) != 10 || len(itemIDs(p2)) != 2 || p2.Body["next_cursor"] != nil {
			t.Fatalf("pages %v %v", itemIDs(p1), itemIDs(p2))
		}
		for i := 1; i < len(ids); i++ {
			if ids[i] >= ids[i-1] {
				t.Fatalf("order not (ingested_at, task_id) DESC: %v", ids)
			}
		}
	})

	run(t, "Tampered cursor", func(t *testing.T) {
		h := seed(t)
		cur := str(h.ui("list-tasks", 3, 1, nil).Body["next_cursor"])
		reads := h.Store.Reads
		b := []byte(cur)
		b[0] ^= 2
		other, _ := cursor.New([]cursor.Key{{ID: 2, Key: bytes.Repeat([]byte{9}, 32)}})
		foreignKey := ""
		if other != nil {
			foreignKey = other.Encode(cursor.Cursor{Scope: "x", Pos: store.Position{IngestedAt: t0, TeamID: 3, TaskID: 1}})
		}
		for _, bad := range []string{string(b), cur + "A", "not-a-cursor", foreignKey} {
			if r := h.ui("list-tasks", 3, 1, map[string]any{"cursor": bad}); r.Status != 400 || str(r.Body["code"]) != "invalid_cursor" {
				t.Fatalf("cursor %q -> %d %v", bad, r.Status, r.Body)
			}
		}
		if h.Store.Reads != reads {
			t.Fatalf("rejected cursors read rows")
		}
	})

	run(t, "Cursor reused for another operation or team", func(t *testing.T) {
		h := seed(t)
		cur := str(h.ui("list-tasks", 3, 1, nil).Body["next_cursor"])
		for _, r := range []reply{
			h.ui("list-tasks", 4, 1, map[string]any{"cursor": cur}),
			h.ui("list-archive", 3, 1, map[string]any{"cursor": cur}),
			h.ui("list-executor-tasks", 3, 1, map[string]any{"cursor": cur, "executor_id": 7}),
		} {
			if r.Status != 400 || str(r.Body["code"]) != "invalid_cursor" {
				t.Fatalf("reuse -> %d %v", r.Status, r.Body)
			}
		}
	})

	run(t, "Changed filter", func(t *testing.T) {
		h := seed(t)
		cur := str(h.ui("list-tasks", 3, 1, map[string]any{"status": "queued"}).Body["next_cursor"])
		if r := h.ui("list-tasks", 3, 1, map[string]any{"status": "running", "cursor": cur}); r.Status != 400 || str(r.Body["code"]) != "invalid_cursor" {
			t.Fatalf("changed filter -> %d", r.Status)
		}
		if r := h.ui("list-tasks", 3, 1, map[string]any{"status": "queued", "cursor": cur, "limit": 25}); r.Status != 400 || str(r.Body["code"]) != "invalid_cursor" {
			t.Fatalf("changed limit -> %d", r.Status)
		}
	})

	run(t, "Limit out of range", func(t *testing.T) {
		h := seed(t)
		if r := h.ui("list-archive", 3, 1, map[string]any{"limit": 1000}); r.Status != 400 || str(r.Body["code"]) != "invalid_pagination" {
			t.Fatalf("limit 1000 -> %d", r.Status)
		}
	})
}
