package test

import "testing"

// Spec: service-platform — service filters team-scoped reads before pagination.
// "Concurrent team streams" is not applicable to task-manager (journal's
// WebSocket stream); its form here is two teams paging the same filter.
func TestV0048ServicePlatformServiceFiltersTeamScopedReads(t *testing.T) {
	run(t, "Teams share filter values", func(t *testing.T) {
		h := newHarness(t)
		for i := 0; i < 12; i++ {
			h.seedTask(4, 1, "F"+tok(int64(i)))
		}
		mine := h.seedTask(3, 1, "MINE")
		r := h.ui("list-tasks", 3, 1, map[string]any{"status": "queued", "search": "", "limit": 10})
		if !sameIDs(itemIDs(r), []int64{mine}) || r.Body["next_cursor"] != nil {
			t.Fatalf("page = %v next %v", itemIDs(r), r.Body["next_cursor"])
		}
	})

	run(t, "Concurrent team streams", func(t *testing.T) {
		h := newHarness(t)
		a := h.seedTask(3, 1, "A")
		b := h.seedTask(4, 1, "B")
		ra := h.ui("list-tasks", 3, 1, nil)
		rb := h.ui("list-tasks", 4, 1, nil)
		if !sameIDs(itemIDs(ra), []int64{a}) || !sameIDs(itemIDs(rb), []int64{b}) || num(obj(list(rb.Body["items"])[0])["team_id"]) != 4 {
			t.Fatalf("pages %v %v", ra.Body, rb.Body)
		}
	})
}
