package test

import (
	"sort"
	"strings"
	"testing"
	"time"
)

// Re-homed and transport-replaced State Registry cases (verification/
// test-inventory.md, task-manager rows). Each subtest keeps the old test's
// name; the oracle is the same, read through task-manager's broker requests.
func TestV0048TaskManagerRehomedStateRegistryCases(t *testing.T) {
	summaryKeys := []string{"task_id", "task_name", "team_id", "flow_id", "source_id", "external_id", "required_tag", "current_state", "attempt", "executor_id", "ingested_at", "claimed_at", "completed_at"}

	run(t, "TestTaskListEntryOpenAPISchema", func(t *testing.T) {
		h := newHarness(t)
		h.seedTask(3, 1, "A")
		r := h.ui("list-tasks", 3, 1, nil)
		if err := validateAgainst(t, "svc/task-manager/schemas/ui.list-tasks.reply.schema.json", r.Raw); err != nil {
			t.Fatalf("list entry: %v", err)
		}
	})

	run(t, "TestTaskListEntryNullablesRendersNull", func(t *testing.T) {
		h := newHarness(t)
		h.seedTask(3, 1, "A")
		it := obj(list(h.ui("list-tasks", 3, 1, nil).Body["items"])[0])
		for _, k := range []string{"executor_id", "claimed_at", "completed_at"} {
			v, ok := it[k]
			if !ok || v != nil {
				t.Fatalf("%s = %v (present %v), want explicit null", k, v, ok)
			}
		}
		if str(it["current_state"]) != "queued" {
			t.Fatalf("state %v", it["current_state"])
		}
	})

	run(t, "TestGatewayTaskFilterDoesNotExposeFlowID", func(t *testing.T) {
		// Adapted: list entries expose no snapshot data (prompt, parameters, image).
		h := newHarness(t)
		h.seedTask(3, 1, "A")
		raw := string(h.ui("list-tasks", 3, 1, nil).Raw)
		for _, k := range []string{"rendered_prompt", "execution_parameters", "resolved_image", "payload"} {
			if strings.Contains(raw, k) {
				t.Fatalf("list exposes %s", k)
			}
		}
	})

	run(t, "TestIngestSetsIngestedAt", func(t *testing.T) {
		h := newHarness(t)
		at := t0.Add(-3 * time.Hour)
		h.at(at)
		task := h.seedTask(3, 1, "A")
		if got := str(h.getTask(3, task).Body["ingested_at"]); got != ts(at) {
			t.Fatalf("ingested_at = %s, want %s", got, ts(at))
		}
	})

	run(t, "TestCrossTeamSourceIdIndependenceViaSeparateSourceSystems", func(t *testing.T) {
		h := newHarness(t)
		a := h.seedTask(3, 1, "ISSUE-1")
		b := h.seedTask(4, 1, "ISSUE-1")
		if h.current(3, a) == nil || h.current(4, b) == nil {
			t.Fatalf("source 1 of team 4 deduplicated against team 3")
		}
	})

	run(t, "TestListTasksOrdering", func(t *testing.T) {
		h := newHarness(t)
		var want []int64
		for i := 0; i < 4; i++ {
			h.advance(time.Minute)
			want = append([]int64{h.seedTask(3, 1, "O"+tok(int64(i)))}, want...)
		}
		if got := itemIDs(h.ui("list-tasks", 3, 1, nil)); !sameIDs(got, want) {
			t.Fatalf("order %v want %v", got, want)
		}
	})

	run(t, "TestListTasksClaimedTimestamp", func(t *testing.T) {
		h := newHarness(t)
		task := h.seedTask(3, 1, "A")
		h.ensureExecutor(3, 7)
		h.advance(42 * time.Second)
		h.created(3, 7, task, 1, 1)
		it := obj(list(h.ui("list-tasks", 3, 1, nil).Body["items"])[0])
		if str(it["claimed_at"]) != ts(h.Now) || num(it["executor_id"]) != 7 || str(it["current_state"]) != "created" {
			t.Fatalf("entry = %v", it)
		}
	})

	run(t, "TestListTasksPagination", func(t *testing.T) {
		h := newHarness(t)
		for i := 0; i < 26; i++ {
			h.advance(time.Second)
			h.seedTask(3, 1, "P"+tok(int64(i)))
		}
		seen := map[int64]bool{}
		cur := ""
		for page := 0; page < 3; page++ {
			p := map[string]any{"limit": 10}
			if cur != "" {
				p["cursor"] = cur
			}
			r := h.ui("list-tasks", 3, 1, p)
			for _, id := range itemIDs(r) {
				if seen[id] {
					t.Fatalf("task %d repeated", id)
				}
				seen[id] = true
			}
			cur = str(r.Body["next_cursor"])
		}
		if len(seen) != 26 || cur != "" {
			t.Fatalf("pages covered %d tasks, final cursor %q", len(seen), cur)
		}
	})

	run(t, "TestListTasksContinuationBoundaries", func(t *testing.T) {
		h := newHarness(t)
		for i := 0; i < 12; i++ { // equal ingested_at: task_id DESC breaks ties across pages
			h.seedTask(3, 1, "B"+tok(int64(i)))
		}
		p1 := h.ui("list-tasks", 3, 1, nil)
		p2 := h.ui("list-tasks", 3, 1, map[string]any{"cursor": str(p1.Body["next_cursor"])})
		all := append(itemIDs(p1), itemIDs(p2)...)
		if len(all) != 12 || !sort.SliceIsSorted(all, func(i, j int) bool { return all[i] > all[j] }) {
			t.Fatalf("boundary pages = %v", all)
		}

		// The old oracle: limits 2/3/4 over 3 rows; a cursor only when the
		// limit is below the row count (the administrator projection takes
		// any limit up to 200).
		three := newHarness(t)
		for i := 0; i < 3; i++ {
			three.seedTask(3, 1, "C"+tok(int64(i)))
		}
		for _, c := range []struct {
			limit, count int
			cursor       bool
		}{{2, 2, true}, {3, 3, false}, {4, 3, false}} {
			expectPage(t, three.admin(map[string]any{"limit": c.limit}, nil), "next", "admin limit "+tok(int64(c.limit))+" over 3", c.count, c.cursor)
		}
		// The team list's own limits: 10 over 9, 10 and 11 rows.
		for _, c := range []struct {
			rows   int
			cursor bool
		}{{9, false}, {10, false}, {11, true}} {
			hh := newHarness(t)
			for i := 0; i < c.rows; i++ {
				hh.seedTask(3, 1, "R"+tok(int64(i)))
			}
			want := min(c.rows, 10)
			expectPage(t, hh.ui("list-tasks", 3, 1, map[string]any{"limit": 10}), "next_cursor", "limit 10 over "+tok(int64(c.rows)), want, c.cursor)
		}
	})

	run(t, "TestListTasksTeamFilter", func(t *testing.T) {
		h := newHarness(t)
		h.seedTask(3, 1, "A")
		h.advance(time.Second)
		b1 := h.seedTask(4, 1, "B1")
		h.advance(time.Second)
		h.seedTask(3, 1, "A2")
		h.advance(time.Second)
		b2 := h.seedTask(4, 1, "B2")
		h.advance(time.Second)
		h.seedTask(3, 1, "A3")
		items := list(h.ui("list-tasks", 4, 1, nil).Body["items"])
		for _, it := range items {
			if num(obj(it)["team_id"]) != 4 {
				t.Fatalf("foreign entry")
			}
		}
		// The positive half of the filter: exactly the own team's tasks, newest
		// first, while the foreign team's rows are present and newer.
		var got []string
		for _, it := range items {
			got = append(got, tok(num(obj(it)["task_id"]))+"/"+str(obj(it)["external_id"]))
		}
		if want := []string{tok(b2) + "/B2", tok(b1) + "/B1"}; !sameStrings(got, want) {
			t.Fatalf("team 4 list = %v, want %v", got, want)
		}
		// The admin team_id filter (the old test's surface) returns the same
		// exact own-team result.
		var adminGot []string
		for _, it := range list(h.admin(map[string]any{"team_id": 4}, nil).Body["items"]) {
			adminGot = append(adminGot, tok(num(obj(it)["team_id"]))+"/"+tok(num(obj(it)["task_id"]))+"/"+str(obj(it)["external_id"]))
		}
		if want := []string{"4/" + tok(b2) + "/B2", "4/" + tok(b1) + "/B1"}; !sameStrings(adminGot, want) {
			t.Fatalf("admin team_id=4 list = %v, want %v", adminGot, want)
		}
	})

	run(t, "TestListTasksFullCanonicalProjection", func(t *testing.T) {
		h := newHarness(t)
		h.runningTask(3, 7, "A")
		it := obj(list(h.ui("list-tasks", 3, 1, nil).Body["items"])[0])
		var keys []string
		for k := range it {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		want := append([]string(nil), summaryKeys...)
		sort.Strings(want)
		if !sameStrings(keys, want) {
			t.Fatalf("keys %v want %v", keys, want)
		}
	})

	run(t, "TestTrustedGatewayTaskPointRead", func(t *testing.T) {
		h := newHarness(t)
		task := h.seedTask(3, 1, "A")
		if r := h.getTask(3, task); r.Status != 200 || num(r.Body["task_id"]) != task {
			t.Fatalf("point read = %d", r.Status)
		}
	})

	run(t, "TestUITaskCursorFreezesCutoffAndRejectsChangedContextBeforeRead", func(t *testing.T) {
		h := newHarness(t)
		for i := 0; i < 11; i++ {
			h.advance(time.Second)
			h.seedTask(3, 1, "C"+tok(int64(i)))
		}
		cur := str(h.ui("list-tasks", 3, 1, map[string]any{"period": "week"}).Body["next_cursor"])
		reads := h.Store.Reads
		if r := h.ui("list-tasks", 3, 1, map[string]any{"period": "month", "cursor": cur}); r.Status != 400 || str(r.Body["code"]) != "invalid_cursor" || h.Store.Reads != reads {
			t.Fatalf("changed period -> %d", r.Status)
		}
	})

	run(t, "TestV0002_44_TeamNameIsDisplayOnly", func(t *testing.T) {
		h := newHarness(t)
		task := h.seedTask(3, 1, "A")
		hdr := h.uiHeader(3, 1)
		delete(hdr, "team_name")
		if r := h.request("task-manager.ui.get-task.sub", hdr, map[string]any{"params": map[string]any{"task_id": task}}, ""); r.Status != 200 {
			t.Fatalf("missing display-only team_name blocked the read: %d", r.Status)
		}
	})

	run(t, "TestForeignResourceNotFound", func(t *testing.T) {
		h := newHarness(t)
		task := h.seedTask(4, 1, "A")
		h.ensureTeam(3)
		if r := h.cancel(3, 1, task); r.Status != 404 || str(r.Body["code"]) != "not_found" {
			t.Fatalf("foreign = %d", r.Status)
		}
	})

	cursorCases := func(t *testing.T) (*harness, string) {
		h := newHarness(t)
		for i := 0; i < 11; i++ {
			h.advance(time.Second)
			h.seedTask(3, 1, "K"+tok(int64(i)))
		}
		return h, str(h.ui("list-tasks", 3, 1, map[string]any{"status": "queued"}).Body["next_cursor"])
	}
	expect400 := func(t *testing.T, h *harness, code string, r reply) {
		t.Helper()
		if r.Status != 400 || str(r.Body["code"]) != code {
			t.Fatalf("got %d %v, want 400 %s", r.Status, r.Body, code)
		}
	}

	run(t, "TestCursorIntegrityRejectedBeforeQuery", func(t *testing.T) {
		h, cur := cursorCases(t)
		reads := h.Store.Reads
		expect400(t, h, "invalid_cursor", h.ui("list-tasks", 3, 1, map[string]any{"status": "queued", "cursor": cur[:len(cur)-2]}))
		if h.Store.Reads != reads {
			t.Fatalf("read before rejection")
		}
	})
	run(t, "TestCursorCrossEndpointRejected", func(t *testing.T) {
		h, cur := cursorCases(t)
		expect400(t, h, "invalid_cursor", h.ui("list-archive", 3, 1, map[string]any{"status": "queued", "cursor": cur}))
	})
	run(t, "TestCursorCrossScopeRejected", func(t *testing.T) {
		h, cur := cursorCases(t)
		expect400(t, h, "invalid_cursor", h.ui("list-tasks", 4, 1, map[string]any{"status": "queued", "cursor": cur}))
	})
	run(t, "TestCursorChangedFilterRejected", func(t *testing.T) {
		h, cur := cursorCases(t)
		expect400(t, h, "invalid_cursor", h.ui("list-tasks", 3, 1, map[string]any{"cursor": cur}))
	})
	run(t, "TestCursorChangedOrderingRejected", func(t *testing.T) {
		h, cur := cursorCases(t)
		expect400(t, h, "invalid_request", h.ui("list-tasks", 3, 1, map[string]any{"status": "queued", "cursor": cur, "order": "asc"}))
	})
	run(t, "TestCursorWrongDirectionRejected", func(t *testing.T) {
		h, cur := cursorCases(t)
		expect400(t, h, "invalid_request", h.ui("list-tasks", 3, 1, map[string]any{"status": "queued", "cursor": cur, "direction": "prev"}))
	})
	run(t, "TestLimitOutOfRangeRejectedBeforeQuery", func(t *testing.T) {
		h, _ := cursorCases(t)
		reads := h.Store.Reads
		expect400(t, h, "invalid_pagination", h.ui("list-tasks", 3, 1, map[string]any{"limit": 500}))
		if h.Store.Reads != reads {
			t.Fatalf("read before rejection")
		}
	})
	run(t, "TestLimitMalformedRejectedBeforeQuery", func(t *testing.T) {
		h, _ := cursorCases(t)
		expect400(t, h, "invalid_pagination", h.ui("list-tasks", 3, 1, map[string]any{"limit": "ten"}))
	})
	run(t, "TestGatewayListTasksOrderingIngestedAtDesc", func(t *testing.T) {
		h, _ := cursorCases(t)
		ids := itemIDs(h.ui("list-tasks", 3, 1, nil))
		if !sort.SliceIsSorted(ids, func(i, j int) bool { return ids[i] > ids[j] }) {
			t.Fatalf("order %v", ids)
		}
	})
	run(t, "TestAdminTasksOrderingDeterministic", func(t *testing.T) {
		h := newHarness(t)
		h.seedTask(3, 1, "A")
		h.seedTask(4, 1, "B")
		h.seedTask(3, 1, "C")
		a, b := itemIDs(h.admin(map[string]any{}, nil)), itemIDs(h.admin(map[string]any{}, nil))
		if !sameIDs(a, b) || len(a) != 3 {
			t.Fatalf("admin order not deterministic: %v %v", a, b)
		}
	})
	run(t, "TestAdminTasksResponseExactKeys", func(t *testing.T) {
		h := newHarness(t)
		h.seedTask(3, 1, "A")
		it := obj(list(h.admin(map[string]any{}, nil).Body["items"])[0])
		if len(it) != 11 {
			t.Fatalf("admin entry has %d keys: %v", len(it), it)
		}
	})
	run(t, "TestGatewayTasksRejectsFlowIDQuery", func(t *testing.T) {
		// Adapted: the delta spec makes flow_id a Gateway list filter; it is
		// accepted, team-scoped and rejected only when not a positive integer.
		h := newHarness(t)
		a := h.seedTask(3, 1, "A")
		if r := h.ui("list-tasks", 3, 1, map[string]any{"flow_id": 2}); !sameIDs(itemIDs(r), []int64{a}) {
			t.Fatalf("flow filter = %v", itemIDs(r))
		}
		expect400(t, h, "invalid_request", h.ui("list-tasks", 3, 1, map[string]any{"flow_id": "2"}))
	})
	run(t, "TestListEndpointsRejectDuplicateFiltersBeforeQuery", func(t *testing.T) {
		h, _ := cursorCases(t)
		reads := h.Store.Reads
		expect400(t, h, "invalid_request", h.ui("list-tasks", 3, 1, map[string]any{"status": []string{"queued", "running"}}))
		expect400(t, h, "invalid_request", h.ui("list-tasks", 3, 1, map[string]any{"search": []string{"a", "b"}}))
		if h.Store.Reads != reads {
			t.Fatalf("read before rejection")
		}
	})
	run(t, "TestListEndpointsRejectOversizedInputBeforeQuery", func(t *testing.T) {
		h, _ := cursorCases(t)
		reads := h.Store.Reads
		expect400(t, h, "invalid_request", h.ui("list-tasks", 3, 1, map[string]any{"search": strings.Repeat("x", 257)}))
		expect400(t, h, "invalid_cursor", h.ui("list-tasks", 3, 1, map[string]any{"cursor": strings.Repeat("x", 5000)}))
		if h.Store.Reads != reads {
			t.Fatalf("read before rejection")
		}
	})
	run(t, "TestAdminTasksFlowIDFilterStillWorks", func(t *testing.T) {
		h := newHarness(t)
		a := h.seedTask(3, 1, "A")
		h.seedTask(3, 1, "B", flow(5))
		if r := h.admin(map[string]any{"team_id": 3, "flow_id": 2}, nil); !sameIDs(itemIDs(r), []int64{a}) {
			t.Fatalf("admin flow filter = %v", itemIDs(r))
		}
	})
}

// expectPage checks a list reply's item count and whether its
// continuation field (next_cursor for team lists, next for the
// administrator projection) is present and set, or an explicit null.
func expectPage(t *testing.T, r reply, field, what string, count int, cursor bool) {
	t.Helper()
	next, present := r.Body[field]
	if r.Status != 200 || len(list(r.Body["items"])) != count || !present {
		t.Fatalf("%s: status %d, %d items (want %d), %s present %v: %s", what, r.Status, len(list(r.Body["items"])), count, field, present, r.Raw)
	}
	if set := next != nil && (str(next) != "" || obj(next) != nil); set != cursor {
		t.Fatalf("%s: %s %v, want a continuation %v", what, field, next, cursor)
	}
}
