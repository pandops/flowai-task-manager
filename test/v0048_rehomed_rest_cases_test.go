package test

// Survivors of deleted State Registry cases that had no task-manager test
// (ledger docs/testing/v0048-test-decisions.csv, rows named per subtest).

import (
	"encoding/base64"
	"encoding/json"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/pandops/flowai-task-manager/internal/claimtoken"
	"github.com/pandops/flowai-task-manager/internal/cursor"
	"github.com/pandops/flowai-task-manager/internal/wire"
)

func expectCode(t *testing.T, r reply, status int, code string) {
	t.Helper()
	if r.Status != status || str(r.Body["code"]) != code {
		t.Fatalf("got %d %v, want %d %s", r.Status, r.Body, status, code)
	}
}

func TestV0048TaskManagerRehomedRestCases(t *testing.T) {
	// M122 (TestValidUIDate): the date filter accepts a real leap day and
	// rejects a non-leap 29 February, unpadded dates and timestamp forms
	// before any task row is read.
	run(t, "M122 strict civil date forms", func(t *testing.T) {
		h := newHarness(t)
		ids := datedTasks(t, h, "2024-02-28T23:59:59Z", "2024-02-29T00:00:00Z", "2024-02-29T23:59:59Z", "2024-03-01T00:00:00Z")
		if r := h.ui("list-tasks", 3, 1, map[string]any{"date": "2024-02-29"}); r.Status != 200 || !sameIDs(itemIDs(r), []int64{ids[2], ids[1]}) {
			t.Fatalf("leap day 2024-02-29 = %d %v (ids %v)", r.Status, itemIDs(r), ids)
		}
		if r := h.ui("list-tasks", 3, 1, map[string]any{"date": "2026-08-15"}); r.Status != 200 {
			t.Fatalf("valid date = %d %v", r.Status, r.Body)
		}
		if r := h.ui("list-tasks", 3, 1, nil); r.Status != 200 || len(itemIDs(r)) != 4 {
			t.Fatalf("absent date = %d %v", r.Status, itemIDs(r))
		}
		for _, bad := range []string{"2026-02-29", "2026-8-15", "2026-08-15T00:00:00Z", "2026-08-5", "26-08-15", "2026/08/15", " 2026-08-15", "2026-08-15 "} {
			reads := h.Store.Reads
			expectCode(t, h.ui("list-tasks", 3, 1, map[string]any{"date": bad}), 400, "invalid_request")
			if h.Store.Reads != reads {
				t.Fatalf("date %q read task rows before the rejection", bad)
			}
		}
	})

	// M76 (TestLimitMalformedRejectedBeforeQuery), list-tasks part: every
	// malformed limit shape is invalid_pagination before any task row is read.
	run(t, "M76 malformed limit shapes", func(t *testing.T) {
		h := newHarness(t)
		h.seedTask(3, 1, "A")
		shapes := []any{"", " ", "abc", "01", "010", "+1", "+10", "1.5", "50 ", " 50", "10 ", " 10", "0x1", "0xA", "1e1", "-10",
			json.Number("10.0"), json.Number("1e1"), json.Number("1.5"), json.Number("-10"), true, nil, []any{10}, map[string]any{"v": 10}}
		for _, raw := range shapes {
			reads := h.Store.Reads
			expectCode(t, h.ui("list-tasks", 3, 1, map[string]any{"limit": raw}), 400, "invalid_pagination")
			if h.Store.Reads != reads {
				t.Fatalf("limit %#v read task rows before the rejection", raw)
			}
		}
		for _, ok := range []any{10, 25, 50, 100} {
			if r := h.ui("list-tasks", 3, 1, map[string]any{"limit": ok}); r.Status != 200 {
				t.Fatalf("limit %#v = %d %v", ok, r.Status, r.Body)
			}
		}
	})

	// M51 (TestDecodeRejectsChangedOrdering): a correctly signed cursor
	// bound to another ordering or another read is rejected before any task
	// row is read.
	run(t, "M51 cursor bound to another ordering", func(t *testing.T) {
		h := newHarness(t)
		for i := 0; i < 11; i++ {
			h.advance(time.Second)
			h.seedTask(3, 1, "K"+tok(int64(i)))
		}
		first := h.ui("list-tasks", 3, 1, nil)
		cur := str(first.Body["next_cursor"])
		c, err := h.Codec.Decode(cur)
		if err != nil || cur == "" {
			t.Fatalf("issued cursor %q does not decode: %v", cur, err)
		}
		if r := h.ui("list-tasks", 3, 1, map[string]any{"cursor": cur}); r.Status != 200 || len(itemIDs(r)) != 1 {
			t.Fatalf("own cursor = %d %v", r.Status, r.Body)
		}
		// Same position and key, but the scope names an ascending ordering.
		asc := h.Codec.Encode(cursor.Cursor{Scope: c.Scope + "\x1forder=asc", Cutoff: c.Cutoff, Pos: c.Pos})
		// Same ordering text, but issued for the archive read.
		archive := h.Codec.Encode(cursor.Cursor{Scope: strings.Replace(c.Scope, "list-tasks", "list-archive", 1), Cutoff: c.Cutoff, Pos: c.Pos})
		for name, tok := range map[string]string{"ascending ordering": asc, "archive read": archive} {
			reads := h.Store.Reads
			expectCode(t, h.ui("list-tasks", 3, 1, map[string]any{"cursor": tok}), 400, "invalid_cursor")
			if h.Store.Reads != reads {
				t.Fatalf("%s cursor read task rows before the rejection", name)
			}
		}
		// The list-tasks cursor is refused by the reads with their own binding.
		for _, op := range []string{"list-archive", "list-executor-tasks"} {
			params := map[string]any{"cursor": cur}
			if op == "list-executor-tasks" {
				params["executor_id"] = 7
			}
			expectCode(t, h.ui(op, 3, 1, params), 400, "invalid_cursor")
		}
	})

	// M382 (TestDiscoveryFifoSameTimestampTieBreakByTaskId): tasks that share
	// one ingested_at reach the task queue in task_id ascending order.
	run(t, "M382 same instant publishes in task_id order", func(t *testing.T) {
		h := newHarness(t)
		h.teamCreated(3, imageTeam3)
		h.bind(3, 1, flow(2))
		h.intake(3, 1, entry(1, "C"), entry(2, "A"), entry(3, "B"))
		h.intake(3, 1, entry(4, "E"), entry(5, "D"))
		h.flush()
		msgs := h.published("task-manager.task.3.openhands.2.pub")
		if len(msgs) != 5 {
			t.Fatalf("queue messages = %d, want 5", len(msgs))
		}
		var order []int64
		for _, m := range msgs {
			order = append(order, num(decode(m.Data)["task_id"]))
		}
		if !sort.SliceIsSorted(order, func(i, j int) bool { return order[i] < order[j] }) {
			t.Fatalf("queue order %v, want task_id ascending", order)
		}
		for _, v := range h.currentOf(3) {
			if !v.IngestedAt.Equal(t0) {
				t.Fatalf("task %d ingested_at %v, want the shared instant %v", v.TaskID, v.IngestedAt, t0)
			}
		}
		// Reads break the same tie by task_id DESC.
		if got := itemIDs(h.ui("list-tasks", 3, 1, nil)); !sort.SliceIsSorted(got, func(i, j int) bool { return got[i] > got[j] }) || len(got) != 5 {
			t.Fatalf("list order %v, want task_id descending", got)
		}
	})

	// M226 (TestV0002_51_TaskImageOverrideRejectedAndClaimReplayStable):
	// an image field outside the payload (null or an image object) rejects
	// the message with nothing written; the clean task resolves the team
	// default image, and a repeated claim-token request for the same claim
	// returns a token for the same claim without a second claim record.
	run(t, "M226 image override rejected and claim replay stable", func(t *testing.T) {
		h := newHarness(t)
		h.teamCreated(3, imageTeam3)
		h.bind(3, 1, flow(2))
		for i, image := range []any{nil, map[string]any{"repository": "registry.invalid/untrusted", "digest": "sha256:" + strings.Repeat("b", 64)}} {
			bad := entry(int64(i+1), "same")
			bad["image"] = image
			ack, seq := h.intake(3, 1, bad)
			h.flush()
			if ack.Kind != wire.AckOK || len(h.Store.CurrentTasks()) != 0 {
				t.Fatalf("image %v: ack %v tasks %d, want rejected with no task", image, ack.Kind, len(h.Store.CurrentTasks()))
			}
			expectRejected(t, h, seq, 1, "invalid_source_task")
		}
		if len(h.taskMsgs()) != 0 || len(h.targetRequests()) != 0 {
			t.Fatalf("rejected image messages published %d tasks / %d targets requests", len(h.taskMsgs()), len(h.targetRequests()))
		}
		task, s := h.claimed(3, 7, "same")
		original := h.claimToken(7, task, 1, 1, s)
		writes := h.Store.Writes
		retry := h.claimToken(7, task, 1, 1, s)
		if original.Status != 200 || retry.Status != 200 {
			t.Fatalf("claim token = %d, retry = %d", original.Status, retry.Status)
		}
		want := claimtoken.Payload{TeamID: 3, TaskID: task, Attempt: 1, ClaimSeq: 1, ExecutorID: 7}
		for name, r := range map[string]reply{"original": original, "retry": retry} {
			p, err := claimtoken.Verify(h.Signer.Public(), str(r.Body["claim_token"]))
			if err != nil || p != want {
				t.Fatalf("%s token payload %+v err %v, want %+v", name, p, err, want)
			}
		}
		if h.Store.Writes != writes {
			t.Fatalf("repeated claim-token request wrote %d transactions", h.Store.Writes-writes)
		}
		if n := len(h.eventsFor("claim_accepted", 3, task)); n != 1 {
			t.Fatalf("claim_accepted events = %d, want 1", n)
		}
		card := h.getTask(3, task)
		if str(card.Body["image_source"]) != "team_default" || str(card.Body["resolved_image"]) != imageTeam3 {
			t.Fatalf("claim image = %v / %v, want team_default %s", card.Body["image_source"], card.Body["resolved_image"], imageTeam3)
		}
		claims := list(obj(list(card.Body["attempts"])[0])["claims"])
		if len(claims) != 1 {
			t.Fatalf("claims after replay = %d, want 1", len(claims))
		}
	})

	// M33 / M306 (TestLoadRotatingCursorKeyring,
	// TestCursorKeyringForBootLoadsRotationSet): a two-key cursor_keys
	// configuration loads as a rotation set; the first key signs new
	// cursors, the previous key still verifies, and a retired key does not.
	run(t, "M33 M306 two-key cursor rotation configuration", func(t *testing.T) {
		key1 := "cursor_keys:\n  - id: 1\n    key_hex: 0909090909090909090909090909090909090909090909090909090909090909\n"
		hexOf := func(b string) string { return strings.Repeat(b, 32) }
		codecFor := func(keys string) *cursor.Codec {
			cfg, err := parseConfig(t, configWith(t, key1, keys))
			if err != nil {
				t.Fatalf("parse %q: %v", keys, err)
			}
			c, err := cfg.CursorCodec()
			if err != nil {
				t.Fatalf("codec %q: %v", keys, err)
			}
			return c
		}
		only1 := "cursor_keys:\n  - id: 1\n    key_hex: " + hexOf("01") + "\n"
		rotated := "cursor_keys:\n  - id: 2\n    key_hex: " + hexOf("02") + "\n  - id: 1\n    key_hex: " + hexOf("01") + "\n"
		only2 := "cursor_keys:\n  - id: 2\n    key_hex: " + hexOf("02") + "\n"
		cfg, err := parseConfig(t, configWith(t, key1, rotated))
		if err != nil || len(cfg.CursorKeys) != 2 || cfg.CursorKeys[0].ID != 2 || cfg.CursorKeys[1].ID != 1 {
			t.Fatalf("rotation set = %+v err %v, want active 2 then previous 1", cfg.CursorKeys, err)
		}

		h := newHarness(t)
		for i := 0; i < 12; i++ {
			h.advance(time.Second)
			h.seedTask(3, 1, "R"+tok(int64(i)))
		}
		use := func(c *cursor.Codec) { h.Codec = c; h.Svc = h.peer(h.St) }
		use(codecFor(only1))
		old := str(h.ui("list-tasks", 3, 1, nil).Body["next_cursor"])
		if old == "" {
			t.Fatalf("no cursor under key 1")
		}

		use(codecFor(rotated))
		if r := h.ui("list-tasks", 3, 1, map[string]any{"cursor": old}); r.Status != 200 || len(itemIDs(r)) != 2 {
			t.Fatalf("previous-key cursor after rotation = %d %v", r.Status, r.Body)
		}
		issued := str(h.ui("list-tasks", 3, 1, nil).Body["next_cursor"])
		body, err := base64.RawURLEncoding.DecodeString(strings.SplitN(issued, ".", 2)[0])
		var content struct {
			K int `json:"k"`
		}
		if err != nil || json.Unmarshal(body, &content) != nil || content.K != 2 {
			t.Fatalf("new cursor key_id = %d (%v), want active key 2", content.K, err)
		}
		if _, err := codecFor(only2).Decode(issued); err != nil {
			t.Fatalf("new cursor does not verify under key 2 alone: %v", err)
		}
		if _, err := codecFor(only1).Decode(issued); err == nil {
			t.Fatalf("new cursor verifies under the previous key")
		}

		use(codecFor(only2))
		expectCode(t, h.ui("list-tasks", 3, 1, map[string]any{"cursor": old}), 400, "invalid_cursor")

		dup := "cursor_keys:\n  - id: 1\n    key_hex: " + hexOf("02") + "\n  - id: 1\n    key_hex: " + hexOf("01") + "\n"
		if cfg, err := parseConfig(t, configWith(t, key1, dup)); err == nil {
			if _, err := cfg.CursorCodec(); err == nil {
				t.Fatalf("duplicate cursor key ids were accepted")
			}
		}
	})
}
