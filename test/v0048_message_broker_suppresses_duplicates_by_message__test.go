package test

import "testing"

// Spec: message-broker — suppresses duplicates by message ID (task-manager's
// consumer side: deduplication by (publisher, publisher_seq)).
func TestV0048MessageBrokerSuppressesDuplicates(t *testing.T) {
	run(t, "Equal sequence numbers of two publishers are distinct messages", func(t *testing.T) {
		h := newHarness(t)
		a := h.seedTask(3, 1, "A")
		b := h.seedTask(3, 1, "B")
		h.ensureExecutor(3, 7)
		h.ensureExecutor(3, 8)
		h.execSeq(3, 7, "created", a, 1, 1, 5, nil)
		h.execSeq(3, 8, "created", b, 1, 1, 5, nil)
		if h.current(3, a).State != "created" || h.current(3, b).State != "created" {
			t.Fatalf("equal publisher_seq of two Executors deduplicated")
		}
		// source-controller and team-registry with the same, unused-by-the
		// publisher sequence number are distinct as well.
		h.teamCreated(6, imageTeam3)
		h.teamUpdated(6, imageTeam4) // team-registry reaches publisher_seq 3
		h.bind(6, 1, flow(2))
		seq := h.nextSeq("source-controller") // source-controller's first use of 3
		if seq != 3 || h.seq["team-registry"] != 3 {
			t.Fatalf("fixture: source-controller seq %d, team-registry seq %d; want 3 and 3", seq, h.seq["team-registry"])
		}
		h.intakeSeq(6, 1, seq, entry(1, "X"))
		h.flush()
		if len(h.currentOf(6)) != 1 {
			t.Fatalf("intake seq equal to a team-registry seq was dropped")
		}
		if msgs := h.taskMsgsFor(6, h.currentOf(6)[0].TaskID); len(msgs) != 1 || str(msgs[0]["resolved_image"]) != imageTeam4 {
			t.Fatalf("team-registry seq 3 was not applied: %v", msgs)
		}
	})
}
