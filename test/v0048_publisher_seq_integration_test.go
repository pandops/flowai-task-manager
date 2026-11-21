//go:build integration

package test

import (
	"testing"
)

// Coverage: "Outbox publisher_seq comes from the service's own sequence in
// the committing transaction: a rolled-back transaction publishes nothing,
// and a restart continues the sequence, so a new message never reuses a
// Nats-Msg-Id" (1.7).
func TestV0048PublisherSeqIntegration(t *testing.T) {
	env := newPGEnv(t, true)
	st := openPG(t, env)
	h := newHarnessOn(t, st)
	h.teamCreated(3, imageTeam3)
	h.bind(3, 1, flow(2))
	h.intakeSeq(3, 1, 1, entry(1, "A"))
	h.flush()
	before := len(h.Bus.Messages())

	st.Hook = func(step string) error {
		if step == "append_outbox" {
			return errTest
		}
		return nil
	}
	h.intakeSeq(3, 1, 2, entry(2, "B"))
	st.Hook = nil
	h.flush()
	if len(h.Bus.Messages()) != before {
		t.Fatalf("rolled-back transaction published")
	}

	restarted := openPG(t, env)
	h2 := newHarnessOn(t, restarted)
	h2.Bus = h.Bus
	h2.Relay.Pub = h.Bus
	h2.Svc = h.peer(restarted)
	h2.intakeSeq(3, 1, 3, entry(3, "C"))
	h2.flush()
	ids := map[string]bool{}
	var last int64
	for _, m := range h.Bus.Messages() {
		id := m.Header["Nats-Msg-Id"]
		if ids[id] {
			t.Fatalf("Nats-Msg-Id %s reused", id)
		}
		ids[id] = true
		seq := num(decode(m.Data)["publisher_seq"])
		if seq <= last {
			t.Fatalf("sequence not continued: %d after %d", seq, last)
		}
		last = seq
	}
	if len(h.Bus.Messages()) <= before || int64(len(h.Bus.Messages())) >= last {
		t.Fatalf("no gap for the rolled-back transaction: %d messages, last seq %d", len(h.Bus.Messages()), last)
	}
}
