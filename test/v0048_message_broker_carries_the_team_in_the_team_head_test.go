package test

import "testing"

// Spec: message-broker — carries the team in the team header.
func TestV0048MessageBrokerCarriesTheTeamInTheTeamHeader(t *testing.T) {
	run(t, "Executor header names another team", func(t *testing.T) {
		h := newHarness(t)
		task, _ := h.claimed(3, 7, "A")
		h.ensureTeam(4)
		// Executor 7 belongs to team 3; its header and envelope name team 4.
		h.exec(4, 7, "running", task, 1, 1, map[string]any{"header_team": "4"})
		cur := h.current(3, task)
		if cur.State != "running" {
			t.Fatalf("team must come from executor_id, not the header: state %s", cur.State)
		}
		foreign := h.seedTask(4, 1, "B")
		h.exec(4, 7, "created", foreign, 1, 1, map[string]any{"header_team": "4"})
		if h.current(4, foreign).State != "queued" {
			t.Fatalf("a team-4 header let a team-3 Executor claim a team-4 task")
		}
		h.flush()
		for _, m := range h.Bus.Messages() {
			body := decode(m.Data)
			if team, ok := body["team_id"]; ok && m.Header["team"] != tok(num(team)) {
				t.Fatalf("%s: header team %q, payload team %v", m.Subject, m.Header["team"], team)
			}
		}
	})
}
