package test

import "testing"

// Spec: service-platform — service gets foreign data only through events or requests.
func TestV0048ServicePlatformServiceGetsForeignDataOnlyThroughEvents(t *testing.T) {
	run(t, "Team archive reaches a projection", func(t *testing.T) {
		h := newHarness(t)
		h.teamCreated(3, imageTeam3)
		h.bind(3, 1, flow(2))
		h.teamArchived(3, imageTeam3)
		_, seq := h.intake(3, 1, entry(1, "A"))
		h.flush()
		expectRejected(t, h, seq, 1, "team_archived")
	})

	run(t, "Projection ignores a replayed event", func(t *testing.T) {
		h := newHarness(t)
		h.teamEvent("team_created", 3, imageTeam3, false) // team-registry seq 1
		h.bind(3, 1, flow(2))
		h.teamArchived(3, imageTeam3) // seq 2
		// Redelivery of team_created (seq 1) after the archive.
		h.event("team-registry.event.team_created.sub", map[string]string{"team": "admin"}, map[string]any{"publisher": "team-registry", "publisher_seq": 1,
			"kind": "team_created", "occurred_at": ts(h.Now), "team_id": 3, "team_name": "team-3", "default_image": imageTeam3, "ingested_at": ts(h.Now), "archived_at": nil})
		_, seq := h.intake(3, 1, entry(1, "A"))
		h.flush()
		expectRejected(t, h, seq, 1, "team_archived")
	})
}
