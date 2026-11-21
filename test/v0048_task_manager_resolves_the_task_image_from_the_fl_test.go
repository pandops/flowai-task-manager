package test

import (
	"testing"
	"time"
)

// Spec: task-manager — task-manager resolves the task image from the Flow or the team default.
func TestV0048TaskManagerResolvesTheTaskImage(t *testing.T) {
	run(t, "Flow supplies the image", func(t *testing.T) {
		h := newHarness(t)
		f := flow(2)
		f.Image = map[string]any{"repository": "registry.example/flow", "digest": flowDigest}
		h.seedTask(3, 1, "A", f)
		m := h.taskMsgs()[0]
		if str(m["resolved_image"]) != "registry.example/flow@"+flowDigest || str(m["image_source"]) != "flow_default" {
			t.Fatalf("image = %v / %v", m["resolved_image"], m["image_source"])
		}
	})

	run(t, "Flow has no image", func(t *testing.T) {
		h := newHarness(t)
		h.seedTask(3, 1, "A")
		m := h.taskMsgs()[0]
		if str(m["resolved_image"]) != imageTeam3 || str(m["image_source"]) != "team_default" {
			t.Fatalf("image = %v / %v", m["resolved_image"], m["image_source"])
		}
	})

	run(t, "Default image changes before a republication", func(t *testing.T) {
		h := newHarness(t)
		task, _ := h.claimed(3, 7, "A")
		h.released(3, 7, task, 1, 1, "pending_timeout")
		h.flush()
		h.teamUpdated(3, imageNew)
		h.due(5 * time.Minute)
		msgs := h.taskMsgsFor(3, task)
		if len(msgs) != 2 || str(msgs[1]["resolved_image"]) != imageNew || num(msgs[1]["claim_seq"]) != 2 {
			t.Fatalf("republication = %v", msgs)
		}
		first := h.current(3, task).Attempts[0].Claims[0]
		if first.ResolvedImage != imageTeam3 || first.ImageSource != "team_default" {
			t.Fatalf("earlier claim lost its image: %+v", first)
		}
		card := h.getTask(3, task)
		claims := list(obj(list(card.Body["attempts"])[0])["claims"])
		if len(claims) != 1 || str(obj(claims[0])["resolved_image"]) != imageTeam3 {
			t.Fatalf("card claims = %v", claims)
		}
	})
}
