package test

import "time"

// at sets the clock so the next intake gets ingested_at = when.
func (h *harness) at(when time.Time) {
	h.mu.Lock()
	h.Now = when
	h.mu.Unlock()
}

func itemIDs(r reply) []int64 {
	var out []int64
	for _, it := range list(r.Body["items"]) {
		out = append(out, num(obj(it)["task_id"]))
	}
	return out
}

func sameIDs(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func consumerName(team int64, tag string) string {
	sum := sha256Sum([]byte(tag))
	return "tasks-" + tok(team) + "-" + hexSHA(sum[:])[:16]
}
