package test

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/pandops/flowai-task-manager/internal/claimtoken"
)

// Spec: task-manager — task-manager issues signed claim tokens bound to the claim.
func TestV0048TaskManagerIssuesSignedClaimTokens(t *testing.T) {
	run(t, "Winner requests its claim token", func(t *testing.T) {
		h := newHarness(t)
		task, s := h.claimed(3, 7, "A")
		r := h.claimToken(7, task, 1, 1, s)
		tokStr := str(r.Body["claim_token"])
		if r.Status != 200 || tokStr == "" {
			t.Fatalf("token reply = %d %v", r.Status, r.Body)
		}
		if h.Signer == nil {
			t.Fatalf("no signer")
		}
		p, err := claimtoken.Verify(h.Signer.Public(), tokStr)
		if err != nil || p != (claimtoken.Payload{TeamID: 3, TaskID: task, Attempt: 1, ClaimSeq: 1, ExecutorID: 7}) {
			t.Fatalf("payload = %+v err %v", p, err)
		}
		parts := strings.Split(tokStr, ".")
		if len(parts) != 2 || strings.ContainsAny(tokStr, "=+/") {
			t.Fatalf("token is not base64url(payload).base64url(sig): %q", tokStr)
		}
		raw, _ := base64.RawURLEncoding.DecodeString(parts[0])
		var fields map[string]any
		_ = json.Unmarshal(raw, &fields)
		if _, ok := fields["expires_at"]; ok || len(fields) != 5 {
			t.Fatalf("payload fields = %v (rule 12: exactly team_id, task_id, attempt, claim_seq, executor_id)", fields)
		}
		tampered := parts[0] + "." + strings.Repeat("A", len(parts[1]))
		if _, err := claimtoken.Verify(h.Signer.Public(), tampered); err == nil {
			t.Fatalf("tampered token verified")
		}
		if again := h.claimToken(7, task, 1, 1, s); again.Status != 200 {
			t.Fatalf("repeated request = %d", again.Status)
		}
	})

	run(t, "Claim token stays valid while its claim is current", func(t *testing.T) {
		h := newHarness(t)
		task := h.runningTask(3, 7, "A")
		h.advance(3 * time.Hour)
		if r := h.claimCurrent(3, task, 1, 1, 7); r.Status != 200 || r.Body["current"] != true {
			t.Fatalf("after 3 h = %d %v", r.Status, r.Body)
		}
		h.lost(3, 7, 900)
		h.flush()
		if r := h.claimCurrent(3, task, 1, 1, 7); r.Body["current"] != false || str(r.Body["reason"]) != "claim_ended" {
			t.Fatalf("after release = %v", r.Body)
		}
	})

	run(t, "Created event not yet processed", func(t *testing.T) {
		h := newHarness(t)
		task := h.seedTask(3, 1, "A")
		h.ensureExecutor(3, 7)
		writes := h.Store.Writes
		r := h.claimToken(7, task, 1, 1, 42)
		if r.Status != 409 || str(r.Body["code"]) != "claim_not_processed" || r.Body["claim_token"] != nil || h.Store.Writes != writes {
			t.Fatalf("reply = %d %v writes %d->%d", r.Status, r.Body, writes, h.Store.Writes)
		}
	})

	run(t, "Losing Executor requests a claim token", func(t *testing.T) {
		h := newHarness(t)
		task, _ := h.claimed(3, 7, "A")
		h.ensureExecutor(3, 8)
		s8 := h.created(3, 8, task, 1, 1)
		if r := h.claimToken(8, task, 1, 1, s8); r.Status != 409 || str(r.Body["code"]) != "claim_rejected" {
			t.Fatalf("reply = %d %v", r.Status, r.Body)
		}
	})

	run(t, "Released claim requests a token", func(t *testing.T) {
		h := newHarness(t)
		task, s := h.claimed(3, 7, "A")
		h.released(3, 7, task, 1, 1, "pending_timeout")
		if r := h.claimToken(7, task, 1, 1, s); r.Status != 409 || str(r.Body["code"]) != "claim_rejected" {
			t.Fatalf("reply = %d %v", r.Status, r.Body)
		}
	})

	run(t, "Claim token stays off the streams", func(t *testing.T) {
		h := newHarness(t)
		task, s := h.claimed(3, 7, "A")
		token := str(h.claimToken(7, task, 1, 1, s).Body["claim_token"])
		if token == "" {
			t.Fatalf("no token issued")
		}
		h.running(3, 7, task, 1, 1)
		h.cancel(3, 11, task)
		h.flush()
		claims := h.executorClaims(3, 7, 0, claimRef{Task: task, Attempt: 1, Claim: 1})
		for _, m := range h.Bus.Messages() {
			if strings.Contains(string(m.Data), token) || strings.Contains(string(m.Data), strings.Split(token, ".")[1]) {
				t.Fatalf("token on %s", m.Subject)
			}
		}
		if strings.Contains(string(claims.Raw), token) {
			t.Fatalf("token in the executor-claims reply")
		}
	})

	run(t, "Unknown Executor", func(t *testing.T) {
		h := newHarness(t)
		task := h.seedTask(3, 1, "A")
		if r := h.claimToken(77, task, 1, 1, 1); r.Status != 503 || str(r.Body["code"]) != "service_unavailable" {
			t.Fatalf("unprojected Executor = %d %v", r.Status, r.Body)
		}
	})
}
