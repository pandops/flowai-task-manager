package test

import (
	"strings"
	"testing"
)

// Spec: message-broker — never carries secrets in streams.
func TestV0048MessageBrokerNeverCarriesSecretsInStreams(t *testing.T) {
	run(t, "Stream content inspection", func(t *testing.T) {
		h := newHarness(t)
		task, s := h.claimed(3, 7, "A")
		token := str(h.claimToken(7, task, 1, 1, s).Body["claim_token"])
		h.running(3, 7, task, 1, 1)
		h.cancel(3, 11, task)
		h.lost(3, 7, 900)
		h.admin(map[string]any{}, nil)
		h.flush()
		forbidden := []string{"secret-password", "signing_key", testSeed, "api_key", "master_key", "litellm", "BwcHBwcH"}
		if token != "" {
			forbidden = append(forbidden, token)
		}
		for _, m := range h.Bus.Messages() {
			for _, f := range forbidden {
				if strings.Contains(string(m.Data), f) {
					t.Fatalf("%s carries %q", m.Subject, f)
				}
			}
			for k, v := range m.Header {
				if strings.Contains(v, "claim") || strings.Contains(k, "Authorization") {
					t.Fatalf("%s header %s=%s", m.Subject, k, v)
				}
			}
		}
	})
}
