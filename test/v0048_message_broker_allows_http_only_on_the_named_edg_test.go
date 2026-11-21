package test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pandops/flowai-task-manager/internal/health"
)

// Spec: message-broker — allows HTTP only on the named edges. task-manager
// has no edge; "Source system reads its settings on the public edge",
// "Executor edge errors are distinguishable" and "Journal stream edge
// requires the Gateway token" are not applicable to task-manager.
func TestV0048MessageBrokerAllowsHTTPOnlyOnTheNamedEdges(t *testing.T) {
	run(t, "Service offers no internal HTTP API", func(t *testing.T) {
		ok := func() error { return nil }
		if probe(t, ok, "/v1/livez") != http.StatusOK || probe(t, ok, "/v1/readyz") != http.StatusOK {
			t.Fatalf("probes missing")
		}
		for _, p := range []string{"/", "/v1/tasks", "/ui/v1/tasks", "/v1/claims", "/healthz", "/v1/livez/x", "/metrics"} {
			if code := probe(t, ok, p); code != http.StatusNotFound {
				t.Fatalf("%s -> %d, want 404", p, code)
			}
		}
	})
}

// probe answers the status of a GET on the health handler.
func probe(t *testing.T, ready func() error, path string) int {
	t.Helper()
	rec := httptest.NewRecorder()
	health.Handler(ready).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec.Code
}
