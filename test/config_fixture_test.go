package test

import (
	"strings"
	"testing"

	"github.com/pandops/flowai-task-manager/internal/config"
)

// configYAML is a complete production-shaped configuration.
const configYAML = `
http:
  listen: ":8080"
db:
  host: postgres
  port: 5432
  user: task_manager
  password: secret-password
  schema: task_manager_schema
  sslmode: verify-full
  ca_file: /run/flowai/stand-ca/ca.pem
nats:
  url: tls://nats:4222
  ca_file: /run/flowai/stand-ca/ca.pem
  credentials_file: /run/flowai/nats/user.creds
  inbox_prefix: _INBOX.task-manager
  request_timeout: 5s
claim_token:
  signing_key: BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc=
cursor_keys:
  - id: 1
    key_hex: 0909090909090909090909090909090909090909090909090909090909090909
`

// configWith returns configYAML with one line replaced (old must exist).
func configWith(t *testing.T, old, repl string) string {
	t.Helper()
	if !strings.Contains(configYAML, old) {
		t.Fatalf("config fixture has no %q", old)
	}
	return strings.Replace(configYAML, old, repl, 1)
}

func parseConfig(t *testing.T, doc string) (config.Config, error) {
	t.Helper()
	return config.Parse([]byte(doc))
}
