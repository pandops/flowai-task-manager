package test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Spec: service-platform — service owns one PostgreSQL schema.
// "Access to a foreign schema is denied" is proven on PostgreSQL in
// v0048_schema_integration_test.go; the unit form checks the migrations.
func TestV0048ServicePlatformServiceOwnsOnePostgreSQLSchema(t *testing.T) {
	run(t, "Access to a foreign schema is denied", func(t *testing.T) {
		files, _ := filepath.Glob("../internal/pgstore/sql/*.sql")
		if len(files) == 0 {
			t.Fatalf("no migrations")
		}
		for _, f := range files {
			raw, _ := os.ReadFile(f)
			sql := strings.ToLower(string(raw))
			for _, other := range []string{"public.", "state_registry", "team_registry", "flow_registry", "pg_catalog."} {
				if strings.Contains(sql, other) {
					t.Fatalf("%s names %q", f, other)
				}
			}
		}
	})
}
