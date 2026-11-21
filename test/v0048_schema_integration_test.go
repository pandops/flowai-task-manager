//go:build integration

package test

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

// Coverage: "Each service writes only its own <name>_schema with its own
// role; another schema is refused" (1.7; service-platform) and
// task-manager "Service starts with its configured schema".
func TestV0048SchemaIntegration(t *testing.T) {
	run(t, "Migrations create objects only in task_manager_schema without CREATE on the database", func(t *testing.T) {
		env := newPGEnv(t, true)
		openPG(t, env)
		openPG(t, env) // a second start re-checks pg_namespace and migrates nothing twice
		rows, err := env.Admin.Query(`SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE pg_get_userbyid(c.relowner) = $1 AND n.nspname <> 'pg_toast'`, env.Role) // TOAST relations are created by PostgreSQL itself
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		defer rows.Close()
		n := 0
		for rows.Next() {
			var ns, rel string
			rows.Scan(&ns, &rel)
			n++
			if ns != tmSchema {
				t.Fatalf("object %s.%s outside %s", ns, rel, tmSchema)
			}
		}
		if n == 0 {
			t.Fatalf("migrations created nothing")
		}
		var hasSeq bool
		env.Admin.QueryRow(`SELECT EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relkind='S')`, tmSchema).Scan(&hasSeq)
		if !hasSeq {
			t.Fatalf("no publisher_seq sequence in %s", tmSchema)
		}
	})

	run(t, "Missing schema is created when the role may create it", func(t *testing.T) {
		env := newPGEnv(t, false)
		openPG(t, env)
		var owner string
		if err := env.Admin.QueryRow(`SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname=$1`, tmSchema).Scan(&owner); err != nil || owner != env.Role {
			t.Fatalf("schema owner %q (%v)", owner, err)
		}
	})

	run(t, "Another schema is refused", func(t *testing.T) {
		env := newPGEnv(t, true)
		openPG(t, env)
		for _, s := range []string{"CREATE SCHEMA team_registry_schema", "CREATE TABLE team_registry_schema.teams(id bigint)", "INSERT INTO team_registry_schema.teams VALUES (1)"} {
			if _, err := env.Admin.Exec(s); err != nil {
				t.Fatalf("%s: %v", s, err)
			}
		}
		roleDB, err := sql.Open("pgx", env.roleDSN())
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer roleDB.Close()
		ctx := context.Background()
		for _, q := range []string{"SELECT count(*) FROM team_registry_schema.teams", "CREATE TABLE public.x(id int)", "CREATE SCHEMA other_schema"} {
			_, err := roleDB.ExecContext(ctx, q)
			if err == nil || !strings.Contains(err.Error(), "permission denied") {
				t.Fatalf("%q as task-manager's role: %v", q, err)
			}
		}
	})
}
