//go:build integration

package test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pandops/flowai-task-manager/internal/pgstore"
)

const tmSchema = "task_manager_schema"

// pgEnv is one fresh database with the deployment's role layout: a LOGIN
// role without CREATE on the database that owns task_manager_schema.
type pgEnv struct {
	Admin    *sql.DB
	Database string
	Role     string
	Password string
}

func (e pgEnv) roleDSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable&search_path=%s", e.Role, e.Password, shared.host, shared.port, e.Database, tmSchema)
}

// newPGEnv creates the role (dropped after the database) and the database.
// precreate=false leaves the schema missing and grants CREATE on the
// database, as a Testcontainers run without the deployment does.
func newPGEnv(t *testing.T, precreate bool) pgEnv {
	t.Helper()
	tokenStr, err := randomToken(10)
	if err != nil {
		t.Fatalf("token: %v", err)
	}
	role := "task_manager_" + strings.ToLower(tokenStr)
	password, _ := randomToken(20)
	cluster, err := sql.Open("pgx", shared.adminDSN("postgres"))
	if err != nil {
		t.Fatalf("admin: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := cluster.ExecContext(ctx, fmt.Sprintf(`CREATE ROLE %s LOGIN PASSWORD '%s'`, role, password)); err != nil {
		t.Fatalf("create role: %v", err)
	}
	t.Cleanup(func() {
		defer cluster.Close()
		c, cc := context.WithTimeout(context.Background(), 15*time.Second)
		defer cc()
		if _, err := cluster.ExecContext(c, "DROP ROLE IF EXISTS "+role); err != nil {
			t.Errorf("cleanup: drop role %s: %v", role, err)
		}
	})
	admin := freshDB(t)
	var db string
	if err := admin.QueryRowContext(ctx, "SELECT current_database()").Scan(&db); err != nil {
		t.Fatalf("current_database: %v", err)
	}
	stmts := []string{
		"REVOKE ALL ON DATABASE " + db + " FROM PUBLIC",
		"REVOKE ALL ON SCHEMA public FROM PUBLIC",
		"GRANT CONNECT ON DATABASE " + db + " TO " + role,
		"ALTER ROLE " + role + " IN DATABASE " + db + " SET search_path = " + tmSchema,
	}
	if precreate {
		stmts = append(stmts, "CREATE SCHEMA "+tmSchema+" AUTHORIZATION "+role)
	} else {
		stmts = append(stmts, "GRANT CREATE ON DATABASE "+db+" TO "+role)
	}
	for _, s := range stmts {
		if _, err := admin.ExecContext(ctx, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	t.Cleanup(func() {
		c, cc := context.WithTimeout(context.Background(), 15*time.Second)
		defer cc()
		// The role owns objects in the database; drop them so the database
		// cleanup and DROP ROLE both succeed.
		if _, err := admin.ExecContext(c, "DROP OWNED BY "+role+" CASCADE"); err != nil {
			t.Errorf("cleanup: drop owned by %s: %v", role, err)
		}
	})
	return pgEnv{Admin: admin, Database: db, Role: role, Password: password}
}

// openPG opens and migrates a pgstore as the role (one process).
func openPG(t *testing.T, env pgEnv) *pgstore.Store {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	st, err := pgstore.Open(ctx, env.roleDSN(), tmSchema)
	if err != nil {
		t.Fatalf("open pgstore: %v", err)
	}
	t.Cleanup(st.Close)
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return st
}
