package main

import (
	"bytes"
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Regression for the backend-less "COMMIT; BEGIN" reopen drop.
//
// Autocommit-off drivers (e.g. ADBC) commit-and-reopen in one simple-query batch
// ("COMMIT; BEGIN"). PgDog routes on the first statement only, so when the
// committed transaction never checked out a backend (it only touched
// PgDog-intercepted state, e.g. a tracked SET), the trailing BEGIN was dropped:
// the next statements ran unpinned and transaction-local state set by one
// statement was invisible to the next. Concretely this broke RLS scoping done
// via `set_config('app.tenant_id', <uuid>, true)` -> `current_setting(...)::uuid`
// evaluating '' -> 22P02.
const _reopenTenant = "11111111-1111-1111-1111-111111111111"

// connReplica connects over the simple protocol with the same libpq routing
// options a read-only ADBC client uses, so reads load-balance to replicas.
func connReplica(t *testing.T) *pgx.Conn {
	t.Helper()
	cfg, err := pgx.ParseConfig("postgres://postgres:postgres@127.0.0.1:6432/postgres?sslmode=disable")
	require.NoError(t, err)
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	cfg.RuntimeParams["options"] = "-c pgdog.role=replica"
	conn, err := pgx.ConnectConfig(context.Background(), cfg)
	require.NoError(t, err)
	return conn
}

// A transaction whose only work is a tracked SET commits without a backend; the
// trailing BEGIN of "COMMIT; BEGIN" must still open a transaction that the next
// statements share, so a transaction-local set_config survives to the read.
func TestCommitThenBeginKeepsTransactionAfterBackendlessSet(t *testing.T) {
	conn := connReplica(t)
	defer conn.Close(context.Background())
	ctx := context.Background()

	_, err := conn.Exec(ctx, "BEGIN")
	require.NoError(t, err)
	// Tracked SET: PgDog intercepts it, no backend is checked out.
	_, err = conn.Exec(ctx, "SET statement_timeout = '30s'")
	require.NoError(t, err)
	// Commit (backend-less) and immediately reopen, in one simple-query batch.
	_, err = conn.Exec(ctx, "COMMIT; BEGIN")
	require.NoError(t, err)

	// In the reopened transaction: scope, then read the scope back.
	_, err = conn.Exec(ctx, "SELECT set_config('app.tenant_id', '"+_reopenTenant+"', true)")
	require.NoError(t, err)
	var got string
	err = conn.QueryRow(ctx, "SELECT current_setting('app.tenant_id')::uuid::text").Scan(&got)
	require.NoError(t, err, "transaction-local set_config must survive into the reopened transaction")
	assert.Equal(t, _reopenTenant, got)
	_, _ = conn.Exec(ctx, "COMMIT")
}

// Same, but the COPY read path (ADBC's fetch_arrow_table uses COPY ... TO STDOUT):
// the ::uuid cast fails inside COPY when the GUC is empty.
func TestCommitThenBeginKeepsTransactionCopyRead(t *testing.T) {
	conn := connReplica(t)
	defer conn.Close(context.Background())
	ctx := context.Background()

	for _, q := range []string{"BEGIN", "SET statement_timeout = '30s'", "COMMIT; BEGIN",
		"SELECT set_config('app.tenant_id', '" + _reopenTenant + "', true)"} {
		_, err := conn.Exec(ctx, q)
		require.NoError(t, err)
	}

	var buf bytes.Buffer
	_, err := conn.PgConn().CopyTo(ctx, &buf,
		"COPY (SELECT current_setting('app.tenant_id')::uuid::text) TO STDOUT")
	_, _ = conn.Exec(ctx, "COMMIT")
	require.NoError(t, err, "COPY read in the reopened transaction must see the transaction-local set_config")
	assert.Contains(t, buf.String(), _reopenTenant)
}
