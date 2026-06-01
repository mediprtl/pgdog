package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The postgres_affinity database overrides load_balancing_strategy to
// "client_affinity" (same cluster as postgres, on the main 6432 instance).
func affinityConn(t *testing.T) *pgx.Conn {
	conn, err := pgx.Connect(
		context.Background(),
		"postgres://postgres:postgres@127.0.0.1:6432/postgres_affinity?sslmode=disable",
	)
	require.NoError(t, err)
	return conn
}

// affinityConnWithKey passes pgdog.replica_affinity_key via the startup
// `options` field (-c k=v), the same mechanism used for pgdog.role.
func affinityConnWithKey(t *testing.T, key string) *pgx.Conn {
	conn, err := pgx.Connect(
		context.Background(),
		fmt.Sprintf(
			"postgres://postgres:postgres@127.0.0.1:6432/postgres_affinity?sslmode=disable&options=-c%%20pgdog.replica_affinity_key%%3D%s",
			key,
		),
	)
	require.NoError(t, err)
	return conn
}

// A single client connection must read from exactly one replica for its whole
// lifetime (read-your-writes / monotonic reads), instead of round-robining.
func TestClientAffinityPinsToOneReplica(t *testing.T) {
	ResetStats()

	conn := affinityConn(t)
	defer conn.Close(context.Background())

	// Write goes to the primary and replicates to both replicas.
	_, err := conn.Exec(context.Background(),
		"CREATE TABLE IF NOT EXISTS lb_client_affinity (id BIGINT)")
	require.NoError(t, err)
	defer conn.Exec(context.Background(), "DROP TABLE IF EXISTS lb_client_affinity")

	// Let the replicas catch up before reading.
	time.Sleep(2 * time.Second)

	const reads = 20
	for i := range reads {
		_, err := conn.Exec(context.Background(),
			"SELECT * FROM lb_client_affinity WHERE id = $1", int64(i))
		assert.NoError(t, err)
	}

	replicaCalls := LoadStatsForReplicas("lb_client_affinity")
	assert.Equal(t, 2, len(replicaCalls))

	var served, total int64
	for _, call := range replicaCalls {
		if call.Calls > 0 {
			served++
		}
		total += call.Calls
	}

	assert.Equal(t, int64(1), served,
		fmt.Sprintf("client must pin to exactly one replica, got calls=%v", replicaCalls))
	assert.Equal(t, int64(reads), total, "all reads must reach the pinned replica")
}

// Separate connections sharing a pgdog.replica_affinity_key (e.g. all
// connections from one pod) must pin to the *same* replica, so a process-wide
// read-your-writes check on one connection is valid for reads on another.
func TestClientAffinityKeyPinsConnectionsToSameReplica(t *testing.T) {
	ResetStats()

	setup := affinityConnWithKey(t, "pod-shared")
	_, err := setup.Exec(context.Background(),
		"CREATE TABLE IF NOT EXISTS lb_affinity_key (id BIGINT)")
	require.NoError(t, err)
	setup.Close(context.Background())
	defer func() {
		drop := affinityConn(t)
		drop.Exec(context.Background(), "DROP TABLE IF EXISTS lb_affinity_key")
		drop.Close(context.Background())
	}()

	time.Sleep(2 * time.Second)

	// Distinct connections, same affinity key.
	const conns = 4
	const readsPerConn = 5
	for c := range conns {
		conn := affinityConnWithKey(t, "pod-shared")
		for i := range readsPerConn {
			_, err := conn.Exec(context.Background(),
				"SELECT * FROM lb_affinity_key WHERE id = $1", int64(c*readsPerConn+i))
			assert.NoError(t, err)
		}
		conn.Close(context.Background())
	}

	replicaCalls := LoadStatsForReplicas("lb_affinity_key")
	assert.Equal(t, 2, len(replicaCalls))

	var served, total int64
	for _, call := range replicaCalls {
		if call.Calls > 0 {
			served++
		}
		total += call.Calls
	}

	assert.Equal(t, int64(1), served,
		fmt.Sprintf("connections sharing a key must pin to one replica, got calls=%v", replicaCalls))
	assert.Equal(t, int64(conns*readsPerConn), total, "all keyed reads must reach the same replica")
}
