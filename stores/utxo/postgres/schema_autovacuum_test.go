package postgres

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPendingDeletesLeavesHaveAutovacuumParams(t *testing.T) {
	store, _ := setupTestStore(t)

	var reloptions []string
	err := store.pool.QueryRow(context.Background(),
		`SELECT COALESCE(reloptions, '{}') FROM pg_class WHERE relname = 'pending_deletes_p00'`,
	).Scan(&reloptions)
	require.NoError(t, err)

	joined := strings.Join(reloptions, ",")
	require.Contains(t, joined, "autovacuum_vacuum_scale_factor=0.05")
	require.Contains(t, joined, "autovacuum_vacuum_insert_scale_factor=0.02")
	require.Contains(t, joined, "autovacuum_vacuum_cost_limit=2000")
	require.Contains(t, joined, "autovacuum_vacuum_cost_delay=2")
	require.Contains(t, joined, "autovacuum_analyze_scale_factor=0.05")
}
