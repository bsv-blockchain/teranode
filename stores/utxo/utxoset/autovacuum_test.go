package utxoset

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestAutovacuumThresholdIsSizedToTheIndex pins a number that silently caps throughput.
//
// A delete-on-spend store creates roughly 33,000 dead tuples per block, forever, so
// autovacuum is the one background job it does have, and unlike the pruner it CAN fall
// behind. Each pass is a full scan of that partition's index.
//
// The shipped value was 200,000, which is ten times more aggressive than the figure the
// design was costed against (utxo-set-store-design.md:459 uses 2,000,000). At fat-band
// density 200,000 triggers a full index scan every 19 blocks per partition, about 1.13 GB
// per block of index scanning, which caps the node near 2.65 blocks per second on that
// term alone. 1,000,000 gives roughly 0.23 GB per block, at a cost of about 1.6 GB of
// resident dead rows.
//
// scale_factor stays 0 deliberately: scale-factor triggering is proportional to table
// size, and the dead-row rate here is proportional to block production instead.
func TestAutovacuumThresholdIsSizedToTheIndex(t *testing.T) {
	s, ctx := newTestStore(t)

	var opts []string

	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT reloptions FROM pg_class WHERE oid = 'utxo_p0'::regclass`).Scan(&opts))

	require.Contains(t, opts, "autovacuum_vacuum_threshold=1000000",
		"200000 caps the node near 2.65 blk/s on index scanning alone")
	require.Contains(t, opts, "autovacuum_vacuum_scale_factor=0",
		"dead rows scale with block production, not with table size")
}

// TestIdentityPartitionsCarryTheReclaimerSizedAutovacuum. The identity table shipped with no
// storage parameters at all, so it vacuumed at the default 20 percent dead-tuple threshold with
// default throttling. With the reclaimer deleting several hundred thousand rows a minute that
// is a pass every twenty minutes or so per partition, each a full scan of its primary key, and
// between passes the key drifts toward the measured 2x bloat plateau. The coin table's block
// applies here for the same reason it applies there.
func TestIdentityPartitionsCarryTheReclaimerSizedAutovacuum(t *testing.T) {
	s, ctx := newTestStore(t)

	var opts []string

	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT reloptions FROM pg_class WHERE oid = 'tx_ident_l0'::regclass`).Scan(&opts))

	require.Contains(t, opts, "autovacuum_vacuum_threshold=1000000")
	require.Contains(t, opts, "autovacuum_vacuum_scale_factor=0")
	require.Contains(t, opts, "autovacuum_vacuum_cost_delay=0")
}
