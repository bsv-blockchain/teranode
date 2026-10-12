package netsync

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

func TestOrphanTxs_PoolCapRespected(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.OrphanEvictionDuration = time.Hour
	tSettings.Legacy.MaxOrphanTxs = 5

	pool := expiringmap.New[chainhash.Hash, *orphanTxAndParents](tSettings.Legacy.OrphanEvictionDuration).
		WithMaxSize(tSettings.Legacy.MaxOrphanTxs)
	defer pool.Stop()

	for i := uint32(0); i < 10; i++ {
		var hash chainhash.Hash
		binary.LittleEndian.PutUint32(hash[:], i)

		pool.Set(hash, &orphanTxAndParents{
			tx:      &bt.Tx{},
			parents: nil,
			addedAt: time.Now(),
		})

		time.Sleep(time.Millisecond)
	}

	require.LessOrEqual(t, pool.Len(), 5, "orphan pool must respect MaxOrphanTxs cap")
	require.Equal(t, 5, pool.Len(), "after 10 inserts with cap 5, exactly 5 entries should remain")
}

func TestOrphanTxs_DefaultSettingsAppliesCap(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	require.Greater(t, tSettings.Legacy.MaxOrphanTxs, 0,
		"default MaxOrphanTxs must be > 0 to enforce a cap on the orphan pool")
	require.LessOrEqual(t, tSettings.Legacy.MaxOrphanTxs, 1000,
		"default MaxOrphanTxs should be a sensible bound (Bitcoin Core uses 100)")
}
