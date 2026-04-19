// Package tests provides tests for the PurgeConflictingUnmined function.
package tests

import (
	"context"
	"fmt"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// setupSQLiteFileStore creates a file-based SQLite store in t.TempDir().
func setupSQLiteFileStore(ctx context.Context, t *testing.T) utxo.Store {
	t.Helper()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.DBTimeout = 30 * time.Second

	dbPath := filepath.Join(t.TempDir(), "purge-test.db")
	storeURL, err := url.Parse(fmt.Sprintf("sqlite:///%s", dbPath))
	require.NoError(t, err)

	store, err := sql.New(ctx, logger, tSettings, storeURL)
	require.NoError(t, err)

	return store
}

// testBlockchainQuerier is a local implementation of utxo.BlockchainQuerier for testing.
type testBlockchainQuerier struct {
	bestBlockHash   *chainhash.Hash
	bestBlockHeight uint32
	blockHeaderIDs  []uint32
}

func (tbq *testBlockchainQuerier) GetBestBlockHeaderInfo(ctx context.Context) (utxo.BlockHeaderInfo, error) {
	return utxo.BlockHeaderInfo{Hash: tbq.bestBlockHash, Height: tbq.bestBlockHeight}, nil
}

func (tbq *testBlockchainQuerier) GetBlockHeaderIDs(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]uint32, error) {
	return tbq.blockHeaderIDs, nil
}

// newQuerier creates a simple querier with no blocks on the best chain.
func newQuerier() *testBlockchainQuerier {
	h := &chainhash.Hash{}
	return &testBlockchainQuerier{
		bestBlockHash:   h,
		bestBlockHeight: 0,
		blockHeaderIDs:  []uint32{},
	}
}

// TestPurgeConflictingUnmined_CleanState verifies that running the purge on an
// empty store produces a zeroed report.
func TestPurgeConflictingUnmined_CleanState(t *testing.T) {
	ctx := context.Background()
	store := setupSQLiteFileStore(ctx, t)
	querier := newQuerier()

	report, err := utxo.PurgeConflictingUnmined(ctx, store, querier, false, nil)
	require.NoError(t, err)
	require.Equal(t, 0, report.UnminedSinceFixed)
	require.Equal(t, 0, report.ConflictingUnminedPurged)
}
