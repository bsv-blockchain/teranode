// Package utxopersister — the missing-deletions tolerance must fire on the
// error the shipped store actually reports (notes section 42, open question 23).
package utxopersister

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestConsumeDeletions_ToleratesMissingLedgerOnFileStore pins the fix: the file
// store reports a missing blob as errors.ErrNotFound, while the tolerance was
// keyed to errors.ErrBlobNotFound, so a legal block that spent nothing fell
// through to the generic "No new block to process" line and the coin archive
// stalled looking healthy.
func TestConsumeDeletions_ToleratesMissingLedgerOnFileStore(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewVerboseTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	storeURL, err := url.Parse("file://" + t.TempDir())
	require.NoError(t, err)
	store, err := blob.NewStore(logger, storeURL)
	require.NoError(t, err)

	// A block that spent nothing has no deletions ledger in this store at all.
	hash := chainhash.HashH([]byte("block-that-spent-nothing"))

	us := &UTXOSet{
		logger:           logger,
		settings:         tSettings,
		blockHash:        hash,
		blockHeight:      200,
		blockHeightKnown: true,
		store:            store,
	}

	c := &consolidator{
		logger:    logger,
		settings:  tSettings,
		additions: map[UTXODeletion]*Addition{},
		deletions: map[UTXODeletion]struct{}{},
	}

	// Negative control: on this store the raw reader really does miss, and the
	// miss arrives as ErrNotFound - the very name the tolerance used to ignore.
	_, err = us.GetUTXODeletionsReader(ctx)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrNotFound),
		"expected the file store's ErrNotFound signal, got %v", err)

	// consumeDeletions must turn that miss into progress, not a stall.
	require.NoError(t, c.consumeDeletions(ctx, us))
}

// TestConsumeDeletions_PropagatesRealErrors keeps the tolerance narrow: an error
// that is not "ledger missing" must still abort the roll-up.
func TestConsumeDeletions_PropagatesRealErrors(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewVerboseTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	hash := chainhash.HashH([]byte("height-unknown-block"))
	us := &UTXOSet{
		logger:           logger,
		settings:         tSettings,
		blockHash:        hash,
		blockHeightKnown: false, // reader refuses before touching the store
	}
	c := &consolidator{
		logger:    logger,
		settings:  tSettings,
		additions: map[UTXODeletion]*Addition{},
		deletions: map[UTXODeletion]struct{}{},
	}

	err := c.consumeDeletions(ctx, us)
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires the block height")
}
