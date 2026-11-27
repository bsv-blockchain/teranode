package subtreeprocessor

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// clearObserver records whether a request was already superseded when the map is first cleared.
type clearObserver struct {
	TxInpointsMap
	req               NewSubtreeRequest
	clears            int
	supersededAtClear bool
}

func (m *clearObserver) Clear() {
	if m.clears == 0 {
		m.supersededAtClear = m.req.Superseded()
	}

	m.clears++
	m.TxInpointsMap.Clear()
}

// A meta build that misses a parent because reset cleared the map must find its request already
// superseded, so reset flips the flag before it clears currentTxMap.
func TestResetSupersedesStoreRequestsBeforeClearingTheTxMap(t *testing.T) {
	ctx := context.Background()

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	settings := test.CreateBaseTestSettings(t)

	utxoStore, err := sql.New(ctx, ulogger.TestLogger{}, settings, utxoStoreURL)
	require.NoError(t, err)

	stp, err := NewSubtreeProcessor(ctx, ulogger.TestLogger{}, settings, blob_memory.New(),
		&blockchain.Mock{}, utxoStore, make(chan NewSubtreeRequest, 10))
	require.NoError(t, err)

	observer := &clearObserver{TxInpointsMap: stp.currentTxMap, req: NewSubtreeRequest{superseded: stp.supersedeToken()}}
	stp.currentTxMap = observer

	require.NoError(t, stp.reset(&model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      2200000003,
		Nonce:          9203,
	}, nil, nil, false, func() error { return nil }))

	require.NotZero(t, observer.clears, "reset must clear currentTxMap")
	require.True(t, observer.supersededAtClear, "the request must already be superseded when currentTxMap is cleared")
}
