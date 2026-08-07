package blockpersister

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// setupMockUTXOStoreReturning wires a BatchDecorate that answers with whatever
// replacements maps an index to, and the real transaction otherwise. This lets a
// test put a degenerate snapshot reconstruction behind a subtree node whose hash
// is the genuine txid, which is exactly what a snapshot-seeded node does.
func setupMockUTXOStoreReturning(txs []*bt.Tx, replacements map[int]*bt.Tx) *utxo.MockUtxostore {
	mockStore := &utxo.MockUtxostore{}

	mockStore.On("BatchDecorate", mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			hashes := args.Get(1).([]*utxo.UnresolvedMetaData)
			for _, missing := range hashes {
				if missing.Idx >= len(txs) {
					continue
				}

				tx := txs[missing.Idx]
				if replacement, ok := replacements[missing.Idx]; ok {
					tx = replacement
				}

				missing.Data = &meta.Data{Tx: tx}
			}
		}).
		Return(nil)

	mockStore.On("Health", mock.Anything, mock.Anything).Return(0, "", nil)

	return mockStore
}

// TestCreateSubtreeDataFileStreaming_RejectsSnapshotReconstruction pins the other
// place a stored *bt.Tx is turned back into bytes behind a nil-only guard. A
// snapshot reconstruction either panics inside go-bt on a nil output hole or
// serializes cleanly into a short, 0-input transaction with the wrong txid, which
// would be written into the subtree data file as if it were the real transaction.
// Either way the write must fail rather than produce a corrupt file.
func TestCreateSubtreeDataFileStreaming_RejectsSnapshotReconstruction(t *testing.T) {
	tests := []struct {
		name        string
		replacement func(real *bt.Tx) *bt.Tx
	}{
		{
			name: "nil output hole",
			replacement: func(_ *bt.Tx) *bt.Tx {
				return &bt.Tx{Outputs: []*bt.Output{nil, {Satoshis: 1000, LockingScript: bscript.NewFromBytes([]byte{0x51})}}}
			},
		},
		{
			name: "fully live reconstruction, no nil hole",
			replacement: func(real *bt.Tx) *bt.Tx {
				return &bt.Tx{Outputs: []*bt.Output{real.Outputs[0]}}
			},
		},
		{
			// A complete, serializable transaction that is simply not the one the
			// subtree node names. Only the txid comparison catches this.
			name: "complete tx filed under the wrong key",
			replacement: func(real *bt.Tx) *bt.Tx {
				other := bt.NewTx()
				_ = other.From("0000000000000000000000000000000000000000000000000000000000000002", 0, "76a914000000000000000000000000000000000000000088ac", 2000)
				_ = other.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 999)

				return other
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := ulogger.TestLogger{}
			tSettings := test.CreateBaseTestSettings(t)

			txCount := 4
			txs := make([]*bt.Tx, txCount)

			// Every fixture tx carries an input, so the only record the gate can
			// reject is the reconstruction planted at index 2. Without this the
			// test would pass on the wrong transaction.
			for i := 0; i < txCount; i++ {
				txs[i] = bt.NewTx()
				require.NoError(t, txs[i].From("0000000000000000000000000000000000000000000000000000000000000001", uint32(i), "76a914000000000000000000000000000000000000000088ac", 2000))
				require.NoError(t, txs[i].AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", uint64(1000+i)))
			}

			subtree, err := subtreepkg.NewTreeByLeafCount(txCount)
			require.NoError(t, err)

			for _, tx := range txs {
				require.NoError(t, subtree.AddNode(*tx.TxIDChainHash(), 1, uint64(tx.Size())))
			}

			// Index 2's record comes back as a reconstruction, filed under the
			// genuine txid the subtree node names.
			mockUTXOStore := setupMockUTXOStoreReturning(txs, map[int]*bt.Tx{2: tt.replacement(txs[2])})

			subtreeStore := memory.New()
			blockStore := memory.New()

			blockBytes, err := hex.DecodeString("010000006fe28c0ab6f1b372c1a6a246ae63f74f931e8365e15a089c68d61900000000006657a9252aacd5c0b2940996ecff952228c3067cc38d4885efb5a4ac4247e9f337221b4d4c86041b0f2b571004010000000100000000000000000000000000000000000000000000000000000000000000" +
				"00ffffffff08044c86041b020602ffffffff0100f2052a010000004341041b0e8c2567c12536aa13357b79a073dc4444acb83c4ec7a0e2f99dd7457516c5817242da796924ca4e99947d087fedf9ce467cb9f7c6287078f801df276fdf84ac00000000")
			require.NoError(t, err)

			block, err := model.NewBlockFromBytes(blockBytes)
			require.NoError(t, err)

			subtreeBytes, err := subtree.Serialize()
			require.NoError(t, err)
			require.NoError(t, subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

			persister := New(context.Background(), logger, tSettings, blockStore, subtreeStore, mockUTXOStore, nil)

			err = persister.CreateSubtreeDataFileStreaming(context.Background(), *subtree.RootHash(), block, 1)
			require.Error(t, err, "a reconstruction must not be written into the subtree data file")
		})
	}
}
