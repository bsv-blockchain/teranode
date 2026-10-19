package netsync

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"sync/atomic"
	"testing"

	bt "github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockvalidation"
	legacychain "github.com/bsv-blockchain/teranode/services/legacy/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

type bodyCommitmentBlockValidation struct {
	blockvalidation.Interface
	called bool
}

func (v *bodyCommitmentBlockValidation) ProcessBlock(context.Context, *model.Block, uint32, string, string, uint32) error {
	v.called = true
	return errors.NewProcessingError("body commitment downstream sentinel")
}

type bodySubtreeBoundary struct {
	subtreevalidation.Interface
	called bool
}

func (v *bodySubtreeBoundary) CheckSubtreeFromBlock(context.Context, chainhash.Hash, string, uint32, *chainhash.Hash, *chainhash.Hash) error {
	v.called = true
	return errors.NewProcessingError("full script validation boundary")
}

type bodySubtreeStore struct {
	*memory.Memory
	writes atomic.Int32
}

func (s *bodySubtreeStore) Set(ctx context.Context, key []byte, kind fileformat.FileType, value []byte, opts ...options.FileOption) error {
	s.writes.Add(1)
	return s.Memory.Set(ctx, key, kind, value, opts...)
}

// Exercise delivery from a real checkpoint header request. Invalid bodies must
// neither spend/create UTXOs nor write even a subtree marker. The valid controls
// also check that the per-peer proof, rather than the global map, selects the route.
func TestHandleBlockMsg_BodyCommitment(t *testing.T) {
	for _, proven := range []bool{false, true} {
		for _, mutation := range []string{"none", "transaction", "coinbase", "coinbase only", "valid coinbase only", "duplicate", "incomplete final subtree"} {
			t.Run(fmt.Sprintf("proven=%v/%s", proven, mutation), func(t *testing.T) {
				initPrometheusMetrics()
				sm, p, state := newHeaderProvenanceManager(t)
				sm.settings.BlockAssembly.MaximumMerkleItemsPerSubtree = 4
				storeURL, err := url.Parse("sqlitememory:///")
				require.NoError(t, err)
				store, err := utxosql.New(sm.ctx, ulogger.TestLogger{}, sm.settings, storeURL)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, store.Close(sm.ctx)) })
				sm.utxoStore = store
				blobs := &bodySubtreeStore{Memory: memory.New()}
				sm.subtreeStore = blobs
				fullValidation := &bodySubtreeBoundary{}
				sm.subtreeValidation = fullValidation
				downstream := &bodyCommitmentBlockValidation{}
				sm.blockValidation = downstream
				sm.validationClient = &bodySpendValidator{store: store}

				makeTx := func(parent chainhash.Hash, value int64) *wire.MsgTx {
					tx := wire.NewMsgTx(1)
					tx.AddTxIn(&wire.TxIn{PreviousOutPoint: wire.OutPoint{Hash: parent}, Sequence: 0xffffffff, SignatureScript: []byte{0x00}})
					tx.AddTxOut(&wire.TxOut{Value: value, PkScript: []byte{0x51}})
					return tx
				}
				parentWire := makeTx(chainhash.Hash{}, 1000)
				parentWire.TxIn[0].PreviousOutPoint.Index = 0xffffffff
				parentWire.TxIn[0].SignatureScript = []byte{1, 1}
				parent := &bt.Tx{}
				require.NoError(t, WireTxToGoBtTx(bsvutil.NewTx(parentWire), parent))
				_, err = store.Create(sm.ctx, parent, 0)
				require.NoError(t, err)
				before, err := store.Get(sm.ctx, parent.TxIDChainHash(), fields.Utxos)
				require.NoError(t, err)

				block := makeDuplicateTxidBlock(1).MsgBlock()
				block.Transactions = block.Transactions[:1]
				count := 3
				if mutation == "coinbase only" || mutation == "valid coinbase only" {
					count = 1
				}
				if mutation == "incomplete final subtree" {
					count = 7
				}
				prev := parentWire.TxHash()
				for i := 1; i < count; i++ {
					tx := makeTx(prev, 1000-int64(i))
					block.Transactions = append(block.Transactions, tx)
					prev = tx.TxHash()
				}
				block.Header.PrevBlock = *sm.chainParams.GenesisHash
				block.Header.Bits = 0x207fffff
				setBodyMerkleRoot(block)
				require.True(t, solveBlock(&block.Header, sm.chainParams.PowLimit))
				hash := block.Header.BlockHash()
				sm.nextCheckpoint = &chaincfg.Checkpoint{Height: 1, Hash: &hash}
				sm.headerList.PushBack(&headerNode{height: 0, hash: sm.chainParams.GenesisHash})
				headers := wire.NewMsgHeaders()
				require.NoError(t, headers.AddBlockHeader(&block.Header))
				sm.handleHeadersMsg(&headersMsg{headers: headers, peer: p})
				require.True(t, sm.blockOrigin(state, hash).headerProven)
				// Simulate an ordinary inventory request for the unproven case, and put
				// the opposite provenance in the shorter-lived global deduplication map.
				state.requestedBlocks.Set(hash, blockRequestOrigin{headerProven: proven})
				sm.requestedBlocks.Set(hash, blockRequestOrigin{headerProven: !proven})

				switch mutation {
				case "duplicate":
					block.Transactions = append(block.Transactions, block.Transactions[len(block.Transactions)-1])
				case "transaction":
					block.Transactions[len(block.Transactions)-1].TxOut[0].Value--
				case "coinbase", "coinbase only":
					block.Transactions[0].TxOut[0].Value--
				}
				if mutation == "duplicate" {
					roots := legacychain.BuildMerkleTreeStore(bsvutil.NewBlock(block).Transactions())
					require.Equal(t, block.Header.MerkleRoot, *roots[len(roots)-1], "duplicate padding must preserve the proven header's commitment")
				}
				err = sm.handleBlockMsg(&blockQueueMsg{block: block, blockHash: hash, peer: p})
				_, requested := state.requestedBlocks.Get(hash)
				require.False(t, requested)
				if mutation == "none" || mutation == "incomplete final subtree" || mutation == "valid coinbase only" {
					if mutation != "valid coinbase only" {
						require.Positive(t, blobs.writes.Load(), "valid bodies must exercise the tracked write boundary")
					}
					if proven || mutation == "valid coinbase only" {
						require.ErrorContains(t, err, "body commitment downstream sentinel")
						require.True(t, downstream.called)
						require.False(t, fullValidation.called)
					} else {
						require.ErrorContains(t, err, "full script validation boundary")
						require.True(t, fullValidation.called)
						require.False(t, downstream.called)
					}
					return
				}
				if mutation == "duplicate" {
					require.ErrorContains(t, err, "duplicate transaction")
				} else {
					require.ErrorContains(t, err, "merkle root does not match")
				}
				require.True(t, errors.Is(err, errors.ErrBlockInvalid), "%v", err)
				require.False(t, downstream.called)
				require.False(t, fullValidation.called)
				require.Zero(t, blobs.writes.Load())
				after, err := store.Get(sm.ctx, parent.TxIDChainHash(), fields.Utxos)
				require.NoError(t, err)
				require.Equal(t, before.SpendingDatas, after.SpendingDatas)
				for _, tx := range block.Transactions {
					hash := tx.TxHash()
					_, err := store.Get(sm.ctx, &hash)
					require.True(t, errors.Is(err, errors.ErrTxNotFound), "received transaction must not be created: %v", err)
				}
			})
		}
	}
}

// Use real SQL spends to exercise the release's legacy prevalidation writes.
type bodySpendValidator struct {
	validator.Interface
	store *utxosql.Store
}

func (v *bodySpendValidator) Validate(ctx context.Context, tx *bt.Tx, height uint32, _ ...validator.Option) (*meta.Data, error) {
	_, err := v.store.Spend(ctx, tx, height)
	return &meta.Data{}, err
}

func (s *bodySubtreeStore) SetFromReader(ctx context.Context, key []byte, kind fileformat.FileType, reader io.ReadCloser, opts ...options.FileOption) error {
	s.writes.Add(1)
	return s.Memory.SetFromReader(ctx, key, kind, reader, opts...)
}

func (v *bodySpendValidator) EnsureMTPLoaded(context.Context, uint32) error { return nil }
