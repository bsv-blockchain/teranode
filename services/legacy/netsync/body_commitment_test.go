package netsync

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockvalidation"
	legacychain "github.com/bsv-blockchain/teranode/services/legacy/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// Stop valid bodies at the downstream boundary so this test needs no background
// block processing. Invalid bodies must never reach this boundary.
type bodyCommitmentBlockValidation struct {
	blockvalidation.Interface
	called bool
}

func (v *bodyCommitmentBlockValidation) ProcessBlock(context.Context, *model.Block, uint32, string, string, uint32) error {
	v.called = true
	return errors.NewBlockInvalidError("body commitment downstream sentinel")
}

func TestHandleBlockMsg_VerifiedHeaderBodyCommitment(t *testing.T) {
	for _, unified := range []bool{false, true} {
		route := "inline"
		if unified {
			route = "unified"
		}
		t.Run(route, func(t *testing.T) {
			for _, mutation := range []string{"none", "transaction", "coinbase", "coinbase only", "duplicate"} {
				t.Run(mutation, func(t *testing.T) {
					initPrometheusMetrics()
					sm, p, _ := newHeaderProvenanceManager(t)
					sm.settings.BlockValidation.LegacyUnifiedBelowCheckpoint = unified
					storeURL, err := url.Parse("sqlitememory:///")
					require.NoError(t, err)
					store, err := utxosql.New(sm.ctx, ulogger.TestLogger{}, sm.settings, storeURL)
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, store.Close(sm.ctx)) })
					sm.utxoStore = store
					sm.subtreeStore = memory.New()
					sm.validationClient = makeSpendValidator(store)
					downstream := &bodyCommitmentBlockValidation{}
					sm.blockValidation = downstream

					corpus := buildParityCorpus(t)
					parent, err := wireMsgTxToBt(t, corpus.blocks[0].MsgBlock().Transactions[1])
					require.NoError(t, err)
					_, _, err = store.SpendAndCreate(sm.ctx, parent, 0, utxo.WithCreateOnly(), utxo.WithSkipExtendedInputs(true))
					require.NoError(t, err)
					before, err := store.Get(sm.ctx, parent.TxIDChainHash(), fields.Utxos)
					require.NoError(t, err)

					block := corpus.blocks[1].MsgBlock()
					if mutation != "duplicate" {
						block.Transactions = block.Transactions[:2]
					}
					if mutation == "coinbase only" {
						block.Transactions = block.Transactions[:1]
					}
					block.Header.PrevBlock = *sm.chainParams.GenesisHash
					block.Header.Bits = 0x207fffff
					roots := legacychain.BuildMerkleTreeStore(bsvutil.NewBlock(block).Transactions())
					block.Header.MerkleRoot = *roots[len(roots)-1]
					require.True(t, solveBlock(&block.Header, sm.chainParams.PowLimit))
					hash := block.Header.BlockHash()
					// Prove the header the honest way, through the production path: pin a
					// checkpoint at height 1 naming this block and hand the handler a headers
					// batch containing it. The cache anchors on the committed tip, which is
					// genesis here, so the batch's single header sits at height 1 and matches.
					pinCheckpoint(sm, 1, &hash)
					headers := wire.NewMsgHeaders()
					require.NoError(t, headers.AddBlockHeader(&block.Header))
					sm.handleHeadersMsg(&headersMsg{headers: headers, peer: p})
					require.True(t, sm.blockOrigin(hash).headerProven)

					switch mutation {
					case "duplicate":
						block.Transactions = append(block.Transactions, block.Transactions[len(block.Transactions)-1])
					case "transaction":
						block.Transactions[1].TxOut[0].Value--
					case "coinbase", "coinbase only":
						block.Transactions[0].TxOut[0].Value--
					}
					require.Equal(t, hash, block.Header.BlockHash(), "body substitution preserves the proven header")
					roots = legacychain.BuildMerkleTreeStore(bsvutil.NewBlock(block).Transactions())
					if mutation == "none" || mutation == "duplicate" {
						require.Equal(t, block.Header.MerkleRoot, *roots[len(roots)-1])
					} else {
						require.NotEqual(t, block.Header.MerkleRoot, *roots[len(roots)-1])
					}

					// This branch's download ledger, not upstream's requestedBlocks maps,
					// is what handleBlockMsg consults to admit a block: mark it owed by
					// this peer or the call is rejected as unrequested before it ever
					// reaches the check this test is about.
					require.True(t, sm.blockDownloads.Add(p, hash))
					err = sm.handleBlockMsg(&blockQueueMsg{block: block, blockHash: hash, peer: p})
					if mutation == "none" {
						require.ErrorContains(t, err, "body commitment downstream sentinel")
						require.True(t, downstream.called, "a matching body must retain the processing route")
						return
					}
					if mutation == "duplicate" {
						require.ErrorContains(t, err, "duplicate transaction")
					} else {
						require.ErrorContains(t, err, "merkle root mismatch")
					}
					// Every mutation here is body-derived and caught BEFORE the body is bound to
					// the header, so the verdict is corrupt, not invalid (bitcoin-sv/teranode#4692):
					// the received body cannot distinguish an honest block corrupted in transit
					// from an attacker's junk, so it may not condemn the hash. ErrBlockCorrupt is
					// deliberately NOT matched by errors.Is(err, ErrBlockInvalid), so assert both
					// directions — the non-poisoning guarantee is exactly the second one. What this
					// test already pins below is unchanged and is the other half of the point: no
					// UTXO effect, no downstream call, no transaction created.
					require.True(t, errors.IsBlockCorrupt(err))
					require.False(t, errors.Is(err, errors.ErrBlockInvalid))
					require.False(t, downstream.called)
					after, err := store.Get(sm.ctx, parent.TxIDChainHash(), fields.Utxos)
					require.NoError(t, err)
					require.Equal(t, before.SpendingDatas, after.SpendingDatas, "parent spends must not change")
					for _, tx := range block.Transactions {
						txHash := tx.TxHash()
						_, err := store.Get(sm.ctx, &txHash)
						require.True(t, errors.Is(err, errors.ErrTxNotFound), "received transaction %s must not be created: %v", txHash, err)
					}
				})
			}
		})
	}
}

// bodyCommitment mirrors the small model passed by HandleBlockDirect. The
// constructor is safe for these small fixtures; production reuses its decode.
func bodyCommitment(t *testing.T, block *bsvutil.Block) *model.Block {
	t.Helper()
	commitment, err := model.NewBlockFromMsgBlock(block.MsgBlock(), nil)
	require.NoError(t, err)
	commitment.Subtrees = nil
	commitment.SubtreeSlices = nil
	return commitment
}

func setBodyMerkleRoot(block *wire.MsgBlock) {
	roots := legacychain.BuildMerkleTreeStore(bsvutil.NewBlock(block).Transactions())
	block.Header.MerkleRoot = *roots[len(roots)-1]
}
