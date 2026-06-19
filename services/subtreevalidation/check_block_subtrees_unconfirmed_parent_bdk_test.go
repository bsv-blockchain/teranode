package subtreevalidation

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation/subtreevalidation_api"
	"github.com/bsv-blockchain/teranode/services/validator"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// These tests are the BLOCK-PATH counterparts of the validator-level
// real-GoBDK pair TestValidate_ConsensusAcceptsUnconfirmedParentAtCandidateHeight /
// TestValidate_ConsensusRejectsUnconfirmedParent (services/validator).
// Same mainnet fixtures (child at height 257727 spending a parent that sits
// in the UTXO store with empty BlockHeights), same real TxValidator + GoBDK —
// but driven through subtreevalidation:
//
//   - accept leg: CheckBlockSubtrees, which hardwires
//     WithUnconfirmedParentsAtCandidateHeight(true) on the block path. The
//     sentinel resolves to the candidate height and BDK accepts.
//   - reject leg: the peer-facing ValidateSubtreeInternal option set (no
//     candidate-height option). The sentinel reaches BDK as MEMPOOL_HEIGHT
//     and the tx is rejected with bad-txns-unconfirmed-input-in-block —
//     pinning that the block-path acceptance comes from the option, not
//     from some store-level change.
//
// A third test pins the consensus backstop for the fail-open tx level: the
// parent here is NOT in the candidate block (a mempool floater), so the
// block itself must be rejected by model.Block.Valid →
// checkParentsExistOnChain even though every tx-level validation succeeded.

// realBDKChildTxHex / realBDKParentTxHex are the exact fixtures used by the
// validator-level pair (mainnet txs at heights 257727/257726).
const (
	realBDKChildTxHex  = "010000000000000000ef01febe0cbd7d87d44cbd4b5adac0a5bfcdbd2b672c9113f5d74a6459a2b85569db010000008b48304502207ec38d0a4ef79c3a4286ba3e5a5b6ede1fa678af9242465140d78a901af9e4e0022100c26c377d44b761469cf0bdcdbf4931418f2c5a02ce6b72bbb7af52facd7228c1014104bc9eb4fe4cb53e35df7e7734c4c3cd91c6af7840be80f4a1fff283e2cd6ae8f7713cb263a4590263240e3c01ec36bc603c32281ac08773484dc69b8152e48cecffffffff60b74700000000001976a9148ac9bdc626352d16e18c26f431e834f9aae30e2888ac0230424700000000001976a9148ac9bdc626352d16e18c26f431e834f9aae30e2888ac1027000000000000166a148ac9bdc626352d16e18c26f431e834f9aae30e2800000000"
	realBDKParentTxHex = "010000000000000000ef01154d5d31268f7ea94c80a7bf6de54e47812712feec25c17b8feceb570dfd9daf000000008b4830450220612b3ec065ec2b2a1757d97b7f57fba3c363645355cf6e1a5a1834411e6ab425022100bd071b90d391eb75dc9e2eea8b6774f36bf9c55439a971f0d1f4470b6448aef601410426e4e0654f72721b97a03c8170417c9ddabadcef97fe8ea626176ea62665b55ca2ff485f84df12ddec171e01ee8f9c7472c6c8467b0cf74ae8b3b614ed16cbdbffffffff008a6600000000001976a91429be45311cc66a5a6cc4a42516dbb7c9b126a3c188ac0280841e00000000001976a914996ed5e55d68aef653c85339f83873fac1321f0788ac60b74700000000001976a9148ac9bdc626352d16e18c26f431e834f9aae30e2888ac00000000"

	// realBDKCandidateHeight is the candidate block height for the fixtures
	// (mainnet 257727, pre-CSV and pre-Genesis — identical to the validator
	// pair, so the BDK consensus flags match exactly).
	realBDKCandidateHeight = uint32(257727)
)

// newRealBDKServer builds a subtreevalidation Server whose validatorClient is
// a REAL *validator.Validator (real TxValidator + GoBDK) over a sqlitememory
// UTXO store. dbName must be unique per test — sqlitememory databases are
// shared per path within a process.
//
// The parent fixture is created in the UTXO store WITHOUT WithMinedBlockInfo,
// so its BlockHeights/BlockIDs stay empty: the "parent unconfirmed" sentinel
// shape under test.
func newRealBDKServer(t *testing.T, dbName string) (*Server, utxo.Store, *bt.Tx, *bt.Tx) {
	InitPrometheusMetrics()

	ctx := context.Background()
	logger := ulogger.TestLogger{}

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams = &chaincfg.MainNetParams
	// Keep the real validator away from block assembly: the consensus
	// outcome under test is independent of BA, and the FSM in this test
	// setup is RUNNING (LocalClient placeholder).
	tSettings.BlockAssembly.Disabled = true

	utxoStoreURL, err := url.Parse("sqlitememory:///" + dbName)
	require.NoError(t, err)
	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	require.NoError(t, utxoStore.SetBlockHeight(realBDKCandidateHeight))
	require.NoError(t, utxoStore.SetMedianBlockTime(uint32(time.Now().Unix()))) //nolint:gosec

	parentTx, err := bt.NewTxFromString(realBDKParentTxHex)
	require.NoError(t, err)
	childTx, err := bt.NewTxFromString(realBDKChildTxHex)
	require.NoError(t, err)

	// Parent created WITHOUT mined-block info — BlockHeights stays empty.
	_, err = utxoStore.Create(ctx, parentTx, realBDKCandidateHeight-1)
	require.NoError(t, err)

	realValidator, err := validator.New(ctx, logger, tSettings, utxoStore,
		kafka.NewKafkaAsyncProducerMock(), kafka.NewKafkaAsyncProducerMock(), nil, nil, nil)
	require.NoError(t, err)

	subtreeStore := blobmemory.New()
	txStore := blobmemory.New()

	blockchainClient, err := blockchain.NewLocalClient(logger, tSettings, &blockchainstore.MockStore{}, subtreeStore, utxoStore)
	require.NoError(t, err)

	nilConsumer := &kafka.KafkaConsumerGroup{}
	server, err := New(ctx, logger, tSettings, subtreeStore, txStore, utxoStore, realValidator, blockchainClient, nilConsumer, nilConsumer, nil, nil)
	require.NoError(t, err)

	return server, utxoStore, parentTx, childTx
}

// buildRealBDKCandidateBlock assembles a structurally valid candidate block
// holding exactly the child tx: one subtree [coinbase placeholder, child],
// a real coinbase (output well under the 25-BTC subsidy at 257727), the
// merkle root computed with the coinbase substituted for the placeholder,
// and a header ground to meet its (regtest-easy) difficulty bits — so the
// same block object passes the header checks in model.Block.Valid.
//
// Both the subtree structure (FileTypeSubtreeToCheck) and the subtree data
// (FileTypeSubtreeData) are written to the server's subtree store, which is
// what CheckBlockSubtrees consumes for locally-held subtrees.
func buildRealBDKCandidateBlock(t *testing.T, server *Server, childTx *bt.Tx) *model.Block {
	ctx := context.Background()

	subtree, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())
	require.NoError(t, subtree.AddNode(*childTx.TxIDChainHash(), 111, uint64(childTx.Size()))) //nolint:gosec
	subtreeHash := *subtree.RootHash()

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, server.subtreeStore.Set(ctx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck, subtreeBytes))

	// SubtreeData stream: the coinbase placeholder at node 0 is skipped by
	// readTransactionsFromSubtreeDataStream, so the stream holds only the
	// child's standard wire bytes.
	require.NoError(t, server.subtreeStore.Set(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData, childTx.Bytes()))

	coinbaseTx, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)
	coinbaseTx.Outputs = nil
	// Well under the 25-BTC subsidy at height 257727 (+ fees), so
	// checkBlockRewardAndFees passes.
	require.NoError(t, coinbaseTx.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1_000_000))

	merkleRoot, err := subtree.RootHashWithReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, uint64(coinbaseTx.Size())) //nolint:gosec
	require.NoError(t, err)

	bits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	header := &model.BlockHeader{
		Version:        1, // version 1 → no BIP34 coinbase-height check in Valid
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: merkleRoot,
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           *bits,
		Nonce:          0,
	}
	// Grind the (trivial) 207fffff target so model.Block.Valid's PoW check
	// passes — CheckBlockSubtrees itself never looks at the header.
	// HasMetTargetDifficulty returns (false, err) while the target is not
	// met, so only the boolean is inspected here.
	for {
		if ok, _, _ := header.HasMetTargetDifficulty(); ok {
			break
		}
		header.Nonce++
	}

	block, err := model.NewBlock(header, coinbaseTx, []*chainhash.Hash{&subtreeHash},
		2, // coinbase + child
		uint64(coinbaseTx.Size()+childTx.Size()+80), //nolint:gosec
		realBDKCandidateHeight,
		0,
	)
	require.NoError(t, err)

	return block
}

// runRealBDKBlockPath drives the candidate block through CheckBlockSubtrees
// (the block-validation path that sets WithUnconfirmedParentsAtCandidateHeight)
// and requires the fail-open tx-level acceptance: response blessed, child
// validated into the UTXO store by the real validator.
func runRealBDKBlockPath(t *testing.T, server *Server, utxoStore utxo.Store, childTx *bt.Tx) *model.Block {
	block := buildRealBDKCandidateBlock(t, server, childTx)

	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	resp, err := server.CheckBlockSubtrees(context.Background(), &subtreevalidation_api.CheckBlockSubtreesRequest{
		Block:   blockBytes,
		BaseUrl: "legacy", // forces local-store reads; no HTTP fetches
	})
	require.NoError(t, err,
		"block path must accept a child of an unconfirmed parent: WithUnconfirmedParentsAtCandidateHeight resolves the sentinel to the candidate height before BDK sees it")
	require.NotNil(t, resp)
	require.True(t, resp.Blessed)

	childMeta, err := utxoStore.Get(context.Background(), childTx.TxIDChainHash())
	require.NoError(t, err, "child must have been validated into the UTXO store by the real validator")
	require.NotNil(t, childMeta)

	return block
}

// TestCheckBlockSubtrees_RealBDK_AcceptsUnconfirmedParentAtCandidateHeight is
// the accept leg of the block-path pair: same fixtures and real GoBDK as
// TestValidate_ConsensusAcceptsUnconfirmedParentAtCandidateHeight, but the
// option is supplied by CheckBlockSubtrees itself (production wiring), not by
// the test.
func TestCheckBlockSubtrees_RealBDK_AcceptsUnconfirmedParentAtCandidateHeight(t *testing.T) {
	server, utxoStore, _, childTx := newRealBDKServer(t, "blockpath_bdk_accept")

	runRealBDKBlockPath(t, server, utxoStore, childTx)
}

// TestValidateSubtreeInternal_RealBDK_PeerPathRejectsUnconfirmedParent is the
// reject leg: identical store state and fixtures, but validated through the
// peer-facing option set (no WithUnconfirmedParentsAtCandidateHeight). The
// unconfirmedParentHeight sentinel reaches BDK as MEMPOOL_HEIGHT and the tx
// is rejected with bad-txns-unconfirmed-input-in-block — proving the block
// path's acceptance above is caused by the option and that the peer path
// remains fail-closed.
func TestValidateSubtreeInternal_RealBDK_PeerPathRejectsUnconfirmedParent(t *testing.T) {
	server, _, _, childTx := newRealBDKServer(t, "peerpath_bdk_reject")

	childHash := *childTx.TxIDChainHash()

	subtree, err := subtreepkg.NewTreeByLeafCount(1)
	require.NoError(t, err)
	require.NoError(t, subtree.AddNode(childHash, 111, uint64(childTx.Size()))) //nolint:gosec

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, server.subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtreeToCheck, subtreeBytes))
	require.NoError(t, server.subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtreeData, childTx.ExtendedBytes()))

	v := ValidateSubtree{
		SubtreeHash:   *subtree.RootHash(),
		BaseURL:       "legacy", // store-read path; the OPTIONS below are the peer-facing set
		TxHashes:      []chainhash.Hash{childHash},
		AllowFailFast: false,
	}

	_, err = server.ValidateSubtreeInternal(context.Background(), v, realBDKCandidateHeight, nil,
		validator.WithSkipPolicyChecks(true),
		validator.WithCreateConflicting(true),
		validator.WithIgnoreLocked(true),
	)
	require.Error(t, err,
		"peer-facing option set must stay fail-closed for children of unconfirmed parents")
	require.Contains(t, err.Error(), "bad-txns-unconfirmed-input-in-block",
		"the rejection must be BDK's consensus-mode UnconfirmedInputInBlock, proving the sentinel was NOT substituted on the peer path")
}

// TestCheckBlockSubtrees_FloaterParent_BlockRejectedByValidBackstop pins
// consensus condition (b) of the WithUnconfirmedParentsAtCandidateHeight
// contract: tx-level validation on the block path is fail-open (a mempool
// floater's child blesses fine — the parent is unconfirmed and NOT in the
// block), so the block-level membership backstop must reject the block.
//
// Stage 1 runs the production block path (CheckBlockSubtrees + real BDK) and
// requires the fail-open blessing. Stage 2 feeds the very same block to
// model.Block.Valid with the same stores: checkParentsExistOnChain finds the
// parent with no recorded block IDs and fails the BLOCK with a
// BlockIncompleteError ("has no block IDs") — fail-soft per issue #1031, so
// catchup retries instead of persisting invalid, but the block is NOT
// accepted. Without this backstop the option would let a no-membership
// floater-child block onto the chain.
func TestCheckBlockSubtrees_FloaterParent_BlockRejectedByValidBackstop(t *testing.T) {
	server, utxoStore, parentTx, childTx := newRealBDKServer(t, "blockpath_bdk_backstop")

	// Stage 1 — fail-open at tx level: the parent is NOT in the block (only
	// the child is), yet the block path blesses the child.
	block := runRealBDKBlockPath(t, server, utxoStore, childTx)

	// Sanity: the parent really is the floater shape — in the UTXO store,
	// no recorded block IDs, and not a transaction of this block.
	parentMeta, err := utxoStore.Get(context.Background(), parentTx.TxIDChainHash())
	require.NoError(t, err)
	require.Empty(t, parentMeta.BlockIDs, "fixture must keep the parent unconfirmed (no block IDs)")

	// Stage 2 — the block-level backstop. ValidateSubtreeInternal (invoked
	// by CheckBlockSubtrees) wrote FileTypeSubtree + FileTypeSubtreeMeta to
	// the server's subtree store, which is exactly what Block.Valid reads.
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams = &chaincfg.MainNetParams

	valid, err := block.Valid(context.Background(), ulogger.TestLogger{},
		server.subtreeStore, utxoStore,
		txmap.NewSyncedMap[chainhash.Hash, []uint32](),
		[]*model.BlockHeader{}, []uint32{}, tSettings, nil)

	require.False(t, valid, "a block whose tx spends a floater (unconfirmed, not-in-block) parent must NOT validate")
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrBlockIncomplete),
		"floater parents surface as BlockIncompleteError (fail-soft, issue #1031) — got: %v", err)
	require.Contains(t, err.Error(), "has no block IDs",
		"the rejection must come from checkParentsExistOnChain's missing-membership check")
}
