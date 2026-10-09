package blockvalidation

import (
	"context"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/blockvalidation_api"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	"github.com/bsv-blockchain/teranode/stores/blob"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	bloboptions "github.com/bsv-blockchain/teranode/stores/blob/options"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// These tests pin processBlockFound on the unified below-checkpoint route now that legacy
// blocks are applied one at a time: a block whose parent is stored is quick-validated and
// committed inline, and a legacy block whose parent is not stored comes back as a local fault
// rather than a silent nil from the catch-up divert.
//
// The routing tests use coinbase-only blocks, which need no subtree files on disk. The
// provenance tests below use a block that spends a stored parent, because "never reaches
// SpendAndCreate" has to be read off the UTXO store.

// recordingBlockchainClient is the real blockchain client (LocalClient over a sqlitememory
// store) that also records every block reaching AddBlock.
type recordingBlockchainClient struct {
	blockchain.ClientI

	mu    sync.Mutex
	added []chainhash.Hash
}

func (c *recordingBlockchainClient) AddBlock(ctx context.Context, block *model.Block, peerID string, opts ...options.StoreBlockOption) error {
	c.mu.Lock()
	c.added = append(c.added, *block.Hash())
	c.mu.Unlock()

	return c.ClientI.AddBlock(ctx, block, peerID, opts...)
}

func (c *recordingBlockchainClient) addedBlocks() []chainhash.Hash {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]chainhash.Hash(nil), c.added...)
}

type unifiedRouteServer struct {
	s            *Server
	client       *recordingBlockchainClient
	utxoStore    *sql.Store
	subtreeStore blob.Store
	// subtreeValidation is a testify mock with no expectations: the quick route must never
	// call it, and a call with no expectation panics, which is the point. A test that drives
	// the full route adds the one expectation it wants first.
	subtreeValidation *subtreevalidation.MockSubtreeValidation
	genesis           chainhash.Hash
	bits              model.NBit
}

// newUnifiedRouteServer builds a Server wired for the unified below-checkpoint route: real
// sqlitememory blockchain and UTXO stores, a checkpoint above every test height, no
// block-assembly client so the gate is skipped, no p2p client, and a subtree validation
// mock that panics on any call nobody expected.
func newUnifiedRouteServer(t *testing.T, name string) *unifiedRouteServer {
	t.Helper()

	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	genesisHash := tSettings.ChainCfgParams.GenesisHash
	tSettings.ChainCfgParams.Checkpoints = []chaincfg.Checkpoint{{Height: 1000, Hash: genesisHash}}

	tSettings.BlockValidation.LegacyUnifiedBelowCheckpoint = true
	tSettings.BlockValidation.OutpointOnlyBelowCheckpoint = true
	tSettings.BlockValidation.QuickValidateSkipUtxoLock = true

	utxoStoreURL, err := url.Parse("sqlitememory:///" + name)
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)
	require.NoError(t, utxoStore.SetBlockHeight(1))

	blockchainStore, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	localClient, err := blockchain.NewLocalClient(logger, tSettings, blockchainStore, nil, utxoStore)
	require.NoError(t, err)

	client := &recordingBlockchainClient{ClientI: localClient}

	subtreeStore := blobmemory.New()
	txStore := blobmemory.New()
	subtreeValidation := &subtreevalidation.MockSubtreeValidation{}

	s := New(logger, tSettings, subtreeStore, txStore, utxoStore, nil, client, nil, nil, nil)
	s.blockValidation = NewBlockValidation(ctx, logger, tSettings, client, subtreeStore, txStore, utxoStore, nil, subtreeValidation)

	bits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	return &unifiedRouteServer{
		s: s, client: client, utxoStore: utxoStore, subtreeStore: subtreeStore,
		subtreeValidation: subtreeValidation, genesis: *genesisHash, bits: *bits,
	}
}

// unifiedRouteSpendingBlock builds a block at height 1 on genesis whose one subtree holds a
// transaction spending output 0 of a parent that is genuinely in the UTXO store: the
// coinbase placeholder at slot 0 and the child at slot 1, written as the files netsync
// writes (.subtreeToCheck and .subtreeData), with the header's merkle root composed from the
// coinbase-substituted subtree root and the header ground to its own target. The child is
// unextended and its unlocking script is nonsense, which is exactly what the outpoint-only
// quick route accepts without a script check and what makes "did it spend the parent" the
// question this fixture answers.
func unifiedRouteSpendingBlock(t *testing.T, us *unifiedRouteServer, seed byte) (block *model.Block, parent, child *bt.Tx) {
	t.Helper()

	ctx := context.Background()

	parent = genuineParentTx(t, seed)

	_, err := us.utxoStore.Create(ctx, parent, 1, utxo.WithSkipExtendedInputs(true))
	require.NoError(t, err)

	child = preBindSpendOf(t, parent, 9_000)
	coinbase := preBindCoinbase(t, seed)

	st := buildSubtreeOver(t, true, []*bt.Tx{child})

	structureBytes, err := st.Serialize()
	require.NoError(t, err)

	require.NoError(t, us.subtreeStore.Set(ctx, st.RootHash()[:], fileformat.FileTypeSubtreeToCheck, structureBytes, bloboptions.WithAllowOverwrite(true)))
	require.NoError(t, us.subtreeStore.Set(ctx, st.RootHash()[:], fileformat.FileTypeSubtreeData, serializeSubtreeData(t, st, true, coinbase, []*bt.Tx{child}), bloboptions.WithAllowOverwrite(true)))

	merkleRoot := coinbaseSubstitutedRoot(t, st, coinbase)
	prev := us.genesis

	header := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &prev,
		HashMerkleRoot: &merkleRoot,
		Timestamp:      1_700_000_000,
		Bits:           us.bits,
	}

	for {
		if ok, _, _ := header.HasMetTargetDifficulty(); ok {
			break
		}

		header.Nonce++
	}

	block = &model.Block{
		Header:           header,
		CoinbaseTx:       coinbase,
		TransactionCount: 2,
		Subtrees:         []*chainhash.Hash{st.RootHash()},
		Height:           1,
	}

	return block, parent, child
}

// genuineParentTx is a transaction with one 10,000 satoshi P2PKH output and a fabricated
// input, the honest parent a forged block would want to spend.
func genuineParentTx(t *testing.T, seed byte) *bt.Tx {
	t.Helper()

	parent := bt.NewTx()

	in := &bt.Input{PreviousTxOutIndex: 0, SequenceNumber: 0xffffffff, UnlockingScript: bscript.NewFromBytes([]byte{0x00})}
	require.NoError(t, in.PreviousTxIDAdd(&chainhash.Hash{seed, 0xee}))
	parent.Inputs = append(parent.Inputs, in)
	require.NoError(t, parent.AddP2PKHOutputFromAddress(preBindPayToAddress, 10_000))

	return parent
}

// parentSpendStatus reads output 0 of parent back from the UTXO store.
func parentSpendStatus(t *testing.T, us *unifiedRouteServer, parent *bt.Tx) int {
	t.Helper()

	utxoHash, err := util.UTXOHashFromOutput(parent.TxIDChainHash(), parent.Outputs[0], 0)
	require.NoError(t, err)

	resp, err := us.utxoStore.GetSpend(context.Background(), &utxo.Spend{TxID: parent.TxIDChainHash(), Vout: 0, UTXOHash: utxoHash})
	require.NoError(t, err)

	return resp.Status
}

// requireNothingApplied is the end state every unproven case shares: the block is not in
// the chain under any mark, nothing reached AddBlock, the parent output is still unspent
// and no record exists for the child.
func requireNothingApplied(t *testing.T, us *unifiedRouteServer, block *model.Block, parent, child *bt.Tx) {
	t.Helper()

	ctx := context.Background()

	exists, err := us.s.blockchainClient.GetBlockExists(ctx, block.Hash())
	require.NoError(t, err)
	require.False(t, exists, "the block must not be stored, valid or invalid")
	require.Empty(t, us.client.addedBlocks(), "nothing may reach AddBlock")

	require.Equal(t, int(utxo.Status_OK), parentSpendStatus(t, us, parent), "the parent output must still be unspent")

	_, err = us.utxoStore.Get(ctx, child.TxIDChainHash())
	require.Error(t, err, "no record may exist for the child")
	require.True(t, errors.Is(err, errors.ErrTxNotFound), "expected not-found, got %v", err)
}

// unifiedRouteCoinbaseOnlyBlock builds a block with a coinbase and no subtrees on prev, with
// height written into block.Height the way the legacy client writes the request height.
func unifiedRouteCoinbaseOnlyBlock(t *testing.T, us *unifiedRouteServer, prev chainhash.Hash, height uint32) *model.Block {
	t.Helper()

	_, publicKey := bec.PrivateKeyFromBytes([]byte("unified-route-server"))

	coinbase := transactions.Create(t,
		transactions.WithCoinbaseData(height, "/unified-route-server/"),
		transactions.WithP2PKHOutputs(1, 5_000_000_000, publicKey),
	)

	prevHash := prev

	return &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &prevHash,
			HashMerkleRoot: coinbase.TxIDChainHash(),
			Timestamp:      1_700_000_000 + height,
			Bits:           us.bits,
			Nonce:          height,
		},
		CoinbaseTx:       coinbase,
		TransactionCount: 1,
		Subtrees:         []*chainhash.Hash{},
		Height:           height,
	}
}

// A proven legacy block whose parent is stored is quick-validated and committed by this call,
// inline, with nothing else in flight to wait for.
func TestProcessBlockFound_UnifiedRouteBlockWithAStoredParentIsCommitted(t *testing.T) {
	us := newUnifiedRouteServer(t, "unified_route_committed")

	block := unifiedRouteCoinbaseOnlyBlock(t, us, us.genesis, 1)

	require.NoError(t, us.s.processBlockFound(context.Background(), block.Hash(), "peer-1", "legacy", true, block))

	exists, err := us.s.blockchainClient.GetBlockExists(context.Background(), block.Hash())
	require.NoError(t, err)
	require.True(t, exists, "the block is stored when processBlockFound returns")
	require.Equal(t, []chainhash.Hash{*block.Hash()}, us.client.addedBlocks())
}

// A legacy block on the unified route whose parent is not stored must never come back nil:
// the catch-up divert returns nil, and legacy sync would record the block as accepted with
// nothing stored. Legacy hands a block over only once its parent has committed, so this is a
// local fault, returned as a transient local error. Proven or not: the guard reads
// eligibility, because an unproven legacy orphan is still a legacy orphan, and diverting it
// to catch-up would report it accepted.
func TestProcessBlockFound_UnifiedRouteBlockWithNoStoredParentIsALocalFault(t *testing.T) {
	for _, headerProven := range []bool{true, false} {
		t.Run(map[bool]string{true: "proven", false: "unproven"}[headerProven], func(t *testing.T) {
			us := newUnifiedRouteServer(t, "unified_route_unknown_parent")

			orphan := unifiedRouteCoinbaseOnlyBlock(t, us, chainhash.Hash{0x01, 0x02, 0x03}, 2)

			err := us.s.processBlockFound(context.Background(), orphan.Hash(), "peer-1", "legacy", headerProven, orphan)
			require.Error(t, err, "a legacy block with no stored parent must never come back nil")
			require.True(t, errors.IsTransientLocalError(err), "the failure is ours, not the peer's: %v", err)
			require.Contains(t, err.Error(), "has no stored parent")

			require.Empty(t, us.client.addedBlocks(), "nothing may be stored for a block whose parent is unknown")
			require.Empty(t, us.s.catchupCh, "a legacy block never takes the catch-up divert")
		})
	}
}

// TestProcessBlockFound_UnprovenBelowCheckpointBlockNeverReachesSpendAndCreate is the
// block-validation half of GHSA-gggq-8f59-4jm9. A legacy block below the checkpoint whose
// parent is stored, served with headerProven false, must not take the quick route: the
// route spends by outpoint with no script or value check, so a block nobody has tied to a
// pinned checkpoint hash must never reach SpendAndCreate.
//
// With the proof absent the block takes full validation instead, which is the correct
// slow path for a .subtreeToCheck record (subtree validation creates the transactions and
// rebuilds the fees before the reward check) and the defence in depth behind netsync's own
// refusal. Full validation is where subtree validation is called, so the mock expects
// exactly one CheckBlockSubtrees and answers with a ServiceError, which
// ValidateBlockWithOptions returns as it stands (no revalidation, no invalid mark). That
// one expected call is the proof the full route was taken; the quick route makes none and
// would panic the expectation-free mock. The end state is read off the real stores: the
// block is not in the chain, nothing reached AddBlock, the parent output is unspent and
// the child has no record.
//
// Reverting the `headerProven &&` conjunct in Server.legacyUnifiedRoute sends the block
// down the quick route: the parent is spent, the child is created, the block is added, and
// the mock is never called.
func TestProcessBlockFound_UnprovenBelowCheckpointBlockNeverReachesSpendAndCreate(t *testing.T) {
	us := newUnifiedRouteServer(t, "unified_route_unproven")

	block, parent, child := unifiedRouteSpendingBlock(t, us, 0x11)

	us.subtreeValidation.On("CheckBlockSubtrees", mock.Anything, mock.Anything, "peer-1", "legacy").
		Return(errors.NewServiceError("no subtree validation in this harness")).Once()

	err := us.s.processBlockFound(context.Background(), block.Hash(), "peer-1", "legacy", false, block)
	require.Error(t, err, "an unproven eligible block must not commit through this harness")
	require.True(t, errors.IsTransientLocalError(err), "the full route's ServiceError comes back as retry-later: %v", err)

	us.subtreeValidation.AssertExpectations(t)
	require.Len(t, us.subtreeValidation.Calls, 1, "exactly one subtree validation call: the full route, never the quick route")

	requireNothingApplied(t, us, block, parent, child)
}

// TestProcessBlockFound_ProvenBelowCheckpointBlockIsAppliedByTheQuickRoute is the positive
// control for the test above: the same fixture with the proof present reaches
// SpendAndCreate. Without it the unproven test could pass because the fixture never
// reaches the route at all.
func TestProcessBlockFound_ProvenBelowCheckpointBlockIsAppliedByTheQuickRoute(t *testing.T) {
	us := newUnifiedRouteServer(t, "unified_route_proven")

	block, parent, child := unifiedRouteSpendingBlock(t, us, 0x12)

	require.NoError(t, us.s.processBlockFound(context.Background(), block.Hash(), "peer-1", "legacy", true, block))

	require.Equal(t, int(utxo.Status_SPENT), parentSpendStatus(t, us, parent), "the quick route spends the parent output by outpoint")

	_, err := us.utxoStore.Get(context.Background(), child.TxIDChainHash())
	require.NoError(t, err, "the quick route creates the child")

	require.Equal(t, []chainhash.Hash{*block.Hash()}, us.client.addedBlocks())
	require.Empty(t, us.subtreeValidation.Calls, "the quick route never calls subtree validation")
}

// TestServer_ProcessBlock_ForwardsHeaderProvenToTheRoute drives the gRPC handler rather than
// processBlockFound, so the request field is what decides the route. Two servers, because a
// block applied by the first would already exist for the second.
func TestServer_ProcessBlock_ForwardsHeaderProvenToTheRoute(t *testing.T) {
	t.Run("proven takes the quick route", func(t *testing.T) {
		us := newUnifiedRouteServer(t, "unified_route_rpc_proven")
		block, parent, _ := unifiedRouteSpendingBlock(t, us, 0x13)

		blockBytes, err := block.Bytes()
		require.NoError(t, err)

		_, err = us.s.ProcessBlock(context.Background(), &blockvalidation_api.ProcessBlockRequest{
			Block: blockBytes, Height: 1, BaseUrl: "legacy", PeerId: "peer-1", HeaderProven: true,
		})
		require.NoError(t, err)

		require.Equal(t, int(utxo.Status_SPENT), parentSpendStatus(t, us, parent))
		require.Equal(t, []chainhash.Hash{*block.Hash()}, us.client.addedBlocks())
	})

	t.Run("unproven takes full validation", func(t *testing.T) {
		us := newUnifiedRouteServer(t, "unified_route_rpc_unproven")
		block, parent, child := unifiedRouteSpendingBlock(t, us, 0x14)

		us.subtreeValidation.On("CheckBlockSubtrees", mock.Anything, mock.Anything, "peer-1", "legacy").
			Return(errors.NewServiceError("no subtree validation in this harness")).Once()

		blockBytes, err := block.Bytes()
		require.NoError(t, err)

		_, err = us.s.ProcessBlock(context.Background(), &blockvalidation_api.ProcessBlockRequest{
			Block: blockBytes, Height: 1, BaseUrl: "legacy", PeerId: "peer-1", HeaderProven: false,
		})
		require.Error(t, err)

		us.subtreeValidation.AssertExpectations(t)
		requireNothingApplied(t, us, block, parent, child)
	})
}

// Only a legacy block is refused. Any other block with a missing parent still goes to
// catch-up, which is how the native path resolves an orphan, and still returns nil.
func TestProcessBlockFound_NonLegacyBlockKeepsTheCatchupDivert(t *testing.T) {
	us := newUnifiedRouteServer(t, "unified_route_native")

	orphan := unifiedRouteCoinbaseOnlyBlock(t, us, chainhash.Hash{0x06, 0x05, 0x04}, 2)

	err := us.s.processBlockFound(context.Background(), orphan.Hash(), "peer-1", "http://peer:8000", false, orphan)
	require.NoError(t, err, "a non-legacy block with a missing parent still takes the catch-up divert")

	require.Eventually(t, func() bool { return len(us.s.catchupCh) == 1 }, 5*time.Second, 10*time.Millisecond,
		"the block must have been handed to catch-up")
	require.Empty(t, us.client.addedBlocks())
}

// TestProcessBlockFound_BlockAssemblyBehindIsALocalFaultForAnEligibleBlockProvenOrNot
// pins the wrap around the block-assembly gate. The gate's own failure is a
// ProcessingError, which legacy sync's park reads as a rejection and answers by
// deleting its only copy of the block, so for a legacy block on the unified
// route it is wrapped as a ServiceError. The wrap must key on eligibility, not on
// the proven route: an unproven eligible block is still a legacy block whose park
// entry would be destroyed by the raw error, which is why both rows are pinned.
//
// The gate retries a hundred times with exponential backoff and no deadline of
// its own, so the test bounds it with a context deadline. The wait then gives up
// with the context's error, which the wrap carries just as it carries the gate's
// own refusal; with the wrap keyed on the proven route instead, the unproven row
// comes back raw and the assertion below sees it.
func TestProcessBlockFound_BlockAssemblyBehindIsALocalFaultForAnEligibleBlockProvenOrNot(t *testing.T) {
	for _, headerProven := range []bool{true, false} {
		t.Run(map[bool]string{true: "proven", false: "unproven"}[headerProven], func(t *testing.T) {
			us := newUnifiedRouteServer(t, "unified_route_ba_behind")

			// Block assembly reports height 0 and the gate allows nothing behind it,
			// so a block at height 1 is refused by the gate for as long as it is asked.
			ba := blockassembly.NewMock()
			ba.On("GetBlockAssemblyState", mock.Anything).Return(&blockassembly_api.StateMessage{CurrentHeight: 0}, nil)
			us.s.blockAssemblyClient = ba
			us.s.settings.BlockValidation.MaxBlocksBehindBlockAssembly = 0

			block, parent, child := unifiedRouteSpendingBlock(t, us, 0x15)

			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()

			err := us.s.processBlockFound(ctx, block.Hash(), "peer-1", "legacy", headerProven, block)
			require.Error(t, err)
			require.True(t, errors.IsTransientLocalError(err), "a parked block-assembly gate is a local condition, proven or not: %v", err)
			require.Contains(t, err.Error(), "block assembly not ready")

			requireNothingApplied(t, us, block, parent, child)
		})
	}
}

// TestProcessBlockFound_AKeyMismatchingLocalSubtreeFileIsACorruptRecordOnTheLegacyRoute pins the
// unified branch's treatment of a structure file that does not hash to the key it is stored under.
// The read that rejects it returns a bare ProcessingError carrying only a data marker, which the
// legacy caller's commit table cannot key on and would read as a judgement on the block: the only
// copy deleted, the hash frozen for ten minutes, a reject sent to an honest peer. The route is
// legacy, so the file is this node's own and the sink has already verified the bytes it was written
// from; once the quarantine has removed the blob, the verdict comes back as corrupt, the code the
// caller's RecordCorrupt row reads (drop the record, no mark, no blame, download again). The end
// state is read off the real stores: the mismatching file is gone, and nothing was applied.
func TestProcessBlockFound_AKeyMismatchingLocalSubtreeFileIsACorruptRecordOnTheLegacyRoute(t *testing.T) {
	us := newUnifiedRouteServer(t, "unified_route_key_mismatch")

	ctx := context.Background()

	block, parent, child := unifiedRouteSpendingBlock(t, us, 0x13)
	key := block.Subtrees[0]

	// Another valid subtree, over a different spend of the same parent, written under THIS
	// block's key: a local file that does not hash to the name it is stored under.
	other := buildSubtreeOver(t, true, []*bt.Tx{preBindSpendOf(t, parent, 8_000)})
	require.NotEqual(t, *key, *other.RootHash(), "sanity: the substitute must hash to a different root")

	otherBytes, err := other.Serialize()
	require.NoError(t, err)
	require.NoError(t, us.subtreeStore.Set(ctx, key[:], fileformat.FileTypeSubtreeToCheck, otherBytes, bloboptions.WithAllowOverwrite(true)))

	err = us.s.processBlockFound(ctx, block.Hash(), "peer-1", "legacy", true, block)
	require.Error(t, err, "a structure file that does not hash to its key must not commit")
	require.True(t, errors.IsBlockCorrupt(err), "the verdict must be corrupt so the legacy caller drops the record and downloads it again, got: %v", err)
	require.False(t, errors.Is(err, errors.ErrBlockInvalid), "a damaged local file is never a judgement on the block: %v", err)
	require.False(t, errors.IsTransientLocalError(err), "a quarantined file is not a retry-later fault: %v", err)

	exists, err := us.subtreeStore.Exists(ctx, key[:], fileformat.FileTypeSubtreeToCheck)
	require.NoError(t, err)
	require.False(t, exists, "the mismatching file must have been quarantined, or the re-download could not rewrite it")

	requireNothingApplied(t, us, block, parent, child)
}

// TestProcessBlockFound_AnInvalidTransactionOnTheLegacyFullRouteIsAJudgementNotACorruptRecord
// pins the full route's answer for a legacy block with a consensus-invalid transaction.
// ValidateBlockWithOptions reclassifies that verdict as corrupt because, on the p2p route,
// nothing has bound the subtree list to the header and the body may be the serving peer's
// own. On the legacy route the list IS bound: the pipeline sink checked the merkle root
// against the header before the record existed and derived every subtree hash from the
// bytes it wrote. A corrupt verdict there lands on the legacy caller's RecordCorrupt row,
// which drops the record with no mark and no blame, so the same block is downloaded,
// validated in full and dropped again on every wanted-range pass. A judgement must come
// back as one: ErrBlockInvalid, which the caller's BlockRejected row marks and rejects.
//
// The control drives the same verdict with a p2p baseURL and requires corrupt, so the gate
// is proven to be the route and not the fixture. headerProven is false so the block takes
// full validation, which is where subtree validation is called.
func TestProcessBlockFound_AnInvalidTransactionOnTheLegacyFullRouteIsAJudgementNotACorruptRecord(t *testing.T) {
	txInvalid := func() error {
		// The shape processTransactionsInLevels raises: the per-transaction invalid under
		// subtree validation's processing wrap.
		return errors.NewProcessingError("[CheckBlockSubtrees] failed to process transactions",
			errors.NewTxInvalidError("transaction in subtree is invalid"))
	}

	t.Run("legacy route", func(t *testing.T) {
		us := newUnifiedRouteServer(t, "unified_route_legacy_tx_invalid")

		block, parent, child := unifiedRouteSpendingBlock(t, us, 0x14)

		us.subtreeValidation.On("CheckBlockSubtrees", mock.Anything, mock.Anything, "peer-1", "legacy").
			Return(txInvalid()).Once()

		err := us.s.processBlockFound(context.Background(), block.Hash(), "peer-1", "legacy", false, block)
		require.Error(t, err, "a block with an invalid transaction must not commit")
		require.False(t, errors.IsBlockCorrupt(err), "the sink bound the subtree list, so this is a judgement, not a corrupt record: %v", err)
		require.True(t, errors.Is(err, errors.ErrBlockInvalid), "the legacy caller's BlockRejected row keys on ErrBlockInvalid: %v", err)
		require.True(t, errors.Is(err, errors.ErrTxInvalid), "the cause must still be in the chain: %v", err)
		require.False(t, errors.IsTransientLocalError(err), "a judgement must not come back as retry-later: %v", err)

		us.subtreeValidation.AssertExpectations(t)
		require.Len(t, us.subtreeValidation.Calls, 1, "exactly one subtree validation call: the full route, never the quick route")

		requireNothingApplied(t, us, block, parent, child)
	})

	t.Run("p2p control keeps the corrupt verdict", func(t *testing.T) {
		us := newUnifiedRouteServer(t, "unified_route_p2p_tx_invalid")

		block, parent, child := unifiedRouteSpendingBlock(t, us, 0x15)

		us.subtreeValidation.On("CheckBlockSubtrees", mock.Anything, mock.Anything, "peer-1", "http://peer:8000").
			Return(txInvalid()).Once()

		err := us.s.blockValidation.ValidateBlockWithOptions(context.Background(), block, "http://peer:8000", &ValidateBlockOptions{PeerID: "peer-1"})
		require.Error(t, err)
		require.True(t, errors.IsBlockCorrupt(err), "on the p2p route the list is unbound, so the verdict stays corrupt: %v", err)
		require.False(t, errors.Is(err, errors.ErrBlockInvalid), "a corrupt verdict never carries the invalid code: %v", err)

		us.subtreeValidation.AssertExpectations(t)
		requireNothingApplied(t, us, block, parent, child)
	})
}
