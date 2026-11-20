package blockvalidation

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// These tests cover catch-up quick validation racing another route that validates the same
// block. Seen on a test-network node syncing from genesis with legacy sync and P2P catch-up both
// active: legacy committed a block while catch-up was still quick-validating it, trusting the
// .subtree quick validation had already written although the block's transactions were still
// being created. Quick validation then failed locally and its cleanup deleted that .subtree, the
// fall-back stopped at "tried to validate existing valid block", and setTxMined could never mark
// the block mined.

func catchupContextForQuickValidation(block *model.Block) *CatchupContext {
	return &CatchupContext{
		blockUpTo:               block,
		baseURL:                 "http://peer",
		peerID:                  "peer",
		startTime:               time.Now(),
		useQuickValidation:      true,
		highestCheckpointHeight: 1000,
	}
}

func catchupValidateOptions() *ValidateBlockOptions {
	return &ValidateBlockOptions{IsCatchupMode: true, DisableOptimisticMining: true, PeerID: "peer"}
}

type catchupStepResult struct {
	tryNormal bool
	normalErr error
	err       error
}

// TestValidateCatchupBlock_LegacyValidationWaitsForInFlightQuickValidation pins the guard: a
// ValidateBlockWithOptions call for a block that catch-up is quick-validating waits for catch-up's
// whole step, and takes its result, instead of validating the block alongside it.
//
// The legacy call's existence lookup answers "stored", so without the guard it would return at
// once with "tried to validate existing valid block", while catch-up's commit is still held.
//
// Mutation proof: calling tryQuickValidation and validateBlockWithOptionsHeld in
// validateCatchupBlock directly rather than under runOncePerBlock lets the legacy call return
// while the commit is held, which fails the first select below.
func TestValidateCatchupBlock_LegacyValidationWaitsForInFlightQuickValidation(t *testing.T) {
	suite := NewCatchupTestSuite(t)
	defer suite.Cleanup()

	addBlockEntered := make(chan struct{})
	release := make(chan struct{})

	// Registered before setupQuickValidateMocks so these are the expectations testify matches.
	suite.MockBlockchain.On("AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Run(func(mock.Arguments) {
			close(addBlockEntered)
			<-release
		}).
		Return(nil).Once()
	suite.MockBlockchain.On("GetBlockExists", mock.Anything, mock.Anything).Return(true, nil).Maybe()
	suite.MockBlockchain.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(&model.BlockHeader{}, &model.BlockHeaderMeta{}, nil).Maybe()
	setupQuickValidateMocks(suite)

	block := buildOneSubtreeBlock(t, suite, 100)

	writeJobsChan := make(chan *SubtreeWriteJob, 16)
	go func() { _ = suite.Server.blockValidation.subtreeWriteWorker(suite.Ctx, writeJobsChan) }()

	catchupCh := make(chan catchupStepResult, 1)
	go func() {
		tryNormal, normalErr, err := suite.Server.validateCatchupBlock(suite.Ctx, block, catchupContextForQuickValidation(block), "peer", "http://peer", writeJobsChan, nil, catchupValidateOptions(), false)
		catchupCh <- catchupStepResult{tryNormal, normalErr, err}
	}()

	select {
	case <-addBlockEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("catch-up quick validation never reached its commit")
	}

	legacyCh := make(chan error, 1)
	go func() {
		legacyCh <- suite.Server.blockValidation.ValidateBlockWithOptions(suite.Ctx, block, "legacy", &ValidateBlockOptions{PeerID: "legacy"})
	}()

	select {
	case err := <-legacyCh:
		t.Fatalf("legacy validation returned (%v) while catch-up was still quick-validating the same block", err)
	case <-time.After(200 * time.Millisecond):
	}

	close(release)

	select {
	case res := <-catchupCh:
		require.NoError(t, res.err)
		require.NoError(t, res.normalErr)
		require.False(t, res.tryNormal, "quick validation succeeded, so standard validation must not have run")
	case <-time.After(5 * time.Second):
		t.Fatal("catch-up step did not return after its commit was released")
	}

	select {
	case err := <-legacyCh:
		require.NoError(t, err, "legacy takes catch-up's result for the block")
	case <-time.After(5 * time.Second):
		t.Fatal("legacy validation did not return after catch-up's step finished")
	}

	suite.MockBlockchain.AssertNumberOfCalls(t, "AddBlock", 1)
}

// TestValidateCatchupBlock_WaitsForValidationAlreadyInFlight pins the other direction: when another
// route is already validating the block, catch-up waits and takes that result instead of
// quick-validating the block alongside it.
//
// Mutation proof: the same mutation as above lets catch-up quick-validate and commit the block
// while the other route still holds it, so AddBlock is called and the first select fails.
func TestValidateCatchupBlock_WaitsForValidationAlreadyInFlight(t *testing.T) {
	suite := NewCatchupTestSuite(t)
	defer suite.Cleanup()

	// All optional: with the guard in place quick validation never runs here.
	setupQuickValidateMocksBodyRejected(suite)

	block := buildOneSubtreeBlock(t, suite, 100)

	otherEntered := make(chan struct{})
	release := make(chan struct{})
	otherCh := make(chan error, 1)

	go func() {
		otherCh <- suite.Server.blockValidation.runOncePerBlock(block.Hash(), nil, func(*ValidateBlockOptions) error {
			close(otherEntered)
			<-release

			return suite.Server.blockValidation.SetBlockExists(block.Hash())
		})
	}()

	select {
	case <-otherEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("the other route never started validating")
	}

	writeJobsChan := make(chan *SubtreeWriteJob, 16)
	go func() { _ = suite.Server.blockValidation.subtreeWriteWorker(suite.Ctx, writeJobsChan) }()

	catchupCh := make(chan catchupStepResult, 1)
	go func() {
		tryNormal, normalErr, err := suite.Server.validateCatchupBlock(suite.Ctx, block, catchupContextForQuickValidation(block), "peer", "http://peer", writeJobsChan, nil, catchupValidateOptions(), false)
		catchupCh <- catchupStepResult{tryNormal, normalErr, err}
	}()

	select {
	case res := <-catchupCh:
		t.Fatalf("catch-up returned (%+v) while another route was still validating the same block", res)
	case <-time.After(200 * time.Millisecond):
	}

	suite.MockBlockchain.AssertNotCalled(t, "AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything)

	close(release)
	require.NoError(t, <-otherCh)

	select {
	case res := <-catchupCh:
		require.NoError(t, res.err)
		require.NoError(t, res.normalErr, "catch-up takes the other route's result as its standard-validation result")
		require.True(t, res.tryNormal)
	case <-time.After(5 * time.Second):
		t.Fatal("catch-up did not return after the other route finished")
	}

	suite.MockBlockchain.AssertNotCalled(t, "AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

// blockingBanScoreRecorder holds the first AddBanScore call until release is closed, so a test can
// keep quick validation's corrupt branch in flight after it has reached its verdict.
type blockingBanScoreRecorder struct {
	P2PClientI

	entered chan struct{}
	release chan struct{}
}

func (b *blockingBanScoreRecorder) AddBanScore(context.Context, string, string) error {
	close(b.entered)
	<-b.release

	return nil
}

// TestValidateCatchupBlock_WaiterGetsNoVerdictOnCatchupsBody pins what a route waiting on catch-up's
// step receives when quick validation fails without falling back: a retry-later ServiceError, not
// the corrupt verdict on the catch-up peer's body. The waiter holds its own copy of the block, so
// taking that verdict would charge its own peer and, on the legacy route, drop an honest download.
//
// Mutation proof: returning tryQuickValidation's error from the runOncePerBlock closure, instead of
// the ServiceError, hands the legacy call the corrupt verdict and fails the IsBlockCorrupt check.
func TestValidateCatchupBlock_WaiterGetsNoVerdictOnCatchupsBody(t *testing.T) {
	suite := NewCatchupTestSuite(t)
	defer suite.Cleanup()
	setupQuickValidateMocksBodyRejected(suite)

	strike := &blockingBanScoreRecorder{entered: make(chan struct{}), release: make(chan struct{})}
	suite.Server.blockValidation.p2pClient = strike

	block := buildOneSubtreeBlock(t, suite, 100)
	// A body whose subtrees do not combine to the header's merkle root: quick validation's verdict
	// is corrupt, and it strikes the catch-up peer before returning it.
	block.Header.HashMerkleRoot = &chainhash.Hash{}

	writeJobsChan := make(chan *SubtreeWriteJob, 16)
	go func() { _ = suite.Server.blockValidation.subtreeWriteWorker(suite.Ctx, writeJobsChan) }()

	catchupCh := make(chan catchupStepResult, 1)
	go func() {
		tryNormal, normalErr, err := suite.Server.validateCatchupBlock(suite.Ctx, block, catchupContextForQuickValidation(block), "peer", "http://peer", writeJobsChan, nil, catchupValidateOptions(), false)
		catchupCh <- catchupStepResult{tryNormal, normalErr, err}
	}()

	select {
	case <-strike.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("catch-up quick validation never reached its corrupt verdict")
	}

	legacyCh := make(chan error, 1)
	go func() {
		legacyCh <- suite.Server.blockValidation.ValidateBlockWithOptions(suite.Ctx, block, "legacy", &ValidateBlockOptions{PeerID: "legacy"})
	}()

	// Let the legacy call reach the entry and wait on it before catch-up finishes.
	select {
	case err := <-legacyCh:
		t.Fatalf("legacy validation returned (%v) while catch-up was still quick-validating the same block", err)
	case <-time.After(200 * time.Millisecond):
	}

	close(strike.release)

	select {
	case res := <-catchupCh:
		require.True(t, errors.IsBlockCorrupt(res.err), "catch-up itself still sees the corrupt verdict, got: %v", res.err)
	case <-time.After(5 * time.Second):
		t.Fatal("catch-up step did not return")
	}

	select {
	case err := <-legacyCh:
		require.Error(t, err)
		require.False(t, errors.IsBlockCorrupt(err), "a waiter must not take the corrupt verdict on catch-up's body, got: %v", err)
		require.True(t, errors.Is(err, errors.ErrServiceError), "a waiter is told to retry, got: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("legacy validation did not return after catch-up's step finished")
	}
}

// TestTryQuickValidation_LocalFault_KeepsSubtreeOfBlockStoredMeanwhile reproduces the incident's
// interleaving for a route that commits without the runOncePerBlock entry: quick validation is in
// flight, another route stores the block, then quick validation fails locally. The .subtree the
// stored block depends on must survive, so setTxMined can read it.
//
// Mutation proof: deleting the existence check before removeCatchupSubtreeFiles in the local-fault
// branch deletes the freshly written .subtree, which fails the final Exists assertion.
func TestTryQuickValidation_LocalFault_KeepsSubtreeOfBlockStoredMeanwhile(t *testing.T) {
	suite := NewCatchupTestSuite(t)
	defer suite.Cleanup()

	suite.MockBlockchain.On("AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(errors.NewProcessingError("simulated local commit failure"))
	setupQuickValidateMocks(suite)

	block := buildOneSubtreeBlock(t, suite, 100)
	subtreeHash := block.Subtrees[0]

	relayChan := make(chan *SubtreeWriteJob, 16)
	workerChan := make(chan *SubtreeWriteJob, 16)
	release := make(chan struct{})

	go gatedWriteJobRelay(relayChan, workerChan, release)
	go func() { _ = suite.Server.blockValidation.subtreeWriteWorker(suite.Ctx, workerChan) }()

	type result struct {
		tryNormal bool
		err       error
	}
	resultCh := make(chan result, 1)
	go func() {
		tryNormal, err := suite.Server.tryQuickValidation(suite.Ctx, block, catchupContextForQuickValidation(block), "peer", "http://peer", relayChan, nil)
		resultCh <- result{tryNormal, err}
	}()

	select {
	case <-resultCh:
		t.Fatal("tryQuickValidation returned before its .subtree write was released")
	case <-time.After(200 * time.Millisecond):
	}

	// Another route stores the block while quick validation is still in flight.
	require.NoError(t, suite.Server.blockValidation.SetBlockExists(block.Hash()))

	close(release)

	var res result
	select {
	case res = <-resultCh:
	case <-time.After(5 * time.Second):
		t.Fatal("tryQuickValidation did not return after its .subtree write was released")
	}

	require.NoError(t, res.err)
	require.True(t, res.tryNormal)

	subtreeBytes, err := suite.Server.subtreeStore.Get(suite.Ctx, subtreeHash[:], fileformat.FileTypeSubtree)
	require.NoError(t, err, "the .subtree of a block another route stored must not be removed")

	subtree, err := subtreepkg.NewSubtreeFromBytes(subtreeBytes)
	require.NoError(t, err)
	require.Equal(t, *subtreeHash, *subtree.RootHash())
}
