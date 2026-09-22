package blockassembly

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/util/testutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// postGenesisCandidateHeight is a height at which Genesis is active on regtest (activation is 100),
// so the rules bitcoin-sv applies only before Genesis must not apply to a candidate issued here.
const postGenesisCandidateHeight = uint32(150)

// payoutAddress is a testnet P2PKH address; AddressToScript accepts it regardless of the configured
// network and it produces an ordinary (non-P2SH) coinbase output, so the submission's own P2SH guard
// stays out of the way of what these tests are pinning.
const payoutAddress = "n2ZNV88uQbede7C5M5jzi6SyG4GVVr5JTt"

// coinbaseHeightHarness is a server whose submit path can run all the way to AddBlock: a mocked
// blockchain client that accepts the block, an assembler carrying settings and a best block that
// keeps the candidate non-stale, and a job the test controls.
type coinbaseHeightHarness struct {
	server    *BlockAssembly
	blockchai *blockchain.Mock
	candidate *model.MiningCandidate
	job       *subtreeprocessor.Job
	id        chainhash.Hash
	idBytes   []byte
	nBits     model.NBit
	// resets receives every reset request the server sends to the assembler, so a test can
	// assert whether a rejected submission reset block assembly.
	resets chan resetRequest
}

// newCoinbaseHeightHarness wires that server for a candidate at candidateHeight. params selects the
// consensus rules; pass chaincfg.RegressionNetParams for the Genesis boundary, or a copy with
// BIP0034Height lowered to reach the coinbase-height check.
func newCoinbaseHeightHarness(t *testing.T, params *chaincfg.Params, candidateHeight uint32, coinbaseValue uint64) *coinbaseHeightHarness {
	t.Helper()

	common := testutil.NewCommonTestSetup(t)

	tSettings := *common.Settings
	tSettings.ChainCfgParams = params

	blockchainMock := &blockchain.Mock{}
	blockchainMock.On("AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	blockchainMock.On("SetBlockSubtreesSet", mock.Anything, mock.Anything).Return(nil)

	s := New(common.Logger, &tSettings, nil, nil, nil, blockchainMock)

	// Otherwise a zero value (not loading, non-nil), but with a live reset channel. Reset sends on
	// resetCh from a goroutine, so on a nil channel that goroutine would block forever; the recorder
	// below drains it, answers so the sender finishes, and hands each request to the test.
	resetCh := make(chan resetRequest, 2)
	resets := make(chan resetRequest, 2)
	s.blockAssembler = &BlockAssembler{resetCh: resetCh, logger: common.Logger}
	s.blockAssembler.settings = &tSettings

	done := make(chan struct{})

	go func() {
		for {
			select {
			case req := <-resetCh:
				resets <- req

				if req.ErrCh != nil {
					req.ErrCh <- nil
				}
			case <-done:
				return
			}
		}
	}()

	t.Cleanup(func() {
		close(done)

		_ = s.Stop(context.Background())
	})

	// A best block whose parent differs from the job's, so the candidate is not stale.
	s.blockAssembler.bestBlock.Store(&BestBlockInfo{
		Header: &model.BlockHeader{HashPrevBlock: &chainhash.Hash{7}},
		Height: candidateHeight - 1,
	})

	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	idBytes := make([]byte, 32)
	idBytes[0] = 0x01

	id, err := chainhash.NewHash(idBytes)
	require.NoError(t, err)

	parent := &chainhash.Hash{9}

	candidate := &model.MiningCandidate{
		Id:            idBytes,
		PreviousHash:  parent[:],
		Version:       miningCandidateVersion,
		NBits:         nBits.CloneBytes(),
		Time:          uint32(time.Now().Unix()),
		Height:        candidateHeight,
		CoinbaseValue: coinbaseValue,
	}

	job := &subtreeprocessor.Job{ID: id, MiningCandidate: candidate}
	s.jobStore.Set(*id, job, time.Minute)

	return &coinbaseHeightHarness{
		server:    s,
		blockchai: blockchainMock,
		candidate: candidate,
		job:       job,
		id:        *id,
		idBytes:   idBytes,
		nBits:     *nBits,
		resets:    resets,
	}
}

// setTip moves the assembler's best block to height, keeping its parent different from the job's
// so the candidate is still not stale. It models a chain that has moved on since the candidate was
// issued.
func (h *coinbaseHeightHarness) setTip(height uint32) {
	h.server.blockAssembler.bestBlock.Store(&BestBlockInfo{
		Header: &model.BlockHeader{HashPrevBlock: &chainhash.Hash{7}},
		Height: height,
	})
}

// requireReset waits for the server to reset block assembly.
func (h *coinbaseHeightHarness) requireReset(t *testing.T) {
	t.Helper()

	select {
	case <-h.resets:
	case <-time.After(time.Second):
		require.FailNow(t, "block assembly was not reset")
	}
}

// requireNoReset asserts no reset request arrives. Reset sends from a goroutine, so this waits a
// short while rather than checking once.
func (h *coinbaseHeightHarness) requireNoReset(t *testing.T) {
	t.Helper()

	select {
	case req := <-h.resets:
		require.FailNow(t, "block assembly was reset", "reset request: %+v", req)
	case <-time.After(250 * time.Millisecond):
	}
}

// addedBlock returns the block the server handed to AddBlock.
func (h *coinbaseHeightHarness) addedBlock(t *testing.T) *model.Block {
	t.Helper()

	for _, call := range h.blockchai.Calls {
		if call.Method == "AddBlock" {
			block, ok := call.Arguments.Get(1).(*model.Block)
			require.True(t, ok)

			return block
		}
	}

	require.FailNow(t, "AddBlock was not called")

	return nil
}

// jobSubtree builds a job subtree the way the subtree processor does: the coinbase placeholder
// first, then the given transaction hashes, each with no fee.
func jobSubtree(t *testing.T, txs ...chainhash.Hash) *subtreepkg.Subtree {
	t.Helper()

	st, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())

	for _, tx := range txs {
		require.NoError(t, st.AddNode(tx, 0, 0))
	}

	return st
}

// submit mines the nonce for the header the server will build from this coinbase, then submits the
// solution the way a pool does, with its own coinbase bytes.
func (h *coinbaseHeightHarness) submit(t *testing.T, coinbase *bt.Tx) (*blockassembly_api.OKResponse, error) {
	t.Helper()

	// The pool branch uses the supplied coinbase unaltered. With no subtrees the block's merkle
	// root is the coinbase txid; with subtrees it is computed the way submitMiningSolution does it,
	// with the coinbase in place of the first subtree's placeholder. Either way this is exactly
	// the header the server will build.
	prev, err := chainhash.NewHash(h.candidate.PreviousHash)
	require.NoError(t, err)

	merkleRoot := coinbase.TxIDChainHash()

	if len(h.job.Subtrees) > 0 {
		subtrees := make([]*subtreepkg.Subtree, len(h.job.Subtrees))
		hashes := make([]chainhash.Hash, len(h.job.Subtrees))

		for i, st := range h.job.Subtrees {
			subtrees[i] = st
			if i == 0 {
				subtrees[i] = st.Duplicate()
				subtrees[i].ReplaceRootNode(coinbase.TxIDChainHash(), 0, uint64(coinbase.Size()))
			}

			hashes[i] = *subtrees[i].RootHash()
		}

		merkleRoot, err = h.server.createMerkleTreeFromSubtrees("test", subtrees, hashes, coinbase.TxIDChainHash())
		require.NoError(t, err)
	}

	header := &model.BlockHeader{
		Version:        h.candidate.Version,
		HashPrevBlock:  prev,
		HashMerkleRoot: merkleRoot,
		Timestamp:      h.candidate.Time,
		Bits:           h.nBits,
		Nonce:          0,
	}

	// HasMetTargetDifficulty reports a miss as an error, so the nonce search ignores it and keeps
	// rolling. Regtest's target takes roughly half of all hashes, so this ends almost immediately.
	for {
		met, _, _ := header.HasMetTargetDifficulty()
		if met {
			break
		}

		header.Nonce++
	}

	return h.server.submitMiningSolution(context.Background(), &BlockSubmissionRequest{
		SubmitMiningSolutionRequest: &blockassembly_api.SubmitMiningSolutionRequest{
			Id:         h.idBytes,
			CoinbaseTx: coinbase.Bytes(),
			Nonce:      header.Nonce,
		},
	})
}

// coinbaseEncoding builds a coinbase that encodes height in its scriptSig and pays value to a
// standard address, the shape a pool submits.
func coinbaseEncoding(t *testing.T, height uint32, value uint64) *bt.Tx {
	t.Helper()

	coinbase, err := model.CreateCoinbase(height, value, "t", []string{payoutAddress})
	require.NoError(t, err)

	return coinbase
}

// withSigOps replaces the coinbase's first output with one whose locking script carries count
// OP_CHECKSIG opcodes, and pays nothing, so only the sigop count distinguishes it.
func withSigOps(t *testing.T, coinbase *bt.Tx, count int) *bt.Tx {
	t.Helper()

	script := make(bscript.Script, count)
	for i := range script {
		script[i] = bscript.OpCHECKSIG
	}

	coinbase.Outputs = []*bt.Output{{Satoshis: 0, LockingScript: &script}}

	return coinbase
}

// TestSubmitMiningSolution_CoinbaseRulesUseCandidateHeight pins the height the block-assembly
// submission path hands to Block.Valid.
//
// The coinbase rules from bitcoin-sv's CheckTransactionCommon read the block height to decide
// whether Genesis is active: before Genesis a coinbase is capped at 1MB and 20,000 sigops, after it
// neither limit exists. submitMiningSolution built its model.Block without a Height, so every
// submission was judged at height 0, which is below every network's Genesis activation and therefore
// took the pre-Genesis branch. That rejects blocks bitcoin-sv accepts, and the handler treats the
// rejection as a subtree-processor fault: it resets block assembly and deletes the job, throwing
// away a block that cost real proof-of-work and that every peer would have taken.
//
// Both cases assert the end state rather than the call: the block reaches AddBlock, or it does not.
func TestSubmitMiningSolution_CoinbaseRulesUseCandidateHeight(t *testing.T) {
	t.Run("a post-Genesis candidate is not judged by the pre-Genesis sigop limit", func(t *testing.T) {
		params := chaincfg.RegressionNetParams
		h := newCoinbaseHeightHarness(t, &params, postGenesisCandidateHeight, 5_000_000)

		// 20,001 sigops: over the pre-Genesis limit of 20,000, unlimited after Genesis. The
		// candidate is issued at height 150, where regtest has been past Genesis for 50 blocks.
		coinbase := withSigOps(t, coinbaseEncoding(t, postGenesisCandidateHeight, 5_000_000), 20_001)

		resp, err := h.submit(t, coinbase)
		require.NoError(t, err, "a coinbase legal at the candidate's own height must not be rejected")
		require.True(t, resp.Ok)
		h.blockchai.AssertCalled(t, "AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
		h.requireNoReset(t)
	})

	t.Run("the coinbase height check sees the candidate height, not zero", func(t *testing.T) {
		// The opposite direction, and the reason a non-zero height is not enough: BIP34 requires the
		// coinbase to encode the block's own height. At height 0 that check is skipped entirely, so
		// this rejection can only happen if the real candidate height reached Block.Valid.
		params := chaincfg.RegressionNetParams
		params.BIP0034Height = 1

		h := newCoinbaseHeightHarness(t, &params, postGenesisCandidateHeight, 5_000_000)

		coinbase := coinbaseEncoding(t, postGenesisCandidateHeight-1, 5_000_000)

		resp, err := h.submit(t, coinbase)
		require.Error(t, err)
		require.Nil(t, resp)
		require.Contains(t, err.Error(), "block height in coinbase tx")
		h.blockchai.AssertNotCalled(t, "AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything)

		// The wrong height is the pool's, not assembly's: the job is dropped but assembly is not
		// reset, since a reset would not stop the same pool doing it again.
		require.Nil(t, h.server.jobStore.Get(h.id), "the rejected job must be deleted")
		h.requireNoReset(t)
	})
}

// TestSubmitMiningSolution_InvalidBlockResetOnlyForLocalFaults pins what submitMiningSolution does
// with a block that fails Block.Valid. A consensus verdict on data the submitter chose
// (ErrBlockInvalid) says nothing about local assembly state, so it drops the job without a reset.
// Any other failure points at the block this node assembled, so it still resets.
func TestSubmitMiningSolution_InvalidBlockResetOnlyForLocalFaults(t *testing.T) {
	t.Run("a coinbase that overpays is rejected without a reset", func(t *testing.T) {
		params := chaincfg.RegressionNetParams
		h := newCoinbaseHeightHarness(t, &params, postGenesisCandidateHeight, 5_000_000)

		// Regtest halves every 150 blocks, so the subsidy at height 150 is 25 BSV. Paying one
		// satoshi more, with no fees in the block, breaks the reward rule.
		coinbase := coinbaseEncoding(t, postGenesisCandidateHeight, 25*100_000_000+1)

		resp, err := h.submit(t, coinbase)
		require.Error(t, err)
		require.Nil(t, resp)
		require.Contains(t, err.Error(), "is greater than the fees + block subsidy")
		h.blockchai.AssertNotCalled(t, "AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
		require.Nil(t, h.server.jobStore.Get(h.id), "the rejected job must be deleted")
		h.requireNoReset(t)
	})

	t.Run("a block assembly built with a duplicate transaction still resets", func(t *testing.T) {
		params := chaincfg.RegressionNetParams
		h := newCoinbaseHeightHarness(t, &params, postGenesisCandidateHeight, 5_000_000)

		dup := chainhash.HashH([]byte("duplicate"))
		h.job.Subtrees = []*subtreepkg.Subtree{jobSubtree(t, dup, dup)}

		resp, err := h.submit(t, coinbaseEncoding(t, postGenesisCandidateHeight, 5_000_000))
		require.Error(t, err)
		require.Nil(t, resp)
		require.Contains(t, err.Error(), "duplicate transaction")
		h.blockchai.AssertNotCalled(t, "AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
		require.Nil(t, h.server.jobStore.Get(h.id), "the job must be deleted")
		h.requireReset(t)
	})
}

// TestSubmitMiningSolution_CoinbaseBUMPUsesCandidateHeight pins the height written into the coinbase
// BUMP. The block is at the candidate's height, so its proof must be too, even when the chain tip
// has moved several blocks past the candidate's parent since the candidate was issued.
func TestSubmitMiningSolution_CoinbaseBUMPUsesCandidateHeight(t *testing.T) {
	params := chaincfg.RegressionNetParams
	h := newCoinbaseHeightHarness(t, &params, postGenesisCandidateHeight, 5_000_000)

	h.job.Subtrees = []*subtreepkg.Subtree{jobSubtree(t, chainhash.HashH([]byte("tx1")))}
	h.setTip(postGenesisCandidateHeight + 5)

	resp, err := h.submit(t, coinbaseEncoding(t, postGenesisCandidateHeight, 5_000_000))
	require.NoError(t, err)
	require.True(t, resp.Ok)

	block := h.addedBlock(t)
	require.Equal(t, postGenesisCandidateHeight, block.Height)
	require.NotEmpty(t, block.CoinbaseBUMP)

	// A BRC-74 BUMP opens with the block height as a varint.
	bumpHeight, _ := bt.NewVarIntFromBytes(block.CoinbaseBUMP)
	require.Equal(t, uint64(postGenesisCandidateHeight), uint64(bumpHeight), "the BUMP must carry the block's own height")
}
