package blockassembly

import (
	"context"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/require"
)

// finalityMTPLag is how far the parent's median time past sits below the candidate time in these
// tests: a time lock between the two is final by the header time and non-final by the median time
// past, which is the comparison bitcoin-sv makes from CSV onwards.
const finalityMTPLag = uint32(600)

// headerLookupCounter is a real blockchain client that counts the header reads the median-time-past
// lookup makes, so a test can assert that a lookup did or did not happen without mocking the client.
type headerLookupCounter struct {
	blockchain.ClientI
	lookups atomic.Int32
}

func (c *headerLookupCounter) GetBlockHeader(ctx context.Context, hash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	c.lookups.Add(1)

	return c.ClientI.GetBlockHeader(ctx, hash)
}

func (c *headerLookupCounter) GetBlockHeaders(ctx context.Context, hash *chainhash.Hash, n uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	c.lookups.Add(1)

	return c.ClientI.GetBlockHeaders(ctx, hash, n)
}

// coinbaseFinalityHarness is the coinbase-height harness over a real blockchain store: a
// sqlitememory store behind the ordinary LocalClient, holding a chain up to the candidate's parent.
// The server adds the mined block to that store, so a test asserts on what the store holds rather
// than on calls to a mocked client. The assembler reads the same store through headerLookupCounter,
// so a test can also tell whether the median-time-past lookup ran.
type coinbaseFinalityHarness struct {
	*coinbaseHeightHarness
	chain *headerLookupCounter
	store blockchain.ClientI
	tip   *model.BlockHeader
}

// newCoinbaseFinalityHarness is the coinbase-height harness with CSV active at the candidate height,
// so a time-based coinbase nLockTime is measured against the parent's median time past. The store
// holds a chain from genesis to the candidate's parent, whose last eleven blocks are stamped so that
// the parent's median time past is parentMTP. Regtest has BIP34 off and Genesis active from height
// 100, so the stored chain and the candidate agree on height and on the post-Genesis rules.
func newCoinbaseFinalityHarness(t *testing.T) *coinbaseFinalityHarness {
	t.Helper()

	params := chaincfg.RegressionNetParams
	params.CSVHeight = 1

	h := newCoinbaseHeightHarness(t, &params, postGenesisCandidateHeight, 5_000_000)

	storeURL, err := url.Parse("sqlitememory://")
	require.NoError(t, err)

	store, err := blockchainstore.NewStore(ulogger.TestLogger{}, storeURL, h.server.blockAssembler.settings)
	require.NoError(t, err)

	client, err := blockchain.NewLocalClient(ulogger.TestLogger{}, h.server.blockAssembler.settings, store, nil, nil)
	require.NoError(t, err)

	tip := buildChainToParent(t, client, postGenesisCandidateHeight-1, h.parentMTP()-5)
	h.candidate.PreviousHash = tip.Hash().CloneBytes()

	chain := &headerLookupCounter{ClientI: client}
	h.server.blockchainClient = client
	h.server.blockAssembler.blockchainClient = chain

	return &coinbaseFinalityHarness{coinbaseHeightHarness: h, chain: chain, store: client, tip: tip}
}

// buildChainToParent stores blocks from genesis up to height and returns the last one. The last
// eleven are stamped base..base+10, so the returned block's median time past is base+5; the ones
// before them carry base, which keeps the chain's timestamps non-decreasing.
func buildChainToParent(t *testing.T, client blockchain.ClientI, height uint32, base uint32) *model.BlockHeader {
	t.Helper()

	ctx := t.Context()

	prev, _, err := client.GetBestBlockHeader(ctx)
	require.NoError(t, err)

	coinbase, err := bt.NewTxFromString("02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff03510101ffffffff0100f2052a01000000232103656065e6886ca1e947de3471c9e723673ab6ba34724476417fa9fcef8bafa604ac00000000")
	require.NoError(t, err)

	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	for i := uint32(1); i <= height; i++ {
		timestamp := base
		if height-i < 11 {
			timestamp = base + 10 - (height - i)
		}

		header := &model.BlockHeader{
			Version:        0x20000000,
			HashPrevBlock:  prev.Hash(),
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      timestamp,
			Bits:           *nBits,
			Nonce:          i,
		}

		require.NoError(t, client.AddBlock(ctx, &model.Block{
			Header:           header,
			CoinbaseTx:       coinbase,
			TransactionCount: 1,
			Subtrees:         []*chainhash.Hash{},
		}, ""))

		prev = header
	}

	return prev
}

// requireMined asserts whether the server added a block on the candidate's parent to the store, and
// returns it when it did.
func (h *coinbaseFinalityHarness) requireMined(t *testing.T, want bool) *model.Block {
	t.Helper()

	best, _, err := h.store.GetBestBlockHeader(t.Context())
	require.NoError(t, err)

	if !want {
		require.Equal(t, h.tip.Hash(), best.Hash(), "no block may be added on the candidate's parent")

		return nil
	}

	require.Equal(t, h.tip.Hash(), best.HashPrevBlock, "a block must be added on the candidate's parent")

	block, err := h.store.GetBlock(t.Context(), best.Hash())
	require.NoError(t, err)

	return block
}

// parentMTP is the median time past the tests give the candidate's parent.
func (h *coinbaseHeightHarness) parentMTP() uint32 {
	return h.candidate.Time - finalityMTPLag
}

// warmMTP puts the parent's median time past in the assembler's memo, as a candidate built on that
// parent leaves it.
func (h *coinbaseHeightHarness) warmMTP(t *testing.T) {
	t.Helper()

	parent, err := chainhash.NewHash(h.candidate.PreviousHash)
	require.NoError(t, err)

	h.server.blockAssembler.placeMtpFloor(&mtpFloorEntry{parent: *parent, minTime: int64(h.parentMTP()) + 1})
}

// timeLockedCoinbase is a pool coinbase whose time-based nLockTime is enforced: its input sequence is
// not final.
func timeLockedCoinbase(t *testing.T, lockTime uint32) *bt.Tx {
	t.Helper()

	coinbase := coinbaseEncoding(t, postGenesisCandidateHeight, 5_000_000)
	coinbase.LockTime = lockTime
	coinbase.Inputs[0].SequenceNumber = 0

	return coinbase
}

// TestSubmitMiningSolution_CoinbaseMustBeFinal pins bitcoin-sv's IsFinalTx rule on the coinbase of a
// block this node mines (bitcoin-sv/teranode#4981). block.Valid on the submit path has no ancestor
// headers, so from CSV onwards it cannot evaluate a time-based nLockTime, and the block goes from the
// submit path straight to AddBlock. The submit path therefore fetches the parent's median time past
// itself, but only when the coinbase's verdict depends on it.
func TestSubmitMiningSolution_CoinbaseMustBeFinal(t *testing.T) {
	t.Run("a time lock at or after the parent's median time past is refused", func(t *testing.T) {
		h := newCoinbaseFinalityHarness(t)
		h.warmMTP(t)

		// Final by the header time, which is what block.Valid would fall back to without headers
		// before CSV, and non-final by the median time past bitcoin-sv uses after it.
		resp, err := h.submit(t, timeLockedCoinbase(t, h.parentMTP()))
		require.Error(t, err)
		require.Nil(t, resp)
		require.Contains(t, err.Error(), "bad-txns-nonfinal")
		h.requireMined(t, false)

		// The pool may submit again with a final coinbase, so the job stays and assembly is not
		// reset: nothing local is at fault.
		require.NotNil(t, h.server.jobStore.Get(h.id), "the job must survive a refused coinbase")
		h.requireNoReset(t)
	})

	t.Run("a time lock before the parent's median time past is accepted", func(t *testing.T) {
		h := newCoinbaseFinalityHarness(t)
		h.warmMTP(t)

		resp, err := h.submit(t, timeLockedCoinbase(t, h.parentMTP()-1))
		require.NoError(t, err)
		require.True(t, resp.Ok)
		require.Equal(t, h.parentMTP()-1, h.requireMined(t, true).CoinbaseTx.LockTime)
		require.Zero(t, h.chain.lookups.Load(), "no median-time-past lookup was needed")
	})

	t.Run("a cold memo is read from the blockchain service", func(t *testing.T) {
		h := newCoinbaseFinalityHarness(t)

		resp, err := h.submit(t, timeLockedCoinbase(t, h.parentMTP()))
		require.Error(t, err)
		require.Nil(t, resp)
		require.Contains(t, err.Error(), "bad-txns-nonfinal")
		h.requireMined(t, false)
		require.NotZero(t, h.chain.lookups.Load(), "a cold memo must be read from the blockchain service")

		resp, err = h.submit(t, timeLockedCoinbase(t, h.parentMTP()-1))
		require.NoError(t, err)
		require.True(t, resp.Ok)
		h.requireMined(t, true)
	})

	t.Run("an unknown median time past refuses the solution", func(t *testing.T) {
		// A parent the store does not hold, so the lookup fails as it would against an unreachable
		// or out-of-step blockchain service.
		h := newCoinbaseFinalityHarness(t)
		h.candidate.PreviousHash = (&chainhash.Hash{9}).CloneBytes()

		resp, err := h.submit(t, timeLockedCoinbase(t, h.parentMTP()-1))
		require.Error(t, err)
		require.Nil(t, resp)
		require.Contains(t, err.Error(), "cannot check that the coinbase is final")
		h.requireMined(t, false)
		h.requireNoReset(t)
	})

	t.Run("a coinbase whose verdict does not depend on the median time past needs no lookup", func(t *testing.T) {
		h := newCoinbaseFinalityHarness(t)

		coinbase := timeLockedCoinbase(t, h.candidate.Time)
		coinbase.Inputs[0].SequenceNumber = bt.DefaultSequenceNumber

		resp, err := h.submit(t, coinbase)
		require.NoError(t, err)
		require.True(t, resp.Ok)
		h.requireMined(t, true)
		require.Zero(t, h.chain.lookups.Load(), "no median-time-past lookup was needed")
	})

	t.Run("a height lock is refused without a lookup", func(t *testing.T) {
		h := newCoinbaseFinalityHarness(t)

		resp, err := h.submit(t, timeLockedCoinbase(t, postGenesisCandidateHeight))
		require.Error(t, err)
		require.Nil(t, resp)
		require.Contains(t, err.Error(), "bad-txns-nonfinal")
		h.requireMined(t, false)
		require.Zero(t, h.chain.lookups.Load(), "no median-time-past lookup was needed")
	})
}

// TestParentMedianTimePast_ColdLookupLeavesTheFloorAlone pins that the submit path's lookup is not a
// candidate's: a miss reads the store without filling the floor memo, so it cannot evict an entry a
// candidate path relies on, and a failure is returned to the caller without spending the floor's
// once-per-parent log latch.
func TestParentMedianTimePast_ColdLookupLeavesTheFloorAlone(t *testing.T) {
	h := newCoinbaseFinalityHarness(t)
	ba := h.server.blockAssembler

	// Fill both memo slots with other parents, as the two candidate paths leave them.
	others := []*mtpFloorEntry{{parent: chainhash.Hash{1}, minTime: 1}, {parent: chainhash.Hash{2}, minTime: 2}}
	for _, entry := range others {
		ba.placeMtpFloor(entry)
	}

	mtp, err := ba.ParentMedianTimePast(t.Context(), h.tip.Hash())
	require.NoError(t, err)
	require.Equal(t, h.parentMTP(), mtp)
	require.NotZero(t, h.chain.lookups.Load(), "a cold memo must be read from the blockchain service")

	require.Nil(t, ba.memoizedMtpFloor(h.tip.Hash()), "a submit lookup must not fill the floor memo")

	for _, entry := range others {
		require.Same(t, entry, ba.memoizedMtpFloor(&entry.parent), "a submit lookup must not evict a candidate path's entry")
	}

	_, err = ba.ParentMedianTimePast(t.Context(), &chainhash.Hash{9})
	require.Error(t, err)
	require.Nil(t, ba.mtpFloorFailureLoggedTip.Load(), "a submit lookup failure must not spend the floor's log latch")
}

// TestSubmitMiningSolution_RecreatedCoinbaseIsFinal pins the coinbase block assembly builds when a
// solution omits one. The solution's time and nonce used to be copied into that coinbase as its
// nLockTime and input sequence, which made it non-final for any non-zero nonce
// (bitcoin-sv/teranode#4981). They belong in the header only.
func TestSubmitMiningSolution_RecreatedCoinbaseIsFinal(t *testing.T) {
	h := newCoinbaseFinalityHarness(t)
	h.warmMTP(t)

	nTime := h.candidate.Time

	// The recreated coinbase carries a fresh random extranonce, so the merkle root, and with it
	// whether a given nonce meets the target, cannot be known in advance. Regtest's target takes
	// about half of all hashes, so resubmit until one does; a miss is a proof-of-work rejection,
	// which deletes the job and resets assembly, so put the job back each time.
	//
	// This loop exists only because of that random rebuild, which is also why a real miner can
	// never land a coinbase-less solution. It becomes a single submit once the job keeps the
	// candidate's coinbase and reuses it, as bitcoin-sv does; that change is a separate PR.
	var (
		resp *blockassembly_api.OKResponse
		err  error
	)

	for attempt := 0; attempt < 64; attempt++ {
		h.server.jobStore.Set(h.id, h.job, time.Minute)

		resp, err = h.server.submitMiningSolution(context.Background(), &BlockSubmissionRequest{
			SubmitMiningSolutionRequest: &blockassembly_api.SubmitMiningSolutionRequest{
				Id:    h.idBytes,
				Nonce: 7,
				Time:  &nTime,
			},
		})
		if err == nil {
			break
		}

		require.Contains(t, err.Error(), "target difficulty", "the only acceptable failure is a nonce that misses the target")

		select {
		case <-h.resets:
		case <-time.After(time.Second):
		}
	}

	require.NoError(t, err)
	require.True(t, resp.Ok)

	block := h.requireMined(t, true)
	require.Equal(t, uint32(7), block.Header.Nonce, "the nonce belongs in the header")
	require.Equal(t, nTime, block.Header.Timestamp, "the time belongs in the header")
	require.Zero(t, block.CoinbaseTx.LockTime, "the time must not be copied into the coinbase nLockTime")
	require.Equal(t, bt.DefaultSequenceNumber, block.CoinbaseTx.Inputs[0].SequenceNumber, "the nonce must not be copied into the coinbase sequence")
	require.NoError(t, util.IsTransactionFinal(block.CoinbaseTx, block.Height, h.parentMTP()))
}
