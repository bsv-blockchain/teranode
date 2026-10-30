package blockassembly

import (
	"context"
	"net/url"
	"sync"
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
	"github.com/bsv-blockchain/teranode/util/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// candidateCoinbaseHarness is a block assembly server over a real blockchain store: a sqlitememory
// store behind the ordinary LocalClient, holding a short regtest chain the candidate builds on. The
// server adds a mined block to that store, so a test reads back what was actually mined.
type candidateCoinbaseHarness struct {
	server    *BlockAssembly
	store     blockchain.ClientI
	tip       *model.BlockHeader
	candidate *model.MiningCandidate
	id        *chainhash.Hash
}

func newCandidateCoinbaseHarness(t *testing.T) *candidateCoinbaseHarness {
	t.Helper()

	common := testutil.NewCommonTestSetup(t)

	tSettings := *common.Settings
	params := chaincfg.RegressionNetParams
	tSettings.ChainCfgParams = &params

	storeURL, err := url.Parse("sqlitememory://")
	require.NoError(t, err)

	store, err := blockchainstore.NewStore(ulogger.TestLogger{}, storeURL, &tSettings)
	require.NoError(t, err)

	client, err := blockchain.NewLocalClient(ulogger.TestLogger{}, &tSettings, store, nil, nil)
	require.NoError(t, err)

	tip := buildFutureChain(t, t.Context(), client, uint32(time.Now().Add(-time.Hour).Unix())) //nolint:gosec

	_, tipMeta, err := client.GetBlockHeader(t.Context(), tip.Hash())
	require.NoError(t, err)

	s := New(common.Logger, &tSettings, nil, nil, nil, client)
	s.blockAssembler = &BlockAssembler{settings: &tSettings, logger: common.Logger, resetCh: make(chan resetRequest, 4)}

	t.Cleanup(func() { _ = s.Stop(context.Background()) })

	// A best block whose parent differs from the candidate's, so the candidate is not stale.
	s.blockAssembler.bestBlock.Store(&BestBlockInfo{
		Header: &model.BlockHeader{HashPrevBlock: &chainhash.Hash{7}},
		Height: tipMeta.Height,
	})

	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	height := tipMeta.Height + 1

	candidate := &model.MiningCandidate{
		Id:            chainhash.HashB(tip.Hash()[:]),
		PreviousHash:  tip.Hash().CloneBytes(),
		CoinbaseValue: util.GetBlockSubsidyForHeight(height, &params),
		Version:       miningCandidateVersion,
		NBits:         nBits.CloneBytes(),
		Time:          tip.Timestamp + 1,
		Height:        height,
		MerkleProof:   [][]byte{},
	}

	id, err := chainhash.NewHash(candidate.Id)
	require.NoError(t, err)

	return &candidateCoinbaseHarness{server: s, store: client, tip: tip, candidate: candidate, id: id}
}

// issue stores the candidate's job the way GetMiningCandidate does, and returns a copy of the
// candidate as the miner received it.
func (h *candidateCoinbaseHarness) issue(t *testing.T) *model.MiningCandidate {
	t.Helper()

	h.server.storeJobWithCoinbase(h.id, h.candidate, nil)
	require.NotEmpty(t, h.candidate.CoinbaseTx, "a stored job must carry its coinbase")

	issued := proto.Clone(h.candidate).(*model.MiningCandidate)

	return issued
}

// mine finds a nonce for the header a miner builds from the issued candidate: the candidate's own
// coinbase, which with no subtrees is the whole merkle root, under the given version.
func mine(t *testing.T, issued *model.MiningCandidate, version uint32) uint32 {
	t.Helper()

	return searchNonce(t, issued, version, true)
}

// missTarget finds a nonce whose header, built as in mine, does not meet the target. The regtest
// target passes about half of all nonces, so an arbitrary "wrong" nonce would be a coin flip.
func missTarget(t *testing.T, issued *model.MiningCandidate, version uint32) uint32 {
	t.Helper()

	return searchNonce(t, issued, version, false)
}

func searchNonce(t *testing.T, issued *model.MiningCandidate, version uint32, meetsTarget bool) uint32 {
	t.Helper()

	coinbase, err := bt.NewTxFromBytes(issued.CoinbaseTx)
	require.NoError(t, err)

	prev, err := chainhash.NewHash(issued.PreviousHash)
	require.NoError(t, err)

	nBits, err := model.NewNBitFromSlice(issued.NBits)
	require.NoError(t, err)

	header := &model.BlockHeader{
		Version:        version,
		HashPrevBlock:  prev,
		HashMerkleRoot: coinbase.TxIDChainHash(),
		Timestamp:      issued.Time,
		Bits:           *nBits,
	}

	for {
		if ok, _, _ := header.HasMetTargetDifficulty(); ok == meetsTarget {
			return header.Nonce
		}

		header.Nonce++
	}
}

// TestSubmitMiningSolution_CoinbaseLessMinesTheIssuedCoinbase pins bitcoin-sv's submitminingsolution
// fallback: a solution with no coinbase is mined with the coinbase the candidate was issued with.
// Block assembly used to rebuild one with a fresh random extranonce, so its merkle root was never
// the one the miner hashed and every such solution failed the proof-of-work check. A version in the
// solution goes into the header only, as in bitcoin-sv; it used to be copied into the coinbase too.
// One submit, no retries: the miner hashes exactly the block the server builds.
func TestSubmitMiningSolution_CoinbaseLessMinesTheIssuedCoinbase(t *testing.T) {
	h := newCandidateCoinbaseHarness(t)
	issued := h.issue(t)

	version := uint32(0x20000004)
	nonce := mine(t, issued, version)

	resp, err := h.server.submitMiningSolution(t.Context(), &BlockSubmissionRequest{
		SubmitMiningSolutionRequest: &blockassembly_api.SubmitMiningSolutionRequest{
			Id:      issued.Id,
			Nonce:   nonce,
			Version: &version,
		},
	})
	require.NoError(t, err)
	require.True(t, resp.Ok)

	best, _, err := h.store.GetBestBlockHeader(t.Context())
	require.NoError(t, err)
	require.Equal(t, h.tip.Hash(), best.HashPrevBlock, "the block must be stored on the candidate's parent")

	block, err := h.store.GetBlock(t.Context(), best.Hash())
	require.NoError(t, err)
	require.Equal(t, issued.CoinbaseTx, block.CoinbaseTx.Bytes(), "the block must carry the coinbase the candidate was issued with")
	require.Equal(t, version, block.Header.Version, "the solution's version belongs in the header")
	require.Equal(t, nonce, block.Header.Nonce)
}

// TestSubmitMiningSolution_InvalidSolutionKeepsTheIssuedCoinbase pins that a consensus-invalid
// solution leaves the job, and so its coinbase, in place, as bitcoin-sv leaves a candidate alone
// after an invalid submitminingsolution. Deleting the job made the next poll for the same id, which
// for an empty block is derived from the parent alone, build a coinbase with a new random
// extranonce, so every other miner still hashing the first one failed the proof-of-work check and
// deleted the job again. A share that misses the target, the commonest invalid solution, did worse:
// block.Valid reports a miss as a processing error, so it also reset block assembly. A miner on the
// original coinbase must still get its block.
func TestSubmitMiningSolution_InvalidSolutionKeepsTheIssuedCoinbase(t *testing.T) {
	h := newCandidateCoinbaseHarness(t)
	issued := h.issue(t)

	version := issued.Version

	// One miner's share that misses the target, coinbase-less like the miners it shares the id with.
	_, err := h.server.submitMiningSolution(t.Context(), &BlockSubmissionRequest{
		SubmitMiningSolutionRequest: &blockassembly_api.SubmitMiningSolutionRequest{
			Id:      issued.Id,
			Nonce:   missTarget(t, issued, version),
			Version: &version,
		},
	})
	require.ErrorContains(t, err, "block hash does not meet the target")

	// A share that misses the target is the miner's fault, so block assembly is not reset. Reset
	// sends from a goroutine, so wait a short while rather than checking once.
	select {
	case req := <-h.server.blockAssembler.resetCh:
		require.FailNow(t, "block assembly was reset", "reset request: %+v", req)
	case <-time.After(250 * time.Millisecond):
	}

	held := h.server.jobStore.Get(*h.id)
	require.NotNil(t, held, "a consensus-invalid solution must not drop the job")
	require.Equal(t, issued.CoinbaseTx, held.Value().MiningCandidate.CoinbaseTx, "the job must keep the coinbase it was issued with")

	// Another poll for the same id is handed the same coinbase.
	again := proto.Clone(h.candidate).(*model.MiningCandidate)
	again.CoinbaseTx = nil
	h.server.storeJobWithCoinbase(h.id, again, nil)
	require.Equal(t, issued.CoinbaseTx, again.CoinbaseTx)

	// A second miner, hashing the coinbase issued before the bad share, solves the block.
	nonce := mine(t, issued, version)

	resp, err := h.server.submitMiningSolution(t.Context(), &BlockSubmissionRequest{
		SubmitMiningSolutionRequest: &blockassembly_api.SubmitMiningSolutionRequest{
			Id:      issued.Id,
			Nonce:   nonce,
			Version: &version,
		},
	})
	require.NoError(t, err)
	require.True(t, resp.Ok)

	best, _, err := h.store.GetBestBlockHeader(t.Context())
	require.NoError(t, err)
	require.Equal(t, h.tip.Hash(), best.HashPrevBlock)

	block, err := h.store.GetBlock(t.Context(), best.Hash())
	require.NoError(t, err)
	require.Equal(t, issued.CoinbaseTx, block.CoinbaseTx.Bytes(), "the block must carry the coinbase issued before the invalid share")
	require.Equal(t, nonce, block.Header.Nonce)
}

// TestStoreJobWithCoinbase_UnusableMinerKeysWarn pins that a configured miner key that cannot build a
// coinbase is reported at Warn with its cause, so it is not mistaken for a node with no keys, which
// stays quiet. A node polled every second warns once per minerKeyWarnInterval, not once a poll.
func TestStoreJobWithCoinbase_UnusableMinerKeysWarn(t *testing.T) {
	t.Run("a key that is not a WIF warns once with the cause", func(t *testing.T) {
		h := newCandidateCoinbaseHarness(t)
		logger := &warnCapturingLogger{}
		h.server.logger = logger
		h.server.blockAssembler.settings.BlockAssembly.MinerWalletPrivateKeys = []string{"not-a-wif"}

		for range 3 {
			poll := proto.Clone(h.candidate).(*model.MiningCandidate)
			h.server.storeJobWithCoinbase(h.id, poll, nil)
			require.Empty(t, poll.CoinbaseTx)
		}

		require.Len(t, logger.warns, 1, "repeated polls within the interval must warn once")
		require.Contains(t, logger.warns[0], "miner_wallet_private_keys is set but cannot build a coinbase")
		require.Contains(t, logger.warns[0], "can't decode coinbase priv key", "the warning must carry the cause")
	})

	t.Run("no keys, or only blank ones, stays quiet", func(t *testing.T) {
		for _, keys := range [][]string{nil, {""}, {" "}} {
			h := newCandidateCoinbaseHarness(t)
			logger := &warnCapturingLogger{}
			h.server.logger = logger
			h.server.blockAssembler.settings.BlockAssembly.MinerWalletPrivateKeys = keys

			h.server.storeJobWithCoinbase(h.id, h.candidate, nil)
			require.Empty(t, h.candidate.CoinbaseTx)
			require.Empty(t, logger.warns, "keys %q must not warn", keys)
		}
	})
}

// TestGetCandidateBlock_CarriesTheIssuedCoinbase pins that the candidate block served for a job is
// built from the same coinbase the job was issued with, not one built on the spot.
func TestGetCandidateBlock_CarriesTheIssuedCoinbase(t *testing.T) {
	h := newCandidateCoinbaseHarness(t)
	issued := h.issue(t)

	resp, err := h.server.GetCandidateBlock(t.Context(), &blockassembly_api.GetCandidateBlockRequest{Id: issued.Id})
	require.NoError(t, err)
	require.Equal(t, issued.CoinbaseTx, resp.CoinbaseTx)
}

// TestStoreJobWithCoinbase_OncePerCandidateID pins that a candidate id keeps one coinbase for as long
// as its job is held. Ids repeat across polls, so building a coinbase per poll would leave an earlier
// miner holding a coinbase the job no longer has.
func TestStoreJobWithCoinbase_OncePerCandidateID(t *testing.T) {
	t.Run("a repeated id reuses the held coinbase", func(t *testing.T) {
		h := newCandidateCoinbaseHarness(t)
		first := h.issue(t)

		again := proto.Clone(h.candidate).(*model.MiningCandidate)
		again.CoinbaseTx = nil

		h.server.storeJobWithCoinbase(h.id, again, nil)
		require.Equal(t, first.CoinbaseTx, again.CoinbaseTx)
		require.Equal(t, first.CoinbaseTx, h.server.jobStore.Get(*h.id).Value().MiningCandidate.CoinbaseTx)
	})

	t.Run("concurrent polls of one id agree on its coinbase", func(t *testing.T) {
		// Two candidate paths and many miners poll at once. Run under -race.
		h := newCandidateCoinbaseHarness(t)

		const polls = 16

		got := make([][]byte, polls)

		var wg sync.WaitGroup

		for i := range polls {
			wg.Add(1)

			go func() {
				defer wg.Done()

				poll := proto.Clone(h.candidate).(*model.MiningCandidate)
				poll.CoinbaseTx = nil

				h.server.storeJobWithCoinbase(h.id, poll, nil)
				got[i] = poll.CoinbaseTx
			}()
		}

		wg.Wait()

		held := h.server.jobStore.Get(*h.id).Value().MiningCandidate.CoinbaseTx
		for i := range polls {
			require.Equal(t, held, got[i], "poll %d was handed a coinbase the job does not hold", i)
		}
	})

	t.Run("a different coinbase value gets a new coinbase", func(t *testing.T) {
		h := newCandidateCoinbaseHarness(t)
		first := h.issue(t)

		changed := proto.Clone(h.candidate).(*model.MiningCandidate)
		changed.CoinbaseTx = nil
		changed.CoinbaseValue--

		h.server.storeJobWithCoinbase(h.id, changed, nil)
		require.NotEqual(t, first.CoinbaseTx, changed.CoinbaseTx)

		coinbase, err := bt.NewTxFromBytes(changed.CoinbaseTx)
		require.NoError(t, err)
		require.Equal(t, changed.CoinbaseValue, coinbase.TotalOutputSatoshis())
	})
}

// TestStoreJobWithCoinbase_NoMinerKeys pins that a node without miner_wallet_private_keys still
// serves candidates. A pool that submits its own coinbase, and generatetoaddress, never needed those
// keys, so a coinbase that cannot be built leaves the job without one instead of failing the
// candidate. Only the paths that need the job's coinbase refuse, and refusing does not drop the job.
func TestStoreJobWithCoinbase_NoMinerKeys(t *testing.T) {
	h := newCandidateCoinbaseHarness(t)

	// The miner's own coinbase, built while the keys are still configured.
	own, err := h.candidate.CreateCoinbaseTxCandidate(h.server.blockAssembler.settings)
	require.NoError(t, err)

	h.server.blockAssembler.settings.BlockAssembly.MinerWalletPrivateKeys = nil

	h.server.storeJobWithCoinbase(h.id, h.candidate, nil)
	require.Empty(t, h.candidate.CoinbaseTx)

	held := h.server.jobStore.Get(*h.id)
	require.NotNil(t, held, "the job must be stored even though it has no coinbase")
	require.Empty(t, held.Value().MiningCandidate.CoinbaseTx)

	_, err = h.server.GetCandidateBlock(t.Context(), &blockassembly_api.GetCandidateBlockRequest{Id: h.candidate.Id})
	require.ErrorContains(t, err, "miner_wallet_private_keys")

	_, err = h.server.submitMiningSolution(t.Context(), &BlockSubmissionRequest{
		SubmitMiningSolutionRequest: &blockassembly_api.SubmitMiningSolutionRequest{Id: h.candidate.Id},
	})
	require.ErrorContains(t, err, "miner_wallet_private_keys")
	require.NotNil(t, h.server.jobStore.Get(*h.id), "a refused coinbase-less solution must not drop the job")

	// A solution carrying its own coinbase is mined.
	hashed := proto.Clone(h.candidate).(*model.MiningCandidate)
	hashed.CoinbaseTx = own.Bytes()

	version := h.candidate.Version
	nonce := mine(t, hashed, version)

	resp, err := h.server.submitMiningSolution(t.Context(), &BlockSubmissionRequest{
		SubmitMiningSolutionRequest: &blockassembly_api.SubmitMiningSolutionRequest{
			Id:         h.candidate.Id,
			Nonce:      nonce,
			Version:    &version,
			CoinbaseTx: own.Bytes(),
		},
	})
	require.NoError(t, err)
	require.True(t, resp.Ok)

	best, _, err := h.store.GetBestBlockHeader(t.Context())
	require.NoError(t, err)

	block, err := h.store.GetBlock(t.Context(), best.Hash())
	require.NoError(t, err)
	require.Equal(t, own.Bytes(), block.CoinbaseTx.Bytes())
}

// TestGenerateBlocks_NoMinerKeys pins that generatetoaddress mines with miner_wallet_private_keys
// empty. It pays the given address and never needed those keys, so a candidate must still be
// served when block assembly cannot build the job's own coinbase.
func TestGenerateBlocks_NoMinerKeys(t *testing.T) {
	server, _ := setupServer(t)
	require.NoError(t, server.blockAssembler.Start(t.Context()))

	server.blockAssembler.settings.BlockAssembly.MinerWalletPrivateKeys = nil

	_, before, err := server.blockchainClient.GetBestBlockHeader(t.Context())
	require.NoError(t, err)

	// Testnet P2PKH address, as in TestGenerateBlock_AllowsP2PKHCoinbaseOutput.
	address := "n2ZNV88uQbede7C5M5jzi6SyG4GVVr5JTt"

	_, err = server.GenerateBlocks(t.Context(), &blockassembly_api.GenerateBlocksRequest{Count: 1, Address: &address})
	require.NoError(t, err)

	best, after, err := server.blockchainClient.GetBestBlockHeader(t.Context())
	require.NoError(t, err)
	require.Equal(t, before.Height+1, after.Height)

	block, err := server.blockchainClient.GetBlock(t.Context(), best.Hash())
	require.NoError(t, err)

	want, err := model.AddressToScript(address)
	require.NoError(t, err)
	require.Len(t, block.CoinbaseTx.Outputs, 1)
	require.Equal(t, want, block.CoinbaseTx.Outputs[0].LockingScript.Bytes(), "the coinbase must pay the requested address")
}
