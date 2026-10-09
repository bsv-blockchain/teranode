package netsync

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"math/big"
	"net/url"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/work"
	"github.com/bsv-blockchain/teranode/settings"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// mainnetFixtureFirst and mainnetFixtureLast bound testdata/mainnet_headers_501900_506100.bin.
const (
	mainnetFixtureFirst = int32(501900)
	mainnetFixtureLast  = int32(506100)
)

// mainnetFixture reads the real mainnet headers in testdata and checks them
// against the digest and the hashes testdata/README.md pins, so a test that
// accepts them is accepting the real chain and nothing else.
func mainnetFixture(t *testing.T) []*wire.BlockHeader {
	t.Helper()

	data, err := os.ReadFile("testdata/mainnet_headers_501900_506100.bin")
	require.NoError(t, err)
	require.Len(t, data, int(mainnetFixtureLast-mainnetFixtureFirst+1)*80)

	digest := sha256.Sum256(data)
	require.Equal(t, "456d2613596bf496d8706e962744abaac927bab8eab1a6d92baae5560c8d159a", hex.EncodeToString(digest[:]))

	headers := make([]*wire.BlockHeader, 0, mainnetFixtureLast-mainnetFixtureFirst+1)

	for i := 0; i < len(data); i += 80 {
		var header wire.BlockHeader
		require.NoError(t, header.Deserialize(bytes.NewReader(data[i:i+80])))

		if n := len(headers); n > 0 {
			require.Equal(t, headers[n-1].BlockHash(), header.PrevBlock, "fixture linkage at index %d", n)
		}

		headers = append(headers, &header)
	}

	pins := map[int32]string{
		501900: "000000000000000006f12e51b8024c6b4a10f0fea7ebfe8af655230525503d0e",
		504000: "0000000000000000006cdeece5716c9c700f34ad98cb0ed0ad2c5767bbe0bc8c",
		504031: "0000000000000000011ebf65b60d0a3de80b8175be709d653b4c1a1beeb6ab9c",
		504032: "00000000000000000343e9875012f2062554c8752929892c82a0c0743ac7dcfd",
		506100: "0000000000000000003beb1044e40bef309e1dab9f64f6abe4c6d1596a060869",
	}

	for height, want := range pins {
		require.Equal(t, want, headers[height-mainnetFixtureFirst].BlockHash().String(), "pinned hash at %d", height)
	}

	return headers
}

// realTrunk is a sqlitememory blockchain store holding real headers as the
// committed chain, and the blockchain client production reads it through.
type realTrunk struct {
	settings *settings.Settings
	params   *chaincfg.Params
	store    blockchainstore.Store
	client   blockchain.ClientI
}

// newRealTrunk builds a store for params and, when headers is non-empty,
// writes them into it as committed main-chain blocks from firstHeight, the way
// services/blockchain's TestDifficultyHistoricalMainnet seeds its slice: real
// parent links and cumulative chain work counted from the first one, which is
// all the difficulty rules read (they use differences of chain work, never its
// absolute value).
func newRealTrunk(t *testing.T, params *chaincfg.Params, firstHeight int32, headers []*wire.BlockHeader) *realTrunk {
	t.Helper()

	ctx := context.Background()

	s := test.CreateBaseTestSettings(t)
	s.ChainCfgParams = params

	store, err := blockchainstore.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, s)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })

	if len(headers) > 0 {
		tx, err := store.GetDB().BeginTx(ctx, nil)
		require.NoError(t, err)

		defer func() { _ = tx.Rollback() }()

		insert, err := tx.PrepareContext(ctx, `INSERT INTO blocks (
			id, parent_id, hash, height, version, previous_hash, merkle_root,
			block_time, n_bits, nonce, chain_work, tx_count, size_in_bytes,
			subtree_count, subtrees, coinbase_tx, peer_id, on_main_chain
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 0, 80, 0, X'', X'', 'test', TRUE)`)
		require.NoError(t, err)

		defer func() { _ = insert.Close() }()

		chainWork := new(big.Int)

		for i, header := range headers {
			m := modelHeader(header)

			var parentID any
			if i > 0 {
				parentID = i
			}

			chainWork.Add(chainWork, work.CalcBlockWork(header.Bits))

			_, err = insert.ExecContext(ctx, i+1, parentID, m.Hash()[:], firstHeight+int32(i), m.Version, //nolint:gosec // a fixture index
				m.HashPrevBlock[:], m.HashMerkleRoot[:], m.Timestamp, m.Bits.CloneBytes(), m.Nonce, chainWork.FillBytes(make([]byte, 32)))
			require.NoError(t, err)
		}

		require.NoError(t, tx.Commit())
	}

	client, err := blockchain.NewLocalClient(ulogger.TestLogger{}, s, store, nil, nil)
	require.NoError(t, err)

	return &realTrunk{settings: s, params: params, store: store, client: client}
}

// rules returns the header rules over this trunk with the clock fixed at now.
func (r *realTrunk) rules(t *testing.T, now time.Time) *headerRules {
	t.Helper()

	rules, err := newHeaderRules(ulogger.TestLogger{}, r.settings, r.params, r.client)
	require.NoError(t, err)
	require.NotNil(t, rules)

	rules.now = func() time.Time { return now }

	return rules
}

// relink makes headers one linked run again after a test edited one of them,
// so the edited header is the only fault in the batch.
func relink(headers []*wire.BlockHeader) {
	for i := 1; i < len(headers); i++ {
		headers[i].PrevBlock = headers[i-1].BlockHash()
	}
}

// copyHeaders copies headers so a test can edit them without touching the fixture.
func copyHeaders(headers []*wire.BlockHeader) []*wire.BlockHeader {
	out := make([]*wire.BlockHeader, len(headers))

	for i, header := range headers {
		h := *header
		out[i] = &h
	}

	return out
}

// TestHeaderRules_RealMainnetHeadersAcrossARetargetAndTheDAASwitchAreAccepted
// feeds the cache 2,201 real mainnet headers above a committed trunk of 2,000,
// with every rule on: proof of work against mainnet's ceiling, the checkpoints,
// and the contextual rules reading the trunk through the blockchain client over
// a sqlitememory store. The run crosses the emergency-difficulty era, the
// periodic retarget at 504000, whose 2016-block window starts in the trunk, and
// the switch to the 144-block DAA at 504032, whose window straddles the trunk
// and the batch. The second reply extends the cache's own top, so its DAA
// windows are read from headers held only in the cache.
func TestHeaderRules_RealMainnetHeadersAcrossARetargetAndTheDAASwitchAreAccepted(t *testing.T) {
	headers := mainnetFixture(t)
	at := func(h int32) *wire.BlockHeader { return headers[h-mainnetFixtureFirst] }

	require.NotEqual(t, at(503999).Bits, at(504000).Bits, "sanity: 504000 is a retarget that changed the target")
	require.NotEqual(t, at(504031).Bits, at(504032).Bits, "sanity: the first DAA child changed the target")

	trunk := newRealTrunk(t, &chaincfg.MainNetParams, mainnetFixtureFirst, headers[:503900-mainnetFixtureFirst])
	cache := newHeaderCache().
		WithCheckpoints(chaincfg.MainNetParams.Checkpoints).
		WithPowLimit(model.PowLimitCeiling(&chaincfg.MainNetParams)).
		WithHeaderRules(trunk.rules(t, at(mainnetFixtureLast).Timestamp.Add(time.Hour)))

	tip := at(503899).BlockHash()

	first := cache.FillDetailed(tip, 503900, headers[503900-mainnetFixtureFirst:505900-mainnetFixtureFirst])
	require.Equal(t, headerAccepted, first.rejection, "%s at %d: %s", first.rejection, first.rejectedHeight, first.detail)
	require.True(t, first.accepted)
	require.False(t, first.extended)

	second := cache.FillDetailed(tip, 503900, headers[505900-mainnetFixtureFirst:])
	require.Equal(t, headerAccepted, second.rejection, "%s at %d: %s", second.rejection, second.rejectedHeight, second.detail)
	require.True(t, second.accepted)
	require.True(t, second.extended, "below the last checkpoint the second reply extends the cache's own top")

	top, ok := cache.Top()
	require.True(t, ok)
	require.Equal(t, mainnetFixtureLast, top)

	for _, h := range []int32{504000, 504031, 504032, 506100} {
		got, ok := cache.At(h)
		require.True(t, ok)
		require.Equal(t, at(h).BlockHash(), got)
	}
}

// TestHeaderRules_MinimumDifficultyBitsAtARealHeightAreRefused is the attack
// the rule exists for: a header at 503902, where mainnet's real target is far
// below the proof-of-work ceiling, declaring the ceiling's own bits, the
// difficulty a fake run can be mined at in seconds. The cache has no
// proof-of-work ceiling here, so the unmined header reaches the bits rule
// instead of failing high-hash first. The whole batch is refused, the two real
// headers before it included, because SV Node scores bad-diffbits DoS 100.
func TestHeaderRules_MinimumDifficultyBitsAtARealHeightAreRefused(t *testing.T) {
	headers := mainnetFixture(t)
	trunk := newRealTrunk(t, &chaincfg.MainNetParams, mainnetFixtureFirst, headers[:503900-mainnetFixtureFirst])
	now := headers[503905-mainnetFixtureFirst].Timestamp.Add(time.Hour)

	batch := copyHeaders(headers[503900-mainnetFixtureFirst : 503906-mainnetFixtureFirst])
	batch[2].Bits = chaincfg.MainNetParams.PowLimitBits
	relink(batch)

	cache := newHeaderCache().WithCheckpoints(chaincfg.MainNetParams.Checkpoints).WithHeaderRules(trunk.rules(t, now))

	result := cache.FillDetailed(batch[0].PrevBlock, 503900, batch)
	require.Equal(t, rejectBadDiffBits, result.rejection, result.detail)
	require.Equal(t, int32(503902), result.rejectedHeight)
	require.True(t, result.rejection.disconnects())
	require.False(t, result.accepted)
	require.Zero(t, cache.Len(), "nothing from a batch carrying a wrong difficulty is kept")

	// The control: the same six headers unedited are accepted by the same rules.
	control := newHeaderCache().WithCheckpoints(chaincfg.MainNetParams.Checkpoints).WithHeaderRules(trunk.rules(t, now))
	require.True(t, control.Fill(batch[0].PrevBlock, 503900, headers[503900-mainnetFixtureFirst:503906-mainnetFixtureFirst]))
	require.Equal(t, 6, control.Len())
}

// TestHeaderRules_TimeAndVersionRulesRefuseWithoutBlame pins the three
// ContextualCheckBlockHeader rules SV Node marks invalid without a DoS score:
// a time not after the parent's median time past, a time more than two hours
// ahead of this node's clock, and a version below 4 above BIP65. Each refuses
// the edited header, keeps the real header before it, and does not ask for the
// peer to be dropped. The two-hour limit is inclusive, as SV Node's ">" makes it.
func TestHeaderRules_TimeAndVersionRulesRefuseWithoutBlame(t *testing.T) {
	headers := mainnetFixture(t)
	trunk := newRealTrunk(t, &chaincfg.MainNetParams, mainnetFixtureFirst, headers[:503900-mainnetFixtureFirst])
	now := headers[503900-mainnetFixtureFirst].Timestamp

	// The median of 503890 to 503900's timestamps, computed from the fixture
	// rather than by the code under test.
	window := make([]int64, 0, medianTimeSpan)
	for h := int32(503890); h <= 503900; h++ {
		window = append(window, headers[h-mainnetFixtureFirst].Timestamp.Unix())
	}

	mtp := sortedMedian(window)

	cases := []struct {
		name   string
		edit   func(*wire.BlockHeader)
		reason headerRejection
	}{
		{"time equal to median time past", func(h *wire.BlockHeader) { h.Timestamp = time.Unix(mtp, 0) }, rejectTimeTooOld},
		{"time one second past the future limit", func(h *wire.BlockHeader) { h.Timestamp = now.Add(maxFutureBlockTime + time.Second) }, rejectTimeTooNew},
		{"version 3 above BIP65", func(h *wire.BlockHeader) { h.Version = 3 }, rejectBadVersion},
		{"time exactly at the future limit", func(h *wire.BlockHeader) { h.Timestamp = now.Add(maxFutureBlockTime) }, headerAccepted},
		{"time one second after median time past", func(h *wire.BlockHeader) { h.Timestamp = time.Unix(mtp+1, 0) }, headerAccepted},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			batch := copyHeaders(headers[503900-mainnetFixtureFirst : 503902-mainnetFixtureFirst])
			tc.edit(batch[1])
			relink(batch)

			cache := newHeaderCache().WithCheckpoints(chaincfg.MainNetParams.Checkpoints).WithHeaderRules(trunk.rules(t, now))
			result := cache.FillDetailed(batch[0].PrevBlock, 503900, batch)

			require.Equal(t, tc.reason, result.rejection, result.detail)
			require.False(t, result.rejection.disconnects())
			require.True(t, result.accepted, "the real header before the refused one is kept")

			top, ok := cache.Top()
			require.True(t, ok)

			if tc.reason == headerAccepted {
				require.Equal(t, int32(503901), top)
			} else {
				require.Equal(t, int32(503901), result.rejectedHeight)
				require.Equal(t, int32(503900), top, "the refused header is not held")
			}
		})
	}
}

// sortedMedian is the n/2 element of the sorted values, GetMedianTimePast's choice.
func sortedMedian(values []int64) int64 {
	sorted := append([]int64(nil), values...)
	slices.Sort(sorted)

	return sorted[len(sorted)/2]
}

// TestHeaderRules_RealTestnetHeadersFromGenesisAreAccepted runs testnet's first
// 547 real headers through the rules from a trunk holding only genesis. Testnet
// allows a minimum-difficulty block whenever its time is more than twenty
// minutes after its parent's, which the calculator's testnet branch decides, and
// the run proves through the checkpoint at 546.
func TestHeaderRules_RealTestnetHeadersFromGenesisAreAccepted(t *testing.T) {
	data, err := os.ReadFile("../../blockchain/testdata/testnet_headers_0_547.bin")
	require.NoError(t, err)
	require.Len(t, data, 548*80)

	headers := make([]*wire.BlockHeader, 0, 547)

	for i := 80; i < len(data); i += 80 {
		var header wire.BlockHeader
		require.NoError(t, header.Deserialize(bytes.NewReader(data[i:i+80])))
		headers = append(headers, &header)
	}

	params := chaincfg.TestNetParams
	trunk := newRealTrunk(t, &params, 0, nil)

	genesis := *params.GenesisHash
	require.Equal(t, genesis, headers[0].PrevBlock)

	cache := newHeaderCache().
		WithCheckpoints(params.Checkpoints).
		WithPowLimit(model.PowLimitCeiling(&params)).
		WithHeaderRules(trunk.rules(t, time.Now()))

	result := cache.FillDetailed(genesis, 1, headers)
	require.Equal(t, headerAccepted, result.rejection, "%s at %d: %s", result.rejection, result.rejectedHeight, result.detail)
	require.True(t, result.accepted)
	require.Equal(t, int32(546), cache.ProvenTo())

	minimum := 0

	for _, header := range headers {
		if header.Bits == params.PowLimitBits {
			minimum++
		}
	}

	require.Equal(t, len(headers), minimum, "sanity: every early testnet header carries the minimum difficulty")
}

// TestFillHeaderCache_ABadDifficultyHeaderDisconnectsTheSender is the handler
// side: the manager reads its committed tip from a sqlitememory store holding
// real mainnet headers, the cache judges a reply with the rules New wires, and a
// header declaring the wrong difficulty costs the sender its connection, while a
// time-too-new header in the same position does not.
func TestFillHeaderCache_ABadDifficultyHeaderDisconnectsTheSender(t *testing.T) {
	headers := mainnetFixture(t)
	now := headers[503905-mainnetFixtureFirst].Timestamp.Add(time.Hour)

	run := func(t *testing.T, idx uint8, edit func(*wire.BlockHeader)) (bool, bool) {
		trunk := newRealTrunk(t, &chaincfg.MainNetParams, mainnetFixtureFirst, headers[:503900-mainnetFixtureFirst])

		sm := newHeaderCacheManager(t)
		sm.blockchainClient = trunk.client
		sm.headerCache = newHeaderCache().WithCheckpoints(chaincfg.MainNetParams.Checkpoints).WithHeaderRules(trunk.rules(t, now))

		best, tipHash, ok := sm.committedTip()
		require.True(t, ok)
		require.Equal(t, int32(503899), best, "sanity: the committed tip is the seeded trunk's top")
		require.Equal(t, headers[503899-mainnetFixtureFirst].BlockHash(), tipHash)

		batch := copyHeaders(headers[503900-mainnetFixtureFirst : 503906-mainnetFixtureFirst])
		edit(batch[2])
		relink(batch)

		msg := wire.NewMsgHeaders()
		for _, header := range batch {
			require.NoError(t, msg.AddBlockHeader(header))
		}

		peer, _, _ := connectRacePeer(t, idx, 1000)

		return sm.fillHeaderCache(peer, msg), peer.Connected()
	}

	t.Run("bad-diffbits disconnects", func(t *testing.T) {
		accepted, connected := run(t, 231, func(h *wire.BlockHeader) { h.Bits = chaincfg.MainNetParams.PowLimitBits })
		require.False(t, accepted)
		require.False(t, connected)
	})

	t.Run("time-too-new keeps the peer and the headers before it", func(t *testing.T) {
		accepted, connected := run(t, 232, func(h *wire.BlockHeader) { h.Timestamp = now.Add(3 * time.Hour) })
		require.True(t, accepted)
		require.True(t, connected)
	})
}
