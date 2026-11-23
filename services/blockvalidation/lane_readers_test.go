package blockvalidation

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/settings"
	blockchainoptions "github.com/bsv-blockchain/teranode/stores/blockchain/options"
	blockchainsql "github.com/bsv-blockchain/teranode/stores/blockchain/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/bsv-blockchain/teranode/util/lanereader"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// laneReaderAnswer is teranode_lane_reader_answers_total for one reader and kind, over every via.
func laneReaderAnswer(t *testing.T, reader, kind string) float64 {
	t.Helper()

	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	var sum float64

	for _, f := range families {
		if f.GetName() != "teranode_lane_reader_answers_total" {
			continue
		}

		for _, m := range f.GetMetric() {
			labels := map[string]string{}
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}

			if labels["reader"] == reader && labels["kind"] == kind {
				sum += m.GetCounter().GetValue()
			}
		}
	}

	return sum
}

// At off there is no lane index: the parent check's context is unchanged and setMined's verdict
// passes through with nothing compared.
func TestLaneReadersDoNothingAtOff(t *testing.T) {
	lanereader.Start(context.Background(), nil, test.CreateBaseTestSettings(t), nil)

	ctx := context.Background()
	blk := &model.Block{Header: &model.BlockHeader{HashPrevBlock: &chainhash.Hash{1}}}

	require.Equal(t, ctx, laneParentsContext(ctx, blk))

	verdict := errors.NewBlockInvalidError("duplicate")
	require.Equal(t, verdict, shadowLaneDuplicates(ctx, blk, 1, nil, verdict))
	require.NoError(t, shadowLaneDuplicates(ctx, blk, 1, nil, nil))
}

// At shadow the duplicate check's verdict is compared with the lane index in the background, and
// returned unchanged; an unset and a verdict that is neither a pass nor a duplicate are not
// compared.
func TestShadowLaneDuplicatesComparesSetMinedsVerdict(t *testing.T) {
	u, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.LaneMigration = settings.LaneMigrationShadow

	store, err := blockchainsql.New(ulogger.TestLogger{}, u, tSettings)
	require.NoError(t, err)

	t.Cleanup(func() { _ = store.Close(context.Background()) })

	genesis, _, err := store.GetBestBlockHeader(context.Background())
	require.NoError(t, err)

	coinbase, err := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff17030100002f6d312d65752f29c267ffea1adb87f33b398fffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac00000000")
	require.NoError(t, err)

	bits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	blk := &model.Block{
		Header: &model.BlockHeader{
			Version: 1, Timestamp: 1729259800, Nonce: 77, HashPrevBlock: genesis.Hash(),
			HashMerkleRoot: &chainhash.Hash{}, Bits: *bits,
		},
		Height:           1,
		CoinbaseTx:       coinbase,
		TransactionCount: 2,
	}

	rows, err := lane.BuildMapRows(3, lanePositions(5))
	require.NoError(t, err)

	id, _, err := store.StoreBlock(context.Background(), blk, "", blockchainoptions.WithLaneMapRows(rows))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		lanereader.Start(context.Background(), nil, test.CreateBaseTestSettings(t), nil)
	})

	r := lanereader.Start(ctx, ulogger.TestLogger{}, tSettings, store)
	require.Eventually(t, func() bool { return r.Snapshot() != nil }, 10*time.Second, 10*time.Millisecond)

	agree := laneReaderAnswer(t, lanereader.ReaderBlockDuplicate, lanereader.KindAgree)
	notLoaded := laneReaderAnswer(t, lanereader.ReaderBlockDuplicate, lanereader.KindNotLoaded)

	require.NoError(t, shadowLaneDuplicates(ctx, blk, uint32(id), nil, nil)) //nolint:gosec // a test block ID
	require.Eventually(t, func() bool {
		return laneReaderAnswer(t, lanereader.ReaderBlockDuplicate, lanereader.KindAgree) == agree+1
	}, 10*time.Second, 10*time.Millisecond)

	verdict := errors.NewBlockInvalidError("contains a transaction already on our chain")
	require.Equal(t, verdict, shadowLaneDuplicates(ctx, blk, uint32(id), nil, verdict)) //nolint:gosec // a test block ID
	require.Eventually(t, func() bool {
		return laneReaderAnswer(t, lanereader.ReaderBlockDuplicate, lanereader.KindNotLoaded) == notLoaded+1
	}, 10*time.Second, 10*time.Millisecond)

	// Neither an unset nor a processing error is compared.
	other := errors.NewProcessingError("store down")
	require.Equal(t, other, shadowLaneDuplicates(ctx, blk, uint32(id), nil, other))   //nolint:gosec // a test block ID
	require.NoError(t, shadowLaneDuplicates(ctx, blk, uint32(id), []bool{true}, nil)) //nolint:gosec // a test block ID

	time.Sleep(100 * time.Millisecond)
	require.Equal(t, agree+1, laneReaderAnswer(t, lanereader.ReaderBlockDuplicate, lanereader.KindAgree))
	require.Equal(t, notLoaded+1, laneReaderAnswer(t, lanereader.ReaderBlockDuplicate, lanereader.KindNotLoaded))

	// The parent check's context carries the shadow at shadow.
	require.NotEqual(t, ctx, laneParentsContext(ctx, blk))
}
