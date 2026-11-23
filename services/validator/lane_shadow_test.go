package validator

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/settings"
	blockchainsql "github.com/bsv-blockchain/teranode/stores/blockchain/sql"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/bsv-blockchain/teranode/util/lanereader"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// laneReaderAnswers sums teranode_lane_reader_answers_total for reader over every kind and via,
// and for kind alone.
func laneReaderAnswers(t *testing.T, reader, kind string) (all, ofKind float64) {
	t.Helper()

	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	for _, f := range families {
		if f.GetName() != "teranode_lane_reader_answers_total" {
			continue
		}

		for _, m := range f.GetMetric() {
			labels := map[string]string{}
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}

			if labels["reader"] != reader {
				continue
			}

			all += m.GetCounter().GetValue()

			if labels["kind"] == kind {
				ofKind += m.GetCounter().GetValue()
			}
		}
	}

	return all, ofKind
}

// startLaneReader starts the process's lane reader at shadow on a SQLite blockchain store holding
// one block without rows at height 1, waits for the index to reach it, and clears the reader when
// the test ends.
func startLaneReader(t *testing.T) {
	t.Helper()

	u, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.LaneMigration = settings.LaneMigrationShadow

	store, err := blockchainsql.New(ulogger.TestLogger{}, u, tSettings)
	require.NoError(t, err)

	genesis, _, err := store.GetBestBlockHeader(context.Background())
	require.NoError(t, err)

	coinbase, err := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff17030100002f6d312d65752f29c267ffea1adb87f33b398fffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac00000000")
	require.NoError(t, err)

	bits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	_, _, err = store.StoreBlock(context.Background(), &model.Block{
		Header: &model.BlockHeader{
			Version: 1, Timestamp: 1729259800, Nonce: 5, HashPrevBlock: genesis.Hash(),
			HashMerkleRoot: &chainhash.Hash{}, Bits: *bits,
		},
		Height:           1,
		CoinbaseTx:       coinbase,
		TransactionCount: 1,
	}, "")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	r := lanereader.Start(ctx, ulogger.TestLogger{}, tSettings, store)
	require.NotNil(t, r)

	t.Cleanup(func() {
		cancel()
		lanereader.Start(context.Background(), nil, test.CreateBaseTestSettings(t), nil)
		_ = store.Close(context.Background())
	})

	require.Eventually(t, func() bool { return r.Snapshot() != nil && r.Snapshot().TipHeight() == 1 }, 10*time.Second, 10*time.Millisecond)
}

// At shadow each distinct parent of the parent read is answered from the lane index beside the
// store's answer and counted, under validator for a store answer and subtree_prefetch for a
// prefetched parent; at off nothing is counted.
func TestParentReadIsShadowedByTheLaneIndex(t *testing.T) {
	ctx := context.Background()
	base := parentOutputsStore(t)

	s0 := bscript.NewFromBytes([]byte{0x76, 0xa9, 0x00})
	mined := storedParent(t, base, 1, 120, s0, s0)
	unmined := storedParent(t, base, 2, 0, s0)

	positions := map[chainhash.Hash]uint64{
		*mined.TxIDChainHash():   lane.Pack(7, 100),
		*unmined.TxIDChainHash(): lane.Pack(8, 200),
	}

	store := &countingParentStore{Store: base}
	store.override = func(answers []utxostore.ParentOutput) ([]utxostore.ParentOutput, error) {
		for i := range answers {
			answers[i].LanePosition = positions[store.outpoints[len(store.outpoints)-len(answers)+i].TxID]
		}

		return answers, nil
	}

	// The mined parent is spent twice: it is answered once.
	tx := parentOutputsChild(t, []chainhash.Hash{*unmined.TxIDChainHash(), *mined.TxIDChainHash(), *mined.TxIDChainHash()}, []uint32{0, 0, 1})

	startLaneReader(t)

	all, notLoaded := laneReaderAnswers(t, lanereader.ReaderValidator, lanereader.KindNotLoaded)
	_, agree := laneReaderAnswers(t, lanereader.ReaderValidator, lanereader.KindAgree)

	_, _, _, err := laneShadowValidator(t, store).getTransactionInputBlockHeightsAndExtendTx(ctx, tx, tx.TxID(), &Options{})
	require.NoError(t, err)

	// The index holds no block at height 120, so the mined parent is not_loaded; both say the
	// unmined one is not mined.
	all2, notLoaded2 := laneReaderAnswers(t, lanereader.ReaderValidator, lanereader.KindNotLoaded)
	_, agree2 := laneReaderAnswers(t, lanereader.ReaderValidator, lanereader.KindAgree)
	require.Equal(t, all+2, all2)
	require.Equal(t, notLoaded+1, notLoaded2)
	require.Equal(t, agree+1, agree2)

	t.Run("prefetched", func(t *testing.T) {
		// The record names block 999, which the index never loaded, mined at height 1, where the
		// index holds a main-chain block with no rows. Judged by its block list the parent is
		// not_loaded; judged by its height it would be no_rows.
		prefetched := map[chainhash.Hash]*meta.Data{
			*mined.TxIDChainHash(): {Tx: mined, BlockIDs: []uint32{999}, BlockHeights: []uint32{1}, LanePosition: lane.Pack(9, 1)},
		}

		prefetchAll, prefetchNotLoaded := laneReaderAnswers(t, lanereader.ReaderSubtreePrefetch, lanereader.KindNotLoaded)
		validatorAll, _ := laneReaderAnswers(t, lanereader.ReaderValidator, lanereader.KindAgree)

		_, _, _, err := laneShadowValidator(t, store).getTransactionInputBlockHeightsAndExtendTx(ctx, tx, tx.TxID(), &Options{PrefetchedParents: prefetched})
		require.NoError(t, err)

		prefetchAll2, prefetchNotLoaded2 := laneReaderAnswers(t, lanereader.ReaderSubtreePrefetch, lanereader.KindNotLoaded)
		validatorAll2, _ := laneReaderAnswers(t, lanereader.ReaderValidator, lanereader.KindAgree)
		require.Equal(t, prefetchAll+1, prefetchAll2)
		require.Equal(t, prefetchNotLoaded+1, prefetchNotLoaded2, "judged by the block list, not the height")
		require.Equal(t, validatorAll+1, validatorAll2)
	})

	t.Run("off", func(t *testing.T) {
		before, _ := laneReaderAnswers(t, lanereader.ReaderValidator, lanereader.KindAgree)

		_, _, _, err := parentOutputsValidator(t, store).getTransactionInputBlockHeightsAndExtendTx(ctx, tx, tx.TxID(), &Options{})
		require.NoError(t, err)

		after, _ := laneReaderAnswers(t, lanereader.ReaderValidator, lanereader.KindAgree)
		require.Equal(t, before, after)
	})
}

// The shadow answers each distinct parent once, past the linear stretch too.
func TestParentShadowSeesEachParentOnce(t *testing.T) {
	var s parentShadow

	for round := 0; round < 2; round++ {
		for i := 0; i < 3*parentShadowLinearMax; i++ {
			require.Equal(t, round == 1, s.repeat(chainhash.Hash{byte(i)}), "round %d parent %d", round, i)
		}
	}
}
