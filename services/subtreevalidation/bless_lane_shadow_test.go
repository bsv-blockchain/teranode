package subtreevalidation

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/settings"
	blockchainoptions "github.com/bsv-blockchain/teranode/stores/blockchain/options"
	blockchainsql "github.com/bsv-blockchain/teranode/stores/blockchain/sql"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/bsv-blockchain/teranode/util/lanereader"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// positionStore answers Get with a lane position and counts the reads.
type positionStore struct {
	utxo.Store
	pos   uint64
	reads int
}

func (s *positionStore) Get(_ context.Context, _ *chainhash.Hash, f ...fields.FieldName) (*meta.Data, error) {
	s.reads++

	return &meta.Data{LanePosition: s.pos}, nil
}

// metaValidator accepts every transaction with md as its metadata.
type metaValidator struct {
	validator.Interface
	md *meta.Data
}

func (m *metaValidator) ValidateWithOptions(context.Context, *bt.Tx, uint32, *validator.Options) (*meta.Data, error) {
	d := *m.md

	return &d, nil
}

// subtreeAlreadyMined is teranode_lane_reader_answers_total for subtree_already_mined, by kind and
// via.
func subtreeAlreadyMined(t *testing.T, kind, via string) float64 {
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

			if labels["reader"] == lanereader.ReaderSubtreeAlreadyMined && labels["kind"] == kind && labels["via"] == via {
				return m.GetCounter().GetValue()
			}
		}
	}

	return 0
}

// At shadow, blessMissing answers its already-mined check from the lane index too, judged on the
// chain the check judges on: the caller's block IDs, or the main chain when it gives none.
func TestBlessMissingShadowsTheAlreadyMinedCheck(t *testing.T) {
	u, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.LaneMigration = settings.LaneMigrationShadow

	// A copy of the chain parameters with the usual 100-block settle depth, so b1 stays a row.
	params := *tSettings.ChainCfgParams
	params.CoinbaseMaturity = 100
	tSettings.ChainCfgParams = &params

	chain, err := blockchainsql.New(ulogger.TestLogger{}, u, tSettings)
	require.NoError(t, err)

	t.Cleanup(func() { _ = chain.Close(context.Background()) })

	genesis, _, err := chain.GetBestBlockHeader(context.Background())
	require.NoError(t, err)

	coinbase, err := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff17030100002f6d312d65752f29c267ffea1adb87f33b398fffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac00000000")
	require.NoError(t, err)

	bits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	store := func(prev *chainhash.Hash, nonce uint32, rows []lane.MapRow) (uint32, *chainhash.Hash) {
		blk := &model.Block{
			Header:           &model.BlockHeader{Version: 1, Timestamp: 1729259800 + nonce, Nonce: nonce, HashPrevBlock: prev, HashMerkleRoot: &chainhash.Hash{}, Bits: *bits},
			CoinbaseTx:       coinbase,
			TransactionCount: 1,
		}

		var opts []blockchainoptions.StoreBlockOption
		if rows != nil {
			opts = append(opts, blockchainoptions.WithLaneMapRows(rows))
		}

		id, _, err := chain.StoreBlock(context.Background(), blk, "", opts...)
		require.NoError(t, err)

		return uint32(id), blk.Hash() //nolint:gosec // test block IDs are small
	}

	taken := roaring64.New()
	taken.Add(5)

	rows, err := lane.BuildMapRows(3, taken)
	require.NoError(t, err)

	b1, h1 := store(genesis.Hash(), 1, rows)
	b2, _ := store(h1, 2, nil)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		lanereader.Start(context.Background(), nil, test.CreateBaseTestSettings(t), nil)
	})

	r := lanereader.Start(ctx, ulogger.TestLogger{}, tSettings, chain)
	require.Eventually(t, func() bool { return r.Snapshot() != nil && r.Snapshot().TipHeight() == 2 }, 10*time.Second, 10*time.Millisecond)

	// The validator answers with a record the lane index says block b1 took, mined in b1, the way
	// it answers for a record that already existed: without its lane position, which the shadow
	// reads from the store, once.
	records := &positionStore{pos: lane.Pack(3, 5)}
	server := &Server{
		logger:          ulogger.TestLogger{},
		settings:        tSettings,
		utxoStore:       records,
		validatorClient: &metaValidator{md: &meta.Data{BlockIDs: []uint32{b1}}},
	}

	tx := bt.NewTx()

	// No block IDs from the caller: judged on the main chain, which holds b1, so the row agrees.
	before := subtreeAlreadyMined(t, lanereader.KindAgree, "row")

	_, _, err = server.blessMissing(context.Background(), chainhash.Hash{}, chainhash.Hash{}, tx, 3, map[uint32]bool{}, validator.ProcessOptions())
	require.NoError(t, err)
	require.Equal(t, before+1, subtreeAlreadyMined(t, lanereader.KindAgree, "row"))
	require.Equal(t, 1, records.reads, "the position is read once, before the check, and not again at the end")

	// The caller's chain is only b2: b1 is off it for both answers, so they agree with no row.
	before = subtreeAlreadyMined(t, lanereader.KindAgree, "none")

	_, _, err = server.blessMissing(context.Background(), chainhash.Hash{}, chainhash.Hash{}, tx, 3, map[uint32]bool{b2: true}, validator.ProcessOptions())
	require.NoError(t, err, "b1 is not on the caller's chain, so the transaction is not already mined there")
	require.Equal(t, before+1, subtreeAlreadyMined(t, lanereader.KindAgree, "none"))
}
