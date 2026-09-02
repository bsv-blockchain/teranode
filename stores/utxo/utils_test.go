// Package utxo provides UTXO (Unspent Transaction Output) management for the BSV Blockchain Teranode implementation.
package utxo

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	tx, _         = bt.NewTxFromString("010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")
	hash1, _      = chainhash.NewHashFromStr("5cee463416702311eace06a42e700f3d95ee7793d3ae52af9c051a4981e8345a")
	hash2, _      = chainhash.NewHashFromStr("b067b2d2a51cb3f63678cc2bf12efaa5d57235d296bcba09ead42f4147b63bf7")
	hash3, _      = chainhash.NewHashFromStr("0ab59604a1c249d0cbfe18f01fe423df3035840f9a609395ccd177d2b217cae6")
	hash4, _      = chainhash.NewHashFromStr("08c3d6e8388415d8f6190a40c0acb9328b41a89a5854468e62c2bbd1dc740460")
	hash5, _      = chainhash.NewHashFromStr("72629cff00e9f33dc7a96976717b7c86d4d168252c3550d3f24ae9f7bbe5cc68")
	utxoHashesMap = map[chainhash.Hash]struct{}{
		*hash1: {},
		*hash2: {},
		*hash3: {},
		*hash4: {},
		*hash5: {},
	}
)

func TestGetFeesAndUtxoHashes(t *testing.T) {
	t.Run("should return fees and utxo hashes", func(t *testing.T) {
		fees, utxoHashes, err := GetFeesAndUtxoHashes(context.Background(), tx, chaincfg.GenesisActivationHeight)
		require.NoError(t, err)

		assert.Equal(t, uint64(215), fees)
		assert.Equal(t, 5, len(utxoHashes))

		createdUtxoHashesMap := make(map[chainhash.Hash]struct{})

		for _, utxoHash := range utxoHashes {
			_, ok := utxoHashesMap[*utxoHash]
			assert.True(t, ok, "utxo hash not found in map: "+utxoHash.String())

			createdUtxoHashesMap[*utxoHash] = struct{}{}
		}

		for utxoHash := range utxoHashesMap {
			_, ok := createdUtxoHashesMap[utxoHash]
			assert.True(t, ok, "utxo hash not found in created map: "+utxoHash.String())
		}
	})
}

func TestCalculateUtxoStatus(t *testing.T) {
	// Test case when spendingTxId is not nil
	spendingTxId, _ := chainhash.NewHashFromStr("b067b2d2a51cb3f63678cc2bf12efaa5d57235d296bcba09ead42f4147b63bf7")
	spendingData := spendpkg.NewSpendingData(spendingTxId, 0)

	status := CalculateUtxoStatus(spendingData, 0, 0)
	assert.Equal(t, Status_SPENT, status)

	// Test case when lockTime is greater than 0 and less than 500000000 and greater than blockHeight
	status = CalculateUtxoStatus(nil, 400000000, 300000000)
	assert.Equal(t, Status_IMMATURE, status)

	// Test case when lockTime is greater than or equal to 500000000 and greater than current Unix time
	status = CalculateUtxoStatus(nil, uint32(time.Now().Add(1*time.Hour).Unix()), 0)
	assert.Equal(t, Status_IMMATURE, status)

	// Test case when spendingTxId is nil and lockTime is 0
	status = CalculateUtxoStatus(nil, 0, 0)
	assert.Equal(t, Status_OK, status)
}

func TestGetUtxoHashes(t *testing.T) {
	t.Run("should return utxo hashes", func(t *testing.T) {
		utxoHashes, err := GetUtxoHashes(tx)
		require.NoError(t, err)

		assert.Equal(t, 5, len(utxoHashes))

		createdUtxoHashesMap := make(map[chainhash.Hash]struct{})

		for _, utxoHash := range utxoHashes {
			_, ok := utxoHashesMap[*utxoHash]
			assert.True(t, ok, "utxo hash not found in map: "+utxoHash.String())

			createdUtxoHashesMap[*utxoHash] = struct{}{}
		}

		for utxoHash := range utxoHashesMap {
			_, ok := createdUtxoHashesMap[utxoHash]
			assert.True(t, ok, "utxo hash not found in created map: "+utxoHash.String())
		}
	})
}

func TestShouldStoreNonZeroUTXO(t *testing.T) {
	txID := "956685dffd466d3051c8372c4f3bdf0e061775ed054d7e8f0bc5695ca747d604"
	tx, err := bt.NewTxFromString("010000000000000000ef015400c3490d91f3f742e73e81bc37dfca4f24f9a73a17c90ccab3012ddbc795bb000000008a473044022006a960f73ea637af867f69ed69edd291bee1d6daec241649caf909fb864dcd3b022011c82189c4a3379aba85fdb907d341db8067e426d7660fbba05c12fa370fa8aa0141048e69627b4807fe4ab00002a01c4a26a50d558cce969708e75dc5bfb345bbe92f06082757c85cbcac4ff0bbb91e221c59d3f9e675125da07e8110fd7d9b0ab6eeffffffff00000000000000001976a9146f9e896bb7cd9d27ca5b18c3ec9587ff0be7895188ac0100000000000000001976a9144477154cba7f0474a578fe734e00bd60513fbab588ac00000000")
	require.NoError(t, err)
	require.Equal(t, txID, tx.TxIDChainHash().String())

	t.Run("should return true for non-zero UTXO", func(t *testing.T) {
		const genesisActivation = uint32(620538)
		assert.True(t, ShouldStoreOutputAsUTXO(tx.Outputs[0], genesisActivation-1, genesisActivation))
		assert.True(t, ShouldStoreOutputAsUTXO(tx.Outputs[0], genesisActivation+1, genesisActivation))
	})
}

// TestShouldStoreOutputAsUTXO_EraAware pins the era-aware UTXO-set membership
// rule against SV Node's CScript::IsUnspendable(era) (bitcoin-sv
// src/script/script.h:265-281). The era is keyed to the output's CREATION
// (mining) height, compared against the network-specific Genesis activation
// height, matching SV Node's AddCoin gate (coins.cpp:51, IsUnspendable(
// coin.GetHeight() >= genesisActivationHeight)). The rule is value-agnostic,
// like SV Node: a value-bearing but provably-unspendable output (e.g. an
// OP_FALSE OP_RETURN carrying satoshis) is burned and excluded from the set.
//
// The activation height is driven per-case as a parameter (NOT the mainnet
// chaincfg global) so non-mainnet networks — whose real activation is below
// 620538 — are exercised: a 0-value bare OP_RETURN created at a height >=
// activation but < 620538 must be STORED. That is the over-exclusion the global
// would silently reintroduce on teratestnet/tstn/stn/regtest.
func TestShouldStoreOutputAsUTXO_EraAware(t *testing.T) {
	const (
		mainnetGenesis = uint32(620538)
		ttnGenesis     = uint32(1)   // teratestnet / tstn
		stnGenesis     = uint32(100) // stn
		// Not the live regtest height (which has moved); these cases pass
		// genesisActivation to ShouldStoreOutputAsUTXO explicitly, so the value
		// only has to be consistent with the heights used in the table below.
		regtestGenesis = uint32(10000)
	)

	mkScript := func(b []byte) *bscript.Script {
		s := bscript.Script(b)
		return &s
	}

	bareOpReturn := []byte{bscript.OpRETURN, 0x04, 0xde, 0xad, 0xbe, 0xef}
	opFalseOpReturn := []byte{bscript.OpFALSE, bscript.OpRETURN, 0x04, 0xde, 0xad, 0xbe, 0xef}
	p2pkh := []byte{0x76, 0xa9, 0x14, 0x89, 0xab, 0xcd, 0xef, 0xab, 0xba, 0xab, 0xba, 0xab, 0xba, 0xab, 0xba, 0xab, 0xba, 0xab, 0xba, 0xab, 0xba, 0xab, 0xba, 0x88, 0xac}

	oversized := make([]byte, 10001) // > maxScriptSizeBeforeGenesis (10000)
	oversized[0] = 0x76              // OP_DUP — not OP_RETURN / OP_FALSE OP_RETURN

	exactly10000 := make([]byte, 10000) // boundary: == limit, NOT oversized
	exactly10000[0] = 0x76

	tests := []struct {
		name              string
		satoshis          uint64
		script            []byte
		blockHeight       uint32
		genesisActivation uint32
		want              bool
	}{
		// --- mainnet activation (620538) ---
		{"p2pkh_value_pregenesis", 1000, p2pkh, mainnetGenesis - 1, mainnetGenesis, true},
		{"p2pkh_value_postgenesis", 1000, p2pkh, mainnetGenesis + 1, mainnetGenesis, true},
		{"p2pkh_zero_pregenesis", 0, p2pkh, mainnetGenesis - 1, mainnetGenesis, true},
		{"p2pkh_zero_postgenesis", 0, p2pkh, mainnetGenesis + 1, mainnetGenesis, true},
		{"bare_opreturn_zero_pregenesis", 0, bareOpReturn, mainnetGenesis - 1, mainnetGenesis, false},
		// THE dangerous over-exclusion case: post-Genesis bare OP_RETURN is
		// spendable; SV Node keeps it. Must be stored.
		{"bare_opreturn_zero_postgenesis_DANGEROUS", 0, bareOpReturn, mainnetGenesis + 1, mainnetGenesis, true},
		{"bare_opreturn_zero_at_genesis_boundary", 0, bareOpReturn, mainnetGenesis, mainnetGenesis, true},
		// value-agnostic: a value-bearing bare OP_RETURN is still spendable
		// post-Genesis, so it is kept.
		{"bare_opreturn_value_postgenesis", 1000, bareOpReturn, mainnetGenesis + 1, mainnetGenesis, true},
		// value-agnostic: value-bearing provably-unspendable outputs are BURNED
		// and excluded, matching SV Node IsUnspendable (value plays no role).
		{"opfalse_opreturn_value_postgenesis_BURNED", 1000, opFalseOpReturn, mainnetGenesis + 1, mainnetGenesis, false},
		{"opfalse_opreturn_value_pregenesis_BURNED", 1000, opFalseOpReturn, mainnetGenesis - 1, mainnetGenesis, false},
		{"bare_opreturn_value_pregenesis_BURNED", 1000, bareOpReturn, mainnetGenesis - 1, mainnetGenesis, false},
		{"oversized_value_pregenesis_BURNED", 1000, oversized, mainnetGenesis - 1, mainnetGenesis, false},
		{"opfalse_opreturn_zero_pregenesis", 0, opFalseOpReturn, mainnetGenesis - 1, mainnetGenesis, false},
		{"opfalse_opreturn_zero_postgenesis", 0, opFalseOpReturn, mainnetGenesis + 1, mainnetGenesis, false},
		{"opfalse_opreturn_zero_at_genesis_boundary", 0, opFalseOpReturn, mainnetGenesis, mainnetGenesis, false},
		{"oversized_non_opreturn_zero_pregenesis", 0, oversized, mainnetGenesis - 1, mainnetGenesis, false},
		{"oversized_non_opreturn_zero_postgenesis", 0, oversized, mainnetGenesis + 1, mainnetGenesis, true},
		{"oversized_exactly_10000_pregenesis", 0, exactly10000, mainnetGenesis - 1, mainnetGenesis, true},
		{"empty_script_zero_pregenesis", 0, []byte{}, mainnetGenesis - 1, mainnetGenesis, true},
		{"empty_script_zero_postgenesis", 0, []byte{}, mainnetGenesis + 1, mainnetGenesis, true},

		// --- non-mainnet activation: the regression the mainnet global hid ---
		// teratestnet activation=1: height 50 is post-Genesis, so a 0-value bare
		// OP_RETURN must be STORED (the mainnet global would treat 50 as
		// pre-Genesis and wrongly drop it).
		{"ttn_bare_opreturn_zero_postgenesis_STORED", 0, bareOpReturn, 50, ttnGenesis, true},
		{"ttn_bare_opreturn_at_activation_boundary", 0, bareOpReturn, ttnGenesis, ttnGenesis, true},
		{"ttn_opfalse_opreturn_zero_postgenesis", 0, opFalseOpReturn, 50, ttnGenesis, false},
		// stn activation=100: height 50 is genuinely pre-Genesis => dropped.
		{"stn_bare_opreturn_zero_pregenesis", 0, bareOpReturn, 50, stnGenesis, false},
		{"stn_bare_opreturn_zero_postgenesis_STORED", 0, bareOpReturn, 150, stnGenesis, true},
		// regtestGenesis (above): height 15000 is post-Genesis => stored.
		{"regtest_bare_opreturn_zero_postgenesis_STORED", 0, bareOpReturn, 15000, regtestGenesis, true},
		{"regtest_oversized_zero_pregenesis", 0, oversized, 5000, regtestGenesis, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output := &bt.Output{Satoshis: tt.satoshis, LockingScript: mkScript(tt.script)}
			got := ShouldStoreOutputAsUTXO(output, tt.blockHeight, tt.genesisActivation)
			require.Equal(t, tt.want, got)
		})
	}
}

// TestShouldStoreOutputAsUTXO_NilLockingScript guards the deref of
// *output.LockingScript: a nil locking script must be treated as an empty
// (non-OP_RETURN, non-oversized) script and stored, never panic. Realistic
// outputs always carry a non-nil script, but not every caller guards it.
func TestShouldStoreOutputAsUTXO_NilLockingScript(t *testing.T) {
	const genesisActivation = uint32(620538)

	for _, sats := range []uint64{0, 1000} {
		for _, h := range []uint32{genesisActivation - 1, genesisActivation + 1} {
			output := &bt.Output{Satoshis: sats, LockingScript: nil}
			require.NotPanics(t, func() {
				require.True(t, ShouldStoreOutputAsUTXO(output, h, genesisActivation))
			})
		}
	}
}

func buildExtendedTx(t *testing.T, nOutputs int) *bt.Tx {
	t.Helper()
	tx := bt.NewTx()
	err := tx.From(
		"4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b",
		0,
		"76a914000000000000000000000000000000000000000088ac",
		uint64(nOutputs)*1000,
	)
	require.NoError(t, err)
	script, err := bscript.NewFromHexString("76a914000000000000000000000000000000000000000088ac")
	require.NoError(t, err)
	for i := 0; i < nOutputs; i++ {
		tx.AddOutput(&bt.Output{Satoshis: 1, LockingScript: script})
	}
	return tx
}

func TestGetUtxoHashes_AllocsAreConstant(t *testing.T) {
	small := buildExtendedTx(t, 1)
	large := buildExtendedTx(t, 64)

	allocsSmall := testing.AllocsPerRun(50, func() {
		_, err := GetUtxoHashes(small)
		require.NoError(t, err)
	})
	allocsLarge := testing.AllocsPerRun(50, func() {
		_, err := GetUtxoHashes(large)
		require.NoError(t, err)
	})

	// O(1) in output count: large (64 outputs) allocates no more than small.
	require.LessOrEqual(t, allocsLarge, allocsSmall)
	// 4 fixed allocs: scratch buf, hashes backing array, utxoHashes slice, TxIDChainHash heap escape.
	require.LessOrEqual(t, allocsSmall, float64(4))
}

func TestGetUtxoHashes_ValuesUnchanged(t *testing.T) {
	tx := buildExtendedTx(t, 4)
	got, err := GetUtxoHashes(tx)
	require.NoError(t, err)
	txid := tx.TxIDChainHash()
	for i, output := range tx.Outputs {
		want, err := util.UTXOHashFromOutput(txid, output, uint32(i))
		require.NoError(t, err)
		require.Equal(t, *want, *got[i])
	}
}

func TestHasNoSpendableOutputs(t *testing.T) {
	// h is pre-Genesis (genesis = 620538), so bare OP_RETURN and OP_FALSE OP_RETURN
	// are both provably unspendable here - the era-aware predicate matches the
	// expectations below.
	const h = uint32(100)
	const genesisActivation = uint32(620538)

	t.Run("op_return only returns true", func(t *testing.T) {
		tx := bt.NewTx()
		require.NoError(t, tx.AddOpReturnOutput([]byte("data")))
		assert.True(t, HasNoSpendableOutputs(tx, false, h, genesisActivation))
	})

	t.Run("spendable output returns false", func(t *testing.T) {
		tx := bt.NewTx()
		tx.AddOutput(&bt.Output{Satoshis: 1000, LockingScript: &bscript.Script{}})
		assert.False(t, HasNoSpendableOutputs(tx, false, h, genesisActivation))
	})

	t.Run("mixed spendable and op_return returns false", func(t *testing.T) {
		tx := bt.NewTx()
		tx.AddOutput(&bt.Output{Satoshis: 1000, LockingScript: &bscript.Script{}})
		require.NoError(t, tx.AddOpReturnOutput([]byte("data")))
		assert.False(t, HasNoSpendableOutputs(tx, false, h, genesisActivation))
	})

	t.Run("coinbase returns false", func(t *testing.T) {
		tx := bt.NewTx()
		require.NoError(t, tx.AddOpReturnOutput([]byte("data")))
		assert.False(t, HasNoSpendableOutputs(tx, true, h, genesisActivation))
	})

	t.Run("no outputs returns false", func(t *testing.T) {
		tx := bt.NewTx()
		assert.False(t, HasNoSpendableOutputs(tx, false, h, genesisActivation))
	})

	t.Run("nil output slot treated as not spendable", func(t *testing.T) {
		tx := bt.NewTx()
		tx.Outputs = []*bt.Output{nil}
		assert.True(t, HasNoSpendableOutputs(tx, false, h, genesisActivation))
	})

	t.Run("bare op_return returns true", func(t *testing.T) {
		s, err := bscript.NewFromASM("OP_RETURN")
		require.NoError(t, err)
		tx := bt.NewTx()
		tx.AddOutput(&bt.Output{Satoshis: 0, LockingScript: s})
		assert.True(t, HasNoSpendableOutputs(tx, false, h, genesisActivation))
	})
}

func BenchmarkGetUtxoHashes(b *testing.B) {
	txs := make([]*bt.Tx, b.N)

	for i := 0; i < b.N; i++ {
		tx := bt.NewTx()
		_ = tx.From(
			"5cee463416702311eace06a42e700f3d95ee7793d3ae52af9c051a4981e8345a",
			uint32(i),
			"76a914eb0bd5edba389198e73f8efabddfc61666969d1688ac",
			uint64(i),
		)
		txs[i] = tx
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := GetUtxoHashes(txs[i])
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestGetSpendsOutpointOnly_NoDecorateDependency(t *testing.T) {
	// A NON-extended tx: inputs have a parent outpoint but no PreviousTxScript/Satoshis.
	parent := chainhash.HashH([]byte("parent"))
	tx := bt.NewTx()
	require.NoError(t, tx.From(parent.String(), 0, "", 0)) // outpoint only, empty script, 0 sats

	spends, err := GetSpendsOutpointOnly(tx)
	require.NoError(t, err)
	require.Len(t, spends, 1)
	require.Equal(t, uint32(0), spends[0].Vout)
	require.Equal(t, parent.String(), spends[0].TxID.String())
	require.NotNil(t, spends[0].UTXOHash, "must be a non-nil pointer so spend.UTXOHash[:] never panics")
	require.Equal(t, chainhash.Hash{}, *spends[0].UTXOHash, "outpoint-only spends carry the zero hash")
	require.NotNil(t, spends[0].SpendingData)
}

func BenchmarkGetUtxoHashes_ManyOutputs(b *testing.B) {
	// Create a mock transaction with 1000 outputs
	tx := bt.NewTx()
	for i := 0; i < 1000; i++ {
		_ = tx.From(
			"5cee463416702311eace06a42e700f3d95ee7793d3ae52af9c051a4981e8345a",
			uint32(i),
			"76a914eb0bd5edba389198e73f8efabddfc61666969d1688ac",
			uint64(i),
		)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := GetUtxoHashes(tx)
		if err != nil {
			b.Fatal(err)
		}
	}
}
