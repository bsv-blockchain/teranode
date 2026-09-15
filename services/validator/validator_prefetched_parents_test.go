package validator

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Test_getUtxoBlockHeightsAndExtendTx_Prefetched is the Phase-1 parity test:
// when the per-level bulk reader has already fetched a tx's parents, supplying
// them via PrefetchedParents must yield IDENTICAL block heights to the per-parent
// store path AND perform ZERO `Get` calls against the store. This is the core
// correctness contract — the bulk read is a pure read-source swap, not a change
// in what the validator computes (bitcoin-expert invariants #2/#4: the data and
// the unconfirmed-parent sentinel logic are unchanged; only the source differs).
func Test_getUtxoBlockHeightsAndExtendTx_Prefetched(t *testing.T) {
	ctx := context.Background()

	// Same extended 3-parent tx used by Test_getUtxoBlockHeights.
	tx, err := bt.NewTxFromString("010000000000000000ef03fe1a25c8774c1e827f9ebdae731fe609ff159d6f7c15094e1d467a99a01e03100000000002012affffffffa086010000000000018253a080075d834402e916390940782236b29d23db6f52dfc940a12b3eff99159c0000000000ffffffffa086010000000000100f5468616e6b7320456c69676975732161e4ed95239756bbb98d11dcf973146be0c17cc1cc94340deb8bc4d44cd88e92000000000a516352676a675168948cffffffff40548900000000000763516751676a680220aa4400000000001976a9149bc0bbdd3024da4d0c38ed1aecf5c68dd1d3fa1288ac20aa4400000000001976a914169ff4804fd6596deb974f360c21584aa1e19c9788ac00000000")
	require.NoError(t, err)

	parent0, err := chainhash.NewHashFromStr("10031ea0997a461d4e09157c6f9d15ff09e61f73aebd9e7f821e4c77c8251afe")
	require.NoError(t, err)
	parent1, err := chainhash.NewHashFromStr("9c1599ff3e2ba140c9df526fdb239db236227840093916e90244835d0780a053")
	require.NoError(t, err)
	parent2, err := chainhash.NewHashFromStr("928ed84cd4c48beb0d3494ccc17cc1e06b1473f9dc118db9bb56972395ede461")
	require.NoError(t, err)

	// Identical data to the "mined parent txs" subtest of Test_getUtxoBlockHeights:
	// parent1 has empty BlockHeights (unconfirmed → sentinel).
	//
	// Every parent carries outputs: the validator re-extends unconditionally
	// (GHSA-v76m-6vc7-g7c7), so a prefetched entry without them fails the
	// prefetch guard and falls back to a per-parent store Get.
	prefetched := map[chainhash.Hash]*meta.Data{
		*parent0: {BlockHeights: []uint32{125, 126}, Tx: prefetchParentTx(1000000)},
		*parent1: {BlockHeights: []uint32{}, Tx: prefetchParentTx(2000000)},
		*parent2: {BlockHeights: []uint32{768, 769}, Tx: prefetchParentTx(3000000)},
	}

	mockUtxoStore := &utxostore.MockUtxostore{}
	v := &Validator{settings: settings.NewSettings(), utxoStore: mockUtxoStore}

	utxoHeights, err := v.getUtxoBlockHeightsAndExtendTx(ctx, tx, tx.TxID(), prefetched)
	require.NoError(t, err)

	require.Equal(t, []uint32{125, unconfirmedParentHeight, 768}, utxoHeights,
		"prefetched heights must match the per-parent store path exactly")

	mockUtxoStore.AssertNotCalled(t, "Get", mock.Anything, mock.Anything, mock.Anything)
}

// Test_getUtxoBlockHeightsAndExtendTx_PartialPrefetchFallsBackToStore proves the
// safety invariant: a parent absent from PrefetchedParents falls back to a store
// Get, so a partial prefetch never reduces correctness — the heights are identical
// to the all-store path, and only the missing parent is read.
func Test_getUtxoBlockHeightsAndExtendTx_PartialPrefetchFallsBackToStore(t *testing.T) {
	ctx := context.Background()

	tx, err := bt.NewTxFromString("010000000000000000ef03fe1a25c8774c1e827f9ebdae731fe609ff159d6f7c15094e1d467a99a01e03100000000002012affffffffa086010000000000018253a080075d834402e916390940782236b29d23db6f52dfc940a12b3eff99159c0000000000ffffffffa086010000000000100f5468616e6b7320456c69676975732161e4ed95239756bbb98d11dcf973146be0c17cc1cc94340deb8bc4d44cd88e92000000000a516352676a675168948cffffffff40548900000000000763516751676a680220aa4400000000001976a9149bc0bbdd3024da4d0c38ed1aecf5c68dd1d3fa1288ac20aa4400000000001976a914169ff4804fd6596deb974f360c21584aa1e19c9788ac00000000")
	require.NoError(t, err)

	parent0, err := chainhash.NewHashFromStr("10031ea0997a461d4e09157c6f9d15ff09e61f73aebd9e7f821e4c77c8251afe")
	require.NoError(t, err)
	parent1, err := chainhash.NewHashFromStr("9c1599ff3e2ba140c9df526fdb239db236227840093916e90244835d0780a053")
	require.NoError(t, err)
	parent2, err := chainhash.NewHashFromStr("928ed84cd4c48beb0d3494ccc17cc1e06b1473f9dc118db9bb56972395ede461")
	require.NoError(t, err)

	// Prefetch covers only parent0 and parent2; parent1 must fall back to the store.
	prefetched := map[chainhash.Hash]*meta.Data{
		*parent0: {BlockHeights: []uint32{125, 126}, Tx: prefetchParentTx(1000000)},
		*parent2: {BlockHeights: []uint32{768, 769}, Tx: prefetchParentTx(3000000)},
	}

	mockUtxoStore := &utxostore.MockUtxostore{}
	v := &Validator{settings: settings.NewSettings(), utxoStore: mockUtxoStore}

	// Only parent1 should ever be read from the store.
	mockUtxoStore.On("Get", mock.Anything, mock.MatchedBy(func(hash *chainhash.Hash) bool {
		return hash.IsEqual(parent1)
	}), mock.Anything).Return(&meta.Data{BlockHeights: []uint32{}, Tx: prefetchParentTx(2000000)}, nil).Once()

	utxoHeights, err := v.getUtxoBlockHeightsAndExtendTx(ctx, tx, tx.TxID(), prefetched)
	require.NoError(t, err)

	require.Equal(t, []uint32{125, unconfirmedParentHeight, 768}, utxoHeights)
	// parent0 and parent2 came from the prefetch; only parent1 hit the store.
	mockUtxoStore.AssertNumberOfCalls(t, "Get", 1)
}

// Test_getUtxoBlockHeightAndExtendForParentTx_InputIndexOutOfBounds guards the
// bounds check: an input index >= len(tx.Inputs) must return an out-of-bounds
// error rather than panic. The check is hoisted to the top of the function so
// it fires before the utxoHeights[idx] height-write loops (utxoHeights is sized
// to len(tx.Inputs) by the caller, so an out-of-range idx would otherwise panic
// there, before the extend path).
func Test_getUtxoBlockHeightAndExtendForParentTx_InputIndexOutOfBounds(t *testing.T) {
	ctx := context.Background()

	// Child tx with a single input, so len(tx.Inputs) == 1.
	childTx := &bt.Tx{Inputs: []*bt.Input{{}}}

	// utxoHeights sized exactly as the real caller does
	// (make([]uint32, len(tx.Inputs))), so the test exercises the true call
	// shape rather than an artificially oversized slice.
	utxoHeights := make([]uint32, len(childTx.Inputs))

	// Parent supplied via prefetch (Tx non-nil so the extend path would be
	// reached) with a recorded block height, so no store Get is needed.
	parentHash := chainhash.Hash{}
	prefetched := map[chainhash.Hash]*meta.Data{
		parentHash: {BlockHeights: []uint32{100}, Tx: &bt.Tx{Outputs: []*bt.Output{{}}}},
	}

	v := &Validator{}

	// idx == len(childTx.Inputs) would panic in the height-write loop
	// (utxoHeights[idx]) without the up-front guard.
	err := v.getUtxoBlockHeightAndExtendForParentTx(ctx, parentHash, []int{1}, utxoHeights, childTx, prefetched)
	require.Error(t, err)
	require.Contains(t, err.Error(), "out of bounds")
}

// Test_getUtxoBlockHeightAndExtendForParentTx_VoutOutOfRange guards the extend
// path against an out-of-range PreviousTxOutIndex. The vout comes from the
// (untrusted) child transaction; a raw tx that references a real parent but a
// vout beyond that parent's output count must be rejected with a clean error
// rather than panicking on txMeta.Tx.Outputs[vout] and crashing the validator.
func Test_getUtxoBlockHeightAndExtendForParentTx_VoutOutOfRange(t *testing.T) {
	ctx := context.Background()

	// Child tx with a single, validly-indexed input (idx 0) whose
	// PreviousTxOutIndex points past the parent's outputs.
	childTx := &bt.Tx{Inputs: []*bt.Input{{PreviousTxOutIndex: 99}}}
	utxoHeights := make([]uint32, len(childTx.Inputs))

	// Parent exists and is confirmed, but has only 2 outputs (vouts 0 and 1).
	parentHash := chainhash.Hash{}
	prefetched := map[chainhash.Hash]*meta.Data{
		parentHash: {BlockHeights: []uint32{100}, Tx: &bt.Tx{Outputs: []*bt.Output{{}, {}}}},
	}

	v := &Validator{}

	err := v.getUtxoBlockHeightAndExtendForParentTx(ctx, parentHash, []int{0}, utxoHeights, childTx, prefetched)
	require.Error(t, err)
	require.Contains(t, err.Error(), "has no output for index")
}

// Test_getUtxoBlockHeightAndExtendForParentTx_BodylessParent_CoinExists is test 1
// of the coin-table fallback: a body-less parent (Tx nil — the steady state once
// a body window has aged out, or for every transaction mined at or below the
// checkpoint when utxostore_skipTxBodyBelowCheckpoint is on) whose coin is still
// in the UTXO table must be filled from PreviousOutputsDecorate, and the values
// the child transaction ARRIVED carrying must be discarded even though they are
// well-formed. The mock's Run callback mirrors the real
// BatchPreviousOutputsDecorate contract precisely (skip an input that already
// has a script — see decorate.go's doc comment), so if the production code
// failed to clear the forged script/satoshis before decorating, the mock would
// skip it exactly as the real store would, and this test would see the forged
// values survive.
func Test_getUtxoBlockHeightAndExtendForParentTx_BodylessParent_CoinExists(t *testing.T) {
	ctx := context.Background()

	realScript := bscript.NewFromBytes([]byte{0x51})
	const realSats = uint64(4200)

	forgedScript := bscript.NewFromBytes([]byte{0x00, 0x00, 0x00})
	const forgedSats = uint64(999999999)

	childTx := &bt.Tx{Inputs: []*bt.Input{{
		PreviousTxOutIndex: 0,
		// Deliberately wrong: what an attacker (or a stale caller) supplied.
		PreviousTxScript:   forgedScript,
		PreviousTxSatoshis: forgedSats,
	}}}
	utxoHeights := make([]uint32, len(childTx.Inputs))
	parentHash := chainhash.Hash{0xAA}

	store := &utxostore.MockUtxostore{}
	store.On("Get", mock.Anything, &parentHash, mock.Anything).
		Return(&meta.Data{BlockHeights: []uint32{100}, Tx: nil}, nil)
	store.On("PreviousOutputsDecorate", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			scratch, ok := args.Get(1).(*bt.Tx)
			require.True(t, ok)
			for _, in := range scratch.Inputs {
				if in.PreviousTxScript != nil {
					// Mirrors decorate.go: "Inputs that already carry a script are skipped".
					continue
				}
				in.PreviousTxScript = realScript
				in.PreviousTxSatoshis = realSats
			}
		}).
		Return(nil)

	v := &Validator{utxoStore: store}

	err := v.getUtxoBlockHeightAndExtendForParentTx(ctx, parentHash, []int{0}, utxoHeights, childTx, nil)
	require.NoError(t, err)
	require.Equal(t, uint32(100), utxoHeights[0])
	require.Equal(t, realSats, childTx.Inputs[0].PreviousTxSatoshis,
		"the forged satoshis the tx arrived with must be discarded in favour of the store's")
	require.Equal(t, realScript.Bytes(), childTx.Inputs[0].PreviousTxScript.Bytes(),
		"the forged script the tx arrived with must be discarded in favour of the store's")
}

// Test_getUtxoBlockHeightAndExtendForParentTx_BodylessParent_CoinGone is test 2:
// a body-less parent whose coin is no longer in the UTXO table (spent, or never
// created) must classify as a missing parent — the same errors.ErrTxNotFound
// code extendTransaction already maps to TxMissingParent — not as
// "has no output for index", which would send an operator hunting a malformed
// transaction that does not exist. The distinct "body is not retained" message
// must still be present so an operator can tell this apart from an ordinary
// missing-parent lookup failure.
func Test_getUtxoBlockHeightAndExtendForParentTx_BodylessParent_CoinGone(t *testing.T) {
	ctx := context.Background()

	childTx := &bt.Tx{Inputs: []*bt.Input{{PreviousTxOutIndex: 0}}}
	utxoHeights := make([]uint32, len(childTx.Inputs))
	parentHash := chainhash.Hash{}

	store := &utxostore.MockUtxostore{}
	store.On("Get", mock.Anything, &parentHash, mock.Anything).
		Return(&meta.Data{BlockHeights: []uint32{100}, Tx: nil}, nil)
	store.On("PreviousOutputsDecorate", mock.Anything, mock.Anything).
		Return(errors.NewTxNotFoundError("[utxoset][BatchPreviousOutputsDecorate] 1 of 1 parent outputs not in the utxo set (spent or never created)"))

	v := &Validator{utxoStore: store}

	err := v.getUtxoBlockHeightAndExtendForParentTx(ctx, parentHash, []int{0}, utxoHeights, childTx, nil)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxNotFound),
		"a coin absent from the table must classify the same way extendTransaction classifies a missing parent")
	require.Contains(t, err.Error(), "its body is not retained by this node")
	require.Contains(t, err.Error(), "utxostore_skipTxBodyBelowCheckpoint")
	require.NotContains(t, err.Error(), "has no output for index",
		"must not be confused with the out-of-range-vout error, which names a real but malformed transaction")
}

// Test_getUtxoBlockHeightAndExtendForParentTx_BodylessParent_CoinReassigned is
// test 3: a body-less parent whose coin was reassigned (ReAssignUTXO) must fail
// as a processing error, not a missing parent — waiting for a reassigned coin
// never makes it decoratable, unlike a coin that has simply not arrived yet.
// BatchPreviousOutputsDecorate's own doc comment is explicit that this case
// must never be silent: an empty script must never reach the caller.
func Test_getUtxoBlockHeightAndExtendForParentTx_BodylessParent_CoinReassigned(t *testing.T) {
	ctx := context.Background()

	childTx := &bt.Tx{Inputs: []*bt.Input{{PreviousTxOutIndex: 0}}}
	utxoHeights := make([]uint32, len(childTx.Inputs))
	parentHash := chainhash.Hash{0xBB}

	store := &utxostore.MockUtxostore{}
	store.On("Get", mock.Anything, &parentHash, mock.Anything).
		Return(&meta.Data{BlockHeights: []uint32{100}, Tx: nil}, nil)
	// Mirrors decorate.go's own behaviour for a reassigned coin: left
	// undecorated (no script set) and reported as a processing error, never
	// as ErrTxNotFound.
	store.On("PreviousOutputsDecorate", mock.Anything, mock.Anything).
		Return(errors.NewProcessingError("[utxoset][BatchPreviousOutputsDecorate] 1 of 1 parent outputs were reassigned"))

	v := &Validator{utxoStore: store}

	err := v.getUtxoBlockHeightAndExtendForParentTx(ctx, parentHash, []int{0}, utxoHeights, childTx, nil)
	require.Error(t, err)
	require.False(t, errors.Is(err, errors.ErrTxNotFound),
		"a reassigned coin never becomes decoratable, so it must not be classified as a fetchable missing parent")
	require.Contains(t, err.Error(), "its body is not retained by this node")
	require.Nil(t, childTx.Inputs[0].PreviousTxScript,
		"an empty script must never reach the caller")
}

// Test_getUtxoBlockHeightAndExtendForParentTx_DecorateReportsSuccessButLeavesGap
// pins the defensive insurance check: if PreviousOutputsDecorate ever returned
// nil while actually leaving an input undecorated (a hypothetical regression in
// the store, not something the real implementation does today), the validator
// must still refuse rather than let a nil-script input through into signature
// verification.
func Test_getUtxoBlockHeightAndExtendForParentTx_DecorateReportsSuccessButLeavesGap(t *testing.T) {
	ctx := context.Background()

	childTx := &bt.Tx{Inputs: []*bt.Input{{PreviousTxOutIndex: 0}}}
	utxoHeights := make([]uint32, len(childTx.Inputs))
	parentHash := chainhash.Hash{0xCC}

	store := &utxostore.MockUtxostore{}
	store.On("Get", mock.Anything, &parentHash, mock.Anything).
		Return(&meta.Data{BlockHeights: []uint32{100}, Tx: nil}, nil)
	// Reports success (nil error) but never touches the scratch input's script
	// — the regression scenario the defensive check exists for.
	store.On("PreviousOutputsDecorate", mock.Anything, mock.Anything).Return(nil)

	v := &Validator{utxoStore: store}

	err := v.getUtxoBlockHeightAndExtendForParentTx(ctx, parentHash, []int{0}, utxoHeights, childTx, nil)
	require.Error(t, err)
	require.Nil(t, childTx.Inputs[0].PreviousTxScript,
		"an empty script must never reach the caller even if decorate lies about success")
}

// Test_getUtxoBlockHeightsAndExtendTx_MixedBodyfulAndBodylessRace is test 4: run
// under `-race`. Several distinct parents are resolved concurrently by the real
// errgroup in getUtxoBlockHeightsAndExtendTx — one WITH a body (the ordinary
// extend path) and two body-less (the coin-table fallback, resolved by two
// different goroutines racing over the SAME shared tx at the same time). Each
// goroutine must only ever read and write the inputs of the parent it owns.
// GetBatcherSize is raised so the errgroup actually runs the three lookups
// concurrently rather than serialising them behind a limit of 1.
func Test_getUtxoBlockHeightsAndExtendTx_MixedBodyfulAndBodylessRace(t *testing.T) {
	ctx := context.Background()

	parentWithBody := chainhash.Hash{0x01}
	parentBodyless1 := chainhash.Hash{0x02}
	parentBodyless2 := chainhash.Hash{0x03}

	tx := &bt.Tx{}
	addInput := func(parent chainhash.Hash, vout uint32) {
		in := &bt.Input{PreviousTxOutIndex: vout, SequenceNumber: 0xffffffff, UnlockingScript: bscript.NewFromBytes([]byte{})}
		require.NoError(t, in.PreviousTxIDAdd(&parent))
		tx.Inputs = append(tx.Inputs, in)
	}
	addInput(parentWithBody, 0)
	addInput(parentBodyless1, 0)
	addInput(parentBodyless2, 0)

	store := &utxostore.MockUtxostore{}
	store.On("Get", mock.Anything, mock.MatchedBy(func(h *chainhash.Hash) bool { return h.IsEqual(&parentWithBody) }), mock.Anything).
		Return(&meta.Data{BlockHeights: []uint32{111}, Tx: prefetchParentTx(1111)}, nil)
	store.On("Get", mock.Anything, mock.MatchedBy(func(h *chainhash.Hash) bool { return h.IsEqual(&parentBodyless1) }), mock.Anything).
		Return(&meta.Data{BlockHeights: []uint32{222}, Tx: nil}, nil)
	store.On("Get", mock.Anything, mock.MatchedBy(func(h *chainhash.Hash) bool { return h.IsEqual(&parentBodyless2) }), mock.Anything).
		Return(&meta.Data{BlockHeights: []uint32{333}, Tx: nil}, nil)

	script1 := bscript.NewFromBytes([]byte{0x52})
	script2 := bscript.NewFromBytes([]byte{0x53})

	store.On("PreviousOutputsDecorate", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			scratch, ok := args.Get(1).(*bt.Tx)
			require.True(t, ok)
			require.Len(t, scratch.Inputs, 1, "each parent's scratch tx must hold only its own input")

			switch {
			case scratch.Inputs[0].PreviousTxIDChainHash().IsEqual(&parentBodyless1):
				scratch.Inputs[0].PreviousTxScript = script1
				scratch.Inputs[0].PreviousTxSatoshis = 2000
			case scratch.Inputs[0].PreviousTxIDChainHash().IsEqual(&parentBodyless2):
				scratch.Inputs[0].PreviousTxScript = script2
				scratch.Inputs[0].PreviousTxSatoshis = 3000
			default:
				t.Fatalf("unexpected parent in scratch tx: %s", scratch.Inputs[0].PreviousTxIDChainHash())
			}
		}).
		Return(nil)

	tSettings := settings.NewSettings()
	tSettings.UtxoStore.GetBatcherSize = 8

	v := &Validator{settings: tSettings, utxoStore: store}

	utxoHeights, err := v.getUtxoBlockHeightsAndExtendTx(ctx, tx, "race-test-tx", nil)
	require.NoError(t, err)
	require.Equal(t, []uint32{111, 222, 333}, utxoHeights)

	require.Equal(t, uint64(1111), tx.Inputs[0].PreviousTxSatoshis)
	require.Equal(t, []byte{0x51}, tx.Inputs[0].PreviousTxScript.Bytes())

	require.Equal(t, uint64(2000), tx.Inputs[1].PreviousTxSatoshis)
	require.Equal(t, script1.Bytes(), tx.Inputs[1].PreviousTxScript.Bytes())

	require.Equal(t, uint64(3000), tx.Inputs[2].PreviousTxSatoshis)
	require.Equal(t, script2.Bytes(), tx.Inputs[2].PreviousTxScript.Bytes())
}

// prefetchParentTx builds the minimal parent metadata the unconditional
// re-extension path needs: a single spendable output at vout 0.
func prefetchParentTx(satoshis uint64) *bt.Tx {
	return &bt.Tx{
		Outputs: []*bt.Output{{Satoshis: satoshis, LockingScript: bscript.NewFromBytes([]byte{0x51})}},
	}
}
