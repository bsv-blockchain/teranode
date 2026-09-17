package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/require"
)

// newOwnerScript is a locking script that is not the one mkTx writes, so a coin reassigned to
// it can be told apart from the coin as created.
func newOwnerScript(t *testing.T) *bscript.Script {
	t.Helper()

	script, err := bscript.NewFromHexString("76a914111111111111111111111111111111111111111188ac")
	require.NoError(t, err)

	return script
}

// TestReAssignRefusesACoinThatWasNeverFrozen pins the order the alert system has to work in.
//
// Freezing first is not paperwork. The freeze is what stops the CURRENT owner spending the
// coin while the reassignment is being written, and this store enforces it as a predicate on
// the same UPDATE rather than as a prior read, so there is no window between the two. A store
// that reassigned an unfrozen coin would race the owner's own spend and could lose.
//
// The refusal has to be the frozen error specifically, not a generic miss: the alert system
// distinguishes "you skipped the freeze" from "there is no such coin", and an operator sent
// looking for a missing coin that is sitting right there loses the incident.
func TestReAssignRefusesACoinThatWasNeverFrozen(t *testing.T) {
	s, ctx := newTestStore(t)
	tSettings := settings.NewSettings()

	require.NoError(t, s.SetBlockHeight(100))

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	old := &utxo.Spend{TxID: parent.TxIDChainHash(), Vout: 0}

	newHash, err := util.UTXOHashFromOutput(parent.TxIDChainHash(),
		&bt.Output{Satoshis: 5_000, LockingScript: newOwnerScript(t)}, 0)
	require.NoError(t, err)

	err = s.ReAssignUTXO(ctx, old, &utxo.Spend{TxID: old.TxID, Vout: 0, UTXOHash: newHash}, tSettings)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrFrozen), "got %v", err)

	// Nothing was written: the coin is still spendable by its owner on the original terms.
	spends, err := spendOnly(ctx, s, spendOneOutputTx(t, parent, 0), 101)
	require.NoError(t, err)
	require.NoError(t, spends[0].Err)
}

// TestReAssignRefusesACoinThatIsNotThere is the other half of the same refusal, and it must
// not be silence. A reassignment that quietly affected no rows would leave the alert system
// believing a court-ordered confiscation had been applied when nothing had.
//
// The coin here was spent rather than never created, which is the case that actually happens:
// delete-on-spend means a spent coin leaves no row at all, so the freeze that should have
// preceded this would have failed too.
func TestReAssignRefusesACoinThatIsNotThere(t *testing.T) {
	s, ctx := newTestStore(t)
	tSettings := settings.NewSettings()

	require.NoError(t, s.SetBlockHeight(100))

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	_, err = spendOnly(ctx, s, spendOneOutputTx(t, parent, 0), 101)
	require.NoError(t, err)

	newHash, err := util.UTXOHashFromOutput(parent.TxIDChainHash(),
		&bt.Output{Satoshis: 5_000, LockingScript: newOwnerScript(t)}, 0)
	require.NoError(t, err)

	err = s.ReAssignUTXO(ctx, &utxo.Spend{TxID: parent.TxIDChainHash(), Vout: 0},
		&utxo.Spend{TxID: parent.TxIDChainHash(), Vout: 0, UTXOHash: newHash}, tSettings)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxNotFound), "got %v", err)
}

// TestReAssignInvertsWhoMaySpendTheCoin is the property the whole feature exists for, and the
// one this store has to reach differently from every other.
//
// The other stores keep a UTXO hash as the coin's identity and reassignment overwrites it.
// This one keeps the satoshis and the locking script themselves, because its spend is also its
// decorate fetch, and ReAssignUTXO is handed only a hash -- there is no new script in the
// argument to write. So the row keeps the OLD owner's script and hash_override carries what
// the new output must hash to, and the spend path switches from comparing values to comparing
// the digest for exactly the coins that have one.
//
// The test spends past the delay in both directions. The old owner's claim is the script the
// coin still literally carries and must be refused; the new owner's matches nothing on the row
// and must be accepted. Getting this backwards is not a failed spend, it is the confiscated
// coin going back to the party it was taken from.
func TestReAssignInvertsWhoMaySpendTheCoin(t *testing.T) {
	s, ctx := newTestStore(t)
	tSettings := settings.NewSettings()

	require.NoError(t, s.SetBlockHeight(100))

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	old := &utxo.Spend{TxID: parent.TxIDChainHash(), Vout: 0}
	newOutput := &bt.Output{Satoshis: 5_000, LockingScript: newOwnerScript(t)}

	newHash, err := util.UTXOHashFromOutput(parent.TxIDChainHash(), newOutput, 0)
	require.NoError(t, err)

	require.NoError(t, s.FreezeUTXOs(ctx, []*utxo.Spend{old}, tSettings))
	require.NoError(t, s.ReAssignUTXO(ctx, old,
		&utxo.Spend{TxID: old.TxID, Vout: 0, UTXOHash: newHash}, tSettings))

	// Inside the delay nobody spends it, whichever output they claim. The delay is the window
	// in which the reassignment itself can be challenged.
	spends, err := spendOnly(ctx, s, spendOneOutputTx(t, parent, 0), 101)
	require.Error(t, err)
	// ErrFrozen specifically, not a plain processing error and not ErrTxLocked. The
	// reassignment delay is the alert system's hold, and the shared rollback predicate lists
	// ErrFrozen: classified as anything else, a multi-input transaction failing on one held
	// input strands its other inputs marked spent by a transaction that can never be accepted.
	require.True(t, errors.Is(spends[0].Err, errors.ErrFrozen), "got %v", spends[0].Err)

	spendable := 100 + tSettings.UtxoStore.ReAssignedUtxoSpendableAfterBlocks
	require.NoError(t, s.SetBlockHeight(spendable))

	// The old owner, past the delay, offering the script the row still holds: refused.
	spends, err = spendOnly(ctx, s, spendOneOutputTx(t, parent, 0), spendable)
	require.Error(t, err)
	require.True(t, errors.Is(spends[0].Err, errors.ErrUtxoHashMismatch), "got %v", spends[0].Err)

	// The new owner, offering the output the coin was reassigned to: taken.
	claimed := spendOneOutputTx(t, parent, 0)
	claimed.Inputs[0].PreviousTxSatoshis = newOutput.Satoshis
	claimed.Inputs[0].PreviousTxScript = newOutput.LockingScript

	spends, err = spendOnly(ctx, s, claimed, spendable)
	require.NoError(t, err)
	require.NoError(t, spends[0].Err)

	// The decorate left the new owner's output in place rather than overwriting it with the
	// confiscated one, which is what script validation reads next.
	require.Equal(t, newOutput.LockingScript.String(), claimed.Inputs[0].PreviousTxScript.String())

	require.Equal(t, 0, coinCount(t, s, ctx, parent))
}

// TestReAssignedCoinAnswersOnlyToItsNewHash covers the read side. GetSpend is how the alert
// system and the RPC surface confirm a confiscation landed, so the old owner asking about the
// outpoint by the hash they used to own must be refused rather than shown a live coin, and the
// new owner must see the coin held by the delay rather than reported spendable.
func TestReAssignedCoinAnswersOnlyToItsNewHash(t *testing.T) {
	s, ctx := newTestStore(t)
	tSettings := settings.NewSettings()

	require.NoError(t, s.SetBlockHeight(100))

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	oldHash, err := util.UTXOHashFromOutput(parent.TxIDChainHash(), parent.Outputs[0], 0)
	require.NoError(t, err)

	newHash, err := util.UTXOHashFromOutput(parent.TxIDChainHash(),
		&bt.Output{Satoshis: 5_000, LockingScript: newOwnerScript(t)}, 0)
	require.NoError(t, err)

	old := &utxo.Spend{TxID: parent.TxIDChainHash(), Vout: 0, UTXOHash: oldHash}

	require.NoError(t, s.FreezeUTXOs(ctx, []*utxo.Spend{old}, tSettings))
	require.NoError(t, s.ReAssignUTXO(ctx, old,
		&utxo.Spend{TxID: old.TxID, Vout: 0, UTXOHash: newHash}, tSettings))

	_, err = s.GetSpend(ctx, old)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrUtxoHashMismatch), "got %v", err)

	resp, err := s.GetSpend(ctx, &utxo.Spend{TxID: old.TxID, Vout: 0, UTXOHash: newHash})
	require.NoError(t, err)
	require.Equal(t, int(utxo.Status_IMMATURE), resp.Status)
	require.Nil(t, resp.SpendingData)

	require.NoError(t, s.SetBlockHeight(100+tSettings.UtxoStore.ReAssignedUtxoSpendableAfterBlocks))

	resp, err = s.GetSpend(ctx, &utxo.Spend{TxID: old.TxID, Vout: 0, UTXOHash: newHash})
	require.NoError(t, err)
	require.Equal(t, int(utxo.Status_OK), resp.Status)
}

// TestReAssignedCoinIsNotDecoratedFromItsOldOutput closes the hole that made the whole feature
// work only for pre-extended transactions.
//
// The validator extends every transaction through BatchPreviousOutputsDecorate before it
// validates one (Validator.go:1631). That read used to hand back the coin row's satoshis and
// script unconditionally, and on a reassigned coin those are the CONFISCATED owner's. So the
// new owner's unextended transaction had the old output written onto it, the spend then hashed
// exactly those stale values against hash_override, and the spend was refused -- the store
// fabricating the wrong claim and then rejecting the victim for making it.
//
// The input is left alone instead. Only the new owner holds the script the coin was reassigned
// to, so only they can present it, and the refusal has to be loud: a nil script returned with
// no error would have the validator mark the transaction extended and validate it against no
// script at all.
func TestReAssignedCoinIsNotDecoratedFromItsOldOutput(t *testing.T) {
	s, ctx := newTestStore(t)
	tSettings := settings.NewSettings()

	require.NoError(t, s.SetBlockHeight(100))

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	old := &utxo.Spend{TxID: parent.TxIDChainHash(), Vout: 0}

	newHash, err := util.UTXOHashFromOutput(parent.TxIDChainHash(),
		&bt.Output{Satoshis: 5_000, LockingScript: newOwnerScript(t)}, 0)
	require.NoError(t, err)

	require.NoError(t, s.FreezeUTXOs(ctx, []*utxo.Spend{old}, tSettings))
	require.NoError(t, s.ReAssignUTXO(ctx, old,
		&utxo.Spend{TxID: old.TxID, Vout: 0, UTXOHash: newHash}, tSettings))

	// An unextended arrival: the input names the outpoint and carries no output.
	child := spendOneOutputTx(t, parent, 0)
	child.Inputs[0].PreviousTxScript = nil
	child.Inputs[0].PreviousTxSatoshis = 0

	err = s.PreviousOutputsDecorate(ctx, child)
	require.Error(t, err)
	require.Contains(t, err.Error(), "reassigned")
	require.Nil(t, child.Inputs[0].PreviousTxScript, "the confiscated output must not be written onto the input")
	require.Zero(t, child.Inputs[0].PreviousTxSatoshis)

	// Not a missing parent: the coin is right there, and sending the caller to fetch a parent
	// transaction would never resolve it.
	require.False(t, errors.Is(err, errors.ErrTxNotFound), "got %v", err)

	// A coin that was NOT reassigned still decorates from the row, in the same batch shape.
	other := mkTx(t, 1, 7_000)
	_, err = s.Create(ctx, other, 100)
	require.NoError(t, err)

	plain := spendOneOutputTx(t, other, 0)
	plain.Inputs[0].PreviousTxScript = nil
	plain.Inputs[0].PreviousTxSatoshis = 0

	require.NoError(t, s.PreviousOutputsDecorate(ctx, plain))
	require.Equal(t, uint64(7_000), plain.Inputs[0].PreviousTxSatoshis)
	require.NotNil(t, plain.Inputs[0].PreviousTxScript)
}

// TestReAssignedCoinCannotBeSpentOutpointOnly pins the interaction between two exemptions that
// were written independently.
//
// WithSkipUTXOHashCheck is the gated below-checkpoint path: the transaction arrives as
// outpoints alone, there is no claim to authenticate, and script validation is off in the same
// breath. A reassigned coin is the one coin where that reasoning fails, because the digest IS
// its only authentication -- the row's own satoshis and script belong to the party it was taken
// from. Waiving the claim there would authorise the spend on the outpoint alone, and the
// outpoint is exactly what the confiscated party still knows.
//
// It cannot arise below a checkpoint in practice. It is refused anyway, because the two guards
// live in different files and the next person to widen either one should hit a test rather
// than a silent hole.
func TestReAssignedCoinCannotBeSpentOutpointOnly(t *testing.T) {
	s, ctx := newTestStore(t)
	tSettings := settings.NewSettings()

	require.NoError(t, s.SetBlockHeight(100))

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	old := &utxo.Spend{TxID: parent.TxIDChainHash(), Vout: 0}
	newOutput := &bt.Output{Satoshis: 5_000, LockingScript: newOwnerScript(t)}

	newHash, err := util.UTXOHashFromOutput(parent.TxIDChainHash(), newOutput, 0)
	require.NoError(t, err)

	require.NoError(t, s.FreezeUTXOs(ctx, []*utxo.Spend{old}, tSettings))
	require.NoError(t, s.ReAssignUTXO(ctx, old,
		&utxo.Spend{TxID: old.TxID, Vout: 0, UTXOHash: newHash}, tSettings))

	spendable := 100 + tSettings.UtxoStore.ReAssignedUtxoSpendableAfterBlocks
	require.NoError(t, s.SetBlockHeight(spendable))

	outpointOnly := spendOneOutputTx(t, parent, 0)
	outpointOnly.Inputs[0].PreviousTxScript = nil
	outpointOnly.Inputs[0].PreviousTxSatoshis = 0

	_, spends, err := s.SpendAndCreate(ctx, outpointOnly, spendable, utxo.WithSpendOnly(),
		utxo.WithSkipUTXOHashCheck(true), utxo.WithSkipExtendedInputs(true))
	require.Error(t, err)
	require.True(t, errors.Is(spends[0].Err, errors.ErrUtxoHashMismatch), "got %v", spends[0].Err)
	require.Contains(t, spends[0].Err.Error(), "outpoint-only")

	// Rejection is all-or-nothing, so the coin is still there for its rightful new owner.
	require.Equal(t, 1, coinCount(t, s, ctx, parent))

	claimed := spendOneOutputTx(t, parent, 0)
	claimed.Inputs[0].PreviousTxSatoshis = newOutput.Satoshis
	claimed.Inputs[0].PreviousTxScript = newOutput.LockingScript

	spends, err = spendOnly(ctx, s, claimed, spendable)
	require.NoError(t, err)
	require.NoError(t, spends[0].Err)
}

// spendOneOutputTx builds a transaction taking one of parent's outputs, WITHOUT applying it,
// so a test can adjust what the input claims about the coin before offering it.
func spendOneOutputTx(t *testing.T, parent *bt.Tx, vout uint32) *bt.Tx {
	t.Helper()

	child := bt.NewTx()
	require.NoError(t, child.FromUTXOs(&bt.UTXO{
		TxIDHash:      parent.TxIDChainHash(),
		Vout:          vout,
		LockingScript: parent.Outputs[vout].LockingScript,
		Satoshis:      parent.Outputs[vout].Satoshis,
	}))
	child.AddOutput(&bt.Output{
		Satoshis:      parent.Outputs[vout].Satoshis - 1_000,
		LockingScript: parent.Outputs[vout].LockingScript,
	})

	return child
}

// mkCoinbase builds a real coinbase: the all-zero previous txid with the 0xffffffff index, so
// go-bt's IsCoinbase() answers true and the create path stamps FlagCoinbase and the maturity
// height on the coin.
func mkCoinbase(t *testing.T, sats uint64) *bt.Tx {
	t.Helper()

	in := &bt.Input{
		PreviousTxOutIndex: 0xffffffff,
		SequenceNumber:     0xffffffff,
		UnlockingScript:    bscript.NewFromBytes([]byte{0x04, 0xff, 0xff, 0x00, 0x1d}),
	}
	require.NoError(t, in.PreviousTxIDAdd(&chainhash.Hash{}))

	tx := bt.NewTx()
	tx.Inputs = []*bt.Input{in}
	tx.AddOutput(&bt.Output{Satoshis: sats, LockingScript: newOwnerScript(t)})

	require.True(t, tx.IsCoinbase())

	return tx
}

// TestAReassignedCoinbaseInsideItsDelayIsFrozenNotImmature.
//
// One column, spendable_from, carries two different holds, and the spend classifier has to say
// which one refused the coin because the two are acted on differently: the shared rollback
// predicate lists ErrFrozen and does not list ErrTxCoinbaseImmature, so a multi-input
// transaction failing on a held input strands its siblings marked spent unless the alert
// system's hold is reported as frozen.
//
// A reassigned coinbase is where a flag test alone gets it wrong. FlagCoinbase is still set --
// the coin was minted by one -- but reassignSQL has overwritten spendable_from with the
// reassignment delay, so the maturity window it used to hold is gone and the surviving hold is
// the alert system's. hash_override is what says so, because reassignSQL is the only writer of
// one on a live coin and writes both columns together.
func TestAReassignedCoinbaseInsideItsDelayIsFrozenNotImmature(t *testing.T) {
	s, ctx := newTestStore(t)
	tSettings := settings.NewSettings()

	require.NoError(t, s.SetBlockHeight(100))

	cb := mkCoinbase(t, 5_000)
	_, err := s.Create(ctx, cb, 100)
	require.NoError(t, err)

	old := &utxo.Spend{TxID: cb.TxIDChainHash(), Vout: 0}
	newOutput := &bt.Output{Satoshis: 5_000, LockingScript: newOwnerScript(t)}

	newHash, err := util.UTXOHashFromOutput(cb.TxIDChainHash(), newOutput, 0)
	require.NoError(t, err)

	require.NoError(t, s.FreezeUTXOs(ctx, []*utxo.Spend{old}, tSettings))
	require.NoError(t, s.ReAssignUTXO(ctx, old,
		&utxo.Spend{TxID: old.TxID, Vout: 0, UTXOHash: newHash}, tSettings))

	// Past coinbase maturity, so the only hold left is the reassignment's. Spending it must be
	// refused as frozen, not as an immature coinbase.
	spendHeight := uint32(100 + tSettings.ChainCfgParams.CoinbaseMaturity + 1)
	spends, err := spendOnly(ctx, s, spendOneOutputTx(t, cb, 0), spendHeight)
	require.Error(t, err)
	require.True(t, errors.Is(spends[0].Err, errors.ErrFrozen), "got %v", spends[0].Err)
	require.False(t, errors.Is(spends[0].Err, errors.ErrTxCoinbaseImmature),
		"the maturity window was overwritten by the reassignment; only the alert system's hold is left")
}

// TestAnOrdinaryCoinbaseInsideItsMaturityIsStillImmature pins the branch the test above must
// not have taken over: with no reassignment there is no override, and the coinbase answer is
// the right one.
func TestAnOrdinaryCoinbaseInsideItsMaturityIsStillImmature(t *testing.T) {
	s, ctx := newTestStore(t)

	require.NoError(t, s.SetBlockHeight(100))

	cb := mkCoinbase(t, 5_000)
	_, err := s.Create(ctx, cb, 100)
	require.NoError(t, err)

	spends, err := spendOnly(ctx, s, spendOneOutputTx(t, cb, 0), 100)
	require.Error(t, err)
	require.True(t, errors.Is(spends[0].Err, errors.ErrTxCoinbaseImmature), "got %v", spends[0].Err)
}
