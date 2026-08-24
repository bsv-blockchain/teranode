package bsv

import (
	"slices"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/test/utils/wirepeer"
	"github.com/stretchr/testify/require"
)

// Port of bitcoin-sv's bsv-invalid-transactions.py.
//
// Upstream runs eight scenarios in two halves. The first half sends a single
// invalid transaction and checks what the node tells the sender, whether it
// relays the transaction to a second peer, and whether the sender is banned. The
// second half sends children before their parents, so the children are orphans,
// and checks what happens when the parents arrive - including when one parent is
// itself invalid.
//
// The orphan half is the reason this port is worth having. Teranode's legacy
// service keeps a real orphan pool: SyncManager.orphanTxs, an expiring map filled
// at netsync/manager.go:1258 when a transaction's parent is missing and drained
// recursively by processOrphanTransactions at :1310 when the parent arrives.
// Nothing else in this package exercises it. MEASURED before the port was
// written: a child sent alone draws no reject and does not reach block assembly,
// and when its parent arrives both are accepted.
//
// Two things upstream asserts are not reproducible, and they are not the same
// kind of obstacle.
//
//  1. The whitelisted-relay scenario cannot be set up at all: no whitelist can be
//     in force in Teranode, so there is no way to be the whitelisted peer. See the
//     legacy-whitelist-inert gap.
//  2. Every ban assertion fails, because Teranode charges no ban score for an
//     invalid transaction. MEASURED: ten script-invalid transactions from one peer
//     drew ten rejects, left the peer connected, and left listbanned empty. READ
//     FROM CODE: the legacy service has exactly two addBanScore call sites, an
//     oversized getdata and a malformed message, and the transaction-failure path
//     at netsync/manager.go:1284 only logs. bitcoin-sv charges 100 for a script
//     verification failure against a default threshold of 100, so upstream's very
//     first bad transaction bans the sender. See the
//     no-ban-score-for-invalid-transactions gap; the ban expectations are asserted
//     here as tripwires in the opposite direction.
//
// Fidelity notes on the substitutions this port makes, neither of which changes
// what is being measured:
//
//   - Upstream's invalidity "bad_signature" corrupts a real signature over a P2PK
//     output. Teranode's funding outputs here are anyone-can-spend, so there is no
//     signature to corrupt; the port uses an unlocking script GoBDK refuses
//     instead. Both are script-verification failures, both produce
//     REJECT_INVALID, and both are charged the same nothing.
//   - Upstream builds 20 outputs per parent and so 20 orphans. The port builds
//     three. Upstream's assertions are set comparisons over whatever it built, so
//     the count carries no meaning beyond making a batch visible.
const invalidTxOrphans = 3

// invalidTxFixture starts a daemon and confirms one funding transaction with
// nOutputs anyone-can-spend outputs.
//
// A daemon per scenario, which is what upstream does - each of its scenarios is a
// separate run_node_with_connections block with its own node and its own
// command line. It also keeps the orphan pool and block assembly from carrying
// state between scenarios, which would make the "nothing is in the mempool yet"
// assertions meaningless.
func invalidTxFixture(t *testing.T, nOutputs int) (*daemon.TestDaemon, *bt.Tx) {
	t.Helper()

	td := wirepeer.NewLegacyDaemon(t)

	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, td.Ctx)

	opts := make([]transactions.TxOption, 0, 1+nOutputs)
	opts = append(opts, transactions.WithInput(coinbaseTx, 0))

	for range nOutputs {
		opts = append(opts, transactions.WithOutput(1e6, anyoneCanSpendScript()))
	}

	fundingTx := td.CreateTransactionWithOptions(t, opts...)

	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, fundingTx),
		"funding transaction should be accepted")
	td.WaitForBlockAssemblyToProcessTx(t, fundingTx.TxID())
	td.MineAndWait(t, 1)

	return td, fundingTx
}

// spendAnyoneCanSpend builds a transaction spending vout of parent into nOutputs
// anyone-can-spend outputs, leaving fee satoshis unspent across the whole
// transaction.
//
// The unlocking script is empty, which satisfies an OP_TRUE locking script, and
// contributes no signature - so these transactions are valid whatever the fee.
func spendAnyoneCanSpend(t *testing.T, parent *bt.Tx, vout uint32, nOutputs int, fee uint64) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()

	require.NoError(t, tx.FromUTXOs(&bt.UTXO{
		TxIDHash:      parent.TxIDChainHash(),
		Vout:          vout,
		LockingScript: parent.Outputs[vout].LockingScript,
		Satoshis:      parent.Outputs[vout].Satoshis,
	}), "add input spending %s:%d", parent.TxID(), vout)

	tx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{})

	perOutput := (parent.Outputs[vout].Satoshis - fee) / uint64(nOutputs)
	for range nOutputs {
		tx.AddOutput(&bt.Output{Satoshis: perOutput, LockingScript: anyoneCanSpendScript()})
	}

	return tx
}

// spendTwoParents builds upstream's child shape: one transaction spending output
// n of each of two parents. Both inputs must be satisfiable for the child to be
// valid, which is what makes it stay an orphan while only one parent is known.
//
// invalid makes the second input's unlocking script one GoBDK refuses, standing
// in for upstream's children_invalidity="bad_signature".
func spendTwoParents(t *testing.T, parent1, parent2 *bt.Tx, n uint32, fee uint64, invalid bool) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()

	for _, p := range []*bt.Tx{parent1, parent2} {
		require.NoError(t, tx.FromUTXOs(&bt.UTXO{
			TxIDHash:      p.TxIDChainHash(),
			Vout:          n,
			LockingScript: p.Outputs[n].LockingScript,
			Satoshis:      p.Outputs[n].Satoshis,
		}), "add input spending %s:%d", p.TxID(), n)
	}

	tx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{})

	if invalid {
		tx.Inputs[1].UnlockingScript = opNotIfUnlockingScript()
	} else {
		tx.Inputs[1].UnlockingScript = bscript.NewFromBytes([]byte{})
	}

	total := parent1.Outputs[n].Satoshis + parent2.Outputs[n].Satoshis
	tx.AddOutput(&bt.Output{Satoshis: total - fee, LockingScript: anyoneCanSpendScript()})

	return tx
}

// inBlockAssembly reports whether txid is among block assembly's pending
// transactions. Teranode has no mempool; getrawmempool reports block assembly's
// pending hashes, which is the closest thing to upstream's mempool contents. See
// tryRawMempool for why the all-ff sentinel means callers must look for a
// specific hash rather than count.
func inBlockAssembly(td *daemon.TestDaemon, txid string) bool {
	return slices.Contains(tryRawMempool(td), txid)
}

// requireEventuallyInBlockAssembly is upstream's check_mempool_equals for one
// transaction.
func requireEventuallyInBlockAssembly(t *testing.T, td *daemon.TestDaemon, txid, why string) {
	t.Helper()

	require.Eventually(t, func() bool {
		return inBlockAssembly(td, txid)
	}, 30*time.Second, 250*time.Millisecond, why)
}

// rejectedTxids is upstream's on_reject collector: the set of transaction hashes
// the node has rejected to this peer.
func rejectedTxids(p *wirepeer.Peer) map[string]bool {
	out := map[string]bool{}

	for _, m := range p.Received(wire.CmdReject) {
		r, ok := m.(*wire.MsgReject)
		if !ok || r.Cmd != wire.CmdTx {
			continue
		}

		out[r.Hash.String()] = true
	}

	return out
}

// invTxids is upstream's on_inv collector, restricted to transaction inventory.
func invTxids(p *wirepeer.Peer) map[string]bool {
	out := map[string]bool{}

	for _, m := range p.Received(wire.CmdInv) {
		iv, ok := m.(*wire.MsgInv)
		if !ok {
			continue
		}

		for _, v := range iv.InvList {
			if v.Type == wire.InvTypeTx {
				out[v.Hash.String()] = true
			}
		}
	}

	return out
}

func TestBSVInvalidTransactions(t *testing.T) {
	t.Run("invalid transactions", func(t *testing.T) {
		// Upstream Scenario 1: low fee from a non-whitelisted peer. Rejected, and
		// not relayed.
		t.Run("a low-fee transaction is rejected and not relayed", func(t *testing.T) {
			td, fundingTx := invalidTxFixture(t, 2)
			defer td.Stop(t)

			sender := wirepeer.Connect(t, td)
			defer sender.Close()

			receiver := wirepeer.Connect(t, td)
			defer receiver.Close()

			// A zero-fee transaction: every input satoshi is spent. Upstream sets
			// -mindebugrejectionfee and underpays it by 10 satoshis; Teranode's
			// minminingtxfee is 0.00000001 in settings.conf and is applied at
			// acceptance rather than at mining, so zero fee is the way to be under it.
			// MEASURED: TX_POLICY (39) "transaction fee is too low" -> insufficient-fee.
			lowFee := spendAnyoneCanSpend(t, fundingTx, 0, 1, 0)
			sender.Send(t, asWireTx(t, lowFee))

			reject := sender.WaitForReject(t, 30*time.Second)
			require.Equal(t, wire.CmdTx, reject.Cmd, "the reject should be about a transaction")
			require.Equal(t, wire.RejectInvalid, reject.Code, "upstream expects code 16")
			require.Equal(t, lowFee.TxID(), reject.Hash.String(),
				"the reject should name the transaction that was refused")

			require.False(t, inBlockAssembly(td, lowFee.TxID()),
				"a transaction rejected for its fee must not reach block assembly")

			// Upstream: assert len(relayed_txs) == 0. The control below is what makes
			// this meaningful rather than vacuously true.
			receiver.AssertNotReceived(t, 3*time.Second, "an inv for the rejected transaction",
				func(m wire.Message) bool {
					iv, ok := m.(*wire.MsgInv)
					if !ok {
						return false
					}

					for _, v := range iv.InvList {
						if v.Type == wire.InvTypeTx && v.Hash.String() == lowFee.TxID() {
							return true
						}
					}

					return false
				})

			// CONTROL, and not something upstream needs: upstream's framework proves
			// relay works elsewhere, but here "not relayed" would pass just as well if
			// Teranode never relayed anything. A valid transaction from the same sender
			// must reach the other peer as a tx inv. MEASURED while establishing this
			// port: it does, and it is not echoed back to the sender.
			valid := spendAnyoneCanSpend(t, fundingTx, 1, 1, 1000)
			sender.Send(t, asWireTx(t, valid))

			requireEventuallyInBlockAssembly(t, td, valid.TxID(),
				"the control transaction should be accepted")

			receiver.WaitForInvOf(t, 30*time.Second, valid.TxIDChainHash())

			require.NotContains(t, invTxids(receiver), lowFee.TxID(),
				"the rejected transaction must still not have been relayed, now that "+
					"relay has been shown to work on this connection")

			sender.AssertStillConnected(t, 2*time.Second,
				"one low-fee transaction must not cost the peer its connection")
		})

		// Upstream Scenario 3: bad signature with a ban threshold high enough that a
		// single failure cannot reach it. Rejected, not relayed, not banned. This is
		// the one ban-related scenario whose outcome Teranode reproduces, though for
		// a different reason - upstream is under its threshold, Teranode has no score
		// at all.
		t.Run("a script-invalid transaction is rejected and not relayed", func(t *testing.T) {
			td, fundingTx := invalidTxFixture(t, 1)
			defer td.Stop(t)

			sender := wirepeer.Connect(t, td)
			defer sender.Close()

			receiver := wirepeer.Connect(t, td)
			defer receiver.Close()

			bad := spendWithOpNotIf(t, fundingTx, 0)
			sender.Send(t, asWireTx(t, bad))

			reject := sender.WaitForReject(t, 30*time.Second)
			require.Equal(t, bad.TxID(), reject.Hash.String(),
				"the reject should name the refused transaction")

			require.False(t, inBlockAssembly(td, bad.TxID()),
				"a script-invalid transaction must not reach block assembly")
			require.NotContains(t, invTxids(receiver), bad.TxID(),
				"a rejected transaction must not be relayed")

			sender.AssertStillConnected(t, 2*time.Second,
				"upstream keeps using this connection, so it must survive")
		})

		// Upstream Scenario 4: the same bad transaction with -banscore=1, expecting a
		// disconnect and one entry in listbanned.
		//
		// TRIPWIRE, inverted. Teranode charges no ban score for an invalid
		// transaction, so no threshold can be crossed. Ten of them leave the peer
		// connected and listbanned empty. If this subtest starts failing, ban scoring
		// has been added to the transaction path and the
		// no-ban-score-for-invalid-transactions gap should be revisited.
		t.Run("repeated invalid transactions are never banned", func(t *testing.T) {
			// Upstream's -banscore=1 has a Teranode counterpart in
			// legacy_config_BanThreshold, set as low as it goes so that any score at
			// all would ban. That it does not is the finding.
			setLegacyConfigForTest(t, "legacy_config_BanThreshold", "1")

			td, fundingTx := invalidTxFixture(t, 1)
			defer td.Stop(t)

			// The override cannot be read back from td.Settings: the legacy service
			// loads its own config by reflection over legacy_config_* keys inside
			// loadConfig, and does not surface the result on settings.LegacySettings.
			// setLegacyConfigForTest does assert the key reached the gocore config map
			// the service reads, which is the same route legacy_config_MaxPeers takes
			// in TestBSVP2PMaxConnections, where the effect IS observable. The finding
			// here does not rest on the override anyway - it rests on there being no
			// addBanScore call site on the transaction path at all, so no threshold
			// could matter.
			sender := wirepeer.Connect(t, td)
			defer sender.Close()

			const attempts = 10

			sent := map[string]bool{}

			for i := range attempts {
				bad := spendWithOpNotIf(t, fundingTx, 0)
				// Vary the output value so each attempt is a distinct txid, otherwise
				// the node may recognise a duplicate and not re-validate.
				bad.Outputs[0].Satoshis -= uint64(i)
				sent[bad.TxID()] = true

				sender.Send(t, asWireTx(t, bad))
			}

			require.Eventually(t, func() bool {
				return len(rejectedTxids(sender)) == attempts
			}, 30*time.Second, 250*time.Millisecond,
				"every invalid transaction should draw its own reject; got %d of %d",
				len(rejectedTxids(sender)), attempts)

			require.Equal(t, sent, rejectedTxids(sender),
				"the rejects should name exactly the transactions that were sent")

			sender.AssertStillConnected(t, 3*time.Second,
				"TRIPWIRE: ten invalid transactions against a ban threshold of 1 leave the peer "+
					"connected, where bitcoin-sv bans on the first. If this now fails, see the "+
					"no-ban-score-for-invalid-transactions gap")

			require.Empty(t, wirepeer.ListBanned(t, td),
				"TRIPWIRE: listbanned should still be empty, since nothing charged the peer")
		})
	})

	t.Run("orphan transactions", func(t *testing.T) {
		// Upstream Scenario 1: valid orphans sent before two valid parents. Nothing
		// is rejected, nothing is in the mempool until the parents arrive, then
		// everything is.
		t.Run("valid orphans are held, then accepted when both parents arrive", func(t *testing.T) {
			td, fundingTx := invalidTxFixture(t, 2)
			defer td.Stop(t)

			p := wirepeer.Connect(t, td)
			defer p.Close()

			parent1 := spendAnyoneCanSpend(t, fundingTx, 0, invalidTxOrphans, 1000)
			parent2 := spendAnyoneCanSpend(t, fundingTx, 1, invalidTxOrphans, 1000)

			orphans := make([]*bt.Tx, 0, invalidTxOrphans)
			for n := range invalidTxOrphans {
				orphans = append(orphans, spendTwoParents(t, parent1, parent2, uint32(n), 1000, false))
			}

			for _, o := range orphans {
				p.Send(t, asWireTx(t, o))
			}

			// Upstream: getmempoolinfo()["size"] == 0 and no rejects. getmempoolinfo is
			// handleUnimplemented in Teranode, so this reads block assembly instead.
			requireNoneInBlockAssembly(t, td, orphans, "an orphan must not reach block assembly")
			require.Empty(t, rejectedTxids(p), "an orphan must not be rejected, only held")

			// First parent only: the children each need both, so they stay orphans.
			p.Send(t, asWireTx(t, parent1))
			requireEventuallyInBlockAssembly(t, td, parent1.TxID(), "the first parent should be accepted")
			requireNoneInBlockAssembly(t, td, orphans,
				"the orphans still need their second parent, so none may be accepted yet")
			require.Empty(t, rejectedTxids(p), "still nothing should be rejected")

			// Second parent: now every orphan can be validated.
			p.Send(t, asWireTx(t, parent2))
			requireEventuallyInBlockAssembly(t, td, parent2.TxID(), "the second parent should be accepted")

			for _, o := range orphans {
				requireEventuallyInBlockAssembly(t, td, o.TxID(),
					"orphan "+o.TxID()+" should be accepted once both parents are known")
			}

			require.Empty(t, rejectedTxids(p), "nothing should have been rejected at any point")
			p.AssertStillConnected(t, 2*time.Second, "sending orphans must not cost the peer its connection")
		})

		// Upstream Scenario 2: low-fee orphans. Held while parents are missing, then
		// rejected once both parents are known, and the peer is not banned.
		t.Run("low-fee orphans are held, then rejected when their parents arrive", func(t *testing.T) {
			td, fundingTx := invalidTxFixture(t, 2)
			defer td.Stop(t)

			p := wirepeer.Connect(t, td)
			defer p.Close()

			parent1 := spendAnyoneCanSpend(t, fundingTx, 0, invalidTxOrphans, 1000)
			parent2 := spendAnyoneCanSpend(t, fundingTx, 1, invalidTxOrphans, 1000)

			orphans := make([]*bt.Tx, 0, invalidTxOrphans)
			for n := range invalidTxOrphans {
				// fee 0: spends every satoshi of both inputs.
				orphans = append(orphans, spendTwoParents(t, parent1, parent2, uint32(n), 0, false))
			}

			for _, o := range orphans {
				p.Send(t, asWireTx(t, o))
			}

			requireNoneInBlockAssembly(t, td, orphans, "an orphan must not reach block assembly")
			require.Empty(t, rejectedTxids(p),
				"an orphan must not be rejected before its parents are known - its fee cannot "+
					"be assessed until its inputs exist")

			p.Send(t, asWireTx(t, parent1))
			requireEventuallyInBlockAssembly(t, td, parent1.TxID(), "the first parent should be accepted")

			p.Send(t, asWireTx(t, parent2))
			requireEventuallyInBlockAssembly(t, td, parent2.TxID(), "the second parent should be accepted")

			// Upstream: check_rejected(rejected_txs, orphans) - every orphan draws its
			// own reject once its fee can be assessed.
			//
			// TRIPWIRE. Teranode assesses them and refuses them, but tells the peer
			// nothing. MEASURED: three low-fee orphans, three internal failures logged
			// as TX_POLICY (39) "transaction fee is too low" from
			// processOrphanTransactions, and zero rejects on the wire. The direct
			// submission path pushes a reject at netsync/manager.go:1288; the orphan
			// path at :1346 only logs and continues. See the
			// orphan-rejection-not-reported-to-peer gap.
			requireNoneInBlockAssembly(t, td, orphans,
				"a low-fee orphan must not reach block assembly once its fee can be assessed")

			p.AssertNotReceived(t, 5*time.Second, "a reject for any low-fee orphan",
				func(m wire.Message) bool {
					r, ok := m.(*wire.MsgReject)
					if !ok {
						return false
					}

					for _, o := range orphans {
						if r.Hash.String() == o.TxID() {
							return true
						}
					}

					return false
				})

			require.Empty(t, rejectedTxids(p),
				"TRIPWIRE: no reject reaches the peer for a rejected orphan, where bitcoin-sv "+
					"sends one per orphan. If this now fails, the "+
					"orphan-rejection-not-reported-to-peer gap has been fixed and this subtest "+
					"should assert upstream's rejects instead")

			p.AssertStillConnected(t, 2*time.Second,
				"upstream asserts we are not banned for low-fee orphans")
		})

		// Upstream Scenario 4: valid orphans whose second parent is script-invalid.
		// The valid parent is accepted, the invalid one rejected, the orphans can
		// never be validated, and the peer is not banned. Upstream raises -banscore
		// to 101 specifically so that the single bad parent's 100 points do not ban
		// it; Teranode needs no such allowance, which is the finding.
		t.Run("orphans whose parent is invalid stay held, and the peer is not banned", func(t *testing.T) {
			td, fundingTx := invalidTxFixture(t, 2)
			defer td.Stop(t)

			p := wirepeer.Connect(t, td)
			defer p.Close()

			validParent := spendAnyoneCanSpend(t, fundingTx, 0, invalidTxOrphans, 1000)

			// The invalid parent spends the funding output with a script GoBDK refuses,
			// so it can never be accepted and its outputs never exist.
			invalidParent := spendAnyoneCanSpend(t, fundingTx, 1, invalidTxOrphans, 1000)
			invalidParent.Inputs[0].UnlockingScript = opNotIfUnlockingScript()

			orphans := make([]*bt.Tx, 0, invalidTxOrphans)
			for n := range invalidTxOrphans {
				orphans = append(orphans, spendTwoParents(t, validParent, invalidParent, uint32(n), 1000, false))
			}

			for _, o := range orphans {
				p.Send(t, asWireTx(t, o))
			}

			requireNoneInBlockAssembly(t, td, orphans, "an orphan must not reach block assembly")
			require.Empty(t, rejectedTxids(p), "nothing should be rejected yet")

			p.Send(t, asWireTx(t, validParent))
			requireEventuallyInBlockAssembly(t, td, validParent.TxID(), "the valid parent should be accepted")

			p.Send(t, asWireTx(t, invalidParent))

			// Upstream: check_rejected(rejected_txs, [invalid_parent_tx]) - only the
			// invalid parent is rejected.
			require.Eventually(t, func() bool {
				return rejectedTxids(p)[invalidParent.TxID()]
			}, 30*time.Second, 250*time.Millisecond,
				"the invalid parent should be rejected")
			require.Equal(t, map[string]bool{invalidParent.TxID(): true}, rejectedTxids(p),
				"only the invalid parent should be rejected - the orphans are still merely held")

			require.False(t, inBlockAssembly(td, invalidParent.TxID()),
				"the invalid parent must not reach block assembly")
			requireNoneInBlockAssembly(t, td, orphans,
				"the orphans can never be validated, since one of their parents will never exist")

			p.AssertStillConnected(t, 2*time.Second,
				"TRIPWIRE: upstream needs -banscore=101 to survive one bad parent; Teranode "+
					"charges nothing, so no allowance is needed. See the "+
					"no-ban-score-for-invalid-transactions gap")
			require.Empty(t, wirepeer.ListBanned(t, td), "nothing should be banned")
		})
	})
}

// requireNoneInBlockAssembly asserts that none of txs has reached block assembly.
//
// Read once rather than polled: it is asserting an absence, and a poll would only
// widen the window in which a late arrival is missed. Callers order it after
// something that has already been waited for, so the node has demonstrably
// processed the surrounding work.
func requireNoneInBlockAssembly(t *testing.T, td *daemon.TestDaemon, txs []*bt.Tx, why string) {
	t.Helper()

	pool := tryRawMempool(td)
	seen := map[string]bool{}

	for _, h := range pool {
		seen[h] = true
	}

	for _, tx := range txs {
		require.False(t, seen[tx.TxID()], "%s: %s is in block assembly (%v)", why, tx.TxID(), pool)
	}
}
