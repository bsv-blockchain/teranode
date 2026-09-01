package validator

import (
	"encoding/hex"
	"strings"
	"testing"

	bdkscript "github.com/bitcoin-sv/bdk/module/gobdk/script"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-chaincfg"
	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	shash "github.com/bsv-blockchain/go-sdk/primitives/hash"
	sdkscript "github.com/bsv-blockchain/go-sdk/script"
	sdktx "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/transaction/template/p2pkh"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// This file pins the svnode/BDK semantics the per-script fee tiers depend on,
// by measuring real BDK behaviour instead of asserting it from documentation.
//
// BDK exposes no op-count getter (bdkcgo/txvalidator_cgo.h has only
// GetSigOpCount and the policy setters), so the executed op count is measured
// indirectly: maxopsperscriptpolicy is set to a chosen cap and BDK is asked to
// validate the transaction. A rejection with SCRIPT_ERR_OP_COUNT means BDK's
// executed count exceeded the cap; acceptance means it did not. That turns the
// cap into an oracle for what BDK actually counts.

// opCountRejectionText is svnode's ScriptErrorString entry for
// SCRIPT_ERR_OP_COUNT. The typed bdkscript.ScriptError does not survive the
// adapter's error wrapping (bdkCause renders the cause as text), so the reject
// reason is matched on that string, the same static svnode table
// ScriptVerifierGoBDK already relies on for its public reject reasons.
const opCountRejectionText = "Operation limit exceeded"

// isOpCountRejection reports whether err is BDK's SCRIPT_ERR_OP_COUNT verdict.
func isOpCountRejection(err error) bool {
	if err == nil {
		return false
	}

	var scriptErr bdkscript.ScriptError
	if errors.As(err, &scriptErr) {
		return scriptErr.Code() == bdkscript.SCRIPT_ERR_OP_COUNT
	}

	return strings.Contains(err.Error(), opCountRejectionText)
}

// bdkSpendVerdict validates a transaction that spends a single output carrying
// lockingScript with unlockingScript, under the given op cap and coin height,
// and returns BDK's error (nil when accepted).
func bdkSpendVerdict(t *testing.T, params *chaincfg.Params, lockingScript, unlockingScript []byte, opCap int64, coinHeight, blockHeight uint32) error {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams = params

	po := tSettings.Policy
	po.MinMiningTxFee = 0
	po.MaxOpsPerScriptPolicy = opCap

	verifier := newScriptVerifierGoBDK(ulogger.TestLogger{}, po, tSettings.ChainCfgParams)

	tx := bt.NewTx()
	require.NoError(t, tx.From("aa00000000000000000000000000000000000000000000000000000000000001", 0, hex.EncodeToString(lockingScript), 1000))

	unlocking := bscript.Script(unlockingScript)
	tx.Inputs[0].UnlockingScript = &unlocking

	outScript, err := bscript.NewFromHexString(scriptTierTestP2PKHScript)
	require.NoError(t, err)
	tx.AddOutput(&bt.Output{Satoshis: 900, LockingScript: outScript})

	return verifier.ValidateTransaction(tx, blockHeight, false, []uint32{coinHeight})
}

// TestOpCountSemanticsDifferentialBDK proves, against real BDK, the two svnode
// subtleties countScriptOps reproduces (PR review P0-3 and P1-6):
//
//   - a TOP-LEVEL OP_RETURN ends execution, so opcodes after it are never
//     counted (an attacker-appended data tail cannot inflate the metric);
//   - an OP_RETURN nested inside OP_IF does NOT end execution, so counting
//     continues there.
func TestOpCountSemanticsDifferentialBDK(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	params := tSettings.ChainCfgParams
	blockHeight := params.GenesisActivationHeight + 10_000
	coinHeight := blockHeight - 1_000 // post-Genesis coin

	t.Run("the op cap is a working oracle", func(t *testing.T) {
		// 1000 counted opcodes: accepted under a generous cap, rejected under a
		// tight one. Without this control the tests below prove nothing.
		control := append(repeatedOps(1_000), bscript.OpTRUE)
		require.Equal(t, uint64(1_000), countOps(control))

		require.NoError(t, bdkSpendVerdict(t, params, control, nil, 2_000, coinHeight, blockHeight))

		err := bdkSpendVerdict(t, params, control, nil, 100, coinHeight, blockHeight)
		require.Error(t, err)
		require.True(t, isOpCountRejection(err), "expected SCRIPT_ERR_OP_COUNT, got %v", err)
	})

	t.Run("a top-level OP_RETURN stops BDK's op count", func(t *testing.T) {
		// OP_TRUE leaves a true on the stack, then a top-level OP_RETURN ends
		// execution successfully. The 1000 opcodes after it are never fetched,
		// so a cap of 100 is not exceeded. If BDK counted the tail, this would
		// be SCRIPT_ERR_OP_COUNT.
		script := append([]byte{bscript.OpTRUE, bscript.OpRETURN}, repeatedOps(1_000)...)

		require.Equal(t, uint64(1), countOps(script), "our metric counts the OP_RETURN and stops")

		require.NoError(t, bdkSpendVerdict(t, params, script, nil, 100, coinHeight, blockHeight),
			"BDK must not count the opcodes after a top-level OP_RETURN")
	})

	t.Run("an OP_RETURN nested in OP_IF does not stop BDK's op count", func(t *testing.T) {
		// OP_FALSE OP_IF OP_RETURN <1000 ops> OP_ENDIF OP_TRUE. svnode keeps
		// counting fetched opcodes here, and so must we.
		script := append([]byte{bscript.OpFALSE, bscript.OpIF, bscript.OpRETURN}, repeatedOps(1_000)...)
		script = append(script, bscript.OpENDIF, bscript.OpTRUE)

		// OP_IF + OP_RETURN + 1000 NOPs + OP_ENDIF = 1003 counted opcodes.
		require.Equal(t, uint64(1_003), countOps(script))

		err := bdkSpendVerdict(t, params, script, nil, 100, coinHeight, blockHeight)
		require.Error(t, err)
		require.True(t, isOpCountRejection(err), "expected SCRIPT_ERR_OP_COUNT, got %v", err)

		require.NoError(t, bdkSpendVerdict(t, params, script, nil, 2_000, coinHeight, blockHeight),
			"the same script passes under a cap above its true count")
	})
}

// bareMultiSig builds OP_1 <keys pubkey pushes> <keys> OP_CHECKMULTISIG, the
// canonical 1-of-n multisig locking script. The key count uses its minimal
// encoding, which BDK requires: a small-constant opcode up to 16, and a
// single-byte data push above that.
func bareMultiSig(keys int) []byte {
	script := []byte{bscript.OpONE}

	for i := 0; i < keys; i++ {
		script = append(script, bscript.OpDATA33, 0x02)
		script = append(script, make([]byte, 32)...)
	}

	if keys <= 16 {
		script = append(script, byte(int(bscript.OpONE)+keys-1))
	} else {
		script = append(script, 0x01, byte(keys))
	}

	return append(script, bscript.OpCHECKMULTISIG)
}

// TestCheckMultiSigKeyCountDifferentialBDK pins the multisig key count (PR review
// P1-6). svnode charges OP_CHECKMULTISIG its key count on top of the opcode
// (nOpCount += nKeysCount, popped from the stack). At IF-depth zero the opcode
// always executes and the immediately preceding literal push IS the stack top it
// consumes, so the count is statically certain there and countScriptOps adds it.
// That is the densest shape the ops tier exists to price, and it used to be
// charged for a single operation.
//
// Where the count is not statically certain, inside a conditional, the walk falls
// back to one. That under-counts, which is the containment direction, and is
// measured here rather than assumed.
func TestCheckMultiSigKeyCountDifferentialBDK(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	params := tSettings.ChainCfgParams
	blockHeight := params.GenesisActivationHeight + 10_000
	coinHeight := blockHeight - 1_000

	t.Run("a bare multisig at the top level is counted exactly", func(t *testing.T) {
		multisig := bareMultiSig(16)

		// 1 for the opcode plus the 16 keys.
		require.Equal(t, uint64(17), countOps(multisig))

		requireBDKOpCount(t, params, multisig, 17, coinHeight, blockHeight)
	})

	t.Run("a key count pushed as a number is counted exactly", func(t *testing.T) {
		// Above 16 the key count is a data push rather than a small-constant
		// opcode, so this exercises the CScriptNum decode.
		multisig := bareMultiSig(20)

		require.Equal(t, uint64(21), countOps(multisig), "1 opcode plus a key count of 20")

		requireBDKOpCount(t, params, multisig, 21, coinHeight, blockHeight)
	})

	t.Run("a three-byte multisig counts far more ops than it has bytes", func(t *testing.T) {
		// The bound that lets priceScript skip the ops walk on short scripts,
		// counted ops cannot exceed script length, does not hold for a multisig.
		// Pin that against BDK: three bytes, eighteen operations.
		script := []byte{0x01, 17, bscript.OpCHECKMULTISIG} // <push 17> OP_CHECKMULTISIG

		require.Len(t, script, 3)
		require.Equal(t, uint64(18), countOps(script))

		requireBDKOpCount(t, params, script, 18, coinHeight, blockHeight)
	})

	t.Run("a nested OP_RETURN suppresses the key count that follows it", func(t *testing.T) {
		// Found by the differential fuzz, not derived from the source: an
		// OP_RETURN inside a conditional does not end the script post-Genesis,
		// it stops EXECUTION while fetching continues. So the multisig below is
		// at depth zero yet never executes, and svnode adds no key count for it.
		// Counting the keys here would over-charge by 16.
		script := []byte{bscript.OpONE, bscript.OpIF, bscript.OpRETURN, bscript.OpENDIF}
		script = append(script, bareMultiSig(16)...)

		// OP_IF, OP_RETURN, OP_ENDIF, OP_CHECKMULTISIG: four opcodes, no keys.
		require.Equal(t, uint64(4), countOps(script))

		requireBDKOpCount(t, params, script, 4, coinHeight, blockHeight)
	})

	t.Run("inside a conditional the walk under-counts, never over-counts", func(t *testing.T) {
		// OP_1 OP_IF <bare 1-of-16 multisig> OP_ENDIF. The key count is not
		// statically certain here, so the walk charges one for the opcode.
		script := append([]byte{bscript.OpONE, bscript.OpIF}, bareMultiSig(16)...)
		script = append(script, bscript.OpENDIF)

		// OP_IF + OP_CHECKMULTISIG + OP_ENDIF, with no key count added.
		ours := countOps(script)
		require.Equal(t, uint64(3), ours)

		// BDK counts more than we do: a cap at our count is still exceeded.
		err := bdkSpendVerdict(t, params, script, nil, int64(ours), coinHeight, blockHeight) //nolint:gosec // small constant
		require.True(t, isOpCountRejection(err),
			"BDK must count more than our fallback, proving we under-count rather than over-count; got %v", err)
	})
}

// TestP2SHRedeemEraDifferentialBDK answers, by measurement, the question the PR
// review left open (P1-8): whether BDK still evaluates a P2SH redeem script, and
// for which coins. The answer is that the COIN's height decides, not the
// spending tip: a coin created before Genesis runs its redeem script even when
// spent at a post-Genesis tip, and a post-Genesis coin never does. That is
// exactly the gate checkScriptTieredFees applies before billing the redeem.
//
// Mainnet parameters are used so BIP16 activation and Genesis are both real
// heights; the base test parameters put Genesis at 100, which is below P2SH
// activation and so cannot express a "pre-Genesis P2SH" coin at all.
func TestP2SHRedeemEraDifferentialBDK(t *testing.T) {
	params := &chaincfg.MainNetParams

	const (
		preGenesisCoin  = uint32(300_000) // after BIP16 (173805), before Genesis (620538)
		postGenesisCoin = uint32(650_000)
		tip             = uint32(700_000) // post-Genesis tip in both cases
	)

	require.True(t, isPreGenesisCoin(preGenesisCoin, params.GenesisActivationHeight))
	require.False(t, isPreGenesisCoin(postGenesisCoin, params.GenesisActivationHeight))

	p2shSpend := func(redeemOps int) (locking, unlocking []byte) {
		redeem := append(repeatedOps(redeemOps), bscript.OpTRUE)

		locking = append([]byte{bscript.OpHASH160, bscript.OpDATA20}, shash.Hash160(redeem)...)
		locking = append(locking, bscript.OpEQUAL)

		unlocking = append([]byte{bscript.OpPUSHDATA2, byte(len(redeem)), byte(len(redeem) >> 8)}, redeem...)

		return locking, unlocking
	}

	t.Run("pre-Genesis coins DO execute the redeem script", func(t *testing.T) {
		// Pre-Genesis, BDK enforces the fixed consensus limit of 500 ops and
		// ignores maxopsperscriptpolicy, so the cap is set generously and the
		// consensus limit is what binds. First establish that limit.
		require.NoError(t, bdkSpendVerdict(t, params, append(repeatedOps(500), bscript.OpTRUE), nil, 1_000_000, preGenesisCoin, tip),
			"500 ops is at the pre-Genesis consensus limit")

		overLimit := bdkSpendVerdict(t, params, append(repeatedOps(505), bscript.OpTRUE), nil, 1_000_000, preGenesisCoin, tip)
		require.Error(t, overLimit)
		require.True(t, isOpCountRejection(overLimit), "505 ops must exceed the pre-Genesis limit, got %v", overLimit)

		// A 505-op redeem is 506 bytes: over the 500-op limit, still under the
		// 520-byte pre-Genesis push limit. It can only be counted if BDK
		// actually executes the redeem script.
		locking, unlocking := p2shSpend(505)
		require.Equal(t, uint64(2), countOps(locking), "the locking script alone is only 2 ops")

		err := bdkSpendVerdict(t, params, locking, unlocking, 1_000_000, preGenesisCoin, tip)
		require.Error(t, err)
		require.True(t, isOpCountRejection(err),
			"a pre-Genesis P2SH spend must execute (and count) its redeem script, got %v", err)
	})

	t.Run("post-Genesis coins do NOT execute the redeem script", func(t *testing.T) {
		// Post-Genesis the policy cap binds again. A plain 400-op script is
		// rejected at a cap of 100, so if the redeem were executed the P2SH
		// spend carrying a 400-op redeem would be rejected too. It is not.
		plain := bdkSpendVerdict(t, params, append(repeatedOps(400), bscript.OpTRUE), nil, 100, postGenesisCoin, tip)
		require.Error(t, plain)
		require.True(t, isOpCountRejection(plain), "control: 400 ops must exceed a cap of 100, got %v", plain)

		locking, unlocking := p2shSpend(400)
		require.NoError(t, bdkSpendVerdict(t, params, locking, unlocking, 100, postGenesisCoin, tip),
			"a post-Genesis P2SH spend must NOT execute its redeem script")
	})
}

// TestConsolidationConfigNormalisationDifferentialBDK proves the PR-review P1-7
// claim by measurement: BDK rewrites a zero MinConfConsolidationInput to the
// svnode default of 6, so reading the raw setting in Go would disagree with it.
// isFreeConsolidationTxn normalises the same way and must agree with BDK.
func TestConsolidationConfigNormalisationDifferentialBDK(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	genesisHeight := tSettings.ChainCfgParams.GenesisActivationHeight
	blockHeight := genesisHeight + 10_000

	priv, err := ec.NewPrivateKey()
	require.NoError(t, err)

	addr, err := sdkscript.NewAddressFromPublicKey(priv.PubKey(), true)
	require.NoError(t, err)

	lock, err := p2pkh.Lock(addr)
	require.NoError(t, err)
	lockHex := hex.EncodeToString(*lock)

	unlockTemplate, err := p2pkh.Unlock(priv, nil)
	require.NoError(t, err)

	const prevTxID = "aa00000000000000000000000000000000000000000000000000000000000001"

	// A signed 20-in, 1-out P2PKH consolidation with ~107-byte unlocking scripts.
	buildConsolidation := func(t *testing.T) *bt.Tx {
		t.Helper()

		const perInput = uint64(1000)

		stx := sdktx.NewTransaction()
		for i := 0; i < 20; i++ {
			require.NoError(t, stx.AddInputFrom(prevTxID, uint32(i), lockHex, perInput, unlockTemplate)) //nolint:gosec // test loop index
		}

		stx.AddOutput(&sdktx.TransactionOutput{LockingScript: lock, Satoshis: perInput * 20})
		require.NoError(t, stx.Sign())

		btTx := bt.NewTx()
		for i := 0; i < 20; i++ {
			require.NoError(t, btTx.From(prevTxID, uint32(i), lockHex, perInput)) //nolint:gosec // test loop index
			btTx.Inputs[i].UnlockingScript = bscript.NewFromBytes(*stx.Inputs[i].UnlockingScript)
		}

		outScript, err := bscript.NewFromHexString(lockHex)
		require.NoError(t, err)
		btTx.AddOutput(&bt.Output{Satoshis: perInput * 20, LockingScript: outScript})

		return btTx
	}

	// check runs one configuration through BDK and the Go predicate and requires
	// they agree on whether the zero-fee consolidation is exempt.
	check := func(t *testing.T, name string, maxInputScriptSize, minConf int, confirmations uint32, wantExempt bool) {
		t.Helper()

		t.Run(name, func(t *testing.T) {
			s := test.CreateBaseTestSettings(t)
			po := s.Policy
			po.MinMiningTxFee = 0.01 // any non-exempt zero-fee tx must fail
			po.MaxConsolidationInputScriptSize = maxInputScriptSize
			po.MinConfConsolidationInput = minConf

			verifier := newScriptVerifierGoBDK(ulogger.TestLogger{}, po, s.ChainCfgParams)

			tx := buildConsolidation(t)

			heights := make([]uint32, 20)
			for i := range heights {
				heights[i] = blockHeight - confirmations
			}

			bdkErr := verifier.ValidateTransaction(tx, blockHeight, false, heights)
			goExempt := isFreeConsolidationTxn(po, tx, blockHeight, heights, genesisHeight)

			if wantExempt {
				require.NoError(t, bdkErr, "BDK must exempt this zero-fee consolidation")
			} else {
				require.Error(t, bdkErr, "BDK must charge this zero-fee consolidation")
				require.True(t, errors.Is(bdkErr, errors.ErrTxPolicy), "expected a policy error, got %v", bdkErr)
			}

			require.Equal(t, wantExempt, goExempt, "the Go predicate must agree with BDK")
		})
	}

	// Baseline: the documented defaults, deeply confirmed inputs.
	check(t, "defaults, deeply confirmed", 150, 6, 1_000, true)

	// A zero max-input-script-size must not be read raw: a raw 0 would reject
	// every 107-byte unlocking script and refuse the exemption.
	check(t, "zero maxconsolidationinputscriptsize still exempts", 0, 6, 1_000, true)

	// A zero minconf must not be read raw either: raw 0 would waive the
	// confirmation rule and exempt a 3-confirmation consolidation. BDK rewrites
	// it to 6, so this is NOT exempt.
	check(t, "zero minconfconsolidationinput still requires 6 confirmations", 150, 0, 3, false)

	// Control: the same shallow consolidation with an explicit 6.
	check(t, "explicit minconf 6 rejects 3 confirmations", 150, 6, 3, false)
}

// TestScriptTieredFeesAcceptWhatBDKAccepts guards the containment property the
// whole design rests on: with the tiers enabled but priced at zero surcharge for
// ordinary scripts, a transaction BDK accepts must not be rejected by the Go
// tier check. It uses the same signed consolidation as the differential tests.
func TestScriptTieredFeesAcceptWhatBDKAccepts(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Policy.MinMiningTxFee = 0
	// Thresholds far above any ordinary script: the surcharge must be zero.
	tSettings.Policy.MinMiningTxFeeByScriptSize = []settings.FeeTier{{Threshold: 500_000, SatoshisPerK: 10}}
	tSettings.Policy.MinMiningTxFeeByScriptOps = []settings.FeeTier{{Threshold: 1_000_000, SatoshisPerK: 10}}

	tv := NewTxValidator(ulogger.TestLogger{}, tSettings)
	tv.bdk = noopBDKValidator{}

	tx := newConsolidationTestTx(t, 20, 1, 107, 0)

	require.NoError(t, tv.checkScriptTieredFees(tx, scriptTierTestBlockHeight, deepHeights(20)),
		"ordinary scripts must incur no surcharge under realistic thresholds")
}
