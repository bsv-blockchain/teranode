package model

import (
	"encoding/binary"
	"math"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-chaincfg"
)

const (
	// maxMoneySatoshis is bitcoin-sv's MAX_MONEY: 21 million coins of 100 million satoshis each.
	maxMoneySatoshis = 21_000_000 * 100_000_000

	// maxTxSizeConsensusBeforeGenesis and maxTxSizeConsensusAfterGenesis are bitcoin-sv's
	// MAX_TX_SIZE_CONSENSUS_BEFORE_GENESIS (ONE_MEGABYTE) and MAX_TX_SIZE_CONSENSUS_AFTER_GENESIS
	// (ONE_GIGABYTE), both decimal. The post-Genesis value is a fixed constant in bitcoin-sv, not
	// a setting.
	maxTxSizeConsensusBeforeGenesis = 1_000_000
	maxTxSizeConsensusAfterGenesis  = 1_000_000_000

	// maxTxSigOpsCountBeforeGenesis is bitcoin-sv's MAX_TX_SIGOPS_COUNT_BEFORE_GENESIS.
	maxTxSigOpsCountBeforeGenesis = 20_000

	// maxPubKeysPerMultiSigBeforeGenesis is what one CHECKMULTISIG counts for in the pre-Genesis
	// (inaccurate) sigop count: bitcoin-sv's MAX_PUBKEYS_PER_MULTISIG_BEFORE_GENESIS.
	maxPubKeysPerMultiSigBeforeGenesis = 20
)

// CoinbaseCommonRuleViolation applies bitcoin-sv's CheckTransactionCommon to a block's coinbase and
// returns the bitcoin-sv reject reason of the first rule it breaks, or "" when it breaks none. The
// rules run in bitcoin-sv's order: non-empty inputs, non-empty outputs, the consensus size limit,
// the per-output and total money range, and, before Genesis only, the sigop limit.
//
// bitcoin-sv's CheckCoinbase runs these between the is-coinbase check and bad-cb-length. Regular
// transactions get the same rules from BDK, but the coinbase never goes through BDK, which is how a
// coinbase with no outputs was accepted (bitcoin-sv/teranode#4835): the reward check sums zero
// outputs, and zero never exceeds the subsidy.
//
// Genesis is enabled from params.GenesisActivationHeight onwards, the same boundary bitcoin-sv's
// IsGenesisEnabled and the validator use.
//
// The caller must guarantee a non-nil coinbaseTx: Inputs and Outputs are read unconditionally.
// Valid's step 4 and both quick-validation entry points nil-check before calling.
//
// Deliberately a reason rather than an error, for the same reason CoinbaseScriptSigLengthInBounds
// is a predicate: the verdict depends on whether the body is bound to its header, which only the
// caller knows. Valid turns a violation into bindErr's binding-aware verdict, and the
// quick-validation path, which never calls Valid, applies its own.
func CoinbaseCommonRuleViolation(coinbaseTx *bt.Tx, height uint32, params *chaincfg.Params) string {
	if len(coinbaseTx.Inputs) == 0 {
		return "bad-txns-vin-empty"
	}

	if len(coinbaseTx.Outputs) == 0 {
		return "bad-txns-vout-empty"
	}

	genesisEnabled := height >= params.GenesisActivationHeight

	maxTxSize := maxTxSizeConsensusAfterGenesis
	if !genesisEnabled {
		maxTxSize = maxTxSizeConsensusBeforeGenesis
	}

	if coinbaseTx.Size() > maxTxSize {
		return "bad-txns-oversize"
	}

	// bitcoin-sv reads each 8-byte value as a signed amount, so a value with the sign bit set is
	// negative. Every value added is within [0, maxMoneySatoshis], so the running total cannot wrap
	// before it is rejected.
	var totalOut uint64

	for _, out := range coinbaseTx.Outputs {
		if out.Satoshis > math.MaxInt64 {
			return "bad-txns-vout-negative"
		}

		if out.Satoshis > maxMoneySatoshis {
			return "bad-txns-vout-toolarge"
		}

		totalOut += out.Satoshis
		if totalOut > maxMoneySatoshis {
			return "bad-txns-txouttotal-toolarge"
		}
	}

	// No sigop limit after Genesis. Before it, bitcoin-sv's GetSigOpCountWithoutP2SH sums the
	// inaccurate count over every input script and every output script. Its sigOpCountError is
	// only ever raised on the accurate or post-Genesis counting paths, so it cannot fire here.
	if !genesisEnabled {
		var sigOps uint64

		for _, in := range coinbaseTx.Inputs {
			if in.UnlockingScript != nil {
				sigOps += preGenesisSigOpCount(*in.UnlockingScript)
			}
		}

		for _, out := range coinbaseTx.Outputs {
			if out.LockingScript != nil {
				sigOps += preGenesisSigOpCount(*out.LockingScript)
			}
		}

		if sigOps > maxTxSigOpsCountBeforeGenesis {
			return "bad-txn-sigops"
		}
	}

	return ""
}

// preGenesisSigOpCount ports bitcoin-sv's CScript::GetSigOpCount(fAccurate=false) for a pre-Genesis
// era: CHECKSIG and CHECKSIGVERIFY count 1, CHECKMULTISIG and CHECKMULTISIGVERIFY count 20, and
// pushed data counts nothing. Counting stops at the first instruction bitcoin-sv's
// decode_instruction cannot decode (a push longer than the bytes left), and at a literal 0xff byte,
// since both surface there as OP_INVALIDOPCODE.
//
// Not services/legacy/txscript.GetSigOpCount: that one keeps counting past a 0xff byte, where
// bitcoin-sv stops, so the two disagree on a script like CHECKSIG 0xff CHECKSIG.
func preGenesisSigOpCount(script []byte) uint64 {
	var n uint64

	for len(script) > 0 {
		opcode := script[0]
		script = script[1:]

		switch {
		case opcode == bscript.OpINVALIDOPCODE:
			return n
		case opcode == bscript.OpCHECKSIG || opcode == bscript.OpCHECKSIGVERIFY:
			n++
		case opcode == bscript.OpCHECKMULTISIG || opcode == bscript.OpCHECKMULTISIGVERIFY:
			n += maxPubKeysPerMultiSigBeforeGenesis
		case opcode > bscript.OpPUSHDATA4 || opcode == bscript.Op0:
			// Not a push, and not a sigop.
		default:
			operandLen, prefixLen, ok := pushOperandLength(opcode, script)
			if !ok || operandLen > uint64(len(script)-prefixLen) {
				return n
			}

			script = script[prefixLen+int(operandLen):] //nolint:gosec // operandLen <= len(script) was checked above
		}
	}

	return n
}

// pushOperandLength decodes the operand length of a push opcode (0x01 to OP_PUSHDATA4) from the
// bytes that follow it, returning the length, how many bytes the length itself took, and false
// when those length bytes are missing.
func pushOperandLength(opcode byte, rest []byte) (operandLen uint64, prefixLen int, ok bool) {
	switch opcode {
	case bscript.OpPUSHDATA1:
		if len(rest) < 1 {
			return 0, 0, false
		}

		return uint64(rest[0]), 1, true
	case bscript.OpPUSHDATA2:
		if len(rest) < 2 {
			return 0, 0, false
		}

		return uint64(binary.LittleEndian.Uint16(rest)), 2, true
	case bscript.OpPUSHDATA4:
		if len(rest) < 4 {
			return 0, 0, false
		}

		return uint64(binary.LittleEndian.Uint32(rest)), 4, true
	default:
		return uint64(opcode), 0, true
	}
}
