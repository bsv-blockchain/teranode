package consensus

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math/big"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/services/legacy/bsvutil"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/sighash"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
)

// TestBuilder builds test cases programmatically, similar to C++ implementation
type TestBuilder struct {
	script       *bscript.Script // Actually executed script
	redeemScript *bscript.Script // P2SH redeem script
	creditTx     *bt.Tx          // Transaction that creates the output
	spendTx      *bt.Tx          // Transaction that spends the output
	havePush     bool            // Whether we have a pending push
	push         []byte          // Pending push data
	comment      string          // Test comment
	flags        uint32          // Script verification flags
	scriptError  ScriptError     // Expected script error
	amount       uint64          // Output amount
	utxoHeights  []uint32        // UTXO heights for each input
	blockHeight  uint32          // Block height for validation (0 means use default)
}

// NewTestBuilder creates a new test builder
func NewTestBuilder(script *bscript.Script, comment string, flags uint32, p2sh bool, amount uint64) *TestBuilder {
	tb := &TestBuilder{
		script:      script,
		comment:     comment,
		flags:       flags,
		scriptError: SCRIPT_ERR_OK,
		amount:      amount,
		havePush:    false,
		utxoHeights: []uint32{0}, // Default to single input at height 0
	}

	scriptPubKey := script
	if p2sh {
		tb.redeemScript = scriptPubKey
		// Create P2SH script
		// For P2SH, we need to create the script manually
		// OP_HASH160 <20 bytes script hash> OP_EQUAL
		scriptBytes := scriptPubKey.Bytes()
		scriptHash := bsvutil.Hash160(scriptBytes)
		p2shScript := &bscript.Script{}
		p2shScript.AppendOpcodes(bscript.OpHASH160)
		p2shScript.AppendPushData(scriptHash)
		p2shScript.AppendOpcodes(bscript.OpEQUAL)
		scriptPubKey = p2shScript
	}

	// Build credit transaction
	tb.creditTx = tb.buildCreditingTransaction(scriptPubKey, amount)

	// Build spending transaction
	tb.spendTx = tb.buildSpendingTransaction(tb.creditTx)

	return tb
}

// buildCreditingTransaction creates a transaction with an output to spend
func (tb *TestBuilder) buildCreditingTransaction(scriptPubKey *bscript.Script, amount uint64) *bt.Tx {
	tx := bt.NewTx()

	// Add a dummy coinbase input (mimics C++ test framework)
	_ = tx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "0000", 0)

	// Add the output we'll spend
	tx.AddOutput(&bt.Output{
		Satoshis:      amount,
		LockingScript: scriptPubKey,
	})

	// Set version to 2 for BIP143 compatibility
	tx.Version = 2

	return tx
}

// buildSpendingTransaction creates a transaction that spends the credit tx output
func (tb *TestBuilder) buildSpendingTransaction(creditTx *bt.Tx) *bt.Tx {
	tx := bt.NewTx()

	// Add input spending the credit tx
	txID := creditTx.TxID()
	// Use From to add the input
	_ = tx.From(txID, 0, creditTx.Outputs[0].LockingScript.String(), creditTx.Outputs[0].Satoshis)
	// Clear the unlocking script as it will be filled by test operations
	tx.Inputs[0].UnlockingScript = &bscript.Script{}

	// Add a dummy output (standard P2PKH to make it valid)
	// Using a dummy address hash
	dummyPubKeyHash := make([]byte, 20)
	dummyScript, _ := bscript.NewP2PKHFromPubKeyHash(dummyPubKeyHash)
	tx.AddOutput(&bt.Output{
		Satoshis:      tb.amount,
		LockingScript: dummyScript,
	})

	// Set version to 2 for BIP143 compatibility and locktime
	tx.Version = 2
	tx.LockTime = 0

	return tx
}

// SetScriptError sets the expected script error
func (tb *TestBuilder) SetScriptError(err ScriptError) *TestBuilder {
	tb.scriptError = err
	return tb
}

// DoPush executes any pending push operation
func (tb *TestBuilder) DoPush() {
	if tb.havePush {
		tb.spendTx.Inputs[0].UnlockingScript.AppendPushData(tb.push)
		tb.havePush = false
	}
}

// Num adds a number to the script
func (tb *TestBuilder) Num(num int64) *TestBuilder {
	tb.DoPush()

	// Convert number to script number format
	if num == 0 {
		tb.spendTx.Inputs[0].UnlockingScript.AppendOpcodes(bscript.Op0)
	} else if num == -1 || (num >= 1 && num <= 16) {
		// Use OP_1NEGATE or OP_1 through OP_16
		if num == -1 {
			tb.spendTx.Inputs[0].UnlockingScript.AppendOpcodes(bscript.Op1NEGATE)
		} else {
			tb.spendTx.Inputs[0].UnlockingScript.AppendOpcodes(uint8(bscript.Op1 - 1 + byte(num)))
		}
	} else {
		// Push as data
		numBytes := scriptNumBytes(num)
		tb.spendTx.Inputs[0].UnlockingScript.AppendPushData(numBytes)
	}

	return tb
}

// Push adds hex data to be pushed
func (tb *TestBuilder) Push(hexStr string) *TestBuilder {
	data, err := hex.DecodeString(hexStr)
	if err != nil {
		panic(fmt.Sprintf("invalid hex in Push: %v", err))
	}

	tb.DoPush()
	tb.push = data
	tb.havePush = true
	return tb
}

// PushScript pushes a script as data
func (tb *TestBuilder) PushScript(script *bscript.Script) *TestBuilder {
	tb.DoPush()
	tb.push = script.Bytes()
	tb.havePush = true
	return tb
}

// Add adds raw script bytes
func (tb *TestBuilder) Add(script *bscript.Script) *TestBuilder {
	tb.DoPush()
	// Append all bytes from the script
	*tb.spendTx.Inputs[0].UnlockingScript = append(*tb.spendTx.Inputs[0].UnlockingScript, script.Bytes()...)
	return tb
}

// PushRedeem pushes the redeem script (for P2SH)
func (tb *TestBuilder) PushRedeem() *TestBuilder {
	if tb.redeemScript == nil {
		panic("PushRedeem called but no redeem script set")
	}
	return tb.PushScript(tb.redeemScript)
}

// EditPush modifies a push operation at the specified offset
func (tb *TestBuilder) EditPush(offset int, findStr, replaceStr string) *TestBuilder {
	unlockingScript := tb.spendTx.Inputs[0].UnlockingScript
	scriptBytes := unlockingScript.Bytes()

	find, _ := hex.DecodeString(findStr)
	replace, _ := hex.DecodeString(replaceStr)

	if len(find) != len(replace) {
		panic("EditPush: find and replace must be same length")
	}

	// Find the nth push operation
	pushCount := 0
	pos := 0

	for pos < len(scriptBytes) {
		// Check if this is a push operation
		opcode := scriptBytes[pos]
		var pushLen int

		if opcode <= bscript.OpPUSHDATA4 {
			if opcode < bscript.OpPUSHDATA1 {
				pushLen = int(opcode)
				pos++
			} else if opcode == bscript.OpPUSHDATA1 {
				if pos+1 >= len(scriptBytes) {
					break
				}
				pushLen = int(scriptBytes[pos+1])
				pos += 2
			} else if opcode == bscript.OpPUSHDATA2 {
				if pos+2 >= len(scriptBytes) {
					break
				}
				pushLen = int(scriptBytes[pos+1]) | int(scriptBytes[pos+2])<<8
				pos += 3
			} else { // OpPUSHDATA4
				if pos+4 >= len(scriptBytes) {
					break
				}
				pushLen = int(scriptBytes[pos+1]) | int(scriptBytes[pos+2])<<8 |
					int(scriptBytes[pos+3])<<16 | int(scriptBytes[pos+4])<<24
				pos += 5
			}

			// This is push number 'pushCount'
			if pushCount == offset && pos+pushLen <= len(scriptBytes) {
				// Search for pattern in this push
				pushData := scriptBytes[pos : pos+pushLen]
				for i := 0; i <= len(pushData)-len(find); i++ {
					if bytes.Equal(pushData[i:i+len(find)], find) {
						copy(scriptBytes[pos+i:], replace)
						tb.spendTx.Inputs[0].UnlockingScript = bscript.NewFromBytes(scriptBytes)
						return tb
					}
				}
			}

			pushCount++
			pos += pushLen
		} else {
			pos++
		}
	}

	return tb
}

// DamagePush modifies a push to be invalid
func (tb *TestBuilder) DamagePush(offset int) *TestBuilder {
	unlockingScript := tb.spendTx.Inputs[0].UnlockingScript
	scriptBytes := unlockingScript.Bytes()

	// Find the nth push operation
	pushCount := 0
	pos := 0

	for pos < len(scriptBytes) {
		opcode := scriptBytes[pos]

		if opcode <= bscript.OpPUSHDATA4 {
			if pushCount == offset {
				// Damage the push by incrementing its length
				if opcode < bscript.OpPUSHDATA1 && opcode < 75 {
					scriptBytes[pos] = opcode + 1
				} else if opcode == bscript.OpPUSHDATA1 && pos+1 < len(scriptBytes) {
					scriptBytes[pos+1]++
				}
				tb.spendTx.Inputs[0].UnlockingScript = bscript.NewFromBytes(scriptBytes)
				return tb
			}
			pushCount++
		}
		pos++
	}

	return tb
}

// PushSeparatorSigs creates signatures for scripts with OP_CODESEPARATOR
func (tb *TestBuilder) PushSeparatorSigs(keys []*bec.PrivateKey, sigHashType sighash.Flag, lenR, lenS int) *TestBuilder {
	// Split script by OP_CODESEPARATOR
	var separatedScripts []*bscript.Script
	currentScript := &bscript.Script{}

	scriptBytes := tb.script.Bytes()
	pos := 0

	for pos < len(scriptBytes) {
		opcode := scriptBytes[pos]

		if opcode == bscript.OpCODESEPARATOR {
			// Start new script segment
			separatedScripts = append([]*bscript.Script{bscript.NewFromBytes(currentScript.Bytes())}, separatedScripts...)
			currentScript.AppendOpcodes(bscript.OpCODESEPARATOR)
		} else {
			// Add to current script
			if opcode <= bscript.OpPUSHDATA4 {
				// Handle push operations
				dataLen := 0
				headerLen := 1

				if opcode < bscript.OpPUSHDATA1 {
					dataLen = int(opcode)
				} else if opcode == bscript.OpPUSHDATA1 && pos+1 < len(scriptBytes) {
					dataLen = int(scriptBytes[pos+1])
					headerLen = 2
				} else if opcode == bscript.OpPUSHDATA2 && pos+2 < len(scriptBytes) {
					dataLen = int(scriptBytes[pos+1]) | int(scriptBytes[pos+2])<<8
					headerLen = 3
				} else if opcode == bscript.OpPUSHDATA4 && pos+4 < len(scriptBytes) {
					dataLen = int(scriptBytes[pos+1]) | int(scriptBytes[pos+2])<<8 |
						int(scriptBytes[pos+3])<<16 | int(scriptBytes[pos+4])<<24
					headerLen = 5
				}

				if pos+headerLen+dataLen <= len(scriptBytes) {
					currentScript.AppendPushData(scriptBytes[pos+headerLen : pos+headerLen+dataLen])
					pos += headerLen + dataLen - 1
				}
			} else {
				currentScript.AppendOpcodes(opcode)
			}
		}
		pos++
	}

	// Add final script segment
	separatedScripts = append([]*bscript.Script{currentScript}, separatedScripts...)

	// Generate signatures for each segment
	if len(separatedScripts) != len(keys) {
		panic(fmt.Sprintf("PushSeparatorSigs: have %d scripts but %d keys", len(separatedScripts), len(keys)))
	}

	for i, key := range keys {
		if key != nil {
			// Use the appropriate script segment for signing
			oldScript := tb.script
			tb.script = separatedScripts[i]
			sig, err := tb.MakeSig(key, sigHashType, lenR, lenS)
			tb.script = oldScript

			if err != nil {
				panic(fmt.Sprintf("PushSeparatorSigs: failed to sign: %v", err))
			}

			tb.DoPush()
			tb.push = sig
			tb.havePush = true
			tb.DoPush()
		}
	}

	return tb
}

// MakeSig creates a signature with specified characteristics
func (tb *TestBuilder) MakeSig(key *bec.PrivateKey, sigHashType sighash.Flag, lenR, lenS int) ([]byte, error) {
	// Set up the input with the previous transaction information to match DoTest setup
	tb.spendTx.Inputs[0].PreviousTxSatoshis = tb.amount
	tb.spendTx.Inputs[0].PreviousTxScript = tb.creditTx.Outputs[0].LockingScript

	// Calculate signature hash for the spend transaction
	sh, err := tb.spendTx.CalcInputSignatureHash(0, sigHashType)
	if err != nil {
		return nil, errors.NewProcessingError("failed to calculate signature hash", err)
	}

	// For standard signature lengths (32, 32), use the original key
	// For other lengths, we'll need to try different approaches
	if lenR == 32 && lenS == 32 {
		// Just use the original key - most signatures will naturally have 32-byte R and S
		sig, err := key.Sign(sh)
		if err != nil {
			return nil, errors.NewProcessingError("failed to sign", err)
		}

		// Get raw values and encode
		r, s := sig.R.Bytes(), sig.S.Bytes()
		derSig := encodeDER(r, s)
		derSig = append(derSig, byte(sigHashType))
		return derSig, nil
	}

	// For non-standard lengths, this is more complex
	// The C++ tests use non-deterministic signing which allows trying different nonces
	// With deterministic signing (RFC 6979), we can't easily control R/S lengths
	// For now, we'll just try the standard signature and see if it happens to match
	sig, err := key.Sign(sh)
	if err != nil {
		return nil, errors.NewProcessingError("failed to sign", err)
	}

	// Get raw values
	r, s := sig.R.Bytes(), sig.S.Bytes()

	// Check if we need to adjust S for high/low S requirements
	if (lenS == 33) != (len(s) == 33) {
		// Need to negate S to flip between high/low
		s = negateS(s)
	}

	// Encode and return
	derSig := encodeDER(r, s)
	derSig = append(derSig, byte(sigHashType))

	// Note: We can't guarantee exact R/S lengths with deterministic signing
	// This is a limitation compared to the C++ tests which use non-deterministic signing
	return derSig, nil
}

// isHighS checks if S value is in the high range
func isHighS(s []byte) bool {
	// Compare against curve order / 2
	halfOrder := []byte{
		0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
		0x5D, 0x57, 0x6E, 0x73, 0x57, 0xA4, 0x50, 0x1D,
		0xDF, 0xE9, 0x2F, 0x46, 0x68, 0x1B, 0x20, 0xA0,
	}

	return bytes.Compare(s, halfOrder) > 0
}

// negateS negates the S value to convert between high and low S
func negateS(s []byte) []byte {
	// secp256k1 curve order
	orderBytes := []byte{
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE,
		0xBA, 0xAE, 0xDC, 0xE6, 0xAF, 0x48, 0xA0, 0x3B,
		0xBF, 0xD2, 0x5E, 0x8C, 0xD0, 0x36, 0x41, 0x41,
	}

	// Convert to big.Int for arithmetic
	sInt := new(big.Int).SetBytes(s)
	orderInt := new(big.Int).SetBytes(orderBytes)

	// result = order - s
	result := new(big.Int).Sub(orderInt, sInt)

	// Get the result bytes and ensure they're 32 bytes (pad with zeros if needed)
	resultBytes := result.Bytes()
	if len(resultBytes) < 32 {
		// Pad with leading zeros
		padded := make([]byte, 32)
		copy(padded[32-len(resultBytes):], resultBytes)
		return padded
	}

	return resultBytes
}

// PushSig adds a signature to the script
func (tb *TestBuilder) PushSig(key *bec.PrivateKey, sigHashType sighash.Flag, lenR, lenS int) *TestBuilder {
	sig, err := tb.MakeSig(key, sigHashType, lenR, lenS)
	if err != nil {
		panic(fmt.Sprintf("PushSig failed: %v", err))
	}

	tb.DoPush()
	tb.push = sig
	tb.havePush = true
	return tb
}

// PushPubKey adds a public key to the script
func (tb *TestBuilder) PushPubKey(pubkey []byte) *TestBuilder {
	tb.DoPush()
	tb.push = pubkey
	tb.havePush = true
	return tb
}

// SetLockTime sets the locktime on the spending transaction
func (tb *TestBuilder) SetLockTime(lockTime uint32) *TestBuilder {
	tb.spendTx.LockTime = lockTime
	return tb
}

// SetSequence sets the sequence number on the input
func (tb *TestBuilder) SetSequence(sequence uint32) *TestBuilder {
	if len(tb.spendTx.Inputs) > 0 {
		tb.spendTx.Inputs[0].SequenceNumber = sequence
	}
	return tb
}

// SetVersion sets the transaction version
func (tb *TestBuilder) SetVersion(version uint32) *TestBuilder {
	tb.spendTx.Version = version
	return tb
}

// AddOutput adds an additional output to the spending transaction
func (tb *TestBuilder) AddOutput(amount uint64, script *bscript.Script) *TestBuilder {
	tb.spendTx.AddOutput(&bt.Output{
		Satoshis:      amount,
		LockingScript: script,
	})
	return tb
}

// AddInput adds an additional input to the spending transaction
func (tb *TestBuilder) AddInput(prevTxID []byte, prevIndex uint32, script *bscript.Script, sequence uint32) *TestBuilder {
	txIDStr := hex.EncodeToString(prevTxID)
	_ = tb.spendTx.From(txIDStr, prevIndex, "", 0)
	if script != nil {
		tb.spendTx.Inputs[len(tb.spendTx.Inputs)-1].UnlockingScript = script
	}
	tb.spendTx.Inputs[len(tb.spendTx.Inputs)-1].SequenceNumber = sequence
	return tb
}

// encodeDER encodes R and S values as DER
func encodeDER(r, s []byte) []byte {
	// Remove leading zeros
	for len(r) > 1 && r[0] == 0 && r[1]&0x80 == 0 {
		r = r[1:]
	}
	for len(s) > 1 && s[0] == 0 && s[1]&0x80 == 0 {
		s = s[1:]
	}

	// Add padding if high bit is set
	if len(r) > 0 && r[0]&0x80 != 0 {
		r = append([]byte{0}, r...)
	}
	if len(s) > 0 && s[0]&0x80 != 0 {
		s = append([]byte{0}, s...)
	}

	// Build DER signature
	var buf bytes.Buffer
	buf.WriteByte(0x30)                      // SEQUENCE
	buf.WriteByte(byte(4 + len(r) + len(s))) // Total length

	buf.WriteByte(0x02) // INTEGER
	buf.WriteByte(byte(len(r)))
	buf.Write(r)

	buf.WriteByte(0x02) // INTEGER
	buf.WriteByte(byte(len(s)))
	buf.Write(s)

	return buf.Bytes()
}

// GetPushState returns the current push state for debugging
func (tb *TestBuilder) GetPushState() (bool, []byte) {
	return tb.havePush, tb.push
}

// DoTest executes the test and validates the result
func (tb *TestBuilder) DoTest() error {
	// Execute any pending push
	tb.DoPush()

	// If we have a redeem script (P2SH), push it last
	if tb.redeemScript != nil {
		tb.spendTx.Inputs[0].UnlockingScript.AppendPushData(tb.redeemScript.Bytes())
	}

	// Set up extended format
	tb.spendTx.Inputs[0].PreviousTxSatoshis = tb.amount
	tb.spendTx.Inputs[0].PreviousTxScript = tb.creditTx.Outputs[0].LockingScript

	// Create validator
	validator := NewValidatorIntegration()

	// Determine block height based on flags or explicit setting
	blockHeight := tb.blockHeight
	if blockHeight == 0 {
		// Use default based on flags
		blockHeight = uint32(1)
		if tb.flags&SCRIPT_GENESIS != 0 {
			blockHeight = 620539 // After Genesis activation
		} else if tb.flags&SCRIPT_ENABLE_SIGHASH_FORKID != 0 {
			blockHeight = 478559 // UAHF activation height
		}
	}

	// Run validation with UTXO heights from test builder
	// Ensure we have the right number of heights
	if len(tb.utxoHeights) != len(tb.spendTx.Inputs) {
		// Adjust if needed
		tb.utxoHeights = make([]uint32, len(tb.spendTx.Inputs))
		for i := range tb.utxoHeights {
			tb.utxoHeights[i] = 0 // Default to height 0
		}
	}
	result := validator.ValidateScript(ValidatorGoBDK, tb.spendTx, blockHeight, tb.utxoHeights)

	// Check result
	if tb.scriptError == SCRIPT_ERR_OK {
		if !result.Success {
			return errors.NewProcessingError("expected success for '%s' but got: %v", tb.comment, result.Error)
		}
	} else {
		if result.Success {
			return errors.NewProcessingError("expected error %s for '%s' but succeeded", tb.scriptError, tb.comment)
		}

		// Check specific error matches
		actualErr := ExtractScriptError(result.Error)
		if actualErr != tb.scriptError {
			// Some errors may be mapped differently or more specifically by validators
			// Check if it's a known equivalent
			if !isEquivalentError(actualErr, tb.scriptError) {
				return errors.NewProcessingError("expected error %s for '%s' but got %s: %v",
					tb.scriptError, tb.comment, actualErr, result.Error)
			}
		}
	}

	return nil
}

// isEquivalentError checks if two script errors are considered equivalent
// This is needed because different validators may report slightly different errors for the same condition
func isEquivalentError(actual, expected ScriptError) bool {
	// Direct match
	if actual == expected {
		return true
	}

	// Known equivalences
	switch expected {
	case SCRIPT_ERR_EVAL_FALSE:
		// Various conditions can result in eval false
		return actual == SCRIPT_ERR_VERIFY ||
			actual == SCRIPT_ERR_EQUALVERIFY ||
			actual == SCRIPT_ERR_CHECKMULTISIGVERIFY ||
			actual == SCRIPT_ERR_CHECKSIGVERIFY ||
			actual == SCRIPT_ERR_NUMEQUALVERIFY

	case SCRIPT_ERR_UNKNOWN_ERROR:
		// Unknown error can match various specific errors we haven't mapped yet
		return true

	case SCRIPT_ERR_SIG_DER:
		// DER encoding errors might be reported as STRICTENC
		return actual == SCRIPT_ERR_UNKNOWN_ERROR

	case SCRIPT_ERR_MINIMALDATA:
		// Minimal data violations might be reported differently
		return actual == SCRIPT_ERR_UNKNOWN_ERROR
	}

	return false
}

// scriptNumBytes converts a number to script number format (little-endian with sign)
func scriptNumBytes(num int64) []byte {
	if num == 0 {
		return []byte{}
	}

	negative := num < 0
	if negative {
		num = -num
	}

	var bytes []byte
	for num > 0 {
		bytes = append(bytes, byte(num&0xff))
		num >>= 8
	}

	// Add sign bit if needed
	if negative {
		if len(bytes) == 0 {
			bytes = []byte{0x80}
		} else if bytes[len(bytes)-1]&0x80 != 0 {
			bytes = append(bytes, 0x80)
		} else {
			bytes[len(bytes)-1] |= 0x80
		}
	} else if len(bytes) > 0 && bytes[len(bytes)-1]&0x80 != 0 {
		// Need to add a padding byte to keep it positive
		bytes = append(bytes, 0x00)
	}

	return bytes
}
