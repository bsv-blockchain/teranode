package consensus

import (
	"bytes"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/stretchr/testify/require"
)

// TxType represents transaction output types
type TxType int

const (
	TX_NONSTANDARD TxType = iota
	TX_PUBKEY
	TX_PUBKEYHASH
	TX_SCRIPTHASH
	TX_MULTISIG
	TX_NULL_DATA
)

// TestScriptSolver tests script template matching
func TestScriptSolver(t *testing.T) {
	tests := []struct {
		name         string
		script       *bscript.Script
		expectedType TxType
		solutions    [][]byte
	}{
		{
			name: "P2PK script",
			script: func() *bscript.Script {
				s := &bscript.Script{}
				_ = s.AppendPushData(make([]byte, 33)) // Compressed pubkey
				_ = s.AppendOpcodes(bscript.OpCHECKSIG)
				return s
			}(),
			expectedType: TX_PUBKEY,
			solutions:    [][]byte{make([]byte, 33)},
		},
		{
			name: "P2PKH script",
			script: func() *bscript.Script {
				s := &bscript.Script{}
				_ = s.AppendOpcodes(bscript.OpDUP)
				_ = s.AppendOpcodes(bscript.OpHASH160)
				_ = s.AppendPushData(make([]byte, 20))
				_ = s.AppendOpcodes(bscript.OpEQUALVERIFY)
				_ = s.AppendOpcodes(bscript.OpCHECKSIG)
				return s
			}(),
			expectedType: TX_PUBKEYHASH,
			solutions:    [][]byte{make([]byte, 20)},
		},
		{
			name: "P2SH script",
			script: func() *bscript.Script {
				s := &bscript.Script{}
				_ = s.AppendOpcodes(bscript.OpHASH160)
				_ = s.AppendPushData(make([]byte, 20))
				_ = s.AppendOpcodes(bscript.OpEQUAL)
				return s
			}(),
			expectedType: TX_SCRIPTHASH,
			solutions:    [][]byte{make([]byte, 20)},
		},
		{
			name: "1-of-2 multisig",
			script: func() *bscript.Script {
				s := &bscript.Script{}
				_ = s.AppendOpcodes(bscript.Op1)
				_ = s.AppendPushData(make([]byte, 33))
				_ = s.AppendPushData(make([]byte, 33))
				_ = s.AppendOpcodes(bscript.Op2)
				_ = s.AppendOpcodes(bscript.OpCHECKMULTISIG)
				return s
			}(),
			expectedType: TX_MULTISIG,
			solutions: [][]byte{
				{1}, // n = 1
				make([]byte, 33),
				make([]byte, 33),
				{2}, // m = 2
			},
		},
		{
			name: "OP_RETURN data",
			script: func() *bscript.Script {
				s := &bscript.Script{}
				_ = s.AppendOpcodes(bscript.OpRETURN)
				_ = s.AppendPushData([]byte("Hello Bitcoin"))
				return s
			}(),
			expectedType: TX_NULL_DATA,
			solutions:    [][]byte{[]byte("Hello Bitcoin")},
		},
		{
			name:         "Empty script",
			script:       &bscript.Script{},
			expectedType: TX_NONSTANDARD,
			solutions:    nil,
		},
		{
			name: "OP_TRUE only",
			script: func() *bscript.Script {
				s := &bscript.Script{}
				_ = s.AppendOpcodes(bscript.OpTRUE)
				return s
			}(),
			expectedType: TX_NONSTANDARD,
			solutions:    nil,
		},
		{
			name: "Non-standard script",
			script: func() *bscript.Script {
				s := &bscript.Script{}
				_ = s.AppendOpcodes(bscript.OpDUP)
				_ = s.AppendOpcodes(bscript.OpDUP)
				_ = s.AppendOpcodes(bscript.OpDUP)
				return s
			}(),
			expectedType: TX_NONSTANDARD,
			solutions:    nil,
		},
		{
			name: "Invalid P2PKH - wrong length",
			script: func() *bscript.Script {
				s := &bscript.Script{}
				_ = s.AppendOpcodes(bscript.OpDUP)
				_ = s.AppendOpcodes(bscript.OpHASH160)
				_ = s.AppendPushData(make([]byte, 19)) // Wrong length
				_ = s.AppendOpcodes(bscript.OpEQUALVERIFY)
				_ = s.AppendOpcodes(bscript.OpCHECKSIG)
				return s
			}(),
			expectedType: TX_NONSTANDARD,
			solutions:    nil,
		},
		{
			name: "3-of-3 multisig",
			script: func() *bscript.Script {
				s := &bscript.Script{}
				_ = s.AppendOpcodes(bscript.Op3)
				_ = s.AppendPushData(make([]byte, 33))
				_ = s.AppendPushData(make([]byte, 65)) // Uncompressed
				_ = s.AppendPushData(make([]byte, 33))
				_ = s.AppendOpcodes(bscript.Op3)
				_ = s.AppendOpcodes(bscript.OpCHECKMULTISIG)
				return s
			}(),
			expectedType: TX_MULTISIG,
			solutions: [][]byte{
				{3}, // n = 3
				make([]byte, 33),
				make([]byte, 65),
				make([]byte, 33),
				{3}, // m = 3
			},
		},
		{
			name: "OP_FALSE OP_RETURN",
			script: func() *bscript.Script {
				s := &bscript.Script{}
				_ = s.AppendOpcodes(bscript.OpFALSE)
				_ = s.AppendOpcodes(bscript.OpRETURN)
				_ = s.AppendPushData([]byte("data"))
				return s
			}(),
			expectedType: TX_NULL_DATA,
			solutions:    [][]byte{[]byte("data")},
		},
		{
			name: "Large OP_RETURN",
			script: func() *bscript.Script {
				s := &bscript.Script{}
				_ = s.AppendOpcodes(bscript.OpRETURN)
				_ = s.AppendPushData(make([]byte, 100000)) // Large data
				return s
			}(),
			expectedType: TX_NULL_DATA,
			solutions:    [][]byte{make([]byte, 100000)},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			txType, solutions := Solver(test.script)
			require.Equal(t, test.expectedType, txType)

			if test.solutions == nil {
				require.Nil(t, solutions)
			} else {
				require.Equal(t, len(test.solutions), len(solutions))
				for i, sol := range solutions {
					require.True(t, bytes.Equal(test.solutions[i], sol),
						"Solution %d mismatch: expected %x, got %x", i, test.solutions[i], sol)
				}
			}
		})
	}
}

// Solver analyzes a script and returns its type and data components
func Solver(script *bscript.Script) (TxType, [][]byte) {
	if script == nil || len(*script) == 0 {
		return TX_NONSTANDARD, nil
	}

	// Check for P2PKH: DUP HASH160 <20 bytes> EQUALVERIFY CHECKSIG
	if IsP2PKH(script) {
		hash160 := (*script)[3:23]
		return TX_PUBKEYHASH, [][]byte{hash160}
	}

	// Check for P2PK: <pubkey> CHECKSIG
	if IsP2PK(script) {
		s := *script
		if len(s) >= 35 && s[len(s)-1] == bscript.OpCHECKSIG {
			pubkeyLen := int(s[0])
			if pubkeyLen == 33 && len(s) == 35 { // Compressed
				return TX_PUBKEY, [][]byte{s[1:34]}
			} else if pubkeyLen == 65 && len(s) == 67 { // Uncompressed
				return TX_PUBKEY, [][]byte{s[1:66]}
			}
		}
		return TX_NONSTANDARD, nil
	}

	// Check for P2SH: HASH160 <20 bytes> EQUAL
	if IsP2SH(script) {
		hash160 := (*script)[2:22]
		return TX_SCRIPTHASH, [][]byte{hash160}
	}

	// Check for NULL_DATA (OP_RETURN)
	if IsNullData(script) {
		s := *script
		var data []byte

		if len(s) == 1 {
			// Just OP_RETURN
			return TX_NULL_DATA, [][]byte{}
		}

		startIdx := 1
		if s[0] == bscript.OpFALSE && len(s) > 1 && s[1] == bscript.OpRETURN {
			startIdx = 2
		}

		// Extract data after OP_RETURN
		if startIdx < len(s) {
			data = extractPushData(s[startIdx:])
		}

		return TX_NULL_DATA, [][]byte{data}
	}

	// Check for multisig: <n> <pubkey>... <m> CHECKMULTISIG
	if IsMultisig(script) {
		return parseMultisig(script)
	}

	return TX_NONSTANDARD, nil
}

// IsP2PKH checks if script is Pay-to-PubKey-Hash
func IsP2PKH(script *bscript.Script) bool {
	if script == nil || len(*script) != 25 {
		return false
	}
	s := *script
	return s[0] == bscript.OpDUP &&
		s[1] == bscript.OpHASH160 &&
		s[2] == 0x14 && // Push 20 bytes
		s[23] == bscript.OpEQUALVERIFY &&
		s[24] == bscript.OpCHECKSIG
}

// IsP2PK checks if script is Pay-to-PubKey
func IsP2PK(script *bscript.Script) bool {
	if script == nil {
		return false
	}
	s := *script

	// Compressed: 33 byte pubkey + CHECKSIG
	if len(s) == 35 && s[0] == 33 && s[34] == bscript.OpCHECKSIG {
		return true
	}

	// Uncompressed: 65 byte pubkey + CHECKSIG
	if len(s) == 67 && s[0] == 65 && s[66] == bscript.OpCHECKSIG {
		return true
	}

	return false
}

// IsP2SH checks if script is Pay-to-Script-Hash
func IsP2SH(script *bscript.Script) bool {
	if script == nil || len(*script) != 23 {
		return false
	}
	s := *script
	return s[0] == bscript.OpHASH160 &&
		s[1] == 0x14 && // Push 20 bytes
		s[22] == bscript.OpEQUAL
}

// IsNullData checks if script is an OP_RETURN output
func IsNullData(script *bscript.Script) bool {
	if script == nil || len(*script) == 0 {
		return false
	}
	s := *script

	// Standard OP_RETURN
	if s[0] == bscript.OpRETURN {
		return true
	}

	// OP_FALSE OP_RETURN pattern
	if len(s) >= 2 && s[0] == bscript.OpFALSE && s[1] == bscript.OpRETURN {
		return true
	}

	return false
}

// IsMultisig checks if script is a multisig output
func IsMultisig(script *bscript.Script) bool {
	if script == nil || len(*script) < 3 {
		return false
	}
	s := *script

	// Must end with CHECKMULTISIG
	if s[len(s)-1] != bscript.OpCHECKMULTISIG {
		return false
	}

	// Basic length check
	if len(s) < 5 { // Minimum: OP_1 <pubkey> OP_1 OP_CHECKMULTISIG
		return false
	}

	return true
}

// parseMultisig extracts n, pubkeys, and m from a multisig script
func parseMultisig(script *bscript.Script) (TxType, [][]byte) {
	s := *script
	solutions := [][]byte{}

	// First byte should be OP_1 through OP_16 for n
	if s[0] < bscript.Op1 || s[0] > bscript.Op16 {
		return TX_NONSTANDARD, nil
	}
	n := s[0] - bscript.Op1 + 1
	solutions = append(solutions, []byte{byte(n)})

	// Parse pubkeys
	idx := 1
	pubkeyCount := 0

	for idx < len(s)-2 { // Leave room for m and CHECKMULTISIG
		if s[idx] == 33 && idx+33 < len(s) {
			// Compressed pubkey
			solutions = append(solutions, s[idx+1:idx+34])
			idx += 34
			pubkeyCount++
		} else if s[idx] == 65 && idx+65 < len(s) {
			// Uncompressed pubkey
			solutions = append(solutions, s[idx+1:idx+66])
			idx += 66
			pubkeyCount++
		} else {
			break
		}
	}

	// Next byte should be OP_1 through OP_16 for m
	if idx >= len(s)-1 || s[idx] < bscript.Op1 || s[idx] > bscript.Op16 {
		return TX_NONSTANDARD, nil
	}
	m := s[idx] - bscript.Op1 + 1
	solutions = append(solutions, []byte{byte(m)})

	// Verify constraints
	// In Bitcoin script, for m-of-n multisig:
	// - First number (n) is the required signatures
	// - Last number (m) is the total pubkeys
	// - pubkeyCount should equal m (total pubkeys)
	if int(m) != pubkeyCount || n > m || m > 16 {
		return TX_NONSTANDARD, nil
	}

	return TX_MULTISIG, solutions
}

// extractPushData extracts pushed data from script bytes
func extractPushData(script []byte) []byte {
	if len(script) == 0 {
		return []byte{}
	}

	opcode := script[0]
	if opcode <= bscript.OpPUSHDATA4 {
		var dataLen int
		offset := 1

		if opcode < bscript.OpPUSHDATA1 {
			dataLen = int(opcode)
		} else if opcode == bscript.OpPUSHDATA1 && len(script) > 1 {
			dataLen = int(script[1])
			offset = 2
		} else if opcode == bscript.OpPUSHDATA2 && len(script) > 2 {
			dataLen = int(script[1]) | int(script[2])<<8
			offset = 3
		} else if opcode == bscript.OpPUSHDATA4 && len(script) > 4 {
			dataLen = int(script[1]) | int(script[2])<<8 | int(script[3])<<16 | int(script[4])<<24
			offset = 5
		}

		if offset+dataLen <= len(script) {
			return script[offset : offset+dataLen]
		}
	}

	return []byte{}
}

// TestExtractPubKeys tests public key extraction from scripts
func TestExtractPubKeys(t *testing.T) {
	keyData := NewKeyData()

	tests := []struct {
		name            string
		script          *bscript.Script
		expectedPubKeys [][]byte
	}{
		{
			name: "P2PK compressed",
			script: func() *bscript.Script {
				s := &bscript.Script{}
				_ = s.AppendPushData(keyData.Pubkey0)
				_ = s.AppendOpcodes(bscript.OpCHECKSIG)
				return s
			}(),
			expectedPubKeys: [][]byte{keyData.Pubkey0},
		},
		{
			name: "P2PK uncompressed",
			script: func() *bscript.Script {
				s := &bscript.Script{}
				_ = s.AppendPushData(keyData.Pubkey0U)
				_ = s.AppendOpcodes(bscript.OpCHECKSIG)
				return s
			}(),
			expectedPubKeys: [][]byte{keyData.Pubkey0U},
		},
		{
			name: "2-of-3 multisig mixed",
			script: func() *bscript.Script {
				s := &bscript.Script{}
				_ = s.AppendOpcodes(bscript.Op2)
				_ = s.AppendPushData(keyData.Pubkey0)
				_ = s.AppendPushData(keyData.Pubkey1U)
				_ = s.AppendPushData(keyData.Pubkey2C)
				_ = s.AppendOpcodes(bscript.Op3)
				_ = s.AppendOpcodes(bscript.OpCHECKMULTISIG)
				return s
			}(),
			expectedPubKeys: [][]byte{keyData.Pubkey0, keyData.Pubkey1U, keyData.Pubkey2C},
		},
		{
			name: "P2PKH - no direct pubkeys",
			script: func() *bscript.Script {
				s := &bscript.Script{}
				_ = s.AppendOpcodes(bscript.OpDUP)
				_ = s.AppendOpcodes(bscript.OpHASH160)
				_ = s.AppendPushData(keyData.Pubkey0Hash)
				_ = s.AppendOpcodes(bscript.OpEQUALVERIFY)
				_ = s.AppendOpcodes(bscript.OpCHECKSIG)
				return s
			}(),
			expectedPubKeys: [][]byte{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pubkeys := ExtractPubKeys(test.script)
			require.Equal(t, len(test.expectedPubKeys), len(pubkeys))

			for i, pk := range pubkeys {
				require.True(t, bytes.Equal(test.expectedPubKeys[i], pk),
					"Pubkey %d mismatch", i)
			}
		})
	}
}

// ExtractPubKeys extracts all public keys from a script
func ExtractPubKeys(script *bscript.Script) [][]byte {
	if script == nil {
		return nil
	}

	var pubkeys [][]byte
	s := *script
	i := 0

	for i < len(s) {
		// Check for pubkey push
		if s[i] == 33 && i+33 < len(s) {
			// Compressed pubkey
			pubkey := s[i+1 : i+34]
			if isValidPubKey(pubkey) {
				pubkeys = append(pubkeys, pubkey)
			}
			i += 34
		} else if s[i] == 65 && i+65 < len(s) {
			// Uncompressed pubkey
			pubkey := s[i+1 : i+66]
			if isValidPubKey(pubkey) {
				pubkeys = append(pubkeys, pubkey)
			}
			i += 66
		} else {
			i++
		}
	}

	return pubkeys
}

// isValidPubKey performs basic validation on a public key
func isValidPubKey(pubkey []byte) bool {
	if len(pubkey) == 33 {
		// Compressed: first byte must be 0x02 or 0x03
		return pubkey[0] == 0x02 || pubkey[0] == 0x03
	} else if len(pubkey) == 65 {
		// Uncompressed: first byte must be 0x04 (or 0x06/0x07 for hybrid)
		return pubkey[0] == 0x04 || pubkey[0] == 0x06 || pubkey[0] == 0x07
	}
	return false
}
