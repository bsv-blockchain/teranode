package consensus

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseScriptBitcoinSV(t *testing.T) {
	long := strings.Repeat("a", 76)

	tests := []struct {
		script string
		want   string
	}{
		// Numbers: OP_N where one fits, otherwise a minimal CScriptNum push.
		{"0", "00"},
		{"-1", "4f"},
		{"1 16", "5160"},
		{"17", "0111"},
		{"-2", "0182"},
		{"127", "017f"},
		{"128", "028000"},
		{"-128", "028080"},
		{"255", "02ff00"},
		{"65535", "03ffff00"},
		{"2147483647", "04ffffff7f"},
		{"2147483648", "050000008000"},
		{"4294967295", "05ffffffff00"},
		{"-2147483648", "050000008080"},
		{"9223372036854775807", "08ffffffffffffff7f"},
		{"-9223372036854775808", "09000000000000008080"},
		{"-", "00"}, // bitcoin-sv's digit check passes a lone '-', and atoi64 reads 0

		// 0x tokens are raw bytes, never pushed.
		{"0x4c 0x02 0x6065", "4c026065"},
		{"1 0x00 0x02 0x0203 2 CHECKMULTISIG", "510002020352ae"},
		{"0x01 0x0b", "010b"},
		{"0xdeadbeef", "deadbeef"},

		// Opcode names with or without OP_.
		{"DUP HASH160 0x14 0x89abcdefabbaabbaabbaabbaabbaabbaabbaabba EQUALVERIFY CHECKSIG", "76a91489abcdefabbaabbaabbaabbaabbaabbaabbaabba88ac"},
		{"OP_DUP OP_CHECKSIG", "76ac"},
		{"NOP CHECKLOCKTIMEVERIFY", "61b1"},

		// Quoted strings are pushed, with PUSHDATA1 from 76 bytes.
		{"'abc'", "03616263"},
		{"''", "00"},
		{"'" + long + "'", "4c4c" + hex.EncodeToString([]byte(long))},

		// PUSHDATAn followed by its length and exactly that much data.
		{"PUSHDATA1 0x01 0x07", "4c0107"},
		{"0x4d 0x0200 0xabcd", "4d0200abcd"},
	}

	for _, tt := range tests {
		t.Run(tt.script, func(t *testing.T) {
			got, err := ParseScriptBitcoinSV(tt.script)
			require.NoError(t, err)
			require.Equal(t, tt.want, hex.EncodeToString(got))
		})
	}

	t.Run("a push followed by the wrong amount of data is an error", func(t *testing.T) {
		for _, s := range []string{"0x02 0x01", "0x01 0x0102", "0x4c 0xae 0x6065", "PUSHDATA1 0x02 0x01", "0x4d 0x0300 0xabcd"} {
			_, err := ParseScriptBitcoinSV(s)
			require.ErrorContains(t, err, "wrong number of bytes being pushed", s)
		}
	})

	t.Run("bad tokens are errors", func(t *testing.T) {
		for _, s := range []string{"0x0", "0xzz", "NOTANOPCODE", "1.5"} {
			_, err := ParseScriptBitcoinSV(s)
			require.Error(t, err, s)
		}
	})
}
