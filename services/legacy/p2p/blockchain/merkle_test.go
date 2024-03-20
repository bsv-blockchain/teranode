// Copyright (c) 2013-2017 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package blockchain

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestMerkle tests the BuildMerkleTreeStore API.
func TestMerkle(t *testing.T) {
	hexStrings := []string{
		"73CF0C15C8FDF9E23BFE47156316011F2BA69F5BDBE8D84822B3CF0354717522",
		"6D3E501C5FA574D920BC04FACA8AB67BF14232B73D5ACEBBDE208361D0C8609E",
		"C1AF996FA78A508FE479E8EE76A110D2B1FA66A3C1D6602DE385F5E326D6D466",
		"40A3B46822E862052147C6A0CDC4C6A4207C23FFDDF4F17B78709489180FC3A5",
		"C3F8BDBC6524F14FD752B7C6C4710F0B6DB86371EA534F5F456E346E72C38D1F",
	}

	var transactions [][]byte
	for _, hexString := range hexStrings {
		transaction, err := hex.DecodeString(hexString)
		if err != nil {
			t.Errorf("DecodeString: unexpected error: %v", err)
		}
		transactions = append(transactions, transaction)
	}

	merkles := BuildMerkleTreeStore(transactions)
	calculatedMerkleRoot := merkles[len(merkles)-1]

	assert.Equal(t, "76c11b3572336ff5aac56355b2e41f6673053b37c2494a909168c87b66daa312", hex.EncodeToString(calculatedMerkleRoot))
}
