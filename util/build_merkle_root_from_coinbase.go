package util

// BuildMerkleRootFromCoinbase builds the merkle root of the block from the coinbase transaction hash (txid)
// and the merkle branches needed to work up the merkle tree and returns the merkle root byte array.
func BuildMerkleRootFromCoinbase(coinbaseHash []byte, merkleBranches [][]byte) []byte {
	acc := coinbaseHash

	for _, branch := range merkleBranches {
		concat := append(acc, branch...)
		hash := Sha256d(concat)
		acc = hash
	}

	return acc
}
