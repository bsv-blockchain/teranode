package util

// BuildMerkleRootFromCoinbase builds the merkle root of the block from the coinbase transaction hash (txid)
// and the merkle branches needed to work up the merkle tree and returns the merkle root byte array.
func BuildMerkleRootFromCoinbase(coinbaseHash []byte, merkleBranches [][]byte) []byte {
	acc := coinbaseHash

	for _, branch := range merkleBranches {
		// Build the concatenation in a freshly allocated buffer rather than
		// append(acc, branch...): on the first iteration acc still aliases the
		// caller's coinbaseHash backing array, so a spare-capacity slice would be
		// mutated in place (corrupting the caller and the merkle root).
		concat := make([]byte, 0, len(acc)+len(branch))
		concat = append(concat, acc...)
		concat = append(concat, branch...)
		hash := Sha256d(concat)
		acc = hash
	}

	return acc
}
