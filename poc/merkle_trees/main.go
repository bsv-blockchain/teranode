package main

import (
	"fmt"
	"log"
	"sync"

	"github.com/libsv/go-bk/crypto"
	"github.com/libsv/go-bt/v2/chainhash"
)

func main() {
	// list of transactions
	txIds := []string{
		"5f133b8d45469ca8ba9fb576b4685f864da30ff6c9567dcf3b2a316fff45d573",
		"87c7386d4db171d0fbc5867ff568708646983a60c4a9726638b917827c128aeb",
		"291f39bc77b2cde5464b9078900d8af6b32a74a57b881e6182650bad92ca366d",
		"318f73840f5930cceb630834b5410191326dd55ba0504670ec6665038353bd8a",
		"982d92684d5d6600046a561e93a70ce30b06a27660d3371c52021aa5b338ac5d",
		"2c910c835e914168743aec281d8512b257e963050aec7f58ee5fa1b430ba4625",
		"f559cda9bb3edd5dd8c17604709ff618542ca23adac5e62942f3fe34bad44f8f",
		"09da086e2c3c9fe0bb29d0b6f5bf3a4329c197eb89ade9d73c63f0ef25b2195f",
	}

	var txs []*chainhash.Hash

	for _, txid := range txIds {
		// convert the transaction ID from a string to a byte slice
		tx, err := chainhash.NewHashFromStr(txid)
		if err != nil {
			log.Fatal(err)
		}

		txs = append(txs, tx)
	}

	merkleRootAll := merkleRoot(txs)
	fmt.Printf("Single Merkle Root:   %s\n", merkleRootAll.String())

	// split the transactions into two smaller lists
	half := len(txs) / 2
	txs1 := txs[:half]
	txs2 := txs[half:]

	var subRoots []*chainhash.Hash

	// create wait group to synchronize the two goroutines
	var wg sync.WaitGroup
	wg.Add(2)

	// calculate the Merkle root for the first half of the transactions in a goroutine
	var merkleRoot1 *chainhash.Hash
	go func() {
		defer wg.Done()
		merkleRoot1 = merkleRoot(txs1)
	}()

	// calculate the Merkle root for the second half of the transactions in a goroutine
	var merkleRoot2 *chainhash.Hash
	go func() {
		defer wg.Done()
		merkleRoot2 = merkleRoot(txs2)
	}()

	// wait for both goroutines to finish
	wg.Wait()

	// combine the two Merkle roots and calculate the final Merkle root

	subRoots = append(subRoots, merkleRoot1)
	subRoots = append(subRoots, merkleRoot2)

	finalMerkleRoot := merkleRoot(subRoots)

	fmt.Printf("Combined Merkle Root: %s\n", finalMerkleRoot)
}

func merkleRoot(leaves []*chainhash.Hash) *chainhash.Hash {
	for len(leaves) > 1 {
		var newLeaves []*chainhash.Hash

		for i := 0; i < len(leaves); i += 2 {
			var left, right *chainhash.Hash

			left = leaves[i]

			if i+1 < len(leaves) {
				right = leaves[i+1]
			} else {
				right = left
			}

			hash, err := chainhash.NewHash(crypto.Sha256d(append(left.CloneBytes(), right.CloneBytes()...))[:])
			if err != nil {
				log.Fatal(err)
			}

			newLeaves = append(newLeaves, hash)
		}

		leaves = newLeaves
	}

	return leaves[0]
}
