package util

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildMerkleRootFromCoinbase(t *testing.T) {
	t.Run("block 797330 - tx 0", func(t *testing.T) {
		merkleBranchesStr := []string{
			"e736164db06b4ca5e1020bca8ddba837ecaa029d2caef7e57cf84e4383d66cad",
			"19dd8d4640891daef573cdf059c7539ff5d9dd8e06697c000925fd2015cee7d4",
			"10f00d867168e4e51bbe2800706d3acf7d8a8fd53bbe3e8388a45d8b3fadf265",
			"7fb48c2d1fe6fb6e1dfe411695e1297f3603dda9e1976cb3f931132aab59c0c9",
			"c3a387dc2f98da4f7706290e78714f904770a0be1e509a8f9cc4f19a4ab39d57",
			"6f27cd328e23e676a3d94c1dc55ded5a1d38905c0079c9f2ca4a467d810705d4",
			"b321f9bcd334766be8188524a9bb454d8f41129f2b48f82dd6084b9722e234a7",
			"e005abf2adbc22bdadda8da0eb79aae08627b1e07411c51c5fd58c63da0c6032",
			"881fe6b97fc34f6f4a4bb1ed3d5912108edb29dec69ac4eb2436f2345c31abd0",
			"cc6da767edfd473466d70a747348eee48f649d3173f762be0f41ac3bd418e681",
			"d8920119ca681be1d616ce0079edc0eb2664c389bcde81e7ca4d32993bd83180",
		}

		merkleBranches := make([][]byte, len(merkleBranchesStr))

		for i, s := range merkleBranchesStr {
			hash, err := chainhash.NewHashFromStr(s)
			require.NoError(t, err)

			merkleBranches[i] = hash.CloneBytes()
		}

		coinbaseHash, err := chainhash.NewHashFromStr("ddee6d8fb3cf03276e3ffcb25ccb34a7dacf733cea1b40ba3fa7a716426e93cd")
		require.NoError(t, err)

		merkleRoot := BuildMerkleRootFromCoinbase(coinbaseHash.CloneBytes(), merkleBranches)
		merkleRootHash, err := chainhash.NewHash(merkleRoot)
		require.NoError(t, err)

		assert.Equal(t, "596907991495a39471b16d4d69804372d7dc2d7d64bf9255767db34f36f5669f", merkleRootHash.String())
	})
}

func TestBuildMerkleRootFromCoinbaseOnlyTransaction(t *testing.T) {
	t.Run("ternanode block 1 - coinbase only", func(t *testing.T) {
		merkleBranchesStr := []string{}

		merkleBranches := make([][]byte, len(merkleBranchesStr))

		for i, s := range merkleBranchesStr {
			hash, err := chainhash.NewHashFromStr(s)
			require.NoError(t, err)

			merkleBranches[i] = hash.CloneBytes()
		}

		coinbaseHash, err := chainhash.NewHashFromStr("370b0097df4dc2caeaadae5cb703bc39dca5599b9d3443bb02387fc050f40b0d")
		require.NoError(t, err)

		merkleRoot := BuildMerkleRootFromCoinbase(coinbaseHash.CloneBytes(), merkleBranches)
		merkleRootHash, err := chainhash.NewHash(merkleRoot)
		require.NoError(t, err)

		assert.Equal(t, "370b0097df4dc2caeaadae5cb703bc39dca5599b9d3443bb02387fc050f40b0d", merkleRootHash.String())
	})
}
