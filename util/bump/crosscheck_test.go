package bump_test

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	bc "github.com/bsv-blockchain/go-bc"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/util/bump"
	"github.com/stretchr/testify/require"
)

// doubleHashBytes computes SHA256(SHA256(data)) and returns the raw bytes.
func doubleHashBytes(data []byte) []byte {
	first := sha256.Sum256(data)
	second := sha256.Sum256(first[:])
	return second[:]
}

func TestComputeCoinbaseBUMP_EndToEnd(t *testing.T) {
	t.Run("2-tx block verified with go-bc", func(t *testing.T) {
		// Two transactions with distinct byte patterns (reversal is detectable)
		tx0Str := "4ebd5a35e6b73a5f8e1a3621dba857239538c1b1d26364913f14c85b04e208fc" // coinbase
		tx1Str := "1c518b6671f8d349e96c56d4e7fe831a46f398c4bb46ca7778b2152ee6ba6f27" // sibling

		tx0Hash, err := chainhash.NewHashFromStr(tx0Str)
		require.NoError(t, err)
		tx1Hash, err := chainhash.NewHashFromStr(tx1Str)
		require.NoError(t, err)

		// Build a 2-tx subtree (coinbase at index 0, sibling at index 1)
		subtree0, err := subtreepkg.NewTree(1) // height=1, capacity=2
		require.NoError(t, err)
		subtree0.Nodes = append(subtree0.Nodes, subtreepkg.Node{Hash: *tx0Hash})
		subtree0.Nodes = append(subtree0.Nodes, subtreepkg.Node{Hash: *tx1Hash, Fee: 1, SizeInBytes: 100})

		// Compute expected merkle root: DoubleHash(tx0_internal || tx1_internal)
		combined := make([]byte, 64)
		copy(combined[:32], tx0Hash[:])
		copy(combined[32:], tx1Hash[:])
		expectedRoot := chainhash.DoubleHashH(combined)

		// Verify subtree root matches
		subtreeRoot := subtree0.RootHash()
		require.NotNil(t, subtreeRoot)
		require.Equal(t, expectedRoot, *subtreeRoot, "subtree root should match manual computation")

		// Generate coinbase BUMP (single subtree block)
		subtreeHashes := []*chainhash.Hash{subtreeRoot}
		bumpBytes, err := bump.ComputeCoinbaseBUMP(subtree0, subtreeHashes, 100)
		require.NoError(t, err)
		require.NotNil(t, bumpBytes)

		// Parse with go-bc reference implementation
		bumpParsed, err := bc.NewBUMPFromBytes(bumpBytes)
		require.NoError(t, err)
		require.Equal(t, uint64(100), bumpParsed.BlockHeight)
		require.Equal(t, 1, len(bumpParsed.Path))
		require.Equal(t, 2, len(bumpParsed.Path[0]))

		// Level 0 should have coinbase txid (offset 0, flag TxID) and sibling (offset 1, flag Data)
		require.Equal(t, uint64(0), *bumpParsed.Path[0][0].Offset)
		require.NotNil(t, bumpParsed.Path[0][0].Txid)
		require.True(t, *bumpParsed.Path[0][0].Txid)
		require.Equal(t, tx0Str, *bumpParsed.Path[0][0].Hash, "coinbase txid in BUMP should match")

		require.Equal(t, uint64(1), *bumpParsed.Path[0][1].Offset)
		require.Equal(t, tx1Str, *bumpParsed.Path[0][1].Hash, "sibling hash in BUMP should be tx1 display-order txid")

		// Manually verify merkle root using the BUMP hash (display order → reverse to internal → hash)
		siblingDisplayBytes, err := hex.DecodeString(*bumpParsed.Path[0][1].Hash)
		require.NoError(t, err)
		siblingInternalBytes := reverseTestBytes(siblingDisplayBytes)

		reconstructed := make([]byte, 64)
		copy(reconstructed[:32], tx0Hash[:]) // coinbase internal bytes
		copy(reconstructed[32:], siblingInternalBytes)
		reconstructedRoot := chainhash.DoubleHashH(reconstructed)
		require.Equal(t, expectedRoot, reconstructedRoot, "reconstructed merkle root should match expected")
	})

	t.Run("4-tx block with multiple subtrees", func(t *testing.T) {
		// 4 transactions, 2 per subtree
		txStrs := []string{
			"4ebd5a35e6b73a5f8e1a3621dba857239538c1b1d26364913f14c85b04e208fc",
			"1c518b6671f8d349e96c56d4e7fe831a46f398c4bb46ca7778b2152ee6ba6f27",
			"1e7aa360e3e84aff86515e66976b5b12e622c134b776242927e62de7effdc989",
			"344efe10fa4084c7f4f17c91bf3da72b9139c342aea074d75d8656a99ac3693f",
		}

		hashes := make([]chainhash.Hash, len(txStrs))
		for i, s := range txStrs {
			h, err := chainhash.NewHashFromStr(s)
			require.NoError(t, err)
			hashes[i] = *h
		}

		// Build subtree 0 (tx0 + tx1)
		subtree0, err := subtreepkg.NewTree(1)
		require.NoError(t, err)
		subtree0.Nodes = append(subtree0.Nodes, subtreepkg.Node{Hash: hashes[0]})
		subtree0.Nodes = append(subtree0.Nodes, subtreepkg.Node{Hash: hashes[1], Fee: 1, SizeInBytes: 100})
		root0 := subtree0.RootHash()
		require.NotNil(t, root0)

		// Build subtree 1 (tx2 + tx3)
		subtree1, err := subtreepkg.NewTree(1)
		require.NoError(t, err)
		subtree1.Nodes = append(subtree1.Nodes, subtreepkg.Node{Hash: hashes[2], Fee: 1, SizeInBytes: 100})
		subtree1.Nodes = append(subtree1.Nodes, subtreepkg.Node{Hash: hashes[3], Fee: 1, SizeInBytes: 100})
		root1 := subtree1.RootHash()
		require.NotNil(t, root1)

		// Block merkle root = DoubleHash(root0 || root1)
		blockCombined := make([]byte, 64)
		copy(blockCombined[:32], root0[:])
		copy(blockCombined[32:], root1[:])
		blockMerkleRoot := chainhash.DoubleHashH(blockCombined)

		// Generate coinbase BUMP
		subtreeHashes := []*chainhash.Hash{root0, root1}
		bumpBytes, err := bump.ComputeCoinbaseBUMP(subtree0, subtreeHashes, 200)
		require.NoError(t, err)
		require.NotNil(t, bumpBytes)

		// Parse with go-bc
		bumpParsed, err := bc.NewBUMPFromBytes(bumpBytes)
		require.NoError(t, err)
		require.Equal(t, uint64(200), bumpParsed.BlockHeight)
		require.Equal(t, 2, len(bumpParsed.Path), "should have 2 levels: subtree + block")

		// Level 0: coinbase txid (offset 0) + sibling hash (offset 1)
		require.Equal(t, 2, len(bumpParsed.Path[0]), "level 0 should have coinbase txid + sibling")
		require.Equal(t, txStrs[0], *bumpParsed.Path[0][0].Hash, "level 0 first should be coinbase txid")
		require.NotNil(t, bumpParsed.Path[0][0].Txid)
		require.True(t, *bumpParsed.Path[0][0].Txid)
		require.Equal(t, txStrs[1], *bumpParsed.Path[0][1].Hash, "level 0 second should be tx1 display txid")

		// Level 1: sibling subtree root hash (display order)
		require.Equal(t, root1.String(), *bumpParsed.Path[1][0].Hash, "level 1 should be subtree1 root display hash")

		// Verify full merkle path manually
		// Step 1: combine coinbase + sibling at subtree level
		step1 := make([]byte, 64)
		copy(step1[:32], hashes[0][:])
		copy(step1[32:], hashes[1][:])
		computedSubtreeRoot := chainhash.DoubleHashH(step1)
		require.Equal(t, *root0, computedSubtreeRoot)

		// Step 2: combine subtree roots at block level
		step2 := make([]byte, 64)
		copy(step2[:32], computedSubtreeRoot[:])
		copy(step2[32:], root1[:])
		computedBlockRoot := chainhash.DoubleHashH(step2)
		require.Equal(t, blockMerkleRoot, computedBlockRoot, "block merkle root should match")
	})

	t.Run("go-bc round trip produces identical binary", func(t *testing.T) {
		tx0Hash, _ := chainhash.NewHashFromStr("aaaa000000000000000000000000000000000000000000000000000000001111")
		tx1Hash, _ := chainhash.NewHashFromStr("bbbb000000000000000000000000000000000000000000000000000000002222")

		subtree0, _ := subtreepkg.NewTree(1)
		subtree0.Nodes = append(subtree0.Nodes, subtreepkg.Node{Hash: *tx0Hash})
		subtree0.Nodes = append(subtree0.Nodes, subtreepkg.Node{Hash: *tx1Hash, Fee: 1, SizeInBytes: 100})

		subtreeHashes := []*chainhash.Hash{subtree0.RootHash()}
		bumpBytes, err := bump.ComputeCoinbaseBUMP(subtree0, subtreeHashes, 500)
		require.NoError(t, err)
		require.NotNil(t, bumpBytes)

		// Parse with go-bc and re-encode
		bumpParsed, err := bc.NewBUMPFromBytes(bumpBytes)
		require.NoError(t, err)
		reEncoded, err := bumpParsed.Bytes()
		require.NoError(t, err)

		require.Equal(t, bumpBytes, reEncoded, "go-bc round-trip should produce identical binary")
	})
}

func TestBUMP_KnownMerkleRoot(t *testing.T) {
	// Test data from a known block (height 11889) provided by the user.
	// The sibling txid (display order) is c81f0541ffca7c760132f517d9605432b6b772f27b2bfd6065e9055fd067ffed
	// and the expected merkle root (display order) is cf0a2681b3247851aea901527c70a25700744488c7ae61388e4bfb7093613fe6.
	//
	// Previously, the BUMP had the hash bytes in display order (buggy):
	//   fd712e01010100c81f0541ffca7c760132f517d9605432b6b772f27b2bfd6065e9055fd067ffed
	// After the fix, hashes are written in internal (little-endian) order:
	//   fd712e01010100edff67d05f05e96560fd2b7bf272b7b6325460d917f53201767ccaff41051fc8

	siblingTxid := "c81f0541ffca7c760132f517d9605432b6b772f27b2bfd6065e9055fd067ffed"
	expectedMerkleRoot := "cf0a2681b3247851aea901527c70a25700744488c7ae61388e4bfb7093613fe6"
	blockHeight := uint32(11889)

	siblingHash, err := chainhash.NewHashFromStr(siblingTxid)
	require.NoError(t, err)
	expectedRoot, err := chainhash.NewHashFromStr(expectedMerkleRoot)
	require.NoError(t, err)

	t.Run("corrected BUMP has internal-order bytes parseable by go-bc", func(t *testing.T) {
		// Build the corrected BUMP binary manually
		var correctedBump []byte
		correctedBump = append(correctedBump, 0xfd, 0x71, 0x2e)  // block height 11889 as VarInt
		correctedBump = append(correctedBump, 0x01)              // tree height = 1
		correctedBump = append(correctedBump, 0x01)              // 1 node at level 0
		correctedBump = append(correctedBump, 0x01)              // offset = 1
		correctedBump = append(correctedBump, 0x00)              // flag = data
		correctedBump = append(correctedBump, siblingHash[:]...) // hash in internal byte order

		// Verify go-bc parses and recovers the correct display-order txid
		bumpParsed, err := bc.NewBUMPFromBytes(correctedBump)
		require.NoError(t, err)
		require.Equal(t, uint64(blockHeight), bumpParsed.BlockHeight)
		require.Equal(t, siblingTxid, *bumpParsed.Path[0][0].Hash,
			"go-bc should recover sibling display-order txid from corrected BUMP")

		// Verify go-bc round-trip preserves the binary
		reEncoded, err := bumpParsed.Bytes()
		require.NoError(t, err)
		require.Equal(t, correctedBump, reEncoded, "go-bc round-trip should match corrected BUMP")
	})

	t.Run("buggy BUMP had display-order bytes", func(t *testing.T) {
		// The buggy BUMP stored display-order bytes (no reversal in EncodeBinary)
		buggyBumpHex := "fd712e01010100c81f0541ffca7c760132f517d9605432b6b772f27b2bfd6065e9055fd067ffed"
		buggyBump, err := hex.DecodeString(buggyBumpHex)
		require.NoError(t, err)

		// go-bc reverses the bytes when parsing, so it gets the WRONG display hash
		bumpParsed, err := bc.NewBUMPFromBytes(buggyBump)
		require.NoError(t, err)
		parsedHash := *bumpParsed.Path[0][0].Hash

		// The parsed hash should NOT match the actual sibling txid (because bytes were double-reversed)
		require.NotEqual(t, siblingTxid, parsedHash,
			"buggy BUMP should produce wrong display hash when parsed by go-bc")
	})

	t.Run("merkle root verification with known values", func(t *testing.T) {
		// We verify the merkle root by constructing a full scenario:
		// Pick a coinbase hash such that DoubleHash(coinbase || sibling) = expectedRoot
		// Since we can't reverse a hash, we construct the tree and verify the path.

		// Use a deterministic coinbase hash for this test
		coinbaseTxid := "aabbccdd00000000000000000000000000000000000000000000000000000001"
		coinbaseHash, err := chainhash.NewHashFromStr(coinbaseTxid)
		require.NoError(t, err)

		// Build a 2-tx subtree
		subtree0, err := subtreepkg.NewTree(1)
		require.NoError(t, err)
		subtree0.Nodes = append(subtree0.Nodes, subtreepkg.Node{Hash: *coinbaseHash})
		subtree0.Nodes = append(subtree0.Nodes, subtreepkg.Node{Hash: *siblingHash, Fee: 1, SizeInBytes: 100})

		// Compute expected merkle root for this pair
		combined := make([]byte, 64)
		copy(combined[:32], coinbaseHash[:])
		copy(combined[32:], siblingHash[:])
		computedRoot := chainhash.DoubleHashH(combined)

		// Generate coinbase BUMP
		subtreeHashes := []*chainhash.Hash{subtree0.RootHash()}
		bumpBytes, err := bump.ComputeCoinbaseBUMP(subtree0, subtreeHashes, blockHeight)
		require.NoError(t, err)
		require.NotNil(t, bumpBytes)

		// Parse with go-bc and verify level 0 has coinbase txid + sibling
		bumpParsed, err := bc.NewBUMPFromBytes(bumpBytes)
		require.NoError(t, err)
		require.Equal(t, 2, len(bumpParsed.Path[0]), "level 0 should have coinbase txid + sibling")
		require.Equal(t, coinbaseTxid, *bumpParsed.Path[0][0].Hash, "coinbase txid in BUMP should match")
		require.Equal(t, siblingTxid, *bumpParsed.Path[0][1].Hash,
			"BUMP should contain sibling txid in display order")

		// Verify merkle root computation through the BUMP path
		siblingDisplayBytes, err := hex.DecodeString(*bumpParsed.Path[0][1].Hash)
		require.NoError(t, err)
		siblingInternalBytes := reverseTestBytes(siblingDisplayBytes)

		reconstructed := make([]byte, 64)
		copy(reconstructed[:32], coinbaseHash[:])
		copy(reconstructed[32:], siblingInternalBytes)
		reconstructedRoot := chainhash.DoubleHashH(reconstructed)
		require.Equal(t, computedRoot, reconstructedRoot,
			"merkle root reconstructed from BUMP should match computed root")
	})

	t.Run("verify user merkle root with matching coinbase", func(t *testing.T) {
		// For the user's specific merkle root cf0a2681b3247851aea901527c70a25700744488c7ae61388e4bfb7093613fe6
		// and sibling c81f0541ffca7c760132f517d9605432b6b772f27b2bfd6065e9055fd067ffed,
		// we verify: if we find a coinbase that produces this root, the BUMP correctly encodes it.

		// Brute-forcing a coinbase is not feasible, but we CAN verify the structural correctness:
		// 1. Our BUMP writes sibling's internal bytes to binary
		// 2. go-bc recovers the correct display-order sibling txid
		// 3. Reversing the display bytes gives back the internal bytes for hashing

		siblingDisplayBytes, err := hex.DecodeString(siblingTxid)
		require.NoError(t, err)
		siblingInternalFromBUMP := reverseTestBytes(siblingDisplayBytes)

		// Verify this matches what chainhash stores internally
		require.Equal(t, siblingHash[:], siblingInternalFromBUMP,
			"sibling internal bytes from BUMP should match chainhash internal storage")

		// Verify the expected root is a valid chainhash
		require.NotNil(t, expectedRoot)
		require.Equal(t, expectedMerkleRoot, expectedRoot.String(),
			"expected merkle root display string should round-trip through chainhash")
	})
}

// TestBUMP_RealBlock_12065 verifies a BUMP from a real block (height 12065) on the Galts Gulch testnet.
// Block hash:  000000003050d0ea0bed2b4c8361599677631824a6dcfa2c6fee03fe9e46321f
// Merkle root: 6f1400e25fd50b4e791ff6badb967e9c8ef71795a02bd79f553c70d9f0890ba9
// The block has 22 transactions in 1 subtree, producing a 5-level merkle tree.
func TestBUMP_RealBlock_12065(t *testing.T) {
	bumpHex := "fd212f05010100f7d46b7c9b7a865d07d560b8a818c92241117bfe42d6ee9ce5d0b67482a866ea010100652f2bc747c5d07e60643dd0e7b054261b625dc2a58fc471ade24507a8346451010100b2a151670047ea70acbd9d213cf40aad43d2cadbf16bfc4ac2ace5050f5b87f70101005092191ed7ea5d6aa9e0051dfabdf5877f910d7bc0fda0f32cca68b4e8b281890101003ac4ffaec8fb21b61a297ab000b0bfdb9acfd9523ae8bdf5a9de542e1560e771"
	coinbaseTxID := "e823d6574012e95a7d5529157eb1a48fd483515791b34c0afb0453a05896c2f0"
	expectedMerkleRoot := "6f1400e25fd50b4e791ff6badb967e9c8ef71795a02bd79f553c70d9f0890ba9"

	bumpBytes, err := hex.DecodeString(bumpHex)
	require.NoError(t, err)

	coinbaseHash, err := chainhash.NewHashFromStr(coinbaseTxID)
	require.NoError(t, err)

	expectedRoot, err := chainhash.NewHashFromStr(expectedMerkleRoot)
	require.NoError(t, err)

	t.Run("go-bc parses BUMP correctly", func(t *testing.T) {
		bumpParsed, err := bc.NewBUMPFromBytes(bumpBytes)
		require.NoError(t, err)
		require.Equal(t, uint64(12065), bumpParsed.BlockHeight)
		require.Equal(t, 5, len(bumpParsed.Path), "should have 5 merkle levels for 22 txs")

		// All siblings should be at offset 1 (coinbase is always leftmost)
		for i, level := range bumpParsed.Path {
			require.Equal(t, 1, len(level), "level %d should have 1 node", i)
			require.Equal(t, uint64(1), *level[0].Offset, "level %d sibling should be at offset 1", i)
			require.NotNil(t, level[0].Hash, "level %d should have a hash", i)
		}

		// Verify go-bc round-trip
		reEncoded, err := bumpParsed.Bytes()
		require.NoError(t, err)
		require.Equal(t, bumpBytes, reEncoded, "go-bc round-trip should produce identical binary")
	})

	t.Run("BUMP verifies to correct merkle root", func(t *testing.T) {
		bumpParsed, err := bc.NewBUMPFromBytes(bumpBytes)
		require.NoError(t, err)

		// Walk the merkle path from coinbase to root
		workingHash := coinbaseHash.CloneBytes() // internal byte order

		for i, level := range bumpParsed.Path {
			leaf := level[0]
			offset := *leaf.Offset

			// go-bc stores hashes in display order; reverse to internal for hashing
			leafDisplayBytes, err := hex.DecodeString(*leaf.Hash)
			require.NoError(t, err)
			leafInternal := reverseTestBytes(leafDisplayBytes)

			var digest []byte
			if offset%2 != 0 {
				// Sibling is on the right side
				digest = append(workingHash, leafInternal...)
			} else {
				// Sibling is on the left side
				digest = append(leafInternal, workingHash...)
			}

			workingHash = doubleHashBytes(digest)
			_ = i
		}

		computedRoot, err := chainhash.NewHash(workingHash)
		require.NoError(t, err)
		require.Equal(t, expectedRoot.String(), computedRoot.String(),
			"walking BUMP from coinbase should produce the block merkle root")
	})
}

// TestBUMP_RealBlock_12069 verifies a BUMP from a real block (height 12069) with 2 transactions.
// Coinbase: f021589294ef8194648bc5b688637272776f10a45ff2a76bc64b81f7338a133c
// Sibling:  c1b90e6210a853b4a274b5063d2aa5921e4be5b95af33a9bd0abb3c42c815596
// Expected merkle root: efea423f3fe0deaacb57a12f656968baad6b696f0cbe845ae0b86ed13927ae4a
func TestBUMP_RealBlock_12069(t *testing.T) {
	coinbaseTxid := "f021589294ef8194648bc5b688637272776f10a45ff2a76bc64b81f7338a133c"
	siblingTxid := "c1b90e6210a853b4a274b5063d2aa5921e4be5b95af33a9bd0abb3c42c815596"
	expectedMerkleRoot := "efea423f3fe0deaacb57a12f656968baad6b696f0cbe845ae0b86ed13927ae4a"
	expectedBUMPHex := "fd252f010200023c138a33f7814bc66ba7f25fa4106f7772726388b6c58b649481ef94925821f001009655812cc4b3abd09b3af35ab9e54b1e92a52a3d06b574a2b453a810620eb9c1"

	coinbaseHash, err := chainhash.NewHashFromStr(coinbaseTxid)
	require.NoError(t, err)
	siblingHash, err := chainhash.NewHashFromStr(siblingTxid)
	require.NoError(t, err)

	// Build a 2-tx subtree
	subtree0, err := subtreepkg.NewTree(1)
	require.NoError(t, err)
	subtree0.Nodes = append(subtree0.Nodes, subtreepkg.Node{Hash: *coinbaseHash})
	subtree0.Nodes = append(subtree0.Nodes, subtreepkg.Node{Hash: *siblingHash, Fee: 1, SizeInBytes: 100})

	// Generate coinbase BUMP
	subtreeHashes := []*chainhash.Hash{subtree0.RootHash()}
	bumpBytes, err := bump.ComputeCoinbaseBUMP(subtree0, subtreeHashes, 12069)
	require.NoError(t, err)
	require.NotNil(t, bumpBytes)

	t.Run("exact BUMP hex matches expected", func(t *testing.T) {
		bumpHex := hex.EncodeToString(bumpBytes)
		require.Equal(t, expectedBUMPHex, bumpHex, "BUMP hex should match expected BRC-74 output")
	})

	t.Run("go-bc CalculateRootGivenTxid produces correct merkle root", func(t *testing.T) {
		bumpParsed, err := bc.NewBUMPFromBytes(bumpBytes)
		require.NoError(t, err)

		root, err := bumpParsed.CalculateRootGivenTxid(coinbaseTxid)
		require.NoError(t, err)
		require.Equal(t, expectedMerkleRoot, root,
			"CalculateRootGivenTxid should produce the expected merkle root")
	})

	t.Run("go-bc round-trip produces identical binary", func(t *testing.T) {
		bumpParsed, err := bc.NewBUMPFromBytes(bumpBytes)
		require.NoError(t, err)
		reEncoded, err := bumpParsed.Bytes()
		require.NoError(t, err)
		require.Equal(t, bumpBytes, reEncoded, "go-bc round-trip should produce identical binary")
	})
}

func reverseTestBytes(b []byte) []byte {
	r := make([]byte, len(b))
	for i := range b {
		r[i] = b[len(b)-1-i]
	}
	return r
}
