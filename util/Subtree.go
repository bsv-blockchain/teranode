package util

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"math"

	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/libsv/go-p2p/wire"
)

type Subtree struct {
	Height           int
	Fees             uint64
	Nodes            []*chainhash.Hash
	ConflictingNodes []*chainhash.Hash // conflicting nodes need to be checked when doing block assembly

	// temporary (calculated) variables
	rootHash *chainhash.Hash
	treeSize int
}

// NewTree creates a new Subtree with a fixed height
func NewTree(height int) *Subtree {
	var treeSize = int(math.Pow(2, float64(height))) // 1024 * 1024
	return &Subtree{
		Nodes:    make([]*chainhash.Hash, 0, treeSize),
		Height:   height,
		treeSize: treeSize,
	}
}

func NewTreeByLeafCount(maxNumberOfLeaves int) *Subtree {
	if !IsPowerOfTwo(maxNumberOfLeaves) {
		panic("numberOfLeaves must be a power of two")
	}

	height := math.Ceil(math.Log2(float64(maxNumberOfLeaves)))
	return NewTree(int(height))
}
func NewIncompleteTreeByLeafCount(maxNumberOfLeaves int) *Subtree {
	height := math.Ceil(math.Log2(float64(maxNumberOfLeaves)))
	return NewTree(int(height))
}

func (st *Subtree) Size() int {
	return cap(st.Nodes)
}

func (st *Subtree) Length() int {
	return len(st.Nodes)
}

func (st *Subtree) IsComplete() bool {
	return len(st.Nodes) == cap(st.Nodes)
}

func (st *Subtree) ReplaceRootNode(node *chainhash.Hash) *chainhash.Hash {
	if len(st.Nodes) < 1 {
		st.Nodes = append(st.Nodes, node)
	} else {
		st.Nodes[0] = node
	}

	st.rootHash = nil // reset rootHash

	return st.RootHash()
}

func (st *Subtree) AddNode(node *chainhash.Hash, fee uint64) error {
	if (len(st.Nodes) + 1) > st.treeSize {
		return fmt.Errorf("subtree is full")
	}

	st.Nodes = append(st.Nodes, node)
	st.rootHash = nil // reset rootHash
	st.Fees += fee

	return nil
}

func (st *Subtree) AddNodes(nodes []*chainhash.Hash) error {
	if (len(st.Nodes) + len(nodes)) > st.treeSize {
		return fmt.Errorf("subtree is full")
	}

	st.Nodes = append(st.Nodes, nodes...)
	st.rootHash = nil // reset rootHash

	return nil
}

func (st *Subtree) RootHash() *chainhash.Hash {
	if st.rootHash != nil {
		return st.rootHash
	}

	// calculate rootHash
	store, err := st.BuildMerkleTreeStoreFromBytes()
	if err != nil {
		return nil
	}

	st.rootHash = store[len(store)-1]

	return st.rootHash
}

func (st *Subtree) Difference(ids txMap) ([]*chainhash.Hash, error) {
	// return all the ids that are in st.Nodes, but not in ids
	diff := make([]*chainhash.Hash, 0, 1_000)
	for _, id := range st.Nodes {
		if !ids.Exists(*id) {
			diff = append(diff, id)
		}
	}

	//fmt.Printf("diff: %d\n", len(diff))
	//for i := 0; i < len(diff); i++ {
	//	hash, _ := chainhash.NewHash(diff[i][:])
	//	fmt.Printf("%s\n", hash.String())
	//	if i > 10 {
	//		break
	//	}
	//}

	return diff, nil
}

func (st *Subtree) GetMerkleProof(index int) ([]*chainhash.Hash, error) {
	if index >= len(st.Nodes) {
		return nil, fmt.Errorf("index out of range")
	}

	merkleTree, err := st.BuildMerkleTreeStoreFromBytes()
	if err != nil {
		return nil, err
	}

	height := math.Ceil(math.Log2(float64(len(st.Nodes))))
	totalLength := int(math.Pow(2, height)) + len(merkleTree)

	treeIndexPos := 0
	treeIndex := index
	nodes := make([]*chainhash.Hash, 0, int(height))
	for i := height; i > 0; i-- {
		if i == height {
			// we are at the leaf level and read from the Nodes array
			if index%2 == 0 {
				nodes = append(nodes, st.Nodes[index+1])
			} else {
				nodes = append(nodes, st.Nodes[index-1])
			}
		} else {
			treePos := treeIndexPos + treeIndex
			if treePos%2 == 0 {
				if totalLength > treePos+1 && merkleTree[treePos+1] != nil {
					treePos++
				}
			} else {
				if merkleTree[treePos-1] != nil {
					treePos--
				}
			}

			nodes = append(nodes, merkleTree[treePos])
			treeIndexPos += int(math.Pow(2, i))
		}

		treeIndex = int(math.Floor(float64(treeIndex) / 2))
	}

	return nodes, nil
}

func (st *Subtree) BuildMerkleTreeStoreFromBytes() ([]*chainhash.Hash, error) {
	// Calculate how many entries are re?n array of that size.
	nextPoT := NextPowerOfTwo(len(st.Nodes))
	arraySize := nextPoT*2 - 1
	// we do not include the original nodes in the merkle tree
	merkles := make([]*chainhash.Hash, nextPoT-1)

	if arraySize == 1 {
		// Handle this Bitcoin exception that the merkle root is the same as the transaction hash if there
		// is only one transaction.
		return st.Nodes, nil
	}

	// Start the array offset after the last transaction and adjusted to the
	// next power of two.
	offset := 0
	var hash [32]byte
	var currentMerkle *chainhash.Hash
	var currentMerkle1 *chainhash.Hash
	for i := 0; i < arraySize-1; i += 2 {
		if i < nextPoT {
			if i >= len(st.Nodes) {
				currentMerkle = nil
			} else {
				currentMerkle = st.Nodes[i]
			}

			if i+1 >= len(st.Nodes) {
				currentMerkle1 = nil
			} else {
				currentMerkle1 = st.Nodes[i+1]
			}
		} else {
			currentMerkle = merkles[i-nextPoT]
			currentMerkle1 = merkles[i-nextPoT+1]
		}

		switch {
		// When there is no left child node, the parent is nil ("") too.
		case currentMerkle == nil:
			merkles[offset] = nil

		// When there is no right child, the parent is generated by
		// hashing the concatenation of the left child with itself.
		case currentMerkle1 == nil:
			hash = sha256.Sum256(append(currentMerkle[:], currentMerkle[:]...))
			hash = sha256.Sum256(hash[:])
			merkles[offset], _ = chainhash.NewHash(hash[:])

		// The normal case sets the parent node to the double sha256
		// of the concatenation of the left and right children.
		default:
			hash = sha256.Sum256(append(currentMerkle[:], currentMerkle1[:]...))
			hash = sha256.Sum256(hash[:])
			merkles[offset], _ = chainhash.NewHash(hash[:])
		}
		offset++
	}

	return merkles, nil
}

func (st *Subtree) Serialize() ([]byte, error) {
	buf := bytes.NewBuffer([]byte{})

	// write root hash - this is only for checking the correctness of the data
	_, err := buf.Write(st.RootHash()[:])
	if err != nil {
		return nil, fmt.Errorf("unable to write root hash: %v", err)
	}

	// write fees
	if err = wire.WriteVarInt(buf, 0, st.Fees); err != nil {
		return nil, fmt.Errorf("unable to write fees: %v", err)
	}

	// write number of nodes
	if err = wire.WriteVarInt(buf, 0, uint64(len(st.Nodes))); err != nil {
		return nil, fmt.Errorf("unable to write number of nodes: %v", err)
	}

	// write nodes
	for _, node := range st.Nodes {
		_, err = buf.Write(node[:])
		if err != nil {
			return nil, fmt.Errorf("unable to write node: %v", err)
		}
	}

	// write number of conflicting nodes
	if err = wire.WriteVarInt(buf, 0, uint64(len(st.ConflictingNodes))); err != nil {
		return nil, fmt.Errorf("unable to write number of conflicting nodes: %v", err)
	}

	// write conflicting nodes
	for _, node := range st.ConflictingNodes {
		_, err = buf.Write(node[:])
		if err != nil {
			return nil, fmt.Errorf("unable to write conflicting node: %v", err)
		}
	}

	return buf.Bytes(), nil
}

func (st *Subtree) Deserialize(b []byte) (err error) {
	buf := bytes.NewBuffer(b)

	// read root hash
	var rootHash [32]byte
	_, err = buf.Read(rootHash[:])
	if err != nil {
		return fmt.Errorf("unable to read root hash: %v", err)
	}

	// read fees
	st.Fees, err = wire.ReadVarInt(buf, 0)
	if err != nil {
		return fmt.Errorf("unable to read fees: %v", err)
	}

	// read number of leaves
	numLeaves, err := wire.ReadVarInt(buf, 0)
	if err != nil {
		return fmt.Errorf("unable to read number of leaves: %v", err)
	}

	if !IsPowerOfTwo(int(numLeaves)) {
		return fmt.Errorf("numberOfLeaves must be a power of two")
	}

	st.treeSize = int(numLeaves)
	st.Height = int(math.Ceil(math.Log2(float64(numLeaves))))

	// read leaves
	st.Nodes = make([]*chainhash.Hash, numLeaves)
	for i := uint64(0); i < numLeaves; i++ {
		st.Nodes[i], err = chainhash.NewHash(buf.Next(32))
		if err != nil {
			return fmt.Errorf("unable to read leaves: %v", err)
		}
	}

	// read number of conflicting nodes
	numConflictingLeaves, err := wire.ReadVarInt(buf, 0)
	if err != nil {
		return fmt.Errorf("unable to read number of conflicting nodes: %v", err)
	}

	// read conflicting nodes
	st.ConflictingNodes = make([]*chainhash.Hash, numConflictingLeaves)
	for i := uint64(0); i < numConflictingLeaves; i++ {
		st.ConflictingNodes[i], err = chainhash.NewHash(buf.Next(32))
		if err != nil {
			return fmt.Errorf("unable to read conflicting node: %v", err)
		}
	}

	// calculate rootHash and compare with given rootHash
	if !bytes.Equal(st.RootHash()[:], rootHash[:]) {
		return fmt.Errorf("root hash mismatch")
	}

	// TODO: look up the fee for the last tx and subtract it from the last chainedSubtree and add it to the currentSubtree
	return nil
}
