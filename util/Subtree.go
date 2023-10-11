package util

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/libsv/go-p2p/wire"
)

type SubtreeNode struct {
	Hash        chainhash.Hash `json:"hash"`
	Fee         uint64         `json:"fee"`
	SizeInBytes uint64         `json:"size"`
}

type Subtree struct {
	Height           int
	Fees             uint64
	SizeInBytes      uint64
	FeeHash          chainhash.Hash
	Nodes            []*SubtreeNode
	ConflictingNodes []*chainhash.Hash // conflicting nodes need to be checked when doing block assembly

	// temporary (calculated) variables
	rootHash     *chainhash.Hash
	treeSize     int
	feeBytes     []byte
	feeHashBytes []byte
}

// NewTree creates a new Subtree with a fixed height
func NewTree(height int) *Subtree {
	var treeSize = int(math.Pow(2, float64(height))) // 1024 * 1024
	return &Subtree{
		Nodes:        make([]*SubtreeNode, 0, treeSize),
		Height:       height,
		FeeHash:      chainhash.Hash{},
		treeSize:     treeSize,
		feeBytes:     make([]byte, 8),
		feeHashBytes: make([]byte, 40),
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

func NewSubtreeFromBytes(b []byte) (*Subtree, error) {
	subtree := &Subtree{}
	err := subtree.Deserialize(b)
	if err != nil {
		return nil, err
	}

	return subtree, nil
}

func (st *Subtree) Duplicate() *Subtree {
	newSubtree := &Subtree{
		Height:           st.Height,
		Fees:             st.Fees,
		SizeInBytes:      st.SizeInBytes,
		FeeHash:          st.FeeHash,
		Nodes:            make([]*SubtreeNode, len(st.Nodes)),
		ConflictingNodes: make([]*chainhash.Hash, len(st.ConflictingNodes)),
		rootHash:         st.rootHash,
		treeSize:         st.treeSize,
		feeBytes:         make([]byte, 8),
		feeHashBytes:     make([]byte, 40),
	}

	copy(newSubtree.Nodes, st.Nodes)
	copy(newSubtree.ConflictingNodes, st.ConflictingNodes)

	return newSubtree
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

func (st *Subtree) ReplaceRootNode(node *chainhash.Hash, fee uint64, sizeInBytes uint64) *chainhash.Hash {
	if len(st.Nodes) < 1 {
		st.Nodes = append(st.Nodes, &SubtreeNode{
			Hash:        *node,
			Fee:         fee,
			SizeInBytes: sizeInBytes,
		})
	} else {
		st.Nodes[0] = &SubtreeNode{
			Hash:        *node,
			Fee:         fee,
			SizeInBytes: sizeInBytes,
		}
	}

	st.rootHash = nil // reset rootHash
	st.SizeInBytes += sizeInBytes

	return st.RootHash()
}

func (st *Subtree) AddSubtreeNode(node *SubtreeNode) error {
	if (len(st.Nodes) + 1) > st.treeSize {
		return fmt.Errorf("subtree is full")
	}

	// AddNode is not concurrency safe, so we can reuse the same byte arrays
	//binary.LittleEndian.PutUint64(st.feeBytes, fee)
	//st.feeHashBytes = append(node[:], st.feeBytes[:]...)
	//if len(st.Nodes) == 0 {
	//	st.FeeHash = chainhash.HashH(st.feeHashBytes)
	//} else {
	//	st.FeeHash = chainhash.HashH(append(st.FeeHash[:], st.feeHashBytes...))
	//}

	st.Nodes = append(st.Nodes, node)
	st.rootHash = nil // reset rootHash
	st.Fees += node.Fee
	st.SizeInBytes += node.SizeInBytes

	return nil
}

func (st *Subtree) AddNode(node *chainhash.Hash, fee uint64, sizeInBytes uint64) error {
	if (len(st.Nodes) + 1) > st.treeSize {
		return fmt.Errorf("subtree is full")
	}

	// AddNode is not concurrency safe, so we can reuse the same byte arrays
	//binary.LittleEndian.PutUint64(st.feeBytes, fee)
	//st.feeHashBytes = append(node[:], st.feeBytes[:]...)
	//if len(st.Nodes) == 0 {
	//	st.FeeHash = chainhash.HashH(st.feeHashBytes)
	//} else {
	//	st.FeeHash = chainhash.HashH(append(st.FeeHash[:], st.feeHashBytes...))
	//}

	st.Nodes = append(st.Nodes, &SubtreeNode{
		Hash:        *node,
		Fee:         fee,
		SizeInBytes: sizeInBytes,
	})
	st.rootHash = nil // reset rootHash
	st.Fees += fee
	st.SizeInBytes += sizeInBytes

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

	st.rootHash, _ = chainhash.NewHash((*store)[len(*store)-1][:])

	return st.rootHash
}

func (st *Subtree) Difference(ids TxMap) ([]*SubtreeNode, error) {
	// return all the ids that are in st.Nodes, but not in ids
	diff := make([]*SubtreeNode, 0, 1_000)
	for _, node := range st.Nodes {
		if !ids.Exists(node.Hash) {
			diff = append(diff, node)
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

// GetMerkleProof returns the merkle proof for the given index
// TODO rewrite this to calculate this from the subtree nodes needed, and not the whole tree
func (st *Subtree) GetMerkleProof(index int) ([]*chainhash.Hash, error) {
	if index >= len(st.Nodes) {
		return nil, fmt.Errorf("index out of range")
	}

	merkleTree, err := st.BuildMerkleTreeStoreFromBytes()
	if err != nil {
		return nil, err
	}

	height := math.Ceil(math.Log2(float64(len(st.Nodes))))
	totalLength := int(math.Pow(2, height)) + len(*merkleTree)

	treeIndexPos := 0
	treeIndex := index
	nodes := make([]*chainhash.Hash, 0, int(height))
	for i := height; i > 0; i-- {
		if i == height {
			// we are at the leaf level and read from the Nodes array
			if index%2 == 0 {
				nodes = append(nodes, &st.Nodes[index+1].Hash)
			} else {
				nodes = append(nodes, &st.Nodes[index-1].Hash)
			}
		} else {
			treePos := treeIndexPos + treeIndex
			if treePos%2 == 0 {
				if totalLength > treePos+1 && !(*merkleTree)[treePos+1].Equal(chainhash.Hash{}) {
					treePos++
				}
			} else {
				if !(*merkleTree)[treePos-1].Equal(chainhash.Hash{}) {
					treePos--
				}
			}

			nodes = append(nodes, &(*merkleTree)[treePos])
			treeIndexPos += int(math.Pow(2, i))
		}

		treeIndex = int(math.Floor(float64(treeIndex) / 2))
	}

	return nodes, nil
}

func (st *Subtree) BuildMerkleTreeStoreFromBytes() (*[]chainhash.Hash, error) {
	if len(st.Nodes) == 0 {
		return &[]chainhash.Hash{}, nil
	}

	// Calculate how many entries are in an array of that size.
	nextPoT := NextPowerOfTwo(len(st.Nodes))
	arraySize := nextPoT*2 - 1
	// we do not include the original nodes in the merkle tree
	merkles := make([]chainhash.Hash, nextPoT-1)

	if arraySize == 1 {
		// Handle this Bitcoin exception that the merkle root is the same as the transaction hash if there
		// is only one transaction.
		return &[]chainhash.Hash{st.Nodes[0].Hash}, nil
	}

	// Start the array offset after the last transaction and adjusted to the
	// next power of two.
	offset := 0
	var hash [32]byte
	var currentMerkle chainhash.Hash
	var currentMerkle1 chainhash.Hash
	for i := 0; i < arraySize-1; i += 2 {
		if i < nextPoT {
			if i >= len(st.Nodes) {
				currentMerkle = chainhash.Hash{}
			} else {
				currentMerkle = st.Nodes[i].Hash
			}

			if i+1 >= len(st.Nodes) {
				currentMerkle1 = chainhash.Hash{}
			} else {
				currentMerkle1 = st.Nodes[i+1].Hash
			}
		} else {
			currentMerkle = merkles[i-nextPoT]
			currentMerkle1 = merkles[i-nextPoT+1]
		}

		switch {
		// When there is no left child node, the parent is nil ("") too.
		case currentMerkle.Equal(chainhash.Hash{}):
			merkles[offset] = chainhash.Hash{}

		// When there is no right child, the parent is generated by
		// hashing the concatenation of the left child with itself.
		case currentMerkle1.Equal(chainhash.Hash{}):
			hash = sha256.Sum256(append(currentMerkle[:], currentMerkle[:]...))
			hash = sha256.Sum256(hash[:])
			merkles[offset] = chainhash.Hash(hash[:])

		// The normal case sets the parent node to the double sha256
		// of the concatenation of the left and right children.
		default:
			hash = sha256.Sum256(append(currentMerkle[:], currentMerkle1[:]...))
			hash = sha256.Sum256(hash[:])
			merkles[offset] = chainhash.Hash(hash[:])
		}
		offset++
	}

	return &merkles, nil
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

	// write size
	if err = wire.WriteVarInt(buf, 0, st.SizeInBytes); err != nil {
		return nil, fmt.Errorf("unable to write sizeInBytes: %v", err)
	}

	// write number of nodes
	if err = wire.WriteVarInt(buf, 0, uint64(len(st.Nodes))); err != nil {
		return nil, fmt.Errorf("unable to write number of nodes: %v", err)
	}

	// write nodes
	feeBytes := make([]byte, 8)
	sizeBytes := make([]byte, 8)
	for _, node := range st.Nodes {
		_, err = buf.Write(node.Hash[:])
		if err != nil {
			return nil, fmt.Errorf("unable to write node: %v", err)
		}

		binary.LittleEndian.PutUint64(feeBytes, node.Fee)
		_, err = buf.Write(feeBytes)
		if err != nil {
			return nil, fmt.Errorf("unable to write fee: %v", err)
		}

		binary.LittleEndian.PutUint64(sizeBytes, node.SizeInBytes)
		_, err = buf.Write(sizeBytes)
		if err != nil {
			return nil, fmt.Errorf("unable to write sizeInBytes: %v", err)
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

// SerializeNodes serializes only the nodes (list of transaction ids), not the root hash, fees, etc.
func (st *Subtree) SerializeNodes() ([]byte, error) {
	buf := bytes.NewBuffer([]byte{})

	var err error

	// write nodes
	for _, node := range st.Nodes {
		_, err = buf.Write(node.Hash[:])
		if err != nil {
			return nil, fmt.Errorf("unable to write node: %v", err)
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

	// read sizeInBytes
	st.SizeInBytes, err = wire.ReadVarInt(buf, 0)
	if err != nil {
		return fmt.Errorf("unable to read sizeInBytes: %v", err)
	}

	// read number of leaves
	numLeaves, err := wire.ReadVarInt(buf, 0)
	if err != nil {
		return fmt.Errorf("unable to read number of leaves: %v", err)
	}

	// we must be able to support incomplete subtrees
	//if !IsPowerOfTwo(int(numLeaves)) {
	//	return fmt.Errorf("numberOfLeaves must be a power of two")
	//}

	st.treeSize = int(numLeaves)
	// the height of a subtree is always a power of two
	st.Height = int(math.Ceil(math.Log2(float64(numLeaves))))

	// read leaves
	st.Nodes = make([]*SubtreeNode, numLeaves)
	var hash *chainhash.Hash
	for i := uint64(0); i < numLeaves; i++ {
		hash, err = chainhash.NewHash(buf.Next(32))
		if err != nil {
			return fmt.Errorf("unable to read leaves: %v", err)
		}

		feeBytes := buf.Next(8)
		fee := binary.LittleEndian.Uint64(feeBytes)

		sizeBytes := buf.Next(8)
		sizeInBytes := binary.LittleEndian.Uint64(sizeBytes)

		st.Nodes[i] = &SubtreeNode{
			Hash:        *hash,
			Fee:         fee,
			SizeInBytes: sizeInBytes,
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
