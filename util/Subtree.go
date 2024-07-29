package util

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/bitcoin-sv/ubsv/errors"
	"io"
	"math"

	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
	"github.com/libsv/go-bt/v2/chainhash"
)

type SubtreeNode struct {
	Hash        chainhash.Hash `json:"txid"` // This is called txid so that the UI knows to add a link to /tx/<txid>
	Fee         uint64         `json:"fee"`
	SizeInBytes uint64         `json:"size"`
}

type Subtree struct {
	Height           int
	Fees             uint64
	SizeInBytes      uint64
	FeeHash          chainhash.Hash
	Nodes            []SubtreeNode
	ConflictingNodes []chainhash.Hash // conflicting nodes need to be checked when doing block assembly

	// temporary (calculated) variables
	rootHash     *chainhash.Hash
	treeSize     int
	feeBytes     []byte
	feeHashBytes []byte
}

// NewTree creates a new Subtree with a fixed height
//
// height is the number if levels in a merkle tree of the subtree
func NewTree(height int) (*Subtree, error) {
	if height < 0 {
		return nil, errors.NewProcessingError("height must be at least 0")
	}

	var treeSize = int(math.Pow(2, float64(height)))

	return &Subtree{
		Nodes:        make([]SubtreeNode, 0, treeSize),
		Height:       height,
		FeeHash:      chainhash.Hash{},
		treeSize:     treeSize,
		feeBytes:     make([]byte, 8),
		feeHashBytes: make([]byte, 40),
	}, nil
}

func NewTreeByLeafCount(maxNumberOfLeaves int) (*Subtree, error) {
	if !IsPowerOfTwo(maxNumberOfLeaves) {
		return nil, errors.NewProcessingError("numberOfLeaves must be a power of two")
	}

	height := math.Ceil(math.Log2(float64(maxNumberOfLeaves)))

	return NewTree(int(height))
}

func NewIncompleteTreeByLeafCount(maxNumberOfLeaves int) (*Subtree, error) {
	height := math.Ceil(math.Log2(float64(maxNumberOfLeaves)))

	return NewTree(int(height))
}

func NewSubtreeFromBytes(b []byte) (*Subtree, error) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("Recovered in NewSubtreeFromBytes: %v\n", r)
		}
	}()

	subtree := &Subtree{}
	err := subtree.Deserialize(b)
	if err != nil {
		return nil, err
	}

	return subtree, nil
}

func DeserializeNodesFromReader(reader io.Reader) (subtreeBytes []byte, err error) {
	buf := bufio.NewReaderSize(reader, 1024*1024*16) // 16MB buffer

	// root len(st.rootHash[:]) bytes
	// first 8 bytes, fees
	// second 8 bytes, sizeInBytes
	// third 8 bytes, number of leaves
	// total read at once = len(st.rootHash[:]) + 8 + 8 + 8
	byteBuffer := make([]byte, chainhash.HashSize+24)
	if _, err = io.ReadFull(buf, byteBuffer); err != nil {
		return nil, errors.NewSubtreeError("unable to read subtree root information : %v", err)
	}

	numLeaves := binary.LittleEndian.Uint64(byteBuffer[chainhash.HashSize+16 : chainhash.HashSize+24])
	subtreeBytes = make([]byte, chainhash.HashSize*int(numLeaves))
	byteBuffer = byteBuffer[8:] // reduce read byteBuffer size by 8
	for i := uint64(0); i < numLeaves; i++ {
		if _, err = io.ReadFull(buf, byteBuffer); err != nil {
			return nil, errors.NewSubtreeError("unable to read subtree node information : %v", err)
		}
		copy(subtreeBytes[i*chainhash.HashSize:(i+1)*chainhash.HashSize], byteBuffer[:chainhash.HashSize])
	}

	return subtreeBytes, nil
}

func (st *Subtree) Duplicate() *Subtree {
	newSubtree := &Subtree{
		Height:           st.Height,
		Fees:             st.Fees,
		SizeInBytes:      st.SizeInBytes,
		FeeHash:          st.FeeHash,
		Nodes:            make([]SubtreeNode, len(st.Nodes)),
		ConflictingNodes: make([]chainhash.Hash, len(st.ConflictingNodes)),
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
		st.Nodes = append(st.Nodes, SubtreeNode{
			Hash:        *node,
			Fee:         fee,
			SizeInBytes: sizeInBytes,
		})
	} else {
		st.Nodes[0] = SubtreeNode{
			Hash:        *node,
			Fee:         fee,
			SizeInBytes: sizeInBytes,
		}
	}

	st.rootHash = nil // reset rootHash
	st.SizeInBytes += sizeInBytes

	return st.RootHash()
}

func (st *Subtree) AddSubtreeNode(node SubtreeNode) error {
	if (len(st.Nodes) + 1) > st.treeSize {
		return errors.NewSubtreeError("subtree is full")
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

func (st *Subtree) AddNode(node chainhash.Hash, fee uint64, sizeInBytes uint64) error {
	if (len(st.Nodes) + 1) > st.treeSize {
		return errors.NewSubtreeError("subtree is full")
	}

	// AddNode is not concurrency safe, so we can reuse the same byte arrays
	//binary.LittleEndian.PutUint64(st.feeBytes, fee)
	//st.feeHashBytes = append(node[:], st.feeBytes[:]...)
	//if len(st.Nodes) == 0 {
	//	st.FeeHash = chainhash.HashH(st.feeHashBytes)
	//} else {
	//	st.FeeHash = chainhash.HashH(append(st.FeeHash[:], st.feeHashBytes...))
	//}

	st.Nodes = append(st.Nodes, SubtreeNode{
		Hash:        node,
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

	if st.Length() == 0 {
		return nil
	}

	// calculate rootHash
	store, err := BuildMerkleTreeStoreFromBytes(st.Nodes)
	if err != nil {
		return nil
	}

	st.rootHash, _ = chainhash.NewHash((*store)[len(*store)-1][:])

	return st.rootHash
}

func (st *Subtree) RootHashWithReplaceRootNode(node *chainhash.Hash, fee uint64, sizeInBytes uint64) (*chainhash.Hash, error) {
	// clone the subtree, so we do not overwrite anything in it
	subtreeClone := st.Duplicate()
	subtreeClone.ReplaceRootNode(node, fee, sizeInBytes)

	// calculate rootHash
	store, err := BuildMerkleTreeStoreFromBytes(subtreeClone.Nodes)
	if err != nil {
		return nil, err
	}

	rootHash := chainhash.Hash((*store)[len(*store)-1][:])
	return &rootHash, nil
}

func (st *Subtree) GetMap() TxMap {
	m := NewSwissMapUint64(len(st.Nodes))
	for idx, node := range st.Nodes {
		_ = m.Put(node.Hash, uint64(idx))
	}

	return m
}

func (st *Subtree) Difference(ids TxMap) ([]SubtreeNode, error) {
	// return all the ids that are in st.Nodes, but not in ids
	diff := make([]SubtreeNode, 0, 1_000)
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
		return nil, errors.NewSubtreeError("index out of range")
	}

	merkleTree, err := BuildMerkleTreeStoreFromBytes(st.Nodes)
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

func (st *Subtree) Serialize() ([]byte, error) {
	bufBytes := make([]byte, 0, 32+8+8+8+(len(st.Nodes)*32)+8+(len(st.ConflictingNodes)*32))
	buf := bytes.NewBuffer(bufBytes)

	// write root hash - this is only for checking the correctness of the data
	_, err := buf.Write(st.RootHash()[:])
	if err != nil {
		return nil, errors.NewSubtreeError("unable to write root hash", err)
	}

	var b [8]byte

	// write fees
	binary.LittleEndian.PutUint64(b[:], st.Fees)
	if _, err = buf.Write(b[:]); err != nil {
		return nil, errors.NewProcessingError("unable to write fees", err)
	}

	// write size
	binary.LittleEndian.PutUint64(b[:], st.SizeInBytes)
	if _, err = buf.Write(b[:]); err != nil {
		return nil, errors.NewProcessingError("unable to write sizeInBytes", err)
	}

	// write number of nodes
	binary.LittleEndian.PutUint64(b[:], uint64(len(st.Nodes)))
	if _, err = buf.Write(b[:]); err != nil {
		return nil, errors.NewProcessingError("unable to write number of nodes", err)
	}

	// write nodes
	feeBytes := make([]byte, 8)
	sizeBytes := make([]byte, 8)
	var node SubtreeNode
	for _, node = range st.Nodes {
		_, err = buf.Write(node.Hash[:])
		if err != nil {
			return nil, errors.NewProcessingError("unable to write node", err)
		}

		binary.LittleEndian.PutUint64(feeBytes, node.Fee)
		_, err = buf.Write(feeBytes)
		if err != nil {
			return nil, errors.NewProcessingError("unable to write fee", err)
		}

		binary.LittleEndian.PutUint64(sizeBytes, node.SizeInBytes)
		_, err = buf.Write(sizeBytes)
		if err != nil {
			return nil, errors.NewProcessingError("unable to write sizeInBytes", err)
		}
	}

	// write number of conflicting nodes
	binary.LittleEndian.PutUint64(b[:], uint64(len(st.ConflictingNodes)))
	if _, err = buf.Write(b[:]); err != nil {
		return nil, errors.NewProcessingError("unable to write number of conflicting nodes", err)
	}

	// write conflicting nodes
	var nodeHash chainhash.Hash
	for _, nodeHash = range st.ConflictingNodes {
		_, err = buf.Write(nodeHash[:])
		if err != nil {
			return nil, errors.NewProcessingError("unable to write conflicting node", err)
		}
	}

	return buf.Bytes(), nil
}

// SerializeNodes serializes only the nodes (list of transaction ids), not the root hash, fees, etc.
func (st *Subtree) SerializeNodes() ([]byte, error) {
	b := make([]byte, 0, len(st.Nodes)*32)
	buf := bytes.NewBuffer(b)

	var err error

	// write nodes
	var node SubtreeNode
	for _, node = range st.Nodes {
		_, err = buf.Write(node.Hash[:])
		if err != nil {
			return nil, errors.NewProcessingError("unable to write node", err)
		}
	}

	return buf.Bytes(), nil
}

func (st *Subtree) Deserialize(b []byte) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.NewProcessingError("recovered in Deserialize", r)
		}
	}()

	buf := bytes.NewBuffer(b)

	// read root hash
	st.rootHash, err = chainhash.NewHash(buf.Next(32))
	if err != nil {
		return errors.NewProcessingError("unable to read root hash", err)
	}

	// read fees
	st.Fees = binary.LittleEndian.Uint64(buf.Next(8))

	// read sizeInBytes
	st.SizeInBytes = binary.LittleEndian.Uint64(buf.Next(8))

	// read number of leaves
	numLeaves := binary.LittleEndian.Uint64(buf.Next(8))

	st.treeSize = int(numLeaves)
	// the height of a subtree is always a power of two
	st.Height = int(math.Ceil(math.Log2(float64(numLeaves))))

	// read leaves
	st.Nodes = make([]SubtreeNode, numLeaves)
	for i := uint64(0); i < numLeaves; i++ {
		st.Nodes[i].Hash = chainhash.Hash(buf.Next(32))
		st.Nodes[i].Fee = binary.LittleEndian.Uint64(buf.Next(8))
		st.Nodes[i].SizeInBytes = binary.LittleEndian.Uint64(buf.Next(8))
	}

	// read number of conflicting nodes
	numConflictingLeaves := binary.LittleEndian.Uint64(buf.Next(8))

	// read conflicting nodes
	st.ConflictingNodes = make([]chainhash.Hash, numConflictingLeaves)
	for i := uint64(0); i < numConflictingLeaves; i++ {
		st.ConflictingNodes[i] = chainhash.Hash(buf.Next(32))
	}

	// calculate rootHash and compare with given rootHash
	// we don't have to do this, because we already verified the root hash when we created the subtree
	// this Deserialize function is only used for internally saved subtrees
	//if !rootHash.IsEqual(st.RootHash()) {
	//	return fmt.Errorf("root hash mismatch")
	//}

	return nil
}

func (st *Subtree) DeserializeFromReader(reader io.Reader) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.NewProcessingError("recovered in DeserializeFromReader", r)
		}
	}()

	buf := bufio.NewReaderSize(reader, 1024*1024*16) // 16MB buffer

	// read root hash
	st.rootHash = new(chainhash.Hash)
	if _, err = io.ReadFull(buf, st.rootHash[:]); err != nil {
		return errors.NewProcessingError("unable to read root hash", err)
	}

	bytes8 := make([]byte, 8)

	// read fees
	if _, err = io.ReadFull(buf, bytes8); err != nil {
		return errors.NewProcessingError("unable to read fees", err)
	}
	st.Fees = binary.LittleEndian.Uint64(bytes8)

	// read sizeInBytes
	if _, err = io.ReadFull(buf, bytes8); err != nil {
		return errors.NewProcessingError("unable to read sizeInBytes", err)
	}
	st.SizeInBytes = binary.LittleEndian.Uint64(bytes8)

	// read number of leaves
	if _, err = io.ReadFull(buf, bytes8); err != nil {
		return errors.NewProcessingError("unable to read number of leaves", err)
	}
	numLeaves := binary.LittleEndian.Uint64(bytes8)

	st.treeSize = int(numLeaves)
	// the height of a subtree is always a power of two
	st.Height = int(math.Ceil(math.Log2(float64(numLeaves))))

	// read leaves
	st.Nodes = make([]SubtreeNode, numLeaves)
	bytes48 := make([]byte, 48)
	for i := uint64(0); i < numLeaves; i++ {
		// read all the node data in 1 go
		if _, err = io.ReadFull(buf, bytes48); err != nil {
			return errors.NewProcessingError("unable to read node", err)
		}
		st.Nodes[i].Hash = chainhash.Hash(bytes48[:32])
		st.Nodes[i].Fee = binary.LittleEndian.Uint64(bytes48[32:40])
		st.Nodes[i].SizeInBytes = binary.LittleEndian.Uint64(bytes48[40:48])
	}

	// read number of conflicting nodes
	if _, err = io.ReadFull(buf, bytes8); err != nil {
		return errors.NewProcessingError("unable to read number of conflicting nodes", err)
	}
	numConflictingLeaves := binary.LittleEndian.Uint64(bytes8)

	// read conflicting nodes
	st.ConflictingNodes = make([]chainhash.Hash, numConflictingLeaves)
	for i := uint64(0); i < numConflictingLeaves; i++ {
		if _, err = io.ReadFull(buf, st.ConflictingNodes[i][:]); err != nil {
			return errors.NewProcessingError("unable to read conflicting node", err)
		}
	}

	return nil
}

func (st *Subtree) DeserializeOld(b []byte) (err error) {
	buf := bytes.NewBuffer(b)

	// read root hash
	var rootHash [32]byte
	_, err = buf.Read(rootHash[:])
	if err != nil {
		return errors.NewProcessingError("unable to read root hash", err)
	}

	// read fees
	st.Fees, err = wire.ReadVarInt(buf, 0)
	if err != nil {
		return errors.NewProcessingError("unable to read fees", err)
	}

	// read sizeInBytes
	st.SizeInBytes, err = wire.ReadVarInt(buf, 0)
	if err != nil {
		return errors.NewProcessingError("unable to read sizeInBytes", err)
	}

	// read number of leaves
	numLeaves, err := wire.ReadVarInt(buf, 0)
	if err != nil {
		return errors.NewProcessingError("unable to read number of leaves", err)
	}

	// we must be able to support incomplete subtrees
	//if !IsPowerOfTwo(int(numLeaves)) {
	//	return fmt.Errorf("numberOfLeaves must be a power of two")
	//}

	st.treeSize = int(numLeaves)
	// the height of a subtree is always a power of two
	st.Height = int(math.Ceil(math.Log2(float64(numLeaves))))

	// read leaves
	st.Nodes = make([]SubtreeNode, numLeaves)
	var hash *chainhash.Hash
	for i := uint64(0); i < numLeaves; i++ {
		hash, err = chainhash.NewHash(buf.Next(32))
		if err != nil {
			return errors.NewProcessingError("unable to read leaves", err)
		}

		feeBytes := buf.Next(8)
		fee := binary.LittleEndian.Uint64(feeBytes)

		sizeBytes := buf.Next(8)
		sizeInBytes := binary.LittleEndian.Uint64(sizeBytes)

		st.Nodes[i] = SubtreeNode{
			Hash:        *hash,
			Fee:         fee,
			SizeInBytes: sizeInBytes,
		}
	}

	// read number of conflicting nodes
	numConflictingLeaves, err := wire.ReadVarInt(buf, 0)
	if err != nil {
		return errors.NewProcessingError("unable to read number of conflicting nodes", err)
	}

	// read conflicting nodes
	var conflictingHash *chainhash.Hash
	st.ConflictingNodes = make([]chainhash.Hash, numConflictingLeaves)
	for i := uint64(0); i < numConflictingLeaves; i++ {
		conflictingHash, err = chainhash.NewHash(buf.Next(32))
		if err != nil {
			return errors.NewProcessingError("unable to read conflicting node", err)
		}
		st.ConflictingNodes[i] = *conflictingHash
	}

	// calculate rootHash and compare with given rootHash
	if !bytes.Equal(st.RootHash()[:], rootHash[:]) {
		return errors.NewProcessingError("root hash mismatch")
	}

	// TODO: look up the fee for the last tx and subtract it from the last chainedSubtree and add it to the currentSubtree
	return nil
}

func (st *Subtree) DeserializeChan(b []byte) (nodeChan chan SubtreeNode, errChan chan error, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.NewProcessingError("recovered in DeserializeChan", r)
		}
	}()

	nodeChan = make(chan SubtreeNode)
	errChan = make(chan error, 1)
	buf := bytes.NewBuffer(b)

	// read root hash
	_ = chainhash.Hash(buf.Next(32))

	// read fees
	st.Fees = binary.LittleEndian.Uint64(buf.Next(8))

	// read sizeInBytes
	st.SizeInBytes = binary.LittleEndian.Uint64(buf.Next(8))

	// read number of leaves
	numLeaves := binary.LittleEndian.Uint64(buf.Next(8))

	st.treeSize = int(numLeaves)
	st.Height = int(math.Ceil(math.Log2(float64(numLeaves))))

	go func() {
		defer close(nodeChan)
		defer close(errChan)

		for i := uint64(0); i < numLeaves; i++ {
			nodeChan <- SubtreeNode{
				Hash:        chainhash.Hash(buf.Next(32)),
				Fee:         binary.LittleEndian.Uint64(buf.Next(8)),
				SizeInBytes: binary.LittleEndian.Uint64(buf.Next(8)),
			}
		}
	}()

	return nodeChan, errChan, nil
}

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
