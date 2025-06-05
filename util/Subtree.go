package util

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"sync"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/pkg/go-wire"
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

	mu        sync.RWMutex           // protects Nodes slice
	nodeIndex map[chainhash.Hash]int // maps txid to index in Nodes slice
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
		Nodes:    make([]SubtreeNode, 0, treeSize),
		Height:   height,
		FeeHash:  chainhash.Hash{},
		treeSize: treeSize,
		// feeBytes:     make([]byte, 8),
		// feeHashBytes: make([]byte, 40),
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

func NewSubtreeFromReader(reader io.Reader) (*Subtree, error) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("Recovered in NewSubtreeFromReader: %v\n", r)
		}
	}()

	subtree := &Subtree{}

	if err := subtree.DeserializeFromReader(reader); err != nil {
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
	if _, err = ReadBytes(buf, byteBuffer); err != nil {
		return nil, errors.NewSubtreeError("unable to read subtree root information", err)
	}

	numLeaves := binary.LittleEndian.Uint64(byteBuffer[chainhash.HashSize+16 : chainhash.HashSize+24])
	subtreeBytes = make([]byte, chainhash.HashSize*int(numLeaves))

	byteBuffer = byteBuffer[8:] // reduce read byteBuffer size by 8
	for i := uint64(0); i < numLeaves; i++ {
		if _, err = ReadBytes(buf, byteBuffer); err != nil {
			return nil, errors.NewSubtreeError("unable to read subtree node information", err)
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
		// feeBytes:         make([]byte, 8),
		// feeHashBytes:     make([]byte, 40),
	}

	copy(newSubtree.Nodes, st.Nodes)
	copy(newSubtree.ConflictingNodes, st.ConflictingNodes)

	return newSubtree
}

// Size returns the capacity of the subtree
func (st *Subtree) Size() int {
	st.mu.RLock()
	size := cap(st.Nodes)
	st.mu.RUnlock()

	return size
}

// Length returns the number of nodes in the subtree
func (st *Subtree) Length() int {
	st.mu.RLock()
	length := len(st.Nodes)
	st.mu.RUnlock()

	return length
}

func (st *Subtree) IsComplete() bool {
	st.mu.RLock()
	isComplete := len(st.Nodes) == cap(st.Nodes)
	st.mu.RUnlock()

	return isComplete
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
	st.mu.Lock()
	defer st.mu.Unlock()

	if (len(st.Nodes) + 1) > st.treeSize {
		return errors.NewSubtreeError("subtree is full")
	}

	if node.Hash.Equal(CoinbasePlaceholder) {
		return errors.NewSubtreeError("[AddSubtreeNode] coinbase placeholder node should be added with AddCoinbaseNode, tree length is %d", len(st.Nodes))
	}

	// AddNode is not concurrency safe, so we can reuse the same byte arrays
	// binary.LittleEndian.PutUint64(st.feeBytes, fee)
	// st.feeHashBytes = append(node[:], st.feeBytes[:]...)
	// if len(st.Nodes) == 0 {
	//	st.FeeHash = chainhash.HashH(st.feeHashBytes)
	// } else {
	//	st.FeeHash = chainhash.HashH(append(st.FeeHash[:], st.feeHashBytes...))
	// }

	st.Nodes = append(st.Nodes, node)
	st.rootHash = nil // reset rootHash
	st.Fees += node.Fee
	st.SizeInBytes += node.SizeInBytes

	if st.nodeIndex != nil {
		// node index map exists, add the node to it
		st.nodeIndex[node.Hash] = len(st.Nodes) - 1
	}

	return nil
}

func (st *Subtree) AddCoinbaseNode() error {
	if len(st.Nodes) != 0 {
		return errors.NewSubtreeError("subtree should be empty before adding a coinbase node")
	}

	st.Nodes = append(st.Nodes, SubtreeNode{
		Hash:        CoinbasePlaceholder,
		Fee:         0,
		SizeInBytes: 0,
	})
	st.rootHash = nil // reset rootHash
	st.Fees = 0
	st.SizeInBytes = 0

	return nil
}

func (st *Subtree) AddConflictingNode(newConflictingNode chainhash.Hash) error {
	if st.ConflictingNodes == nil {
		st.ConflictingNodes = make([]chainhash.Hash, 0, 1)
	}

	// check the conflicting node is actually in the subtree
	found := false

	for _, n := range st.Nodes {
		if n.Hash.Equal(newConflictingNode) {
			found = true
			break
		}
	}

	if !found {
		return errors.NewSubtreeError("conflicting node is not in the subtree")
	}

	// check whether the conflicting node has already been added
	for _, conflictingNode := range st.ConflictingNodes {
		if conflictingNode.Equal(newConflictingNode) {
			return nil
		}
	}

	st.ConflictingNodes = append(st.ConflictingNodes, newConflictingNode)

	return nil
}

// AddNode adds a node to the subtree
// NOTE: this function is not concurrency safe, so it should be called from a single goroutine
//
// Parameters:
//   - node: the transaction id of the node to add
//   - fee: the fee of the node
//   - sizeInBytes: the size of the node in bytes
//
// Returns:
//   - error: an error if the node could not be added
func (st *Subtree) AddNode(node chainhash.Hash, fee uint64, sizeInBytes uint64) error {
	if (len(st.Nodes) + 1) > st.treeSize {
		return errors.NewSubtreeError("subtree is full")
	}

	if node.Equal(CoinbasePlaceholder) {
		return errors.NewSubtreeError("[AddNode] coinbase placeholder node should be added with AddCoinbaseNode")
	}

	// AddNode is not concurrency safe, so we can reuse the same byte arrays
	// binary.LittleEndian.PutUint64(st.feeBytes, fee)
	// st.feeHashBytes = append(node[:], st.feeBytes[:]...)
	// if len(st.Nodes) == 0 {
	//	st.FeeHash = chainhash.HashH(st.feeHashBytes)
	// } else {
	//	st.FeeHash = chainhash.HashH(append(st.FeeHash[:], st.feeHashBytes...))
	// }

	st.Nodes = append(st.Nodes, SubtreeNode{
		Hash:        node,
		Fee:         fee,
		SizeInBytes: sizeInBytes,
	})
	st.rootHash = nil // reset rootHash
	st.Fees += fee
	st.SizeInBytes += sizeInBytes

	if st.nodeIndex != nil {
		// node index map exists, add the node to it
		st.nodeIndex[node] = len(st.Nodes) - 1
	}

	return nil
}

// RemoveNodeAtIndex removes a node at the given index and makes sure the subtree is still valid
func (st *Subtree) RemoveNodeAtIndex(index int) error {
	st.mu.Lock()
	defer st.mu.Unlock()

	if index >= len(st.Nodes) {
		return errors.NewSubtreeError("index out of range")
	}

	st.Fees -= st.Nodes[index].Fee
	st.SizeInBytes -= st.Nodes[index].SizeInBytes

	hash := st.Nodes[index].Hash
	st.Nodes = append(st.Nodes[:index], st.Nodes[index+1:]...)
	st.rootHash = nil // reset rootHash

	if st.nodeIndex != nil {
		// remove the node from the node index map
		delete(st.nodeIndex, hash)
	}

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

func (st *Subtree) GetMap() (TxMap, error) {
	lengthUint32, err := SafeIntToUint32(len(st.Nodes))
	if err != nil {
		return nil, err
	}

	m := NewSwissMapUint64(lengthUint32)
	for idx, node := range st.Nodes {
		_ = m.Put(node.Hash, uint64(idx))
	}

	return m, nil
}

func (st *Subtree) NodeIndex(hash chainhash.Hash) int {
	if st.nodeIndex == nil {
		// create the node index map
		st.mu.Lock()
		st.nodeIndex = make(map[chainhash.Hash]int, len(st.Nodes))

		for idx, node := range st.Nodes {
			st.nodeIndex[node.Hash] = idx
		}

		st.mu.Unlock()
	}

	nodeIndex, ok := st.nodeIndex[hash]
	if ok {
		return nodeIndex
	}

	return -1
}

func (st *Subtree) HasNode(hash chainhash.Hash) bool {
	return st.NodeIndex(hash) != -1
}

func (st *Subtree) GetNode(hash chainhash.Hash) (*SubtreeNode, error) {
	nodeIndex := st.NodeIndex(hash)
	if nodeIndex != -1 {
		return &st.Nodes[nodeIndex], nil
	}

	return nil, errors.NewSubtreeError("node not found")
}

func (st *Subtree) Difference(ids TxMap) ([]SubtreeNode, error) {
	// return all the ids that are in st.Nodes, but not in ids
	diff := make([]SubtreeNode, 0, 1_000)

	for _, node := range st.Nodes {
		if !ids.Exists(node.Hash) {
			diff = append(diff, node)
		}
	}

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

	for _, subtreeNode := range st.Nodes {
		_, err = buf.Write(subtreeNode.Hash[:])
		if err != nil {
			return nil, errors.NewProcessingError("unable to write node", err)
		}

		binary.LittleEndian.PutUint64(feeBytes, subtreeNode.Fee)

		_, err = buf.Write(feeBytes)
		if err != nil {
			return nil, errors.NewProcessingError("unable to write fee", err)
		}

		binary.LittleEndian.PutUint64(sizeBytes, subtreeNode.SizeInBytes)

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
	for _, nodeHash := range st.ConflictingNodes {
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
	for _, subtreeNode := range st.Nodes {
		if _, err = buf.Write(subtreeNode.Hash[:]); err != nil {
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

	return st.DeserializeFromReader(buf)
}

func (st *Subtree) DeserializeFromReader(reader io.Reader) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.NewProcessingError("recovered in DeserializeFromReader", r)
		}
	}()

	buf := bufio.NewReaderSize(reader, 1024*1024*16) // 16MB buffer

	var (
		n      int
		bytes8 = make([]byte, 8)
	)

	// read root hash
	st.rootHash = new(chainhash.Hash)
	if n, err = buf.Read(st.rootHash[:]); err != nil || n != chainhash.HashSize {
		// if _, err = io.ReadFull(buf, st.rootHash[:]); err != nil {
		return errors.NewProcessingError("unable to read root hash", err)
	}

	// read fees
	if n, err = buf.Read(bytes8); err != nil || n != 8 {
		// if _, err = io.ReadFull(buf, bytes8); err != nil {
		return errors.NewProcessingError("unable to read fees", err)
	}

	st.Fees = binary.LittleEndian.Uint64(bytes8)

	// read sizeInBytes
	if n, err = buf.Read(bytes8); err != nil || n != 8 {
		// if _, err = io.ReadFull(buf, bytes8); err != nil {
		return errors.NewProcessingError("unable to read sizeInBytes", err)
	}

	st.SizeInBytes = binary.LittleEndian.Uint64(bytes8)

	if err = st.deserializeNodes(buf); err != nil {
		return err
	}

	if err = st.deserializeConflictingNodes(buf); err != nil {
		return err
	}

	return nil
}

func (st *Subtree) deserializeNodes(buf *bufio.Reader) error {
	bytes8 := make([]byte, 8)

	// read number of leaves
	if n, err := buf.Read(bytes8); err != nil || n != 8 {
		// if _, err = io.ReadFull(buf, bytes8); err != nil {
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
		if n, err := ReadBytes(buf, bytes48); err != nil || n != 48 {
			// if _, err = io.ReadFull(buf, bytes48); err != nil {
			return errors.NewProcessingError("unable to read node", err)
		}

		st.Nodes[i].Hash = chainhash.Hash(bytes48[:32])
		st.Nodes[i].Fee = binary.LittleEndian.Uint64(bytes48[32:40])
		st.Nodes[i].SizeInBytes = binary.LittleEndian.Uint64(bytes48[40:48])
	}

	return nil
}

func (st *Subtree) deserializeConflictingNodes(buf *bufio.Reader) error {
	bytes8 := make([]byte, 8)

	// read number of conflicting nodes
	if n, err := buf.Read(bytes8); err != nil || n != 8 {
		// if _, err = io.ReadFull(buf, bytes8); err != nil {
		return errors.NewProcessingError("unable to read number of conflicting nodes", err)
	}

	numConflictingLeaves := binary.LittleEndian.Uint64(bytes8)

	// read conflicting nodes
	st.ConflictingNodes = make([]chainhash.Hash, numConflictingLeaves)

	for i := uint64(0); i < numConflictingLeaves; i++ {
		if n, err := buf.Read(st.ConflictingNodes[i][:]); err != nil || n != 32 {
			return errors.NewProcessingError("unable to read conflicting node %d", i, err)
		}
	}

	return nil
}

func ReadBytes(buf *bufio.Reader, p []byte) (n int, err error) {
	minRead := len(p)
	for n < minRead && err == nil {
		p[n], err = buf.ReadByte()
		n++
	}

	if n >= minRead {
		err = nil
	} else if n > 0 && err == io.EOF {
		err = io.ErrUnexpectedEOF
	}

	return
}

func DeserializeSubtreeConflictingFromReader(reader io.Reader) (conflictingNodes []chainhash.Hash, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.NewProcessingError("recovered in DeserializeFromReader", r)
		}
	}()

	buf := bufio.NewReaderSize(reader, 1024*1024*16) // 16MB buffer

	// skip root hash 32 bytes
	// skip fees, 8 bytes
	// skip sizeInBytes, 8 bytes
	_, _ = buf.Discard(32 + 8 + 8)

	bytes8 := make([]byte, 8)

	// read number of leaves
	if _, err = io.ReadFull(buf, bytes8); err != nil {
		return nil, errors.NewProcessingError("unable to read number of leaves", err)
	}

	numLeaves := binary.LittleEndian.Uint64(bytes8)

	numLeavesInt, err := SafeUint64ToInt(numLeaves)
	if err != nil {
		return nil, err
	}

	_, _ = buf.Discard(48 * numLeavesInt)

	// read number of conflicting nodes
	if _, err = io.ReadFull(buf, bytes8); err != nil {
		return nil, errors.NewProcessingError("unable to read number of conflicting nodes", err)
	}

	numConflictingLeaves := binary.LittleEndian.Uint64(bytes8)

	// read conflicting nodes
	conflictingNodes = make([]chainhash.Hash, numConflictingLeaves)
	for i := uint64(0); i < numConflictingLeaves; i++ {
		if _, err = io.ReadFull(buf, conflictingNodes[i][:]); err != nil {
			return nil, errors.NewProcessingError("unable to read conflicting node", err)
		}
	}

	return conflictingNodes, nil
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
	// if !IsPowerOfTwo(int(numLeaves)) {
	//	return fmt.Errorf("numberOfLeaves must be a power of two")
	// }

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
