package svnode

import (
	"encoding/binary"
	"encoding/hex"
	"math/big"
	"strconv"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

// BlockCreator helps create and mine blocks for testing
// It manages a coinbase address and uses an SVNode to fetch chain context
type BlockCreator struct {
	svNode          SVNodeI
	coinbaseAddress string
}

// BlockHeader represents a Bitcoin block header
type BlockHeader struct {
	Version    int32
	PrevBlock  string
	MerkleRoot string
	Timestamp  uint32
	Bits       uint32
	Nonce      uint32
}

// Block represents a complete Bitcoin block with header and transactions
type Block struct {
	Header       *BlockHeader
	Transactions []*bt.Tx
	Hash         string // Block hash (hex string)
	Hex          string // Serialized block (hex string)
}

// NewBlockCreator creates a block creator with the given coinbase address
// The coinbase address receives the block reward for all blocks created
func NewBlockCreator(svNode SVNodeI, coinbaseAddress string) *BlockCreator {
	return &BlockCreator{
		svNode:          svNode,
		coinbaseAddress: coinbaseAddress,
	}
}

// CoinbaseAddress returns the address that receives block rewards
func (bc *BlockCreator) CoinbaseAddress() string {
	return bc.coinbaseAddress
}

// CreateBlock creates a complete block containing the given transactions
// This includes: creating coinbase, calculating merkle root, mining (finding valid nonce)
// Returns the mined block ready for submission to the network
func (bc *BlockCreator) CreateBlock(txs []*bt.Tx) (*Block, error) {
	// Get previous block info from node
	prevBlockHash, err := bc.svNode.GetBestBlockHash()
	if err != nil {
		return nil, errors.NewProcessingError("failed to get best block hash", err)
	}

	prevBlockHeader, err := bc.svNode.GetBlockHeader(prevBlockHash, true)
	if err != nil {
		return nil, errors.NewProcessingError("failed to get block header", err)
	}
	prevHeaderMap := prevBlockHeader.(map[string]interface{})

	// Get current height
	height := uint32(prevHeaderMap["height"].(float64)) + 1

	// Create coinbase transaction
	coinbaseTx, err := bc.createCoinbaseTransaction(height, bc.coinbaseAddress)
	if err != nil {
		return nil, err
	}

	// Build transaction list: coinbase must be first
	blockTxs := append([]*bt.Tx{coinbaseTx}, txs...)

	// Calculate merkle root
	merkleRoot := bc.calculateMerkleRoot(blockTxs)

	// Create block header
	header, err := bc.createBlockHeader(prevBlockHash, merkleRoot.String(), prevHeaderMap)
	if err != nil {
		return nil, err
	}

	// Mine the block (find valid nonce)
	minedHeader, err := bc.mineBlock(header)
	if err != nil {
		return nil, err
	}

	// Serialize the block
	blockHex := bc.serializeBlock(minedHeader, blockTxs)

	// Calculate block hash
	headerBytes := bc.serializeBlockHeader(minedHeader)
	blockHash := chainhash.DoubleHashH(headerBytes)

	return &Block{
		Header:       minedHeader,
		Transactions: blockTxs,
		Hash:         blockHash.String(),
		Hex:          blockHex,
	}, nil
}

// CreateAndSubmitBlock creates a block and submits it to the SVNode
// Returns the block hash if successful
func (bc *BlockCreator) CreateAndSubmitBlock(txs []*bt.Tx) (string, error) {
	// Create the block
	block, err := bc.CreateBlock(txs)
	if err != nil {
		return "", err
	}

	// Submit to node
	_, err = bc.svNode.SubmitBlock(block.Hex)
	if err != nil {
		return "", errors.NewProcessingError("failed to submit block", err)
	}

	return block.Hash, nil
}

// createCoinbaseTransaction creates a coinbase transaction for a block
// Coinbase transactions have special rules:
// - No inputs (just a coinbase input with height in script per BIP34)
// - Creates new coins (block reward)
func (bc *BlockCreator) createCoinbaseTransaction(height uint32, address string) (*bt.Tx, error) {
	tx := bt.NewTx()

	// Coinbase script: block height + arbitrary data
	coinbaseScript := &bscript.Script{}

	// Add block height (BIP34) - must be encoded as script number
	// For height <= 16, use OP_1-OP_16; for larger, push as little-endian bytes
	if height <= 16 {
		_ = coinbaseScript.AppendOpcodes(uint8(0x50 + height)) // OP_1 = 0x51, etc.
	} else {
		// Encode height as minimal little-endian bytes
		heightBytes := encodeScriptNum(int64(height))
		_ = coinbaseScript.AppendPushData(heightBytes)
	}
	// Add some arbitrary data
	_ = coinbaseScript.AppendPushData([]byte("TeranodeTest"))

	// Create coinbase input manually
	// Previous outpoint is all zeros with index 0xffffffff
	coinbaseInput := &bt.Input{
		PreviousTxOutIndex: 0xffffffff,
		UnlockingScript:    coinbaseScript,
		SequenceNumber:     0xffffffff,
	}

	// Set previous tx ID to all zeros (coinbase)
	err := coinbaseInput.PreviousTxIDAddStr("0000000000000000000000000000000000000000000000000000000000000000")
	if err != nil {
		return nil, errors.NewProcessingError("failed to set coinbase previous tx ID", err)
	}

	tx.Inputs = append(tx.Inputs, coinbaseInput)

	// Create coinbase output - 50 BSV block reward
	lockingScript, err := bscript.NewP2PKHFromAddress(address)
	if err != nil {
		return nil, errors.NewProcessingError("failed to create coinbase locking script", err)
	}

	coinbaseOutput := &bt.Output{
		Satoshis:      5000000000, // 50 BSV
		LockingScript: lockingScript,
	}
	tx.Outputs = append(tx.Outputs, coinbaseOutput)

	return tx, nil
}

// calculateMerkleRoot calculates the merkle root from a list of transactions
// Bitcoin uses a binary merkle tree where transactions are paired and hashed
func (bc *BlockCreator) calculateMerkleRoot(txs []*bt.Tx) *chainhash.Hash {
	if len(txs) == 0 {
		return &chainhash.Hash{}
	}

	// Get transaction hashes
	hashes := make([]*chainhash.Hash, 0, len(txs))
	for _, tx := range txs {
		hash := tx.TxIDChainHash()
		hashes = append(hashes, hash)
	}

	// Build merkle tree by repeatedly hashing pairs
	for len(hashes) > 1 {
		var newLevel []*chainhash.Hash

		for i := 0; i < len(hashes); i += 2 {
			var left, right *chainhash.Hash
			left = hashes[i]

			if i+1 < len(hashes) {
				right = hashes[i+1]
			} else {
				// If odd number of hashes, duplicate the last one
				right = hashes[i]
			}

			// Concatenate and double SHA256
			combined := append(left[:], right[:]...)
			hash := chainhash.DoubleHashH(combined)
			newLevel = append(newLevel, &hash)
		}

		hashes = newLevel
	}

	return hashes[0]
}

// createBlockHeader creates a block header from previous block info
// Fetches difficulty and version from previous block
func (bc *BlockCreator) createBlockHeader(prevBlockHash string, merkleRoot string, prevBlockHeader map[string]interface{}) (*BlockHeader, error) {
	// Get current timestamp.
	// In regtest, blocks are mined instantly and may all share the same Unix timestamp.
	// BSV requires block.timestamp > MTP(last 11 blocks). When all blocks share timestamp T,
	// MTP = T, so we must use timestamp > T. Clamp to prevBlock.time+1 at minimum.
	timestamp := uint32(time.Now().Unix())
	if prevTime, ok := prevBlockHeader["time"].(float64); ok && timestamp <= uint32(prevTime) {
		timestamp = uint32(prevTime) + 1
	}

	// Use same difficulty bits as previous block (regtest)
	bitsStr := prevBlockHeader["bits"].(string)
	bits, err := strconv.ParseUint(bitsStr, 16, 32)
	if err != nil {
		return nil, errors.NewProcessingError("failed to parse difficulty bits", err)
	}

	// Get version from previous block
	version := int32(1)
	if v, ok := prevBlockHeader["version"].(float64); ok {
		version = int32(v)
	}

	return &BlockHeader{
		Version:    version,
		PrevBlock:  prevBlockHash,
		MerkleRoot: merkleRoot,
		Timestamp:  timestamp,
		Bits:       uint32(bits),
		Nonce:      0,
	}, nil
}

// mineBlock finds a valid nonce for the block header (proof-of-work)
// Repeatedly tries different nonces until the block hash meets the difficulty target
func (bc *BlockCreator) mineBlock(header *BlockHeader) (*BlockHeader, error) {
	// In regtest mode, difficulty is very low, so this should be fast
	maxNonce := uint32(0x7fffffff)

	for nonce := uint32(0); nonce < maxNonce; nonce++ {
		header.Nonce = nonce

		// Serialize header and hash it
		headerBytes := bc.serializeBlockHeader(header)
		hash := chainhash.DoubleHashH(headerBytes)

		// Check if hash meets difficulty target
		if bc.checkProofOfWork(hash, header.Bits) {
			return header, nil
		}
	}

	return nil, errors.NewProcessingError("failed to mine block - no valid nonce found")
}

// serializeBlockHeader serializes a block header to 80 bytes
// Bitcoin block header format: version + prevBlock + merkleRoot + timestamp + bits + nonce
func (bc *BlockCreator) serializeBlockHeader(header *BlockHeader) []byte {
	buf := make([]byte, 80)

	// Version (4 bytes, little endian)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(header.Version))

	// Previous block hash (32 bytes in wire byte order)
	// NewHashFromStr parses the display hex and reverses it to internal/wire byte order,
	// so hash[:] is already the correct wire format - no additional reversal needed.
	prevHash, _ := chainhash.NewHashFromStr(header.PrevBlock)
	copy(buf[4:36], prevHash[:])

	// Merkle root (32 bytes in wire byte order) - same convention as prev_block
	merkleHash, _ := chainhash.NewHashFromStr(header.MerkleRoot)
	copy(buf[36:68], merkleHash[:])

	// Timestamp (4 bytes, little endian)
	binary.LittleEndian.PutUint32(buf[68:72], header.Timestamp)

	// Bits (4 bytes, little endian)
	binary.LittleEndian.PutUint32(buf[72:76], header.Bits)

	// Nonce (4 bytes, little endian)
	binary.LittleEndian.PutUint32(buf[76:80], header.Nonce)

	return buf
}

// serializeBlock serializes a complete block (header + transactions) to hex string
func (bc *BlockCreator) serializeBlock(header *BlockHeader, txs []*bt.Tx) string {
	var buf []byte

	// Serialize header
	buf = append(buf, bc.serializeBlockHeader(header)...)

	// Transaction count (varint)
	buf = append(buf, varInt(uint64(len(txs)))...)

	// Serialize each transaction
	for _, tx := range txs {
		txBytes := tx.Bytes()
		buf = append(buf, txBytes...)
	}

	return hex.EncodeToString(buf)
}

// checkProofOfWork checks if a hash meets the difficulty target
func (bc *BlockCreator) checkProofOfWork(hash chainhash.Hash, bits uint32) bool {
	// Extract target from bits (compact representation)
	target := compactToBig(bits)

	// Convert hash to big.Int (reverse byte order)
	hashBytes := reverseBytes(hash[:])
	hashInt := new(big.Int).SetBytes(hashBytes)

	// Check if hash <= target
	return hashInt.Cmp(target) <= 0
}

// Helper functions (Bitcoin protocol encoding)

// compactToBig converts compact difficulty representation to big.Int target
func compactToBig(compact uint32) *big.Int {
	// Extract exponent (first byte) and mantissa (last 3 bytes)
	exponent := compact >> 24
	mantissa := compact & 0x00ffffff

	var target *big.Int
	if exponent <= 3 {
		mantissa >>= 8 * (3 - exponent)
		target = big.NewInt(int64(mantissa))
	} else {
		target = big.NewInt(int64(mantissa))
		target.Lsh(target, uint(8*(exponent-3)))
	}

	return target
}

// reverseBytes reverses a byte slice (Bitcoin uses little-endian for hashes)
func reverseBytes(b []byte) []byte {
	reversed := make([]byte, len(b))
	for i := range b {
		reversed[i] = b[len(b)-1-i]
	}
	return reversed
}

// encodeScriptNum encodes an integer as a Bitcoin script number (minimal encoding)
func encodeScriptNum(n int64) []byte {
	if n == 0 {
		return []byte{}
	}

	negative := n < 0
	if negative {
		n = -n
	}

	var result []byte
	for n > 0 {
		result = append(result, byte(n&0xff))
		n >>= 8
	}

	// If the most significant bit is set, add an extra byte
	if result[len(result)-1]&0x80 != 0 {
		if negative {
			result = append(result, 0x80)
		} else {
			result = append(result, 0x00)
		}
	} else if negative {
		result[len(result)-1] |= 0x80
	}

	return result
}

// varInt encodes an integer as a Bitcoin variable-length integer
func varInt(n uint64) []byte {
	if n < 0xfd {
		return []byte{byte(n)}
	} else if n <= 0xffff {
		buf := make([]byte, 3)
		buf[0] = 0xfd
		binary.LittleEndian.PutUint16(buf[1:], uint16(n))
		return buf
	} else if n <= 0xffffffff {
		buf := make([]byte, 5)
		buf[0] = 0xfe
		binary.LittleEndian.PutUint32(buf[1:], uint32(n))
		return buf
	} else {
		buf := make([]byte, 9)
		buf[0] = 0xff
		binary.LittleEndian.PutUint64(buf[1:], n)
		return buf
	}
}
