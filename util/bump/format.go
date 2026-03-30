// Package bump provides BUMP (BSV Unified Merkle Path) format support for merkle proofs.
// BUMP is a standardized format for representing merkle tree paths in the BSV ecosystem,
// defined in BRC-74: https://github.com/bitcoin-sv/BRCs/blob/master/transactions/0074.md
package bump

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util/merkleproof"
)

// Format represents the BSV Unified Merkle Path format structure.
// This format provides a compact and standardized way to represent merkle tree paths
// for SPV (Simplified Payment Verification) in the BSV ecosystem.
type Format struct {
	// BlockHeight is the height of the block containing the transaction
	BlockHeight uint32 `json:"blockHeight"`

	// Path represents the merkle tree path as an array of levels.
	// Each level contains the nodes at that depth in the tree.
	Path []Level `json:"path"`
}

// Level represents a single level in the merkle tree path.
// Each level contains one or more nodes that help reconstruct the path
// from the transaction to the merkle root.
type Level []Node

// Node represents a single node at a level in the merkle tree.
// It contains the offset (position) and optionally the hash data,
// with flags indicating the type of node and data present.
type Node struct {
	// Offset is the position of this node within its level of the tree
	Offset uint32 `json:"offset"`

	// Hash is the hex-encoded hash value for this node (optional, may be empty)
	Hash string `json:"hash,omitempty"`

	// TxID indicates whether this hash represents a client transaction ID (optional)
	TxID bool `json:"txid,omitempty"`

	// Duplicate indicates whether to duplicate the working hash (optional)
	Duplicate bool `json:"duplicate,omitempty"`
}

// BUMPFlags represents the flag values used in binary BUMP format
const (
	// FlagData indicates that hash data follows, not a client txid
	FlagData = 0x00

	// FlagDuplicate indicates to duplicate the working hash
	FlagDuplicate = 0x01

	// FlagTxID indicates that hash data follows and is a client txid
	FlagTxID = 0x02
)

// hashToDisplayHex returns the hash as a display-order hex string (big-endian / byte-reversed),
// which is the standard representation used in BRC-74 JSON and the go-bc reference implementation.
// The binary BUMP format stores hashes in internal (little-endian) order; the conversion from
// display order to internal order happens in EncodeBinary.
func hashToDisplayHex(h chainhash.Hash) string {
	return h.String()
}

// ConvertToBUMP converts a standard merkle proof to BUMP format.
// This function takes the existing Teranode merkle proof structure and converts
// it to the standardized BUMP format for compatibility with BSV ecosystem tools.
func ConvertToBUMP(proof *merkleproof.MerkleProof) (*Format, error) {
	if proof == nil {
		return nil, errors.NewInvalidArgumentError("proof cannot be nil")
	}

	bump := &Format{
		BlockHeight: proof.BlockHeight,
		Path:        make([]Level, 0),
	}

	// Convert subtree proof levels
	txIndex := proof.TxIndexInSubtree

	for levelIdx, siblingHash := range proof.SubtreeProof {
		level := make(Level, 0, 2)

		// Calculate the offset at this level
		// At each level up the tree, the index is divided by 2
		offset := uint32(txIndex >> levelIdx)

		// Determine sibling offset (adjacent node)
		siblingOffset := offset ^ 1 // Flip the last bit to get sibling

		// Add the sibling node with its hash in display order (BRC-74 convention)
		level = append(level, Node{
			Offset: siblingOffset,
			Hash:   hashToDisplayHex(siblingHash),
		})

		bump.Path = append(bump.Path, level)
	}

	// Convert block proof levels if present
	// Block proofs represent the path from subtree root to block merkle root
	subtreeIndex := proof.SubtreeIndex

	for levelIdx, siblingHash := range proof.BlockProof {
		level := make(Level, 0, 2)

		// Calculate the offset at this block level
		blockLevelOffset := uint32(subtreeIndex >> levelIdx)

		// Determine sibling offset
		siblingOffset := blockLevelOffset ^ 1

		// Add the sibling node with its hash in display order (BRC-74 convention)
		level = append(level, Node{
			Offset: siblingOffset,
			Hash:   hashToDisplayHex(siblingHash),
		})

		bump.Path = append(bump.Path, level)
	}

	// BRC-74 requires level 0 to include the target txid (flag 0x02) alongside its sibling.
	// Without this, go-bc's CalculateRootGivenTxid cannot find the starting transaction.
	var zeroHash chainhash.Hash
	if proof.TxID != zeroHash && len(bump.Path) > 0 {
		txidNode := Node{
			Offset: uint32(proof.TxIndexInSubtree),
			Hash:   hashToDisplayHex(proof.TxID),
			TxID:   true,
		}

		level0 := bump.Path[0]
		if uint32(proof.TxIndexInSubtree)%2 == 0 {
			// Even index: txid is on the left, prepend
			bump.Path[0] = append(Level{txidNode}, level0...)
		} else {
			// Odd index: txid is on the right, append
			bump.Path[0] = append(level0, txidNode)
		}
	}

	return bump, nil
}

// EncodeBinary encodes the BUMP format to binary representation.
// The binary format follows the BUMP specification:
// - Block height as VarInt
// - Tree height as single byte
// - For each level: number of leaf nodes + node data (offset + flags + hash)
func (b *Format) EncodeBinary() ([]byte, error) {
	var buf bytes.Buffer

	// Write block height as VarInt
	if err := writeVarInt(&buf, uint64(b.BlockHeight)); err != nil {
		return nil, errors.NewProcessingError("failed to write block height", err)
	}

	// Write tree height (number of levels)
	treeHeight := uint8(len(b.Path))

	if err := buf.WriteByte(treeHeight); err != nil {
		return nil, errors.NewProcessingError("failed to write tree height", err)
	}

	// Write each level
	for levelIdx, level := range b.Path {
		// Write number of leaf nodes at this level
		if err := writeVarInt(&buf, uint64(len(level))); err != nil {
			return nil, errors.NewProcessingError("failed to write level %d node count", levelIdx, err)
		}

		// Write each node in the level
		for nodeIdx, node := range level {
			// Write offset as VarInt
			if err := writeVarInt(&buf, uint64(node.Offset)); err != nil {
				return nil, errors.NewProcessingError("failed to write offset for level %d, node %d", levelIdx, nodeIdx, err)
			}

			// Determine and write flags
			var flag byte

			if node.Duplicate {
				flag = FlagDuplicate
			} else if node.TxID {
				flag = FlagTxID
			} else {
				flag = FlagData
			}

			if err := buf.WriteByte(flag); err != nil {
				return nil, errors.NewProcessingError("failed to write flag for level %d, node %d", levelIdx, nodeIdx, err)
			}

			// Write hash data if present (flags 0x00 and 0x02 include hash)
			if flag == FlagData || flag == FlagTxID {
				if node.Hash == "" {
					return nil, errors.NewProcessingError("hash required for flag %02x at level %d, node %d", flag, levelIdx, nodeIdx)
				}

				h, err := chainhash.NewHashFromStr(node.Hash)
				if err != nil {
					return nil, errors.NewProcessingError("invalid hash at level %d, node %d", levelIdx, nodeIdx, err)
				}

				// chainhash.NewHashFromStr converts display-order hex to internal (little-endian) byte order
				if _, err := buf.Write(h[:]); err != nil {
					return nil, errors.NewProcessingError("failed to write hash for level %d, node %d", levelIdx, nodeIdx, err)
				}
			}
		}
	}

	return buf.Bytes(), nil
}

// EncodeHex encodes the BUMP format to hexadecimal string representation.
func (b *Format) EncodeHex() (string, error) {
	binaryData, err := b.EncodeBinary()
	if err != nil {
		return "", errors.NewProcessingError("failed to encode binary", err)
	}

	return hex.EncodeToString(binaryData), nil
}

// writeVarInt writes a variable-length integer to the buffer.
// This follows Bitcoin's VarInt encoding standard.
func writeVarInt(buf *bytes.Buffer, value uint64) error {
	if value < 0xFD {
		return buf.WriteByte(byte(value))
	} else if value <= 0xFFFF {
		if err := buf.WriteByte(0xFD); err != nil {
			return err
		}

		return binary.Write(buf, binary.LittleEndian, uint16(value))
	} else if value <= 0xFFFFFFFF {
		if err := buf.WriteByte(0xFE); err != nil {
			return err
		}

		return binary.Write(buf, binary.LittleEndian, uint32(value))
	} else {
		if err := buf.WriteByte(0xFF); err != nil {
			return err
		}

		return binary.Write(buf, binary.LittleEndian, value)
	}
}

// Validate validates that a BUMP structure is correctly formatted.
func Validate(bump *Format) error {
	if bump == nil {
		return errors.NewInvalidArgumentError("BUMP structure cannot be nil")
	}

	if len(bump.Path) == 0 {
		return errors.NewInvalidArgumentError("BUMP path cannot be empty")
	}

	if len(bump.Path) > 64 {
		return errors.NewInvalidArgumentError("BUMP path too long: %d levels (max 64)", len(bump.Path))
	}

	for levelIdx, level := range bump.Path {
		if len(level) == 0 {
			return errors.NewInvalidArgumentError("level %d cannot be empty", levelIdx)
		}

		for nodeIdx, node := range level {
			// Validate hash format if present
			if node.Hash != "" {
				if len(node.Hash) != 64 {
					return errors.NewInvalidArgumentError("invalid hash length at level %d, node %d: expected 64 chars, got %d",
						levelIdx, nodeIdx, len(node.Hash))
				}

				if _, err := hex.DecodeString(node.Hash); err != nil {
					return errors.NewInvalidArgumentError("invalid hash hex at level %d, node %d", levelIdx, nodeIdx, err)
				}
			}

			// Validate flag combinations
			if node.Duplicate && node.Hash != "" {
				return errors.NewInvalidArgumentError("duplicate flag cannot be combined with hash at level %d, node %d", levelIdx, nodeIdx)
			}
		}
	}

	return nil
}
