// Package subtreeanalyzer provides functionality to analyze scheduled blob deletions
// and determine which blocks contain the subtrees scheduled for deletion.
package subtreeanalyzer

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/usql"
)

type scheduledDeletion struct {
	ID             int64
	BlobKey        []byte
	FileType       string
	StoreType      int32
	DeleteAtHeight uint32
	RetryCount     int
}

type blockInfo struct {
	Height   uint32
	Hash     []byte
	Subtrees [][]byte
}

type subtreeDeletionInfo struct {
	SubtreeHash     string
	FileType        string
	StoreType       int32
	MinDeleteHeight uint32
	MaxDeleteHeight uint32
	RetryCount      int
}

type blockWithDeletions struct {
	Height                uint32
	Hash                  string
	SubtreesWithDeletions []*subtreeDeletionInfo
}

// Analyze retrieves all scheduled blob deletions and finds which blocks contain each subtree.
func Analyze(ctx context.Context, logger ulogger.Logger, settings *settings.Settings) error {
	store, err := blockchainstore.NewStore(logger, settings.BlockChain.StoreURL, settings)
	if err != nil {
		return errors.NewServiceError("failed to create blockchain store", err)
	}

	db := store.GetDB()

	logger.Infof("Querying scheduled blob deletions...")
	deletions, err := getScheduledDeletions(ctx, db)
	if err != nil {
		return errors.NewServiceError("failed to get scheduled deletions", err)
	}

	logger.Infof("Found %d scheduled deletions", len(deletions))

	if len(deletions) == 0 {
		fmt.Println("No scheduled blob deletions found.")
		return nil
	}

	logger.Infof("Building subtree deletion map...")
	subtreeDeletionMap := buildSubtreeDeletionMap(deletions)

	logger.Infof("Finding blocks containing scheduled subtrees...")
	blocksWithDeletions, err := findBlocksWithScheduledDeletions(ctx, db, subtreeDeletionMap)
	if err != nil {
		return errors.NewServiceError("failed to find blocks with deletions", err)
	}

	if len(blocksWithDeletions) == 0 {
		fmt.Println("\nNo blocks found containing subtrees with scheduled deletions.")
		return nil
	}

	fmt.Printf("\n=== Blocks with Scheduled Subtree Deletions ===\n")
	fmt.Printf("Found %d blocks containing subtrees scheduled for deletion\n\n", len(blocksWithDeletions))

	for _, blockInfo := range blocksWithDeletions {
		fmt.Printf("Block Height: %d\n", blockInfo.Height)
		fmt.Printf("  Block Hash: %s\n", blockInfo.Hash)
		fmt.Printf("  Subtrees with Scheduled Deletions (%d):\n", len(blockInfo.SubtreesWithDeletions))

		for _, subtreeInfo := range blockInfo.SubtreesWithDeletions {
			if subtreeInfo.MinDeleteHeight == subtreeInfo.MaxDeleteHeight {
				fmt.Printf("    - %s (Delete at: %d, Type: %s, Retries: %d)\n",
					subtreeInfo.SubtreeHash,
					subtreeInfo.MinDeleteHeight,
					subtreeInfo.FileType,
					subtreeInfo.RetryCount)
			} else {
				fmt.Printf("    - %s (Delete at: %d-%d, Type: %s, Retries: %d)\n",
					subtreeInfo.SubtreeHash,
					subtreeInfo.MinDeleteHeight,
					subtreeInfo.MaxDeleteHeight,
					subtreeInfo.FileType,
					subtreeInfo.RetryCount)
			}
		}

		fmt.Println()
	}

	return nil
}

func getScheduledDeletions(ctx context.Context, db *usql.DB) ([]*scheduledDeletion, error) {
	query := `
		SELECT id, blob_key, file_type, store_type, delete_at_height, retry_count
		FROM scheduled_blob_deletions
		ORDER BY delete_at_height ASC, id ASC
	`

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var deletions []*scheduledDeletion
	for rows.Next() {
		var d scheduledDeletion
		if err := rows.Scan(&d.ID, &d.BlobKey, &d.FileType, &d.StoreType, &d.DeleteAtHeight, &d.RetryCount); err != nil {
			return nil, err
		}
		deletions = append(deletions, &d)
	}

	return deletions, rows.Err()
}

func buildSubtreeDeletionMap(deletions []*scheduledDeletion) map[string]*subtreeDeletionInfo {
	deletionMap := make(map[string]*subtreeDeletionInfo)

	for _, deletion := range deletions {
		subtreeHash := hex.EncodeToString(deletion.BlobKey)

		if existing, found := deletionMap[subtreeHash]; found {
			if deletion.DeleteAtHeight < existing.MinDeleteHeight {
				existing.MinDeleteHeight = deletion.DeleteAtHeight
			}
			if deletion.DeleteAtHeight > existing.MaxDeleteHeight {
				existing.MaxDeleteHeight = deletion.DeleteAtHeight
			}
			if deletion.RetryCount > existing.RetryCount {
				existing.RetryCount = deletion.RetryCount
			}
		} else {
			deletionMap[subtreeHash] = &subtreeDeletionInfo{
				SubtreeHash:     subtreeHash,
				FileType:        deletion.FileType,
				StoreType:       deletion.StoreType,
				MinDeleteHeight: deletion.DeleteAtHeight,
				MaxDeleteHeight: deletion.DeleteAtHeight,
				RetryCount:      deletion.RetryCount,
			}
		}
	}

	return deletionMap
}

func findBlocksWithScheduledDeletions(ctx context.Context, db *usql.DB, deletionMap map[string]*subtreeDeletionInfo) ([]*blockWithDeletions, error) {
	query := `
		SELECT height, hash, subtrees
		FROM blocks
		WHERE subtree_count > 0
		ORDER BY height ASC
	`

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var blocksWithDeletions []*blockWithDeletions

	for rows.Next() {
		var height uint32
		var hash []byte
		var subtreesBytes []byte

		if err := rows.Scan(&height, &hash, &subtreesBytes); err != nil {
			return nil, err
		}

		subtrees, err := parseSubtrees(subtreesBytes)
		if err != nil {
			continue
		}

		var matchingSubtrees []*subtreeDeletionInfo
		for _, subtree := range subtrees {
			subtreeHash := hex.EncodeToString(subtree)
			if deletionInfo, found := deletionMap[subtreeHash]; found {
				matchingSubtrees = append(matchingSubtrees, deletionInfo)
			}
		}

		if len(matchingSubtrees) > 0 {
			blocksWithDeletions = append(blocksWithDeletions, &blockWithDeletions{
				Height:                height,
				Hash:                  hex.EncodeToString(hash),
				SubtreesWithDeletions: matchingSubtrees,
			})
		}
	}

	return blocksWithDeletions, rows.Err()
}

func parseSubtrees(subtreesBytes []byte) ([][]byte, error) {
	if len(subtreesBytes) == 0 {
		return nil, nil
	}

	buf := bytes.NewBuffer(subtreesBytes)

	subTreeCount, err := wire.ReadVarInt(buf, 0)
	if err != nil {
		return nil, errors.NewProcessingError("error reading subtree count", err)
	}

	subtrees := make([][]byte, 0, subTreeCount)
	var subtreeBytes [chainhash.HashSize]byte

	for i := uint64(0); i < subTreeCount; i++ {
		_, err = io.ReadFull(buf, subtreeBytes[:])
		if err != nil {
			return nil, errors.NewProcessingError("error reading subtree hash %d", i, err)
		}

		subtreeCopy := make([]byte, chainhash.HashSize)
		copy(subtreeCopy, subtreeBytes[:])
		subtrees = append(subtrees, subtreeCopy)
	}

	return subtrees, nil
}
