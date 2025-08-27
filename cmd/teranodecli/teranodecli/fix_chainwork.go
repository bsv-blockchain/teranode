package teranodecli

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"math/big"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/services/blockchain/work"
	_ "github.com/lib/pq"
	_ "modernc.org/sqlite"
)

type blockData struct {
	Height    uint32
	ID        int64
	ParentID  int64
	Hash      string
	NBits     uint32
	ChainWork *big.Int
}

type errorRecord struct {
	ID               int64
	Height           uint32
	Hash             string
	StoredChainWork  *big.Int
	CorrectChainWork *big.Int
	Difference       *big.Int
}

// buildPostgresConnString builds a PostgreSQL connection string from a URL
// This matches the logic in util/sql.go InitPostgresDB
func buildPostgresConnString(storeURL *url.URL) string {
	dbHost := storeURL.Hostname()
	port := storeURL.Port()
	dbPort := 5432
	if port != "" {
		dbPort, _ = strconv.Atoi(port)
	}
	dbName := storeURL.Path[1:]
	dbUser := ""
	dbPassword := ""

	if storeURL.User != nil {
		dbUser = storeURL.User.Username()
		dbPassword, _ = storeURL.User.Password()
	}

	// Default sslmode to "disable"
	sslMode := "disable"

	// Check if "sslmode" is present in the query parameters
	queryParams := storeURL.Query()
	if val, ok := queryParams["sslmode"]; ok && len(val) > 0 {
		sslMode = val[0] // Use the first value if multiple are provided
	}

	// Build connection string, only include password if it's not empty
	connStr := fmt.Sprintf("host=%s port=%d dbname=%s sslmode=%s",
		dbHost, dbPort, dbName, sslMode)

	// Only add user if it's not empty
	if dbUser != "" {
		connStr = fmt.Sprintf("%s user=%s", connStr, dbUser)
	}

	// Only add password if it's not empty
	if dbPassword != "" {
		connStr = fmt.Sprintf("%s password=%s", connStr, dbPassword)
	}

	return connStr
}

func fixChainwork(dbURL string, dryRun bool, batchSize int, startHeight, endHeight uint32) error {
	// Parse database URL
	parsedURL, err := url.Parse(dbURL)
	if err != nil {
		return errors.NewProcessingError("failed to parse database URL", err)
	}

	// Determine database type and connection string
	var dbType, connStr string
	switch parsedURL.Scheme {
	case "sqlite", "sqlite3":
		dbType = "sqlite"
		connStr = strings.TrimPrefix(dbURL, "sqlite://")
		connStr = strings.TrimPrefix(connStr, "sqlite3://")
	case "postgres", "postgresql":
		dbType = "postgres"
		// Use the same connection string building logic as teranode
		connStr = buildPostgresConnString(parsedURL)
	default:
		return errors.NewProcessingError("unsupported database type: %s", parsedURL.Scheme)
	}

	// Open database connection
	db, err := sql.Open(dbType, connStr)
	if err != nil {
		return errors.NewStorageError("failed to open database", err)
	}
	defer db.Close()

	// Test connection
	if err := db.Ping(); err != nil {
		return errors.NewStorageError("failed to connect to database", err)
	}

	if dbType == "postgres" {
		// Extract sslmode from connection string for display
		sslMode := "disable"
		queryParams := parsedURL.Query()
		if val, ok := queryParams["sslmode"]; ok && len(val) > 0 {
			sslMode = val[0]
		}
		fmt.Printf("Connected to PostgreSQL database (sslmode=%s)\n", sslMode)
	} else {
		fmt.Printf("Connected to %s database\n", dbType)
	}
	fmt.Printf("Dry run mode: %v\n", dryRun)

	// Load longest chain
	chain, err := loadLongestChain(db, dbType)
	if err != nil {
		return err
	}

	fmt.Printf("Loaded %d blocks in longest chain\n", len(chain))

	// Verify chainwork and collect errors for the specified range
	// Note: We need the FULL chain to calculate chainwork correctly
	chainworkErrors := verifyChainworkInRange(chain, startHeight, endHeight)

	// Report results
	fmt.Printf("\n")
	fmt.Printf("%s\n", strings.Repeat("=", 60))
	fmt.Printf("CHAINWORK VERIFICATION COMPLETE\n")

	// If endHeight is 0, use chain tip
	if endHeight == 0 && len(chain) > 0 {
		endHeight = chain[len(chain)-1].Height
	}

	// Calculate blocks checked in range
	blocksChecked := 0
	for _, block := range chain {
		if block.Height >= startHeight && block.Height <= endHeight {
			blocksChecked++
		}
	}

	fmt.Printf("Total blocks checked: %d\n", blocksChecked)
	fmt.Printf("Total errors found: %d\n", len(chainworkErrors))

	if len(chainworkErrors) > 0 {
		fmt.Printf("\n")
		fmt.Printf("Difference summary:\n")
		fmt.Printf("  First difference at height: %d\n", chainworkErrors[0].Height)
		fmt.Printf("  Last difference at height:  %d\n", chainworkErrors[len(chainworkErrors)-1].Height)
		fmt.Printf("  Difference at first occurrence: %s\n", chainworkErrors[0].Difference.Text(16))
		fmt.Printf("  Difference at last occurrence:  %s\n", chainworkErrors[len(chainworkErrors)-1].Difference.Text(16))

		if !dryRun {
			// Apply fixes
			fmt.Printf("\n")
			fmt.Printf("%s\n", strings.Repeat("=", 60))
			fmt.Printf("APPLYING CHAINWORK FIXES\n")

			if err := applyFixes(db, dbType, chainworkErrors, batchSize); err != nil {
				return err
			}

			fmt.Printf("\n")
			fmt.Printf("Successfully updated chainwork values in database!\n")
		} else {
			fmt.Printf("\n")
			fmt.Printf("DRY RUN MODE - No changes made to database\n")
			fmt.Printf("To apply fixes, run with --dry-run=false\n")
		}
	} else {
		fmt.Printf("\n")
		fmt.Printf("All chainwork values are correct!\n")
	}

	return nil
}

func loadLongestChain(db *sql.DB, dbType string) ([]blockData, error) {
	// Get the max height block (chain tip)
	var tipID int64
	var tipHeight uint32

	query := `SELECT id, height FROM blocks ORDER BY height DESC LIMIT 1`
	err := db.QueryRow(query).Scan(&tipID, &tipHeight)
	if err != nil {
		return nil, errors.NewStorageError("failed to get chain tip", err)
	}

	fmt.Printf("Chain tip at height %d (ID: %d)\n", tipHeight, tipID)

	// Walk backwards from tip to genesis using parent_id
	chain := make([]blockData, 0, tipHeight+1)
	currentID := tipID

	for {
		var block blockData
		var parentID sql.NullInt64

		query := `SELECT id, parent_id, height, hash, n_bits, chain_work FROM blocks WHERE id = $1`
		if dbType == "sqlite" {
			query = strings.ReplaceAll(query, "$1", "?")
		}

		// For PostgreSQL, we need to handle bytea columns differently
		var hashData, nBitsData, chainWorkData interface{}

		err := db.QueryRow(query, currentID).Scan(
			&block.ID, &parentID, &block.Height,
			&hashData, &nBitsData, &chainWorkData,
		)
		if err != nil {
			return nil, errors.NewStorageError(fmt.Sprintf("failed to query block at ID %d", currentID), err)
		}

		// Handle NULL parent_id (genesis block)
		if parentID.Valid {
			block.ParentID = parentID.Int64
		} else {
			block.ParentID = -1 // Use -1 to indicate no parent
		}

		// Convert hash to hex string
		switch v := hashData.(type) {
		case []byte:
			// PostgreSQL stores hashes in little-endian, reverse for display
			hashBytes := make([]byte, len(v))
			copy(hashBytes, v)
			// Reverse bytes for big-endian display
			for i := 0; i < len(hashBytes)/2; i++ {
				hashBytes[i], hashBytes[len(hashBytes)-1-i] = hashBytes[len(hashBytes)-1-i], hashBytes[i]
			}
			block.Hash = hex.EncodeToString(hashBytes)
		case string:
			block.Hash = v
		}

		// Convert nBits and chainwork based on database type
		var nBitsBytes []byte
		var chainWorkHex string

		if dbType == "postgres" {
			// PostgreSQL returns bytea as []byte
			switch v := nBitsData.(type) {
			case []byte:
				nBitsBytes = v
			case string:
				// If it's a string with \x prefix, parse it
				hexStr := strings.TrimPrefix(v, "\\x")
				nBitsBytes, _ = hex.DecodeString(hexStr)
			}

			// Handle chainwork
			switch v := chainWorkData.(type) {
			case []byte:
				chainWorkHex = hex.EncodeToString(v)
			case string:
				chainWorkHex = strings.TrimPrefix(v, "\\x")
			}

			// PostgreSQL stores n_bits in little-endian
			if len(nBitsBytes) == 4 {
				// Read as little-endian (reverse byte order)
				block.NBits = uint32(nBitsBytes[3])<<24 |
					uint32(nBitsBytes[2])<<16 |
					uint32(nBitsBytes[1])<<8 |
					uint32(nBitsBytes[0])
			}

		} else {
			// SQLite stores as hex strings
			nBitsHex := nBitsData.(string)
			chainWorkHex = chainWorkData.(string)

			nBitsBytes, _ = hex.DecodeString(nBitsHex)
			if len(nBitsBytes) == 4 {
				// Read as big-endian (network byte order)
				block.NBits = uint32(nBitsBytes[0])<<24 |
					uint32(nBitsBytes[1])<<16 |
					uint32(nBitsBytes[2])<<8 |
					uint32(nBitsBytes[3])
			}
		}

		// Convert chainwork from hex to big.Int
		block.ChainWork = new(big.Int)
		block.ChainWork.SetString(chainWorkHex, 16)

		chain = append(chain, block)

		if block.Height == 0 || block.ParentID == -1 {
			break
		}
		currentID = block.ParentID
	}

	// Reverse to get ascending order
	for i, j := 0, len(chain)-1; i < j; i, j = i+1, j-1 {
		chain[i], chain[j] = chain[j], chain[i]
	}

	return chain, nil
}

func filterChainByHeight(chain []blockData, startHeight, endHeight uint32) []blockData {
	if endHeight == 0 && len(chain) > 0 {
		endHeight = chain[len(chain)-1].Height
	}

	filtered := make([]blockData, 0)
	for _, block := range chain {
		if block.Height >= startHeight && block.Height <= endHeight {
			filtered = append(filtered, block)
		}
	}
	return filtered
}

func verifyChainworkInRange(chain []blockData, startHeight, endHeight uint32) []errorRecord {
	// If endHeight is 0, use the chain tip height
	if endHeight == 0 && len(chain) > 0 {
		endHeight = chain[len(chain)-1].Height
	}

	fmt.Printf("Verifying chainwork calculations from height %d to %d...\n", startHeight, endHeight)

	chainworkErrors := make([]errorRecord, 0)

	// Track the current difference to detect when it increases
	var currentDifference *big.Int

	// Calculate chainwork from genesis through entire chain
	cumulativeChainWork := big.NewInt(0)

	for _, block := range chain {
		// Calculate work for this block
		blockWork := work.CalcBlockWork(block.NBits)

		// Add to cumulative chainwork
		cumulativeChainWork = new(big.Int).Add(cumulativeChainWork, blockWork)

		// Only check and report errors for blocks in the specified range
		if block.Height >= startHeight && block.Height <= endHeight {
			// Compare with stored chainwork
			if block.ChainWork.Cmp(cumulativeChainWork) != 0 {
				difference := new(big.Int).Sub(block.ChainWork, cumulativeChainWork)
				differenceMagnitude := new(big.Int).Abs(difference)

				chainworkErrors = append(chainworkErrors, errorRecord{
					ID:               block.ID,
					Height:           block.Height,
					Hash:             block.Hash,
					StoredChainWork:  new(big.Int).Set(block.ChainWork),
					CorrectChainWork: new(big.Int).Set(cumulativeChainWork),
					Difference:       difference,
				})

				// Print when difference increases or this is the first error
				if currentDifference == nil || differenceMagnitude.Cmp(currentDifference) > 0 {
					if currentDifference == nil {
						fmt.Printf("\nFirst chainwork error detected:\n")
					} else {
						fmt.Printf("\nDifference increased (another bug occurrence):\n")
					}
					fmt.Printf("  Height:   %d\n", block.Height)
					fmt.Printf("  Hash:     %s\n", block.Hash)
					fmt.Printf("  nBits:    %08x\n", block.NBits)
					fmt.Printf("  Stored:   %s\n", block.ChainWork.Text(16))
					fmt.Printf("  Correct:  %s\n", cumulativeChainWork.Text(16))
					fmt.Printf("  Diff:     %s\n", difference.Text(16))

					currentDifference = differenceMagnitude
				}
			}
		}

		// Progress indicator
		if block.Height%100000 == 0 && block.Height > 0 {
			fmt.Printf("Processed %d blocks, found %d errors so far...\n", block.Height, len(chainworkErrors))
		}
	}

	return chainworkErrors
}

func applyFixes(db *sql.DB, dbType string, chainworkErrors []errorRecord, batchSize int) error {
	// Setup signal handling for graceful interruption
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Printf("\n")
		fmt.Printf("⚠️  Received interrupt signal, stopping gracefully...\n")
		cancel()
	}()

	totalUpdates := len(chainworkErrors)
	updateCount := 0

	for i := 0; i < len(chainworkErrors); i += batchSize {
		select {
		case <-ctx.Done():
			fmt.Printf("Update interrupted after %d/%d updates\n", updateCount, totalUpdates)
			return nil
		default:
		}

		// Calculate batch end
		end := i + batchSize
		if end > len(chainworkErrors) {
			end = len(chainworkErrors)
		}
		batch := chainworkErrors[i:end]

		// Start transaction
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return errors.NewStorageError("failed to start transaction", err)
		}

		// Prepare update statement using ID
		var updateQuery string
		if dbType == "postgres" {
			updateQuery = `UPDATE blocks SET chain_work = $1 WHERE id = $2`
		} else {
			updateQuery = `UPDATE blocks SET chain_work = ? WHERE id = ?`
		}

		stmt, err := tx.Prepare(updateQuery)
		if err != nil {
			_ = tx.Rollback()
			return errors.NewStorageError("failed to prepare statement", err)
		}
		defer stmt.Close()

		// Execute updates for this batch
		for _, errRec := range batch {
			chainWorkHex := fmt.Sprintf("%064x", errRec.CorrectChainWork)

			// For PostgreSQL, we need to use bytea format
			var chainWorkValue interface{}
			if dbType == "postgres" {
				chainWorkBytes, _ := hex.DecodeString(chainWorkHex)
				chainWorkValue = chainWorkBytes
			} else {
				chainWorkValue = chainWorkHex
			}

			_, err := stmt.ExecContext(ctx, chainWorkValue, errRec.ID)
			if err != nil {
				_ = tx.Rollback()
				return errors.NewStorageError(fmt.Sprintf("failed to update block %s", errRec.Hash), err)
			}
			updateCount++
		}

		// Commit transaction
		if err := tx.Commit(); err != nil {
			return errors.NewStorageError("failed to commit transaction", err)
		}

		// Progress report
		percentage := float64(updateCount) / float64(totalUpdates) * 100
		fmt.Printf("Updated %d/%d blocks (%.1f%%)\n", updateCount, totalUpdates, percentage)
	}

	fmt.Printf("\n")
	fmt.Printf("✅ Successfully updated %d blocks\n", updateCount)
	return nil
}
