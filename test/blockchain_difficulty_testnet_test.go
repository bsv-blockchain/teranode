package test

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/services/blockchain/work"
	"github.com/bsv-blockchain/teranode/settings"
	sqlstore "github.com/bsv-blockchain/teranode/stores/blockchain/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/usql"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// BlockHeader represents a Bitcoin block header
type BlockHeader struct {
	Version    int32
	PrevBlock  chainhash.Hash
	MerkleRoot chainhash.Hash
	Timestamp  uint32
	Bits       uint32
	Nonce      uint32
	Hash       chainhash.Hash
	Height     uint32
	ChainWork  []byte
}

// parseBlockHeaders reads the binary file and parses block headers
func parseBlockHeaders(filename string, startHeight uint32) ([]*BlockHeader, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var headers []*BlockHeader
	height := startHeight

	for {
		var header BlockHeader
		header.Height = height

		// Read 80 bytes for each header
		buf := make([]byte, 80)
		n, err := io.ReadFull(file, buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if n != 80 {
			return nil, errors.NewProcessingError("invalid header size", nil)
		}

		// Parse the header fields (little-endian)
		header.Version = int32(binary.LittleEndian.Uint32(buf[0:4]))
		copy(header.PrevBlock[:], buf[4:36])
		copy(header.MerkleRoot[:], buf[36:68])
		header.Timestamp = binary.LittleEndian.Uint32(buf[68:72])
		header.Bits = binary.LittleEndian.Uint32(buf[72:76])
		header.Nonce = binary.LittleEndian.Uint32(buf[76:80])

		// Calculate the hash of the header
		hash := chainhash.DoubleHashB(buf)
		copy(header.Hash[:], hash)

		headers = append(headers, &header)
		height++
	}

	return headers, nil
}

// calculateChainWork calculates cumulative chain work
func calculateChainWork(headers []*BlockHeader) error {
	var prevChainWork *big.Int

	for i, header := range headers {
		// Calculate work for this block using the bits directly
		blockWork := work.CalcBlockWork(header.Bits)

		// Calculate cumulative chain work
		var chainWork *big.Int
		if i == 0 {
			chainWork = blockWork
		} else {
			chainWork = new(big.Int).Add(prevChainWork, blockWork)
		}

		// Store chain work as bytes
		header.ChainWork = chainWork.Bytes()
		prevChainWork = chainWork
	}

	return nil
}

// createSQLiteDB creates a new SQLite database with the blockchain schema
func createSQLiteDB(t *testing.T, tSettings *settings.Settings) (*usql.DB, string, func()) {
	// Create database in test directory for easy inspection
	dbName := "testnet_headers_test"
	testDir := "./test"
	dbPath := fmt.Sprintf("%s/%s.db", testDir, dbName)

	// Remove any existing database file
	os.Remove(dbPath)

	dbURL := fmt.Sprintf("sqlite:///%s", dbName)

	parsedURL, err := url.Parse(dbURL)
	require.NoError(t, err)

	logger := ulogger.NewErrorTestLogger(t)

	// Set DataFolder to test directory
	tSettings.DataFolder = testDir

	db, err := util.InitSQLDB(logger, parsedURL, tSettings)
	require.NoError(t, err)

	// Create the blockchain schema for SQLite
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS state (
		 key            VARCHAR(32) PRIMARY KEY
	    ,data           BLOB NOT NULL
        ,inserted_at    TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        ,updated_at     TEXT NULL
	  );
	`)
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS blocks (
		 id           INTEGER PRIMARY KEY AUTOINCREMENT
		,parent_id	  INTEGER
        ,version        INTEGER NOT NULL
	    ,hash           BLOB NOT NULL
	    ,previous_hash  BLOB NOT NULL
	    ,merkle_root    BLOB NOT NULL
        ,block_time		BIGINT NOT NULL
        ,n_bits         BLOB NOT NULL
        ,nonce          BIGINT NOT NULL
	    ,height         BIGINT NOT NULL
        ,chain_work     BLOB NOT NULL
		,tx_count       BIGINT NOT NULL
		,size_in_bytes  BIGINT NOT NULL
		,subtree_count  BIGINT NOT NULL
		,subtrees       BLOB NOT NULL
        ,coinbase_tx    BLOB NOT NULL
		,invalid	    BOOLEAN NOT NULL DEFAULT FALSE
	    ,mined_set 	    BOOLEAN NOT NULL DEFAULT FALSE
        ,subtrees_set   BOOLEAN NOT NULL DEFAULT FALSE
     	,peer_id	    TEXT NOT NULL
        ,inserted_at    TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
		,processed_at   TEXT NULL
	  );
	`)
	require.NoError(t, err)

	// Create indices
	_, err = db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS ux_blocks_hash ON blocks (hash);`)
	require.NoError(t, err)

	_, err = db.Exec(`CREATE INDEX IF NOT EXISTS idx_chain_work_id ON blocks (chain_work DESC, id ASC);`)
	require.NoError(t, err)

	cleanup := func() {
		db.Close()
		// Comment out the line below to keep the database for manual inspection
		os.Remove(dbPath)
	}

	return db, dbPath, cleanup
}

// insertHeaders inserts block headers into the database
func insertHeaders(t *testing.T, db *usql.DB, headers []*BlockHeader) {
	// Create a map for quick parent lookup
	hashToID := make(map[string]int64)

	for _, header := range headers {
		// Find parent ID
		var parentID sql.NullInt64
		prevHashStr := hex.EncodeToString(header.PrevBlock[:])
		if id, ok := hashToID[prevHashStr]; ok {
			parentID = sql.NullInt64{Int64: id, Valid: true}
		}

		// Prepare NBits (4 bytes)
		nBitsBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(nBitsBytes, header.Bits)

		// Insert the block
		var id int64
		err := db.QueryRow(`
			INSERT INTO blocks (
				parent_id, version, hash, previous_hash, merkle_root,
				block_time, n_bits, nonce, height, chain_work,
				tx_count, size_in_bytes, subtree_count, subtrees, coinbase_tx,
				invalid, mined_set, subtrees_set, peer_id
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			RETURNING id
		`,
			parentID,
			header.Version,
			header.Hash[:],
			header.PrevBlock[:],
			header.MerkleRoot[:],
			int64(header.Timestamp),
			nBitsBytes,
			int64(header.Nonce),
			int64(header.Height),
			header.ChainWork,
			0,        // tx_count (placeholder)
			0,        // size_in_bytes (placeholder)
			0,        // subtree_count (placeholder)
			[]byte{}, // subtrees (empty)
			[]byte{}, // coinbase_tx (empty)
			false,    // invalid
			true,     // mined_set
			true,     // subtrees_set
			"test",   // peer_id
		).Scan(&id)
		require.NoError(t, err)

		// Store the ID for future parent lookups
		hashStr := hex.EncodeToString(header.Hash[:])
		hashToID[hashStr] = id
	}
}

func TestGetNextWorkRequiredTestnet(t *testing.T) {
	// Parse the binary headers file containing only the necessary blocks for testing
	// emergency difficulty calculation around blocks 1602682-1602683
	// Original file downloaded from: https://api.whatsonchain.com/v1/bsv/test/block/headers/1600001_1610000_headers.bin
	// Reduced to blocks 1602530-1602710 (181 blocks) for efficiency
	headers, err := parseBlockHeaders("testnet_headers_1602530_1602710.bin", 1602530)
	require.NoError(t, err)
	require.Len(t, headers, 181)

	// Calculate chain work for all headers
	err = calculateChainWork(headers)
	require.NoError(t, err)

	// Use testnet settings
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams = &chaincfg.TestNetParams

	// Create SQLite database
	// Note: To keep the database file for manual inspection, comment out the defer cleanup() line below
	// The database will be created as ./testnet_headers_test.db in the test directory
	db, _, cleanup := createSQLiteDB(t, tSettings)
	defer cleanup() // Comment this line to keep the database file for manual inspection

	// Insert headers into database
	t.Log("Inserting headers into database...")
	insertHeaders(t, db, headers)

	// Create blockchain store (using the same database)
	dbURL, err := url.Parse("sqlite:///testnet_headers_test")
	require.NoError(t, err)

	logger := ulogger.NewErrorTestLogger(t)

	store, err := sqlstore.New(logger, dbURL, tSettings)
	require.NoError(t, err)
	defer store.Close()

	// Start blockchain service
	t.Log("Starting blockchain service...")
	// Use port 0 to let the OS assign an available port
	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	// Get the actual port assigned by the OS
	port := listener.Addr().(*net.TCPAddr).Port

	grpcServer := grpc.NewServer()
	ctx := context.Background()
	blockchainService, err := blockchain.New(ctx, logger, tSettings, store, nil)
	require.NoError(t, err)

	blockchain_api.RegisterBlockchainAPIServer(grpcServer, blockchainService)

	// Start the server in a goroutine
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			t.Logf("Server stopped: %v", err)
		}
	}()
	defer grpcServer.Stop()

	// Give the server time to start
	time.Sleep(100 * time.Millisecond)

	// Create gRPC client
	conn, err := grpc.Dial(fmt.Sprintf("localhost:%d", port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	client := blockchain_api.NewBlockchainAPIClient(conn)

	// Test 1: Normal difficulty calculation for block 1602682
	t.Run("Normal Difficulty Calculation", func(t *testing.T) {
		// Block 1602681 is the previous block
		prevBlockIndex := 1602681 - 1602530
		prevBlockHash := headers[prevBlockIndex].Hash

		req := &blockchain_api.GetNextWorkRequiredRequest{
			PreviousBlockHash: prevBlockHash[:],
			CurrentBlockTime:  int64(headers[prevBlockIndex+1].Timestamp), // Use actual timestamp from block 1602682
		}

		resp, err := client.GetNextWorkRequired(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)

		// The response should be a normal difficulty (not minimum)
		respBits := binary.LittleEndian.Uint32(resp.Bits)
		t.Logf("Block 1602682 - Normal difficulty: 0x%08x", respBits)

		// Verify it's not the minimum difficulty
		require.NotEqual(t, uint32(0x1d00ffff), respBits, "Should not be minimum difficulty for normal block")
	})

	// Test 2: Emergency difficulty calculation for block 1602683
	t.Run("Emergency Difficulty Calculation", func(t *testing.T) {
		// Block 1602682 is the previous block
		prevBlockIndex := 1602682 - 1602530
		prevBlockHash := headers[prevBlockIndex].Hash
		prevBlockTime := headers[prevBlockIndex].Timestamp

		// For emergency difficulty, the new block time should be > 20 minutes after previous
		// In testnet, target spacing is 10 minutes, so 2 * 10 * 60 = 1200 seconds
		emergencyBlockTime := int64(prevBlockTime) + 1201 // Just over 20 minutes

		req := &blockchain_api.GetNextWorkRequiredRequest{
			PreviousBlockHash: prevBlockHash[:],
			CurrentBlockTime:  emergencyBlockTime,
		}

		resp, err := client.GetNextWorkRequired(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)

		// The response should be the minimum difficulty (0x1d00ffff for testnet)
		respBits := binary.LittleEndian.Uint32(resp.Bits)
		t.Logf("Block 1602683 - Emergency difficulty: 0x%08x", respBits)

		// Verify it IS the minimum difficulty
		require.Equal(t, uint32(0x1d00ffff), respBits, "Should be minimum difficulty for emergency block")
	})

	// Test 3: Verify actual block 1602683 timestamps
	t.Run("Verify Actual Block Timestamps", func(t *testing.T) {
		block1602682 := headers[1602682-1602530]
		block1602683 := headers[1602683-1602530]

		timeDiff := int64(block1602683.Timestamp) - int64(block1602682.Timestamp)
		t.Logf("Actual time difference between blocks: %d seconds (%d minutes)", timeDiff, timeDiff/60)

		// Check if it was actually more than 20 minutes
		if timeDiff > 1200 {
			t.Log("Block 1602683 was indeed mined more than 20 minutes after 1602682")

			// Verify the actual difficulty used
			actualBits := block1602683.Bits
			t.Logf("Actual bits used in block 1602683: 0x%08x", actualBits)

			// On testnet, emergency difficulty should be 0x1d00ffff
			if actualBits == 0x1d00ffff {
				t.Log("✓ Block 1602683 correctly used emergency difficulty")
			}
		}
	})
}
