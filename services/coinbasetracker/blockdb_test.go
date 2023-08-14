package coinbasetracker

import (
	"database/sql"
	"fmt"
	"log"
	"testing"
)

func TestBlockDb(t *testing.T) {
	// Initialize database
	// any reason this needs to be a real file ?
	//db, err := sql.Open("sqlite3", "./testblocks.db")
	db, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	if err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	}
	defer db.Close()

	// Create blocks table
	createTableQuery := `
		CREATE TABLE IF NOT EXISTS blocks (
			block_id INTEGER PRIMARY KEY,
			block_hash TEXT NOT NULL,
			previous_block_hash TEXT,
			height INTEGER NOT NULL,
			status Integer NOT NULL DEFAULT 0
		);
	`
	_, err = db.Exec(createTableQuery)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	// Insert 100 blocks
	insertBlockQuery := `
		INSERT INTO blocks (block_hash, previous_block_hash, height)
		VALUES (?, ?, ?)
	`
	stmt, err := db.Prepare(insertBlockQuery)
	if err != nil {
		log.Fatalf("Failed to prepare statement: %v", err)
	}

	for i := 1; i <= 100; i++ {
		// For simplicity, we'll use i as height, and "hash"+i as block_hash.
		// The previous_block_hash is "hash"+(i-1) except for the first block.
		blockHash := fmt.Sprintf("hash%d", i)
		previousBlockHash := fmt.Sprintf("hash%d", i-1)
		if i == 1 {
			previousBlockHash = "" // Genesis block has no previous hash
		}

		_, err := stmt.Exec(blockHash, previousBlockHash, i)
		if err != nil {
			log.Fatalf("Failed to insert block %d: %v", i, err)
		}
	}
	// insert an orphaned fork
	_, err = stmt.Exec("hash32a", "hash31", 32)
	if err != nil {
		log.Fatalf("Failed to insert block %d: %v", 32, err)
	}

	fmt.Println("100 blocks inserted successfully!")
}

/*

WITH RECURSIVE ChainBlocks AS (
    SELECT block_id, block_hash, previous_block_hash, height
    FROM blocks
    WHERE block_id = (SELECT MAX(height) - 50 FROM blocks)
    UNION ALL
    SELECT b.block_id, b.block_hash, b.previous_block_hash, b.height
    FROM blocks b
    JOIN ChainBlocks cb ON b.block_hash = cb.previous_block_hash
)

UPDATE blocks
SET status = '1'
WHERE height <= (SELECT MAX(height) - 50 FROM blocks)
AND block_id IN (SELECT block_id FROM ChainBlocks);

	INSERT INTO blocks (block_hash, previous_block_hash, height) VALUES ('hash32a','hash31' , 32);

*/
