package sql_test

import (
	"context"
	"encoding/csv"
	"encoding/hex"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/bitcoin-sv/teranode/settings"
	bcsql "github.com/bitcoin-sv/teranode/stores/blockchain/sql"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/stretchr/testify/require"
)

func TestExportImportCSV(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewGoCoreLogger("sql-test")
	storeURL, _ := url.Parse("sqlitememory://testdb")
	cfg := &settings.Settings{DataFolder: os.TempDir(), ChainCfgParams: &chaincfg.MainNetParams}
	s, err := bcsql.New(logger, storeURL, cfg)
	require.NoError(t, err)

	// fetch genesis id
	var genesisID int64

	genesisHash := cfg.ChainCfgParams.GenesisHash.CloneBytes()

	err = s.GetDB().QueryRowContext(ctx, `SELECT id FROM blocks WHERE hash=$1`, genesisHash).Scan(&genesisID)
	require.NoError(t, err)

	// insert dummy blocks
	for i, h := range []string{"0a", "0b"} {
		hexHash, _ := hex.DecodeString(h)
		_, err := s.GetDB().ExecContext(ctx,
			`INSERT INTO blocks
              (parent_id,version,hash,previous_hash,merkle_root,block_time,
               n_bits,nonce,height,chain_work,tx_count,size_in_bytes,
               subtree_count,subtrees,coinbase_tx,invalid,mined_set,
               subtrees_set,peer_id)
            VALUES($1,1,$2,$3,$2,123, $4,1, $5,$4,1,1,1,$2,$2,false,false,false,'peer')`,
			genesisID, hexHash, genesisHash, []byte{0xff}, int64(i+1))
		require.NoError(t, err)
	}

	// export
	tmp := filepath.Join(os.TempDir(), "blocks.csv")
	defer os.Remove(tmp)
	require.NoError(t, s.ExportBlockchainCSV(ctx, tmp))

	// clear
	_, err = s.GetDB().ExecContext(ctx, `DELETE FROM blocks`)
	require.NoError(t, err)

	// import
	require.NoError(t, s.ImportBlockchainCSV(ctx, tmp))

	// verify
	rows, err := s.GetDB().QueryContext(ctx,
		`SELECT height, lower(hex(hash)) FROM blocks ORDER BY height`,
	)
	require.NoError(t, err)
	defer rows.Close()

	var got []struct {
		H   int64
		Hex string
	}

	for rows.Next() {
		var h int64

		var hx string

		require.NoError(t, rows.Scan(&h, &hx))
		got = append(got, struct {
			H   int64
			Hex string
		}{h, hx})
	}

	require.NoError(t, rows.Err())

	want := []struct {
		H   int64
		Hex string
	}{
		{0, hex.EncodeToString(genesisHash)},
		{1, "0a"}, {2, "0b"},
	}
	require.Equal(t, want, got)
}

// TestImportOnlyGenesis verifies import aborts when CSV has only genesis
func TestImportOnlyGenesis(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewGoCoreLogger("sql-test")
	storeURL, _ := url.Parse("sqlitememory://testdb2")
	cfg := &settings.Settings{DataFolder: os.TempDir(), ChainCfgParams: &chaincfg.MainNetParams}
	s, err := bcsql.New(logger, storeURL, cfg)
	require.NoError(t, err)

	tmp := filepath.Join(os.TempDir(), "onlygenesis.csv")
	defer os.Remove(tmp)
	// export only genesis
	require.NoError(t, s.ExportBlockchainCSV(ctx, tmp))
	// clear all blocks
	_, err = s.GetDB().ExecContext(ctx, `DELETE FROM blocks`)
	require.NoError(t, err)
	// import should abort
	err = s.ImportBlockchainCSV(ctx, tmp)
	require.Error(t, err)
	require.Contains(t, err.Error(), "contains only genesis block")
}

// TestImportGenesisMismatch verifies abort on mismatched genesis hash
func TestImportGenesisMismatch(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewGoCoreLogger("sql-test")
	storeURL, _ := url.Parse("sqlitememory://testdb3")
	cfg := &settings.Settings{DataFolder: os.TempDir(), ChainCfgParams: &chaincfg.MainNetParams}
	s, err := bcsql.New(logger, storeURL, cfg)
	require.NoError(t, err)

	// export to file
	tmp0 := filepath.Join(os.TempDir(), "orig.csv")
	defer os.Remove(tmp0)
	require.NoError(t, s.ExportBlockchainCSV(ctx, tmp0))
	// read and modify genesis hash
	rows, err := os.Open(tmp0)
	require.NoError(t, err)

	defer rows.Close()
	reader := csv.NewReader(rows)

	all, err := reader.ReadAll()
	require.NoError(t, err)
	// tamper hash field of first record
	all[1][1] = "deadbeef"
	// write to new file
	tmp1 := filepath.Join(os.TempDir(), "mismatch.csv")
	defer os.Remove(tmp1)

	f, err := os.Create(tmp1)
	require.NoError(t, err)

	writer := csv.NewWriter(f)
	require.NoError(t, writer.WriteAll(all))
	writer.Flush()
	f.Close()

	// clear DB
	_, err = s.GetDB().ExecContext(ctx, `DELETE FROM blocks`)
	require.NoError(t, err)
	// import should abort on mismatch
	err = s.ImportBlockchainCSV(ctx, tmp1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "genesis block hash mismatch")
}

// TestImportWithExistingGenesis verifies import succeeds when the DB already has the genesis block
func TestImportWithExistingGenesis(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewGoCoreLogger("sql-test")
	storeURL1, _ := url.Parse("sqlitememory://testdbA")
	cfg := &settings.Settings{DataFolder: os.TempDir(), ChainCfgParams: &chaincfg.MainNetParams}
	s1, err := bcsql.New(logger, storeURL1, cfg)
	require.NoError(t, err)

	// insert dummy blocks into first store
	var genesisID int64

	genesisHash := cfg.ChainCfgParams.GenesisHash.CloneBytes()
	require.NoError(t, s1.GetDB().QueryRowContext(ctx, `SELECT id FROM blocks WHERE hash=$1`, genesisHash).Scan(&genesisID))

	for i, h := range []string{"0a", "0b"} {
		hexHash, _ := hex.DecodeString(h)

		_, err := s1.GetDB().ExecContext(ctx,
			`INSERT INTO blocks
              (parent_id,version,hash,previous_hash,merkle_root,block_time,
               n_bits,nonce,height,chain_work,tx_count,size_in_bytes,
               subtree_count,subtrees,coinbase_tx,invalid,mined_set,
               subtrees_set,peer_id)
            VALUES($1,1,$2,$3,$2,123,$4,1,$5,$4,1,1,1,$2,$2,false,false,false,'peer')`,
			genesisID, hexHash, genesisHash, []byte{0xff}, int64(i+1))
		require.NoError(t, err)
	}

	// export to CSV
	tmp := filepath.Join(os.TempDir(), "blocks_exist.csv")
	defer os.Remove(tmp)
	require.NoError(t, s1.ExportBlockchainCSV(ctx, tmp))

	// new store with only genesis
	storeURL2, _ := url.Parse("sqlitememory://testdbB")
	s2, err := bcsql.New(logger, storeURL2, cfg)
	require.NoError(t, err)

	// import CSV into second store
	require.NoError(t, s2.ImportBlockchainCSV(ctx, tmp))

	// verify contents
	rRows, err := s2.GetDB().QueryContext(ctx,
		`SELECT height, lower(hex(hash)) FROM blocks ORDER BY height`)
	require.NoError(t, err)
	defer rRows.Close()

	var got []struct {
		H   int64
		Hex string
	}

	for rRows.Next() {
		var h int64
		var hx string
		require.NoError(t, rRows.Scan(&h, &hx))
		got = append(got, struct {
			H   int64
			Hex string
		}{h, hx})
	}
	require.NoError(t, rRows.Err())

	want := []struct {
		H   int64
		Hex string
	}{
		{0, hex.EncodeToString(genesisHash)},
		{1, "0a"}, {2, "0b"},
	}

	require.Equal(t, want, got)
}
