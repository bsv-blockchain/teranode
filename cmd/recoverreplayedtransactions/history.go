package recoverreplayedtransactions

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sync"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtree "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/pkg/replayrecovery"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"golang.org/x/sys/unix"
	_ "modernc.org/sqlite"
)

// HistoryOptions bounds each archive read and block's in-memory hash list.
// Zero limits default to 64 MiB per blob and 1,000,000 transactions per block.
// An archive exceeding a bound is a reported gap, never evidence of absence.
type HistoryOptions struct {
	// Progress runs synchronously after each scanned block; it must not call LocalHistory methods.
	Guard                   replayrecovery.Guard `json:"-"`
	TargetsPath             string
	GenesisActivationHeight uint32
	Unconfirmed             replayrecovery.TransactionReader `json:"-"`
	Progress                func(HistoryCoverage)            `json:"-"`
	StartHeight, EndHeight  uint32
	MaxBlobBytes            int64
	MaxBlockTransactions    int
}
type HistoryCoverage struct {
	StartHeight uint32             `json:"start_height"`
	EndHeight   uint32             `json:"end_height"`
	Scanned     uint64             `json:"scanned"`
	GapCount    uint32             `json:"gap_count"`
	Complete    bool               `json:"complete"`
	Tip         replayrecovery.Tip `json:"tip"`
}

// HistoryChain exposes only canonical reads; discovery must not construct a
// production store that starts schema updates or background writers.
type HistoryChain interface {
	GetBestBlockHeader(context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error)
	GetBlockInChainByHeightHash(context.Context, uint32, *chainhash.Hash) (*model.Block, bool, error)
}

// LocalHistory indexes target transactions and authenticated spenders on disk.
// Newly discovered dependencies cause another bounded pass before sealing.
type LocalHistory struct {
	db       *sql.DB
	file     *os.File
	chain    HistoryChain
	archive  blob.Store
	options  HistoryOptions
	mu       sync.RWMutex
	coverage HistoryCoverage
	built    bool
}

func NewHistory(ctx context.Context, path string, chain HistoryChain, archive blob.Store, o HistoryOptions) (_ *LocalHistory, err error) {
	if o.MaxBlobBytes == 0 {
		o.MaxBlobBytes = 64 << 20
	}
	if o.MaxBlockTransactions == 0 {
		o.MaxBlockTransactions = 1000000
	}
	if o.Guard == nil || chain == nil || archive == nil || o.EndHeight < o.StartHeight || o.MaxBlobBytes < 64 || o.MaxBlobBytes > 256<<20 || o.MaxBlockTransactions < 1 || o.MaxBlockTransactions > 4000000 {
		return nil, errors.NewProcessingError("invalid history configuration")
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	parent, err := os.Stat(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	if !parent.IsDir() || parent.Mode().Perm()&0077 != 0 {
		return nil, errors.NewProcessingError("history directory must be private (0700)")
	}
	fd, err := unix.Open(path, unix.O_CREAT|unix.O_EXCL|unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	defer func() {
		if err != nil {
			_ = file.Close()
		}
	}()
	if err = file.Close(); err != nil {
		return nil, err
	}
	lockFD, lockErr := unix.Open(path+".lock", unix.O_CREAT|unix.O_EXCL|unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if lockErr != nil {
		return nil, lockErr
	}
	file = os.NewFile(uintptr(lockFD), path+".lock")
	if err = unix.Flock(lockFD, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return nil, err
	}
	u := url.URL{Scheme: "file", Path: path, RawQuery: "mode=rw"}
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = db.Close()
		}
	}()
	db.SetMaxOpenConns(1)
	_, err = db.ExecContext(ctx, `PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL; PRAGMA cache_size=-4096;
 CREATE TABLE blocks(hash TEXT PRIMARY KEY,height INTEGER NOT NULL,header BLOB NOT NULL,leaves BLOB NOT NULL);
 CREATE TABLE transactions(txid TEXT NOT NULL,block TEXT NOT NULL,position INTEGER NOT NULL,raw BLOB NOT NULL,PRIMARY KEY(txid,block));
 CREATE TABLE targets(id TEXT PRIMARY KEY);
 CREATE TABLE spends(parent TEXT,vout INTEGER,child TEXT,PRIMARY KEY(parent,vout,child));
 CREATE TABLE headers(height INTEGER PRIMARY KEY,hash TEXT,header BLOB);
 CREATE TABLE gaps(height INTEGER PRIMARY KEY,reason TEXT NOT NULL);
 CREATE TABLE coverage(data BLOB NOT NULL);`)
	if err != nil {
		return nil, err
	}
	if o.TargetsPath != "" {
		if _, err = db.ExecContext(ctx, "ATTACH DATABASE ? AS census", o.TargetsPath); err != nil {
			return nil, err
		}
		if _, err = db.ExecContext(ctx, "INSERT INTO targets SELECT id FROM census.targets"); err != nil {
			return nil, err
		}
	}
	return &LocalHistory{db: db, file: file, chain: chain, archive: archive, options: o, coverage: HistoryCoverage{StartHeight: o.StartHeight, EndHeight: o.EndHeight}}, nil
}
func (h *LocalHistory) Close() error {
	dbErr, lockErr := h.db.Close(), h.file.Close()
	if dbErr == nil {
		return lockErr
	}
	if lockErr == nil {
		return dbErr
	}
	return errors.Join(dbErr, lockErr)
}

func (h *LocalHistory) Coverage() HistoryCoverage {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.coverage
}
func (h *LocalHistory) Gaps(ctx context.Context, visit func(uint32, string) error) error {
	rows, err := h.db.QueryContext(ctx, "SELECT height,reason FROM gaps ORDER BY height")
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var height uint32
		var reason string
		if err = rows.Scan(&height, &reason); err != nil {
			return err
		}
		if err = visit(height, reason); err != nil {
			return err
		}
	}
	return rows.Err()
}
func (h *LocalHistory) checkTip(ctx context.Context, tip replayrecovery.Tip) error {
	if h.chain == nil || h.options.Guard == nil {
		return errors.NewProcessingError("live history guard unavailable")
	}
	if err := h.options.Guard(ctx); err != nil {
		return err
	}
	header, meta, err := h.chain.GetBestBlockHeader(ctx)
	if err != nil {
		return err
	}
	if header == nil || meta == nil || header.Hash().String() != tip.Hash || meta.Height != tip.Height {
		return errors.NewProcessingError("local canonical tip changed or disagrees")
	}
	return nil
}
func (h *LocalHistory) Build(ctx context.Context, tip replayrecovery.Tip) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.built {
		if h.coverage.Tip != tip {
			return errors.NewProcessingError("history index bound to different tip")
		}
		return h.checkTip(ctx, tip)
	}
	if h.coverage.Scanned != 0 {
		return errors.NewProcessingError("interrupted history build requires fresh index")
	}
	if h.options.EndHeight > tip.Height {
		return errors.NewProcessingError("history range extends beyond tip")
	}
	if err := h.checkTip(ctx, tip); err != nil {
		return err
	}
	tipHash, err := chainhash.NewHashFromStr(tip.Hash)
	if err != nil {
		return err
	}
	h.coverage.Tip = tip
	// Authenticate every ancestor before accepting any body, including across gaps.
	expected := tip.Hash
	for height := int64(tip.Height); height >= int64(h.options.StartHeight); height-- {
		if err = h.checkTip(ctx, tip); err != nil {
			return err
		}
		block, invalid, e := h.chain.GetBlockInChainByHeightHash(ctx, uint32(height), tipHash) // #nosec G115 -- loop bounds are uint32 heights.
		if e != nil || invalid || block == nil || block.Header == nil || block.Header.HashPrevBlock == nil || block.Hash().String() != expected {
			return errors.NewProcessingError("canonical header ancestry unavailable at %d", height)
		}
		if _, err = h.db.ExecContext(ctx, "INSERT INTO headers VALUES(?,?,?)", height, expected, block.Header.Bytes()); err != nil {
			return err
		}
		expected = block.Header.HashPrevBlock.String()
	}
	for {
		var before int
		if err = h.db.QueryRowContext(ctx, "SELECT count(*) FROM targets").Scan(&before); err != nil {
			return err
		}
		if err = h.expandUnconfirmed(ctx); err != nil {
			return err
		}
		if _, err = h.db.ExecContext(ctx, "DELETE FROM transactions; DELETE FROM spends; DELETE FROM blocks; DELETE FROM gaps"); err != nil {
			return err
		}
		h.coverage.Scanned = 0
		h.coverage.GapCount = 0
		for height := uint64(h.options.StartHeight); height <= uint64(h.options.EndHeight); height++ {
			if err = h.checkTip(ctx, tip); err != nil {
				return err
			}
			block, invalid, readErr := h.chain.GetBlockInChainByHeightHash(ctx, uint32(height), tipHash) // #nosec G115 -- loop bounds are uint32 heights.
			var pinned string
			if err = h.db.QueryRowContext(ctx, "SELECT hash FROM headers WHERE height=?", height).Scan(&pinned); err != nil {
				return err
			}
			if readErr == nil && (invalid || block == nil || block.Header == nil || block.Hash().String() != pinned) {
				return errors.NewProcessingError("canonical block ancestry changed")
			}
			if readErr == nil {
				readErr = h.indexBlock(ctx, block, uint32(height)) // #nosec G115 -- height is bounded by uint32 EndHeight.
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if readErr != nil {
				if _, err = h.db.ExecContext(ctx, "INSERT INTO gaps VALUES(?,?)", height, readErr.Error()); err != nil {
					return err
				}
				h.coverage.GapCount++
			}
			h.coverage.Scanned++
			if h.options.Progress != nil {
				h.options.Progress(h.coverage)
			}
		}
		var after int
		if err = h.db.QueryRowContext(ctx, "SELECT count(*) FROM targets").Scan(&after); err != nil {
			return err
		}
		if before == after {
			break
		}
	}
	if h.options.TargetsPath != "" {
		if _, err = h.db.ExecContext(ctx, "INSERT OR IGNORE INTO census.targets SELECT id FROM targets"); err != nil {
			return err
		}
	}
	if err = h.checkTip(ctx, tip); err != nil {
		return err
	}
	h.coverage.Complete = h.coverage.GapCount == 0
	digest, err := historyDigest(ctx, h.db)
	if err != nil {
		return err
	}
	data, err := json.Marshal(historySeal{Version: 2, Options: h.options, Coverage: h.coverage, Digest: historySealDigest(digest, h.options, h.coverage)})
	if err != nil {
		return err
	}
	if _, err = h.db.ExecContext(ctx, "INSERT INTO coverage(data) VALUES(?)", data); err != nil {
		return err
	}
	h.built = true
	return nil
}
func (h *LocalHistory) readBlob(ctx context.Context, key []byte, kind fileformat.FileType) ([]byte, error) {
	r, err := h.archive.GetIoReader(ctx, key, kind)
	if err != nil {
		return nil, errors.NewProcessingError("archive %s unavailable", kind)
	}
	defer r.Close()
	b, err := io.ReadAll(io.LimitReader(r, h.options.MaxBlobBytes+1))
	if err != nil {
		return nil, errors.NewProcessingError("archive read failed")
	}
	if int64(len(b)) > h.options.MaxBlobBytes {
		return nil, errors.NewProcessingError("archive exceeds byte limit")
	}
	return b, nil
}
func historyPair(a, b chainhash.Hash) chainhash.Hash {
	var p [64]byte
	copy(p[:32], a[:])
	copy(p[32:], b[:])
	return chainhash.DoubleHashH(p[:])
}
func historyRoot(leaves []chainhash.Hash) (chainhash.Hash, error) {
	if len(leaves) == 0 {
		return chainhash.Hash{}, errors.NewProcessingError("empty merkle tree")
	}
	level := append([]chainhash.Hash(nil), leaves...)
	for len(level) > 1 {
		next := make([]chainhash.Hash, 0, (len(level)+1)/2)
		for i := 0; i < len(level); i += 2 {
			right := level[i]
			if i+1 < len(level) {
				right = level[i+1]
				if right == level[i] {
					return chainhash.Hash{}, errors.NewProcessingError("mutated merkle tree")
				}
			}
			next = append(next, historyPair(level[i], right))
		}
		level = next
	}
	return level[0], nil
}

// subtreeLeaves parses the fixed-width archive without trusting allocation counts
// or its cached root hash. Fees/size metadata do not authenticate membership.
func (h *LocalHistory) subtreeLeaves(data []byte, key *chainhash.Hash) ([]chainhash.Hash, error) {
	if len(data) < 64 || !bytes.Equal(data[:32], key[:]) {
		return nil, errors.NewProcessingError("subtree header mismatch")
	}
	count := binary.LittleEndian.Uint64(data[48:56])
	if count == 0 || count > uint64(h.options.MaxBlockTransactions) || count > uint64((len(data)-64)/48) { // #nosec G115 -- constructor validates positive MaxBlockTransactions; len(data) >= 64 above.
		return nil, errors.NewProcessingError("subtree leaf count exceeds bounds")
	}
	end := 56 + int(count)*48 // #nosec G115 -- count <= (len(data)-64)/48 above, so offset arithmetic fits int.
	conflicts := binary.LittleEndian.Uint64(data[end : end+8])
	if conflicts > uint64((len(data)-end-8)/32) || uint64(len(data)-end-8) != conflicts*32 { // #nosec G115 -- bounded count guarantees end+8 <= len(data); remainder is nonnegative.
		return nil, errors.NewProcessingError("malformed subtree conflicts")
	}
	leaves := make([]chainhash.Hash, int(count)) // #nosec G115 -- count <= (len(data)-64)/48, which fits int.
	for i := range leaves {
		copy(leaves[i][:], data[56+i*48:88+i*48])
	}
	root, err := historyRoot(leaves)
	if err != nil {
		return nil, err
	}
	if root != *key {
		return nil, errors.NewProcessingError("subtree root mismatch")
	}
	return leaves, nil
}
func (h *LocalHistory) indexBlock(ctx context.Context, b *model.Block, height uint32) (err error) {
	if b.Header == nil || b.Header.HashPrevBlock == nil || b.Header.HashMerkleRoot == nil || b.CoinbaseTx == nil || !b.CoinbaseTx.IsCoinbase() || b.TransactionCount == 0 || b.TransactionCount > uint64(h.options.MaxBlockTransactions) { // #nosec G115 -- constructor validates MaxBlockTransactions in [1,4000000].
		return errors.NewProcessingError("block metadata missing or exceeds transaction bound")
	}
	tx, err := h.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	block := b.Hash().String()
	leaves := make([]chainhash.Hash, 0, int(b.TransactionCount)) // #nosec G115 -- transaction count is bounded by MaxBlockTransactions <= 4000000 above.
	insert := func(transaction *bt.Tx) error {
		var target int
		if e := tx.QueryRowContext(ctx, "SELECT count(*) FROM targets WHERE id=?", transaction.TxID()).Scan(&target); e != nil {
			return e
		}
		keep := target > 0
		if !transaction.IsCoinbase() {
			for _, input := range transaction.Inputs {
				parent := input.PreviousTxIDStr()
				if target > 0 {
					if _, e := tx.ExecContext(ctx, "INSERT OR IGNORE INTO targets VALUES(?)", parent); e != nil {
						return e
					}
				}
				var n int
				if e := tx.QueryRowContext(ctx, "SELECT count(*) FROM targets WHERE id=?", parent).Scan(&n); e != nil {
					return e
				}
				if n > 0 {
					keep = true
					if _, e := tx.ExecContext(ctx, "INSERT OR IGNORE INTO spends VALUES(?,?,?)", parent, input.PreviousTxOutIndex, transaction.TxID()); e != nil {
						return e
					}
				}
			}
		}
		if !keep {
			return nil
		}
		_, e := tx.ExecContext(ctx, "INSERT OR IGNORE INTO transactions(txid,block,position,raw) VALUES(?,?,?,?)", transaction.TxID(), block, len(leaves), transaction.Bytes())
		return e
	}
	if len(b.Subtrees) == 0 {
		if b.TransactionCount != 1 {
			return errors.NewProcessingError("missing subtrees")
		}
		if err = insert(b.CoinbaseTx); err != nil {
			return err
		}
		leaves = append(leaves, *b.CoinbaseTx.TxIDChainHash())
	}
	for subtreeIndex, key := range b.Subtrees {
		if err = ctx.Err(); err != nil {
			return err
		}
		if key == nil {
			return errors.NewProcessingError("nil subtree reference")
		}
		data, e := h.readBlob(ctx, key[:], fileformat.FileTypeSubtree)
		if e != nil {
			return e
		}
		nodes, e := h.subtreeLeaves(data, key)
		if e != nil {
			return e
		}
		if len(leaves)+len(nodes) > h.options.MaxBlockTransactions {
			return errors.NewProcessingError("block exceeds transaction bound")
		}
		start := 0
		if subtreeIndex == 0 {
			if nodes[0] != *subtree.CoinbasePlaceholderHash {
				return errors.NewProcessingError("missing coinbase placeholder")
			}
			if err = insert(b.CoinbaseTx); err != nil {
				return err
			}
			leaves = append(leaves, *b.CoinbaseTx.TxIDChainHash())
			start = 1
		}
		if start == len(nodes) {
			continue
		}
		stream, e := h.archive.GetIoReader(ctx, key[:], fileformat.FileTypeSubtreeData)
		if e != nil {
			return e
		}
		defer stream.Close()
		reader := bufio.NewReaderSize(stream, 64<<10)
		for i := start; i < len(nodes); i++ {
			if err = ctx.Err(); err != nil {
				return err
			}
			transaction, parseErr := replayrecovery.ReadBoundedTransactionStream(reader, h.options.MaxBlobBytes)
			if parseErr != nil {
				return errors.NewProcessingError("archived transaction missing or malformed")
			}
			if *transaction.TxIDChainHash() != nodes[i] {
				return errors.NewProcessingError("archived transaction identity mismatch")
			}
			if err = insert(transaction); err != nil {
				return err
			}
			leaves = append(leaves, nodes[i])
		}
		if _, e := reader.ReadByte(); e != io.EOF {
			return errors.NewProcessingError("trailing archive transaction bytes")
		}
		if err = stream.Close(); err != nil {
			return err
		}
	}
	if uint64(len(leaves)) != b.TransactionCount {
		return errors.NewProcessingError("block transaction count mismatch")
	}
	root, err := historyRoot(leaves)
	if err != nil {
		return err
	}
	if root != *b.Header.HashMerkleRoot {
		return errors.NewProcessingError("block merkle commitment mismatch")
	}
	var retained int
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM transactions WHERE block=?", block).Scan(&retained); err != nil {
		return err
	}
	if retained == 0 {
		return tx.Commit()
	}
	hashes := make([]byte, 0, len(leaves)*32)
	for _, hash := range leaves {
		hashes = append(hashes, hash[:]...)
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO blocks(hash,height,header,leaves) VALUES(?,?,?,?)", block, height, b.Header.Bytes(), hashes); err != nil {
		return err
	}
	return tx.Commit()
}
func (h *LocalHistory) Lookup(ctx context.Context, id string, tip replayrecovery.Tip) (replayrecovery.Inclusion, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	p := replayrecovery.Inclusion{}
	if !h.built || tip != h.coverage.Tip {
		return p, errors.NewProcessingError("history index unavailable for tip")
	}
	if h.chain != nil {
		if err := h.checkTip(ctx, tip); err != nil {
			return p, err
		}
	}
	var raw, leaves []byte
	err := h.db.QueryRowContext(ctx, `SELECT t.raw,t.position,b.header,b.height,b.leaves FROM transactions t JOIN blocks b ON t.block=b.hash WHERE t.txid=? AND length(t.raw)<=? AND length(b.header)=80 AND length(b.leaves) BETWEEN 32 AND ? ORDER BY b.height DESC LIMIT 1`, id, h.options.MaxBlobBytes, h.options.MaxBlockTransactions*32).Scan(&raw, &p.Index, &p.Header, &p.Height, &leaves)
	if err != nil {
		return p, errors.NewProcessingError("transaction history unavailable in range %d..%d (%d gaps)", h.coverage.StartHeight, h.coverage.EndHeight, h.coverage.GapCount)
	}
	if p.Height > tip.Height {
		return p, errors.NewProcessingError("historical inclusion beyond requested tip")
	}
	p.RawTx = hex.EncodeToString(raw)
	if len(leaves)%32 != 0 || len(leaves)/32 > h.options.MaxBlockTransactions || int(p.Index) >= len(leaves)/32 {
		return p, errors.NewProcessingError("corrupt history index")
	}
	level := make([]chainhash.Hash, len(leaves)/32)
	for i := range level {
		copy(level[i][:], leaves[i*32:(i+1)*32])
	}
	index := int(p.Index)
	for len(level) > 1 {
		other := index ^ 1
		if other >= len(level) {
			other = index
		}
		p.MerkleBranch = append(p.MerkleBranch, level[other].String())
		next := make([]chainhash.Hash, 0, (len(level)+1)/2)
		for i := 0; i < len(level); i += 2 {
			r := i + 1
			if r == len(level) {
				r = i
			}
			next = append(next, historyPair(level[i], level[r]))
		}
		level = next
		index /= 2
	}
	if _, err = historyTransaction(id, p.RawTx); err != nil {
		return p, err
	}
	if err = h.authenticateHeader(ctx, p.Height, p.Header, tip); err != nil {
		return p, err
	}
	if _, err = replayrecovery.VerifyInclusion(id, p); err != nil {
		return p, err
	}
	return p, nil
}

type historySeal struct {
	Digest   string          `json:"digest"`
	Version  int             `json:"version"`
	Options  HistoryOptions  `json:"options"`
	Coverage HistoryCoverage `json:"coverage"`
}

// OpenHistory checks index integrity and reattaches live guarded canonical reads.
func OpenHistory(path string, chain HistoryChain, options HistoryOptions) (_ *LocalHistory, err error) {
	path, err = filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	parent, err := os.Stat(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	if !parent.IsDir() || parent.Mode().Perm()&0077 != 0 {
		return nil, errors.NewProcessingError("history directory must be private (0700)")
	}
	for _, p := range []string{path, path + ".lock"} {
		st, e := os.Lstat(p)
		if e != nil {
			return nil, e
		}
		if !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 {
			return nil, errors.NewProcessingError("history files must be private regular files")
		}
	}
	fd, err := unix.Open(path+".lock", unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), path+".lock")
	defer func() {
		if err != nil {
			_ = file.Close()
		}
	}()
	if err = unix.Flock(fd, unix.LOCK_SH|unix.LOCK_NB); err != nil {
		return nil, errors.NewProcessingError("history index is being built", err)
	}
	u := url.URL{Scheme: "file", Path: path, RawQuery: "mode=ro"}
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = db.Close()
		}
	}()
	db.SetMaxOpenConns(1)
	var data []byte
	if err = db.QueryRow("SELECT data FROM coverage WHERE length(data)<=65536").Scan(&data); err != nil {
		return nil, errors.NewProcessingError("history index incomplete", err)
	}
	var seal historySeal
	if err = json.Unmarshal(data, &seal); err != nil {
		return nil, err
	}
	o := seal.Options
	c := seal.Coverage
	if seal.Version != 2 || o.EndHeight < o.StartHeight || o.MaxBlockTransactions < 1 || o.MaxBlockTransactions > 4000000 || o.MaxBlobBytes < 64 || o.MaxBlobBytes > 256<<20 || c.StartHeight != o.StartHeight || c.EndHeight != o.EndHeight || c.Scanned != uint64(o.EndHeight)-uint64(o.StartHeight)+1 || uint64(c.GapCount) > c.Scanned || c.Complete != (c.GapCount == 0) {
		return nil, errors.NewProcessingError("invalid history index seal")
	}
	digest, e := historyDigest(context.Background(), db)
	if e != nil {
		return nil, e
	}
	if historySealDigest(digest, o, c) != seal.Digest {
		return nil, errors.NewProcessingError("history index integrity mismatch")
	}
	o.Guard = options.Guard
	o.Unconfirmed = options.Unconfirmed
	if chain == nil || o.Guard == nil {
		return nil, errors.NewProcessingError("live history chain and guard required")
	}
	history := &LocalHistory{db: db, file: file, chain: chain, options: o, coverage: c, built: true}
	if err = history.authenticateStoredHeaders(context.Background()); err != nil {
		return nil, err
	}
	return history, nil
}

func (h *LocalHistory) Tip(ctx context.Context) (replayrecovery.Tip, error) {
	if err := h.checkTip(ctx, h.coverage.Tip); err != nil {
		return replayrecovery.Tip{}, err
	}
	return h.coverage.Tip, nil
}

// authenticateHeader binds the proof to the authenticated, pinned header path,
// then checks live point membership and the live tip at the read boundary.
func (h *LocalHistory) authenticateHeader(ctx context.Context, height uint32, header []byte, tip replayrecovery.Tip) error {
	if err := h.checkTip(ctx, tip); err != nil {
		return err
	}
	var pinned []byte
	if err := h.db.QueryRowContext(ctx, "SELECT header FROM headers WHERE height=?", height).Scan(&pinned); err != nil {
		return err
	}
	if !bytes.Equal(header, pinned) {
		return errors.NewProcessingError("history header differs from pinned ancestry")
	}
	tipHash, err := chainhash.NewHashFromStr(tip.Hash)
	if err != nil {
		return err
	}
	b, invalid, err := h.chain.GetBlockInChainByHeightHash(ctx, height, tipHash)
	if err != nil || invalid || b == nil || b.Header == nil || !bytes.Equal(header, b.Header.Bytes()) {
		return errors.NewProcessingError("canonical block membership changed")
	}
	return h.checkTip(ctx, tip)
}

func (h *LocalHistory) authenticateStoredHeaders(ctx context.Context) error {
	tip := h.coverage.Tip
	if err := h.checkTip(ctx, tip); err != nil {
		return err
	}
	rows, err := h.db.QueryContext(ctx, "SELECT height,hash,header FROM headers ORDER BY height DESC")
	if err != nil {
		return err
	}
	defer rows.Close()
	expectedHash := tip.Hash
	expectedHeight := int64(tip.Height)
	for rows.Next() {
		var height int64
		var hash string
		var header []byte
		if err = rows.Scan(&height, &hash, &header); err != nil {
			return err
		}
		if expectedHeight < int64(h.coverage.StartHeight) || height != expectedHeight || hash != expectedHash || len(header) != 80 || chainhash.DoubleHashH(header).String() != expectedHash {
			return errors.NewProcessingError("stored canonical ancestry invalid")
		}
		var previous chainhash.Hash
		copy(previous[:], header[4:36])
		expectedHash = previous.String()
		expectedHeight--
	}
	if err = rows.Err(); err != nil {
		return err
	}
	if expectedHeight != int64(h.coverage.StartHeight)-1 {
		return errors.NewProcessingError("stored canonical ancestry incomplete")
	}
	return h.checkTip(ctx, tip)
}

func historyTransaction(id, raw string) (*bt.Tx, error) {
	data, err := hex.DecodeString(raw)
	if err != nil {
		return nil, err
	}
	r := bytes.NewReader(data)
	tx, err := replayrecovery.ReadBoundedTransaction(r)
	if err != nil {
		return nil, err
	}
	if r.Len() != 0 || tx.TxID() != id {
		return nil, errors.NewProcessingError("history transaction identity mismatch")
	}
	return tx, nil
}

func (h *LocalHistory) expandUnconfirmed(ctx context.Context) error {
	if h.options.Unconfirmed == nil {
		return nil
	}
	// Keyset paging keeps candidate/dependency memory bounded, including new IDs.
	last := ""
	for {
		rows, err := h.db.QueryContext(ctx, "SELECT id FROM targets WHERE id>? ORDER BY id LIMIT 256", last)
		if err != nil {
			return err
		}
		var ids []string
		for rows.Next() {
			var id string
			if err = rows.Scan(&id); err != nil {
				_ = rows.Close()
				return err
			}
			ids = append(ids, id)
		}
		err = rows.Err()
		if closeErr := rows.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			return nil
		}
		for _, id := range ids {
			if err = h.checkTip(ctx, h.coverage.Tip); err != nil {
				return err
			}
			tx, e := h.options.Unconfirmed(ctx, id)
			if e != nil || tx == nil || tx.TxID() != id || tx.IsCoinbase() {
				continue
			}
			for _, in := range tx.Inputs {
				if _, err = h.db.ExecContext(ctx, "INSERT OR IGNORE INTO targets VALUES(?)", in.PreviousTxIDStr()); err != nil {
					return err
				}
			}
		}
		last = ids[len(ids)-1]
	}
}

func (h *LocalHistory) Check(ctx context.Context, id string, tip replayrecovery.Tip) (replayrecovery.Evidence, error) {
	e := replayrecovery.Evidence{TxID: id, Tip: tip, Source: "local-history", Classification: replayrecovery.Unknown}
	if err := h.checkTip(ctx, tip); err != nil {
		return e, err
	}
	p, err := h.Lookup(ctx, id, tip)
	if err != nil {
		var present int
		if qerr := h.db.QueryRowContext(ctx, "SELECT count(*) FROM transactions WHERE txid=?", id).Scan(&present); qerr != nil {
			return e, qerr
		}
		if present != 0 {
			return e, err
		}
		// Absence alone is never proof of validity. All parent outputs must have
		// canonical inclusion and complete spend coverage through this pinned tip.
		if !h.built || tip != h.coverage.Tip || !h.coverage.Complete || h.coverage.StartHeight != 0 || h.coverage.EndHeight != tip.Height || h.options.Unconfirmed == nil {
			e.Reason = "transaction inclusion unavailable or coverage incomplete"
			return e, nil
		}
		var indexed int
		if err = h.db.QueryRowContext(ctx, "SELECT count(*) FROM targets WHERE id=?", id).Scan(&indexed); err != nil {
			return e, err
		}
		if indexed == 0 {
			e.Reason = "transaction outside indexed targets"
			return e, nil
		}
		tx, readErr := h.unconfirmedTransaction(ctx, id, tip, make(map[string]bool), new(int), 0)
		if readErr != nil {
			e.Reason = readErr.Error()
			return e, nil
		}
		e.Classification = replayrecovery.Unconfirmed
		e.RawTx = tx.String()
		return e, nil
	}
	tx, err := historyTransaction(id, p.RawTx)
	if err != nil {
		return e, err
	}
	e.RawTx = p.RawTx
	e.BlockHash = chainhash.DoubleHashH(p.Header).String()
	e.BlockHeight = p.Height
	for vout, out := range tx.Outputs {
		if !utxo.ShouldStoreOutputAsUTXO(out, p.Height, h.options.GenesisActivationHeight) {
			continue
		}
		rows, queryErr := h.db.QueryContext(ctx, "SELECT child FROM spends WHERE parent=? AND vout=?", id, vout)
		if queryErr != nil {
			return e, queryErr
		}
		var children []string
		for rows.Next() {
			var child string
			if err = rows.Scan(&child); err != nil {
				_ = rows.Close()
				return e, err
			}
			children = append(children, child)
		}
		err = rows.Err()
		if closeErr := rows.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			return e, err
		}
		proven := false
		for _, child := range children {
			spend, lookupErr := h.Lookup(ctx, child, tip)
			if lookupErr != nil {
				continue
			}
			if spend.Height < p.Height || (spend.Height == p.Height && spend.Index <= p.Index) {
				continue
			}
			st, parseErr := historyTransaction(child, spend.RawTx)
			if parseErr != nil {
				continue
			}
			for _, input := range st.Inputs {
				if input.PreviousTxIDStr() == id && uint64(input.PreviousTxOutIndex) == uint64(vout) {
					proven = true
					break
				}
			}
			if proven {
				break
			}
		}
		if !proven {
			if len(children) == 0 && h.coverage.Complete && h.coverage.StartHeight <= p.Height && h.coverage.EndHeight == tip.Height {
				e.Classification = replayrecovery.Live
			}
			e.Reason = "spendable output lacks authenticated ordered spend"
			return e, nil
		}
	}
	e.Classification = replayrecovery.FullySpent
	return e, nil
}

// historyDigest detects incomplete or corrupted private indexes on resume. Proof
// reads additionally authenticate raw bytes and live canonical membership.
func historyDigest(ctx context.Context, db *sql.DB) (string, error) {
	hash := sha256.New()
	for _, query := range []string{
		"SELECT hash,height,header,leaves FROM blocks ORDER BY hash",
		"SELECT txid,block,position,raw FROM transactions ORDER BY txid,block",
		"SELECT id FROM targets ORDER BY id",
		"SELECT parent,vout,child FROM spends ORDER BY parent,vout,child",
		"SELECT height,hash,header FROM headers ORDER BY height",
		"SELECT height,reason FROM gaps ORDER BY height",
	} {
		rows, err := db.QueryContext(ctx, query)
		if err != nil {
			return "", err
		}
		cols, err := rows.Columns()
		if err != nil {
			_ = rows.Close()
			return "", err
		}
		values := make([]any, len(cols))
		pointers := make([]any, len(cols))
		for i := range values {
			pointers[i] = &values[i]
		}
		for rows.Next() {
			if err = rows.Scan(pointers...); err != nil {
				_ = rows.Close()
				return "", err
			}
			data, e := json.Marshal(values)
			if e != nil {
				_ = rows.Close()
				return "", e
			}
			hash.Write(data)
			hash.Write([]byte{0})
		}
		err = rows.Err()
		if closeErr := rows.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			return "", err
		}
		hash.Write([]byte{255})
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func historySealDigest(digest string, o HistoryOptions, c HistoryCoverage) string {
	data, _ := json.Marshal(struct {
		Digest   string
		Options  HistoryOptions
		Coverage HistoryCoverage
	}{digest, o, c})
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// unconfirmedTransaction proves available ancestry without treating a missing
// local UTXO or a local spend flag as evidence. Bounds reject pathological graphs.
func (h *LocalHistory) unconfirmedTransaction(ctx context.Context, id string, tip replayrecovery.Tip, visiting map[string]bool, work *int, depth int) (*bt.Tx, error) {
	*work++
	if depth >= 256 || *work > 10000 || visiting[id] {
		return nil, errors.NewProcessingError("unconfirmed dependency cycle or work bound")
	}
	if err := h.checkTip(ctx, tip); err != nil {
		return nil, err
	}
	var indexed, present int
	if err := h.db.QueryRowContext(ctx, "SELECT count(*) FROM targets WHERE id=?", id).Scan(&indexed); err != nil {
		return nil, err
	}
	if err := h.db.QueryRowContext(ctx, "SELECT count(*) FROM transactions WHERE txid=?", id).Scan(&present); err != nil {
		return nil, err
	}
	if indexed == 0 || present != 0 {
		return nil, errors.NewProcessingError("unconfirmed dependency outside proven absence scope")
	}
	tx, err := h.options.Unconfirmed(ctx, id)
	if err != nil || tx == nil || tx.TxID() != id || tx.IsCoinbase() || len(tx.Inputs) == 0 {
		return nil, errors.NewProcessingError("unconfirmed transaction bytes unavailable or invalid")
	}
	visiting[id] = true
	defer delete(visiting, id)
	seen := map[string]bool{}
	for _, in := range tx.Inputs {
		parentID := in.PreviousTxIDStr()
		proof, lookupErr := h.Lookup(ctx, parentID, tip)
		var parent *bt.Tx
		height := tip.Height
		if height == ^uint32(0) {
			return nil, errors.NewProcessingError("unconfirmed height overflow")
		}
		height++
		if lookupErr == nil {
			parent, err = historyTransaction(parentID, proof.RawTx)
			height = proof.Height
		} else {
			parent, err = h.unconfirmedTransaction(ctx, parentID, tip, visiting, work, depth+1)
		}
		if err != nil {
			return nil, err
		}
		if uint64(in.PreviousTxOutIndex) >= uint64(len(parent.Outputs)) {
			return nil, errors.NewProcessingError("unconfirmed input index invalid")
		}
		if parent.IsCoinbase() && (height == 0 || uint64(tip.Height)+1 < uint64(height)+100) {
			return nil, errors.NewProcessingError("unconfirmed input coinbase is not spendable")
		}
		key := parentID + ":" + fmt.Sprint(in.PreviousTxOutIndex)
		if seen[key] || !utxo.ShouldStoreOutputAsUTXO(parent.Outputs[in.PreviousTxOutIndex], height, h.options.GenesisActivationHeight) {
			return nil, errors.NewProcessingError("unconfirmed input unavailable")
		}
		seen[key] = true
		var spent int
		if err = h.db.QueryRowContext(ctx, "SELECT count(*) FROM spends WHERE parent=? AND vout=?", parentID, in.PreviousTxOutIndex).Scan(&spent); err != nil {
			return nil, err
		}
		if spent != 0 {
			return nil, errors.NewProcessingError("unconfirmed input already canonically spent")
		}
	}
	return tx, nil
}
