package pebble

import (
	"context"
	"encoding/binary"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/cockroachdb/pebble/v2"
)

var _ utxo.Store = (*Store)(nil)

const (
	prefixMaster       = 'm'
	prefixPage         = 'g'
	prefixHashes       = 'h'
	prefixPayload      = 'p'
	prefixUnminedIdx   = 'u'
	prefixDAHIdx       = 'd'
	prefixPreserveIdx  = 'v'
	prefixConflictIdx  = 'c'
	prefixChildren     = 'k'
	prefixChildrenRev  = 'K'
	prefixIntent       = 'w'
	prefixOverride     = 'o'
	prefixMeta         = 'M'
	numStripes         = 1024
	spikePageSizeSlots = 64
)

type Store struct {
	logger   ulogger.Logger
	settings *settings.Settings
	db       *pebble.DB
	pageSize uint32
	sync     *pebble.WriteOptions
	// inFlight is held for reading around every database access and for writing
	// by Close, so the drain below cannot overlap live work.
	inFlight   sync.RWMutex
	blockState atomic.Uint64
	stripes    [numStripes]sync.Mutex
	closed     atomic.Bool
}

// enter registers a database access. pebble panics rather than erroring on any
// use after Close (getInternal, applyInternal and newIter all begin with a
// closed check that panics), so every access must pass through here or a
// shutdown races the validator into a process crash.
func (s *Store) enter() error {
	s.inFlight.RLock()

	if s.closed.Load() {
		s.inFlight.RUnlock()

		return errors.NewStorageError("pebble: store is closed")
	}

	return nil
}

func (s *Store) leave() {
	s.inFlight.RUnlock()
}

// commit applies a staged batch under the close guard.
func (s *Store) commit(batch *pebble.Batch) error {
	if err := s.enter(); err != nil {
		return err
	}

	defer s.leave()

	return batch.Commit(s.sync)
}

// setDirect and deleteDirect are for the few writes that are deliberately their
// own durable operation rather than part of a batch, such as the conflict WAL.
func (s *Store) setDirect(key, value []byte) error {
	if err := s.enter(); err != nil {
		return err
	}

	defer s.leave()

	return s.db.Set(key, value, s.sync)
}

func (s *Store) deleteDirect(key []byte) error {
	if err := s.enter(); err != nil {
		return err
	}

	defer s.leave()

	return s.db.Delete(key, s.sync)
}

// newIter returns an iterator and a release function. The close guard is held
// for the iterator's whole lifetime, because iterating after Close panics just
// as a direct read does. Callers must defer the returned release.
func (s *Store) newIter(opts *pebble.IterOptions) (*pebble.Iterator, func(), error) {
	if err := s.enter(); err != nil {
		return nil, nil, err
	}

	iter, err := s.db.NewIter(opts)
	if err != nil {
		s.leave()

		return nil, nil, err
	}

	return iter, func() {
		_ = iter.Close()

		s.leave()
	}, nil
}

func New(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, storeURL *url.URL) (*Store, error) {
	if storeURL == nil || storeURL.Path == "" {
		return nil, errors.NewInvalidArgumentError("pebble: store URL with a directory path is required")
	}

	dir, err := storeDir(tSettings, storeURL)
	if err != nil {
		return nil, err
	}

	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		return nil, errors.NewStorageError("pebble: failed to open database at %s", dir, err)
	}

	// Commits fsync the WAL by default. sync=false trades that durability for
	// throughput and loses recent commits on an unclean shutdown, so it is
	// opt-in per store URL rather than the default.
	writeOpts := pebble.Sync
	if !util.GetQueryParamBool(storeURL, "sync", true) {
		writeOpts = pebble.NoSync

		logger.Warnf("[pebble] WAL fsync disabled via sync=false: recent commits are lost on an unclean shutdown")
	}

	s := &Store{
		logger:   logger,
		settings: tSettings,
		db:       db,
		pageSize: spikePageSizeSlots,
		sync:     writeOpts,
	}

	if err = s.validateMeta(); err != nil {
		_ = db.Close()
		return nil, err
	}

	return s, nil
}

// storeDir resolves the store directory from the URL. url.Host carries the first
// path component whenever the URL has only two leading slashes, so
// pebble://data/utxostore parses as Host="data", Path="/utxostore"; using Path
// alone drops "data" and opens the store at the filesystem root. The joined path
// is resolved under tSettings.DataFolder, matching util.InitSQLiteDB, so
// pebble:///utxostore and sqlite:///utxostore land in the same place.
func storeDir(tSettings *settings.Settings, storeURL *url.URL) (string, error) {
	joined := path.Join(storeURL.Host, strings.TrimPrefix(storeURL.Path, "/"))
	if joined == "" || joined == "." {
		return "", errors.NewInvalidArgumentError("pebble: store URL with a directory path is required")
	}

	folder := "."
	if tSettings != nil && tSettings.DataFolder != "" {
		folder = tSettings.DataFolder
	}

	dir, err := filepath.Abs(filepath.Join(folder, joined))
	if err != nil {
		return "", errors.NewStorageError("pebble: failed to resolve store directory %q", joined, err)
	}

	if err = os.MkdirAll(dir, 0o755); err != nil {
		return "", errors.NewStorageError("pebble: failed to create store directory %s", dir, err)
	}

	return dir, nil
}

func (s *Store) validateMeta() error {
	key := []byte{prefixMeta}

	val, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			stamp := make([]byte, 4)
			binary.LittleEndian.PutUint32(stamp, s.pageSize)

			return s.db.Set(key, stamp, pebble.Sync)
		}

		return errors.NewStorageError("pebble: failed to read store meta", err)
	}

	if len(val) < 4 {
		_ = closer.Close()

		return errors.NewConfigurationError("pebble: store meta record is truncated (%d bytes)", len(val))
	}

	stored := binary.LittleEndian.Uint32(val)
	_ = closer.Close()

	if stored != s.pageSize {
		return errors.NewConfigurationError("pebble: page size is immutable: database has %d, code requests %d", stored, s.pageSize)
	}

	return nil
}

// Close drains in-flight database work before closing the database.
// stores/utxo/Interface.go requires an implementation to wait for outstanding
// batched writes, because returning early risks silently losing UTXO state, so
// the drain is unconditional rather than bounded by ctx: a caller that gave up
// waiting would still be handing pebble a database with live readers on it.
func (s *Store) Close(ctx context.Context) error {
	if s.closed.Swap(true) {
		return nil
	}

	// Every database access holds inFlight for reading, so taking it for writing
	// returns only once none is in progress. New accesses already see closed.
	s.inFlight.Lock()
	defer s.inFlight.Unlock()

	return s.db.Close()
}

func (s *Store) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	if s.closed.Load() {
		return http.StatusServiceUnavailable, "pebble store closed", errors.NewStorageError("pebble: store closed")
	}

	return http.StatusOK, "pebble embedded store", nil
}

func (s *Store) SupportsOutpointOnlySpend() bool {
	return true
}

func masterKey(hash []byte) []byte {
	return append([]byte{prefixMaster}, hash...)
}

func pageKey(hash []byte, page uint32) []byte {
	k := make([]byte, 0, 37)
	k = append(k, prefixPage)
	k = append(k, hash...)

	var pb [4]byte
	binary.BigEndian.PutUint32(pb[:], page)

	return append(k, pb[:]...)
}

func hashesKey(hash []byte, page uint32) []byte {
	k := pageKey(hash, page)
	k[0] = prefixHashes

	return k
}

func payloadKey(hash []byte) []byte {
	return append([]byte{prefixPayload}, hash...)
}

func heightIndexKey(prefix byte, height int64, hash []byte) []byte {
	k := make([]byte, 0, 41)
	k = append(k, prefix)

	var hb [8]byte
	binary.BigEndian.PutUint64(hb[:], uint64(height)) //nolint:gosec
	k = append(k, hb[:]...)

	return append(k, hash...)
}

func conflictIdxKey(hash []byte) []byte {
	return append([]byte{prefixConflictIdx}, hash...)
}

func childrenKey(parent, child []byte) []byte {
	k := make([]byte, 0, 65)
	k = append(k, prefixChildren)
	k = append(k, parent...)

	return append(k, child...)
}

func childrenRevKey(child, parent []byte) []byte {
	k := make([]byte, 0, 65)
	k = append(k, prefixChildrenRev)
	k = append(k, child...)

	return append(k, parent...)
}

func overrideKey(hash []byte, vout uint32) []byte {
	k := make([]byte, 0, 37)
	k = append(k, prefixOverride)
	k = append(k, hash...)

	var vb [4]byte
	binary.BigEndian.PutUint32(vb[:], vout)

	return append(k, vb[:]...)
}

func intentKey(id []byte) []byte {
	return append([]byte{prefixIntent}, id...)
}

func prefixBounds(prefix []byte) ([]byte, []byte) {
	upper := make([]byte, len(prefix))
	copy(upper, prefix)

	for i := len(upper) - 1; i >= 0; i-- {
		upper[i]++
		if upper[i] != 0 {
			return prefix, upper[:i+1]
		}
	}

	return prefix, nil
}

func stripeOf(hash []byte) int {
	return int(binary.LittleEndian.Uint16(hash[:2])) % numStripes
}

func (s *Store) lockStripes(hashes ...[]byte) func() {
	seen := make(map[int]struct{}, len(hashes))
	order := make([]int, 0, len(hashes))

	for _, h := range hashes {
		idx := stripeOf(h)
		if _, ok := seen[idx]; !ok {
			seen[idx] = struct{}{}

			order = append(order, idx)
		}
	}

	sort.Ints(order)

	for _, idx := range order {
		s.stripes[idx].Lock()
	}

	return func() {
		for i := len(order) - 1; i >= 0; i-- {
			s.stripes[order[i]].Unlock()
		}
	}
}

// reader is the read surface shared by *pebble.DB and *pebble.Snapshot, so a
// read path can be pointed at either without duplicating it.
type reader interface {
	Get(key []byte) ([]byte, io.Closer, error)
	NewIter(o *pebble.IterOptions) (*pebble.Iterator, error)
}

// snapshot pins one consistent view for a multi-key read. Every read API
// assembles a logical record from three to six keys (master, payload, pages,
// hashes, overrides, conflicting children). Read straight from the database
// those land at different sequence numbers, so a reader can observe half of an
// atomically committed Spend batch and report a spend state that never existed,
// or read a master whose payload a concurrent Delete has already removed.
//
// The close guard is held for the snapshot's lifetime, because an open snapshot
// over a closed database panics exactly as a direct read does.
func (s *Store) snapshot() (*pebble.Snapshot, func(), error) {
	if err := s.enter(); err != nil {
		return nil, nil, err
	}

	snap := s.db.NewSnapshot()

	return snap, func() {
		_ = snap.Close()

		s.leave()
	}, nil
}

// readValue reads one key from r. The caller must already hold the close guard,
// which is why this does not take it: sync.RWMutex read locks are not safely
// re-entrant when a writer is waiting.
func readValue(r reader, key []byte) ([]byte, error) {
	val, closer, err := r.Get(key)
	if err != nil {
		return nil, err
	}

	out := append([]byte(nil), val...)
	_ = closer.Close()

	return out, nil
}

// getValue reads one key from the database under the close guard. A single key
// is atomic on its own, so callers that need only one key do not need a
// snapshot.
func (s *Store) getValue(key []byte) ([]byte, error) {
	if err := s.enter(); err != nil {
		return nil, err
	}

	defer s.leave()

	return readValue(s.db, key)
}

func (s *Store) getMaster(hash *chainhash.Hash) (*masterRecord, error) {
	if err := s.enter(); err != nil {
		return nil, err
	}

	defer s.leave()

	return getMasterFrom(s.db, hash)
}

func getMasterFrom(r reader, hash *chainhash.Hash) (*masterRecord, error) {
	val, err := readValue(r, masterKey(hash[:]))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, errors.NewTxNotFoundError("pebble: transaction %s not found", hash)
		}

		return nil, errors.NewStorageError("pebble: failed to read transaction %s", hash, err)
	}

	return decodeMaster(val)
}

func (s *Store) retention() int64 {
	if r := s.settings.GetUtxoStoreBlockHeightRetention(); r > 0 {
		return int64(r)
	}

	return 0
}

func packBlockState(height, medianTime uint32) uint64 {
	return uint64(height)<<32 | uint64(medianTime)
}

func (s *Store) SetBlockHeight(height uint32) error {
	if height == 0 {
		return errors.NewInvalidArgumentError("pebble: block height must be non-zero")
	}

	for {
		old := s.blockState.Load()
		if s.blockState.CompareAndSwap(old, packBlockState(height, uint32(old))) { //nolint:gosec
			return nil
		}
	}
}

func (s *Store) GetBlockHeight() uint32 {
	return uint32(s.blockState.Load() >> 32) //nolint:gosec
}

func (s *Store) SetMedianBlockTime(medianTime uint32) error {
	for {
		old := s.blockState.Load()
		if s.blockState.CompareAndSwap(old, old&0xFFFFFFFF00000000|uint64(medianTime)) {
			return nil
		}
	}
}

func (s *Store) GetMedianBlockTime() uint32 {
	return uint32(s.blockState.Load()) //nolint:gosec
}

func (s *Store) SetBlockState(height, medianTime uint32) error {
	if height == 0 {
		return errors.NewInvalidArgumentError("pebble: block height must be non-zero")
	}

	s.blockState.Store(packBlockState(height, medianTime))

	return nil
}

func (s *Store) GetBlockState() utxo.BlockState {
	v := s.blockState.Load()

	return utxo.BlockState{
		Height:     uint32(v >> 32), //nolint:gosec
		MedianTime: uint32(v),       //nolint:gosec
	}
}
