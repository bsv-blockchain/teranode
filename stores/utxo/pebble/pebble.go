package pebble

import (
	"context"
	"encoding/binary"
	"net/http"
	"net/url"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
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
	logger     ulogger.Logger
	settings   *settings.Settings
	db         *pebble.DB
	pageSize   uint32
	sync       *pebble.WriteOptions
	blockState atomic.Uint64
	stripes    [numStripes]sync.Mutex
	closed     atomic.Bool
}

func New(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, storeURL *url.URL) (*Store, error) {
	if storeURL == nil || storeURL.Path == "" {
		return nil, errors.NewInvalidArgumentError("pebble: store URL with a directory path is required")
	}

	db, err := pebble.Open(storeURL.Path, &pebble.Options{})
	if err != nil {
		return nil, errors.NewStorageError("pebble: failed to open database at %s", storeURL.Path, err)
	}

	s := &Store{
		logger:   logger,
		settings: tSettings,
		db:       db,
		pageSize: spikePageSizeSlots,
		sync:     pebble.Sync,
	}

	if err = s.validateMeta(); err != nil {
		_ = db.Close()
		return nil, err
	}

	return s, nil
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

	stored := binary.LittleEndian.Uint32(val)
	_ = closer.Close()

	if stored != s.pageSize {
		return errors.NewConfigurationError("pebble: page size is immutable: database has %d, code requests %d", stored, s.pageSize)
	}

	return nil
}

func (s *Store) Close(ctx context.Context) error {
	if s.closed.Swap(true) {
		return nil
	}

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

func (s *Store) getValue(key []byte) ([]byte, error) {
	val, closer, err := s.db.Get(key)
	if err != nil {
		return nil, err
	}

	out := append([]byte(nil), val...)
	_ = closer.Close()

	return out, nil
}

func (s *Store) getMaster(hash *chainhash.Hash) (*masterRecord, error) {
	val, err := s.getValue(masterKey(hash[:]))
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
