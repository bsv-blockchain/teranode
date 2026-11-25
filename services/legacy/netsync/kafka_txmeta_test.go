package netsync

import (
	"encoding/binary"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-batcher/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/txmetacache"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// buildTXmetaBatchMessage constructs a binary batch message in the format expected
// by processTXmetaBatchMessage. Each entry contains a tx hash, action byte, content
// length, and content (metaBytes for ADD entries).
func buildTXmetaBatchMessage(t *testing.T, entries []txmetaTestEntry) []byte {
	t.Helper()

	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(len(entries)))

	for _, entry := range entries {
		// 32-byte hash
		buf = append(buf, entry.hash[:]...)

		// 1-byte action
		buf = append(buf, entry.action)

		if entry.action == txmetacache.WireActionADD || entry.action == txmetacache.WireActionADDLane || entry.action == txmetacache.WireActionADDOwn {
			metaBytes := entryContent(t, entry)

			// 4-byte content length
			lenBuf := make([]byte, 4)
			binary.LittleEndian.PutUint32(lenBuf, uint32(len(metaBytes)))
			buf = append(buf, lenBuf...)

			// N-byte content
			buf = append(buf, metaBytes...)
		} else {
			// DELETE: 4-byte zero content length
			buf = append(buf, 0, 0, 0, 0)
		}
	}

	return buf
}

type txmetaTestEntry struct {
	hash   chainhash.Hash
	action byte
	meta   meta.Data
	// raw, when set, is the entry's content as is, in place of one built from meta.
	raw []byte
}

// entryContent is an ADD entry's MetaBytes, an ADDLane entry's lane encoding with one position per
// parent, or an ADDOwn entry's MetaBytes and own position.
func entryContent(t *testing.T, entry txmetaTestEntry) []byte {
	t.Helper()

	if entry.raw != nil {
		return entry.raw
	}

	if entry.action == txmetacache.WireActionADDLane {
		parents := make([]uint64, len(entry.meta.TxInpoints.ParentTxHashes))
		for i := range parents {
			parents[i] = lane.Pack(9, uint64(i)+1) //nolint:gosec // i counts up from 0
		}

		b, err := entry.meta.LaneMetaBytesInto(nil, parents)
		require.NoError(t, err)

		return b
	}

	if entry.action == txmetacache.WireActionADDOwn {
		b, err := entry.meta.CacheBytes()
		require.NoError(t, err)

		return b
	}

	b, err := entry.meta.MetaBytes()
	require.NoError(t, err)

	return b
}

// Relay announces a transaction whose entry is in the lane encoding, which the validator sends at
// utxostore_laneMigration shadow, exactly as one in the plain encoding, in both wire formats, and
// still skips one that arrived in a block.
func TestProcessTXmetaBatchMessage_LaneEntriesAreAnnounced(t *testing.T) {
	relayed, err := chainhash.NewHashFromStr("cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")
	require.NoError(t, err)

	inBlock, err := chainhash.NewHashFromStr("dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd")
	require.NoError(t, err)

	entries := []txmetaTestEntry{
		{hash: *relayed, action: txmetacache.WireActionADDLane, meta: meta.Data{Fee: 423, SizeInBytes: 250, LanePosition: lane.Pack(9, 100)}},
		{hash: *inBlock, action: txmetacache.WireActionADDLane, meta: meta.Data{Fee: 500, SizeInBytes: 250, InBlock: true, LanePosition: lane.Pack(9, 101)}},
	}

	for name, build := range map[string]func(*testing.T, []txmetaTestEntry) []byte{
		"v1": buildTXmetaBatchMessage,
		"v2": buildTXmetaBatchMessageV2,
	} {
		t.Run(name, func(t *testing.T) {
			var mu sync.Mutex
			var announced []*TxHashAndFee

			sm := &SyncManager{
				logger: ulogger.TestLogger{},
				txAnnounceBatcher: batcher.NewWithDeduplication[TxHashAndFee](100, 50*time.Millisecond, func(batch []*TxHashAndFee) {
					mu.Lock()
					announced = append(announced, batch...)
					mu.Unlock()
				}, false),
			}

			require.NoError(t, sm.processTXmetaBatchMessage(build(t, entries)))

			require.Eventually(t, func() bool {
				mu.Lock()
				defer mu.Unlock()

				return len(announced) == 1
			}, 2*time.Second, 10*time.Millisecond)

			time.Sleep(100 * time.Millisecond)

			mu.Lock()
			defer mu.Unlock()

			require.Len(t, announced, 1, "the in-block entry is not announced")
			require.Equal(t, *relayed, announced[0].TxHash)
			require.Equal(t, uint64(423), announced[0].Fee)
			require.Equal(t, uint64(250), announced[0].Size)
		})
	}
}

// buildTXmetaBatchMessageV2 constructs a v2 wire-format batch message: an
// 8-byte header (magic 0xFF, version 0x02, uint16 LE BucketsCount, uint32
// LE entry count) followed by per-entry records prefixed with an 8-byte
// xxhash. The xxhash is filled with zeros — the netsync consumer ignores it.
func buildTXmetaBatchMessageV2(t *testing.T, entries []txmetaTestEntry) []byte {
	t.Helper()

	buf := make([]byte, 8)
	buf[0] = txmetacache.WireV2Magic
	buf[1] = txmetacache.WireV2Version
	binary.LittleEndian.PutUint16(buf[2:4], uint16(txmetacache.BucketsCount))
	binary.LittleEndian.PutUint32(buf[4:], uint32(len(entries)))

	for _, entry := range entries {
		// 8-byte xxhash placeholder
		buf = append(buf, 0, 0, 0, 0, 0, 0, 0, 0)

		// 32-byte hash
		buf = append(buf, entry.hash[:]...)

		// 1-byte action
		buf = append(buf, entry.action)

		if entry.action == txmetacache.WireActionADD || entry.action == txmetacache.WireActionADDLane || entry.action == txmetacache.WireActionADDOwn {
			metaBytes := entryContent(t, entry)

			lenBuf := make([]byte, 4)
			binary.LittleEndian.PutUint32(lenBuf, uint32(len(metaBytes)))
			buf = append(buf, lenBuf...)
			buf = append(buf, metaBytes...)
		} else {
			buf = append(buf, 0, 0, 0, 0)
		}
	}

	return buf
}

func TestProcessTXmetaBatchMessage_CoinbaseFiltering(t *testing.T) {
	// Set up hashes for test entries
	coinbaseHash, err := chainhash.NewHashFromStr("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	require.NoError(t, err)

	regularHash, err := chainhash.NewHashFromStr("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	require.NoError(t, err)

	t.Run("coinbase tx is not announced", func(t *testing.T) {
		var mu sync.Mutex
		var announced []*TxHashAndFee

		sm := &SyncManager{
			logger: ulogger.TestLogger{},
			txAnnounceBatcher: batcher.NewWithDeduplication[TxHashAndFee](100, 50*time.Millisecond, func(batch []*TxHashAndFee) {
				mu.Lock()
				announced = append(announced, batch...)
				mu.Unlock()
			}, false),
		}

		msg := buildTXmetaBatchMessage(t, []txmetaTestEntry{
			{
				hash:   *coinbaseHash,
				action: txmetacache.WireActionADD,
				meta: meta.Data{
					Fee:         0,
					SizeInBytes: 100,
					IsCoinbase:  true,
				},
			},
			{
				hash:   *regularHash,
				action: txmetacache.WireActionADD,
				meta: meta.Data{
					Fee:         500,
					SizeInBytes: 250,
					IsCoinbase:  false,
				},
			},
		})

		err := sm.processTXmetaBatchMessage(msg)
		require.NoError(t, err)

		// Trigger the batcher to flush by waiting for its timeout
		time.Sleep(200 * time.Millisecond)

		mu.Lock()
		defer mu.Unlock()

		// Only the regular tx should have been announced
		require.Len(t, announced, 1)
		require.Equal(t, *regularHash, announced[0].TxHash)
		require.Equal(t, uint64(500), announced[0].Fee)
		require.Equal(t, uint64(250), announced[0].Size)
	})

	t.Run("only coinbase txs produces no announcements", func(t *testing.T) {
		var mu sync.Mutex
		var announced []*TxHashAndFee

		sm := &SyncManager{
			logger: ulogger.TestLogger{},
			txAnnounceBatcher: batcher.NewWithDeduplication[TxHashAndFee](100, 50*time.Millisecond, func(batch []*TxHashAndFee) {
				mu.Lock()
				announced = append(announced, batch...)
				mu.Unlock()
			}, false),
		}

		msg := buildTXmetaBatchMessage(t, []txmetaTestEntry{
			{
				hash:   *coinbaseHash,
				action: txmetacache.WireActionADD,
				meta: meta.Data{
					Fee:         0,
					SizeInBytes: 100,
					IsCoinbase:  true,
				},
			},
		})

		err := sm.processTXmetaBatchMessage(msg)
		require.NoError(t, err)

		time.Sleep(200 * time.Millisecond)

		mu.Lock()
		defer mu.Unlock()

		require.Empty(t, announced)
	})

	t.Run("regular txs are announced", func(t *testing.T) {
		var mu sync.Mutex
		var announced []*TxHashAndFee

		sm := &SyncManager{
			logger: ulogger.TestLogger{},
			txAnnounceBatcher: batcher.NewWithDeduplication[TxHashAndFee](100, 50*time.Millisecond, func(batch []*TxHashAndFee) {
				mu.Lock()
				announced = append(announced, batch...)
				mu.Unlock()
			}, false),
		}

		msg := buildTXmetaBatchMessage(t, []txmetaTestEntry{
			{
				hash:   *regularHash,
				action: txmetacache.WireActionADD,
				meta: meta.Data{
					Fee:         1000,
					SizeInBytes: 500,
					IsCoinbase:  false,
				},
			},
		})

		err := sm.processTXmetaBatchMessage(msg)
		require.NoError(t, err)

		time.Sleep(200 * time.Millisecond)

		mu.Lock()
		defer mu.Unlock()

		require.Len(t, announced, 1)
		require.Equal(t, *regularHash, announced[0].TxHash)
		require.Equal(t, uint64(1000), announced[0].Fee)
		require.Equal(t, uint64(500), announced[0].Size)
	})

	t.Run("DELETE entries are skipped", func(t *testing.T) {
		var mu sync.Mutex
		var announced []*TxHashAndFee

		sm := &SyncManager{
			logger: ulogger.TestLogger{},
			txAnnounceBatcher: batcher.NewWithDeduplication[TxHashAndFee](100, 50*time.Millisecond, func(batch []*TxHashAndFee) {
				mu.Lock()
				announced = append(announced, batch...)
				mu.Unlock()
			}, false),
		}

		msg := buildTXmetaBatchMessage(t, []txmetaTestEntry{
			{
				hash:   *coinbaseHash,
				action: txmetacache.WireActionDELETE,
			},
			{
				hash:   *regularHash,
				action: txmetacache.WireActionADD,
				meta: meta.Data{
					Fee:         750,
					SizeInBytes: 300,
					IsCoinbase:  false,
				},
			},
		})

		err := sm.processTXmetaBatchMessage(msg)
		require.NoError(t, err)

		time.Sleep(200 * time.Millisecond)

		mu.Lock()
		defer mu.Unlock()

		require.Len(t, announced, 1)
		require.Equal(t, *regularHash, announced[0].TxHash)
	})

	t.Run("empty message is handled gracefully", func(t *testing.T) {
		sm := &SyncManager{
			logger: ulogger.TestLogger{},
		}

		err := sm.processTXmetaBatchMessage([]byte{})
		require.NoError(t, err)

		err = sm.processTXmetaBatchMessage([]byte{0, 0, 0})
		require.NoError(t, err)
	})

	t.Run("v2 wire format: coinbase filtered, regular tx announced", func(t *testing.T) {
		var mu sync.Mutex
		var announced []*TxHashAndFee

		sm := &SyncManager{
			logger: ulogger.TestLogger{},
			txAnnounceBatcher: batcher.NewWithDeduplication[TxHashAndFee](100, 50*time.Millisecond, func(batch []*TxHashAndFee) {
				mu.Lock()
				announced = append(announced, batch...)
				mu.Unlock()
			}, false),
		}

		msg := buildTXmetaBatchMessageV2(t, []txmetaTestEntry{
			{
				hash:   *coinbaseHash,
				action: txmetacache.WireActionADD,
				meta: meta.Data{
					Fee:         0,
					SizeInBytes: 100,
					IsCoinbase:  true,
				},
			},
			{
				hash:   *regularHash,
				action: txmetacache.WireActionADD,
				meta: meta.Data{
					Fee:         500,
					SizeInBytes: 250,
					IsCoinbase:  false,
				},
			},
		})

		err := sm.processTXmetaBatchMessage(msg)
		require.NoError(t, err)

		time.Sleep(200 * time.Millisecond)

		mu.Lock()
		defer mu.Unlock()

		require.Len(t, announced, 1)
		require.Equal(t, *regularHash, announced[0].TxHash)
		require.Equal(t, uint64(500), announced[0].Fee)
		require.Equal(t, uint64(250), announced[0].Size)
	})

	t.Run("v2 wire format: DELETE entries are skipped", func(t *testing.T) {
		var mu sync.Mutex
		var announced []*TxHashAndFee

		sm := &SyncManager{
			logger: ulogger.TestLogger{},
			txAnnounceBatcher: batcher.NewWithDeduplication[TxHashAndFee](100, 50*time.Millisecond, func(batch []*TxHashAndFee) {
				mu.Lock()
				announced = append(announced, batch...)
				mu.Unlock()
			}, false),
		}

		msg := buildTXmetaBatchMessageV2(t, []txmetaTestEntry{
			{
				hash:   *coinbaseHash,
				action: txmetacache.WireActionDELETE,
			},
			{
				hash:   *regularHash,
				action: txmetacache.WireActionADD,
				meta: meta.Data{
					Fee:         750,
					SizeInBytes: 300,
					IsCoinbase:  false,
				},
			},
		})

		err := sm.processTXmetaBatchMessage(msg)
		require.NoError(t, err)

		time.Sleep(200 * time.Millisecond)

		mu.Lock()
		defer mu.Unlock()

		require.Len(t, announced, 1)
		require.Equal(t, *regularHash, announced[0].TxHash)
	})

	t.Run("v2 wire format: unknown sub-version is rejected", func(t *testing.T) {
		sm := &SyncManager{
			logger: ulogger.TestLogger{},
		}

		// Magic 0xFF + bogus version 0x99 + 6 padding bytes.
		msg := []byte{0xFF, 0x99, 0, 0, 0, 0, 0, 0}
		err := sm.processTXmetaBatchMessage(msg)
		require.NoError(t, err)
	})

	t.Run("v2 wire format: truncated header is handled gracefully", func(t *testing.T) {
		sm := &SyncManager{
			logger: ulogger.TestLogger{},
		}

		err := sm.processTXmetaBatchMessage([]byte{0xFF, 0x02, 0, 0})
		require.NoError(t, err)
	})

	// V1 entry counts whose little-endian encoding begins with 0xFF
	// (255, 511, 767, ...) collide with the v2 magic byte. Naive v2
	// detection would silently drop or garble these legitimate v1
	// messages. The cases below pad a real announce-target entry into a
	// large DELETE-only batch to hit the boundary counts.
	t.Run("v1 wire format with entry count 255 (0xFF low byte) is parsed as v1", func(t *testing.T) {
		var mu sync.Mutex
		var announced []*TxHashAndFee

		sm := &SyncManager{
			logger: ulogger.TestLogger{},
			txAnnounceBatcher: batcher.NewWithDeduplication[TxHashAndFee](1000, 50*time.Millisecond, func(batch []*TxHashAndFee) {
				mu.Lock()
				announced = append(announced, batch...)
				mu.Unlock()
			}, false),
		}

		entries := make([]txmetaTestEntry, 0, 255)
		entries = append(entries, txmetaTestEntry{
			hash:   *regularHash,
			action: txmetacache.WireActionADD,
			meta: meta.Data{
				Fee:         123,
				SizeInBytes: 456,
				IsCoinbase:  false,
			},
		})
		for i := 0; i < 254; i++ {
			entries = append(entries, txmetaTestEntry{
				hash:   *coinbaseHash,
				action: txmetacache.WireActionDELETE,
			})
		}
		require.Len(t, entries, 255, "entry count must collide with the v2 magic byte")

		msg := buildTXmetaBatchMessage(t, entries)
		require.Equal(t, byte(0xFF), msg[0], "v1 length-prefix low byte must be 0xFF for this regression")

		err := sm.processTXmetaBatchMessage(msg)
		require.NoError(t, err)

		time.Sleep(200 * time.Millisecond)

		mu.Lock()
		defer mu.Unlock()
		require.Len(t, announced, 1)
		require.Equal(t, *regularHash, announced[0].TxHash)
	})

	t.Run("v1 wire format with entry count 767 (full v2 header alias) is parsed as v1", func(t *testing.T) {
		var mu sync.Mutex
		var announced []*TxHashAndFee

		sm := &SyncManager{
			logger: ulogger.TestLogger{},
			txAnnounceBatcher: batcher.NewWithDeduplication[TxHashAndFee](2000, 50*time.Millisecond, func(batch []*TxHashAndFee) {
				mu.Lock()
				announced = append(announced, batch...)
				mu.Unlock()
			}, false),
		}

		entries := make([]txmetaTestEntry, 0, 767)
		entries = append(entries, txmetaTestEntry{
			hash:   *regularHash,
			action: txmetacache.WireActionADD,
			meta: meta.Data{
				Fee:         789,
				SizeInBytes: 321,
				IsCoinbase:  false,
			},
		})
		for i := 0; i < 766; i++ {
			entries = append(entries, txmetaTestEntry{
				hash:   *coinbaseHash,
				action: txmetacache.WireActionDELETE,
			})
		}
		require.Len(t, entries, 767)

		msg := buildTXmetaBatchMessage(t, entries)
		// 767 in LE-uint32 = 0xFF 0x02 0x00 0x00 — exactly aliases the v2 header.
		require.Equal(t, []byte{0xFF, 0x02, 0x00, 0x00}, msg[:4])

		err := sm.processTXmetaBatchMessage(msg)
		require.NoError(t, err)

		time.Sleep(200 * time.Millisecond)

		mu.Lock()
		defer mu.Unlock()
		require.Len(t, announced, 1)
		require.Equal(t, *regularHash, announced[0].TxHash)
	})

	t.Run("message with zero entries is handled gracefully", func(t *testing.T) {
		var mu sync.Mutex
		var announced []*TxHashAndFee

		sm := &SyncManager{
			logger: ulogger.TestLogger{},
			txAnnounceBatcher: batcher.NewWithDeduplication[TxHashAndFee](100, 50*time.Millisecond, func(batch []*TxHashAndFee) {
				mu.Lock()
				announced = append(announced, batch...)
				mu.Unlock()
			}, false),
		}

		msg := buildTXmetaBatchMessage(t, []txmetaTestEntry{})

		err := sm.processTXmetaBatchMessage(msg)
		require.NoError(t, err)

		time.Sleep(200 * time.Millisecond)

		mu.Lock()
		defer mu.Unlock()

		require.Empty(t, announced)
	})
}

// relayLaneCount reads teranode_lane_txmeta_messages_total{side="legacy", format}.
func relayLaneCount(t *testing.T, format string) float64 {
	t.Helper()

	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	for _, family := range families {
		if family.GetName() != "teranode_lane_txmeta_messages_total" {
			continue
		}

		for _, m := range family.GetMetric() {
			labels := map[string]string{}
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}

			if labels["side"] == "legacy" && labels["format"] == format {
				return m.GetCounter().GetValue()
			}
		}
	}

	return 0
}

// A lane entry that does not decode is not announced, and the entries after it still are. Relay
// counts every lane entry, the malformed one included, whatever its own setting, and a plain entry
// only at utxostore_laneMigration shadow.
func TestProcessTXmetaBatchMessage_MalformedLaneEntryDropsOnlyItself(t *testing.T) {
	bad, err := chainhash.NewHashFromStr("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
	require.NoError(t, err)

	good, err := chainhash.NewHashFromStr("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
	require.NoError(t, err)

	plain := meta.Data{Fee: 500, SizeInBytes: 250}
	plainBytes, err := plain.MetaBytes()
	require.NoError(t, err)

	entries := []txmetaTestEntry{
		// Plain MetaBytes under the ADDLane action: the lane decode refuses it.
		{hash: *bad, action: txmetacache.WireActionADDLane, raw: plainBytes},
		{hash: *good, action: txmetacache.WireActionADD, meta: meta.Data{Fee: 600, SizeInBytes: 250}},
	}

	for _, tc := range []struct {
		name     string
		settings *settings.Settings
		legacy   float64
	}{
		{name: "off", settings: &settings.Settings{}, legacy: 0},
		{name: "shadow", settings: &settings.Settings{UtxoStore: settings.UtxoStoreSettings{LaneMigration: settings.LaneMigrationShadow}}, legacy: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var announced []*TxHashAndFee

			sm := &SyncManager{
				logger:   ulogger.TestLogger{},
				settings: tc.settings,
				txAnnounceBatcher: batcher.NewWithDeduplication[TxHashAndFee](100, 50*time.Millisecond, func(batch []*TxHashAndFee) {
					mu.Lock()
					announced = append(announced, batch...)
					mu.Unlock()
				}, false),
			}

			lanes, legacy := relayLaneCount(t, "lane"), relayLaneCount(t, "legacy")

			require.NoError(t, sm.processTXmetaBatchMessage(buildTXmetaBatchMessage(t, entries)))

			require.Eventually(t, func() bool {
				mu.Lock()
				defer mu.Unlock()

				return len(announced) == 1
			}, 2*time.Second, 10*time.Millisecond)

			time.Sleep(100 * time.Millisecond)

			mu.Lock()
			defer mu.Unlock()

			require.Len(t, announced, 1, "the malformed lane entry is not announced")
			require.Equal(t, *good, announced[0].TxHash, "the entry after it still is")
			require.Equal(t, uint64(600), announced[0].Fee)

			require.InDelta(t, lanes+1, relayLaneCount(t, "lane"), 0, "the malformed lane entry still counts as lane")
			require.InDelta(t, legacy+tc.legacy, relayLaneCount(t, "legacy"), 0, "a plain entry counts only at shadow")
		})
	}
}

type warnCountingLogger struct {
	ulogger.TestLogger
	warns *atomic.Int32
}

func (l warnCountingLogger) Warnf(string, ...interface{}) {
	l.warns.Add(1)
}

func TestProcessTXmetaBatchMessage_BucketsCountWarning(t *testing.T) {
	hash, err := chainhash.NewHashFromStr("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	require.NoError(t, err)

	entries := []txmetaTestEntry{{hash: *hash, action: txmetacache.WireActionDELETE}}

	newManager := func() (*SyncManager, *atomic.Int32) {
		lastTxmetaBucketsWarnNano.Store(0)

		warns := &atomic.Int32{}

		return &SyncManager{logger: warnCountingLogger{warns: warns}}, warns
	}

	t.Run("pre-buckets producer (zero header) does not warn", func(t *testing.T) {
		sm, warns := newManager()

		msg := buildTXmetaBatchMessageV2(t, entries)
		binary.LittleEndian.PutUint16(msg[txmetacache.WireV2BucketsOffset:], 0)

		for i := 0; i < 5; i++ {
			require.NoError(t, sm.processTXmetaBatchMessage(msg))
		}

		require.Zero(t, warns.Load())
	})

	t.Run("matching producer does not warn", func(t *testing.T) {
		sm, warns := newManager()

		msg := buildTXmetaBatchMessageV2(t, entries)
		require.NoError(t, sm.processTXmetaBatchMessage(msg))
		require.Zero(t, warns.Load())
	})

	t.Run("mismatched producer warns once per throttle window", func(t *testing.T) {
		sm, warns := newManager()

		msg := buildTXmetaBatchMessageV2(t, entries)
		binary.LittleEndian.PutUint16(msg[txmetacache.WireV2BucketsOffset:], uint16(txmetacache.BucketsCount*2))

		for i := 0; i < 5; i++ {
			require.NoError(t, sm.processTXmetaBatchMessage(msg))
		}

		require.Equal(t, int32(1), warns.Load())
	})
}

// Relay announces a transaction whose entry carries only its own position, which the validator
// sends at utxostore_laneMigration shadow when a parent has none, exactly as a plain one, in both
// wire formats; it skips one that arrived in a block, and one that does not check out without the
// entries after it. Every own entry counts, whatever relay's own setting.
func TestProcessTXmetaBatchMessage_OwnPositionEntriesAreAnnounced(t *testing.T) {
	relayed, err := chainhash.NewHashFromStr("1111111111111111111111111111111111111111111111111111111111111111")
	require.NoError(t, err)

	inBlock, err := chainhash.NewHashFromStr("2222222222222222222222222222222222222222222222222222222222222222")
	require.NoError(t, err)

	bad, err := chainhash.NewHashFromStr("3333333333333333333333333333333333333333333333333333333333333333")
	require.NoError(t, err)

	plainBytes, err := (&meta.Data{Fee: 700, SizeInBytes: 250}).MetaBytes()
	require.NoError(t, err)

	entries := []txmetaTestEntry{
		// Plain MetaBytes under the ADDOwn action: no position follows them, so it is refused.
		{hash: *bad, action: txmetacache.WireActionADDOwn, raw: plainBytes},
		{hash: *relayed, action: txmetacache.WireActionADDOwn, meta: meta.Data{Fee: 423, SizeInBytes: 250, LanePosition: lane.Pack(8, 14)}},
		{hash: *inBlock, action: txmetacache.WireActionADDOwn, meta: meta.Data{Fee: 500, SizeInBytes: 250, InBlock: true, LanePosition: lane.Pack(8, 15)}},
	}

	shadow := &settings.Settings{UtxoStore: settings.UtxoStoreSettings{LaneMigration: settings.LaneMigrationShadow}}

	for name, tc := range map[string]struct {
		build    func(*testing.T, []txmetaTestEntry) []byte
		settings *settings.Settings
	}{
		"v1 at off":    {buildTXmetaBatchMessage, &settings.Settings{}},
		"v2 at off":    {buildTXmetaBatchMessageV2, &settings.Settings{}},
		"v1 at shadow": {buildTXmetaBatchMessage, shadow},
		"v2 at shadow": {buildTXmetaBatchMessageV2, shadow},
	} {
		t.Run(name, func(t *testing.T) {
			var mu sync.Mutex
			var announced []*TxHashAndFee

			sm := &SyncManager{
				logger:   ulogger.TestLogger{},
				settings: tc.settings,
				txAnnounceBatcher: batcher.NewWithDeduplication[TxHashAndFee](100, 50*time.Millisecond, func(batch []*TxHashAndFee) {
					mu.Lock()
					announced = append(announced, batch...)
					mu.Unlock()
				}, false),
			}

			own, lanes, legacy := relayLaneCount(t, "own"), relayLaneCount(t, "lane"), relayLaneCount(t, "legacy")

			require.NoError(t, sm.processTXmetaBatchMessage(tc.build(t, entries)))

			require.Eventually(t, func() bool {
				mu.Lock()
				defer mu.Unlock()

				return len(announced) == 1
			}, 2*time.Second, 10*time.Millisecond)

			time.Sleep(100 * time.Millisecond)

			mu.Lock()
			defer mu.Unlock()

			require.Len(t, announced, 1, "neither the malformed entry nor the in-block one is announced")
			require.Equal(t, *relayed, announced[0].TxHash)
			require.Equal(t, uint64(423), announced[0].Fee)
			require.Equal(t, uint64(250), announced[0].Size)

			require.InDelta(t, own+3, relayLaneCount(t, "own"), 0, "every own entry counts, the malformed one too, whatever relay's setting")
			require.InDelta(t, lanes, relayLaneCount(t, "lane"), 0, "and in no other format")
			require.InDelta(t, legacy, relayLaneCount(t, "legacy"), 0)
		})
	}
}
