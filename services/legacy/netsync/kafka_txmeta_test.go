package netsync

import (
	"encoding/binary"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-batcher"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
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

		if entry.action == txmetaActionADD {
			metaBytes, err := entry.meta.MetaBytes()
			require.NoError(t, err)

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
				action: txmetaActionADD,
				meta: meta.Data{
					Fee:         0,
					SizeInBytes: 100,
					IsCoinbase:  true,
				},
			},
			{
				hash:   *regularHash,
				action: txmetaActionADD,
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
				action: txmetaActionADD,
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
				action: txmetaActionADD,
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
				action: txmetaActionDELETE,
			},
			{
				hash:   *regularHash,
				action: txmetaActionADD,
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
