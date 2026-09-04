package bridge

import (
	"context"
	"encoding/binary"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/txmetacache"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	inmemorykafka "github.com/bsv-blockchain/teranode/util/kafka/in_memory_kafka"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// txmetaTestEntry and the two batch builders below are relocated verbatim
// from services/legacy/netsync/kafka_txmeta_test.go (buildTXmetaBatchMessage,
// buildTXmetaBatchMessageV2, txmetaTestEntry) per the plan's "reuse legacy
// test vectors verbatim if relocatable" — this package decodes the exact
// same wire format, so the same byte-builders exercise it.
type txmetaTestEntry struct {
	hash   chainhash.Hash
	action byte
	meta   meta.Data
}

func buildTXmetaBatchMessage(t *testing.T, entries []txmetaTestEntry) []byte {
	t.Helper()

	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(len(entries))) //nolint:gosec // test data

	for _, entry := range entries {
		buf = append(buf, entry.hash[:]...)
		buf = append(buf, entry.action)

		if entry.action == txmetacache.WireActionADD {
			metaBytes, err := entry.meta.MetaBytes()
			require.NoError(t, err)

			lenBuf := make([]byte, 4)
			binary.LittleEndian.PutUint32(lenBuf, uint32(len(metaBytes))) //nolint:gosec // test data
			buf = append(buf, lenBuf...)
			buf = append(buf, metaBytes...)
		} else {
			buf = append(buf, 0, 0, 0, 0)
		}
	}

	return buf
}

func buildTXmetaBatchMessageV2(t *testing.T, entries []txmetaTestEntry) []byte {
	t.Helper()

	buf := make([]byte, 8)
	buf[0] = txmetacache.WireV2Magic
	buf[1] = txmetacache.WireV2Version
	binary.LittleEndian.PutUint32(buf[4:], uint32(len(entries))) //nolint:gosec // test data

	for _, entry := range entries {
		buf = append(buf, 0, 0, 0, 0, 0, 0, 0, 0)
		buf = append(buf, entry.hash[:]...)
		buf = append(buf, entry.action)

		if entry.action == txmetacache.WireActionADD {
			metaBytes, err := entry.meta.MetaBytes()
			require.NoError(t, err)

			lenBuf := make([]byte, 4)
			binary.LittleEndian.PutUint32(lenBuf, uint32(len(metaBytes))) //nolint:gosec // test data
			buf = append(buf, lenBuf...)
			buf = append(buf, metaBytes...)
		} else {
			buf = append(buf, 0, 0, 0, 0)
		}
	}

	return buf
}

func mustHash(t *testing.T, hex string) chainhash.Hash {
	t.Helper()

	h, err := chainhash.NewHashFromStr(hex)
	require.NoError(t, err)

	return *h
}

// TestDecodeTxMetaBatch_V1AndV2 is the plan's "batch decode tables for v1
// and v2" (Step 1): the same three logical entries (ADD, ADD, DELETE),
// encoded once in each wire format, must decode to the same
// hash/action/content triples regardless of which format carried them.
func TestDecodeTxMetaBatch_V1AndV2(t *testing.T) {
	hashA := mustHash(t, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	hashB := mustHash(t, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	hashC := mustHash(t, "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")

	metaA := meta.Data{Fee: 500, SizeInBytes: 250}
	metaAddBytes, err := metaA.MetaBytes()
	require.NoError(t, err)

	entries := []txmetaTestEntry{
		{hash: hashA, action: txmetacache.WireActionADD, meta: metaA},
		{hash: hashB, action: txmetacache.WireActionADD, meta: meta.Data{Fee: 1, SizeInBytes: 200, IsCoinbase: true}},
		{hash: hashC, action: txmetacache.WireActionDELETE},
	}

	metaBBytes, err := entries[1].meta.MetaBytes()
	require.NoError(t, err)

	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"v1", buildTXmetaBatchMessage(t, entries)},
		{"v2", buildTXmetaBatchMessageV2(t, entries)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			decoded, err := decodeTxMetaBatch(tc.data)
			require.NoError(t, err)
			require.Len(t, decoded, 3)

			require.Equal(t, hashA, decoded[0].hash)
			require.Equal(t, txmetacache.WireActionADD, decoded[0].action)
			require.Equal(t, metaAddBytes, decoded[0].content)

			require.Equal(t, hashB, decoded[1].hash)
			require.Equal(t, txmetacache.WireActionADD, decoded[1].action)
			require.Equal(t, metaBBytes, decoded[1].content)

			require.Equal(t, hashC, decoded[2].hash)
			require.Equal(t, txmetacache.WireActionDELETE, decoded[2].action)
			require.Empty(t, decoded[2].content)
		})
	}
}

// TestDecodeTxMetaBatch_WrongVersionByteFallsBackToV1 covers the signature
// half of v2 detection: a v1 message whose leading entry-count bytes happen
// to start with 0xFF (byte[0], the v2 magic) must still decode as v1 when
// byte[1] is not WireV2Version, because the 4-byte signature test
// (magic+version+2 reserved bytes) fails before the
// candidateCount*WireV2MinEntrySize<=remaining plausibility check ever runs.
// The plausibility check itself has its own test,
// TestDecodeTxMetaBatch_ImplausibleV2CountFallsBackToV1 — review round 1,
// Important 2 found this test's PREVIOUS version claimed to cover that
// check (its own doc comment described forcing a count byte) but never
// actually built a buffer with byte[1] == WireV2Version, so the
// plausibility branch was never reached; deleting that check left this test
// passing. Renamed and re-scoped to what it actually exercises.
func TestDecodeTxMetaBatch_WrongVersionByteFallsBackToV1(t *testing.T) {
	// 255 real v1 entries: entryCount=255 LE-encodes to [0xFF, 0x00, 0x00, 0x00]
	// — byte[0] genuinely is the v2 magic byte, purely as a consequence of the
	// entry count's value, and byte[1] is 0x00, not WireV2Version. This is the
	// exact ambiguity the signature test (magic AND version AND 2 reserved
	// bytes) exists to resolve before the plausibility check ever runs.
	const entryCount = 255

	hash := mustHash(t, "1111111111111111111111111111111111111111111111111111111111111111")

	entries := make([]txmetaTestEntry, entryCount)
	for i := range entries {
		entries[i] = txmetaTestEntry{hash: hash, action: txmetacache.WireActionDELETE}
	}

	data := buildTXmetaBatchMessage(t, entries)
	require.Equal(t, txmetacache.WireV2Magic, data[0], "fixture must start with the v2 magic byte to exercise the signature check at all")
	require.NotEqual(t, txmetacache.WireV2Version, data[1], "fixture must fail the signature check on the version byte, not the plausibility check")

	decoded, err := decodeTxMetaBatch(data)
	require.NoError(t, err)
	require.Len(t, decoded, entryCount)

	for _, e := range decoded {
		require.Equal(t, hash, e.hash)
	}
}

// TestDecodeTxMetaBatch_ImplausibleV2CountFallsBackToV1 is the plausibility
// check itself (review round 1, Important 2): a buffer that DOES carry a
// genuine v2 signature (magic 0xFF, version 0x02, 2 reserved zero bytes)
// but whose claimed entry count is implausible for the buffer's length
// must still decode as v1, not be accepted as a (garbage) v2 message.
//
// Proven by actually succeeding as v1, not merely by not erroring: the
// buffer's first 4 bytes are fixed by the v2 signature requirement — magic
// 0xFF, version 0x02, then 2 zero bytes — which, read back as a v1 LE
// uint32 entry count (decodeTxMetaBatch's v1 fallback reads exactly those
// same 4 bytes as its own count field), is 0x000002FF = 767. So the buffer
// is built with exactly 767 real v1-shaped DELETE entries following those 4
// bytes; a correct fallback to v1 decodes all 767 of them cleanly. Bytes
// [4:8] — read under a (wrongly-taken) v2 interpretation as the claimed
// entry count — are the first 4 bytes of entry 0's hash, deliberately
// non-zero (0xAA repeating) so that count, read as a v2 candidateCount, is
// astronomically implausible for this buffer's real length: the
// plausibility check must reject it and fall back to v1. If the
// plausibility check were removed, decodeTxMetaBatch would instead attempt
// a v2 parse with that huge candidateCount and fail truncation on its very
// first entry — a v1-fallback bug here flips this test from a 767-entry
// success to an error, which is what makes the two paths distinguishable.
func TestDecodeTxMetaBatch_ImplausibleV2CountFallsBackToV1(t *testing.T) {
	const entryCount = 767 // 0xFF + 0x02<<8, the value the v2 signature's own first 4 bytes decode to as a v1 LE uint32 count

	var hash chainhash.Hash
	for i := range hash {
		hash[i] = 0xAA
	}

	entries := make([]txmetaTestEntry, entryCount)
	for i := range entries {
		entries[i] = txmetaTestEntry{hash: hash, action: txmetacache.WireActionDELETE}
	}

	data := buildTXmetaBatchMessage(t, entries)

	require.Equal(t, txmetacache.WireV2Magic, data[0])
	require.Equal(t, txmetacache.WireV2Version, data[1], "fixture must carry a genuine v2 signature to exercise the plausibility check, not the signature check")
	require.Equal(t, byte(0), data[2])
	require.Equal(t, byte(0), data[3])

	decoded, err := decodeTxMetaBatch(data)
	require.NoError(t, err, "an implausible v2 count must fall back to a successful v1 decode, not error out as a rejected v2 message")
	require.Len(t, decoded, entryCount)

	for _, e := range decoded {
		require.Equal(t, hash, e.hash)
		require.Equal(t, txmetacache.WireActionDELETE, e.action)
	}
}

func TestDecodeTxMetaBatch_Truncation(t *testing.T) {
	hash := mustHash(t, "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd")

	valid := buildTXmetaBatchMessage(t, []txmetaTestEntry{
		{hash: hash, action: txmetacache.WireActionADD, meta: meta.Data{Fee: 1, SizeInBytes: 1}},
	})

	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"shorter than entry count header", []byte{0x01, 0x02}},
		{"truncated mid-entry header", valid[:6]},
		{"truncated mid-content", valid[:len(valid)-1]},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := decodeTxMetaBatch(tc.data)
			require.Error(t, err)
		})
	}
}

// TestDecodeTxMetaBatch_HugeEntryCountFailsFastRatherThanAllocating guards
// against an allocation DoS: a v1 message that CLAIMS an enormous entry
// count but carries none of the bytes to back it must fail the truncation
// check on the very first iteration, not try to reserve a slice sized by
// that claimed count. Run with a tight overall test timeout via `go test
// -timeout` in the exit gate; a regression here manifests as this single
// test either OOMing or taking a very long time, not a normal assertion
// failure — timing this test directly instead is not float-flaky since it
// is asserting on unreachable code, not on wall-clock behaviour.
func TestDecodeTxMetaBatch_HugeEntryCountFailsFastRatherThanAllocating(t *testing.T) {
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, 0xFFFFFFF0) // huge, implausible entry count

	done := make(chan struct{})

	go func() {
		_, err := decodeTxMetaBatch(data)
		require.Error(t, err)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("decodeTxMetaBatch did not return promptly against a huge claimed entry count")
	}
}

// TestHandleTxMetaMessage_CoinbaseAndInBlockSkip is the plan's Step 1
// coinbase-skip requirement, plus the InBlock skip legacy carries alongside
// it (netsync/manager.go:3596-3601, PR 1073): of four decoded ADD entries —
// plain, coinbase, in-block, and delete — only the plain one reaches onTx.
func TestHandleTxMetaMessage_CoinbaseAndInBlockSkip(t *testing.T) {
	plainHash := mustHash(t, "1111111111111111111111111111111111111111111111111111111111111111")
	coinbaseHash := mustHash(t, "2222222222222222222222222222222222222222222222222222222222222222")
	inBlockHash := mustHash(t, "3333333333333333333333333333333333333333333333333333333333333333")
	deletedHash := mustHash(t, "4444444444444444444444444444444444444444444444444444444444444444")

	data := buildTXmetaBatchMessage(t, []txmetaTestEntry{
		{hash: plainHash, action: txmetacache.WireActionADD, meta: meta.Data{Fee: 500, SizeInBytes: 250}},
		{hash: coinbaseHash, action: txmetacache.WireActionADD, meta: meta.Data{Fee: 0, SizeInBytes: 100, IsCoinbase: true}},
		{hash: inBlockHash, action: txmetacache.WireActionADD, meta: meta.Data{Fee: 10, SizeInBytes: 300, InBlock: true}},
		{hash: deletedHash, action: txmetacache.WireActionDELETE},
	})

	var got []struct {
		hash chainhash.Hash
		fee  uint64
		size uint64
	}

	err := handleTxMetaMessage(ulogger.TestLogger{}, &kafka.KafkaMessage{Value: data}, nil, func(hash chainhash.Hash, fee, size uint64) {
		got = append(got, struct {
			hash chainhash.Hash
			fee  uint64
			size uint64
		}{hash, fee, size})
	})
	require.NoError(t, err)

	require.Len(t, got, 1, "only the plain, non-coinbase, not-in-block ADD entry should reach onTx")
	require.Equal(t, plainHash, got[0].hash)
	require.Equal(t, uint64(500), got[0].fee)
	require.Equal(t, uint64(250), got[0].size)
}

// TestHandleTxMetaMessage_UnparseableMessageIsSkippedNotRetried mirrors
// handleBlocksFinalMessage's own never-going-to-become-parseable discipline:
// a message decodeTxMetaBatch cannot parse logs and returns nil, not an
// error, so the Kafka consumer never retries it.
func TestHandleTxMetaMessage_UnparseableMessageIsSkippedNotRetried(t *testing.T) {
	err := handleTxMetaMessage(ulogger.TestLogger{}, &kafka.KafkaMessage{Value: []byte{0x01}}, nil, func(chainhash.Hash, uint64, uint64) {
		t.Fatal("onTx must not be called for an unparseable message")
	})
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// StartTxMetaConsumer: wire-level, real in-memory Kafka + a controllable FSM.
// ---------------------------------------------------------------------------

// recordingTxHandler collects every TxMetaHandler call so a test can assert
// positively on what arrived. Delivery here goes through the real in-memory
// Kafka consumer and the FSM poller's own 1-second tick, so tests against it
// use require.Eventually against a positive condition (E5) rather than a
// fixed sleep-then-check.
type recordingTxHandler struct {
	mu    sync.Mutex
	calls []chainhash.Hash
}

func (r *recordingTxHandler) handle(hash chainhash.Hash, _ uint64, _ uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.calls = append(r.calls, hash)
}

func (r *recordingTxHandler) hashes() []chainhash.Hash {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]chainhash.Hash(nil), r.calls...)
}

func produceTxMeta(t *testing.T, topic string, entries []txmetaTestEntry) {
	t.Helper()

	require.Eventually(t, func() bool {
		return inmemorykafka.GetSharedBroker().HasConsumer(topic)
	}, 5*time.Second, 10*time.Millisecond, "the txmeta consumer never subscribed")

	require.NoError(t, inmemorykafka.GetSharedBroker().Produce(context.Background(), topic, nil, buildTXmetaBatchMessage(t, entries)))
}

// TestStartTxMetaConsumer_RunningGateFlipsBothDirections is the plan's Step
// 1 RUNNING-gate requirement (spec §7, "Relay txs only in RUNNING"), proven
// against the real controlled listener and the real (now genuinely
// stoppable — see the E4 fix above) in-memory Kafka fake, flipping the FSM
// in BOTH directions (E5): not just "off produces nothing", which cannot
// distinguish correctly-gated from merely-not-yet-started, but "on than off
// than on again", each transition proven by a positive arrival.
func TestStartTxMetaConsumer_RunningGateFlipsBothDirections(t *testing.T) {
	const topic = "txmeta-running-gate-test"

	tSettings := settings.NewSettings()
	tSettings.ClientName = "svp2p-test-runninggate"

	kafkaURL, err := url.Parse("memory://localhost:9092/" + topic)
	require.NoError(t, err)
	tSettings.Kafka.TxMetaConfig = kafkaURL

	t.Cleanup(func() { inmemorykafka.GetSharedBroker().DropTopic(topic) })

	blockchainClient := &blockchain.Mock{}
	fsmCall := blockchainClient.Mock.On("IsFSMCurrentState", mock.Anything, mock.Anything).Return(false, nil)

	handler := &recordingTxHandler{}

	// A live recent-transaction index rides along, so this test also proves
	// the compact-block feed reaches the index through the real consumer
	// (Kafka, gate, decode), not only through handleTxMetaMessage.
	idx := NewRecentTxIndex(16, noFetch)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	StartTxMetaConsumer(ctx, ulogger.TestLogger{}, tSettings, blockchainClient, idx, handler.handle)

	// Not RUNNING: the consumer must never even subscribe, let alone
	// deliver anything, while the poller keeps reporting false.
	require.Never(t, func() bool {
		return inmemorykafka.GetSharedBroker().HasConsumer(topic)
	}, 1500*time.Millisecond, 50*time.Millisecond, "the txmeta consumer must not run while the FSM is not RUNNING")

	// Flip to RUNNING: within a couple of poll ticks the controlled
	// listener must start, and a produced tx must actually arrive.
	fsmCall.Return(true, nil)

	firstHash := mustHash(t, "1111111111111111111111111111111111111111111111111111111111111111")
	produceTxMeta(t, topic, []txmetaTestEntry{
		{hash: firstHash, action: txmetacache.WireActionADD, meta: meta.Data{Fee: 500, SizeInBytes: 250}},
	})

	require.Eventually(t, func() bool {
		for _, h := range handler.hashes() {
			if h == firstHash {
				return true
			}
		}

		return false
	}, 5*time.Second, 20*time.Millisecond, "the RUNNING transition must let a produced tx actually arrive")

	// Flip back off: the consumer must actually stop — proven by the broker
	// itself, not by absence of a handler call, because HasConsumer false
	// means the broker has nobody to even attempt delivery to.
	fsmCall.Return(false, nil)

	require.Eventually(t, func() bool {
		return !inmemorykafka.GetSharedBroker().HasConsumer(topic)
	}, 5*time.Second, 20*time.Millisecond, "the not-RUNNING transition must actually stop the controlled listener")

	// Flip on again: a SECOND tx must arrive, proving the gate re-enables
	// the listener rather than having latched permanently off.
	fsmCall.Return(true, nil)

	secondHash := mustHash(t, "2222222222222222222222222222222222222222222222222222222222222222")
	produceTxMeta(t, topic, []txmetaTestEntry{
		{hash: secondHash, action: txmetacache.WireActionADD, meta: meta.Data{Fee: 500, SizeInBytes: 250}},
	})

	require.Eventually(t, func() bool {
		for _, h := range handler.hashes() {
			if h == secondHash {
				return true
			}
		}

		return false
	}, 5*time.Second, 20*time.Millisecond, "the second RUNNING transition must let a produced tx actually arrive again")

	require.Equal(t, 2, idx.Len(), "both delivered transactions must have entered the recent-transaction index")
	require.True(t, heldByIndex(idx, firstHash))
	require.True(t, heldByIndex(idx, secondHash))
}

// TestStartTxMetaConsumer_ReplayDisabledOnTheURL is E2: the URL the
// consumer actually subscribes with must carry replay=0, mirroring legacy's
// own rewrite exactly.
func TestStartTxMetaConsumer_ReplayDisabledOnTheURL(t *testing.T) {
	tSettings := settings.NewSettings()
	tSettings.ClientName = "svp2p-test-replay"

	kafkaURL, err := url.Parse("memory://localhost:9092/replay-check-topic")
	require.NoError(t, err)
	tSettings.Kafka.TxMetaConfig = kafkaURL

	blockchainClient := &blockchain.Mock{}
	blockchainClient.Mock.On("IsFSMCurrentState", mock.Anything, mock.Anything).Return(true, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	StartTxMetaConsumer(ctx, ulogger.TestLogger{}, tSettings, blockchainClient, nil, func(chainhash.Hash, uint64, uint64) {})

	require.Equal(t, "0", tSettings.Kafka.TxMetaConfig.Query().Get("replay"))
}

// TestStartTxMetaConsumer_NilConfigDoesNothing mirrors
// StartBlocksFinalConsumer's own "topic not configured" case: no consumer
// is started at all, and StartTxMetaConsumer returns without blocking or
// panicking on a nil blockchain client (the poller is never even started).
func TestStartTxMetaConsumer_NilConfigDoesNothing(t *testing.T) {
	tSettings := settings.NewSettings()
	tSettings.ClientName = "svp2p-test-nilconfig"
	tSettings.Kafka.TxMetaConfig = nil

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NotPanics(t, func() {
		StartTxMetaConsumer(ctx, ulogger.TestLogger{}, tSettings, nil, nil, func(chainhash.Hash, uint64, uint64) {
			t.Fatal("onTx must never be called when the topic is not configured")
		})
	})
}
