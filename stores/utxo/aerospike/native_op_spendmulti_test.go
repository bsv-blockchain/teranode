package aerospike

import (
	"bytes"
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/vmihailenco/msgpack/v5"
)

// TestSpendMultiNativePayloadRoundTrip locks the native-path wire contract for
// spendMulti — the only call site that passes a non-scalar arg
// ([]aerospike.MapValue) through encodeNativeOpPayload.
//
// It is the permanent guard for the concern raised on the PR that MapValue
// "wraps its data in an unexported field" and would msgpack-marshal to an empty
// map, silently dropping offset/vOut/utxoHash/spendingData. That is a false
// positive: aerospike.MapValue's underlying type is map[any]any, so msgpack
// dispatches on reflect.Kind()==Map and encodes every entry. This test drives
// the real createSpendMapValue + encodeNativeOpPayload so a future change to
// either (the field set, the MapValue type, or the encoder) that breaks the
// contract fails here rather than corrupting spends against the server.
func TestSpendMultiNativePayloadRoundTrip(t *testing.T) {
	s := &Store{utxoBatchSize: 128}

	utxoHash := &chainhash.Hash{0xaa, 0xbb, 0xcc, 0xdd}
	sd := spendpkg.NewSpendingData(&chainhash.Hash{0x09, 0x09, 0x09}, 7)

	bItem := &batchSpend{spend: &utxo.Spend{
		TxID:         &chainhash.Hash{0x01, 0x02, 0x03},
		Vout:         5,
		UTXOHash:     utxoHash,
		SpendingData: sd,
	}}

	const idx = 3
	batchItems := []aerospike.MapValue{s.createSpendMapValue(idx, bItem)}

	// Args exactly as createBatchRecords passes them to teranodeBatchRecord.
	const (
		ignoreConflicting = false
		ignoreLocked      = true
		blockHeight       = uint32(815000)
		retention         = uint32(288)
	)

	payload, err := encodeNativeOpPayload(subOpSpendMulti, []any{
		batchItems, ignoreConflicting, ignoreLocked, blockHeight, retention,
	})
	if err != nil {
		t.Fatalf("encode spendMulti payload: %v", err)
	}

	var decoded []any
	if err := msgpack.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("decode spendMulti payload: %v", err)
	}

	// Envelope is [sub_op, [args...]].
	if len(decoded) != 2 {
		t.Fatalf("payload length = %d, want 2", len(decoded))
	}
	if got := toInt64(t, decoded[0]); got != int64(subOpSpendMulti) {
		t.Fatalf("sub op = %d, want %d", got, subOpSpendMulti)
	}

	args, ok := decoded[1].([]any)
	if !ok || len(args) != 5 {
		t.Fatalf("args = %#v, want 5 elements", decoded[1])
	}

	// Scalar args survive in order.
	if b, ok := args[1].(bool); !ok || b != ignoreConflicting {
		t.Fatalf("args[1] (ignoreConflicting) = %#v, want %v", args[1], ignoreConflicting)
	}
	if b, ok := args[2].(bool); !ok || b != ignoreLocked {
		t.Fatalf("args[2] (ignoreLocked) = %#v, want %v", args[2], ignoreLocked)
	}
	if got := toInt64(t, args[3]); got != int64(blockHeight) {
		t.Fatalf("args[3] (blockHeight) = %d, want %d", got, blockHeight)
	}
	if got := toInt64(t, args[4]); got != int64(retention) {
		t.Fatalf("args[4] (retention) = %d, want %d", got, retention)
	}

	// The crux: batchItems is an array of maps, and the map is NOT empty.
	items, ok := args[0].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("batchItems = %#v, want 1 element", args[0])
	}
	m, ok := items[0].(map[string]any)
	if !ok {
		t.Fatalf("spend item = %T, want map[string]any", items[0])
	}
	if len(m) != 5 {
		t.Fatalf("spend map has %d keys, want 5 — msgpack must not drop fields: %#v", len(m), m)
	}

	// Every field round-trips faithfully.
	if got := toInt64(t, m["idx"]); got != idx {
		t.Fatalf("idx = %d, want %d", got, idx)
	}
	if got := toInt64(t, m["vOut"]); got != int64(bItem.spend.Vout) {
		t.Fatalf("vOut = %d, want %d", got, bItem.spend.Vout)
	}
	if got, want := toInt64(t, m["offset"]), int64(s.calculateOffsetForOutput(bItem.spend.Vout)); got != want {
		t.Fatalf("offset = %d, want %d", got, want)
	}
	if got, _ := m["utxoHash"].([]byte); !bytes.Equal(got, utxoHash[:]) {
		t.Fatalf("utxoHash = %x, want %x", got, utxoHash[:])
	}
	if got, _ := m["spendingData"].([]byte); !bytes.Equal(got, sd.Bytes()) {
		t.Fatalf("spendingData = %x, want %x", got, sd.Bytes())
	}
}

// toInt64 coerces a msgpack-decoded numeric (which may be any int/uint width)
// into int64 for comparison.
func toInt64(t *testing.T, v any) int64 {
	t.Helper()

	switch n := v.(type) {
	case int8:
		return int64(n)
	case int16:
		return int64(n)
	case int32:
		return int64(n)
	case int64:
		return n
	case uint8:
		return int64(n)
	case uint16:
		return int64(n)
	case uint32:
		return int64(n)
	case uint64:
		return int64(n)
	default:
		t.Fatalf("value %#v (%T) is not a decoded integer", v, v)
		return 0
	}
}
