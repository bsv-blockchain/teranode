package aerospike

import (
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

func TestNativeOpResultBinMatchesLuaSuccess(t *testing.T) {
	if nativeOpResultBin != LuaSuccess.String() {
		t.Fatalf("native result bin %q must match Lua success bin %q",
			nativeOpResultBin, LuaSuccess.String())
	}
}

func TestEncodeNativeOpPayloadUsesSubOpAndArgsArray(t *testing.T) {
	payload, err := encodeNativeOpPayload(subOpSetLocked, []any{true})
	if err != nil {
		t.Fatalf("encode native op payload: %v", err)
	}

	var decoded []any
	if err := msgpack.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("decode native op payload: %v", err)
	}

	if len(decoded) != 2 {
		t.Fatalf("payload length = %d, want 2", len(decoded))
	}
	if decoded[0] != subOpSetLocked {
		t.Fatalf("sub op = %v (%T), want %d", decoded[0], decoded[0], subOpSetLocked)
	}

	args, ok := decoded[1].([]any)
	if !ok {
		t.Fatalf("args = %T, want []any", decoded[1])
	}
	if len(args) != 1 || args[0] != true {
		t.Fatalf("args = %#v, want []any{true}", args)
	}
}
