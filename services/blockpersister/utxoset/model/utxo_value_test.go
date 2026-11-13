package model_test

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"runtime"
	"testing"

	"github.com/bsv-blockchain/teranode/services/blockpersister/utxoset/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewUTXOValue validates the creation of new UTXO values
// Tests basic value assignment and retrieval
func TestNewUTXOValue(t *testing.T) {
	script := []byte{0x00, 0x01, 0x02, 0x03, 0x04}

	utxo := model.NewUTXOValue(1000, 42, script)

	assert.Equal(t, script, utxo.Script)
	assert.Equal(t, uint64(1000), utxo.Value)
	assert.Equal(t, uint32(42), utxo.Locktime)
	assert.Equal(t, 5, len(utxo.Script))
}

// TestEqual validates the equality comparison functionality of UTXO values
// Ensures that equal values are correctly identified and different values are distinguished
func TestEqual(t *testing.T) {
	script1 := []byte{0x00, 0x01, 0x02, 0x03, 0x04}
	script2 := []byte{0x00, 0x01, 0x02, 0x03, 0x04}

	utxo1 := model.NewUTXOValue(1000, 42, script1)
	utxo2 := model.NewUTXOValue(1000, 42, script2)
	utxo3 := model.NewUTXOValue(1000, 43, script1)

	assert.Equal(t, utxo1, utxo2)
	assert.True(t, utxo1.Equal(utxo2))
	assert.NotEqual(t, utxo1, utxo3)
	assert.False(t, utxo1.Equal(utxo3))
}

// TestNewUTXOValueFromBytes validates the serialization and deserialization of UTXO values
// Ensures that UTXO values can be correctly converted to and from bytes
func TestNewUTXOValueFromBytes(t *testing.T) {
	script := []byte{0x00, 0x01, 0x02, 0x03, 0x04}

	utxo := model.NewUTXOValue(1000, 42, script)

	b := utxo.Bytes()

	utxo2, err := model.NewUTXOValueFromBytes(b)
	require.NoError(t, err)

	assert.Equal(t, utxo, utxo2)
}

// TestNewUTXOValueFromReader validates reading UTXO values from an io.Reader
// Tests both single and multiple value reading scenarios
func TestNewUTXOValueFromReader(t *testing.T) {
	script1 := []byte{0x00, 0x01, 0x02, 0x03, 0x04}
	script2 := []byte{0x05, 0x06, 0x07, 0x08, 0x09}

	utxo1 := model.NewUTXOValue(1001, 42, script1)
	utxo2 := model.NewUTXOValue(1002, 0, script2)

	assert.NotEqual(t, utxo1, utxo2)

	// Test the Write(io.Writer) method
	buf := new(bytes.Buffer)
	w := bufio.NewWriter(buf)

	err := utxo1.Write(w)
	require.NoError(t, err)

	err = utxo2.Write(w)
	require.NoError(t, err)

	err = w.Flush()
	require.NoError(t, err)

	// Test the Read(io.Reader) method
	r := bufio.NewReader(buf)

	utxo3, err := model.NewUTXOValueFromReader(r)
	require.NoError(t, err)

	utxo4, err := model.NewUTXOValueFromReader(r)
	require.NoError(t, err)

	assert.Equal(t, utxo1, utxo3)
	assert.Equal(t, utxo2, utxo4)
	assert.NotEqual(t, utxo1, utxo4)
}

// maxExactScriptAlloc mirrors the unexported bound in the model package: script
// lengths at or below it are read into an exactly-sized buffer, above it they
// are streamed.
const maxExactScriptAlloc = 64 * 1024

// TestNewUTXOValueFromBytes_TruncatedReturnsError exercises the bounds checks
// on the byte-slice decoder. b is untrusted, so a buffer shorter than the
// 16-byte header, or one whose declared script length runs past the buffer,
// must return an error rather than panic with a slice-out-of-range.
func TestNewUTXOValueFromBytes_TruncatedReturnsError(t *testing.T) {
	// A well-formed value we can truncate.
	full := model.NewUTXOValue(1000, 42, []byte{0x00, 0x01, 0x02, 0x03, 0x04}).Bytes()

	cases := []struct {
		name string
		b    []byte
	}{
		{"nil", nil},
		{"empty", []byte{}},
		{"partial value", make([]byte, 4)},
		{"header one byte short", make([]byte, 15)},
		{"header only, declares 5-byte script, none present", full[:16]},
		{"script truncated by one byte", full[:len(full)-1]},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var (
				u   *model.UTXOValue
				err error
			)

			// The pre-fix code panicked here; the fix must turn that into an error.
			require.NotPanics(t, func() {
				u, err = model.NewUTXOValueFromBytes(tc.b)
			})

			require.Error(t, err)
			require.Nil(t, u)
		})
	}
}

// TestNewUTXOValueFromBytes_RoundTrip confirms a complete buffer still decodes
// to the original value after the bounds checks were added.
func TestNewUTXOValueFromBytes_RoundTrip(t *testing.T) {
	in := model.NewUTXOValue(5000000000, 7, []byte{0x76, 0xa9, 0x88, 0xac})

	out, err := model.NewUTXOValueFromBytes(in.Bytes())
	require.NoError(t, err)
	require.True(t, in.Equal(out))
}

// TestNewUTXOValueFromReader_HostileScriptLengthIsBounded pins that decoding a
// record whose declared script length is enormous does NOT allocate
// proportionally to that untrusted field. Before the fix the reader did
// make([]byte, length), so a tiny crafted record declaring a multi-GB script
// could force a multi-GB allocation. After the fix the allocation tracks the
// bytes actually present, so a header-only record must allocate well under a
// few MB before erroring out.
func TestNewUTXOValueFromReader_HostileScriptLengthIsBounded(t *testing.T) {
	const (
		hugeLen  = 1 << 25         // 33 MB; pre-sizing the script would cost ~33 MB
		maxAlloc = uint64(4 << 20) // 4 MB — far above a bounded parse, far below the bug
	)

	data := utxoValueReaderBytes(hugeLen, nil) // declares 33 MB script, no body

	alloc := totalAllocDelta(func() {
		u, err := model.NewUTXOValueFromReader(bytes.NewReader(data))
		require.Error(t, err, "a record declaring %d script bytes with none present must not parse", hugeLen)
		require.Nil(t, u)
	})

	require.Less(t, alloc, maxAlloc,
		"decoding a %d-byte input allocated %d bytes — allocation is sized from the untrusted length field, not the available input",
		len(data), alloc)
}

// TestNewUTXOValueFromReader_ScriptAtAllocBoundary checks that scripts on either
// side of maxExactScriptAlloc (the exact-alloc path vs the streamed path) decode
// to the same bytes.
func TestNewUTXOValueFromReader_ScriptAtAllocBoundary(t *testing.T) {
	for _, size := range []uint32{maxExactScriptAlloc, maxExactScriptAlloc + 1} {
		script := make([]byte, size)
		for i := range script {
			script[i] = byte(i)
		}

		data := utxoValueReaderBytes(size, script)

		u, err := model.NewUTXOValueFromReader(bytes.NewReader(data))
		require.NoError(t, err, "size %d", size)
		require.Equal(t, script, u.Script, "size %d", size)
	}
}

// FuzzNewUTXOValueFromBytes exercises the byte-slice decoder against arbitrary
// input. The decoder must never panic, and whatever it parses must re-serialize
// to a prefix of its input (no over- or under-read).
func FuzzNewUTXOValueFromBytes(f *testing.F) {
	f.Add(model.NewUTXOValue(100, 0, []byte{0x76, 0xa9, 0x88, 0xac}).Bytes())
	f.Add([]byte{})
	f.Add(make([]byte, 16))
	f.Add(utxoValueReaderBytes(0xFFFFFFFF, nil)[1:]) // header claiming a 4 GB script, no body

	f.Fuzz(func(t *testing.T, data []byte) {
		u, err := model.NewUTXOValueFromBytes(data)
		if err != nil {
			return
		}

		require.NotNil(t, u)
		require.True(t, bytes.HasPrefix(data, u.Bytes()),
			"re-serialized value (%d bytes) is not a prefix of the %d-byte input", len(u.Bytes()), len(data))
	})
}

// utxoValueReaderBytes builds the reader wire format: a 0x01 non-nil marker,
// then value (8) + locktime (4) + script length (4), then scriptLen's script
// bytes taken from script (which may be shorter than scriptLen, or nil, to
// model a truncated/crafted record).
func utxoValueReaderBytes(scriptLen uint32, script []byte) []byte {
	var buf bytes.Buffer

	buf.WriteByte(0x01)                                    // non-nil marker
	_ = binary.Write(&buf, binary.LittleEndian, uint64(0)) // value
	_ = binary.Write(&buf, binary.LittleEndian, uint32(0)) // locktime
	_ = binary.Write(&buf, binary.LittleEndian, scriptLen) // declared script length
	buf.Write(script)

	return buf.Bytes()
}

// totalAllocDelta runs fn and returns how many bytes were allocated during it.
// runtime.TotalAlloc is monotonic, so the delta is a stable measure unaffected
// by GC.
func totalAllocDelta(fn func()) uint64 {
	var before, after runtime.MemStats

	runtime.ReadMemStats(&before)
	fn()
	runtime.ReadMemStats(&after)

	return after.TotalAlloc - before.TotalAlloc
}
