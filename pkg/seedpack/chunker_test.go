package seedpack

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func pseudoData(n int, seed uint64) []byte {
	out := make([]byte, n)
	x := seed
	for i := range out {
		x += 0x9e3779b97f4a7c15
		z := x
		z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
		z = (z ^ (z >> 27)) * 0x94d049bb133111eb
		z = z ^ (z >> 31)
		out[i] = byte(z)
	}
	return out
}

func testCfg() Config {
	return Config{Min: 16, Max: 256, Mask: (1 << 6) - 1}
}

func TestSplitConcatEqualsInput(t *testing.T) {
	data := pseudoData(10000, 1)
	chunks := Split(data, testCfg())

	require.Equal(t, data, bytes.Join(chunks, nil), "concatenated chunks must equal the input")
}

func TestSplitRespectsSizeBounds(t *testing.T) {
	cfg := testCfg()
	data := pseudoData(10000, 2)
	chunks := Split(data, cfg)

	require.Greater(t, len(chunks), 1, "expected multiple chunks")

	for i, c := range chunks {
		require.LessOrEqual(t, len(c), cfg.Max, "chunk %d exceeds Max", i)

		if i < len(chunks)-1 {
			require.GreaterOrEqual(t, len(c), cfg.Min, "non-final chunk %d below Min", i)
		}
	}
}

func TestSplitDeterministic(t *testing.T) {
	data := pseudoData(5000, 3)
	require.Equal(t, Split(data, testCfg()), Split(data, testCfg()))
}

func TestSplitEmptyAndTiny(t *testing.T) {
	require.Empty(t, Split(nil, testCfg()))

	tiny := []byte{1, 2, 3}
	require.Equal(t, [][]byte{tiny}, Split(tiny, testCfg()), "input smaller than Min is a single chunk")
}

func TestSplitInsertionLocality(t *testing.T) {
	cfg := testCfg()
	a := pseudoData(20000, 4)

	const at = 10000
	b := make([]byte, 0, len(a)+5)
	b = append(b, a[:at]...)
	b = append(b, []byte("XXXXX")...)
	b = append(b, a[at:]...)

	chunksA := Split(a, cfg)
	chunksB := Split(b, cfg)

	hashSet := func(chunks [][]byte) map[string]struct{} {
		m := make(map[string]struct{}, len(chunks))
		for _, c := range chunks {
			m[string(c)] = struct{}{}
		}
		return m
	}

	setB := hashSet(chunksB)

	shared := 0
	for _, c := range chunksA {
		if _, ok := setB[string(c)]; ok {
			shared++
		}
	}

	require.Greater(t, shared*2, len(chunksA), "expected >50%% of chunks to be shared after a local insertion")
}
