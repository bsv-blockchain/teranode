package teranodecli

import (
	"flag"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUint32Flag(t *testing.T) {
	t.Run("accepts valid heights", func(t *testing.T) {
		for _, in := range []struct {
			arg  string
			want uint32
		}{
			{"0", 0},
			{"300", 300},
			{"4294967295", 4294967295}, // math.MaxUint32
		} {
			fs := flag.NewFlagSet("test", flag.ContinueOnError)
			h := uint32Flag(fs, "end-height", 0, "")

			require.NoError(t, fs.Parse([]string{"--end-height", in.arg}))
			require.Equal(t, in.want, *h)
		}
	})

	t.Run("rejects out-of-range instead of truncating", func(t *testing.T) {
		// 4294967297 would silently truncate to 1 with a plain uint flag.
		for _, arg := range []string{"4294967296", "4294967297", "-1", "abc"} {
			fs := flag.NewFlagSet("test", flag.ContinueOnError)
			fs.SetOutput(&nopWriter{})
			uint32Flag(fs, "end-height", 0, "")

			require.Error(t, fs.Parse([]string{"--end-height", arg}), "arg %q must be rejected", arg)
		}
	})

	t.Run("default is used when flag absent", func(t *testing.T) {
		fs := flag.NewFlagSet("test", flag.ContinueOnError)
		h := uint32Flag(fs, "end-height", 7, "")

		require.NoError(t, fs.Parse(nil))
		require.Equal(t, uint32(7), *h)
	})
}

type nopWriter struct{}

func (*nopWriter) Write(p []byte) (int, error) { return len(p), nil }
