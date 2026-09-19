package cohort

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestIDPredicates(t *testing.T) {
	tests := []struct {
		name      string
		id        ID
		unset     bool
		sentinel  bool
		synthetic bool
		clock     bool
	}{
		{name: "unset", id: 0, unset: true, sentinel: true},
		{name: "historical", id: 1, sentinel: true},
		{name: "born mined", id: 2, sentinel: true},
		{name: "first synthetic", id: 3, synthetic: true},
		{name: "mid synthetic", id: 1000, synthetic: true},
		{name: "last synthetic", id: GenesisTime - 1, synthetic: true},
		{name: "genesis second", id: GenesisTime, clock: true},
		{name: "later second", id: GenesisTime + 1, clock: true},
		{name: "max uint32", id: math.MaxUint32, clock: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.unset, test.id.IsUnset(), "IsUnset")
			require.Equal(t, test.sentinel, test.id.IsSentinel(), "IsSentinel")
			require.Equal(t, test.synthetic, test.id.IsSynthetic(), "IsSynthetic")
			require.Equal(t, test.clock, test.id.IsClock(), "IsClock")

			// The four ranges are exclusive: exactly one of sentinel, synthetic
			// and clock holds for every possible value.
			classes := 0
			for _, in := range []bool{test.sentinel, test.synthetic, test.clock} {
				if in {
					classes++
				}
			}

			require.Equal(t, 1, classes, "expected exactly one class to hold for %d", uint32(test.id))
		})
	}
}

func TestIDConstants(t *testing.T) {
	require.Equal(t, ID(0), Unset)
	require.Equal(t, ID(1), Historical)
	require.Equal(t, ID(2), BornMined)
	require.Equal(t, ID(3), FirstSynthetic)
	require.Equal(t, ID(1231006505), GenesisTime)
	require.Equal(t, ID(1231006504), LastSynthetic)
	require.Equal(t, ID(math.MaxUint32), MaxClock)
}

func TestIDString(t *testing.T) {
	tests := []struct {
		name string
		id   ID
		want string
	}{
		{name: "unset", id: Unset, want: "unset"},
		{name: "historical", id: Historical, want: "historical"},
		{name: "born mined", id: BornMined, want: "born-mined"},
		{name: "first synthetic", id: FirstSynthetic, want: "synthetic:3"},
		{name: "last synthetic", id: LastSynthetic, want: "synthetic:1231006504"},
		{name: "genesis second", id: GenesisTime, want: "2009-01-03T18:15:05Z"},
		{name: "max clock", id: MaxClock, want: "2106-02-07T06:28:15Z"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, test.id.String())
		})
	}
}

func TestFromUnix(t *testing.T) {
	tests := []struct {
		name    string
		sec     int64
		want    ID
		wantErr bool
	}{
		{name: "negative", sec: -1, wantErr: true},
		{name: "very negative", sec: math.MinInt64, wantErr: true},
		{name: "zero", sec: 0, wantErr: true},
		{name: "unset boundary", sec: 0, wantErr: true},
		{name: "first synthetic", sec: 3, wantErr: true},
		{name: "last synthetic", sec: int64(GenesisTime) - 1, wantErr: true},
		{name: "genesis second", sec: int64(GenesisTime), want: GenesisTime},
		{name: "genesis second plus one", sec: int64(GenesisTime) + 1, want: GenesisTime + 1},
		{name: "max uint32", sec: math.MaxUint32, want: MaxClock},
		{name: "past max uint32", sec: math.MaxUint32 + 1, wantErr: true},
		{name: "very large", sec: math.MaxInt64, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := FromUnix(test.sec)
			if test.wantErr {
				require.Error(t, err)
				require.Equal(t, Unset, got)

				return
			}

			require.NoError(t, err)
			require.Equal(t, test.want, got)
		})
	}
}

func TestFromTime(t *testing.T) {
	// A time part-way through a second lands in the cohort for that second.
	at := time.Unix(int64(GenesisTime)+42, 999_999_999).UTC()

	got, err := FromTime(at)
	require.NoError(t, err)
	require.Equal(t, GenesisTime+42, got)

	_, err = FromTime(time.Unix(0, 0).UTC())
	require.Error(t, err)
}

func TestIDTime(t *testing.T) {
	tests := []struct {
		name   string
		id     ID
		wantOK bool
	}{
		{name: "unset", id: Unset},
		{name: "historical", id: Historical},
		{name: "born mined", id: BornMined},
		{name: "synthetic", id: FirstSynthetic},
		{name: "last synthetic", id: LastSynthetic},
		{name: "genesis second", id: GenesisTime, wantOK: true},
		{name: "max clock", id: MaxClock, wantOK: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, ok := test.id.Time()
			require.Equal(t, test.wantOK, ok)

			if !test.wantOK {
				require.True(t, got.IsZero())

				return
			}

			require.Equal(t, int64(test.id), got.Unix())

			// Round trip: the second comes back as the same cohort.
			back, err := FromTime(got)
			require.NoError(t, err)
			require.Equal(t, test.id, back)
		})
	}
}
