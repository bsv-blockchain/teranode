package lane

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPackPutsTheLaneInTheHighByte(t *testing.T) {
	// Lane 7, position 41,000: 0x07 then 0x00000000_00A028 in the low seven bytes.
	require.Equal(t, uint64(0x07000000_0000A028), Pack(7, 41_000))

	// The widest lane and position use all 64 bits, so the value is above
	// math.MaxInt64 and every signed store must carry it bit for bit.
	require.Equal(t, uint64(0xFFFFFFFF_FFFFFFFF), Pack(255, MaxPosition))
}

func TestUnpackReversesPack(t *testing.T) {
	for _, c := range []struct {
		lane uint8
		pos  uint64
	}{
		{0, 1},
		{7, 41_000},
		{128, 10_000_000},
		{255, MaxPosition},
	} {
		l, p := Unpack(Pack(c.lane, c.pos))
		require.Equal(t, c.lane, l)
		require.Equal(t, c.pos, p)
	}
}

func TestZeroIsNeverAPosition(t *testing.T) {
	// Lane 0 is a real lane, so the stored value 0 can only mean "absent"
	// because position 0 is never handed out.
	require.Equal(t, None, Pack(0, 0))
	require.False(t, Valid(None))
	require.True(t, Valid(Pack(0, 1)))
	require.False(t, Valid(Pack(9, 0)), "position 0 is not a position in any lane")
}

func TestPackRejectsAPositionWiderThanSevenBytes(t *testing.T) {
	require.Panics(t, func() { Pack(1, MaxPosition+1) })
}

func TestSignedRoundTripKeepsEveryBit(t *testing.T) {
	// Aerospike integer bins, Postgres BIGINT and SQLite INTEGER are signed.
	// Lanes 128 to 255 set the top bit, so the stored value is negative.
	v := Pack(200, 123_456_789)
	require.Less(t, ToSigned(v), int64(0))
	require.Equal(t, v, FromSigned(ToSigned(v)))
}
