package sql

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestParseInsertedAtMillis pins the per-type branch behaviour of the
// helper that feeds meta.Data.CreatedAt. The reverse-reorg counter-
// selection heuristic depends on this value: a wrong parse returns 0,
// and isOlderCounter treats 0 as "newer than any timestamped record" —
// meaning a misparse could silently downgrade a legacy record into the
// fallback branch even when it has a usable timestamp.
//
// Two driver shapes matter:
//   - Postgres returns time.Time directly from the inserted_at column.
//   - SQLite (modernc.org/sqlite) returns the value as []byte / string
//     formatted per the CURRENT_TIMESTAMP layout, sometimes with
//     fractional seconds, sometimes with a trailing Z.
func TestParseInsertedAtMillis(t *testing.T) {
	t.Run("nil yields zero", func(t *testing.T) {
		assert.Equal(t, int64(0), parseInsertedAtMillis(nil))
	})

	t.Run("time.Time round-trips via UnixMilli", func(t *testing.T) {
		ts := time.Date(2025, time.March, 4, 5, 6, 7, 8_000_000, time.UTC)
		assert.Equal(t, ts.UnixMilli(), parseInsertedAtMillis(ts))
	})

	t.Run("SQLite default layout YYYY-MM-DD HH:MM:SS parses", func(t *testing.T) {
		got := parseInsertedAtMillis("2025-03-04 05:06:07")
		want := time.Date(2025, time.March, 4, 5, 6, 7, 0, time.UTC).UnixMilli()
		assert.Equal(t, want, got)
	})

	t.Run("SQLite fractional seconds layout parses", func(t *testing.T) {
		got := parseInsertedAtMillis("2025-03-04 05:06:07.123456789")
		want := time.Date(2025, time.March, 4, 5, 6, 7, 123_456_789, time.UTC).UnixMilli()
		assert.Equal(t, want, got)
	})

	t.Run("RFC3339Nano parses", func(t *testing.T) {
		input := "2025-03-04T05:06:07.123456789Z"
		got := parseInsertedAtMillis(input)
		ts, err := time.Parse(time.RFC3339Nano, input)
		assert.NoError(t, err)
		assert.Equal(t, ts.UnixMilli(), got)
	})

	t.Run("RFC3339 plain parses", func(t *testing.T) {
		got := parseInsertedAtMillis("2025-03-04T05:06:07Z")
		want := time.Date(2025, time.March, 4, 5, 6, 7, 0, time.UTC).UnixMilli()
		assert.Equal(t, want, got)
	})

	t.Run("[]byte routes through string layouts", func(t *testing.T) {
		got := parseInsertedAtMillis([]byte("2025-03-04 05:06:07"))
		want := time.Date(2025, time.March, 4, 5, 6, 7, 0, time.UTC).UnixMilli()
		assert.Equal(t, want, got)
	})

	t.Run("unparseable string falls back to zero", func(t *testing.T) {
		// Garbage in must not panic and must not return a guessed timestamp.
		// isOlderCounter then treats this record as legacy / unknown vintage.
		assert.Equal(t, int64(0), parseInsertedAtMillis("not a time"))
	})

	t.Run("unrecognised type falls back to zero", func(t *testing.T) {
		// int / float / struct etc. shouldn't reach this helper, but the
		// default branch protects against silent corruption if a future
		// driver upgrade changes the column type.
		assert.Equal(t, int64(0), parseInsertedAtMillis(12345))
		assert.Equal(t, int64(0), parseInsertedAtMillis(struct{}{}))
	})
}
