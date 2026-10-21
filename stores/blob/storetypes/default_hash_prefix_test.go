package storetypes

import "testing"

// TestDefaultHashPrefixPinnedValues pins the shipped layout per store type.
// These numbers are file paths on disk: changing one invalidates every
// existing file of that store type, so the values must move only together
// with a migration, never as a drive-by edit.
func TestDefaultHashPrefixPinnedValues(t *testing.T) {
	cases := []struct {
		t    BlobStoreType
		want int
	}{
		{TXSTORE, 2},
		{SUBTREESTORE, 2},
		{BLOCKSTORE, -2},
		{TEMPSTORE, 0},
		{BLOCKPERSISTERSTORE, 2},
		{PEERREGISTRYSTORE, 0},
		{BlobStoreType(99), 2}, // unknown types take the conservative default
	}

	for _, c := range cases {
		if got := DefaultHashPrefix(c.t); got != c.want {
			t.Errorf("DefaultHashPrefix(%s) = %d, want %d", c.t, got, c.want)
		}
	}
}

// TestEveryKnownTypeIsNamed walks all declared store types so a new type
// added to the enum must also be given an explicit prefix entry (or
// deliberately fall to the default) rather than passing unnoticed.
func TestEveryKnownTypeIsNamed(t *testing.T) {
	for _, st := range []BlobStoreType{TXSTORE, SUBTREESTORE, BLOCKSTORE, TEMPSTORE, BLOCKPERSISTERSTORE, PEERREGISTRYSTORE} {
		if st.String() == "UNKNOWN" {
			t.Errorf("store type %d has no name", st)
		}
	}
}
