package protocol

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBanListAddAndCheck(t *testing.T) {
	bl, err := NewBanList("")
	require.NoError(t, err)

	require.NoError(t, bl.Add("1.2.3.4", time.Now().Add(time.Hour)))
	require.True(t, bl.IsBanned("1.2.3.4:8333"))
	require.False(t, bl.IsBanned("1.2.3.5:8333"))
}

func TestBanListCIDR(t *testing.T) {
	bl, err := NewBanList("")
	require.NoError(t, err)

	require.NoError(t, bl.Add("10.0.0.0/8", time.Now().Add(time.Hour)))
	require.True(t, bl.IsBanned("10.1.2.3:8333"))
	require.False(t, bl.IsBanned("11.1.2.3:8333"))
}

func TestBanListExpiry(t *testing.T) {
	bl, err := NewBanList("")
	require.NoError(t, err)

	require.NoError(t, bl.Add("1.2.3.4", time.Now().Add(-time.Second)))
	require.False(t, bl.IsBanned("1.2.3.4:8333"))
}

func TestBanListRemoveUnknown(t *testing.T) {
	bl, err := NewBanList("")
	require.NoError(t, err)

	require.ErrorIs(t, bl.Remove("9.9.9.9"), ErrNotBanned)
}

func TestBanListPersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "banlist.json")

	bl, err := NewBanList(path)
	require.NoError(t, err)
	require.NoError(t, bl.Add("1.2.3.4", time.Now().Add(time.Hour)))

	reopened, err := NewBanList(path)
	require.NoError(t, err)
	require.True(t, reopened.IsBanned("1.2.3.4:8333"))

	entries := reopened.List()
	require.Len(t, entries, 1)
	require.Equal(t, "1.2.3.4", entries[0].Host)
}

func TestBanListClear(t *testing.T) {
	path := filepath.Join(t.TempDir(), "banlist.json")

	bl, err := NewBanList(path)
	require.NoError(t, err)
	require.NoError(t, bl.Add("1.2.3.4", time.Now().Add(time.Hour)))
	require.NoError(t, bl.Clear())
	require.False(t, bl.IsBanned("1.2.3.4:8333"))

	reopened, err := NewBanList(path)
	require.NoError(t, err)
	require.Empty(t, reopened.List())
}
