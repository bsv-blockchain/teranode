package p2p

import (
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestResolveAdvertiseAddresses pins which addresses the node announces. The
// listen addresses must never leak into the announce set: the bus only supports
// wildcard binds and announcing 0.0.0.0 either means nothing or, because it is
// not a multiaddr, makes NewClient refuse to start.
func TestResolveAdvertiseAddresses(t *testing.T) {
	explicit := []string{"/ip4/203.0.113.1/tcp/9905"}

	build := func(listenMode string, advertise []string, sharePrivate bool) *settings.Settings {
		s := settings.NewSettings()
		s.P2P.ListenMode = listenMode
		s.P2P.ListenAddresses = []string{"0.0.0.0"}
		s.P2P.AdvertiseAddresses = advertise
		s.P2P.SharePrivateAddresses = sharePrivate
		return s
	}

	t.Run("silent mode suppresses explicit advertise addresses", func(t *testing.T) {
		s := build(settings.ListenModeSilent, explicit, false)
		require.Empty(t, resolveAdvertiseAddresses(ulogger.TestLogger{}, s))
	})

	t.Run("silent mode suppresses private address sharing", func(t *testing.T) {
		s := build(settings.ListenModeSilent, nil, true)
		require.Empty(t, resolveAdvertiseAddresses(ulogger.TestLogger{}, s))
	})

	t.Run("explicit advertise addresses win over private sharing", func(t *testing.T) {
		s := build(settings.ListenModeFull, explicit, true)
		require.Equal(t, explicit, resolveAdvertiseAddresses(ulogger.TestLogger{}, s))
	})

	t.Run("private sharing leaves announcement to libp2p, never the wildcard bind", func(t *testing.T) {
		s := build(settings.ListenModeFull, nil, true)
		require.Empty(t, resolveAdvertiseAddresses(ulogger.TestLogger{}, s))
	})

	t.Run("no sharing and no explicit addresses leaves announcement to libp2p", func(t *testing.T) {
		s := build(settings.ListenModeFull, nil, false)
		require.Empty(t, resolveAdvertiseAddresses(ulogger.TestLogger{}, s))
	})
}
