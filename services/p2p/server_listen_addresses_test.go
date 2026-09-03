package p2p

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestNewServerRejectsNarrowedListenAddresses drives the real constructor with
// the settings an operator would write to restrict libp2p to one interface. The
// bus cannot honour that, so startup must fail instead of silently binding every
// interface behind the operator's back.
func TestNewServerRejectsNarrowedListenAddresses(t *testing.T) {
	cases := []struct {
		name  string
		addrs []string
		want  string
	}{
		{"unset", nil, "p2p_listen_addresses not set"},
		{"empty", []string{}, "p2p_listen_addresses not set"},
		{"loopback dev value", []string{"127.0.0.1"}, "127.0.0.1"},
		{"private interface", []string{"10.0.1.5"}, "10.0.1.5"},
		{"wrong port", []string{"/ip4/0.0.0.0/tcp/9906"}, "does not match p2p_port"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tSettings := settings.NewSettings()
			tSettings.P2P.Port = 9905
			tSettings.P2P.ListenAddresses = tc.addrs

			s, err := NewServer(context.Background(), ulogger.TestLogger{}, tSettings, nil, nil, nil, nil, nil, nil, nil, nil)
			require.Nil(t, s)
			require.Error(t, err)
			require.True(t, errors.Is(err, errors.ErrConfiguration), "expected configuration error, got %v", err)
			require.Contains(t, err.Error(), tc.want)
		})
	}
}
