package p2p

import (
	"testing"
	"time"

	p2pMessageBus "github.com/bsv-blockchain/go-p2p-message-bus"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/require"
)

func TestParseSubnetConnLimits(t *testing.T) {
	const key = "p2p_rcmgr_ipv4_subnet_limits"

	t.Run("no entries keeps the library default", func(t *testing.T) {
		for _, entries := range [][]string{nil, {}} {
			limits, err := parseSubnetConnLimits(key, entries, 32)
			require.NoError(t, err)
			require.Nil(t, limits, "nil tells the bus to keep the go-libp2p default for this family")
		}
	})

	t.Run("entries parse in order", func(t *testing.T) {
		limits, err := parseSubnetConnLimits(key, []string{"32:8", " 24 : 32 ", "0:1000"}, 32)
		require.NoError(t, err)
		require.Equal(t, []p2pMessageBus.SubnetConnLimit{
			{PrefixLength: 32, Conns: 8},
			{PrefixLength: 24, Conns: 32},
			{PrefixLength: 0, Conns: 1000},
		}, limits)
	})

	t.Run("IPv6 accepts prefixes up to 128", func(t *testing.T) {
		limits, err := parseSubnetConnLimits("p2p_rcmgr_ipv6_subnet_limits", []string{"128:2", "56:8", "48:64"}, 128)
		require.NoError(t, err)
		require.Equal(t, []p2pMessageBus.SubnetConnLimit{
			{PrefixLength: 128, Conns: 2},
			{PrefixLength: 56, Conns: 8},
			{PrefixLength: 48, Conns: 64},
		}, limits)
	})

	bad := []struct {
		name, entry, want string
		maxPrefix         int
	}{
		{"missing separator", "32", "must be <prefix length>:<connections>", 32},
		{"comma instead of pipe", "32:8,24:32", "connection count", 32},
		{"non-integer prefix", "x:8", "prefix length", 32},
		{"empty prefix", ":8", "prefix length", 32},
		{"negative prefix", "-1:8", "prefix length", 32},
		{"IPv4 prefix above 32", "33:8", "from 0 to 32", 32},
		{"IPv6 prefix above 128", "129:8", "from 0 to 128", 128},
		{"non-integer conns", "32:many", "connection count", 32},
		{"empty conns", "32:", "connection count", 32},
		{"zero conns", "32:0", "positive integer", 32},
		{"negative conns", "32:-4", "positive integer", 32},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseSubnetConnLimits(key, []string{tc.entry}, tc.maxPrefix)
			require.Error(t, err)
			require.True(t, errors.Is(err, errors.ErrConfiguration), "a bad entry is a configuration error, got %v", err)
			require.Contains(t, err.Error(), key, "the error must name the setting")
			require.Contains(t, err.Error(), tc.want)
		})
	}

	t.Run("repeated prefix", func(t *testing.T) {
		_, err := parseSubnetConnLimits(key, []string{"32:8", "32:4"}, 32)
		require.Error(t, err)
		require.Contains(t, err.Error(), "prefix length 32 is listed more than once")
	})
}

func TestValidateConnectionLimits(t *testing.T) {
	t.Run("all zero is valid", func(t *testing.T) {
		require.NoError(t, validateConnectionLimits(settings.P2PSettings{}))
	})

	t.Run("negative values name their key", func(t *testing.T) {
		cases := map[string]func(p *settings.P2PSettings){
			"p2p_max_connections":            func(p *settings.P2PSettings) { p.MaxConnections = -1 },
			"p2p_min_connections":            func(p *settings.P2PSettings) { p.MinConnections = -1 },
			"p2p_rcmgr_conns":                func(p *settings.P2PSettings) { p.RcmgrConns = -1 },
			"p2p_rcmgr_conns_inbound":        func(p *settings.P2PSettings) { p.RcmgrConnsInbound = -1 },
			"p2p_rcmgr_conns_outbound":       func(p *settings.P2PSettings) { p.RcmgrConnsOutbound = -1 },
			"p2p_rcmgr_streams":              func(p *settings.P2PSettings) { p.RcmgrStreams = -1 },
			"p2p_rcmgr_streams_inbound":      func(p *settings.P2PSettings) { p.RcmgrStreamsInbound = -1 },
			"p2p_rcmgr_streams_outbound":     func(p *settings.P2PSettings) { p.RcmgrStreamsOutbound = -1 },
			"p2p_rcmgr_fd":                   func(p *settings.P2PSettings) { p.RcmgrFD = -1 },
			"p2p_rcmgr_peer_conns":           func(p *settings.P2PSettings) { p.RcmgrPeerConns = -1 },
			"p2p_rcmgr_peer_conns_inbound":   func(p *settings.P2PSettings) { p.RcmgrPeerConnsInbound = -1 },
			"p2p_rcmgr_peer_streams_inbound": func(p *settings.P2PSettings) { p.RcmgrPeerStreamsInbound = -1 },
			"p2p_connection_grace_period":    func(p *settings.P2PSettings) { p.ConnectionGracePeriod = -time.Second },
		}
		for key, set := range cases {
			t.Run(key, func(t *testing.T) {
				var p settings.P2PSettings
				set(&p)
				err := validateConnectionLimits(p)
				require.Error(t, err)
				require.True(t, errors.Is(err, errors.ErrConfiguration))
				require.Contains(t, err.Error(), key+" must not be negative")
			})
		}
	})

	t.Run("watermarks", func(t *testing.T) {
		cases := []struct {
			name     string
			min, max int
			wantErr  string
		}{
			{name: "both set, ordered", min: 40, max: 80},
			{name: "equal", min: 50, max: 50},
			{name: "max raised alone", max: 200},
			{name: "min lowered alone", min: 5},
			{name: "both set, crossed", min: 80, max: 40, wantErr: "p2p_min_connections (80) exceeds p2p_max_connections (40)"},
			// The bus defaults each watermark on its own, so lowering only the
			// high mark below the default low mark (25) would cross them.
			{name: "max lowered below the default min", max: 20, wantErr: "p2p_min_connections (25) exceeds p2p_max_connections (20)"},
			{name: "min raised above the default max", min: 40, wantErr: "p2p_min_connections (40) exceeds p2p_max_connections (35)"},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				err := validateConnectionLimits(settings.P2PSettings{MinConnections: tc.min, MaxConnections: tc.max})
				if tc.wantErr == "" {
					require.NoError(t, err)
					return
				}
				require.Error(t, err)
				require.True(t, errors.Is(err, errors.ErrConfiguration))
				require.Contains(t, err.Error(), tc.wantErr)
			})
		}
	})

	t.Run("system conns below the high water mark", func(t *testing.T) {
		err := validateConnectionLimits(settings.P2PSettings{MaxConnections: 100, MinConnections: 50, RcmgrConns: 99})
		require.Error(t, err)
		require.Contains(t, err.Error(), "p2p_rcmgr_conns (99) is below p2p_max_connections (100)")

		// Checked against the bus default when the watermark is unset.
		err = validateConnectionLimits(settings.P2PSettings{RcmgrConns: 34})
		require.Error(t, err)
		require.Contains(t, err.Error(), "p2p_rcmgr_conns (34) is below p2p_max_connections (35)")

		require.NoError(t, validateConnectionLimits(settings.P2PSettings{MaxConnections: 100, MinConnections: 50, RcmgrConns: 100}))
	})

	t.Run("directional limit above its total when both are set", func(t *testing.T) {
		cases := []struct {
			set  func(p *settings.P2PSettings)
			want string
		}{
			{func(p *settings.P2PSettings) { p.RcmgrConns, p.RcmgrConnsInbound = 100, 101 }, "p2p_rcmgr_conns_inbound (101) exceeds p2p_rcmgr_conns (100)"},
			{func(p *settings.P2PSettings) { p.RcmgrConns, p.RcmgrConnsOutbound = 100, 101 }, "p2p_rcmgr_conns_outbound (101) exceeds p2p_rcmgr_conns (100)"},
			{func(p *settings.P2PSettings) { p.RcmgrStreams, p.RcmgrStreamsInbound = 100, 101 }, "p2p_rcmgr_streams_inbound (101) exceeds p2p_rcmgr_streams (100)"},
			{func(p *settings.P2PSettings) { p.RcmgrStreams, p.RcmgrStreamsOutbound = 100, 101 }, "p2p_rcmgr_streams_outbound (101) exceeds p2p_rcmgr_streams (100)"},
			{func(p *settings.P2PSettings) { p.RcmgrPeerConns, p.RcmgrPeerConnsInbound = 4, 5 }, "p2p_rcmgr_peer_conns_inbound (5) exceeds p2p_rcmgr_peer_conns (4)"},
		}
		for _, tc := range cases {
			var p settings.P2PSettings
			tc.set(&p)
			err := validateConnectionLimits(p)
			require.Error(t, err, tc.want)
			require.True(t, errors.Is(err, errors.ErrConfiguration))
			require.Contains(t, err.Error(), tc.want)
		}

		// Equal is allowed, and a directional limit with its total unset is
		// left to the bus, which checks it against the auto-scaled total.
		require.NoError(t, validateConnectionLimits(settings.P2PSettings{RcmgrConns: 100, RcmgrConnsInbound: 100}))
		require.NoError(t, validateConnectionLimits(settings.P2PSettings{RcmgrConnsInbound: 100}))
	})
}
