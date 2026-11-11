package p2p

import (
	"strconv"
	"strings"

	p2pMessageBus "github.com/bsv-blockchain/go-p2p-message-bus"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
)

// The go-p2p-message-bus connection manager watermarks used when
// p2p_max_connections / p2p_min_connections are 0. They mirror the bus's
// connManagerSettings and are used only to validate a watermark that is set
// against one that is left at its default.
const (
	busDefaultMaxConnections = 35
	busDefaultMinConnections = 25
)

// applyConnectionLimits copies the connection manager watermarks and the
// libp2p resource manager overrides from settings into conf, after checking
// what can be checked without the host's auto-scaled limits. Every error names
// the settings keys involved.
//
// All of these settings default to 0 (or empty), which the bus treats as "use
// the default", so a node that sets none of them builds exactly the config it
// did before the settings existed: zero watermarks and a zero ResourceLimits,
// which leaves the resource manager entirely to go-libp2p.
//
// Limits that depend on go-libp2p's auto-scaled defaults (an inbound limit
// raised above an unset, auto-scaled total) can only be checked by the bus when
// it builds the resource manager; p2pMessageBus.NewClient rejects them and
// NewServer fails with that error.
func applyConnectionLimits(conf *p2pMessageBus.Config, p settings.P2PSettings) error {
	if err := validateConnectionLimits(p); err != nil {
		return err
	}

	ipv4, err := parseSubnetConnLimits("p2p_rcmgr_ipv4_subnet_limits", p.RcmgrIPv4SubnetLimits, 32)
	if err != nil {
		return err
	}

	ipv6, err := parseSubnetConnLimits("p2p_rcmgr_ipv6_subnet_limits", p.RcmgrIPv6SubnetLimits, 128)
	if err != nil {
		return err
	}

	conf.MaxConnections = p.MaxConnections
	conf.MinConnections = p.MinConnections
	conf.ConnectionGracePeriod = p.ConnectionGracePeriod
	conf.ResourceLimits = p2pMessageBus.ResourceLimits{
		Conns:              p.RcmgrConns,
		ConnsInbound:       p.RcmgrConnsInbound,
		ConnsOutbound:      p.RcmgrConnsOutbound,
		Streams:            p.RcmgrStreams,
		StreamsInbound:     p.RcmgrStreamsInbound,
		StreamsOutbound:    p.RcmgrStreamsOutbound,
		FD:                 p.RcmgrFD,
		PeerConns:          p.RcmgrPeerConns,
		PeerConnsInbound:   p.RcmgrPeerConnsInbound,
		PeerStreamsInbound: p.RcmgrPeerStreamsInbound,
		IPv4SubnetLimits:   ipv4,
		IPv6SubnetLimits:   ipv6,
	}

	return nil
}

type namedLimit struct {
	key   string
	value int
}

// validateConnectionLimits rejects negative values and contradictory pairs
// where both sides are known here: a watermark against the other watermark
// (set or bus default), and a directional resource limit against its total
// when both are set.
func validateConnectionLimits(p settings.P2PSettings) error {
	counts := []namedLimit{
		{"p2p_max_connections", p.MaxConnections},
		{"p2p_min_connections", p.MinConnections},
		{"p2p_rcmgr_conns", p.RcmgrConns},
		{"p2p_rcmgr_conns_inbound", p.RcmgrConnsInbound},
		{"p2p_rcmgr_conns_outbound", p.RcmgrConnsOutbound},
		{"p2p_rcmgr_streams", p.RcmgrStreams},
		{"p2p_rcmgr_streams_inbound", p.RcmgrStreamsInbound},
		{"p2p_rcmgr_streams_outbound", p.RcmgrStreamsOutbound},
		{"p2p_rcmgr_fd", p.RcmgrFD},
		{"p2p_rcmgr_peer_conns", p.RcmgrPeerConns},
		{"p2p_rcmgr_peer_conns_inbound", p.RcmgrPeerConnsInbound},
		{"p2p_rcmgr_peer_streams_inbound", p.RcmgrPeerStreamsInbound},
	}
	for _, c := range counts {
		if c.value < 0 {
			return errors.NewConfigurationError("%s must not be negative (0 keeps the default), got %d", c.key, c.value)
		}
	}

	if p.ConnectionGracePeriod < 0 {
		return errors.NewConfigurationError("p2p_connection_grace_period must not be negative (0 keeps the default), got %s", p.ConnectionGracePeriod)
	}

	// The connection manager does not check its watermarks: with the low mark
	// above the high mark it would never prune down to the low mark. The bus
	// defaults each one independently, so setting only one can cross the other.
	maxConns, minConns := p.MaxConnections, p.MinConnections
	if maxConns == 0 {
		maxConns = busDefaultMaxConnections
	}

	if minConns == 0 {
		minConns = busDefaultMinConnections
	}

	if minConns > maxConns {
		return errors.NewConfigurationError("p2p_min_connections (%d) exceeds p2p_max_connections (%d); a 0 value means the default (min %d, max %d), so set both when moving either past the other's default",
			minConns, maxConns, busDefaultMinConnections, busDefaultMaxConnections)
	}

	if p.RcmgrConns > 0 && p.RcmgrConns < maxConns {
		return errors.NewConfigurationError("p2p_rcmgr_conns (%d) is below p2p_max_connections (%d); the connection manager could never reach its high water mark",
			p.RcmgrConns, maxConns)
	}

	pairs := []struct{ part, total namedLimit }{
		{namedLimit{"p2p_rcmgr_conns_inbound", p.RcmgrConnsInbound}, namedLimit{"p2p_rcmgr_conns", p.RcmgrConns}},
		{namedLimit{"p2p_rcmgr_conns_outbound", p.RcmgrConnsOutbound}, namedLimit{"p2p_rcmgr_conns", p.RcmgrConns}},
		{namedLimit{"p2p_rcmgr_streams_inbound", p.RcmgrStreamsInbound}, namedLimit{"p2p_rcmgr_streams", p.RcmgrStreams}},
		{namedLimit{"p2p_rcmgr_streams_outbound", p.RcmgrStreamsOutbound}, namedLimit{"p2p_rcmgr_streams", p.RcmgrStreams}},
		{namedLimit{"p2p_rcmgr_peer_conns_inbound", p.RcmgrPeerConnsInbound}, namedLimit{"p2p_rcmgr_peer_conns", p.RcmgrPeerConns}},
	}
	for _, pr := range pairs {
		if pr.part.value > 0 && pr.total.value > 0 && pr.part.value > pr.total.value {
			return errors.NewConfigurationError("%s (%d) exceeds %s (%d)", pr.part.key, pr.part.value, pr.total.key, pr.total.value)
		}
	}

	return nil
}

// parseSubnetConnLimits parses "<prefix length>:<connections>" entries, as
// split from a pipe-separated setting, into bus subnet limits. Parsing is
// strict: a missing separator, a non-integer, a prefix outside 0..maxPrefix,
// a non-positive connection count or a repeated prefix is a configuration
// error naming the key and the entry. No entries returns nil, which keeps the
// go-libp2p default for that address family.
func parseSubnetConnLimits(key string, entries []string, maxPrefix int) ([]p2pMessageBus.SubnetConnLimit, error) {
	if len(entries) == 0 {
		return nil, nil
	}

	limits := make([]p2pMessageBus.SubnetConnLimit, 0, len(entries))
	seen := make(map[int]struct{}, len(entries))

	for _, entry := range entries {
		prefixStr, connsStr, ok := strings.Cut(entry, ":")
		if !ok {
			return nil, errors.NewConfigurationError("%s: entry %q must be <prefix length>:<connections>, e.g. 32:8", key, entry)
		}

		prefix, err := strconv.Atoi(strings.TrimSpace(prefixStr))
		if err != nil || prefix < 0 || prefix > maxPrefix {
			return nil, errors.NewConfigurationError("%s: entry %q has prefix length %q, want an integer from 0 to %d", key, entry, prefixStr, maxPrefix)
		}

		conns, err := strconv.Atoi(strings.TrimSpace(connsStr))
		if err != nil || conns <= 0 {
			return nil, errors.NewConfigurationError("%s: entry %q has connection count %q, want a positive integer", key, entry, connsStr)
		}

		if _, dup := seen[prefix]; dup {
			return nil, errors.NewConfigurationError("%s: prefix length %d is listed more than once", key, prefix)
		}

		seen[prefix] = struct{}{}

		limits = append(limits, p2pMessageBus.SubnetConnLimit{PrefixLength: prefix, Conns: conns})
	}

	return limits, nil
}
