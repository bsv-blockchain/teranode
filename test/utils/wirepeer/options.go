package wirepeer

import (
	"github.com/bsv-blockchain/go-wire"
)

// config holds the tunable parts of a test peer's advertised identity and
// handshake behaviour. Several upstream tests vary exactly these — user agent
// (bsv-ban-useragents.py), protocol version (bsv-p2p-version_msg.py), and
// whether the handshake completes at all (p2p-leaktests.py).
type config struct {
	protocolVersion   uint32
	services          wire.ServiceFlag
	userAgentName     string
	userAgentVersion  string
	userAgentComments []string
	skipHandshake     bool
}

// Option customises a test peer.
type Option func(*config)

// WithProtocolVersion advertises a specific protocol version, for tests that
// check version-gated behaviour.
func WithProtocolVersion(v uint32) Option {
	return func(c *config) { c.protocolVersion = v }
}

// WithServices advertises a specific service flag set.
func WithServices(s wire.ServiceFlag) Option {
	return func(c *config) { c.services = s }
}

// WithUserAgent advertises a specific user agent, for tests that assert on
// user-agent banning or filtering. Note that the legacy service bans agents
// containing neither "Bitcoin SV" nor "BSV", so passing an arbitrary name is a
// way to exercise that ban deliberately — and a way to break every other
// assertion by accident.
func WithUserAgent(name, version string, comments ...string) Option {
	return func(c *config) {
		c.userAgentName = name
		c.userAgentVersion = version
		c.userAgentComments = comments
	}
}

// SkipHandshakeWait returns as soon as the connection is associated, without
// waiting for verack. Use it for tests that assert on what the node does to a
// peer that never completes the handshake.
func SkipHandshakeWait() Option {
	return func(c *config) { c.skipHandshake = true }
}
