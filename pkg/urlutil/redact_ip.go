package urlutil

import (
	"net"
	"regexp"
	"strings"
)

// redactedIP is what RedactIPLiterals puts in place of an IP address, with its port.
const redactedIP = "<redacted-ip>"

// ipCandidate matches the shapes an IP literal takes in error text: a bracketed IPv6
// address with an optional port ("[fd00::1]:8080"), an IPv6 address ending in dotted
// IPv4 ("::ffff:10.0.0.4"), an IPv4 address with an optional port ("10.0.0.4:3000"),
// and a bare IPv6 address with an optional zone ("fe80::1%eth0"). The mixed form comes
// before plain IPv6, which would otherwise stop at the first dot. The groups are
// bounded so a match starts at a plausible address boundary however the address is
// glued to surrounding text. It still over-matches - timestamps fit the IPv6 shape -
// so every candidate is confirmed with net.ParseIP before anything is replaced.
var ipCandidate = regexp.MustCompile(
	`\[[0-9A-Fa-f:.]+(?:%[0-9A-Za-z._~-]+)?\](?::[0-9]+)?` +
		`|[0-9A-Fa-f]{0,4}(?::[0-9A-Fa-f]{0,4}){1,6}:(?:[0-9]{1,3}\.){3}[0-9]{1,3}` +
		`|(?:[0-9]{1,3}\.){3}[0-9]{1,3}(?::[0-9]+)?` +
		`|[0-9A-Fa-f]{0,4}(?::[0-9A-Fa-f]{0,4}){2,7}(?:%[0-9A-Za-z._~-]+)?`)

// RedactIPLiterals replaces every IPv4 and IPv6 address in s, together with any port
// attached to it, with "<redacted-ip>". The rest of the text is left alone.
//
// It exists for error text that crosses from this node's network stack into state an
// outside caller can read. Go's dial errors name the address they resolved and tried
// ("dial tcp 10.0.0.4:3000: connect: connection refused"), so a peer that can steer a
// fetch at a hostname learns, from the failure, which private address that name has
// inside this node's network (bitcoin-sv/teranode#4855). Hostnames are left as they
// are: in a peer-steered fetch the peer chose them, so they tell it nothing new.
//
// This hides the address, not the outcome. The words around it still tell the peer
// whether a name resolved inside the network, to which address class, and whether
// the port answered. Closing that needs a stable error kind in place of free text.
func RedactIPLiterals(s string) string {
	return ipCandidate.ReplaceAllStringFunc(s, func(token string) string {
		// The IPv6 shape greedily swallows a colon that belongs to the sentence
		// ("dial tcp fe80::1: connect"), but "::" can also end a valid address
		// ("fd00::"). Drop trailing colons one at a time and keep the longest
		// candidate that parses.
		for core := token; ; core = core[:len(core)-1] {
			if net.ParseIP(ipHost(core)) != nil {
				return redactedIP + token[len(core):]
			}

			if !strings.HasSuffix(core, ":") {
				return token
			}
		}
	})
}

// ipHost strips the brackets, the port and the zone from an ipCandidate match,
// leaving the address part.
func ipHost(token string) string {
	host := token

	switch {
	case strings.HasPrefix(host, "["):
		host = host[1:strings.Index(host, "]")]
	case strings.Count(host, ":") == 1:
		// host:port. A bare IPv6 address has at least two colons, so a single colon
		// can only separate an IPv4 address from its port.
		host = host[:strings.Index(host, ":")]
	}

	if zone := strings.Index(host, "%"); zone >= 0 {
		host = host[:zone]
	}

	return host
}
