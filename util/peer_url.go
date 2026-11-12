package util

import (
	"net/url"
	"strings"

	"github.com/bsv-blockchain/teranode/errors"
)

// JoinPeerURL builds the URL of a protocol endpoint on a peer-supplied base URL, such as
// a DataHub URL announced over p2p. The base is parsed and checked by ValidatePeerBaseURL
// and the elements are appended as path segments, so a real base path like /api/v1 is
// kept. Callers append their own fixed query afterwards.
//
// Never build these URLs by string concatenation. A base ending in "?x=" turns the
// appended path into query data, so the request's effective path is whatever the peer
// chose (issue 4843), and a POST carries its body there.
func JoinPeerURL(base string, elem ...string) (string, error) {
	parsed, err := parsePeerBaseURL(base)
	if err != nil {
		return "", err
	}

	return parsed.JoinPath(elem...).String(), nil
}

// ValidatePeerBaseURL reports whether base is usable as a peer base URL: http or https,
// a host, and no userinfo, query or fragment. Errors never echo the URL, which may carry
// credentials.
func ValidatePeerBaseURL(base string) error {
	_, err := parsePeerBaseURL(base)
	return err
}

func parsePeerBaseURL(base string) (*url.URL, error) {
	parsed, err := url.Parse(base)
	if err != nil {
		return nil, errors.NewInvalidArgumentError("invalid peer base URL")
	}

	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "http" && scheme != "https" {
		return nil, errors.NewInvalidArgumentError("peer base URL has invalid scheme %q (only http/https allowed)", parsed.Scheme)
	}

	parsed.Scheme = scheme

	if parsed.User != nil {
		return nil, errors.NewInvalidArgumentError("peer base URL must not contain userinfo (credentials)")
	}

	if parsed.Opaque != "" || parsed.Hostname() == "" {
		return nil, errors.NewInvalidArgumentError("peer base URL has no host")
	}

	if parsed.RawQuery != "" || parsed.ForceQuery {
		return nil, errors.NewInvalidArgumentError("peer base URL must not contain a query")
	}

	// url.Parse drops an empty fragment ("...#") without a trace, so look at the raw string.
	if parsed.Fragment != "" || strings.Contains(base, "#") {
		return nil, errors.NewInvalidArgumentError("peer base URL must not contain a fragment")
	}

	return parsed, nil
}
