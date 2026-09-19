package urlutil

import (
	"net/url"
	"strings"
)

// ParseMultiHostURL parses a URL string that may contain comma-separated hosts
// in the authority section (e.g. "kafka://host1:9092,host2:9092/topic").
// Go 1.26 rejects such URLs in url.Parse, so this function parses using only
// the first host, then restores the full comma-separated host string.
func ParseMultiHostURL(rawURL string) (*url.URL, error) {
	if !strings.Contains(rawURL, ",") {
		return url.Parse(rawURL)
	}

	schemeEnd := strings.Index(rawURL, "://")
	if schemeEnd == -1 {
		return url.Parse(rawURL)
	}

	afterScheme := rawURL[schemeEnd+3:]

	// RFC 3986 ends the authority at the first "/", "?" or "#", whichever comes
	// first. Looking for "/" before "?" splits "http://host?ids=a,b/c" at the
	// slash inside the query, which puts the query into the host.
	hostPart := afterScheme
	rest := ""

	if end := strings.IndexAny(afterScheme, "/?#"); end >= 0 {
		hostPart = afterScheme[:end]
		rest = afterScheme[end:]
	}

	hosts := strings.Split(hostPart, ",")
	singleHostURL := rawURL[:schemeEnd+3] + hosts[0] + rest

	u, err := url.Parse(singleHostURL)
	if err != nil {
		return nil, err
	}

	// Split the userinfo off at the LAST "@", which is what net/url does. The
	// first "@" is wrong whenever the password contains a raw "@": the tail of
	// the password survives into u.Host and is then printed by the very helper
	// whose job is to mask it, and welded onto the first broker by every
	// consumer that splits u.Host on commas.
	if atIdx := strings.LastIndex(hostPart, "@"); atIdx >= 0 {
		hostPart = hostPart[atIdx+1:]
	}

	u.Host = hostPart

	return u, nil
}
