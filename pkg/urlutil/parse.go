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

	pathStart := strings.Index(afterScheme, "/")
	queryStart := strings.Index(afterScheme, "?")

	var hostPart string
	var rest string

	switch {
	case pathStart >= 0:
		hostPart = afterScheme[:pathStart]
		rest = afterScheme[pathStart:]
	case queryStart >= 0:
		hostPart = afterScheme[:queryStart]
		rest = afterScheme[queryStart:]
	default:
		hostPart = afterScheme
		rest = ""
	}

	hosts := strings.Split(hostPart, ",")
	singleHostURL := rawURL[:schemeEnd+3] + hosts[0] + rest

	u, err := url.Parse(singleHostURL)
	if err != nil {
		return nil, err
	}

	if atIdx := strings.Index(hostPart, "@"); atIdx >= 0 {
		hostPart = hostPart[atIdx+1:]
	}

	u.Host = hostPart

	return u, nil
}
