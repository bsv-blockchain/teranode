package urlutil

import (
	"net/url"
	"regexp"
)

// nilURL is what Redact prints for a nil URL. An empty string would be
// ambiguous in a log line, where an unset store URL and a store URL that
// happens to be empty are different problems.
const nilURL = "<nil>"

// unparseableURL is what RedactString prints for a string that is not a URL.
// The input is deliberately not echoed: a string that fails to parse can still
// hold a credential, and this helper exists precisely to keep credentials out
// of logs.
const unparseableURL = "<unparseable url>"

// Redact returns a log-safe rendering of u with the userinfo password masked.
//
// Teranode store URLs are both configuration and credentials: a blob store
// URL of the form http://user:password@host/ is functional configuration, and
// Go's HTTP client turns that userinfo into a Basic Authorization header. The
// same is true of the postgres, aerospike and kafka store URLs. Any of those
// reaching a log line hands working credentials to everyone who can read the
// logs, which includes ordinary log aggregation and support bundles.
//
// The username is preserved. It is the half of the pair that is not secret,
// and it is what an operator needs to tell which configured credential a
// failing connection is using. Only the password is masked.
//
// The placeholder matches the standard library's, so redaction looks the same
// whether it came through here or through url.URL.Redacted() directly.
//
// Redact does not mutate u. A nil u renders as "<nil>".
//
// Use this at every site where a URL reaches a log line, an error message, a
// metric label or an API response.
func Redact(u *url.URL) string {
	if u == nil {
		return nilURL
	}

	return u.Redacted()
}

// RedactString is Redact for a URL that is still in string form.
//
// It accepts the comma-separated multi-host authority that Kafka and Aerospike
// URLs use, via ParseMultiHostURL. A string that does not parse as a URL
// renders as "<unparseable url>" rather than being echoed back.
func RedactString(rawURL string) string {
	u, err := ParseMultiHostURL(rawURL)
	if err != nil {
		return unparseableURL
	}

	return Redact(u)
}

// urlToken matches a URL embedded in surrounding text: an RFC 3986 scheme,
// "://", then everything up to the next whitespace. Configured store URLs never
// contain a space, so the whitespace bound is exact for them, and a value that
// is not a URL at all cannot match because it has no "scheme://" prefix.
var urlToken = regexp.MustCompile(`[A-Za-z][A-Za-z0-9+.\-]*://\S+`)

// RedactText masks the password in every URL embedded in s, leaving the rest of
// the text alone.
//
// It exists for the configuration dumps: gocore's Config().Stats() renders the
// resolved value of every setting as flat text, so a single Infof of it emits
// every store credential the node holds at once, and the bug-reporting guide
// asks operators to paste that output into an issue. A line-oriented splitter
// would miss the CMDLINE section, where a URL arrives as a bare argv entry with
// no "key=" in front of it, so this works on tokens rather than lines.
//
// A URL-shaped token that will not parse renders as "<unparseable url>" rather
// than being echoed, on the same reasoning as RedactString: a string that fails
// to parse can still hold a credential.
func RedactText(s string) string {
	return urlToken.ReplaceAllStringFunc(s, RedactString)
}

// RedactMapValues returns a copy of m with every URL in its values masked by
// RedactText. The input map is not mutated.
//
// gocore's Config().GetAll() hands back the raw configuration map with no
// masking at all, and teranode registers it as the "CONFIG" advertising
// payload, which gocore POSTs to advertisingURL when that setting is non-empty.
// A string redactor cannot reach a map, so this is the map-shaped twin of
// RedactText.
func RedactMapValues(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = RedactText(v)
	}

	return out
}
