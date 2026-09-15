package urlutil

import "net/url"

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
