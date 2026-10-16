package aerospike

import (
	"net/url"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/urlutil"
)

// parseExternalStoreURL parses the externalStore query parameter of the
// utxostore URL. That value is a blob store URL in its own right, and an HTTP
// blob store URL can carry a working password in its userinfo. url.Parse
// embeds its whole input in the error it returns, and even its bare reason can
// quote part of the password, so only a fixed reason is kept.
func parseExternalStoreURL(rawURL string) (*url.URL, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, errors.NewConfigurationError("invalid externalStore URL in utxostore", urlutil.ParseErrorReason(err))
	}

	return u, nil
}
