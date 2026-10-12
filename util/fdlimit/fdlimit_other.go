//go:build !unix

package fdlimit

import "github.com/bsv-blockchain/teranode/errors"

// Platforms without RLIMIT_NOFILE (Windows, plan9, wasm) expose no descriptor
// limit to read, so Budget reports it as unknown and the caller carries on
// bounded only by its own concurrency limit.
func getRlimit() (soft uint64, err error) {
	return 0, errors.NewProcessingError("open-file limit is not available on this platform")
}
