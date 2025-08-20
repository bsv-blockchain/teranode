package catchup

import "github.com/bitcoin-sv/teranode/errors"

var (
	// ErrCircuitOpen is returned when the circuit breaker is open
	ErrCircuitOpen = errors.NewNetworkError("circuit breaker is open")

	// ErrNoCommonAncestor is returned when no common ancestor is found
	ErrNoCommonAncestor = errors.NewProcessingError("no common ancestor found")

	// ErrSecretMining is returned when secret mining is detected
	ErrSecretMining = errors.NewNetworkPeerMaliciousError("secret mining detected")

	// ErrInvalidChain is returned when the header chain is invalid
	ErrInvalidChain = errors.NewNetworkInvalidResponseError("invalid header chain")

	// ErrTimeout is returned when a catchup operation times out
	ErrTimeout = errors.NewNetworkTimeoutError("catchup operation timed out")
)
