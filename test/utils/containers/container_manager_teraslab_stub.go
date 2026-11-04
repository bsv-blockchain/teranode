//go:build !teraslab

package containers

import (
	"context"
	"net/url"

	"github.com/bsv-blockchain/teranode/errors"
)

// initializeTeraslab is the stub used when the binary is built without the
// `teraslab` build tag. TeraSlab requires a server image and Docker that most
// contributors will not have, so the real container startup (in
// container_manager_teraslab.go) is gated behind that tag. Selecting the
// teraslab UTXO store without the tag is a clear, actionable error rather than a
// silent fallback.
func (cm *ContainerManager) initializeTeraslab(_ context.Context) (*url.URL, error) {
	return nil, errors.NewInvalidArgumentError(
		"teraslab UTXO store support is not built into this test binary; rebuild with -tags teraslab")
}
