package settings

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The Docker E2E, TEC and 3-blaster compose stacks mount only settings.conf and
// authenticate every RPC call as bitcoin:bitcoin (test/utils/helper.go). Their
// contexts are docker.teranodeN.test[.legacy|.coinbase], which gocore resolves by
// stripping suffixes from the right, so rpc_*.test never matches them.
func TestRPCCredentials_DockerTestContexts(t *testing.T) {
	for _, ctx := range []string{
		"docker.teranode1.test",
		"docker.teranode2.test",
		"docker.teranode3.test",
		"docker.teranode1.test.legacy",
		"docker.teranode3.test.coinbase",
	} {
		t.Run(ctx, func(t *testing.T) {
			s := NewSettings(ctx)

			require.Equal(t, "bitcoin", s.RPC.RPCUser)
			require.Equal(t, "bitcoin", s.RPC.RPCPass)
		})
	}
}
