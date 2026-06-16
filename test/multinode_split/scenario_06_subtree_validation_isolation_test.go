//go:build network_chaos

package multinodesplit

import (
	"testing"
)

// TestSubtreeValidationIsolation is intended to assert that PAUSING the
// subtreevalidation service on node 3 stalls block sync, exercising the
// frozen-dependency failure mode (gRPC call hangs, not refused) along the
// blockvalidation.CheckBlockSubtrees -> subtreevalidation path.
//
// Currently SKIPPED for the same reason as TestValidatorIsolation: the
// receive path short-circuits before the subtreevalidation gRPC call when
// the inbound block has 0 subtrees (services/blockvalidation/BlockValidation.go:2023-2027),
// and `Generate`-mined blocks are empty. See the file-level docstring on
// scenario_05_validator_isolation_test.go for the empirical evidence and
// the path forward.
//
// Tracking: follow-up to add tx-injection plumbing before unskipping. When
// unskipped, this scenario specifically exercises the PauseService verb (vs
// scenario 05's KillService) to surface any hidden coupling as a hang rather
// than a connection-refused.
func TestSubtreeValidationIsolation(t *testing.T) {
	t.Skip("subtreevalidation coupling on the receive path requires non-empty blocks; see scenario_05 file-level docstring for the path forward")
}
