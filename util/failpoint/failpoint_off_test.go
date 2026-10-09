//go:build !failpoints

package failpoint

import (
	"testing"
)

// TestInjectIsNoOpWithoutTag checks that an armed name is ignored when the
// binary is built without the "failpoints" tag.
func TestInjectIsNoOpWithoutTag(t *testing.T) {
	t.Setenv(EnvVar, UTXOSpendBeforeCreate)

	Inject(UTXOSpendBeforeCreate)
}
