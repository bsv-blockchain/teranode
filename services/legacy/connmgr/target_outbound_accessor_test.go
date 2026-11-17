package connmgr

import (
	"testing"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestTargetOutboundReportsEffectiveTarget pins why the accessor exists at all.
//
// New substitutes defaultTargetOutbound when the caller leaves the target at
// zero, and it does so before copying the config, so the manager's effective
// target and the caller's own configuration can differ. A caller that judged
// "am I at target?" from its own copy would read zero in exactly the case that
// matters — an unconfigured node at startup with no outbound peers — and
// conclude it was already full.
func TestTargetOutboundReportsEffectiveTarget(t *testing.T) {
	cmgr, err := New(ulogger.TestLogger{}, &Config{
		Dial:           mockDialer,
		TargetOutbound: 5,
	})
	require.NoError(t, err)
	require.Equal(t, uint32(5), cmgr.TargetOutbound())

	substituted, err := New(ulogger.TestLogger{}, &Config{
		Dial: mockDialer,
	})
	require.NoError(t, err)
	require.Equal(t, uint32(defaultTargetOutbound), substituted.TargetOutbound(),
		"an unset target becomes the default, and the accessor must report the default, not zero")
}
