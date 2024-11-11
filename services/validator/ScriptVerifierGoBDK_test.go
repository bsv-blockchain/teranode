//go:build bdk

package validator

import (
	"testing"

	"github.com/bitcoin-sv/ubsv/chaincfg"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/stretchr/testify/require"
)

// To run this test, make sure GoBDK is installed and environment for it is set
// Then run
//
//	go clean -testcache && VERY_LONG_TESTS=1 go test -v -tags "bdk" -run Test_ScriptVerificationGoBDK ./services/validator/...
func Test_ScriptVerificationGoBDK(t *testing.T) {
	util.SkipVeryLongTests(t)

	testBlockID := "000000000000000000a69d478ffc96546356028d192b62534ec22663ac2457e9"
	txs, err := getTxs(testBlockID)
	require.NoError(t, err)

	t.Run("BDK Multi Routine", func(t *testing.T) {
		verifier := newScriptVerifierGoBDK(ulogger.TestLogger{}, NewPolicySettings(), &chaincfg.MainNetParams)
		testBlockMultiRoutines(t, verifier, txs)
	})

	t.Run("BDK Sequential", func(t *testing.T) {
		verifier := newScriptVerifierGoBDK(ulogger.TestLogger{}, NewPolicySettings(), &chaincfg.MainNetParams)
		testBlockSequential(t, verifier, txs)
	})
}
