/*
Package validator implements Bitcoin SV transaction validation functionality.

This package provides comprehensive transaction validation for Bitcoin SV nodes,
including script verification, UTXO management, and policy enforcement. It supports
multiple script interpreters (GoBT, GoSDK, GoBDK) and implements the full Bitcoin
transaction validation ruleset.

Key features:
  - Transaction validation against Bitcoin consensus rules
  - UTXO spending and creation
  - Script verification using multiple interpreters
  - Policy enforcement
  - Block assembly integration
  - Kafka integration for transaction metadata

Usage:

	validator := NewTxValidator(logger, policy, params)
	err := validator.ValidateTransaction(tx, blockHeight)
*/
package validator

import (
	"testing"

	"github.com/bitcoin-sv/ubsv/chaincfg"
	"github.com/bitcoin-sv/ubsv/settings"
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
	block, err := getTxs(testBlockID)
	require.NoError(t, err)

	t.Run("BDK Multi Routine", func(t *testing.T) {
		verifier := newScriptVerifierGoBDK(ulogger.TestLogger{}, settings.NewPolicySettings(), &chaincfg.MainNetParams)
		testBlockMultiRoutines(t, verifier, block.Txs)
	})

	t.Run("BDK Sequential", func(t *testing.T) {
		verifier := newScriptVerifierGoBDK(ulogger.TestLogger{}, settings.NewPolicySettings(), &chaincfg.MainNetParams)
		testBlockSequential(t, verifier, block.Txs)
	})
}
