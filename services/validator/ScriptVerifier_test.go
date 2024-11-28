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
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"syscall"
	"testing"

	bdkconfig "github.com/bitcoin-sv/bdk/module/gobdk/config"
	bdkscript "github.com/bitcoin-sv/bdk/module/gobdk/script"
	"github.com/bitcoin-sv/ubsv/chaincfg"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/settings"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// To run the test
//   go clean -testcache && VERY_LONG_TESTS=1 go test -v -run Test_ScriptVerification ./services/validator/...

var testStoreURL = "https://ubsv-public.s3.eu-west-1.amazonaws.com/testdata"

type TxsExtended struct {
	Txs    []*bt.Tx
	TxsBin [][]byte
}

func Test_ScriptVerification(t *testing.T) {
	util.SkipVeryLongTests(t)

	testBlockID := "000000000000000000a69d478ffc96546356028d192b62534ec22663ac2457e9"
	eBlock, err := getTxs(testBlockID)
	require.NoError(t, err)

	t.Run("BDK Verify Extend Multi Routine", func(t *testing.T) {
		testVerifyExtendMultiRoutines(t, eBlock.TxsBin)
	})

	t.Run("BDK Multi Routine", func(t *testing.T) {
		verifier := newScriptVerifierGoBDK(ulogger.TestLogger{}, settings.NewPolicySettings(), &chaincfg.MainNetParams)
		testBlockMultiRoutines(t, verifier, eBlock.Txs)
	})

	t.Run("GoSDK Multi Routine", func(t *testing.T) {
		verifier := newScriptVerifierGoSDK(ulogger.TestLogger{}, settings.NewPolicySettings(), &chaincfg.MainNetParams)
		testBlockMultiRoutines(t, verifier, eBlock.Txs)
	})

	t.Run("GoBt Multi Routine", func(t *testing.T) {
		verifier := newScriptVerifierGoBt(ulogger.TestLogger{}, settings.NewPolicySettings(), &chaincfg.MainNetParams)
		testBlockMultiRoutines(t, verifier, eBlock.Txs)
	})

	t.Run("BDK Verify Extend Sequential", func(t *testing.T) {
		testVerifyExtendSequential(t, eBlock.TxsBin)
	})

	t.Run("BDK Sequential", func(t *testing.T) {
		verifier := newScriptVerifierGoBDK(ulogger.TestLogger{}, settings.NewPolicySettings(), &chaincfg.MainNetParams)
		testBlockSequential(t, verifier, eBlock.Txs)
	})

	t.Run("GoSDK Sequential", func(t *testing.T) {
		verifier := newScriptVerifierGoSDK(ulogger.TestLogger{}, settings.NewPolicySettings(), &chaincfg.MainNetParams)
		testBlockSequential(t, verifier, eBlock.Txs)
	})

	t.Run("GoBt Sequential", func(t *testing.T) {
		verifier := newScriptVerifierGoBt(ulogger.TestLogger{}, settings.NewPolicySettings(), &chaincfg.MainNetParams)
		testBlockSequential(t, verifier, eBlock.Txs)
	})
}

func testVerifyExtendMultiRoutines(t *testing.T, txsBin [][]byte) {
	bdkScriptConfig := bdkconfig.ScriptConfig{ChainNetwork: "main"}
	if err := bdkscript.SetGlobalScriptConfig(bdkScriptConfig); err != nil {
		panic(errors.New(errors.ERR_UNKNOWN, "gobdk was unable to set global config"))
	}

	g := errgroup.Group{}

	// verify the scripts of all the transactions in parallel
	for _, txBin := range txsBin {
		g.Go(func() error {
			return bdkscript.VerifyExtend(txBin, 725267)
		})
	}

	err := g.Wait()
	require.NoError(t, err)
}
func testVerifyExtendSequential(t *testing.T, txsBin [][]byte) {
	bdkScriptConfig := bdkconfig.ScriptConfig{ChainNetwork: "main"}
	if err := bdkscript.SetGlobalScriptConfig(bdkScriptConfig); err != nil {
		panic(errors.New(errors.ERR_UNKNOWN, "gobdk was unable to set global config"))
	}

	// verify the scripts of all the transactions in parallel
	for _, txBin := range txsBin {
		err := bdkscript.VerifyExtend(txBin, 725267)
		require.NoError(t, err)
	}
}

func testBlockMultiRoutines(t *testing.T, verifier TxScriptInterpreter, txs []*bt.Tx) {
	g := errgroup.Group{}

	// verify the scripts of all the transactions in parallel
	for _, tx := range txs {
		g.Go(func() error {
			return verifier.VerifyScript(tx, 725267)
		})
	}

	err := g.Wait()
	require.NoError(t, err)
}

func testBlockSequential(t *testing.T, verifier TxScriptInterpreter, txs []*bt.Tx) {
	for _, tx := range txs {
		err := verifier.VerifyScript(tx, 725267)
		require.NoError(t, err)
	}
}

func getTxs(testBlockID string) (*TxsExtended, error) {
	blockBytes, err := fetchBlockFromTestStore(testBlockID)
	if err != nil {
		return nil, err
	}

	reader := bytes.NewReader(blockBytes)

	maxNbTx := 44_407 // nb tx at block 000000000000000000a69d478ffc96546356028d192b62534ec22663ac2457e9
	txsExtend := &TxsExtended{}

	for {
		tx := &bt.Tx{}
		if _, erri := tx.ReadFrom(reader); erri != nil {
			if len(txsExtend.Txs) == maxNbTx {
				break
			} else {
				return nil, errors.New(errors.ERR_ERROR, "error reading data, read %v txs, error : %v", len(txsExtend.Txs), err)
			}
		}

		txBin := tx.ExtendedBytes()
		txsExtend.Txs = append(txsExtend.Txs, tx)
		txsExtend.TxsBin = append(txsExtend.TxsBin, txBin)
	}

	return txsExtend, nil
}

func fetchBlockFromTestStore(testBlockID string) ([]byte, error) {
	blockFilename := fmt.Sprintf("testdata/%s.extended.bin", testBlockID)

	exists, err := os.Stat(blockFilename)

	if err != nil {
		if !errors.Is(err, syscall.Errno(2)) {
			return nil, err
		}
	}

	if exists != nil {
		// get the bytes from the file
		return os.ReadFile(blockFilename)
	}

	// get the block from the test store
	URL := fmt.Sprintf("%s/%s.extended.bin", testStoreURL, testBlockID)

	req, err := http.NewRequest("GET", URL, nil)

	if err != nil {
		return nil, err
	}

	client := http.Client{}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = resp.Body.Close()
	}()

	// read the body
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// write the block to a local file
	err = os.WriteFile(blockFilename, b, 0600)
	if err != nil {
		return nil, err
	}

	return b, nil
}
