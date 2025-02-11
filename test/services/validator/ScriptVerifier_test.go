//go:build test_all || test_services || test_validator

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
	"github.com/bitcoin-sv/teranode/chaincfg"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/services/validator"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/libsv/go-bt/v2"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// go test -v -tags test_validator ./test/...

var testStoreURL = "https://teranode-public.s3.eu-west-1.amazonaws.com/testdata"

type TxsExtended struct {
	Txs    []*bt.Tx
	TxsBin [][]byte
}

func createScriptInterpreter(verifierType validator.TxInterpreter, logger ulogger.Logger, policy *settings.PolicySettings, params *chaincfg.Params) validator.TxScriptInterpreter {
	createTxScriptInterpreter, ok := validator.TxScriptInterpreterFactory[verifierType]
	if !ok {
		panic(fmt.Errorf("unable to find script interpreter %v", verifierType))
	}

	return createTxScriptInterpreter(logger, policy, params)
}

func Test_ScriptVerification(t *testing.T) {

	tSettings := test.CreateBaseTestSettings()
	testBlockID := "000000000000000000a69d478ffc96546356028d192b62534ec22663ac2457e9"
	eBlock, err := getTxs(testBlockID)
	require.NoError(t, err)

	t.Run("BDK Verify Extend Multi Routine", func(t *testing.T) {
		testVerifyExtendMultiRoutines(t, eBlock.TxsBin)
	})

	t.Run("BDK Multi Routine", func(t *testing.T) {
		verifier := createScriptInterpreter("GoBDK", ulogger.TestLogger{}, tSettings.Policy, &chaincfg.MainNetParams)
		testBlockMultiRoutines(t, verifier, eBlock.Txs)
	})

	t.Run("GoSDK Multi Routine", func(t *testing.T) {
		verifier := createScriptInterpreter("GoSDK", ulogger.TestLogger{}, tSettings.Policy, &chaincfg.MainNetParams)
		testBlockMultiRoutines(t, verifier, eBlock.Txs)
	})

	t.Run("GoBt Multi Routine", func(t *testing.T) {
		verifier := createScriptInterpreter("GoBT", ulogger.TestLogger{}, tSettings.Policy, &chaincfg.MainNetParams)
		testBlockMultiRoutines(t, verifier, eBlock.Txs)
	})

	t.Run("BDK Verify Extend Sequential", func(t *testing.T) {
		testVerifyExtendSequential(t, eBlock.TxsBin)
	})

	t.Run("BDK Sequential", func(t *testing.T) {
		verifier := createScriptInterpreter("GoBDK", ulogger.TestLogger{}, tSettings.Policy, &chaincfg.MainNetParams)
		testBlockSequential(t, verifier, eBlock.Txs)
	})

	t.Run("GoSDK Sequential", func(t *testing.T) {
		verifier := createScriptInterpreter("GoSDK", ulogger.TestLogger{}, tSettings.Policy, &chaincfg.MainNetParams)
		testBlockSequential(t, verifier, eBlock.Txs)
	})

	t.Run("GoBt Sequential", func(t *testing.T) {
		verifier := createScriptInterpreter("GoBT", ulogger.TestLogger{}, tSettings.Policy, &chaincfg.MainNetParams)
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
			return bdkscript.VerifyExtend(txBin, 725267, true)
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
		err := bdkscript.VerifyExtend(txBin, 725267, true)
		require.NoError(t, err)
	}
}

func testBlockMultiRoutines(t *testing.T, verifier validator.TxScriptInterpreter, txs []*bt.Tx) {
	g := errgroup.Group{}

	// verify the scripts of all the transactions in parallel
	for _, tx := range txs {
		g.Go(func() error {
			return verifier.VerifyScript(tx, 725267, true)
		})
	}

	err := g.Wait()
	require.NoError(t, err)
}

func testBlockSequential(t *testing.T, verifier validator.TxScriptInterpreter, txs []*bt.Tx) {
	for _, tx := range txs {
		err := verifier.VerifyScript(tx, 725267, true)
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
	blockFilename := fmt.Sprintf("%s.extended.bin", testBlockID)

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
