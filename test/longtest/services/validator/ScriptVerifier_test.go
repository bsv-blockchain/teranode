package validator

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/services/validator"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/gocarina/gocsv"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// go test -v -timeout 60m ./test/longtest/services/validator/...

var testStoreURL = "https://ubsv-public.s3.eu-west-1.amazonaws.com/testdata"

// CsvDataRecord hold a data record for csv file
type CsvDataRecord struct {
	ChainNet        string
	BlockHeight     uint32
	TXID            string
	TxHexExtended   string
	UTXOHeights     string // string joinning utxo heights with separator |
	DataUTXOHeights []uint32
	Tx              *bt.Tx
}

// Benchmark run script verification with different verifier without
// caring about the error
//
//	go test -bench=ScriptVerification -tags test_validator -timeout 120m ./test/services/validator/...
func BenchmarkScriptVerification(b *testing.B) {

	csvDataFile := "mainnet_14207txs_b886413_WUH.csv"

	txsData, err := getTxsData(csvDataFile)
	if err != nil {
		panic(err)
	}

	tLogger := &ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings()
	scriptInterpreterTypes := []string{"GoBDK", "GoSDK", "GoBT"}
	for _, siType := range scriptInterpreterTypes {
		createTxScriptInterpreter, ok := validator.TxScriptInterpreterFactory[validator.TxInterpreter(siType)]
		if !ok {
			panic(fmt.Errorf("unable to find script interpreter %v", siType))
		}

		scriptInterpreter := createTxScriptInterpreter(tLogger, tSettings.Policy, &chaincfg.MainNetParams)

		testNameSequential := fmt.Sprintf("ScriptVerification Sequential %v", siType)
		testNameMultiRoutine := fmt.Sprintf("ScriptVerification Multi Routine %v", siType)

		b.ResetTimer()
		b.Run(testNameMultiRoutine, func(b *testing.B) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("recovered from panic: %v", r)
				}
			}()

			for i := 0; i < b.N; i++ {
				benchVerificationMultiRoutines(b, scriptInterpreter, txsData)
			}
		})

		b.Run(testNameSequential, func(b *testing.B) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("recovered from panic: %v", r)
				}
			}()

			for i := 0; i < b.N; i++ {
				benchVerificationSequential(b, scriptInterpreter, txsData)
			}
		})
	}

}

// To run this test
//
//	go clean -testcache && go test -v -tags test_validator -run Test_ScriptVerificationBDKLargeTx -timeout 60m ./test/services/validator/...
func Test_ScriptVerificationBDKLargeTx(t *testing.T) {
	// Test Large Tx with GoBDK only to test accuracy
	csvDataFiles := []string{"mainnet_large_txs.csv", "mainnet_14207txs_b886413_WUH.csv"}
	for _, csvDataFile := range csvDataFiles {
		testName := fmt.Sprintf("File_%v", csvDataFile)
		t.Run(testName, func(t *testing.T) {
			txsData, err := getTxsData(csvDataFile)
			require.NoError(t, err)

			tLogger := &ulogger.TestLogger{}
			tSettings := test.CreateBaseTestSettings()
			tSettings.ChainCfgParams, err = chaincfg.GetChainParams("mainnet")
			require.NoError(t, err)

			createTxScriptInterpreter, ok := validator.TxScriptInterpreterFactory[validator.TxInterpreter("GoBDK")]
			if !ok {
				panic(fmt.Errorf("unable to find script interpreter %v", "GoBDK"))
			}

			bdkScriptInterpreter := createTxScriptInterpreter(tLogger, tSettings.Policy, &chaincfg.MainNetParams)

			for _, txData := range txsData {
				// fmt.Printf("Verify for %v  %v\n", txData.BlockHeight, txData.Tx.TxID())
				err := bdkScriptInterpreter.VerifyScript(txData.Tx, txData.BlockHeight, true, txData.DataUTXOHeights)
				require.NoError(t, err)
			}
		})
	}
}

func benchVerificationMultiRoutines(b *testing.B, verifier validator.TxScriptInterpreter, txsData []CsvDataRecord) {
	g := errgroup.Group{}

	// verify the scripts of all the transactions in parallel
	for _, txData := range txsData {
		g.Go(func() error {
			return verifier.VerifyScript(txData.Tx, txData.BlockHeight, true, txData.DataUTXOHeights)
		})
	}

	err := g.Wait()
	if err != nil {
		fmt.Printf("\nError running multiple routine %v\n", err.Error())
	}
}

func benchVerificationSequential(b *testing.B, verifier validator.TxScriptInterpreter, txsData []CsvDataRecord) {
	nbError := 0
	for _, txData := range txsData {
		err := verifier.VerifyScript(txData.Tx, txData.BlockHeight, true, txData.DataUTXOHeights)
		if err != nil {
			nbError += 1
		}
	}

	if nbError > 0 {
		fmt.Printf("\nError running sequential %v errors\n", nbError)
	}
}

func getTxsData(csvDataFile string) ([]CsvDataRecord, error) {

	exists, err := os.Stat(csvDataFile)

	if err != nil {
		if !errors.Is(err, syscall.Errno(2)) {
			return nil, err
		}
	}

	if exists == nil {
		if err := fetchCsvDataFromTestStore(csvDataFile); err != nil {
			return nil, err
		}
	}

	ret := []CsvDataRecord{}

	file, err := os.OpenFile(csvDataFile, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return ret, fmt.Errorf("error opening file : %v. error : %v", csvDataFile, err)
	}
	defer file.Close()

	if err := gocsv.UnmarshalFile(file, &ret); err != nil {
		return ret, fmt.Errorf("error parsing file : %v. error : %v", csvDataFile, err)
	}

	// Post process, trim all leading and trailing whitespace
	for i := 0; i < len(ret); i++ {
		ret[i].ChainNet = strings.TrimSpace(ret[i].ChainNet)
		ret[i].TXID = strings.TrimSpace(ret[i].TXID)
		ret[i].TxHexExtended = strings.TrimSpace(ret[i].TxHexExtended)
		ret[i].UTXOHeights = strings.TrimSpace(ret[i].UTXOHeights)

		// Preparse binary tx
		tx, err := bt.NewTxFromString(ret[i].TxHexExtended)
		if err != nil {
			return ret, fmt.Errorf("failed to parse tx for line %v, TxID %v, error %v", i, ret[i].TXID, err)
		}
		ret[i].Tx = tx

		// Parse utxo heights
		if len(ret[i].UTXOHeights) > 0 {
			parts := strings.Split(ret[i].UTXOHeights, "|")
			ret[i].DataUTXOHeights = make([]uint32, len(parts))

			for k, p := range parts {
				h, err := strconv.ParseUint(p, 10, 32)
				if err != nil {
					return ret, fmt.Errorf("error parsing utxo height at line %v, error :%v", i, err)
				}
				ret[i].DataUTXOHeights[k] = uint32(h)
			}
		}
	}

	return ret, nil
}

func fetchCsvDataFromTestStore(csvDataFile string) error {
	// get the block from the test store
	URL := fmt.Sprintf("%s/%s", testStoreURL, csvDataFile)

	req, err := http.NewRequest("GET", URL, nil)

	if err != nil {
		return err
	}

	client := http.Client{}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	defer func() {
		_ = resp.Body.Close()
	}()

	// read the body
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	// write the block to a local file
	err = os.WriteFile(csvDataFile, b, 0600)
	if err != nil {
		return err
	}
	return nil
}
