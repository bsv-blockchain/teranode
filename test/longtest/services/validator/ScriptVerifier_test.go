package validator

import (
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
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

	csvDataFile := "mainnet_14207txs_b886413.csv"

	txsData, err := getTxsData(csvDataFile)
	if err != nil {
		panic(err)
	}

	tLogger := &ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(b)
	scriptInterpreterTypes := []string{"GoBDK", "GoSDK", "GoBT"}
	for _, siType := range scriptInterpreterTypes {
		createTxScriptInterpreter, ok := validator.TxScriptInterpreterFactory[validator.TxInterpreter(siType)]
		if !ok {
			panic(errors.NewUnknownError("unable to find script interpreter " + fmt.Sprint(siType)))
		}

		scriptInterpreter := createTxScriptInterpreter(tLogger, tSettings.Policy, &chaincfg.MainNetParams)

		testNameSequential := fmt.Sprintf("ScriptVerification Sequential %v", siType)
		testNameMultiRoutine := fmt.Sprintf("ScriptVerification Multi Routine %v", siType)

		b.ResetTimer()
		b.Run(testNameMultiRoutine, func(b *testing.B) {
			defer func() {
				if r := recover(); r != nil {
					err = errors.NewUnknownError("recovered from panic: " + fmt.Sprint(r))
				}
			}()

			for i := 0; i < b.N; i++ {
				benchVerificationMultiRoutines(b, scriptInterpreter, txsData)
			}
		})

		b.Run(testNameSequential, func(b *testing.B) {
			defer func() {
				if r := recover(); r != nil {
					err = errors.NewUnknownError("recovered from panic: " + fmt.Sprint(r))
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
//	go clean -testcache && go test -v -run Test_ScriptVerificationBDKLargeTx -timeout 60m ./test/longtest/services/validator/...
func Test_ScriptVerificationBDKLargeTx(t *testing.T) {
	// Test Large Tx with GoBDK only to test accuracy
	csvDataFiles := []string{"mainnet_large_txs.csv", "mainnet_14207txs_b886413.csv"}
	for _, csvDataFile := range csvDataFiles {
		testName := fmt.Sprintf("File_%v", csvDataFile)
		t.Run(testName, func(t *testing.T) {
			txsData, err := getTxsData(csvDataFile)
			require.NoError(t, err)

			tLogger := &ulogger.TestLogger{}
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.ChainCfgParams, err = chaincfg.GetChainParams("mainnet")
			require.NoError(t, err)

			createTxScriptInterpreter, ok := validator.TxScriptInterpreterFactory[validator.TxInterpreter("GoBDK")]
			if !ok {
				panic(errors.NewUnknownError("unable to find script interpreter GoBDK"))
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

// To run this test
//
//	go clean -testcache && go test -v -run Test_ScriptVerificationBDKTestNetData -timeout 60m ./test/longtest/services/validator/...
func Test_ScriptVerificationBDKTestNetData(t *testing.T) {
	// Test Large Tx with GoBDK only to test accuracy
	csvDataFiles := []string{"testnet_18869txs_b1682153.csv"}
	for _, csvDataFile := range csvDataFiles {
		testName := fmt.Sprintf("File_%v", csvDataFile)
		t.Run(testName, func(t *testing.T) {
			txsData, err := getTxsData(csvDataFile)
			require.NoError(t, err)

			tLogger := &ulogger.TestLogger{}
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.ChainCfgParams, err = chaincfg.GetChainParams("testnet")
			require.NoError(t, err)

			createTxScriptInterpreter, ok := validator.TxScriptInterpreterFactory[validator.TxInterpreter("GoBDK")]
			if !ok {
				panic(errors.NewUnknownError("unable to find script interpreter GoBDK"))
			}

			bdkScriptInterpreter := createTxScriptInterpreter(tLogger, tSettings.Policy, &chaincfg.TestNetParams)

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
		return ret, errors.NewUnknownError("error opening file : " + csvDataFile + ". error : " + err.Error())
	}
	defer file.Close()

	reader := csv.NewReader(file)

	// Read and discard header row
	if _, err := reader.Read(); err != nil {
		return ret, errors.NewUnknownError("error reading CSV header: " + err.Error())
	}

	records, err := reader.ReadAll()
	if err != nil {
		return ret, errors.NewUnknownError("error parsing file : " + csvDataFile + ". error : " + err.Error())
	}

	ret = make([]CsvDataRecord, len(records))
	for i, record := range records {
		if len(record) < 5 {
			return ret, errors.NewUnknownError("invalid CSV record at line " + strconv.Itoa(i+2))
		}
		blockHeight, err := strconv.ParseUint(strings.TrimSpace(record[1]), 10, 32)
		if err != nil {
			return ret, errors.NewUnknownError("error parsing BlockHeight at line " + strconv.Itoa(i+2) + ": " + err.Error())
		}
		ret[i] = CsvDataRecord{
			ChainNet:      strings.TrimSpace(record[0]),
			BlockHeight:   uint32(blockHeight),
			TXID:          strings.TrimSpace(record[2]),
			TxHexExtended: strings.TrimSpace(record[3]),
			UTXOHeights:   strings.TrimSpace(record[4]),
		}

		// Preparse binary tx
		tx, err := bt.NewTxFromString(ret[i].TxHexExtended)
		if err != nil {
			return ret, errors.NewUnknownError("failed to parse tx for line " + strconv.Itoa(i) + ", TxID " + ret[i].TXID + ", error " + err.Error())
		}
		ret[i].Tx = tx

		// Parse utxo heights
		if len(ret[i].UTXOHeights) > 0 {
			parts := strings.Split(ret[i].UTXOHeights, "|")
			ret[i].DataUTXOHeights = make([]uint32, len(parts))

			for k, p := range parts {
				h, err := strconv.ParseUint(p, 10, 32)
				if err != nil {
					return ret, errors.NewUnknownError("error parsing utxo height at line " + strconv.Itoa(i) + ", error :" + err.Error())
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
