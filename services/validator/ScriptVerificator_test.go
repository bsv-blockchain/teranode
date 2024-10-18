//go:build bdk

package validator

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"syscall"
	"testing"

	"github.com/bitcoin-sv/ubsv/chaincfg"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

var testStoreURL = "https://ubsv-public.s3.eu-west-1.amazonaws.com/testdata"

func Test_ScriptVerification(t *testing.T) {
	util.SkipVeryLongTests(t)

	testBlockID := "000000000000000000a69d478ffc96546356028d192b62534ec22663ac2457e9"
	txs, err := getTxs(testBlockID)
	require.NoError(t, err)

	t.Run("BDK", func(t *testing.T) {
		verifier := newScriptVerificatorGoBDK(ulogger.TestLogger{}, NewPolicySettings(), &chaincfg.MainNetParams)
		testBlock(t, verifier, txs)
	})

	t.Run("GoSDK", func(t *testing.T) {
		verifier := newScriptVerificatorGoSDK(ulogger.TestLogger{}, NewPolicySettings(), &chaincfg.MainNetParams)
		testBlock(t, verifier, txs)
	})

	t.Run("GoBt", func(t *testing.T) {
		verifier := newScriptVerificatorGoBt(ulogger.TestLogger{}, NewPolicySettings(), &chaincfg.MainNetParams)
		testBlock(t, verifier, txs)
	})
}

func testBlock(t *testing.T, verifier TxValidator, txs []*bt.Tx) {
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

func getTxs(testBlockID string) ([]*bt.Tx, error) {
	blockBytes, err := fetchBlockFromTestStore(testBlockID)
	if err != nil {
		return nil, err
	}

	reader := bytes.NewReader(blockBytes)

	txs := make([]*bt.Tx, 0)
	for {
		tx := &bt.Tx{}
		_, err = tx.ReadFrom(reader)
		if err != nil {
			break
		}

		txs = append(txs, tx)
	}

	return txs, nil
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
	err = os.WriteFile(blockFilename, b, 0644)
	if err != nil {
		return nil, err
	}

	return b, nil
}
