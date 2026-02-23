// Package rpc implements the Bitcoin JSON-RPC API service for Teranode.
package rpc

import (
	"context"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/rpc/bsvjson"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandleGetMiningInfo(t *testing.T) {
	difficulty := 97415240192.16336
	expectedHashRate := 6.973254512622107e+17
	networkHashPS := calculateHashRate(difficulty, chaincfg.MainNetParams.TargetTimePerBlock.Seconds())
	assert.Equal(t, networkHashPS, expectedHashRate)
}

func TestIPOrSubnetValidation(t *testing.T) {
	ip := "172.19.0.8"

	assert.True(t, isIPOrSubnet(ip))

	// test subnet
	subnet := "172.19.0.0/24"
	assert.True(t, isIPOrSubnet(subnet))

	// test invalid ip
	invalidIP := "172.19.0.8.8"
	assert.False(t, isIPOrSubnet(invalidIP))

	// test invalid subnet
	invalidSubnet := "172.19.0.0/33"
	assert.False(t, isIPOrSubnet(invalidSubnet))

	// test ip with port
	invalidSubnet = "172.19.0.0:8080"
	assert.False(t, isIPOrSubnet(invalidSubnet))

	// test subnet with port (not valid CIDR)
	invalidSubnet = "172.19.0.0/24:8080"
	assert.False(t, isIPOrSubnet(invalidSubnet))
}

func TestSanityCheckGetRawTransaction(t *testing.T) {
	txHex := "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a01000000434104e70a02f5af48a1989bf630d92523c9d14c45c75f7d1b998e962bff6ff9995fc5bdb44f1793b37495d80324acba7c8f537caaf8432b8d47987313060cc82d8a93ac00000000"

	bytes, err := hex.DecodeString(txHex)
	if err != nil {
		t.Fatal(err)
	}

	tx, err := bt.NewTxFromBytes(bytes)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, "bfc04d8db515101ccbaf674cae7700ad413b9b334984c377e71755f2361cb766", tx.TxID())
}

func TestHandleGetRawTransaction(t *testing.T) {
	// Sample transaction hex data - this is a complete, valid transaction hex
	txHex := "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a01000000434104e70a02f5af48a1989bf630d92523c9d14c45c75f7d1b998e962bff6ff9995fc5bdb44f1793b37495d80324acba7c8f537caaf8432b8d47987313060cc82d8a93ac00000000"
	txid := "bfc04d8db515101ccbaf674cae7700ad413b9b334984c377e71755f2361cb766"

	// Create a test server that will respond with our sample transaction
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/api/v1/tx/") && strings.Contains(r.URL.Path, "/hex") {
			// Extract the transaction ID from the URL path
			pathParts := strings.Split(r.URL.Path, "/")
			requestTxid := ""

			for i, part := range pathParts {
				if part == "tx" && i+1 < len(pathParts) {
					requestTxid = pathParts[i+1]
					break
				}
			}

			// Only return the transaction if the ID matches our test transaction
			if requestTxid == txid {
				w.WriteHeader(http.StatusOK)
				_, _ = io.WriteString(w, txHex)

				return
			}
		}

		// Return 404 for any other request or non-matching transaction ID
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	// Create a mock RPCServer with the test server URL
	assetURL, _ := url.Parse(server.URL)
	s := &RPCServer{
		assetHTTPURL: assetURL,
		settings: &settings.Settings{
			ChainCfgParams: &chaincfg.Params{},
		},
	}

	// Test case 1: Non-verbose mode (return raw hex)
	verboseLevel := 0
	cmd := &bsvjson.GetRawTransactionCmd{
		Txid:    txid,
		Verbose: &verboseLevel,
	}

	result, err := handleGetRawTransaction(context.Background(), s, cmd, nil)
	require.NoError(t, err)
	assert.Equal(t, txHex, result)

	// Test case 2: Verbose mode (return transaction details)
	verboseLevel = 1
	cmd = &bsvjson.GetRawTransactionCmd{
		Txid:    txid,
		Verbose: &verboseLevel,
	}

	result, err = handleGetRawTransaction(context.Background(), s, cmd, nil)
	require.NoError(t, err)

	txResult, ok := result.(bsvjson.TxRawResult)
	require.True(t, ok)
	assert.Equal(t, txid, txResult.Txid)
	assert.Equal(t, txHex, txResult.Hex)
	assert.Equal(t, txid, txResult.Hash)
	assert.Equal(t, int32(134), txResult.Size)
	assert.Equal(t, int32(1), txResult.Version)
	assert.Equal(t, uint32(0x0), txResult.LockTime)
	assert.Equal(t, 1, len(txResult.Vin))
	assert.Equal(t, 1, len(txResult.Vout))
	assert.Equal(t, float64(5000000000), txResult.Vout[0].Value)

	// Test case 3: Error when asset URL is not set
	s.assetHTTPURL = nil
	_, err = handleGetRawTransaction(context.Background(), s, cmd, nil)
	require.Error(t, err)
	assert.True(t, errors.Is(err, errors.ErrConfiguration))

	// Test case 4: Error when transaction not found
	s.assetHTTPURL = assetURL
	cmd.Txid = "nonexistenttxid"
	_, err = handleGetRawTransaction(context.Background(), s, cmd, nil)
	require.Error(t, err)
}
