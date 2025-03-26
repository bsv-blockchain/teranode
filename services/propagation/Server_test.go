package propagation

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/services/validator"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blob/null"
	"github.com/bitcoin-sv/teranode/stores/utxo/memory"
	"github.com/bitcoin-sv/teranode/tracing"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/libsv/go-bt/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidatorErrors(t *testing.T) {
	tracing.SetGlobalMockTracer()

	tx := bt.NewTx()

	tSettings := test.CreateBaseTestSettings()

	v, err := validator.New(context.Background(), ulogger.TestLogger{}, tSettings, memory.New(ulogger.TestLogger{}), nil, nil)
	require.NoError(t, err)

	// _, err = v.Validate(context.Background(), tx, chaincfg.GenesisActivationHeight)
	_, err = v.Validate(context.Background(), tx, 0)
	require.Error(t, err)

	assert.True(t, errors.Is(err, errors.ErrProcessing))
	assert.True(t, errors.Is(err, errors.ErrTxInvalid))
}

func TestStartHTTPServer(t *testing.T) {
	initPrometheusMetrics()

	txStore, err := null.New(ulogger.TestLogger{})
	require.NoError(t, err)

	// Create a mock PropagationServer
	ps := &PropagationServer{
		// Initialize with necessary mock dependencies
		logger:    ulogger.TestLogger{},
		validator: &validator.MockValidator{},
		txStore:   txStore,
		settings: &settings.Settings{
			Propagation: settings.PropagationSettings{
				HTTPRateLimit: 20,
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Start the HTTP server on a random available port
	serverAddr := "localhost:0"
	err = ps.startHTTPServer(ctx, serverAddr)
	require.NoError(t, err)

	// Wait for the server to start and get the actual address
	var actualAddr string

	for i := 0; i < 50; i++ {
		time.Sleep(100 * time.Millisecond)

		listenerAddr := ps.httpServer.ListenerAddr()

		if listenerAddr != nil {
			actualAddr = listenerAddr.String()
			break
		}
	}

	require.NotEmpty(t, actualAddr, "Server failed to start")

	baseURL := fmt.Sprintf("http://%s", actualAddr)

	txHex := "010000000000000000ef01669fb8b6dba279e90e085fdcea3bdc27fb98b1e35e1d54062033f8e1216ac071000000006a4730440220592cbdf8375acaacc1fcec4870331bce6cea8b1668acfcd9dbc3a9bf69edf21802200b7a5f97c50c0e42a0a1b0da819292c4748011e086d7f79618e31916b1008b9d412103501de920437c42b4ef372115e4b63ebab8bce51a019cd24084e97296f8a0dfe0ffffffff806a0100000000001976a914fcde0dd2c418fcd4043a64d31a83e0a17d711cec88ac027d6a0100000000001976a914fcde0dd2c418fcd4043a64d31a83e0a17d711cec88ac00000000000000002c006a2970726f7061676174696f6e207465737420323032352d30332d32345431313a30393a31352e3437305a00000000"
	txData, err := hex.DecodeString(txHex)
	require.NoError(t, err)

	txHex2 := "010000000000000000ef013f868f4142f4bc641e031dfae55ae9de3b471f0717d5b7a7022fb4547df76589000000006b48304502210090604217cb346a2b8fc1777d76180b2c2a0e635c31deda5e18b712f016565d340220391b61facfc424f06ee45f88e8c7d46ee1c62f27cd124cdb77db62335d001cd1412103501de920437c42b4ef372115e4b63ebab8bce51a019cd24084e97296f8a0dfe0ffffffff7d6a0100000000001976a914fcde0dd2c418fcd4043a64d31a83e0a17d711cec88ac027a6a0100000000001976a914fcde0dd2c418fcd4043a64d31a83e0a17d711cec88ac00000000000000002c006a2970726f7061676174696f6e207465737420323032352d30332d32345431313a30393a33392e3039335a00000000"
	txData2, err := hex.DecodeString(txHex2)
	require.NoError(t, err)

	t.Run("Test /tx endpoint", func(t *testing.T) {
		resp, err := http.Post(baseURL+"/tx", "application/octet-stream", bytes.NewBuffer(txData))
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Equal(t, "OK", string(body))
	})

	t.Run("Test /txs endpoint", func(t *testing.T) {
		resp, err := http.Post(baseURL+"/txs", "application/octet-stream", bytes.NewBuffer(append(txData, txData2...)))
		require.NoError(t, err)

		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Equal(t, "OK", string(body))
	})

	t.Run("Test rate limiting", func(t *testing.T) {
		for i := 0; i < 25; i++ {
			resp, err := http.Post(baseURL+"/tx", "application/octet-stream", bytes.NewBuffer(txData))
			require.NoError(t, err)
			defer resp.Body.Close()

			if i > 20 {
				assert.Equal(t, http.StatusTooManyRequests, resp.StatusCode, "unexpected status code for %d: %d", i, resp.StatusCode)
			}
		}
	})
}
