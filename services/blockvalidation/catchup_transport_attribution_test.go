package blockvalidation

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"syscall"
	"testing"
	"testing/iotest"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/require"
)

// Exercise the HTTP reader as well as the decoder: decoder-only bytes.Reader
// tests retain EOF and miss the sanitized transport errors returned by net/http.
func TestPeerBlockFetches_TransportFailureNotInvalid(t *testing.T) {
	for _, batch := range []bool{false, true} {
		for _, failure := range []struct {
			name string
			err  error
			want error
		}{
			{"short Content-Length", nil, errors.ErrNetworkError},
			{"connection reset", syscall.ECONNRESET, errors.ErrNetworkError},
			{"typed transport failure", errors.NewNetworkError("private-token context canceled"), errors.ErrNetworkError},
			{"typed timeout", errors.NewNetworkTimeoutError("private-token context canceled"), errors.ErrNetworkTimeout},
			{"typed connection refused", errors.NewNetworkConnectionRefusedError("private-token context canceled"), errors.ErrNetworkConnectionRefused},
		} {
			t.Run(fmt.Sprintf("batch=%v/%s", batch, failure.name), func(t *testing.T) {
				server := &Server{settings: test.CreateBaseTestSettings(t), logger: ulogger.TestLogger{}}
				server.settings.BlockValidation.PerPeerFetchRate = 0
				block := testhelpers.CreateTestBlockChain(t, 2)[1]
				data, err := block.Bytes()
				require.NoError(t, err)

				httpmock.ActivateNonDefault(util.HTTPClient())
				defer httpmock.DeactivateAndReset()
				httpmock.RegisterNoResponder(func(req *http.Request) (*http.Response, error) {
					if failure.err == nil {
						// A real HTTP/1 body emits UnexpectedEOF when its declared length
						// exceeds the bytes delivered. No network listener is needed.
						wire := append([]byte(fmt.Sprintf("HTTP/1.1 200 OK\r\nContent-Length: %d\r\n\r\n", len(data))), data[:40]...)
						return http.ReadResponse(bufio.NewReader(bytes.NewReader(wire)), req)
					}
					body := io.NopCloser(io.MultiReader(bytes.NewReader(data[:40]), iotest.ErrReader(failure.err)))
					return blockHTTPResponse(body), nil
				})

				ctx := context.Background()
				if batch {
					_, err = server.fetchBlocksBatch(ctx, block.Hash(), 1, "peer", "http://peer")
				} else {
					_, err = server.fetchSingleBlock(ctx, block.Hash(), "peer", "http://peer")
				}
				require.ErrorIs(t, err, failure.want)
				require.False(t, errors.Is(err, errors.ErrBlockInvalid), "transport failure must not trigger malicious-block scoring: %v", err)
				require.False(t, errors.Is(err, errors.ErrTxInvalid))
				require.False(t, isLocalCatchupFault(err))
				require.False(t, errors.IsMaliciousResponseError(err))
				require.NotContains(t, err.Error(), "private-token")
				require.NoError(t, ctx.Err(), "timeout fixture must not rely on caller expiry")
			})
		}
	}
}

func TestPeerBlockFetches_CompleteMalformedBlockRemainsInvalid(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprintf("batch=%v", batch), func(t *testing.T) {
			server := &Server{settings: test.CreateBaseTestSettings(t), logger: ulogger.TestLogger{}}
			server.settings.BlockValidation.PerPeerFetchRate = 0
			block := testhelpers.CreateTestBlockChain(t, 2)[1]
			block.Subtrees = []*chainhash.Hash{{}}
			data, err := block.Bytes()
			require.NoError(t, err)

			httpmock.ActivateNonDefault(util.HTTPClient())
			defer httpmock.DeactivateAndReset()
			httpmock.RegisterNoResponder(httpmock.NewBytesResponder(http.StatusOK, data))

			if batch {
				_, err = server.fetchBlocksBatch(context.Background(), block.Hash(), 1, "peer", "http://peer")
			} else {
				_, err = server.fetchSingleBlock(context.Background(), block.Hash(), "peer", "http://peer")
			}
			require.ErrorIs(t, err, errors.ErrBlockInvalid)
			require.False(t, errors.Is(err, errors.ErrNetworkError))
			require.False(t, isLocalCatchupFault(err))
		})
	}
}
