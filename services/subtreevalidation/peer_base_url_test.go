package subtreevalidation

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jarcoal/httpmock"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// smugglingBase is the base URL shape from issue 4843: with string concatenation the
// appended /subtree/<hash>/txs became query data, and the POST went to the Redpanda admin
// endpoint with a body built from the peer's subtree.
const smugglingBase = "http://attacker.example:9644/v1/debug/bundle?x="

// countAllRequests answers every request that reaches the mocked transport and counts it,
// so a test can prove nothing was sent at all.
func countAllRequests() *atomic.Int64 {
	var hits atomic.Int64

	httpmock.RegisterNoResponder(func(*http.Request) (*http.Response, error) {
		hits.Add(1)
		return httpmock.NewBytesResponse(http.StatusOK, make([]byte, 64)), nil
	})

	return &hits
}

func TestGetMissingTransactionsBatch_QueryBaseSendsNothing(t *testing.T) {
	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()

	hits := countAllRequests()

	server := &Server{
		logger:                       ulogger.TestLogger{},
		settings:                     test.CreateBaseTestSettings(t),
		subtreeStore:                 memory.New(),
		invalidSubtreeKafkaProducer:  &mockKafkaProducer{},
		invalidSubtreeDeDuplicateMap: expiringmap.New[string, struct{}](time.Minute),
	}
	defer server.invalidSubtreeDeDuplicateMap.Stop()

	subtreeHash := chainhash.HashH([]byte("subtree-4843"))
	missing := []utxo.UnresolvedMetaData{{Hash: chainhash.HashH([]byte("tx1")), Idx: 0}}

	_, err := server.getMissingTransactionsBatch(context.Background(), subtreeHash, missing, smugglingBase, "")
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrExternal))
	require.Zero(t, hits.Load(), "no request may leave for a smuggling base URL")
}

func TestGetSubtreeTxHashes_QueryBaseSendsNothing(t *testing.T) {
	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()

	hits := countAllRequests()

	server := &Server{
		logger:       ulogger.TestLogger{},
		settings:     test.CreateBaseTestSettings(t),
		subtreeStore: memory.New(),
	}

	subtreeHash := chainhash.HashH([]byte("subtree-4843"))

	_, err := server.getSubtreeTxHashes(context.Background(), gocore.NewStat("test"), &subtreeHash, smugglingBase, "")
	require.Error(t, err)
	require.Zero(t, hits.Load(), "no request may leave for a smuggling base URL")
}
