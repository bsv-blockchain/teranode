// Package subtreevalidation provides functionality for validating subtrees in a blockchain context.
// It handles the validation of transaction subtrees, manages transaction metadata caching,
// and interfaces with blockchain and validation services.
package subtreevalidation

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jarcoal/httpmock"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

const testPeerURL = "http://test-peer.com"

// mockKafkaProducer mocks the Kafka producer to capture published messages
type mockKafkaProducer struct {
	messages []*kafka.Message
}

func (m *mockKafkaProducer) Start(ctx context.Context, ch chan *kafka.Message) {
	// no-op for testing
}

func (m *mockKafkaProducer) Stop() error {
	return nil
}

func (m *mockKafkaProducer) BrokersURL() []string {
	return []string{"localhost:9092"}
}

func (m *mockKafkaProducer) Publish(msg *kafka.Message) {
	m.messages = append(m.messages, msg)
}

// TestInvalidSubtreeReporting_MalformedTransactionData tests that invalid subtree messages ARE sent
// when malformed transaction data is received from a peer
func TestInvalidSubtreeReporting_MalformedTransactionData(t *testing.T) {
	// setup
	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()

	tSettings := test.CreateBaseTestSettings(t)
	subtreeHash := chainhash.HashH([]byte("test-subtree"))
	baseURL := testPeerURL

	// create server with mocked dependencies
	server := &Server{
		logger:                       ulogger.TestLogger{},
		settings:                     tSettings,
		subtreeStore:                 memory.New(),
		invalidSubtreeKafkaProducer:  &mockKafkaProducer{},
		invalidSubtreeDeDuplicateMap: expiringmap.New[string, struct{}](time.Minute * 1),
	}
	defer server.invalidSubtreeDeDuplicateMap.Stop()

	// mock the HTTP response with malformed transaction data
	url := fmt.Sprintf("%s/subtree/%s/txs", baseURL, subtreeHash.String())
	httpmock.RegisterResponder("POST", url,
		func(req *http.Request) (*http.Response, error) {
			// return malformed data that will fail to parse as a transaction
			return httpmock.NewStringResponse(200, "malformed transaction data"), nil
		})

	// call getMissingTransactionsBatch which should trigger invalid subtree reporting
	ctx := context.Background()
	missingTxHashes := []utxo.UnresolvedMetaData{
		{Hash: chainhash.HashH([]byte("tx1")), Idx: 0},
	}

	_, err := server.getMissingTransactionsBatch(ctx, subtreeHash, missingTxHashes, baseURL)

	// verify error is returned
	assert.Error(t, err)
	assert.True(t, errors.Is(err, errors.ErrProcessing))

	// verify invalid subtree message was published
	kafkaProducer := server.invalidSubtreeKafkaProducer.(*mockKafkaProducer)
	require.Len(t, kafkaProducer.messages, 1)

	// decode and verify the message
	var msg kafkamessage.KafkaInvalidSubtreeTopicMessage
	err = proto.Unmarshal(kafkaProducer.messages[0].Value, &msg)
	require.NoError(t, err)

	assert.Equal(t, subtreeHash.String(), msg.SubtreeHash)
	assert.Equal(t, baseURL, msg.PeerUrl)
	assert.Equal(t, "malformed_transaction_data", msg.Reason)
}

// TestInvalidSubtreeReporting_TransactionCountMismatch tests that invalid subtree messages ARE sent
// when the peer returns a different number of transactions than requested
func TestInvalidSubtreeReporting_TransactionCountMismatch(t *testing.T) {
	// setup
	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()

	tSettings := test.CreateBaseTestSettings(t)
	subtreeHash := chainhash.HashH([]byte("test-subtree"))
	baseURL := testPeerURL

	// create server with mocked dependencies
	server := &Server{
		logger:                       ulogger.TestLogger{},
		settings:                     tSettings,
		subtreeStore:                 memory.New(),
		invalidSubtreeKafkaProducer:  &mockKafkaProducer{},
		invalidSubtreeDeDuplicateMap: expiringmap.New[string, struct{}](time.Minute * 1),
	}
	defer server.invalidSubtreeDeDuplicateMap.Stop()

	// create a valid extended transaction using a known valid tx
	tx, err := bt.NewTxFromString("010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")
	require.NoError(t, err)
	require.True(t, tx.IsExtended(), "Transaction must be extended for this test")

	// mock the HTTP response with only one transaction when two were requested
	url := fmt.Sprintf("%s/subtree/%s/txs", baseURL, subtreeHash.String())
	httpmock.RegisterResponder("POST", url,
		func(req *http.Request) (*http.Response, error) {
			// return zero transactions when two were requested - this should trigger count mismatch
			buf := bytes.NewBuffer(nil)
			// write no transactions at all
			return httpmock.NewBytesResponse(200, buf.Bytes()), nil
		})

	// request two transactions but zero will be returned
	ctx := context.Background()
	missingTxHashes := []utxo.UnresolvedMetaData{
		{Hash: chainhash.HashH([]byte("tx1")), Idx: 0},
		{Hash: chainhash.HashH([]byte("tx2")), Idx: 1},
	}

	_, err2 := server.getMissingTransactionsBatch(ctx, subtreeHash, missingTxHashes, baseURL)

	// verify error is returned
	assert.Error(t, err2)
	assert.True(t, errors.Is(err2, errors.ErrProcessing))

	// verify invalid subtree message was published
	kafkaProducer := server.invalidSubtreeKafkaProducer.(*mockKafkaProducer)
	require.Len(t, kafkaProducer.messages, 1)

	// decode and verify the message
	var msg kafkamessage.KafkaInvalidSubtreeTopicMessage
	err = proto.Unmarshal(kafkaProducer.messages[0].Value, &msg)
	require.NoError(t, err)

	assert.Equal(t, subtreeHash.String(), msg.SubtreeHash)
	assert.Equal(t, baseURL, msg.PeerUrl)
	assert.Equal(t, "transaction_count_mismatch", msg.Reason)
}

// TestInvalidSubtreeReporting_PeerCannotProvideData tests that invalid subtree messages ARE sent
// when a peer cannot provide requested data
func TestInvalidSubtreeReporting_PeerCannotProvideData(t *testing.T) {
	// setup
	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()

	tSettings := test.CreateBaseTestSettings(t)
	subtreeHash := chainhash.HashH([]byte("test-subtree"))
	baseURL := testPeerURL

	// create server with mocked dependencies
	server := &Server{
		logger:                       ulogger.TestLogger{},
		settings:                     tSettings,
		subtreeStore:                 memory.New(),
		invalidSubtreeKafkaProducer:  &mockKafkaProducer{},
		invalidSubtreeDeDuplicateMap: expiringmap.New[string, struct{}](time.Minute * 1),
	}
	defer server.invalidSubtreeDeDuplicateMap.Stop()

	// test case 1: peer cannot provide transactions
	url := fmt.Sprintf("%s/subtree/%s/txs", baseURL, subtreeHash.String())
	httpmock.RegisterResponder("POST", url,
		httpmock.NewBytesResponder(http.StatusNotFound, []byte(errors.New(errors.ERR_NOT_FOUND, "not found").Error())))

	ctx := context.Background()
	missingTxHashes := []utxo.UnresolvedMetaData{
		{Hash: chainhash.HashH([]byte("tx1")), Idx: 0},
	}

	_, err := server.getMissingTransactionsBatch(ctx, subtreeHash, missingTxHashes, baseURL)

	// verify error is returned
	assert.Error(t, err)
	assert.True(t, errors.Is(err, errors.ErrExternal))

	// verify invalid subtree message was published
	kafkaProducer := server.invalidSubtreeKafkaProducer.(*mockKafkaProducer)
	require.Len(t, kafkaProducer.messages, 1)

	var msg kafkamessage.KafkaInvalidSubtreeTopicMessage
	err = proto.Unmarshal(kafkaProducer.messages[0].Value, &msg)
	require.NoError(t, err)

	assert.Equal(t, subtreeHash.String(), msg.SubtreeHash)
	assert.Equal(t, baseURL, msg.PeerUrl)
	assert.Equal(t, "peer_cannot_provide_transactions", msg.Reason)

	// clear out the invalid subtree de-duplicate map
	server.invalidSubtreeDeDuplicateMap.Clear()

	// test case 2: peer cannot provide subtree
	kafkaProducer.messages = nil // reset messages
	subtreeURL := fmt.Sprintf("%s/subtree/%s", baseURL, subtreeHash.String())
	httpmock.RegisterResponder("GET", subtreeURL,
		httpmock.NewBytesResponder(http.StatusNotFound, []byte(errors.New(errors.ERR_NOT_FOUND, "not found").Error())))

	stat := gocore.NewStat("test")
	_, err = server.getSubtreeTxHashes(ctx, stat, &subtreeHash, baseURL)

	// verify error is returned
	assert.Error(t, err)
	assert.True(t, errors.Is(err, errors.ErrNotFound))

	// verify invalid subtree message was published
	require.Len(t, kafkaProducer.messages, 1)

	err = proto.Unmarshal(kafkaProducer.messages[0].Value, &msg)
	require.NoError(t, err)

	assert.Equal(t, subtreeHash.String(), msg.SubtreeHash)
	assert.Equal(t, baseURL, msg.PeerUrl)
	assert.Equal(t, "peer_cannot_provide_subtree", msg.Reason)
}

// TestInvalidSubtreeReporting_NilKafkaProducer tests that the system handles nil kafka producer gracefully
func TestInvalidSubtreeReporting_NilKafkaProducer(t *testing.T) {
	// setup
	tSettings := test.CreateBaseTestSettings(t)
	subtreeHash := chainhash.HashH([]byte("test-subtree"))
	baseURL := testPeerURL

	// create server with nil kafka producer
	server := &Server{
		logger:                       ulogger.TestLogger{},
		settings:                     tSettings,
		subtreeStore:                 memory.New(),
		invalidSubtreeKafkaProducer:  nil, // nil producer
		invalidSubtreeDeDuplicateMap: expiringmap.New[string, struct{}](time.Minute * 1),
	}
	defer server.invalidSubtreeDeDuplicateMap.Stop()

	// call publishInvalidSubtree directly
	// should not panic even with nil producer
	assert.NotPanics(t, func() {
		server.publishInvalidSubtree(context.Background(), subtreeHash.String(), baseURL, "test_reason")
	})
}

// TestInvalidSubtreeReporting_HTTPErrorResponse tests invalid subtree reporting for HTTP errors
func TestInvalidSubtreeReporting_HTTPErrorResponse(t *testing.T) {
	// setup
	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()

	tSettings := test.CreateBaseTestSettings(t)
	subtreeHash := chainhash.HashH([]byte("test-subtree"))
	baseURL := testPeerURL

	// create server with mocked dependencies
	server := &Server{
		logger:                       ulogger.TestLogger{},
		settings:                     tSettings,
		subtreeStore:                 memory.New(),
		invalidSubtreeKafkaProducer:  &mockKafkaProducer{},
		invalidSubtreeDeDuplicateMap: expiringmap.New[string, struct{}](time.Minute * 1),
	}
	defer server.invalidSubtreeDeDuplicateMap.Stop()

	// mock HTTP 500 error response
	url := fmt.Sprintf("%s/subtree/%s/txs", baseURL, subtreeHash.String())
	httpmock.RegisterResponder("POST", url,
		httpmock.NewStringResponder(500, "Internal Server Error"))

	ctx := context.Background()
	missingTxHashes := []utxo.UnresolvedMetaData{
		{Hash: chainhash.HashH([]byte("tx1")), Idx: 0},
	}

	_, err := server.getMissingTransactionsBatch(ctx, subtreeHash, missingTxHashes, baseURL)

	// verify error is returned
	assert.Error(t, err)

	// verify invalid subtree message was published
	kafkaProducer := server.invalidSubtreeKafkaProducer.(*mockKafkaProducer)
	require.Len(t, kafkaProducer.messages, 1)

	var msg kafkamessage.KafkaInvalidSubtreeTopicMessage
	err = proto.Unmarshal(kafkaProducer.messages[0].Value, &msg)
	require.NoError(t, err)

	assert.Equal(t, subtreeHash.String(), msg.SubtreeHash)
	assert.Equal(t, baseURL, msg.PeerUrl)
	assert.Equal(t, "peer_cannot_provide_transactions", msg.Reason)
}

// TestInvalidSubtreeReporting_ReadTxFromReaderPanic tests handling of panics in readTxFromReader
func TestInvalidSubtreeReporting_ReadTxFromReaderPanic(t *testing.T) {
	// setup
	server := &Server{
		logger: ulogger.TestLogger{},
	}

	// create a reader that will cause a panic
	reader := io.NopCloser(strings.NewReader("invalid data that causes panic"))

	// call readTxFromReader which should recover from panic
	tx, err := server.readTxFromReader(reader)

	// verify error is returned and no panic occurs
	assert.Error(t, err)
	assert.Nil(t, tx)
}

// TestPublishInvalidSubtree_DirectCall tests the publishInvalidSubtree method directly
func TestPublishInvalidSubtree_DirectCall(t *testing.T) {
	// setup
	tSettings := test.CreateBaseTestSettings(t)
	subtreeHash := "abc123"
	peerURL := "http://test-peer.com"
	reason := "test_invalid_reason"

	kafkaProducer := &mockKafkaProducer{}

	// create server with mocked dependencies
	server := &Server{
		logger:                       ulogger.TestLogger{},
		settings:                     tSettings,
		subtreeStore:                 memory.New(),
		invalidSubtreeKafkaProducer:  kafkaProducer,
		invalidSubtreeDeDuplicateMap: expiringmap.New[string, struct{}](time.Minute * 1),
	}
	defer server.invalidSubtreeDeDuplicateMap.Stop()

	// call publishInvalidSubtree directly
	server.publishInvalidSubtree(context.Background(), subtreeHash, peerURL, reason)

	// verify invalid subtree message was published
	require.Len(t, kafkaProducer.messages, 1)

	// decode and verify the message
	var msg kafkamessage.KafkaInvalidSubtreeTopicMessage
	err := proto.Unmarshal(kafkaProducer.messages[0].Value, &msg)
	require.NoError(t, err)

	assert.Equal(t, subtreeHash, msg.SubtreeHash)
	assert.Equal(t, peerURL, msg.PeerUrl)
	assert.Equal(t, reason, msg.Reason)

	// verify the Kafka message key is the subtree hash
	assert.Equal(t, []byte(subtreeHash), kafkaProducer.messages[0].Key)
}
