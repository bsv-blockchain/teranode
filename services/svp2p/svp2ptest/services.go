package svp2ptest

import (
	"context"
	mrand "math/rand/v2"
	"net"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/stretchr/testify/require"
)

// FreePort reserves and releases a loopback port so a service can be given a
// distinct listen address.
func FreePort(t *testing.T) string {
	t.Helper()

	// Not ":0": an OS-ephemeral port the kernel just released is exactly the
	// one it hands to the next outgoing dial, and the services here dial each
	// other constantly — on CI the legacy leg lost its listen port that way
	// ("no valid listen address"). A random port outside the ephemeral range,
	// proven free by a bind, leaves only test-vs-test collisions.
	for attempt := 0; attempt < 50; attempt++ {
		port := 20000 + mrand.IntN(20000)
		addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))

		ln, err := net.Listen("tcp", addr)
		if err != nil {
			continue
		}

		require.NoError(t, ln.Close())

		return addr
	}

	t.Fatal("no free port found in 20000-39999 after 50 attempts")

	return ""
}

func StartBlockAssembly(ctx context.Context, t *testing.T, logger ulogger.Logger, tSettings *settings.Settings,
	txStore, subtreeStore blob.Store, utxoStore utxo.Store, blockchainClient blockchain.ClientI) *blockassembly.Client {
	t.Helper()

	ba := blockassembly.New(logger, tSettings, txStore, utxoStore, subtreeStore, blockchainClient)
	require.NoError(t, ba.Init(ctx))

	readyCh := make(chan struct{})

	go func() { _ = ba.Start(ctx, readyCh) }()

	select {
	case <-readyCh:
	case <-time.After(30 * time.Second):
		t.Fatal("block assembly did not become ready")
	}

	client, err := blockassembly.NewClient(ctx, logger, tSettings)
	require.NoError(t, err)

	return client
}

// inMemoryConsumer builds a real KafkaConsumerGroup over the in-memory broker,
// which is what the repo's testing rules ask for instead of a Kafka mock.
func inMemoryConsumer(t *testing.T, logger ulogger.Logger, topic, group string) *kafka.KafkaConsumerGroup {
	t.Helper()

	consumer, err := kafka.NewKafkaConsumerGroup(kafka.KafkaConsumerConfig{
		Logger:          logger,
		URL:             &url.URL{Scheme: "memory", Host: "svp2p-sync-test", Path: "/" + topic},
		Topic:           topic,
		Partitions:      1,
		ConsumerGroupID: group,
	})
	require.NoError(t, err)

	return consumer
}

// StartSubtreeValidation runs the real subtree validation service in-process
// and returns the real gRPC client for it, which is what Deps.SubtreeValidation
// carries in the daemon.
func StartSubtreeValidation(ctx context.Context, t *testing.T, name string, logger ulogger.Logger,
	tSettings *settings.Settings, subtreeStore, txStore blob.Store, utxoStore utxo.Store,
	validatorClient validator.Interface, blockchainClient blockchain.ClientI) subtreevalidation.Interface {
	t.Helper()

	server, err := subtreevalidation.New(ctx, logger, tSettings, subtreeStore, txStore, utxoStore, validatorClient,
		blockchainClient,
		inMemoryConsumer(t, logger, "subtree-"+name, "svp2p-sync-subtree-"+name),
		inMemoryConsumer(t, logger, "txmeta-"+name, "svp2p-sync-txmeta-"+name),
		nil, nil)
	require.NoError(t, err)
	require.NoError(t, server.Init(ctx))

	readyCh := make(chan struct{})

	go func() { _ = server.Start(ctx, readyCh) }()

	waitReady(t, readyCh, "subtree validation")

	t.Cleanup(func() {
		// Bounded: a Stop that waits on a start that never completed must not
		// consume the package's whole test budget (seen on CI: 8 minutes).
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		_ = server.Stop(ctx)
	})

	client, err := subtreevalidation.NewClient(ctx, logger, tSettings, "svp2p-sync-test")
	require.NoError(t, err)

	return client
}

// StartBlockValidation runs the real block validation service in-process and
// returns the real gRPC client for it. It must start after subtree validation:
// blockvalidation Server.Init dials that service.
func StartBlockValidation(ctx context.Context, t *testing.T, name string, logger ulogger.Logger,
	tSettings *settings.Settings, subtreeStore, txStore blob.Store, utxoStore utxo.Store,
	validatorClient validator.Interface, blockchainClient blockchain.ClientI,
	blockAssemblyClient *blockassembly.Client) blockvalidation.Interface {
	t.Helper()

	server := blockvalidation.New(logger, tSettings, subtreeStore, txStore, utxoStore, validatorClient,
		blockchainClient, inMemoryConsumer(t, logger, "blocks-"+name, "svp2p-sync-blocks-"+name),
		blockAssemblyClient, nil)
	require.NoError(t, server.Init(ctx))

	readyCh := make(chan struct{})

	go func() { _ = server.Start(ctx, readyCh) }()

	waitReady(t, readyCh, "block validation")

	t.Cleanup(func() {
		// Bounded: a Stop that waits on a start that never completed must not
		// consume the package's whole test budget (seen on CI: 8 minutes).
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		_ = server.Stop(ctx)
	})

	client, err := blockvalidation.NewClient(ctx, logger, tSettings, "svp2p-sync-test")
	require.NoError(t, err)

	return client
}

func waitReady(t *testing.T, readyCh <-chan struct{}, what string) {
	t.Helper()

	select {
	case <-readyCh:
	case <-time.After(30 * time.Second):
		t.Fatalf("%s did not become ready", what)
	}
}

// ---------------------------------------------------------------------------
// Tests
