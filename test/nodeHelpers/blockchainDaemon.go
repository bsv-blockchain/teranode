package nodehelpers

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/settings"
	blockchain_store "github.com/bitcoin-sv/teranode/stores/blockchain"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/kafka"
	"github.com/bitcoin-sv/teranode/util/servicemanager"
)

const memoryScheme = "memory"

// BlockchainDaemon represents a minimal node that can run specific services
type BlockchainDaemon struct {
	ctx              context.Context
	serviceManager   *servicemanager.ServiceManager
	Logger           ulogger.Logger
	Settings         *settings.Settings
	Store            blockchain_store.Store
	BlockchainClient blockchain.ClientI
	cancel           context.CancelFunc
}

// NewBlockchainDaemon creates a new blockchain daemon instance
func NewBlockchainDaemon(t *testing.T) (*BlockchainDaemon, error) {
	ctx, cancel := context.WithCancel(context.Background())

	logger := ulogger.NewErrorTestLogger(t, cancel)
	tSettings := settings.NewSettings("dev.system.test")
	tSettings.LocalTestStartFromState = "RUNNING"
	tSettings.BlockAssembly.ResetWaitCount = 0
	tSettings.BlockAssembly.ResetWaitDuration = 0

	// Configure settings for in-memory Kafka
	tSettings.Kafka.BlocksConfig.Scheme = memoryScheme
	tSettings.Kafka.BlocksFinalConfig.Scheme = memoryScheme
	tSettings.Kafka.LegacyInvConfig.Scheme = memoryScheme
	tSettings.Kafka.RejectedTxConfig.Scheme = memoryScheme
	tSettings.Kafka.SubtreesConfig.Scheme = memoryScheme
	tSettings.Kafka.TxMetaConfig.Scheme = memoryScheme

	// Configure store URL
	storeURL, _ := url.Parse("sqlite:///blockchainDB")
	tSettings.BlockChain.StoreURL = storeURL
	tSettings.Block.StoreCacheEnabled = false

	// Initialize store
	store, err := blockchain_store.NewStore(logger, storeURL, tSettings)
	if err != nil {
		logger.Errorf("Failed to initialize store: %v", err)
		cancel()

		return nil, err
	}

	return &BlockchainDaemon{
		ctx:      ctx,
		cancel:   cancel,
		Logger:   logger,
		Settings: tSettings,
		Store:    store,
	}, nil
}

// StartBlockchainService starts only the blockchain service
func (m *BlockchainDaemon) StartBlockchainService() error {
	m.serviceManager = servicemanager.NewServiceManager(m.ctx, m.Logger)

	// Get Kafka producer
	blocksFinalKafkaAsyncProducer, err := kafka.NewKafkaAsyncProducerFromURL(m.ctx, ulogger.New("kpbf"), m.Settings.Kafka.BlocksFinalConfig)
	if err != nil {
		return err
	}

	// Initialize blockchain service
	blockchainService, err := blockchain.New(m.ctx, m.Logger, m.Settings, m.Store, blocksFinalKafkaAsyncProducer)
	if err != nil {
		return err
	}

	// Add blockchain service to service manager
	if err := m.serviceManager.AddService("blockchain", blockchainService); err != nil {
		return err
	}

	// Create blockchain client
	m.BlockchainClient, err = blockchain.NewClient(m.ctx, m.Logger, m.Settings, "localhost:8087")
	if err != nil {
		return err
	}

	// Start all services and wait
	go func() {
		if err := m.serviceManager.Wait(); err != nil {
			m.Logger.Errorf("Service manager error: %v", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	return nil
}

// Stop stops all services
func (m *BlockchainDaemon) Stop() {
	if m.cancel != nil {
		m.cancel()
	}
}
