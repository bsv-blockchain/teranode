package nodehelpers

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/bsv-blockchain/teranode/util/servicemanager"
)

const memoryScheme = "memory"

// fsmReadyTimeout is a generous backstop for the blockchain FSM to become ready
// after Run() is issued. The wait is event-driven — WaitUntilFSMTransitionFromIdleState
// is a server-blocking RPC that returns the instant the FSM leaves IDLE and the
// subscription manager is ready — so under no load it returns near-instantly; the
// deadline only guards a genuinely stuck startup, replacing the old fixed 10s poll
// that lost races under CI load.
const fsmReadyTimeout = 60 * time.Second

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

// getFreePort finds a free port to use for testing
func getFreePort(t *testing.T) int {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	return port
}

// NewBlockchainDaemon creates a new blockchain daemon instance
func NewBlockchainDaemon(t *testing.T) (*BlockchainDaemon, error) {
	ctx, cancel := context.WithCancel(context.Background())

	logger := ulogger.NewErrorTestLogger(t, cancel)
	tSettings := settings.NewSettings("dev.system.test")
	tSettings.LocalTestStartFromState = "RUNNING"

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

	// Use a dynamic port for blockchain gRPC to avoid conflicts with running nodes
	blockchainPort := getFreePort(t)
	tSettings.BlockChain.GRPCListenAddress = fmt.Sprintf("localhost:%d", blockchainPort)
	tSettings.BlockChain.GRPCAddress = fmt.Sprintf("localhost:%d", blockchainPort)

	// Use a dynamic port for blockchain HTTP to avoid conflicts with running nodes
	blockchainHTTPPort := getFreePort(t)
	tSettings.BlockChain.HTTPListenAddress = fmt.Sprintf("localhost:%d", blockchainHTTPPort)

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
	blocksFinalKafkaAsyncProducer, err := kafka.NewKafkaAsyncProducerFromURL(m.ctx, ulogger.New("kpbf"), m.Settings.Kafka.BlocksFinalConfig, &m.Settings.Kafka)
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

	// Start all services in background
	go func() {
		if err := m.serviceManager.Wait(); err != nil {
			m.Logger.Errorf("Service manager error: %v", err)
		}
	}()

	// Wait for all services to be ready. StartGRPCServer binds the listening
	// socket before the service signals readiness (GetListener -> register/close
	// readyCh -> Serve), so creating the client only after this point removes the
	// connection-refused race the client retry budget otherwise had to bridge
	// under load.
	m.serviceManager.WaitForServiceToBeReady()

	// Create blockchain client using the configured address from settings
	m.BlockchainClient, err = blockchain.NewClient(m.ctx, m.Logger, m.Settings, m.Settings.BlockChain.GRPCListenAddress)
	if err != nil {
		return err
	}

	// Check initial FSM state
	initialState, err := m.BlockchainClient.GetFSMCurrentState(m.ctx)
	if err != nil {
		return errors.NewProcessingError("failed to get initial FSM state", err)
	}
	m.Logger.Infof("Initial FSM state: %v", initialState)

	// Run the blockchain FSM to transition from IDLE to RUNNING
	if err := m.BlockchainClient.Run(m.ctx, "test"); err != nil {
		return errors.NewProcessingError("failed to run blockchain FSM", err)
	}

	// Wait for the FSM to leave IDLE and the subscription manager to become
	// ready. Event-driven (server-blocking RPC) and bounded by the context
	// deadline rather than a fixed poll budget. Crucially it waits on
	// subscriptionManagerReady — the condition that made GetFSMCurrentState
	// report IDLE and the old fixed 10s poll lose under CI load.
	waitCtx, cancelWait := context.WithTimeout(m.ctx, fsmReadyTimeout)
	defer cancelWait()

	if err := m.BlockchainClient.WaitUntilFSMTransitionFromIdleState(waitCtx); err != nil {
		finalState, _ := m.BlockchainClient.GetFSMCurrentState(m.ctx)
		return errors.NewProcessingError("timeout waiting for FSM to transition out of IDLE state (current state: %v)", finalState, err)
	}

	// Run() fires the RUN event, and every RUN transition has Dst=RUNNING (fsm.go),
	// so the post-IDLE state is always RUNNING. Confirm it explicitly so a future
	// extra transition can't slip through silently.
	state, err := m.BlockchainClient.GetFSMCurrentState(m.ctx)
	if err != nil {
		return errors.NewProcessingError("failed to get FSM state after transition", err)
	}

	if state == nil || *state != blockchain.FSMStateRUNNING {
		return errors.NewProcessingError("FSM left IDLE but did not reach RUNNING (current state: %v)", state)
	}

	m.Logger.Infof("FSM successfully transitioned to RUNNING state")

	return nil
}

// Stop stops all services
func (m *BlockchainDaemon) Stop() {
	if m.cancel != nil {
		m.cancel()
	}
}
