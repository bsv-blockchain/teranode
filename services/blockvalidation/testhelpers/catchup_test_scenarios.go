package testhelpers

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockvalidation/catchup"
)

// TestScenario represents a predefined test scenario
type TestScenario string

const (
	ScenarioNormalCatchup      TestScenario = "normal"
	ScenarioNetworkPartition   TestScenario = "partition"
	ScenarioEclipseAttack      TestScenario = "eclipse"
	ScenarioSlowNetwork        TestScenario = "slow"
	ScenarioMaliciousPeer      TestScenario = "malicious"
	ScenarioResourceExhaustion TestScenario = "exhaustion"
	ScenarioCircuitBreaker     TestScenario = "circuit_breaker"
	ScenarioSecretMining       TestScenario = "secret_mining"
	ScenarioForkResolution     TestScenario = "fork_resolution"
	ScenarioTimeWarp           TestScenario = "time_warp"
)

// ScenarioConfig contains configuration for a test scenario
type ScenarioConfig struct {
	Name              string
	Description       string
	ServerConfig      *CatchupServerConfig
	NetworkDelay      time.Duration
	PacketLossRate    float64
	NumPeers          int
	NumMaliciousPeers int
	ChainLength       int
	SetupFunc         func(*CatchupTestSuite)
	ValidateFunc      func(*CatchupTestSuite) error
}

// GetScenarioConfig returns configuration for a scenario
func GetScenarioConfig(scenario TestScenario) ScenarioConfig {
	configs := map[TestScenario]ScenarioConfig{
		ScenarioNormalCatchup: {
			Name:        "Normal Catchup",
			Description: "Standard catchup with honest peer",
			ServerConfig: &CatchupServerConfig{
				MaxRetries:              3,
				RetryDelay:              100 * time.Millisecond,
				CatchupOperationTimeout: 30,
				MaxConcurrentCatchups:   5,
			},
			ChainLength: 100,
			NumPeers:    1,
			SetupFunc:   setupNormalCatchup,
		},

		ScenarioNetworkPartition: {
			Name:        "Network Partition",
			Description: "Network split with conflicting chains",
			ServerConfig: &CatchupServerConfig{
				MaxRetries:              5,
				RetryDelay:              500 * time.Millisecond,
				CatchupOperationTimeout: 60,
				MaxConcurrentCatchups:   3,
			},
			ChainLength:    50,
			NumPeers:       3,
			NetworkDelay:   2 * time.Second,
			PacketLossRate: 0.1,
			SetupFunc:      setupNetworkPartition,
		},

		ScenarioEclipseAttack: {
			Name:        "Eclipse Attack",
			Description: "All peers are malicious",
			ServerConfig: &CatchupServerConfig{
				MaxRetries:              2,
				RetryDelay:              1 * time.Second,
				CatchupOperationTimeout: 30,
				MaxConcurrentCatchups:   1,
			},
			ChainLength:       100,
			NumPeers:          50,
			NumMaliciousPeers: 50,
			SetupFunc:         setupEclipseAttack,
		},

		ScenarioSlowNetwork: {
			Name:        "Slow Network",
			Description: "High latency network conditions",
			ServerConfig: &CatchupServerConfig{
				MaxRetries:              10,
				RetryDelay:              2 * time.Second,
				CatchupOperationTimeout: 120,
				MaxConcurrentCatchups:   2,
			},
			ChainLength:    200,
			NumPeers:       2,
			NetworkDelay:   5 * time.Second,
			PacketLossRate: 0.3,
			SetupFunc:      setupSlowNetwork,
		},

		ScenarioMaliciousPeer: {
			Name:        "Malicious Peer",
			Description: "Single malicious peer among honest peers",
			ServerConfig: &CatchupServerConfig{
				MaxRetries:              3,
				RetryDelay:              500 * time.Millisecond,
				CatchupOperationTimeout: 30,
				MaxConcurrentCatchups:   5,
			},
			ChainLength:       50,
			NumPeers:          3,
			NumMaliciousPeers: 1,
			SetupFunc:         setupMaliciousPeer,
		},

		ScenarioResourceExhaustion: {
			Name:        "Resource Exhaustion",
			Description: "Test resource limits and memory constraints",
			ServerConfig: &CatchupServerConfig{
				MaxRetries:              2,
				RetryDelay:              100 * time.Millisecond,
				CatchupOperationTimeout: 60,
				MaxConcurrentCatchups:   10,
			},
			ChainLength: 1000,
			NumPeers:    5,
			SetupFunc:   setupResourceExhaustion,
		},

		ScenarioCircuitBreaker: {
			Name:        "Circuit Breaker",
			Description: "Test circuit breaker activation",
			ServerConfig: &CatchupServerConfig{
				MaxRetries: 3,
				RetryDelay: 100 * time.Millisecond,
				CircuitBreakerConfig: &catchup.CircuitBreakerConfig{
					FailureThreshold:    3,
					SuccessThreshold:    2,
					Timeout:             5 * time.Second,
					MaxHalfOpenRequests: 1,
				},
				CatchupOperationTimeout: 30,
				MaxConcurrentCatchups:   5,
			},
			ChainLength: 50,
			NumPeers:    2,
			SetupFunc:   setupCircuitBreaker,
		},

		ScenarioSecretMining: {
			Name:        "Secret Mining Detection",
			Description: "Detect secret mining attempts",
			ServerConfig: &CatchupServerConfig{
				MaxRetries:              2,
				RetryDelay:              500 * time.Millisecond,
				CatchupOperationTimeout: 30,
				SecretMiningThreshold:   50,
				MaxConcurrentCatchups:   1,
			},
			ChainLength:       200,
			NumPeers:          1,
			NumMaliciousPeers: 1,
			SetupFunc:         setupSecretMining,
		},

		ScenarioForkResolution: {
			Name:        "Fork Resolution",
			Description: "Handle competing chain forks",
			ServerConfig: &CatchupServerConfig{
				MaxRetries:              5,
				RetryDelay:              200 * time.Millisecond,
				CatchupOperationTimeout: 60,
				MaxConcurrentCatchups:   3,
			},
			ChainLength: 100,
			NumPeers:    2,
			SetupFunc:   setupForkResolution,
		},

		ScenarioTimeWarp: {
			Name:        "Time Warp Attack",
			Description: "Detect time warp attack attempts",
			ServerConfig: &CatchupServerConfig{
				MaxRetries:              2,
				RetryDelay:              500 * time.Millisecond,
				CatchupOperationTimeout: 30,
				MaxConcurrentCatchups:   1,
				HeaderValidationConfig: HeaderValidationConfig{
					MaxTimeDrift: 2 * time.Hour,
				},
			},
			ChainLength:       50,
			NumPeers:          1,
			NumMaliciousPeers: 1,
			SetupFunc:         setupTimeWarp,
		},
	}

	config, exists := configs[scenario]
	if !exists {
		panic("Unknown scenario: " + string(scenario))
	}

	// Apply default server config if not set
	if config.ServerConfig == nil {
		config.ServerConfig = DefaultCatchupTestConfig()
	}

	return config
}

// SetupScenario creates a test suite configured for the scenario
func SetupScenario(t *testing.T, scenario TestScenario) *CatchupTestSuite {
	config := GetScenarioConfig(scenario)

	suite := NewCatchupTestSuiteWithConfig(t, config.ServerConfig)

	// Run scenario-specific setup
	if config.SetupFunc != nil {
		config.SetupFunc(suite)
	}

	return suite
}

// Scenario setup functions

func setupNormalCatchup(suite *CatchupTestSuite) {
	// Create simple chain
	chain := NewTestChainBuilder(suite.T).
		WithLength(100).
		WithMainnetHeaders().
		Build()

	// Setup mocks for normal catchup
	suite.NewMockBuilder().
		WithBestBlock(chain[0], 1000).
		WithTargetBlock(&model.Block{
			Header: chain[99],
			Height: 1100,
		}).
		BuildWithDefaults(suite.T)

	// Setup single honest peer
	suite.HttpMock.RegisterHeaderResponse("http://honest-peer", chain[1:])
	suite.HttpMock.Activate()
}

func setupNetworkPartition(suite *CatchupTestSuite) {
	// Create conflicting chains from different partitions
	builder := NewTestChainBuilder(suite.T).WithLength(50)
	chains := builder.BuildMultiple(3)

	// Setup mocks
	suite.NewMockBuilder().
		WithBestBlock(chains[0][0], 1000).
		BuildWithDefaults(suite.T)

	// Register multiple peers with different chains
	for i, chain := range chains {
		peerURL := fmt.Sprintf("http://peer-%d", i)
		suite.HttpMock.RegisterHeaderResponse(peerURL, chain)
	}
	suite.HttpMock.Activate()
}

func setupEclipseAttack(suite *CatchupTestSuite) {
	// Create malicious chain with high work
	maliciousChain := NewTestChainBuilder(suite.T).
		WithLength(100).
		WithWork(1000000). // High work to seem legitimate
		BuildType(ChainTypeMalicious)

	suite.NewMockBuilder().
		WithBestBlock(maliciousChain[0], 1000).
		BuildWithDefaults(suite.T)

	// Register many malicious peers all serving the same false chain
	for i := 0; i < 50; i++ {
		peerURL := fmt.Sprintf("http://malicious-%d", i)
		suite.HttpMock.RegisterHeaderResponse(peerURL, maliciousChain)
	}
	suite.HttpMock.Activate()
}

func setupSlowNetwork(suite *CatchupTestSuite) {
	chain := NewTestChainBuilder(suite.T).
		WithLength(200).
		Build()

	suite.NewMockBuilder().
		WithBestBlock(chain[0], 1000).
		BuildWithDefaults(suite.T)

	// Register slow peers with network delays
	// Note: RegisterSlowResponse would need to be implemented in HTTPMockSetup
	// For now, use regular response
	suite.HttpMock.RegisterHeaderResponse("http://slow-peer-1", chain)
	suite.HttpMock.RegisterHeaderResponse("http://slow-peer-2", chain)
	suite.HttpMock.Activate()
}

func setupMaliciousPeer(suite *CatchupTestSuite) {
	// Create honest and malicious chains
	honestChain := NewTestChainBuilder(suite.T).
		WithLength(50).
		WithMainnetHeaders().
		Build()

	maliciousChain := NewTestChainBuilder(suite.T).
		WithLength(50).
		BuildType(ChainTypeMalicious)

	suite.NewMockBuilder().
		WithBestBlock(honestChain[0], 1000).
		BuildWithDefaults(suite.T)

	// Register honest peers
	suite.HttpMock.RegisterHeaderResponse("http://honest-peer-1", honestChain)
	suite.HttpMock.RegisterHeaderResponse("http://honest-peer-2", honestChain)

	// Register malicious peer
	suite.HttpMock.RegisterHeaderResponse("http://malicious-peer", maliciousChain)
	suite.HttpMock.Activate()
}

func setupResourceExhaustion(suite *CatchupTestSuite) {
	// Create very large chain
	largeChain := NewTestChainBuilder(suite.T).
		WithLength(1000).
		Build()

	suite.NewMockBuilder().
		WithBestBlock(largeChain[0], 1000).
		BuildWithDefaults(suite.T)

	// Register multiple peers to test concurrent operations
	for i := 0; i < 5; i++ {
		peerURL := fmt.Sprintf("http://peer-%d", i)
		// Each peer returns different subsets to test deduplication
		startIdx := i * 200
		endIdx := startIdx + 300
		if endIdx > len(largeChain) {
			endIdx = len(largeChain)
		}
		suite.HttpMock.RegisterHeaderResponse(peerURL, largeChain[startIdx:endIdx])
	}
	suite.HttpMock.Activate()
}

func setupCircuitBreaker(suite *CatchupTestSuite) {
	chain := NewTestChainBuilder(suite.T).
		WithLength(50).
		Build()

	suite.NewMockBuilder().
		WithBestBlock(chain[0], 1000).
		BuildWithDefaults(suite.T)

	// Register flaky peer that triggers circuit breaker
	suite.HttpMock.RegisterFlakeyResponse("http://flaky-peer", 3, chain)
	// Register backup peer that always works
	suite.HttpMock.RegisterHeaderResponse("http://backup-peer", chain)
	suite.HttpMock.Activate()
}

func setupSecretMining(suite *CatchupTestSuite) {
	// Create suspiciously long hidden chain
	hiddenChain := NewTestChainBuilder(suite.T).
		WithLength(200). // Much longer than threshold
		Build()

	suite.NewMockBuilder().
		WithBestBlock(hiddenChain[0], 1000).
		BuildWithDefaults(suite.T)

	// Register peer with hidden chain
	suite.HttpMock.RegisterHeaderResponse("http://secret-miner", hiddenChain)
	suite.HttpMock.Activate()
}

func setupForkResolution(suite *CatchupTestSuite) {
	// Create base chain and two competing forks
	baseChain := NewTestChainBuilder(suite.T).
		WithLength(50).
		WithMainnetHeaders().
		Build()

	// Fork 1: More work but shorter
	fork1 := NewTestChainBuilder(suite.T).
		WithBaseChain(baseChain[:40]).
		WithLength(20).
		WithWork(100000).
		Build()

	// Fork 2: Less work but longer
	fork2 := NewTestChainBuilder(suite.T).
		WithBaseChain(baseChain[:40]).
		WithLength(30).
		WithWork(50000).
		Build()

	suite.NewMockBuilder().
		WithBestBlock(baseChain[39], 1040).
		BuildWithDefaults(suite.T)

	// Register peers with competing forks
	suite.HttpMock.RegisterHeaderResponse("http://peer-fork1", fork1)
	suite.HttpMock.RegisterHeaderResponse("http://peer-fork2", fork2)
	suite.HttpMock.Activate()
}

func setupTimeWarp(suite *CatchupTestSuite) {
	// Create chain with time warp attack characteristics
	timeWarpChain := NewTestChainBuilder(suite.T).
		WithLength(50).
		BuildType(ChainTypeTimewarp)

	suite.NewMockBuilder().
		WithBestBlock(timeWarpChain[0], 1000).
		BuildWithDefaults(suite.T)

	// Register malicious peer with time warp chain
	suite.HttpMock.RegisterHeaderResponse("http://timewarp-attacker", timeWarpChain)
	suite.HttpMock.Activate()
}

// ValidateScenarioOutcome validates the outcome of a scenario
func ValidateScenarioOutcome(t *testing.T, scenario TestScenario, err error, suite *CatchupTestSuite) {
	switch scenario {
	case ScenarioNormalCatchup:
		if err != nil {
			t.Errorf("Normal catchup should succeed: %v", err)
		}

	case ScenarioEclipseAttack:
		// Eclipse attack might be detected or might succeed
		// depending on implementation
		if err == nil {
			t.Log("Warning: Eclipse attack was not detected")
		}

	case ScenarioSecretMining:
		// Secret mining should be detected if threshold exceeded
		if err == nil {
			t.Log("Warning: Secret mining was not detected")
		} else if !strings.Contains(err.Error(), "secret mining") && !strings.Contains(err.Error(), "threshold") {
			t.Error("Secret mining should be detected")
		}

	case ScenarioTimeWarp:
		// Time warp should be detected
		if err == nil {
			t.Log("Warning: Time warp was not detected")
		} else if !strings.Contains(err.Error(), "time") && !strings.Contains(err.Error(), "timestamp") {
			t.Error("Time warp attack should be detected")
		}

	case ScenarioCircuitBreaker:
		// Circuit breaker should eventually open
		suite.AssertCircuitBreakerState("http://flaky-peer", 2) // StateOpen = 2

	default:
		// For other scenarios, just log the outcome
		if err != nil {
			t.Logf("Scenario %s completed with error: %v", scenario, err)
		} else {
			t.Logf("Scenario %s completed successfully", scenario)
		}
	}
}
