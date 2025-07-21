package kafka

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	TestContainerRedpandaImage         = "redpandadata/redpanda"
	TestContainerRedpandaVersion       = "latest"
	TestContainerDefaultKafkaPort      = 9092
	TestContainerDefaultKafkaProxyPort = 9093
	TestContainerDefaultKafkaAdminPort = 8081
)

type ContainerConfig struct {
	KafkaPort      int // if 0, will use DefaultKafkaPort
	KafkaProxyPort int // if 0, will use DefaultKafkaProxyPort
	KafkaAdminPort int // if 0, will use DefaultKafkaAdminPort
}

type GenericTestContainerWrapper struct {
	Container      testcontainers.Container
	KafkaPort      int
	KafkaProxyPort int
}

func GetTestContainerFreePort() (int, error) {
	var port int

	// Try up to 3 times to get a free port
	for attempts := 0; attempts < 3; attempts++ {
		addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
		if err != nil {
			return 0, err
		}

		l, err := net.ListenTCP("tcp", addr)
		if err != nil {
			// Wait briefly before retry
			time.Sleep(100 * time.Millisecond)
			continue
		}

		port = l.Addr().(*net.TCPAddr).Port

		// Explicitly close the listener
		err = l.Close()
		if err != nil {
			return 0, err
		}

		// Small delay to ensure port is released
		time.Sleep(100 * time.Millisecond)

		// Verify the port is actually free
		testListener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
		if err != nil {
			continue
		}

		testListener.Close()

		return port, nil
	}

	return 0, errors.NewConfigurationError("failed to find a free port after 3 attempts")
}

// RunContainer starts a Kafka container. If config is nil, uses default configuration. NO TLS.
func RunTestContainer(ctx context.Context) (*GenericTestContainerWrapper, error) {
	randomName := fmt.Sprintf("kafka-test-%s", uuid.New().String()[:8])

	kafkaPort, err := GetTestContainerFreePort()
	if err != nil {
		return nil, err
	}

	kafkaProxyPort, err := GetTestContainerFreePort()
	if err != nil {
		return nil, err
	}

	kafkaAdminPort, err := GetTestContainerFreePort()
	if err != nil {
		return nil, err
	}

	// Check if ports are available
	ports := []int{kafkaPort, kafkaProxyPort, kafkaAdminPort}
	for _, port := range ports {
		listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
		if err != nil {
			return nil, errors.NewConfigurationError("port %d is not available: %v", port, err)
		}

		listener.Close()
	}

	req := testcontainers.ContainerRequest{
		Name:  randomName,
		Image: fmt.Sprintf("%s:%s", TestContainerRedpandaImage, TestContainerRedpandaVersion),
		ExposedPorts: []string{
			fmt.Sprintf("%d:%d/tcp", kafkaPort, kafkaPort),
			fmt.Sprintf("%d:%d/tcp", kafkaProxyPort, kafkaProxyPort),
		},
		Cmd: []string{
			"redpanda", "start",
			"--smp", "1",
			"--overprovisioned",
			"--node-id", "0",
			"--kafka-addr", fmt.Sprintf("PLAINTEXT://0.0.0.0:%d", kafkaPort),
			"--advertise-kafka-addr", fmt.Sprintf("PLAINTEXT://localhost:%d", kafkaPort),
			"--pandaproxy-addr", fmt.Sprintf("0.0.0.0:%d", kafkaProxyPort),
			"--advertise-pandaproxy-addr", fmt.Sprintf("localhost:%d", kafkaProxyPort),
		},
		WaitingFor: wait.ForLog("Successfully started Redpanda!"),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, err
	}

	return &GenericTestContainerWrapper{
		Container:      container,
		KafkaPort:      kafkaPort,
		KafkaProxyPort: kafkaProxyPort,
	}, nil
}

// RunTestContainerTLS starts a Kafka container with TLS enabled
func RunTestContainerTLS(ctx context.Context) (*GenericTestContainerWrapper, error) {
	randomName := fmt.Sprintf("kafka-tls-test-%s", uuid.New().String()[:8])

	kafkaPort, err := GetTestContainerFreePort()
	if err != nil {
		return nil, err
	}

	kafkaProxyPort, err := GetTestContainerFreePort()
	if err != nil {
		return nil, err
	}

	kafkaAdminPort, err := GetTestContainerFreePort()
	if err != nil {
		return nil, err
	}

	// Check if ports are available
	ports := []int{kafkaPort, kafkaProxyPort, kafkaAdminPort}
	for _, port := range ports {
		listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
		if err != nil {
			return nil, errors.NewConfigurationError(fmt.Sprintf("port %d is not available: %v", port, err), err)
		}

		listener.Close()
	}

	req := testcontainers.ContainerRequest{
		Name:  randomName,
		Image: fmt.Sprintf("%s:%s", TestContainerRedpandaImage, TestContainerRedpandaVersion),
		ExposedPorts: []string{
			fmt.Sprintf("%d:%d/tcp", kafkaPort, kafkaPort),
			fmt.Sprintf("%d:%d/tcp", kafkaProxyPort, kafkaProxyPort),
		},
		Cmd: []string{
			"redpanda", "start",
			"--smp", "1",
			"--overprovisioned",
			"--node-id", "0",
			"--kafka-addr", fmt.Sprintf("SSL://0.0.0.0:%d", kafkaPort),
			"--advertise-kafka-addr", fmt.Sprintf("SSL://localhost:%d", kafkaPort),
			"--pandaproxy-addr", fmt.Sprintf("0.0.0.0:%d", kafkaProxyPort),
			"--advertise-pandaproxy-addr", fmt.Sprintf("localhost:%d", kafkaProxyPort),
		},
		WaitingFor: wait.ForLog("Successfully started Redpanda!"),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, err
	}

	return &GenericTestContainerWrapper{
		Container:      container,
		KafkaPort:      kafkaPort,
		KafkaProxyPort: kafkaProxyPort,
	}, nil
}

func (w *GenericTestContainerWrapper) CleanUp() error {
	return nil
}

func (w *GenericTestContainerWrapper) GetBrokerAddresses() []string {
	return []string{fmt.Sprintf("localhost:%d", w.KafkaPort)}
}
