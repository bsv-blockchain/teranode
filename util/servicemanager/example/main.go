package main

import (
	"context"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/servicemanager"
)

// SampleService is a mock service for demonstration purposes.
// It implements the Service interface to show how services can be managed
// by the service manager.
type SampleService struct {
	name   string
	logger ulogger.Logger
}

// NewService creates a new SampleService with the given name.
// It initializes the service with a logger configured for the service name.
func NewService(name string) *SampleService {
	return &SampleService{
		name:   name,
		logger: ulogger.New(name),
	}
}

// Health returns the health status of the service.
// It implements the Service interface health check method.
// Returns status code (0 for healthy), status message, and any error.
func (s *SampleService) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	return 0, "", nil
}

// Init initializes the service.
// It implements the Service interface initialization method.
// Returns an error if initialization fails.
func (s *SampleService) Init(ctx context.Context) error {
	return nil
}
// Start starts the service and runs it until the context is cancelled.
// It implements the Service interface start method.
// The ready channel is signaled when the service is ready to accept requests.
// Returns an error if the service encounters a fatal error during execution.
func (s *SampleService) Start(ctx context.Context, ready chan<- struct{}) error {
	// Simulating some long-running work
	s.logger.Infof("Service %s is running...\n", s.name)

	// Signal that the service is ready
	if ready != nil {
		close(ready)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
			// Simulate an error for demonstration
			if s.name == "SvcB" {
				return errors.NewServiceError("SvcB start encountered an error")
			}
		}
	}
}

// Stop stops the service gracefully.
// It implements the Service interface stop method.
// Performs any necessary cleanup and returns an error if stopping fails.
func (s *SampleService) Stop(ctx context.Context) error {
	// Simulating cleanup work or graceful shutdown
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(1 * time.Second): // Simulating some delay for stopping
		s.logger.Infof("Service %s stopped\n", s.name)
	}

	return nil
}

// main demonstrates how to use the service manager to manage multiple services.
// It creates several sample services, adds them to a service manager, and starts them.
// The application runs until all services complete or encounter an error.
func main() {
	logger := ulogger.New("main")

	// Creating a root context for the application
	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serviceManager := servicemanager.NewServiceManager(rootCtx, logger)

	// Add services to the service manager
	serviceManager.AddService("ServiceA", NewService("SvcA"))
	serviceManager.AddService("ServiceB", NewService("SvcB"))
	serviceManager.AddService("ServiceC", NewService("SvcC"))

	err := serviceManager.Wait()
	if err != nil {
		logger.Infof("Service manager returned error: %v", err)
	} else {
		logger.Infof("Service manager returned with no errors")
	}
}
