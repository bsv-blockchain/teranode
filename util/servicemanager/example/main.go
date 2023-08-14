package main

import (
	"context"
	"errors"
	"time"

	"github.com/ordishs/go-utils/servicemanager"

	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

// SampleService is a mock service for demonstration purposes.
type SampleService struct {
	name   string
	logger utils.Logger
}

func NewService(name string) *SampleService {
	return &SampleService{
		name:   name,
		logger: gocore.Log(name),
	}
}

func (s *SampleService) Init(ctx context.Context) error {
	// if s.name == "SvcB" {
	// 	for {
	// 		select {
	// 		case <-ctx.Done():
	// 			s.logger.Infof("Aborting init of service %s", s.name)
	// 			return ctx.Err()
	// 		case <-time.After(2 * time.Second):
	// 			// Simulate an error for demonstration
	// 			return errors.New("SvcB init encountered an error")
	// 		}
	// 	}
	// }
	return nil
}
func (s *SampleService) Start(ctx context.Context) error {
	// Simulating some long-running work
	s.logger.Infof("Service %s is running...\n", s.name)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
			// Simulate an error for demonstration
			if s.name == "SvcB" {
				return errors.New("SvcB start encountered an error")
			}
		}
	}
}

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

func main() {
	logger := gocore.Log("main")

	serviceManager := servicemanager.NewServiceManager()

	// Add services to the service manager
	serviceManager.AddService("ServiceA", NewService("SvcA"))
	serviceManager.AddService("ServiceB", NewService("SvcB"))
	serviceManager.AddService("ServiceC", NewService("SvcC"))

	// Creating a root context for the application
	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := serviceManager.StartAllAndWait(rootCtx)
	if err != nil {
		logger.Infof("Service manager returned error: %v", err)
	} else {
		logger.Infof("Service manager returned with no errors")
	}

}
