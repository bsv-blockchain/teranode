package servicemanager

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

type serviceWrapper struct {
	name     string
	instance Service
}

type ServiceManager struct {
	services []serviceWrapper
	logger   utils.Logger
}

func NewServiceManager() *ServiceManager {
	return &ServiceManager{
		services: make([]serviceWrapper, 0),
		logger:   gocore.Log("sm"),
	}
}

func (sm *ServiceManager) AddService(name string, service Service) {
	sm.services = append(sm.services, serviceWrapper{
		name:     name,
		instance: service,
	})
}

// StartAllAndWait starts all services and waits for them to complete or error.
// If any service errors, all other services are stopped gracefully and the error is returned.
func (sm *ServiceManager) StartAllAndWait(ctx context.Context) error {
	cancelCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Listen for system signals
	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

		<-sigs
		sm.logger.Infof("Received shutdown signal. Stopping services...")
		cancel()
	}()

	// Init all services in series (not in the background)
	for _, service := range sm.services {
		select {
		case <-ctx.Done():
			return ctx.Err()

		default:
			sm.logger.Infof("[%s] Initializing service...", service.name)
			if err := service.instance.Init(cancelCtx); err != nil {
				return err
			}
		}
	}

	g, ctx := errgroup.WithContext(cancelCtx) // Use cancelCtx here

	// Start all services
	for _, service := range sm.services {
		s := service // capture the loop variable

		select {
		case <-ctx.Done():
			return ctx.Err()

		default:
			sm.logger.Infof("[%s] Starting service...", s.name)

			g.Go(func() error {
				return s.instance.Start(ctx)
			})
		}
	}

	// Wait for all services to complete or error
	err := g.Wait()
	if err != nil {
		sm.logger.Errorf("Received error: %v", err)
	}

	for i := len(sm.services) - 1; i >= 0; i-- {
		service := sm.services[i]

		// Ensure all other services are stopped gracefully with a 10-second timeout
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)

		sm.logger.Infof("[%s] Stopping service...", service.name)

		if err := service.instance.Stop(stopCtx); err != nil {
			sm.logger.Warnf("[%s] Failed to stop service: %v", service.name, err)
		} else {
			sm.logger.Infof("[%s] Service stopped gracefully", service.name)
		}

		stopCancel()
	}

	sm.logger.Infof("\U0001f6d1 All services stopped.")

	return err // This is the original error
}
