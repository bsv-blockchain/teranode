package servicemanager

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

type serviceWrapper struct {
	name     string
	instance Service
	index    int
}

var (
	mu        sync.RWMutex
	listeners []string = make([]string, 0)
)

type ServiceManager struct {
	services           []serviceWrapper
	dependencyChannels []chan bool
	logger             utils.Logger
	ctx                context.Context
	cancelFunc         context.CancelFunc
	g                  *errgroup.Group
}

func NewServiceManager() (*ServiceManager, context.Context) {
	ctx, cancelFunc := context.WithCancel(context.Background())

	g, ctx := errgroup.WithContext(ctx)

	logger := gocore.Log("sm")

	sm := &ServiceManager{
		services:   make([]serviceWrapper, 0),
		logger:     logger,
		ctx:        ctx,
		cancelFunc: cancelFunc,
		g:          g,
	}

	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

		<-sigs
		sm.logger.Infof("🟠 Received shutdown signal. Stopping services...")
		sm.cancelFunc()
	}()

	http.HandleFunc("/services", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		_ = json.NewEncoder(w).Encode(GetListenerInfos())
	})

	return sm, ctx
}

func AddListenerInfo(name string) {
	mu.Lock()
	defer mu.Unlock()

	listeners = append(listeners, name)
}

func GetListenerInfos() []string {
	mu.RLock()
	defer mu.RUnlock()

	// Sort the listeners
	sortedListeners := make([]string, len(listeners))
	copy(sortedListeners, listeners)
	sort.Strings(sortedListeners)

	return sortedListeners
}

func (sm *ServiceManager) AddService(name string, service Service) error {

	sm.dependencyChannels = append(sm.dependencyChannels, make(chan bool))

	sw := serviceWrapper{
		name:     name,
		instance: service,
		index:    len(sm.dependencyChannels) - 1,
	}

	sm.services = append(sm.services, sw)

	sm.logger.Infof("⚪️ Initializing service %s...", name)
	if err := service.Init(sm.ctx); err != nil {
		return err
	}

	sm.logger.Infof("🟢 Starting service %s...", name)

	sm.g.Go(func() error {
		sm.waitForPreviousServiceToStart(sw)
		close(sm.dependencyChannels[sw.index])

		return service.Start(sm.ctx)
	})

	return nil
}

func (sm *ServiceManager) waitForPreviousServiceToStart(sw serviceWrapper) {
	if sw.index > 0 {
		timer := time.NewTimer(5 * time.Second)

		// Wait for previous service to start
		select {
		case <-sm.dependencyChannels[sw.index-1]:
			// Previous service has started
			return
		case <-timer.C:
			sm.logger.Fatalf("%s (index %d) timed out waiting for previous service to start", sw.name, sw.index)
		}
	}
}

// StartAllAndWait starts all services and waits for them to complete or error.
// If any service errors, all other services are stopped gracefully and the error is returned.
func (sm *ServiceManager) Wait() error {
	// Wait for all services to complete or error
	err := sm.g.Wait()
	if err != nil {
		sm.logger.Errorf("Received error: %v", err)
	}

	for i := len(sm.services) - 1; i >= 0; i-- {
		service := sm.services[i]

		// Ensure all other services are stopped gracefully with a 10-second timeout
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)

		sm.logger.Infof("🟠 Stopping service %s...", service.name)

		if err := service.instance.Stop(stopCtx); err != nil {
			sm.logger.Warnf("[%s] Failed to stop service: %v", service.name, err)
		} else {
			sm.logger.Infof("[%s] Service stopped gracefully", service.name)
		}

		stopCancel()
	}

	sm.logger.Infof("🛑 All services stopped.")

	return err // This is the original error
}
