package servicemanager

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/ulogger"
	"golang.org/x/sync/errgroup"
)

type serviceWrapper struct {
	name     string
	instance Service
	index    int
}

var (
	instance  *ServiceManager
	once      sync.Once
	mu        sync.RWMutex
	listeners []string = make([]string, 0)
)

type ServiceManager struct {
	services              []serviceWrapper
	dependencyChannelsMux sync.Mutex
	dependencyChannels    []chan bool
	logger                ulogger.Logger
	Ctx                   context.Context
	cancelFunc            context.CancelFunc
	g                     *errgroup.Group
	// statusClient       status.ClientI
}

// NewServiceManager creates a new service manager or returns the existing instance
func NewServiceManager(logger ulogger.Logger) *ServiceManager {
	once.Do(func() {
		instance = initServiceManager(logger)
	})

	return instance
}

// initServiceManager initializes a new ServiceManager instance
func initServiceManager(logger ulogger.Logger) *ServiceManager {
	ctx, cancelFunc := context.WithCancel(context.Background())

	g, ctx := errgroup.WithContext(ctx)

	sm := &ServiceManager{
		services:   make([]serviceWrapper, 0),
		logger:     logger,
		Ctx:        ctx,
		cancelFunc: cancelFunc,
		g:          g,
		// statusClient: statusClient,
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

	return sm
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
	sm.dependencyChannelsMux.Lock()
	sm.dependencyChannels = append(sm.dependencyChannels, make(chan bool))

	sw := serviceWrapper{
		name:     name,
		instance: service,
		index:    len(sm.dependencyChannels) - 1,
	}
	sm.dependencyChannelsMux.Unlock()

	sm.services = append(sm.services, sw)

	sm.logger.Infof("⚪️ Initializing service %s...", name)

	if err := service.Init(sm.Ctx); err != nil {
		return err
	}

	sm.logger.Infof("🟢 Starting service %s...", name)

	sm.g.Go(func() error {
		if sw.index > 0 {
			sm.dependencyChannelsMux.Lock()
			channel := sm.dependencyChannels[sw.index-1]
			sm.dependencyChannelsMux.Unlock()

			if err := sm.waitForPreviousServiceToStart(sw, channel); err != nil {
				return err
			}
		}

		sm.dependencyChannelsMux.Lock()
		close(sm.dependencyChannels[sw.index])
		sm.dependencyChannelsMux.Unlock()

		if err := service.Start(sm.Ctx); err != nil {
			sm.logger.Errorf("Error from service start %s: %v", name, err)
			return err
		}

		return nil
	})

	return nil
}

func (sm *ServiceManager) waitForPreviousServiceToStart(sw serviceWrapper, channel chan bool) error {
	timer := time.NewTimer(5 * time.Second)

	// Wait for previous service to start
	select {
	case <-channel:
		// Previous service has started
		return nil
	case <-timer.C:
		return errors.NewServiceError("%s (index %d) timed out waiting for previous service to start", sw.name, sw.index)
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

func (sm *ServiceManager) HealthHandler(ctx context.Context, checkLiveness bool) (int, string, error) {
	overallStatus := http.StatusOK
	msgs := make([]string, 0, len(sm.services))

	for _, service := range sm.services {
		status, details, err := service.instance.Health(ctx, checkLiveness)

		if err != nil || status != http.StatusOK {
			overallStatus = http.StatusServiceUnavailable
		}

		jsonStr := fmt.Sprintf(`{"service": "%s","status": "%d","dependencies": [%s]}`, service.name, status, details)

		msgs = append(msgs, jsonStr)
	}

	jsonStr := fmt.Sprintf(`{"status": "%d", "services": [%s]}`, overallStatus, strings.Join(msgs, ",\n"))

	var jsonFormatted bytes.Buffer

	err := json.Indent(&jsonFormatted, []byte(jsonStr), "", "  ")
	if err == nil {
		jsonStr = jsonFormatted.String()
	}

	return overallStatus, jsonStr, nil
}
