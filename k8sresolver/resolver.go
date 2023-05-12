package k8sresolver

import (
	"context"
	"errors"
	"sync"
	"time"

	"google.golang.org/grpc/resolver"
	"k8s.io/apimachinery/pkg/watch"
)

const (
	watcherRetryDuration = 10 * time.Second
)

var (
	errNoEndpoints = errors.New("no endpoints available")
)

type k8sResolver struct {
	k8sC   serviceEndpointResolver
	host   string
	port   string
	ctx    context.Context
	cancel context.CancelFunc
	cc     resolver.ClientConn
	// rn channel is used by ResolveNow() to force an immediate resolution of the target.
	rn chan struct{}
	// wg is used to enforce Close() to return after the watcher() goroutine has finished.
	// Otherwise, data race will be possible.
	// If Close() doesn't wait for watcher() goroutine finishes, race detector sometimes
	// will warns lookup (READ the lookup function pointers) inside watcher() goroutine
	// has data race with replaceNetFunc (WRITE the lookup function pointers).
	wg sync.WaitGroup
}

// ResolveNow invoke an immediate resolution of the target that this k8sResolver watches.
func (k *k8sResolver) ResolveNow(resolver.ResolveNowOptions) {
	select {
	case k.rn <- struct{}{}:
	default:
	}
}

// Close closes the k8sResolver.
func (k *k8sResolver) Close() {
	k.cancel()
	k.wg.Wait()
}

func (k *k8sResolver) watcher() {
	defer k.wg.Done()

	var we <-chan watch.Event
	var stop chan struct{}
	var err error

	for {
		we, stop, err = k.k8sC.Watch(k.ctx, k.host)
		if err != nil {
			logger.Errorf("unable to watch service endpoints (%s:%s): %s - retry in %s", k.host, k.port, err, watcherRetryDuration)
			time.Sleep(watcherRetryDuration)
			continue
		}
		break
	}

	for {
		select {
		case <-k.ctx.Done():
			close(stop)
			return
		case <-k.rn:
		case <-we:
		}

		state, err := k.lookup()
		if err != nil {
			k.cc.ReportError(err)
		} else {
			k.cc.UpdateState(*state)
		}

		// Sleep to prevent excessive re-resolutions. Incoming resolution requests
		// will be queued in k.rn.
		t := time.NewTimer(minK8SResRate)
		select {
		case <-t.C:
		case <-k.ctx.Done():
			t.Stop()
			close(stop)
			return
		}
	}
}

func (k *k8sResolver) lookup() (*resolver.State, error) {
	endpoints, err := k.k8sC.Resolve(k.ctx, k.host, k.port)
	if err != nil {
		return nil, err
	}

	state := &resolver.State{Addresses: []resolver.Address{}}

	for _, ep := range endpoints {
		state.Addresses = append(state.Addresses, resolver.Address{Addr: ep})
	}

	return state, nil
}
