package k8sresolver

import (
	"context"
	"sync"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/jellydator/ttlcache/v3"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc/resolver"
	"k8s.io/apimachinery/pkg/watch"
)

const (
	watcherRetryDuration = 10 * time.Second
)

var (
	errNoEndpoints = errors.NewConfigurationError("no endpoints available")
	resolveTTL     time.Duration
	resolveCache   *ttlcache.Cache[string, *resolver.State]
)

func init() {
	resolveTTLSeconds, _ := gocore.Config().GetInt("k8s_resolver_ttl", 10)
	resolveTTL = time.Duration(resolveTTLSeconds) * time.Second
	resolveCache = ttlcache.New[string, *resolver.State]()
}

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
	wg     sync.WaitGroup
	logger ulogger.Logger
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
			_ = k.cc.UpdateState(*state)
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
	cachedState := resolveCache.Get(k.host+":"+k.port, ttlcache.WithDisableTouchOnHit[string, *resolver.State]())
	if cachedState != nil {
		k.logger.Debugf("[k8s] returning cached endpoints for host: %s, port: %s", k.host, k.port)
		return cachedState.Value(), nil
	}

	k.logger.Debugf("[k8s] looking up service endpoints (%s:%s)", k.host, k.port)
	endpoints, err := k.k8sC.Resolve(k.ctx, k.host, k.port)
	if err != nil {
		return nil, err
	}

	if len(endpoints) == 0 {
		return nil, errNoEndpoints
	}

	k.logger.Debugf("[k8s] found %d service endpoints (%s:%s)", len(endpoints), k.host, k.port)
	k.logger.Debugf("[k8s] endpoints: %v", endpoints)

	state := &resolver.State{Addresses: []resolver.Address{}}

	for _, ep := range endpoints {
		state.Addresses = append(state.Addresses, resolver.Address{Addr: ep})
	}

	k.logger.Debugf("[k8s] state: %v", state)

	if resolveTTL > 0 {
		_ = resolveCache.Set(k.host+":"+k.port, state, resolveTTL)
	}

	return state, nil
}
