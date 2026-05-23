package k8sresolver

import (
	"context"
	"fmt"
	"time"

	"github.com/jellydator/ttlcache/v3"
	discovery "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

type serviceEndpointResolver interface {
	Resolve(ctx context.Context, host string, port string) ([]string, error)
	Watch(ctx context.Context, host string) (<-chan watch.Event, chan struct{}, error)
}

type serviceClient struct {
	k8s          kubernetes.Interface
	namespace    string
	logger       logger
	resolveCache *ttlcache.Cache[string, []string]
}

func newInClusterClient(logger logger, namespace string) (*serviceClient, error) {
	logger.Debugf("[k8s] newInClusterClient called with namespace: %s", namespace)

	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("k8s resolver: failed to build in-cluster kuberenets config: %s", err)
	}

	// creates the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("k8s resolver: failed to provisiong Kubernetes client set: %s", err)
	}

	return &serviceClient{
		k8s:          clientset,
		namespace:    namespace,
		logger:       logger,
		resolveCache: ttlcache.New[string, []string](),
	}, nil
}

func (s *serviceClient) Resolve(ctx context.Context, host string, port string) ([]string, error) {
	cacheKey := host + ":" + port
	cachedEps := s.resolveCache.Get(cacheKey, ttlcache.WithDisableTouchOnHit[string, []string]())

	if cachedEps != nil {
		s.logger.Debugf("[k8s] Resolve returning cached eps for host: %s, port: %s", host, port)

		return cachedEps.Value(), nil
	}

	s.logger.Debugf("[k8s] Resolve called with host: %s, port: %s", host, port)

	eps := make([]string, 0)

	slices, err := s.k8s.DiscoveryV1().EndpointSlices(s.namespace).List(ctx,
		metav1.ListOptions{LabelSelector: discovery.LabelServiceName + "=" + host},
	)
	if err != nil {
		return eps, fmt.Errorf("k8s resolver: failed to fetch service endpoint %s: %s", host, err)
	}

	for _, v := range slices.Items {
		for _, endpoint := range v.Endpoints {
			for _, address := range endpoint.Addresses {
				eps = append(eps, fmt.Sprintf("%s:%s", address, port))
			}
		}
	}

	s.logger.Debugf("[k8s] Found %d endpoints for service %s: %v", len(eps), host, eps)

	if resolveTTL > 0 {
		_ = s.resolveCache.Set(cacheKey, eps, resolveTTL)
	}

	return eps, nil
}

func (s *serviceClient) Watch(ctx context.Context, host string) (<-chan watch.Event, chan struct{}, error) {
	s.logger.Debugf("[k8s] Watch called with host: %s", host)

	ev := make(chan watch.Event)
	stop := make(chan struct{})

	labelSelector := discovery.LabelServiceName + "=" + host
	watchList := &cache.ListWatch{
		ListWithContextFunc: func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
			options.LabelSelector = labelSelector
			return s.k8s.DiscoveryV1().EndpointSlices(s.namespace).List(ctx, options)
		},
		WatchFuncWithContext: func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
			options.LabelSelector = labelSelector
			return s.k8s.DiscoveryV1().EndpointSlices(s.namespace).Watch(ctx, options)
		},
	}
	_, controller := cache.NewInformerWithOptions(cache.InformerOptions{
		ListerWatcher: watchList,
		ObjectType:    &discovery.EndpointSlice{},
		ResyncPeriod:  time.Second * 5,
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				select {
				case ev <- watch.Event{Type: watch.Added, Object: obj.(runtime.Object)}:
				case <-ctx.Done():
				case <-stop:
				}
			},
			DeleteFunc: func(obj interface{}) {
				select {
				case ev <- watch.Event{Type: watch.Deleted, Object: obj.(runtime.Object)}:
				case <-ctx.Done():
				case <-stop:
				}
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				select {
				case ev <- watch.Event{Type: watch.Modified, Object: newObj.(runtime.Object)}:
				case <-ctx.Done():
				case <-stop:
				}
			},
		},
	})

	go func() {
		defer close(ev)

		// Start the controller with the stop channel
		controller.Run(stop)
	}()

	return ev, stop, nil
}
