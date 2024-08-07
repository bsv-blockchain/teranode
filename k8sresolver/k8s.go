package k8sresolver

import (
	"context"
	"fmt"
	"github.com/bitcoin-sv/ubsv/errors"
	"time"

	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/jellydator/ttlcache/v3"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
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
	logger       ulogger.Logger
	resolveCache *ttlcache.Cache[string, []string]
}

func newInClusterClient(logger ulogger.Logger, namespace string) (*serviceClient, error) {
	logger.Debugf("[k8s] newInClusterClient called with namespace: %s", namespace)
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, errors.NewServiceError("k8s resolver: failed to build in-cluster kuberenets config: %s", err)
	}
	// creates the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, errors.NewConfigurationError("k8s resolver: failed to provisiong Kubernetes client set: %s", err)
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

	ep, err := s.k8s.CoreV1().Endpoints(s.namespace).Get(ctx, host, metav1.GetOptions{})
	if err != nil {
		return eps, errors.NewServiceError("k8s resolver: failed to fetch service endpoint: %s", err)
	}

	for _, v := range ep.Subsets {
		for _, addr := range v.Addresses {
			eps = append(eps, fmt.Sprintf("%s:%s", addr.IP, port))
		}
	}

	if resolveTTL > 0 {
		_ = s.resolveCache.Set(cacheKey, eps, resolveTTL)
	}

	return eps, nil
}

func (s *serviceClient) Watch(_ context.Context, host string) (<-chan watch.Event, chan struct{}, error) {
	s.logger.Debugf("[k8s] Watch called with host: %s", host)
	ev := make(chan watch.Event)

	watchList := cache.NewListWatchFromClient(s.k8s.CoreV1().RESTClient(), "endpoints", s.namespace, fields.OneTermEqualSelector("metadata.name", host))
	_, controller := cache.NewInformer(watchList, &v1.Endpoints{}, time.Second*5, cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			ev <- watch.Event{Type: watch.Added, Object: obj.(runtime.Object)}
		},
		DeleteFunc: func(obj interface{}) {
			ev <- watch.Event{Type: watch.Deleted, Object: obj.(runtime.Object)}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			ev <- watch.Event{Type: watch.Modified, Object: newObj.(runtime.Object)}
		},
	})

	stop := make(chan struct{})

	go controller.Run(stop)

	return ev, stop, nil
}
