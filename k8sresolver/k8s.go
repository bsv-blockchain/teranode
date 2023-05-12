package k8sresolver

import (
	"context"
	"fmt"
	"time"

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
	k8s       kubernetes.Interface
	namespace string
}

func newInClusterClient(namespace string) (*serviceClient, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("k8s resolver: failed to build in-cluster kuberenets config: %s", err)
	}
	// creates the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("k8s resolver: failed to provisiong Kubernetes client set: %s", err)
	}

	return &serviceClient{k8s: clientset, namespace: namespace}, nil
}

func (s *serviceClient) Resolve(ctx context.Context, host string, port string) ([]string, error) {
	eps := []string{}

	ep, err := s.k8s.CoreV1().Endpoints(s.namespace).Get(ctx, host, metav1.GetOptions{})
	if err != nil {
		return eps, fmt.Errorf("k8s resolver: failed to fetch service endpoint: %s", err)
	}

	for _, v := range ep.Subsets {
		for _, addr := range v.Addresses {
			eps = append(eps, fmt.Sprintf("%s:%s", addr.IP, port))
		}
	}

	return eps, nil
}

func (s *serviceClient) Watch(ctx context.Context, host string) (<-chan watch.Event, chan struct{}, error) {
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
