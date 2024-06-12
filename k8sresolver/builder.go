package k8sresolver

import (
	"context"
	"strings"
	"time"

	"github.com/bitcoin-sv/ubsv/ulogger"
	"google.golang.org/grpc/resolver"
)

const (
	defaultPort      = "443"
	defaultNamespace = "default"
	minK8SResRate    = 5 * time.Second
)

var (
	logger = ulogger.New("k8sres")
)

func init() {
	logger.Infof("[k8s] GRPC k8sresolver init")
	resolver.Register(NewBuilder(logger))
}

// NewBuilder creates a k8sBuilder which is used to factory K8S service resolvers.
func NewBuilder(l ulogger.Logger) resolver.Builder {
	return &k8sBuilder{
		logger: l,
	}
}

type k8sBuilder struct {
	logger ulogger.Logger
}

func (b *k8sBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	logger.Debugf("[k8s] Build called with target: %v", target)
	host, port, err := parseTarget(target.Endpoint(), defaultPort)
	if err != nil {
		return nil, err
	}

	namespace, host := getNamespaceFromHost(host)

	k8sc, err := newInClusterClient(logger, namespace)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	k := &k8sResolver{
		k8sC:   k8sc,
		host:   host,
		port:   port,
		ctx:    ctx,
		cancel: cancel,
		cc:     cc,
		rn:     make(chan struct{}, 1),
		logger: logger,
	}

	k.wg.Add(1)
	go k.watcher()
	k.ResolveNow(resolver.ResolveNowOptions{})
	return k, nil
}

// Scheme returns the naming scheme of this resolver builder, which is "k8s".
func (b *k8sBuilder) Scheme() string {
	return "k8s"
}

func getNamespaceFromHost(host string) (string, string) {
	namespace := defaultNamespace

	hostParts := strings.Split(host, ".")
	if len(hostParts) >= 2 {
		namespace = hostParts[1]
		host = hostParts[0]
	}
	return namespace, host
}
