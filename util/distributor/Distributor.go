package distributor

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/bitcoin-sv/ubsv/services/propagation/propagation_api"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

type Distributor struct {
	logger             utils.Logger
	propagationServers map[string]propagation_api.PropagationAPIClient
}

func GetPropagationGRPCAddresses() []string {
	addresses, _ := gocore.Config().GetMulti("propagation_grpcAddresses", "|")
	return addresses
}

func NewDistributor(logger utils.Logger) (*Distributor, error) {
	addresses := GetPropagationGRPCAddresses()

	if len(addresses) == 0 {
		return nil, errors.New("no propagation server addresses found")
	}

	propagationServers := make(map[string]propagation_api.PropagationAPIClient)

	for _, address := range addresses {
		pConn, err := util.GetGRPCClient(context.Background(), address, &util.ConnectionOptions{
			OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
			Prometheus:  gocore.Config().GetBool("use_prometheus_grpc_metrics", true),
			MaxRetries:  3,
		})
		if err != nil {
			return nil, fmt.Errorf("error connecting to propagation server %s: %w", address, err)
		}

		propagationServers[address] = propagation_api.NewPropagationAPIClient(pConn)
	}

	return &Distributor{
		logger:             logger,
		propagationServers: propagationServers,
	}, nil
}

type errorWrapper struct {
	addr string
	err  error
}

func (d *Distributor) SendTransaction(ctx context.Context, tx *bt.Tx) error {
	var wg sync.WaitGroup

	errorWrapperCh := make(chan errorWrapper, len(d.propagationServers))

	for addr, propagationServer := range d.propagationServers {
		a := addr // Create a local copy
		p := propagationServer

		wg.Add(1)
		go func() {
			defer wg.Done()

			if _, err := p.ProcessTransaction(ctx, &propagation_api.ProcessTransactionRequest{
				Tx: tx.ExtendedBytes(),
			}); err != nil {
				errorWrapperCh <- errorWrapper{
					addr: a,
					err:  err,
				}
			}
		}()
	}
	wg.Wait()

	close(errorWrapperCh)

	// Read any errors from the channel
	errors := strings.Builder{}
	errorCount := 0

	for errorWrapper := range errorWrapperCh {
		errors.WriteString(fmt.Sprintf("\t%s: %v\n", errorWrapper.addr, errorWrapper.err))
		errorCount++
	}

	if errorCount > 0 {
		d.logger.Debugf("Error(s) distributing transaction %s:\n%s", tx.TxIDChainHash().String(), errors.String())
	} else {
		d.logger.Debugf("Successfully distributed transaction %s", tx.TxIDChainHash().String())
	}

	if float32(errorCount)/float32(len(d.propagationServers)) <= 0.5 {
		return fmt.Errorf("error sending transaction to more than half of the propagation servers")
	}

	return nil
}
