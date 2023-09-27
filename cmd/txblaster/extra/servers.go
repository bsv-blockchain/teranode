package extra

import "github.com/bitcoin-sv/ubsv/services/propagation/propagation_api"

type PropagationServer struct {
	GRPC propagation_api.PropagationAPIClient
	DRPC propagation_api.DRPCPropagationAPIClient
	FRPC *propagation_api.Client
}
