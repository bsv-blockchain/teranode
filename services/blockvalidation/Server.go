package blockvalidation

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	blockvalidation_api "github.com/TAAL-GmbH/ubsv/services/blockvalidation/blockvalidation_api"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	prometheusBlockValidationBlockFound prometheus.Counter
)

func init() {
	prometheusBlockValidationBlockFound = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "blockvalidation_block_found",
			Help: "Number of blocks found",
		},
	)
}

// BlockValidation type carries the logger within it
type BlockValidation struct {
	blockvalidation_api.UnimplementedBlockValidationAPIServer
	logger     utils.Logger
	grpcServer *grpc.Server
}

func Enabled() bool {
	_, found := gocore.Config().Get("blockvalidation_grpcAddress")
	return found
}

// New will return a server instance with the logger stored within it
func New(logger utils.Logger) *BlockValidation {
	// utxostoreURL, err, found := gocore.Config().GetURL("utxostore")
	// if err != nil {
	// 	panic(err)
	// }
	// if !found {
	// 	panic("no utxostore setting found")
	// }

	// s, err := utxo.NewStore(logger, utxostoreURL)
	// if err != nil {
	// 	panic(err)
	// }

	bVal := &BlockValidation{
		// utxoStore: s,
		logger: logger,
	}

	return bVal
}

// Start function
func (u *BlockValidation) Start() error {

	address, ok := gocore.Config().Get("blockvalidation_grpcAddress")
	if !ok {
		return errors.New("no blockvalidation_grpcAddress setting found")
	}

	var err error
	u.grpcServer, err = utils.GetGRPCServer(&utils.ConnectionOptions{
		OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
	})
	if err != nil {
		return fmt.Errorf("could not create GRPC server [%w]", err)
	}

	gocore.SetAddress(address)

	lis, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("GRPC server failed to listen [%w]", err)
	}

	blockvalidation_api.RegisterBlockValidationAPIServer(u.grpcServer, u)

	// Register reflection service on gRPC server.
	reflection.Register(u.grpcServer)

	u.logger.Infof("GRPC server listening on %s", address)

	if err = u.grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("GRPC server failed [%w]", err)
	}

	return nil
}

func (u *BlockValidation) Stop(ctx context.Context) {
	_, cancel := context.WithCancel(ctx)
	defer cancel()

	u.grpcServer.GracefulStop()
}

func (u *BlockValidation) Health(_ context.Context, _ *emptypb.Empty) (*blockvalidation_api.HealthResponse, error) {
	return &blockvalidation_api.HealthResponse{
		Ok:        true,
		Timestamp: timestamppb.New(time.Now()),
	}, nil
}

func (u *BlockValidation) BlockFound(ctx context.Context, req *blockvalidation_api.BlockFoundRequest) (*emptypb.Empty, error) {
	prometheusBlockValidationBlockFound.Inc()

	// check we have all the subtrees. If any are missing request them then validate them.
	subtreeRoots, err := splitSubtreeRoots(req.SubtreeRoots)
	if err != nil {
		return nil, err
	}

	waitGroup := sync.WaitGroup{}

	for _, subtreeRoot := range subtreeRoots {
		go func(chunk []byte) {
			isValid := validateSubtree(chunk)
			if !isValid {
				// an invalid subtree has been found.
				// logging, cleanup
				return
			} else {
				waitGroup.Done()
			}
		}(subtreeRoot)
	}

	waitGroup.Wait()

	// check merkle root
	// check the solution meets the difficulty requirements
	// check the block is valid by concensus rules.
	// persist block
	// inform block assembler that a new block has been found

	return &emptypb.Empty{}, nil
}

func splitSubtreeRoots(subtreeRoots []byte) ([][]byte, error) {
	if len(subtreeRoots) == 0 {
		return nil, errors.New("empty subtreeRoots array")
	}
	if len(subtreeRoots)%32 != 0 {
		return nil, errors.New("length of subtreeRoots must be a multiple of 32")
	}
	// Split the byte array into 32 byte chunks
	chunks := make([][]byte, len(subtreeRoots)/32)
	for i := 0; i < len(chunks); i++ {
		chunks[i] = subtreeRoots[i*32 : (i+1)*32]
	}

	return chunks, nil
}

func validateSubtree(subtree []byte) bool {
	// get subtree from store
	// if not in store get it from the network
	// validate the subtree
	// is the txid in the store?
	// no - get it from the network
	// yes - is the txid blessed?
	// does the merkle tree give the correct root?
	// if all txs in tree are blessed, then bless the tree

	return true
}
