package utxopersister

import (
	"context"

	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/ordishs/gocore"
)

// Server type carries the logger within it
type Server struct {
	logger     ulogger.Logger
	blockStore blob.Store
	stats      *gocore.Stat
}

func New(
	ctx context.Context,
	logger ulogger.Logger,
	blockStore blob.Store,
) *Server {

	return &Server{
		logger:     logger,
		blockStore: blockStore,
		stats:      gocore.NewStat("utxopersister"),
	}
}

func (u *Server) Health(ctx context.Context) (int, string, error) {
	return 0, "", nil
}

func (u *Server) Init(ctx context.Context) (err error) {
	return nil
}

// Start function
func (u *Server) Start(ctx context.Context) error {
	// client, err := blockchain.NewClient(ctx, u.logger)
	// if err != nil {
	// 	http.Error(w, "failed to create blockchain client", http.StatusInternalServerError)
	// 	return
	// }

	// client.Subscribe(ctx, "utxo-persister")

	// if gocore.Config().GetBool("blockPersister_processUTXOSets", false) {
	// 	u.logger.Infof("[BlockPersister] Processing UTXOSet for block %s", block.Header.Hash().String())

	// 	pBlock := block.Header.HashPrevBlock
	// 	if pBlock.String() == "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f" {
	// 		// Genesis block
	// 		pBlock = nil
	// 	}
	// 	if err := utxoDiff.CreateUTXOSet(ctx, pBlock); err != nil {
	// 		u.logger.Errorf("[BlockPersister] Error processing UTXOSet for block %s: %v", block.Header.Hash().String(), err)
	// 	}
	// }

	// 	block, err := client.GetBlock(req.Context(), hash)
	// 	if err != nil {
	// 		http.Error(w, "failed to get block", http.StatusInternalServerError)
	// 		return
	// 	}

	// 	blockBytes, err := block.Bytes()
	// 	if err != nil {
	// 		http.Error(w, "failed to get block bytes", http.StatusInternalServerError)
	// 		return
	// 	}

	// 	u.persistBlock(req.Context(), hash, blockBytes)

	// 	w.WriteHeader(http.StatusOK)
	// })

	<-ctx.Done()

	return nil
}

func (u *Server) Stop(_ context.Context) error {
	return nil
}
