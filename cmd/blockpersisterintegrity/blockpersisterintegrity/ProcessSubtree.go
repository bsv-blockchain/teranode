package blockpersisterintegrity

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

type SubtreeProcessor struct {
	logger ulogger.Logger
	store  blob.Store
	block  *model.Block
	tp     *TxProcessor
}

func NewSubtreeProcessor(logger ulogger.Logger, store blob.Store, block *model.Block, tp *TxProcessor) *SubtreeProcessor {
	return &SubtreeProcessor{
		logger: logger,
		store:  store,
		block:  block,
		tp:     tp,
	}
}

func (stp *SubtreeProcessor) ProcessSubtree(ctx context.Context, subtreeHash chainhash.Hash) error {
	fees := uint64(0)
	subtreeReader, err := stp.store.GetIoReader(ctx, subtreeHash[:], options.WithFileExtension("subtree"))
	if err != nil {
		return errors.NewStorageError("failed to get subtree %s for block %s: %s", subtreeHash, stp.block, err)
	}

	var txCountBytes = make([]byte, 4)
	if _, err := io.ReadFull(subtreeReader, txCountBytes); err != nil {
		return errors.NewProcessingError("failed to read tx count for subtree %s: %s", subtreeHash, err)
	}
	txCount := binary.LittleEndian.Uint32(txCountBytes)

	buffer := make([]byte, 8192)
	partialBuffer := bytes.NewBuffer(nil)
	i := uint32(0)
	for {
		n, _ := io.ReadFull(subtreeReader, buffer)
		partialBuffer.Write(buffer[:n])

		var btTx *bt.Tx
		var bytesRead int
		btTx, bytesRead, err = bt.NewTxFromStream(partialBuffer.Bytes())
		if errors.Is(err, io.ErrUnexpectedEOF) {
			// need to read more data
			continue
		}
		if errors.Is(err, io.EOF) {
			// No more data to read
			break
		}
		if err != nil {
			return errors.NewProcessingError("failed to parse transaction %d in subtree %s: %s", i, subtreeHash, err)
		}
		partialBuffer.Next(bytesRead)
		i++

		var txFees uint64
		err = stp.tp.ProcessTx(ctx, btTx)
		if err != nil {
			stp.logger.Errorf("failed to process tx %s: %s", btTx.TxIDChainHash(), err)
		}
		fees += txFees
	} // for each Tx

	if i != txCount {
		return errors.NewSubtreeError("subtree %s has %d transactions, but only %d were read", subtreeHash, txCount, i)
	}

	return nil
}
