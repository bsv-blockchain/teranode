package redis

import (
	"context"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	redis_db "github.com/redis/go-redis/v9"
)

func (s *Store) Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) (*meta.Data, error) {
	createOptions := &utxo.CreateOptions{}
	for _, opt := range opts {
		opt(createOptions)
	}

	_, _, deferFn := tracing.StartTracing(ctx, "redis:Create")
	defer deferFn()

	if len(tx.Outputs) == 0 {
		return nil, errors.NewProcessingError("tx %s has no outputs", tx.TxIDChainHash())
	}

	var txHash *chainhash.Hash

	if createOptions.TxID != nil {
		txHash = createOptions.TxID
	} else {
		txHash = tx.TxIDChainHash()
	}

	isCoinbase := tx.IsCoinbase()

	if createOptions.IsCoinbase != nil {
		isCoinbase = *createOptions.IsCoinbase
	}

	var (
		fee          uint64
		utxoHashes   []*chainhash.Hash
		size         int
		extendedSize int
		err          error
	)

	if len(tx.Inputs) == 0 {
		fee = 0
		utxoHashes, err = utxo.GetUtxoHashes(tx, txHash)
	} else {
		size = tx.Size()
		extendedSize = len(tx.ExtendedBytes())
		fee, utxoHashes, err = utxo.GetFeesAndUtxoHashes(context.Background(), tx, blockHeight)
	}

	if err != nil {
		return nil, errors.NewProcessingError("failed to get fees and utxo hashes for %s", txHash, err)
	}

	fields := map[string]interface{}{
		"version":      tx.Version,
		"locktime":     tx.LockTime,
		"fee":          fee,
		"sizeInBytes":  size,
		"extendedSize": extendedSize,
		"spentUtxos":   0,
		"isCoinbase":   isCoinbase,
		"nrInput":      len(tx.Inputs),
		"nrOutput":     len(tx.Outputs),
	}

	if len(createOptions.BlockIDs) > 0 {
		// Convert the blockIDs to a comma separated string
		var blockIDs strings.Builder

		for i, blockID := range createOptions.BlockIDs {
			if i > 0 {
				blockIDs.WriteString(",")
			}

			blockIDs.WriteString(strconv.FormatUint(uint64(blockID), 10))
		}

		fields["blockIDs"] = blockIDs.String()
	}

	if isCoinbase {
		// TODO - verify this is correct.  You cannot spend outputs that were created in a coinbase transaction
		// until 100 blocks have been mined on top of the block containing the coinbase transaction.
		// Bitcoin has a 100 block coinbase maturity period and the block in which the coinbase transaction is included is block 0.
		// counts as the 1st confirmation, so we need to wait for 99 more blocks to be mined before the coinbase outputs can be spent.
		// So, for instance an output from the coinbase transaction in block 9 can be spent in block 109.
		fields["spendingHeight"] = blockHeight + 100
	}

	for i, input := range tx.Inputs {
		fields[fmt.Sprintf("input:%d", i)] = hex.EncodeToString(input.ExtendedBytes(false))
	}

	utxoCount := 0

	for i, output := range tx.Outputs {
		if output != nil {
			fields[fmt.Sprintf("output:%d", i)] = hex.EncodeToString(output.Bytes())

			// store all coinbases, non-zero utxos and exceptions from pre-genesis
			if utxo.ShouldStoreOutputAsUTXO(isCoinbase, output, blockHeight) {
				fields[fmt.Sprintf("utxo:%d", i)] = utxoHashes[i].String()
				utxoCount++
			}
		}
	}

	fields["nrUtxos"] = utxoCount

	// Store main transaction data only if key doesn't exist
	if err := s.client.Watch(ctx, func(tx *redis_db.Tx) error {
		// Check if key exists
		exists, err := tx.Exists(ctx, txHash.String()).Result()
		if err != nil {
			return err
		}

		if exists > 0 {
			return errors.NewTxExistsError("%v already exists in store", txHash)
		}

		// Start MULTI transaction
		_, err = tx.TxPipelined(ctx, func(pipe redis_db.Pipeliner) error {
			pipe.HSet(ctx, txHash.String(), fields)

			return nil
		})
		return err
	}, txHash.String()); err != nil {
		return nil, errors.NewStorageError("failed to store transaction", err)
	}

	return util.TxMetaDataFromTx(tx)
}
