package blockassembly

import (
	"context"
	"encoding/binary"
	"fmt"
	"net/http"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
)

// DeepReorgHaltStateKey is the key under which block assembly records that it halted on a reorg
// deeper than the UTXO retention, so a restart stays halted: the byte 0 when it is not halted,
// else 1 followed by the target block's hash, its height and the number of blocks the reorg
// moved back.
const DeepReorgHaltStateKey = "BlockAssemblerDeepReorgHalt"

const deepReorgHaltRecordSize = 1 + chainhash.HashSize + 4 + 4

// deepReorgHalt names the reorg block assembly halted on.
type deepReorgHalt struct {
	target   chainhash.Hash
	height   uint32
	moveBack uint32
}

func (h *deepReorgHalt) bytes() []byte {
	data := make([]byte, deepReorgHaltRecordSize)
	data[0] = 1
	copy(data[1:], h.target[:])
	binary.LittleEndian.PutUint32(data[1+chainhash.HashSize:], h.height)
	binary.LittleEndian.PutUint32(data[1+chainhash.HashSize+4:], h.moveBack)

	return data
}

// parseDeepReorgHalt reads a DeepReorgHaltStateKey record: nil for the byte 0, which is no halt.
func parseDeepReorgHalt(data []byte) (*deepReorgHalt, error) {
	switch {
	case len(data) == 1 && data[0] == 0:
		return nil, nil
	case len(data) == deepReorgHaltRecordSize && data[0] == 1:
		h := &deepReorgHalt{
			height:   binary.LittleEndian.Uint32(data[1+chainhash.HashSize:]),
			moveBack: binary.LittleEndian.Uint32(data[1+chainhash.HashSize+4:]),
		}
		copy(h.target[:], data[1:1+chainhash.HashSize])

		return h, nil
	default:
		return nil, errors.NewProcessingError("[BlockAssembler] state[%s] holds %d bytes, not a deep reorg halt record", DeepReorgHaltStateKey, len(data))
	}
}

// deepReorgNeedsHalt reports whether a reorg that moves back moveBack blocks must halt block
// assembly. A fully spent record is deleted once the tip is the UTXO retention past the block
// that last needed it, so moving back that many blocks can return transactions to the mempool
// whose parents are gone. A retention of 0 means pruning is off and nothing is ever deleted.
func deepReorgNeedsHalt(moveBack int, utxoRetention uint32) bool {
	return utxoRetention > 0 && moveBack >= int(utxoRetention)
}

// DeepReorgHalted reports whether block assembly has halted on a reorg deeper than the UTXO
// retention and waits for an operator reset.
func (b *BlockAssembler) DeepReorgHalted() bool {
	return b.deepReorgHalt.Load() != nil
}

// deepReorgHaltMessage is the operator-facing description of a halt, for the log, the error
// mining callers get and the readiness check.
func (b *BlockAssembler) deepReorgHaltMessage() string {
	h := b.deepReorgHalt.Load()
	if h == nil {
		return ""
	}

	return fmt.Sprintf("block assembly halted: a reorg to %s at height %d moves back %d blocks, at least the UTXO retention of %d, and the transactions it returns to the mempool may spend parents the pruner deleted; run resetblockassembly to reload the mempool from the store and drop what it cannot find",
		h.target, h.height, h.moveBack, b.settings.GetUtxoStoreBlockHeightRetention())
}

// deepReorgHaltedError is the error every refused operation returns while halted.
func (b *BlockAssembler) deepReorgHaltedError() error {
	return errors.NewServiceUnavailableError("[BlockAssembler] %s", b.deepReorgHaltMessage())
}

// haltForDeepReorg stops block assembly before a reset that moves back at least the UTXO
// retention reloads the mempool and quietly loses what the pruner deleted. It records the halt
// so a restart stays halted; a failed record is logged and the halt holds until the restart.
func (b *BlockAssembler) haltForDeepReorg(ctx context.Context, target *model.BlockHeader, height uint32, moveBack, moveForward int) error {
	h := &deepReorgHalt{target: *target.Hash(), height: height, moveBack: uint32(moveBack)} //nolint:gosec // a block count

	b.deepReorgHalt.Store(h)
	prometheusBlockAssemblyDeepReorgHalted.Set(1)

	b.logger.Errorf("[BlockAssembler] MANUAL INTERVENTION REQUIRED: %s (moving forward %d blocks)", b.deepReorgHaltMessage(), moveForward)

	if err := b.blockchainClient.SetState(ctx, DeepReorgHaltStateKey, h.bytes()); err != nil {
		b.logger.Errorf("[BlockAssembler] error recording the deep reorg halt in state[%s], a restart would lift it: %v", DeepReorgHaltStateKey, err)
	}

	return b.deepReorgHaltedError()
}

// loadDeepReorgHalt restores a halt recorded before a restart. A record that cannot be read
// fails the start rather than lift a halt it cannot see.
func (b *BlockAssembler) loadDeepReorgHalt(ctx context.Context) error {
	data, err := b.blockchainClient.GetState(ctx, DeepReorgHaltStateKey)

	switch {
	case err != nil && isStateNotFound(err):
		return nil
	case err != nil:
		return errors.NewStorageError("[BlockAssembler] error reading state[%s]", DeepReorgHaltStateKey, err)
	}

	h, err := parseDeepReorgHalt(data)
	if err != nil {
		return err
	}

	if h == nil {
		return nil
	}

	b.deepReorgHalt.Store(h)
	prometheusBlockAssemblyDeepReorgHalted.Set(1)
	b.logger.Errorf("[BlockAssembler] MANUAL INTERVENTION REQUIRED: still halted from before the restart: %s", b.deepReorgHaltMessage())

	return nil
}

// clearDeepReorgHalt lifts the halt after an operator reset completed. The record is cleared
// first, so a failed write leaves the node halted rather than halted only until a restart.
func (b *BlockAssembler) clearDeepReorgHalt(ctx context.Context) error {
	if b.deepReorgHalt.Load() == nil {
		return nil
	}

	if err := b.blockchainClient.SetState(ctx, DeepReorgHaltStateKey, []byte{0}); err != nil {
		return errors.NewStorageError("[BlockAssembler] error clearing state[%s] after the operator reset; still halted", DeepReorgHaltStateKey, err)
	}

	b.deepReorgHalt.Store(nil)
	prometheusBlockAssemblyDeepReorgHalted.Set(0)
	b.logger.Infof("[BlockAssembler] deep reorg halt lifted by an operator reset")

	return nil
}

// deepReorgHaltHealth is the readiness check that fails while halted. It is never a liveness
// check: a restart does not lift the halt, so failing liveness would only restart in a loop.
func (b *BlockAssembler) deepReorgHaltHealth(_ context.Context, _ bool) (int, string, error) {
	if b.DeepReorgHalted() {
		return http.StatusServiceUnavailable, b.deepReorgHaltMessage(), nil
	}

	return http.StatusOK, "OK", nil
}
