package subtreeprocessor

import (
	"github.com/bitcoin-sv/teranode/model"
	utxostore "github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/libsv/go-bt/v2/chainhash"
)

type Interface interface {
	Add(node util.SubtreeNode, parents []chainhash.Hash)
	GetCurrentRunningState() State
	GetCurrentLength() int
	CheckSubtreeProcessor() error
	MoveForwardBlock(block *model.Block) error
	Reorg(moveBackBlocks []*model.Block, modeUpBlocks []*model.Block) error
	Reset(blockHeader *model.BlockHeader, moveBackBlocks []*model.Block, moveForwardBlocks []*model.Block, isLegacySync bool) ResetResponse
	Remove(hash chainhash.Hash) error
	GetCompletedSubtreesForMiningCandidate() []*util.Subtree
	GetCurrentBlockHeader() *model.BlockHeader
	SetCurrentBlockHeader(blockHeader *model.BlockHeader)
	GetCurrentSubtree() *util.Subtree
	GetCurrentTxMap() *util.SyncedMap[chainhash.Hash, []chainhash.Hash]
	GetChainedSubtrees() []*util.Subtree
	GetUtxoStore() utxostore.Store
	SetCurrentItemsPerFile(v int)
	TxCount() uint64
	QueueLength() int64
	SubtreeCount() int
	DeDuplicateTransactions()
}
