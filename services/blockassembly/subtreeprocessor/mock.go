package subtreeprocessor

import (
	"github.com/bitcoin-sv/teranode/model"
	utxostore "github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/stores/utxo/meta"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/mock"
)

type MockSubtreeProcessor struct {
	mock.Mock
}

func (m *MockSubtreeProcessor) GetCurrentTxMap() *util.SyncedMap[chainhash.Hash, meta.TxInpoints] {
	args := m.Called()
	return args.Get(0).(*util.SyncedMap[chainhash.Hash, meta.TxInpoints])
}

func (m *MockSubtreeProcessor) GetCurrentRunningState() State {
	args := m.Called()
	return args.Get(0).(State)
}

func (m *MockSubtreeProcessor) GetCurrentLength() int {
	args := m.Called()
	return args.Int(0)
}

func (m *MockSubtreeProcessor) Reset(blockHeader *model.BlockHeader, moveBackBlocks []*model.Block, moveForwardBlocks []*model.Block, isLegacySync bool) ResetResponse {
	args := m.Called(blockHeader, moveBackBlocks, moveForwardBlocks, isLegacySync)
	return args.Get(0).(ResetResponse)
}

func (m *MockSubtreeProcessor) GetCurrentBlockHeader() *model.BlockHeader {
	args := m.Called()
	return args.Get(0).(*model.BlockHeader)
}

func (m *MockSubtreeProcessor) GetCurrentSubtree() *util.Subtree {
	args := m.Called()
	return args.Get(0).(*util.Subtree)
}

func (m *MockSubtreeProcessor) GetChainedSubtrees() []*util.Subtree {
	args := m.Called()
	return args.Get(0).([]*util.Subtree)
}

func (m *MockSubtreeProcessor) GetUtxoStore() utxostore.Store {
	args := m.Called()
	return args.Get(0).(utxostore.Store)
}

func (m *MockSubtreeProcessor) SetCurrentItemsPerFile(v int) {
	m.Called(v)
}

func (m *MockSubtreeProcessor) TxCount() uint64 {
	args := m.Called()
	return args.Get(0).(uint64)
}

func (m *MockSubtreeProcessor) QueueLength() int64 {
	args := m.Called()
	return args.Get(0).(int64)
}

func (m *MockSubtreeProcessor) SubtreeCount() int {
	args := m.Called()
	return args.Int(0)
}

// Add implements Interface.Add
func (m *MockSubtreeProcessor) Add(node util.SubtreeNode, txInpoints meta.TxInpoints) {
	m.Called(node, txInpoints)
}

func (m *MockSubtreeProcessor) AddDirectly(node util.SubtreeNode, txInpoints meta.TxInpoints) error {
	args := m.Called(node, txInpoints)

	if args.Get(0) == nil {
		return nil
	}

	return args.Error(0)
}

// CheckSubtreeProcessor implements Interface.CheckSubtreeProcessor
func (m *MockSubtreeProcessor) CheckSubtreeProcessor() error {
	args := m.Called()
	return args.Error(0)
}

// MoveForwardBlock implements Interface.MoveForwardBlock
func (m *MockSubtreeProcessor) MoveForwardBlock(block *model.Block) error {
	args := m.Called(block)
	return args.Error(0)
}

// Reorg implements Interface.Reorg
func (m *MockSubtreeProcessor) Reorg(moveBackBlocks []*model.Block, modeUpBlocks []*model.Block) error {
	args := m.Called(moveBackBlocks, modeUpBlocks)
	return args.Error(0)
}

// Remove implements Interface.Remove
func (m *MockSubtreeProcessor) Remove(hash chainhash.Hash) error {
	args := m.Called(hash)
	return args.Error(0)
}

// GetCompletedSubtreesForMiningCandidate implements Interface.GetCompletedSubtreesForMiningCandidate
func (m *MockSubtreeProcessor) GetCompletedSubtreesForMiningCandidate() []*util.Subtree {
	args := m.Called()
	return args.Get(0).([]*util.Subtree)
}

// SetCurrentBlockHeader implements Interface.SetCurrentBlockHeader
func (m *MockSubtreeProcessor) SetCurrentBlockHeader(blockHeader *model.BlockHeader) {
	m.Called(blockHeader)
}

// DeDuplicateTransactions implements Interface.DeDuplicateTransactions
func (m *MockSubtreeProcessor) DeDuplicateTransactions() {
	m.Called()
}
