// Package subtreeprocessor provides functionality for processing transaction subtrees in Teranode.
//
// This file contains mock implementations of the subtree processor interfaces for testing purposes.
// The mock implementations use the testify/mock framework to provide controllable behavior
// during unit tests, allowing developers to simulate various scenarios and verify
// interactions with the subtree processor without requiring actual processing dependencies.
package subtreeprocessor

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/model"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/mock"
)

// check that MockSubtreeProcessor implements Interface
var _ Interface = (*MockSubtreeProcessor)(nil)

// MockSubtreeProcessor implements a mock version of the Interface for testing.
// This mock provides controllable implementations of all Interface methods,
// allowing tests to define expected behavior, verify method calls, and
// simulate various success and failure scenarios. It uses the testify/mock
// framework to track method calls and return predefined values.
//
// The mock is particularly useful for:
//   - Unit testing components that depend on subtree processor functionality
//   - Integration testing without requiring a full subtree processor
//   - Simulating error conditions and edge cases
//   - Verifying correct interaction patterns with the subtree processor API
//   - Testing blockchain reorganization scenarios
//   - Validating transaction processing workflows
type MockSubtreeProcessor struct {
	mock.Mock
}

func (m *MockSubtreeProcessor) GetCurrentTxMap() TxInpointsMap {
	args := m.Called()
	return args.Get(0).(TxInpointsMap)
}

func (m *MockSubtreeProcessor) GetRemoveMap() txmap.TxMap {
	args := m.Called()
	return args.Get(0).(txmap.TxMap)
}

func (m *MockSubtreeProcessor) GetRemoveMapLength() int {
	args := m.Called()
	return args.Int(0)
}

func (m *MockSubtreeProcessor) GetCurrentRunningState() State {
	args := m.Called()
	return args.Get(0).(State)
}

func (m *MockSubtreeProcessor) GetCurrentLength() int {
	args := m.Called()
	return args.Int(0)
}

func (m *MockSubtreeProcessor) Start(ctx context.Context) {
	m.Called(ctx)
}

func (m *MockSubtreeProcessor) Reset(blockHeader *model.BlockHeader, moveBackBlocks []*model.Block, moveForwardBlocks []*model.Block, isLegacySync bool, postProcess func() error) ResetResponse {
	args := m.Called(blockHeader, moveBackBlocks, moveForwardBlocks, isLegacySync, postProcess)
	return args.Get(0).(ResetResponse)
}

func (m *MockSubtreeProcessor) GetCurrentBlockHeader() *model.BlockHeader {
	args := m.Called()
	return args.Get(0).(*model.BlockHeader)
}

func (m *MockSubtreeProcessor) SetCurrentBlockHeader(blockHeader *model.BlockHeader) {
	m.Called(blockHeader)
}

func (m *MockSubtreeProcessor) GetCurrentSubtree() *subtree.Subtree {
	args := m.Called()
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(*subtree.Subtree)
}

func (m *MockSubtreeProcessor) GetCurrentSubtreeSize() int {
	args := m.Called()
	return args.Get(0).(int)
}

func (m *MockSubtreeProcessor) GetChainedSubtrees() []*subtree.Subtree {
	args := m.Called()
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).([]*subtree.Subtree)
}

func (m *MockSubtreeProcessor) GetSubtreeHashes() []chainhash.Hash {
	args := m.Called()
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).([]chainhash.Hash)
}

func (m *MockSubtreeProcessor) GetTransactionHashes() []chainhash.Hash {
	args := m.Called()
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).([]chainhash.Hash)
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

func (m *MockSubtreeProcessor) GetChainedSubtreesTotalSize() uint64 {
	args := m.Called()
	return args.Get(0).(uint64)
}

// GetPrecomputedMiningData implements Interface.GetPrecomputedMiningData
func (m *MockSubtreeProcessor) GetPrecomputedMiningData() *PrecomputedMiningData {
	args := m.Called()
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(*PrecomputedMiningData)
}

// GetIncompleteSubtreeMiningData implements Interface.GetIncompleteSubtreeMiningData
func (m *MockSubtreeProcessor) GetIncompleteSubtreeMiningData(_ context.Context) *PrecomputedMiningData {
	args := m.Called()
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(*PrecomputedMiningData)
}

// AddBatch implements Interface.AddBatch
func (m *MockSubtreeProcessor) AddBatch(nodes []subtree.Node, txInpoints []*subtree.TxInpoints) {
	m.Called(nodes, txInpoints)
}

func (m *MockSubtreeProcessor) AddDirectly(node *subtree.Node, txInpoints *subtree.TxInpoints, skipNotification bool) error {
	args := m.Called(node, txInpoints, skipNotification)

	if args.Get(0) == nil {
		return nil
	}

	return args.Error(0)
}

// AddNodesDirectly implements Interface.AddNodesDirectly
func (m *MockSubtreeProcessor) AddNodesDirectly(txs []*utxostore.UnminedTransaction, skipNotification bool) error {
	args := m.Called(txs, skipNotification)

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
func (m *MockSubtreeProcessor) Remove(ctx context.Context, hash chainhash.Hash) error {
	args := m.Called(ctx, hash)
	return args.Error(0)
}

// GetCompletedSubtreesForMiningCandidate implements Interface.GetCompletedSubtreesForMiningCandidate
func (m *MockSubtreeProcessor) GetCompletedSubtreesForMiningCandidate() []*subtree.Subtree {
	args := m.Called()
	return args.Get(0).([]*subtree.Subtree)
}

// InitCurrentBlockHeader implements Interface.InitCurrentBlockHeader
func (m *MockSubtreeProcessor) InitCurrentBlockHeader(blockHeader *model.BlockHeader) {
	m.Called(blockHeader)
}

func (m *MockSubtreeProcessor) WaitForPendingBlocks(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

// Stop implements Interface.Stop
func (m *MockSubtreeProcessor) Stop(ctx context.Context) {
	m.Called(ctx)
}
