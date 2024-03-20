package p2p

import (
	"fmt"
	"sync"

	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
	"github.com/libsv/go-bt/v2/chainhash"
)

type MockPeerHandler struct {
	mu                       sync.RWMutex
	transactionGet           []wire.InvVect
	transactionGetBytes      map[string][]byte
	transactionSent          []wire.MsgTx
	transactionAnnouncements []wire.InvVect
	transactionRejection     []wire.MsgReject
	transaction              []wire.MsgTx
	blockAnnouncements       []wire.InvVect
	block                    []wire.Message
	blockTransactionHashes   [][]*chainhash.Hash
}

func NewMockPeerHandler() *MockPeerHandler {
	return &MockPeerHandler{
		blockTransactionHashes: make([][]*chainhash.Hash, 0),
	}
}

func (m *MockPeerHandler) HandleTransactionGet(msg *wire.InvVect, _ PeerI) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.transactionGet = append(m.transactionGet, *msg)

	bytes, ok := m.transactionGetBytes[msg.Hash.String()]
	if !ok {
		return nil, fmt.Errorf("no bytes for transaction %s", msg.Hash.String())
	}
	return bytes, nil
}

func (m *MockPeerHandler) GetTransactionGet() []wire.InvVect {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.transactionGet
}

func (m *MockPeerHandler) HandleTransactionSent(msg *wire.MsgTx, _ PeerI) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.transactionSent = append(m.transactionSent, *msg)
	return nil
}

func (m *MockPeerHandler) GetTransactionSent() []wire.MsgTx {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.transactionSent
}

func (m *MockPeerHandler) HandleTransactionAnnouncement(msg *wire.InvVect, _ PeerI) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.transactionAnnouncements = append(m.transactionAnnouncements, *msg)
	return nil
}

func (m *MockPeerHandler) GetTransactionAnnouncement() []wire.InvVect {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.transactionAnnouncements
}

func (m *MockPeerHandler) HandleTransactionRejection(rejMsg *wire.MsgReject, _ PeerI) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.transactionRejection = append(m.transactionRejection, *rejMsg)
	return nil
}

func (m *MockPeerHandler) GetTransactionRejection() []wire.MsgReject {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.transactionRejection
}

func (m *MockPeerHandler) HandleTransaction(msg *wire.MsgTx, _ PeerI) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.transaction = append(m.transaction, *msg)
	return nil
}

func (m *MockPeerHandler) GetTransaction() []wire.MsgTx {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.transaction
}

func (m *MockPeerHandler) HandleBlockAnnouncement(msg *wire.InvVect, _ PeerI) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.blockAnnouncements = append(m.blockAnnouncements, *msg)
	return nil
}

func (m *MockPeerHandler) GetBlockAnnouncement() []wire.InvVect {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.blockAnnouncements
}

func (m *MockPeerHandler) HandleBlock(msg wire.Message, _ PeerI) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	blockIdx := len(m.block)
	m.block = append(m.block, msg)
	m.blockTransactionHashes = append(m.blockTransactionHashes, make([]*chainhash.Hash, 0))

	if blockMsg, ok := msg.(*wire.MsgBlock); ok {
		for _, tx := range blockMsg.Transactions {
			ttx := tx.TxHash()
			m.blockTransactionHashes[blockIdx] = append(m.blockTransactionHashes[blockIdx], &ttx)
		}
	}

	return nil
}

func (m *MockPeerHandler) GetBlock() []wire.Message {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.block
}

func (m *MockPeerHandler) GetBlockTransactions(index int) []*chainhash.Hash {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.blockTransactionHashes[index]
}
