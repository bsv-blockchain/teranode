package chainintegrity

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/require"
)

// utxoItem represents a spendable UTXO with its transaction and output index
type utxoItem struct {
	tx          *bt.Tx
	outputIndex uint32
}

// Blaster generates transactions continuously for chain integrity testing
type Blaster struct {
	t               *testing.T
	td              *daemon.TestDaemon
	privateKey      *bec.PrivateKey
	numWorkers      int
	txsPerBlock     int
	stopCh          chan struct{}
	wg              sync.WaitGroup
	totalTxCount    atomic.Uint64
	utxoQueue       chan utxoItem
	initialCoinbase *bt.Tx
}

// NewBlaster creates a new transaction blaster with a pre-mined coinbase transaction
func NewBlaster(t *testing.T, td *daemon.TestDaemon, coinbaseTx *bt.Tx, numWorkers int, txsPerBlock int) *Blaster {
	privateKey := td.GetPrivateKey(t)

	return &Blaster{
		t:               t,
		td:              td,
		privateKey:      privateKey,
		numWorkers:      numWorkers,
		txsPerBlock:     txsPerBlock,
		stopCh:          make(chan struct{}),
		utxoQueue:       make(chan utxoItem, 1000), // Buffer for UTXOs
		initialCoinbase: coinbaseTx,
	}
}

// Start begins transaction generation
func (b *Blaster) Start(ctx context.Context) {
	b.t.Logf("Blaster initialized with coinbase: %s", b.initialCoinbase.TxIDChainHash().String())

	// Create initial UTXOs by splitting the coinbase
	b.createInitialUTXOs(ctx)

	// Start worker goroutines
	for i := 0; i < b.numWorkers; i++ {
		b.wg.Add(1)
		go b.worker(ctx, i)
	}
}

// Stop stops transaction generation
func (b *Blaster) Stop() {
	close(b.stopCh)
	b.wg.Wait()
}

// GetTotalTxCount returns the total number of transactions created
func (b *Blaster) GetTotalTxCount() uint64 {
	return b.totalTxCount.Load()
}

// createInitialUTXOs splits the coinbase into multiple UTXOs for parallel spending
func (b *Blaster) createInitialUTXOs(ctx context.Context) {
	// Create a transaction with multiple outputs from the coinbase
	numInitialUTXOs := b.numWorkers * 10 // 10 UTXOs per worker
	amountPerUTXO := uint64(10_000)      // 10,000 satoshis per UTXO

	b.t.Logf("Creating %d initial UTXOs with %d satoshis each...", numInitialUTXOs, amountPerUTXO)

	// Create transaction with multiple outputs from the coinbase
	tx := b.td.CreateTransactionWithOptions(b.t,
		transactions.WithInput(b.initialCoinbase, 0),
		transactions.WithP2PKHOutputs(numInitialUTXOs, amountPerUTXO),
	)

	// Submit the transaction
	err := b.td.PropagationClient.ProcessTransaction(b.td.Ctx, tx)
	require.NoError(b.t, err)

	b.t.Logf("Split transaction submitted: %s", tx.TxIDChainHash().String())

	// Queue all outputs as spendable UTXOs with their specific output index
	for i := 0; i < numInitialUTXOs; i++ {
		select {
		case b.utxoQueue <- utxoItem{tx: tx, outputIndex: uint32(i)}:
		case <-b.stopCh:
			return
		}
	}

	b.t.Logf("Initial UTXOs created and queued")
}

// worker is a goroutine that continuously creates and submits transactions
func (b *Blaster) worker(ctx context.Context, workerID int) {
	defer b.wg.Done()

	for {
		select {
		case <-b.stopCh:
			return
		case <-ctx.Done():
			return
		case utxo := <-b.utxoQueue:
			// Create a child transaction spending from the specific UTXO
			b.createAndSubmitTransaction(ctx, utxo, workerID)
		case <-time.After(100 * time.Millisecond):
			// Timeout to prevent blocking
			continue
		}
	}
}

// createAndSubmitTransaction creates and submits a transaction, then queues the output
func (b *Blaster) createAndSubmitTransaction(ctx context.Context, utxo utxoItem, workerID int) {
	// Get the output amount from the specific UTXO
	if int(utxo.outputIndex) >= len(utxo.tx.Outputs) {
		return
	}

	outputAmount := utxo.tx.Outputs[utxo.outputIndex].Satoshis
	if outputAmount <= 1000 {
		// Not enough for fee
		return
	}

	// Create child transaction
	fee := uint64(500)
	childAmount := outputAmount - fee

	if childAmount < 1000 {
		// Not enough for another transaction
		return
	}

	childTx := b.td.CreateTransactionWithOptions(b.t,
		transactions.WithInput(utxo.tx, utxo.outputIndex),
		transactions.WithP2PKHOutputs(1, childAmount),
	)

	// Submit transaction
	err := b.td.PropagationClient.ProcessTransaction(ctx, childTx)
	if err != nil {
		// Log error but continue (some transactions may fail due to timing)
		b.t.Logf("Worker %d: Failed to submit transaction: %v", workerID, err)
		return
	}

	// Increment counter
	b.totalTxCount.Add(1)

	// Queue the child transaction's output for further spending
	select {
	case b.utxoQueue <- utxoItem{tx: childTx, outputIndex: 0}:
	case <-b.stopCh:
		return
	default:
		// Queue full, skip
	}
}
