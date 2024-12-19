package coinbase

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

type TestSubtrees struct {
	TotalFees     uint64
	SubtreeHashes []*chainhash.Hash
	Subtrees      []*util.Subtree
}

// MalformedUTXOConfig specifies how to generate malformed UTXOs during the splitting phase
type MalformedUTXOConfig struct {
	// Percentage of UTXOs that should be malformed (0-100)
	Percentage int
	// Type of malformation to apply
	Type MalformationType
}

// MalformationType specifies how to malform the UTXO
type MalformationType int

// NOTE:
// We are not able to modify the locking script to be invalid, as this is deemed invalid when genereating transaction in splitUtxo
// So following malformations are not possible:
// 1. EmptyLockingScript
// 2. InvalidLockingScript
// 3. OversizedLockingScript
// 4. NonStandardScript
// We are also not able to make satoshis amount negative.

const (
	// ZeroSatoshis sets the satoshi amount to 0
	ZeroSatoshis MalformationType = iota
)

// MalformOutput applies the specified malformation to a transaction output
func MalformOutput(output *bt.Output, malformType MalformationType) {
	if malformType == ZeroSatoshis {
		output.Satoshis = 0
	}
}

// func GenerateTestBlock(noOfTxs uint64, subtreeSize int, numberOfSpendableOutputs uint64, hashOfPreviousBlock *chainhash.Hash) (*model.Block, error) {
func GenerateTestBlock(blockHeight uint32, noOfTxs uint64, subtreeSize int, numberOfSpendableOutputs uint64, hashOfPreviousBlock *chainhash.Hash) (*model.Block, error) {
	testSubtrees, err := GenerateTestSubtrees(noOfTxs, subtreeSize)
	if err != nil {
		return nil, err
	}

	coinbaseTx := bt.NewTx()

	blockHeightBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(blockHeightBytes, blockHeight)

	arbitraryData := []byte{}
	arbitraryData = append(arbitraryData, 0x03)
	arbitraryData = append(arbitraryData, blockHeightBytes[:3]...)
	arbitraryData = append(arbitraryData, []byte("/Test miner/")...)

	err = coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0)
	if err != nil {
		return nil, err
	}

	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes(arbitraryData)
	coinbaseTx.Inputs[0].SequenceNumber = 0

	totalSatoshis := numberOfSpendableOutputs * 10_000_000

	err = coinbaseTx.AddP2PKHOutputFromAddress("1Jp7AZdMQ3hyfMfk3kJe31TDj8oppZLYdK", totalSatoshis)
	if err != nil {
		return nil, err
	}

	nBits, _ := model.NewNBitFromString("2000ffff")

	var merkleRootsubtreeHashes []*chainhash.Hash

	for i := 0; i < len(testSubtrees.Subtrees); i++ {
		if i == 0 {
			// Safe conversion approach
			txSize := coinbaseTx.Size()
			if txSize < 0 {
				return nil, errors.NewProcessingError("invalid transaction size")
			}

			size := uint64(txSize)

			testSubtrees.Subtrees[i].ReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, size)

			rootHash := testSubtrees.Subtrees[i].RootHash()
			merkleRootsubtreeHashes = append(merkleRootsubtreeHashes, rootHash)
		} else {
			merkleRootsubtreeHashes = append(merkleRootsubtreeHashes, testSubtrees.SubtreeHashes[i])
		}
	}

	calculatedMerkleRootHash, err := calculateMerkleRoot(merkleRootsubtreeHashes)
	if err != nil {
		return nil, err
	}

	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  hashOfPreviousBlock,
		HashMerkleRoot: calculatedMerkleRootHash,
		Timestamp:      uint32(time.Now().Unix()), // #nosec G115
		Bits:           *nBits,
		Nonce:          0,
	}

	for {
		if ok, _, _ := blockHeader.HasMetTargetDifficulty(); ok {
			break
		}

		blockHeader.Nonce++

		if blockHeader.Nonce%1000000 == 0 {
			fmt.Printf("mining Nonce: %d, hash: %s\n", blockHeader.Nonce, blockHeader.Hash().String())
		}
	}

	block := &model.Block{
		Header:           blockHeader,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: noOfTxs,
		SizeInBytes:      123123,
		Subtrees:         testSubtrees.SubtreeHashes,
	}

	return block, nil
}

func GenerateTestSubtrees(noOfTransactions uint64, subtreeSize int) (*TestSubtrees, error) {
	subtree, err := util.NewTreeByLeafCount(subtreeSize)
	if err != nil {
		return nil, err
	}

	_ = subtree.AddCoinbaseNode()

	subtreeCount := 0
	subtreeHashes := make([]*chainhash.Hash, 0)
	subtrees := make([]*util.Subtree, 0)

	txID := make([]byte, 32)

	var hash chainhash.Hash

	fees := uint64(0)

	maxTx := int(math.Min(float64(noOfTransactions), float64(math.MaxInt)))
	for i := 1; i < maxTx; i++ {
		safeI := uint64(i) // #nosec G115 -- i is bounded by maxTx which is already safely converted
		binary.LittleEndian.PutUint64(txID, safeI)
		hash = chainhash.Hash(txID)

		if err = subtree.AddNode(hash, safeI, safeI); err != nil {
			return nil, err
		}

		fees += safeI

		if subtree.IsComplete() {
			clonedSubtree := subtree.Duplicate()
			if clonedSubtree == nil {
				return nil, errors.NewProcessingError("failed to clone subtree")
			}

			subtreeHashes = append(subtreeHashes, clonedSubtree.RootHash())
			subtrees = append(subtrees, clonedSubtree)

			subtreeCount++

			subtree, err = util.NewTreeByLeafCount(subtreeSize)
			if err != nil {
				return nil, err
			}
		}
	}

	if subtree.Length() > 0 {
		subtreeHashes = append(subtreeHashes, subtree.RootHash())
		subtrees = append(subtrees, subtree)
	}

	return &TestSubtrees{
		TotalFees:     fees,
		SubtreeHashes: subtreeHashes,
		Subtrees:      subtrees,
	}, nil
}

func calculateMerkleRoot(hashes []*chainhash.Hash) (*chainhash.Hash, error) {
	if len(hashes) == 0 {
		return nil, nil
	}

	if len(hashes) == 1 {
		return hashes[0], nil
	}

	st, err := util.NewTreeByLeafCount(util.CeilPowerOfTwo(len(hashes)))
	if err != nil {
		return nil, err
	}

	for _, hash := range hashes {
		if err := st.AddNode(*hash, 1, 0); err != nil {
			return nil, err
		}
	}

	calculatedMerkleRoot := st.RootHash()

	return chainhash.NewHash(calculatedMerkleRoot[:])
}

func SetupPostgresContainer() (string, func() error, error) {
	ctx := context.Background()

	dbName := "testdb"
	dbUser := "postgres"
	dbPassword := "password"

	postgresC, err := postgres.Run(ctx,
		"docker.io/postgres:16-alpine",
		postgres.WithDatabase(dbName),
		postgres.WithUsername(dbUser),
		postgres.WithPassword(dbPassword),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2),
			wait.ForListeningPort("5432/tcp")),
	)
	if err != nil {
		return "", nil, err
	}

	host, err := postgresC.Host(ctx)
	if err != nil {
		return "", nil, err
	}

	port, err := postgresC.MappedPort(ctx, "5432")
	if err != nil {
		return "", nil, err
	}

	connStr := fmt.Sprintf("postgres://postgres:password@%s:%s/testdb?sslmode=disable", host, port.Port())

	teardown := func() error {
		return postgresC.Terminate(ctx)
	}

	return connStr, teardown, nil
}

// GenerateInsertValidBlocksToCoinbase generates and inserts a sequence of valid blocks into the coinbase service
func GenerateInsertValidBlocksToCoinbase(ctx context.Context, c *Coinbase, numBlocks int) error {
	genesisBlock, err := c.store.GetBlockByHeight(ctx, 0)
	if err != nil {
		return errors.NewProcessingError("failed to get genesis block: %v", err)
	}

	prevBlockHash := genesisBlock.Hash()

	for i := 0; i < numBlocks; i++ {
		// Generate a block with 2048 transactions, 1024 subtree size, and 500 spendable outputs
		block, err := GenerateTestBlock(uint32(i), 2048, 1024, 500, prevBlockHash) // #nosec G115 -- i is bounded by numBlocks
		if err != nil {
			return errors.NewProcessingError("failed to generate test block %d: %v", i+1, err)
		}

		// Store the block in coinbase
		if err := c.storeBlock(ctx, block); err != nil {
			return errors.NewProcessingError("failed to store block %d: %v", i+1, err)
		}

		// Update previous block hash for next iteration
		prevBlockHash = block.Hash()
	}

	return nil
}

// TrySpendUTXOs attempts to spend the given UTXOs in transactions
// Returns an error if spending fails, nil if successful
func TrySpendUTXOs(tx *bt.Tx, privateKey *bec.PrivateKey) error {
	// Create a new transaction
	spendingTx := bt.NewTx()

	// Add all outputs from the input tx as inputs to our spending tx
	for i, output := range tx.Outputs {
		// Create UTXO from the output
		utxo := &bt.UTXO{
			TxIDHash:      tx.TxIDChainHash(),
			Vout:          uint32(i), //nolint:gosec // G115: integer overflow conversion int -> uint32
			LockingScript: output.LockingScript,
			Satoshis:      output.Satoshis,
		}

		if err := spendingTx.FromUTXOs(utxo); err != nil {
			return errors.NewProcessingError("failed to add UTXO to transaction: %v", err)
		}
	}

	// Add a simple P2PKH output to spend to
	if err := spendingTx.AddP2PKHOutputFromAddress("1Jp7AZdMQ3hyfMfk3kJe31TDj8oppZLYdK", tx.TotalOutputSatoshis()); err != nil {
		return errors.NewProcessingError("failed to add output to transaction: %v", err)
	}

	// Try to fill all inputs (sign the transaction)
	unlockerGetter := unlocker.Getter{PrivateKey: privateKey}
	if err := spendingTx.FillAllInputs(context.Background(), &unlockerGetter); err != nil {
		return errors.NewProcessingError("failed to fill inputs: %v", err)
	}

	return nil
}
