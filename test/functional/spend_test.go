//go:build e2eTest

package test

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	block_model "github.com/bitcoin-sv/ubsv/model"
	ba "github.com/bitcoin-sv/ubsv/services/blockassembly"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	utxom "github.com/bitcoin-sv/ubsv/services/blockpersister/utxoset/model"
	"github.com/bitcoin-sv/ubsv/services/coinbase"
	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
	"github.com/bitcoin-sv/ubsv/services/miner/cpuminer"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	blockchain_store "github.com/bitcoin-sv/ubsv/stores/blockchain"
	tf "github.com/bitcoin-sv/ubsv/test/test_framework"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/distributor"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bk/wif"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	framework *tf.BitcoinTestFramework
)

func TestMain(m *testing.M) {
	setupBitcoinTestFramework()
	defer tearDownBitcoinTestFramework()

	m.Run()

	// os.Exit(exitCode)
}

func setupBitcoinTestFramework() {
	framework = tf.NewBitcoinTestFramework([]string{"../../docker-compose.yml"})
	m := map[string]string{
		"SETTINGS_CONTEXT_1": "docker.ci.ubsv1.tc1",
		"SETTINGS_CONTEXT_2": "docker.ci.ubsv2.tc1",
		"SETTINGS_CONTEXT_3": "docker.ci.ubsv3.tc1",
	}
	if err := framework.SetupNodes(m); err != nil {
		fmt.Printf("Error setting up nodes: %v\n", err)
		os.Exit(1)
	}
}

func tearDownBitcoinTestFramework() {
	if err := framework.StopNodes(); err != nil {
		fmt.Printf("Error stopping nodes: %v\n", err)
	}
}

func TestShouldAllowFairTx(t *testing.T) {
	ctx := context.Background()
	url := "http://localhost:18090"

	var logLevelStr, _ = gocore.Config().Get("logLevel", "INFO")
	logger := ulogger.New("txblast", ulogger.WithLevel(logLevelStr))

	txDistributor, _ := distributor.NewDistributor(logger,
		distributor.WithBackoffDuration(200*time.Millisecond),
		distributor.WithRetryAttempts(3),
		distributor.WithFailureTolerance(0),
	)

	coinbaseClient, err := coinbase.NewClientWithAddress(ctx, logger, "localhost:18093")
	if err != nil {
		t.Fatalf("Failed to create Coinbase client: %v", err)
	}

	utxoBalanceBefore, _, _ := coinbaseClient.GetBalance(ctx)
	fmt.Printf("utxoBalanceBefore: %d\n", utxoBalanceBefore)

	coinbasePrivKey, _ := gocore.Config().Get("coinbase_wallet_private_key")
	coinbasePrivateKey, err := wif.DecodeWIF(coinbasePrivKey)
	if err != nil {
		t.Fatalf("Failed to decode Coinbase private key: %v", err)
	}

	coinbaseAddr, _ := bscript.NewAddressFromPublicKey(coinbasePrivateKey.PrivKey.PubKey(), true)

	privateKey, err := bec.NewPrivateKey(bec.S256())
	if err != nil {
		t.Fatalf("Failed to generate private key: %v", err)
	}

	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	if err != nil {
		t.Fatalf("Failed to create address: %v", err)
	}

	tx, err := coinbaseClient.RequestFunds(ctx, address.AddressString, true)
	if err != nil {
		t.Fatalf("Failed to request funds: %v", err)
	}

	_, err = txDistributor.SendTransaction(ctx, tx)
	if err != nil {
		t.Fatalf("Failed to send transaction: %v", err)
	}

	output := tx.Outputs[0]
	utxo := &bt.UTXO{
		TxIDHash:      tx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: output.LockingScript,
		Satoshis:      output.Satoshis,
	}

	newTx := bt.NewTx()
	err = newTx.FromUTXOs(utxo)
	if err != nil {
		t.Fatalf("Error adding UTXO to transaction: %s\n", err)
	}

	err = newTx.AddP2PKHOutputFromAddress(coinbaseAddr.AddressString, 10000)
	if err != nil {
		t.Fatalf("Error adding output to transaction: %v", err)
	}

	err = newTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey})
	if err != nil {
		t.Fatalf("Error filling transaction inputs: %v", err)
	}

	_, err = txDistributor.SendTransaction(ctx, newTx)
	if err != nil {
		t.Fatalf("Failed to send new transaction: %v", err)
	}

	fmt.Printf("Transaction sent: %s %s\n", newTx.TxIDChainHash(), newTx.TxID())
	time.Sleep(5 * time.Second)

	height, _ := getBlockHeight(url)
	fmt.Printf("Block height: %d\n", height)

	utxoBalanceAfter, _, _ := coinbaseClient.GetBalance(ctx)
	fmt.Printf("utxoBalanceBefore: %d, utxoBalanceAfter: %d\n", utxoBalanceBefore, utxoBalanceAfter)
	assert.Less(t, utxoBalanceAfter, utxoBalanceBefore)

	baClient := ba.NewClient(ctx, logger)
	miningCandidate, err := baClient.GetMiningCandidate(ctx)
	if err != nil {
		t.Fatalf("Failed to get mining candidate: %v", err)
	}
	fmt.Printf("Mining candidate: %d\n", miningCandidate.SubtreeCount)

	// mine block

	solution, err := cpuminer.Mine(ctx, miningCandidate)
	require.NoError(t, err)

	blockHeader, err := cpuminer.BuildBlockHeader(miningCandidate, solution)
	require.NoError(t, err)

	blockHash := util.Sha256d(blockHeader)
	hashStr := utils.ReverseAndHexEncodeSlice(blockHash)

	target := model.NewNBitFromSlice(miningCandidate.NBits).CalculateTarget()

	var bn = big.NewInt(0)
	bn.SetString(hashStr, 16)

	compare := bn.Cmp(target)
	assert.LessOrEqual(t, compare, 0)

	err = baClient.SubmitMiningSolution(ctx, solution)
	require.NoError(t, err)

	fmt.Printf("Block height: %d\n", height)
	for {
		newHeight, _ := getBlockHeight(url)
		if newHeight > height {
			break
		}
		time.Sleep(1 * time.Second)
	}

	blockchainStoreURL, _, found := gocore.Config().GetURL("blockchain_store.docker.ci.chainintegrity.ubsv1")
	blockchainDB, err := blockchain_store.NewStore(logger, blockchainStoreURL)
	logger.Debugf("blockchainStoreURL: %v", blockchainStoreURL)
	if err != nil {
		panic(err.Error())
	}
	if !found {
		panic("no blockchain_store setting found")
	}
	b, _, _ := blockchainDB.GetBlock(ctx, (*chainhash.Hash)(blockHash))
	fmt.Printf("Block: %v\n", b)

	blockStore := getBlockStore(logger)
	var o []options.Options
	o = append(o, options.WithFileExtension("block"))
	//wait
	time.Sleep(5 * time.Second)
	blockchain, err := blockchain.NewClient(context.Background(), logger)
	header, meta, err := blockchain.GetBestBlockHeader(context.Background())
	fmt.Printf("Best block header: %v\n", header.Hash())
	r, err := blockStore.GetIoReader(context.Background(), header.Hash()[:], o...)
	// t.Errorf("error getting block reader: %v", err)
	if err == nil {
		if bl, err := readFile("block", logger, r, *newTx.TxIDChainHash(), ""); err != nil {
			t.Errorf("error reading block: %v", err)
		} else {
			fmt.Printf("Block at height (%d): was tested for the test Tx\n", meta.Height)
			assert.Equal(t, true, bl, "Test Tx not found in block")
		}
	}

}

func TestShouldNotAllowDoubleSpend(t *testing.T) {
	ctx := context.Background()
	url := "http://localhost:18090"

	var logLevelStr, _ = gocore.Config().Get("logLevel", "INFO")
	logger := ulogger.New("txblast", ulogger.WithLevel(logLevelStr))

	txDistributor, _ := distributor.NewDistributor(logger,
		distributor.WithBackoffDuration(200*time.Millisecond),
		distributor.WithRetryAttempts(3),
		distributor.WithFailureTolerance(0),
	)

	txNode1Distributor, _ := distributor.NewDistributorFromAddress("localhost:18084", logger,
		distributor.WithBackoffDuration(200*time.Millisecond),
		distributor.WithRetryAttempts(3),
		distributor.WithFailureTolerance(0),
	)

	txNode2Distributor, _ := distributor.NewDistributorFromAddress("localhost:28084", logger,
		distributor.WithBackoffDuration(200*time.Millisecond),
		distributor.WithRetryAttempts(3),
		distributor.WithFailureTolerance(0),
	)

	coinbaseClient, err := coinbase.NewClientWithAddress(ctx, logger, "localhost:18093")
	if err != nil {
		t.Fatalf("Failed to create Coinbase client: %v", err)
	}

	utxoBalanceBefore, _, _ := coinbaseClient.GetBalance(ctx)
	fmt.Printf("utxoBalanceBefore: %d\n", utxoBalanceBefore)

	coinbasePrivKey, _ := gocore.Config().Get("coinbase_wallet_private_key")
	coinbasePrivateKey, err := wif.DecodeWIF(coinbasePrivKey)
	if err != nil {
		t.Fatalf("Failed to decode Coinbase private key: %v", err)
	}

	coinbaseAddr, _ := bscript.NewAddressFromPublicKey(coinbasePrivateKey.PrivKey.PubKey(), true)
	coinbaseAddrDouble, _ := bscript.NewAddressFromPublicKey(coinbasePrivateKey.PrivKey.PubKey(), true)

	privateKey, err := bec.NewPrivateKey(bec.S256())
	if err != nil {
		t.Fatalf("Failed to generate private key: %v", err)
	}

	// privateKeyDouble, err := bec.NewPrivateKey(bec.S256())
	// if err != nil {
	// 	t.Fatalf("Failed to generate private key: %v", err)
	// }

	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	if err != nil {
		t.Fatalf("Failed to create address: %v", err)
	}

	tx, err := coinbaseClient.RequestFunds(ctx, address.AddressString, true)
	if err != nil {
		t.Fatalf("Failed to request funds: %v", err)
	}

	_, err = txDistributor.SendTransaction(ctx, tx)
	if err != nil {
		t.Fatalf("Failed to send transaction: %v", err)
	}

	output := tx.Outputs[0]
	utxo := &bt.UTXO{
		TxIDHash:      tx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: output.LockingScript,
		Satoshis:      output.Satoshis,
	}

	newTx := bt.NewTx()
	err = newTx.FromUTXOs(utxo)
	if err != nil {
		t.Fatalf("Error adding UTXO to transaction: %s\n", err)
	}
	newTx.LockTime = 0

	newTxDouble := bt.NewTx()
	err = newTxDouble.FromUTXOs(utxo)
	if err != nil {
		t.Fatalf("Error adding UTXO to transaction: %s\n", err)
	}

	newTxDouble.LockTime = 1

	err = newTx.AddP2PKHOutputFromAddress(coinbaseAddr.AddressString, 10000)
	if err != nil {
		t.Fatalf("Error adding output to transaction: %v", err)
	}

	err = newTxDouble.AddP2PKHOutputFromAddress(coinbaseAddrDouble.AddressString, 10000)
	if err != nil {
		t.Fatalf("Error adding output to transaction: %v", err)
	}

	err = newTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey})
	if err != nil {
		t.Fatalf("Error filling transaction inputs: %v", err)
	}

	err = newTxDouble.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey})
	if err != nil {
		t.Fatalf("Error filling transaction inputs: %v", err)
	}

	_, err = txNode1Distributor.SendTransaction(ctx, newTx)
	if err != nil {
		t.Fatalf("Failed to send new transaction: %v", err)
	}

	_, err = txNode2Distributor.SendTransaction(ctx, newTxDouble)
	if err != nil {
		t.Fatalf("Failed to send new transaction: %v", err)
	}

	fmt.Printf("Transaction sent: %s %s\n", newTx.TxIDChainHash(), newTx.TxID())
	time.Sleep(5 * time.Second)

	fmt.Printf("Double spend Transaction sent: %s %s\n", newTxDouble.TxIDChainHash(), newTxDouble.TxID())
	time.Sleep(5 * time.Second)

	height, _ := getBlockHeight(url)
	fmt.Printf("Block height: %d\n", height)

	utxoBalanceAfter, _, _ := coinbaseClient.GetBalance(ctx)
	fmt.Printf("utxoBalanceBefore: %d, utxoBalanceAfter: %d\n", utxoBalanceBefore, utxoBalanceAfter)
	assert.Less(t, utxoBalanceAfter, utxoBalanceBefore)

	baClientNode1 := ba.NewClientFromAddress("localhost:18085", ctx, logger)
	miningCandidate, err := baClientNode1.GetMiningCandidate(ctx)
	if err != nil {
		t.Fatalf("Failed to get mining candidate: %v", err)
	}
	fmt.Printf("Mining candidate: %d\n", miningCandidate.SubtreeCount)

	baClientNode2 := ba.NewClientFromAddress("localhost:28085", ctx, logger)
	miningCandidate2, err := baClientNode2.GetMiningCandidate(ctx)
	if err != nil {
		t.Fatalf("Failed to get mining candidate: %v", err)
	}
	fmt.Printf("Mining candidate: %d\n", miningCandidate2.SubtreeCount)

	// mine block

	solution, err := cpuminer.Mine(ctx, miningCandidate)
	require.NoError(t, err)

	solution2, err := cpuminer.Mine(ctx, miningCandidate2)
	require.NoError(t, err)

	blockHeader, err := cpuminer.BuildBlockHeader(miningCandidate, solution)
	require.NoError(t, err)

	blockHash := util.Sha256d(blockHeader)
	hashStr := utils.ReverseAndHexEncodeSlice(blockHash)

	target := model.NewNBitFromSlice(miningCandidate.NBits).CalculateTarget()

	var bn = big.NewInt(0)
	bn.SetString(hashStr, 16)

	compare := bn.Cmp(target)
	assert.LessOrEqual(t, compare, 0)

	err = baClientNode1.SubmitMiningSolution(ctx, solution)
	require.NoError(t, err)

	err = baClientNode2.SubmitMiningSolution(ctx, solution2)
	require.NoError(t, err)

	time.Sleep(5 * time.Second)

	miningCandidate, err = baClientNode1.GetMiningCandidate(ctx)
	require.NoError(t, err)
	solution, err = cpuminer.Mine(ctx, miningCandidate)
	require.NoError(t, err)
	err = baClientNode1.SubmitMiningSolution(ctx, solution)
	require.NoError(t, err)

	miningCandidate, err = baClientNode1.GetMiningCandidate(ctx)
	require.NoError(t, err)
	solution, err = cpuminer.Mine(ctx, miningCandidate)
	require.NoError(t, err)
	err = baClientNode1.SubmitMiningSolution(ctx, solution)
	require.NoError(t, err)

	// miningCandidate, err = baClientNode2.GetMiningCandidate(ctx)
	// require.NoError(t, err)
	// solution, err = cpuminer.Mine(ctx, miningCandidate)
	// require.NoError(t, err)
	// err = baClientNode2.SubmitMiningSolution(ctx, solution)
	// require.NoError(t, err)

	time.Sleep(3000 * time.Second)

	fmt.Printf("Block height: %d\n", height)
	for {
		newHeight, _ := getBlockHeight(url)
		if newHeight > height {
			break
		}
		time.Sleep(1 * time.Second)
	}

	blockchainStoreURL, _, found := gocore.Config().GetURL("blockchain_store.docker.ci.chainintegrity.ubsv1")
	blockchainDB, err := blockchain_store.NewStore(logger, blockchainStoreURL)
	logger.Debugf("blockchainStoreURL: %v", blockchainStoreURL)
	if err != nil {
		panic(err.Error())
	}
	if !found {
		panic("no blockchain_store setting found")
	}
	b, _, _ := blockchainDB.GetBlock(ctx, (*chainhash.Hash)(blockHash))
	fmt.Printf("Block: %v\n", b)

	blockStore := getBlockStore(logger)
	var o []options.Options
	o = append(o, options.WithFileExtension("block"))
	//wait
	blockchain, err := blockchain.NewClient(context.Background(), logger)
	header, meta, err := blockchain.GetBestBlockHeader(context.Background())
	fmt.Printf("Best block header: %v\n", header.Hash())
	r, err := blockStore.GetIoReader(context.Background(), header.Hash()[:], o...)
	// t.Errorf("error getting block reader: %v", err)
	if err == nil {
		if bl, err := readFile("block", logger, r, *newTx.TxIDChainHash(), ""); err != nil {
			t.Errorf("error reading block: %v", err)
		} else {
			fmt.Printf("Block at height (%d): was tested for the test Tx\n", meta.Height)
			assert.Equal(t, true, bl, "Test Tx not found in block")
		}

		if bl, err := readFile("block", logger, r, *newTxDouble.TxIDChainHash(), ""); err != nil {
			t.Errorf("error reading block: %v", err)
		} else {
			fmt.Printf("Block at height (%d): was tested for the test Tx\n", meta.Height)
			assert.Equal(t, true, bl, "Test Tx not found in block")
		}
	}

}

func getBlockHeight(url string) (int, error) {
	resp, err := http.Get(url + "/api/v1/lastblocks?n=1")
	if err != nil {
		fmt.Printf("Error getting block height: %s\n", err)
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var blocks []struct {
		Height int `json:"height"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&blocks); err != nil {
		return 0, err
	}

	if len(blocks) == 0 {
		return 0, fmt.Errorf("no blocks found in response")
	}

	return blocks[0].Height, nil
}

func getBlockStore(logger ulogger.Logger) blob.Store {
	blockStoreUrl, err, found := gocore.Config().GetURL("blockstore")
	if err != nil {
		panic(err)
	}
	if !found {
		panic("blockstore config not found")
	}

	blockStore, err := blob.NewStore(logger, blockStoreUrl)
	if err != nil {
		panic(err)
	}

	return blockStore
}

func readFile(ext string, logger ulogger.Logger, r io.Reader, queryTxId chainhash.Hash, dir string) (bool, error) {
	switch ext {
	case "utxodiff":
		utxodiff, err := utxom.NewUTXODiffFromReader(logger, r)
		if err != nil {
			return false, fmt.Errorf("error reading utxodiff: %w\n", err)
		}

		fmt.Printf("UTXODiff block hash: %v\n", utxodiff.BlockHash)

		fmt.Printf("UTXODiff removed %d UTXOs", utxodiff.Removed.Length())

		fmt.Printf("UTXODiff added %d UTXOs", utxodiff.Added.Length())

	case "utxoset":
		utxoSet, err := utxom.NewUTXOSetFromReader(logger, r)
		if err != nil {
			return false, fmt.Errorf("error reading utxoSet: %v\n", err)
		}

		fmt.Printf("UTXOSet block hash: %v\n", utxoSet.BlockHash)

		fmt.Printf("UTXOSet with %d UTXOs", utxoSet.Current.Length())

	case "subtree":
		bl := readSubtree(r, logger, true, queryTxId)
		// fmt.Printf("Number of transactions: %d\n", num)
		return bl, nil

	case "":
		blockHeaderBytes := make([]byte, 80)
		// read the first 80 bytes as the block header
		if _, err := io.ReadFull(r, blockHeaderBytes); err != nil {
			return false, errors.New(errors.ERR_BLOCK_INVALID, "error reading block header", err)
		}

		// read the transaction count
		txCount, err := wire.ReadVarInt(r, 0)
		if err != nil {
			return false, errors.New(errors.ERR_BLOCK_INVALID, "error reading transaction count", err)
		}

		fmt.Printf("\t%d transactions\n", txCount)

	case "block":
		block, err := block_model.NewBlockFromReader(r)
		if err != nil {
			return false, fmt.Errorf("error reading block: %v\n", err)
		}

		fmt.Printf("Block hash: %s\n", block.Hash())
		fmt.Printf("%s", block.Header.StringDump())
		fmt.Printf("Number of transactions: %d\n", block.TransactionCount)

		for _, subtree := range block.Subtrees {
			// fmt.Printf("Subtree %s\n", subtree)

			if true {
				filename := filepath.Join(dir, fmt.Sprintf("%s.subtree", subtree.String()))
				_, _, stReader, err := getReader(filename, logger)
				if err != nil {
					return false, err
				}
				return readSubtree(stReader, logger, true, queryTxId), nil
			}
		}

	default:
		return false, fmt.Errorf("unknown file type")
	}

	return false, nil
}

func readSubtree(r io.Reader, logger ulogger.Logger, verbose bool, queryTxId chainhash.Hash) bool {
	var num uint32

	if err := binary.Read(r, binary.LittleEndian, &num); err != nil {
		fmt.Printf("error reading transaction count: %v\n", err)
		os.Exit(1)
	}

	if verbose {
		for i := uint32(0); i < num; i++ {
			var tx bt.Tx
			_, err := tx.ReadFrom(r)
			if err != nil {
				fmt.Printf("error reading transaction: %v\n", err)
				os.Exit(1)
			}

			// if block_model.IsCoinbasePlaceHolderTx(&tx) {
			// 	fmt.Printf("%10d: Coinbase Placeholder\n", i)
			// } else {
			// 	fmt.Printf("%10d: %v", i, tx.TxIDChainHash())
			// }
			if *tx.TxIDChainHash() == queryTxId {
				fmt.Printf(" (test txid) %v found\n", queryTxId)
				return true
			}

		}
	}
	return false
}

func getReader(path string, logger ulogger.Logger) (string, string, io.Reader, error) {
	dir, file := filepath.Split(path)

	ext := filepath.Ext(file)
	fileWithoutExtension := strings.TrimSuffix(file, ext)

	if ext[0] == '.' {
		ext = ext[1:]
	}

	// if ext == "" {
	// 	usage()
	// }

	hash, err := chainhash.NewHashFromStr(fileWithoutExtension)

	if dir == "" && err == nil {
		store := getBlockStore(logger)

		r, err := store.GetIoReader(context.Background(), hash[:], options.WithFileExtension(ext))
		if err != nil {
			return "", "", nil, fmt.Errorf("error getting reader from store: %w", err)
		}

		return dir, ext, r, nil
	}

	f, err := os.Open(path)
	if err != nil {
		return "", "", nil, fmt.Errorf("error opening file: %v\n", err)
	}

	return dir, ext, f, nil
}

func mineBlock(ctx context.Context, baClient ba.Client, logger ulogger.Logger) error {
	miningCandidate, err := baClient.GetMiningCandidate(ctx)
	if err != nil {
		return fmt.Errorf("error getting mining candidate: %w", err)
	}

	solution, err := cpuminer.Mine(ctx, miningCandidate)
	if err != nil {
		return fmt.Errorf("error mining block: %w", err)
	}

	err = baClient.SubmitMiningSolution(ctx, solution)
	if err != nil {
		return fmt.Errorf("error submitting mining solution: %w", err)
	}

	return nil
}
