//go:build test_smoke_rpc || test_rpc || debug

package smoke

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/daemon"
	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bk/wif"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSendTxAndCheckState(t *testing.T) {
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		SettingsContext: "dev.system.test",
	})

	defer td.Stop(t)

	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t)

	newTx := td.CreateTransactionWithOptions(t,
		daemon.WithInput(coinbaseTx, 0),
		daemon.WithP2PKHOutputs(1, 10000),
	)

	// t.Logf("Sending New Transaction with RPC: %s\n", newTx.TxIDChainHash())
	txBytes := hex.EncodeToString(newTx.ExtendedBytes())

	_, err := td.CallRPC("sendrawtransaction", []any{txBytes})
	require.NoError(t, err, "Failed to send new tx with rpc")
	// t.Logf("Transaction sent with RPC: %s\n", resp)

	// Wait for transaction to be processed
	delay := td.Settings.BlockAssembly.DoubleSpendWindow
	if delay != 0 {
		// t.Logf("Waiting %dms [block assembly has delay processing txs to catch double spends]\n", delay)
		time.Sleep(delay * time.Millisecond)
	}

	time.Sleep(250 * time.Millisecond) // Make absolutely sure block assembly has processed the tx

	block := td.MineAndWait(t, 1)

	err = block.GetAndValidateSubtrees(td.Ctx, td.Logger, td.SubtreeStore, nil)
	require.NoError(t, err)

	err = block.CheckMerkleRoot(td.Ctx)
	require.NoError(t, err)

	fallbackGetFunc := func(subtreeHash chainhash.Hash) error {
		return block.SubTreesFromBytes(subtreeHash[:])
	}

	subtree, err := block.GetSubtrees(td.Ctx, td.Logger, td.SubtreeStore, fallbackGetFunc)
	require.NoError(t, err)

	blFound := false
	for i := 0; i < len(subtree); i++ {
		st := subtree[i]
		for _, node := range st.Nodes {
			// t.Logf("node.Hash: %s", node.Hash.String())
			// t.Logf("tx.TxIDChainHash().String(): %s", newTx.TxIDChainHash().String())

			if node.Hash.String() == newTx.TxIDChainHash().String() {
				blFound = true
				break
			}
		}
	}

	assert.True(t, blFound, "TX not found in the blockstore")

	resp, err := td.CallRPC("getblockchaininfo", []any{})
	require.NoError(t, err)

	var blockchainInfo helper.BlockchainInfo
	errJSON := json.Unmarshal([]byte(resp), &blockchainInfo)
	require.NoError(t, errJSON)

	td.LogJSON(t, "blockchainInfo", blockchainInfo)
	assert.Equal(t, int(td.Settings.ChainCfgParams.CoinbaseMaturity+2), blockchainInfo.Result.Blocks)
	assert.Equal(t, block.Hash().String(), blockchainInfo.Result.BestBlockHash)
	assert.Equal(t, "regtest", blockchainInfo.Result.Chain)
	assert.Equal(t, "0800000000000000000000000000000000000000000000000000000000000000", blockchainInfo.Result.Chainwork)
	assert.Equal(t, string("4.6565423739069247246592908691514574469873245939403288293276821765520783128832e-10"), blockchainInfo.Result.Difficulty)
	assert.Equal(t, int(863341), blockchainInfo.Result.Headers)
	assert.Equal(t, int(0), blockchainInfo.Result.Mediantime)
	assert.False(t, blockchainInfo.Result.Pruned)
	assert.Empty(t, blockchainInfo.Result.Softforks)
	assert.Equal(t, float64(0), blockchainInfo.Result.VerificationProgress)
	assert.Nil(t, blockchainInfo.Error)
	assert.Nil(t, blockchainInfo.ID)

	resp, err = td.CallRPC("getinfo", []any{})
	require.NoError(t, err)

	var getInfo helper.GetInfo
	errJSON = json.Unmarshal([]byte(resp), &getInfo)
	require.NoError(t, errJSON)
	require.NotNil(t, getInfo.Result)

	td.LogJSON(t, "getInfo", getInfo)

	assert.Equal(t, int(td.Settings.ChainCfgParams.CoinbaseMaturity+2), getInfo.Result.Blocks)
	assert.Equal(t, int(0), getInfo.Result.Connections)
	assert.Equal(t, float64(4.6565423739069247e-10), getInfo.Result.Difficulty)
	assert.Equal(t, int(70016), getInfo.Result.ProtocolVersion)
	assert.Equal(t, "", getInfo.Result.Proxy)
	assert.Equal(t, float64(0), getInfo.Result.RelayFee)
	assert.False(t, getInfo.Result.Stn)
	assert.False(t, getInfo.Result.TestNet)
	assert.Equal(t, int(0), getInfo.Result.TimeOffset)
	assert.Equal(t, int(1), getInfo.Result.Version)
	assert.Nil(t, getInfo.Error)
	assert.Equal(t, int(0), getInfo.ID)

	var getDifficulty helper.GetDifficultyResponse

	resp, err = td.CallRPC("getdifficulty", []any{})

	require.NoError(t, err)

	errJSON = json.Unmarshal([]byte(resp), &getDifficulty)
	require.NoError(t, errJSON)

	// t.Logf("getDifficulty: %+v", getDifficulty)
	assert.Equal(t, float64(4.6565423739069247e-10), getDifficulty.Result)

	resp, err = td.CallRPC("getblockhash", []any{td.Settings.ChainCfgParams.CoinbaseMaturity + 2})
	require.NoError(t, err, "Failed to get block hash")

	var getBlockHash helper.GetBlockHashResponse
	errJSON = json.Unmarshal([]byte(resp), &getBlockHash)
	require.NoError(t, errJSON)

	// t.Logf("getBlockHash: %+v", getBlockHash)
	assert.Equal(t, block.Hash().String(), getBlockHash.Result)

	td.LogJSON(t, "getBlockHash", getBlockHash)

	resp, err = td.CallRPC("getblockbyheight", []any{td.Settings.ChainCfgParams.CoinbaseMaturity + 2})
	require.NoError(t, err, "Failed to get block by height")

	var getBlockByHeightResp helper.GetBlockByHeightResponse
	errJSON = json.Unmarshal([]byte(resp), &getBlockByHeightResp)
	require.NoError(t, errJSON)

	td.LogJSON(t, "getBlockByHeightResp", getBlockByHeightResp)

	penultimateBlock, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, getBlockByHeightResp.Result.Height-1)
	require.NoError(t, err)

	// Assert block properties
	assert.Equal(t, block.Hash().String(), getBlockByHeightResp.Result.Hash)
	assert.Equal(t, 1, getBlockByHeightResp.Result.Confirmations)
	// assert.Equal(t, 229, getBlockByHeightResp.Result.Size)
	assert.Equal(t, uint32(td.Settings.ChainCfgParams.CoinbaseMaturity+2), getBlockByHeightResp.Result.Height)
	assert.Equal(t, 536870912, getBlockByHeightResp.Result.Version)
	assert.Equal(t, "20000000", getBlockByHeightResp.Result.VersionHex)
	assert.Equal(t, block.Header.HashMerkleRoot.String(), getBlockByHeightResp.Result.Merkleroot)
	assert.Equal(t, "207fffff", getBlockByHeightResp.Result.Bits)
	assert.InDelta(t, 4.6565423739069247e-10, getBlockByHeightResp.Result.Difficulty, 1e-20)
	assert.Equal(t, penultimateBlock.Hash().String(), getBlockByHeightResp.Result.Previousblockhash)
	assert.Equal(t, "", getBlockByHeightResp.Result.Nextblockhash)
}

func TestShouldNotProcessNonFinalTx(t *testing.T) {
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		SettingsContext: "dev.system.test",
	})

	defer td.Stop(t)

	tSettings := td.Settings

	// set run state
	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	height, _, err := td.BlockchainClient.GetBestHeightAndTime(td.Ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(0), height)

	// Generate initial blocks
	// CSVHeight is the block height at which the CSV rules are activated including lock time
	_, err = td.CallRPC("generate", []any{tSettings.ChainCfgParams.CSVHeight + 1})
	require.NoError(t, err)

	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)

	coinbaseTx := block1.CoinbaseTx

	coinbasePrivKey := tSettings.BlockAssembly.MinerWalletPrivateKeys[0]
	coinbasePrivateKey, err := wif.DecodeWIF(coinbasePrivKey)
	require.NoError(t, err)

	_, err = bscript.NewAddressFromPublicKey(coinbasePrivateKey.PrivKey.PubKey(), true)
	require.NoError(t, err)

	privateKey, err := bec.NewPrivateKey(bec.S256())
	require.NoError(t, err)

	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	require.NoError(t, err)

	output := coinbaseTx.Outputs[0]
	utxo := &bt.UTXO{
		TxIDHash:      coinbaseTx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: output.LockingScript,
		Satoshis:      output.Satoshis,
	}

	newTx := bt.NewTx()
	err = newTx.FromUTXOs(utxo)
	require.NoError(t, err)

	err = newTx.AddP2PKHOutputFromAddress(address.AddressString, 10000)
	require.NoError(t, err)

	err = newTx.FillAllInputs(td.Ctx, &unlocker.Getter{PrivateKey: coinbasePrivateKey.PrivKey})
	require.NoError(t, err)

	// When a transaction's nLockTime is set (e.g., 500 for block height),
	// nSequence must be less than 0xffffffff for the locktime to be enforced.
	// Otherwise, the locktime is ignored.
	newTx.Inputs[0].SequenceNumber = 0x10000005
	newTx.LockTime = tSettings.ChainCfgParams.CSVHeight + 123

	t.Logf("Sending New Transaction with RPC: %s\n", newTx.TxIDChainHash())
	txBytes := hex.EncodeToString(newTx.ExtendedBytes())

	resp, err := td.CallRPC("sendrawtransaction", []any{txBytes})
	// "code: -8, message: TX rejected:
	// PROCESSING (4): error sending transaction 1b36fab6c9342373f0c45770f5e46f963e6d70a3f42a5e8fce003b03ed27f631 to 100.00% of the propagation servers: [SERVICE_ERROR (49): address localhost:8084
	//  -> UNKNOWN (0): SERVICE_ERROR (49): [ProcessTransaction][1b36fab6c9342373f0c45770f5e46f963e6d70a3f42a5e8fce003b03ed27f631] failed to validate transaction
	//  -> UTXO_NON_FINAL (61): [Validate][1b36fab6c9342373f0c45770f5e46f963e6d70a3f42a5e8fce003b03ed27f631] transaction is not final
	//  -> TX_LOCK_TIME (35): lock time (699) as block height is greater than block height (578)]"
	require.Error(t, err, "Failed to send new tx with rpc")
	require.Contains(t, err.Error(), "transaction is not final")
	require.Contains(t, err.Error(), "TX_LOCK_TIME")

	t.Logf("Transaction sent with RPC: %s\n", resp)
}

func TestShouldRejectOversizedTx(t *testing.T) {
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		SettingsContext: "dev.system.test.txsizetest",
	})

	defer td.Stop(t)

	tSettings := td.Settings

	// set run state
	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// Generate initial blocks to get coinbase funds
	_, err = td.CallRPC("generate", []any{101})
	require.NoError(t, err)

	// Get the policy settings to know the max tx size
	maxTxSize := td.Settings.Policy.MaxTxSizePolicy

	// Create a transaction that exceeds MaxTxSizePolicy by adding many outputs
	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)
	block101, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 101)
	require.NoError(t, err)

	coinbaseTx := block1.CoinbaseTx

	coinbasePrivKey := tSettings.BlockAssembly.MinerWalletPrivateKeys[0]
	coinbasePrivateKey, err := wif.DecodeWIF(coinbasePrivKey)
	require.NoError(t, err)

	_, err = bscript.NewAddressFromPublicKey(coinbasePrivateKey.PrivKey.PubKey(), true)
	require.NoError(t, err)

	privateKey, err := bec.NewPrivateKey(bec.S256())
	require.NoError(t, err)

	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	require.NoError(t, err)

	output := coinbaseTx.Outputs[0]
	utxo := &bt.UTXO{
		TxIDHash:      coinbaseTx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: output.LockingScript,
		Satoshis:      output.Satoshis,
	}

	newTx := bt.NewTx()
	err = newTx.FromUTXOs(utxo)
	require.NoError(t, err)

	// Add many outputs to make the transaction exceed MaxTxSizePolicy
	// Each P2PKH output is roughly 34 bytes for the locking script
	// Plus 8 bytes for the satoshi amount
	// So each output is roughly 42 bytes
	// We'll create enough outputs to exceed MaxTxSizePolicy
	numOutputs := (maxTxSize / 34) + 1000                     // Add extra outputs to ensure we exceed the limit
	satoshisPerOutput := output.Satoshis / uint64(numOutputs) //nolint:gosec

	t.Logf("Creating transaction with %d outputs to exceed MaxTxSizePolicy of %d bytes", numOutputs, maxTxSize)

	for i := 0; i < numOutputs; i++ {
		err = newTx.AddP2PKHOutputFromAddress(address.AddressString, satoshisPerOutput)
		require.NoError(t, err)
	}

	err = newTx.FillAllInputs(td.Ctx, &unlocker.Getter{PrivateKey: coinbasePrivateKey.PrivKey})
	require.NoError(t, err)

	t.Logf("Created transaction with size: %d bytes", len(newTx.ExtendedBytes()))
	t.Logf("MaxTxSizePolicy: %d bytes", maxTxSize)

	// Try to send the oversized transaction
	txBytes := hex.EncodeToString(newTx.ExtendedBytes())
	_, err = td.CallRPC("sendrawtransaction", []interface{}{txBytes})

	// The transaction should be rejected for being too large
	require.Error(t, err, "Expected transaction to be rejected for exceeding MaxTxSizePolicy")
	require.Contains(t, err.Error(), "transaction size in bytes is greater than max tx size policy", "Expected error message to indicate transaction size policy violation")

	// now try add a block with the transaction
	_, block102 := td.CreateTestBlock(t, block101, 10101, newTx)
	err = td.BlockValidationClient.ProcessBlock(td.Ctx, block102, block102.Height)
	// TODO should this be an error?
	require.NoError(t, err)
}

func TestShouldRejectOversizedScript(t *testing.T) {
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		SettingsContext: "dev.system.test.oversizedscripttest",
	})

	defer td.Stop(t)

	// set run state
	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// Generate initial blocks to get coinbase funds
	_, err = td.CallRPC("generate", []interface{}{101})
	require.NoError(t, err)

	// Get the policy settings to know the max script size
	maxScriptSize := td.Settings.Policy.MaxScriptSizePolicy

	// Create a transaction with an oversized script
	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)
	block101, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 101)
	require.NoError(t, err)

	coinbaseTx := block1.CoinbaseTx
	output := coinbaseTx.Outputs[0]

	coinbasePrivKey := td.Settings.BlockAssembly.MinerWalletPrivateKeys[0]
	coinbasePrivateKey, err := wif.DecodeWIF(coinbasePrivKey)
	require.NoError(t, err)

	utxo := &bt.UTXO{
		TxIDHash:      coinbaseTx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: output.LockingScript,
		Satoshis:      output.Satoshis,
	}

	newTx := bt.NewTx()
	err = newTx.FromUTXOs(utxo)
	require.NoError(t, err)

	// Create an oversized OP_RETURN script
	// We'll create a script that's larger than MaxScriptSizePolicy
	oversizedData := make([]byte, maxScriptSize+1000) // Add extra bytes to ensure we exceed the limit
	for i := range oversizedData {
		oversizedData[i] = byte(i % 256) // Fill with some pattern
	}

	// Create the oversized script using OP_RETURN
	err = newTx.AddOpReturnOutput(oversizedData)
	require.NoError(t, err)

	// Add a normal P2PKH output to spend the rest of the coins
	addr, err := bscript.NewAddressFromPublicKey(coinbasePrivateKey.PrivKey.PubKey(), true)
	require.NoError(t, err)
	err = newTx.AddP2PKHOutputFromAddress(addr.AddressString, 10000) // Leave some for fees
	require.NoError(t, err)
	err = newTx.FillAllInputs(td.Ctx, &unlocker.Getter{PrivateKey: coinbasePrivateKey.PrivKey})
	require.NoError(t, err)

	t.Logf("Created transaction with OP_RETURN data size: %d bytes", len(oversizedData))
	t.Logf("MaxScriptSizePolicy: %d bytes", maxScriptSize)
	padding := bytes.Repeat([]byte{0x00}, maxScriptSize)
	err = newTx.Inputs[0].UnlockingScript.AppendPushDataString(string(padding))
	require.NoError(t, err)

	// Try to send the transaction with oversized script
	txBytes := hex.EncodeToString(newTx.ExtendedBytes())
	_, err = td.CallRPC("sendrawtransaction", []interface{}{txBytes})

	// The transaction should be rejected for having a script that's too large
	require.Error(t, err, "Expected transaction to be rejected for exceeding MaxScriptSizePolicy")
	require.Contains(t, err.Error(), "Script is too big", "Expected error message to indicate script size violation")

	// now try add a block with the transaction
	_, block102 := td.CreateTestBlock(t, block101, 10101, newTx)
	err = td.BlockValidationClient.ProcessBlock(td.Ctx, block102, block102.Height)
	require.Error(t, err)
}

func TestShouldAllowChainedTransactionsUseRpc(t *testing.T) {
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		SettingsContext: "dev.system.test",
	})

	defer td.Stop(t)

	// set run state
	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// Generate initial blocks
	_, err = td.CallRPC("generate", []any{101})
	require.NoError(t, err)

	tSettings := td.Settings

	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)

	// Get the coinbase transaction from block 1
	coinbaseTx := block1.CoinbaseTx
	coinbasePrivKey := tSettings.BlockAssembly.MinerWalletPrivateKeys[0]
	coinbasePrivateKey, err := wif.DecodeWIF(coinbasePrivKey)
	require.NoError(t, err)

	// Create first recipient's key pair
	privateKey1, err := bec.NewPrivateKey(bec.S256())
	require.NoError(t, err)
	address1, err := bscript.NewAddressFromPublicKey(privateKey1.PubKey(), true)
	require.NoError(t, err)

	// Create UTXO from coinbase
	output := coinbaseTx.Outputs[0]
	utxo := &bt.UTXO{
		TxIDHash:      coinbaseTx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: output.LockingScript,
		Satoshis:      output.Satoshis,
	}

	// Create first transaction (TX1)
	tx1 := bt.NewTx()
	err = tx1.FromUTXOs(utxo)
	require.NoError(t, err)

	// Send 50000 satoshis to address1
	err = tx1.AddP2PKHOutputFromAddress(address1.AddressString, 50000)
	require.NoError(t, err)

	// Sign TX1
	err = tx1.FillAllInputs(td.Ctx, &unlocker.Getter{PrivateKey: coinbasePrivateKey.PrivKey})
	require.NoError(t, err)

	t.Logf("Sending TX1 with RPC: %s\n", tx1.TxIDChainHash())
	tx1Bytes := hex.EncodeToString(tx1.ExtendedBytes())

	// Send TX1
	resp, err := td.CallRPC("sendrawtransaction", []any{tx1Bytes})
	require.NoError(t, err, "Failed to send TX1 with rpc")
	t.Logf("TX1 sent with RPC: %s\n", resp)

	// Wait for transaction to be processed if there's a delay window
	delay := tSettings.BlockAssembly.DoubleSpendWindow
	if delay != 0 {
		t.Logf("Waiting %dms [block assembly has delay processing txs to catch double spends]\n", delay)
		time.Sleep(delay * time.Millisecond)
	}

	time.Sleep(250 * time.Millisecond) // Make absolutely sure block assembly has processed the tx

	// Generate one block to include TX1
	_, err = td.CallRPC("generate", []any{1})
	require.NoError(t, err)

	// Create second recipient's key pair
	privateKey2, err := bec.NewPrivateKey(bec.S256())
	require.NoError(t, err)
	address2, err := bscript.NewAddressFromPublicKey(privateKey2.PubKey(), true)
	require.NoError(t, err)

	// Create UTXO from TX1
	utxo2 := &bt.UTXO{
		TxIDHash:      tx1.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: tx1.Outputs[0].LockingScript,
		Satoshis:      tx1.Outputs[0].Satoshis,
	}

	// Create second transaction (TX2)
	tx2 := bt.NewTx()
	err = tx2.FromUTXOs(utxo2)
	require.NoError(t, err)

	// Send 25000 satoshis to address2
	err = tx2.AddP2PKHOutputFromAddress(address2.AddressString, 25000)
	require.NoError(t, err)

	// Sign TX2
	err = tx2.FillAllInputs(td.Ctx, &unlocker.Getter{PrivateKey: privateKey1})
	require.NoError(t, err)

	t.Logf("Sending TX2 with RPC: %s\n", tx2.TxIDChainHash())
	tx2Bytes := hex.EncodeToString(tx2.ExtendedBytes())

	// Send TX2
	resp, err = td.CallRPC("sendrawtransaction", []any{tx2Bytes})
	require.NoError(t, err, "Failed to send TX2 with rpc")
	t.Logf("TX2 sent with RPC: %s\n", resp)

	// Wait for transaction to be processed if there's a delay window
	if delay != 0 {
		t.Logf("Waiting %dms [block assembly has delay processing txs to catch double spends]\n", delay)
		time.Sleep(delay * time.Millisecond)
	}

	// Generate one block to include TX2
	_, err = td.CallRPC("generate", []any{1})
	require.NoError(t, err)

	// Get the block containing TX2 (should be at height 103)
	block103, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 103)
	require.NoError(t, err)

	// Verify block103 contains TX2
	err = block103.GetAndValidateSubtrees(td.Ctx, td.Logger, td.SubtreeStore, nil)
	require.NoError(t, err)

	err = block103.CheckMerkleRoot(td.Ctx)
	require.NoError(t, err)

	fallbackGetFunc := func(subtreeHash chainhash.Hash) error {
		return block103.SubTreesFromBytes(subtreeHash[:])
	}

	subtree, err := block103.GetSubtrees(td.Ctx, td.Logger, td.SubtreeStore, fallbackGetFunc)
	require.NoError(t, err)

	tx2Found := false
	for i := 0; i < len(subtree); i++ {
		st := subtree[i]
		for _, node := range st.Nodes {
			if node.Hash.String() == tx2.TxIDChainHash().String() {
				tx2Found = true
				break
			}
		}
	}

	assert.True(t, tx2Found, "TX2 not found in the blockstore")
}

func TestDoubleInput(t *testing.T) {
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		SettingsContext: "dev.system.test.oversizedscripttest",
	})

	defer td.Stop(t)

	// set run state
	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// Generate initial blocks to get coinbase funds
	_, err = td.CallRPC("generate", []interface{}{101})
	require.NoError(t, err)

	// Create a transaction with an oversized script
	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)

	coinbaseTx := block1.CoinbaseTx

	tx := td.CreateTransactionWithOptions(t,
		daemon.WithInput(coinbaseTx, 0),
		daemon.WithInput(coinbaseTx, 0),
		daemon.WithP2PKHOutputs(1, coinbaseTx.Outputs[0].Satoshis+100000, nil),
		daemon.WithOpReturnData([]byte("test")),
		daemon.WithP2PKHOutputs(1, 1000),
	)

	err = td.PropagationClient.ProcessTransaction(td.Ctx, tx)
	require.Error(t, err)

	t.Logf("tx: %s", tx.String())

}
