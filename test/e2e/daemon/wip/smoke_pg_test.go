package smoke

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/unlocker"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/test"
	postgres "github.com/bsv-blockchain/teranode/test/longtest/util/postgres"
	helper "github.com/bsv-blockchain/teranode/test/utils"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShouldAllowFairTxUseRpcWithPostgres(t *testing.T) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()

	ctx := context.Background()

	pg, errPsql := postgres.RunPostgresTestContainer(ctx, "fairtx")
	require.NoError(t, errPsql)

	t.Cleanup(func() {
		if err := pg.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate postgres container: %v", err)
		}
	})

	// Update the PostgreSQL connection settings with the dynamic port
	gocore.Config().Set("POSTGRES_PORT", pg.Port)

	pgStore := fmt.Sprintf("postgres://teranode:teranode@localhost:%s/teranode?expiration=5m", pg.Port)

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC: true,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(tSettings *settings.Settings) {
				url, err := url.Parse(pgStore)
				require.NoError(t, err)
				tSettings.BlockChain.StoreURL = url
				tSettings.Coinbase.Store = url
				tSettings.UtxoStore.UtxoStore = url
			},
		),
	})

	defer td.Stop(t)

	// set run state
	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// Generate initial blocks
	_, err = td.CallRPC(td.Ctx, "generate", []interface{}{101})
	require.NoError(t, err)

	tSettings := td.Settings

	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)

	coinbaseTx := block1.CoinbaseTx

	coinbasePrivKey := tSettings.BlockAssembly.MinerWalletPrivateKeys[0]
	coinbasePrivateKey, err := bec.PrivateKeyFromWif(coinbasePrivKey)
	require.NoError(t, err)

	_, err = bscript.NewAddressFromPublicKey(coinbasePrivateKey.PubKey(), true)
	require.NoError(t, err)

	privateKey, err := bec.NewPrivateKey()
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

	err = newTx.FillAllInputs(td.Ctx, &unlocker.Getter{PrivateKey: coinbasePrivateKey})
	require.NoError(t, err)

	t.Logf("Sending New Transaction with RPC: %s\n", newTx.TxIDChainHash())
	txBytes := hex.EncodeToString(newTx.ExtendedBytes())

	resp, err := td.CallRPC(td.Ctx, "sendrawtransaction", []interface{}{txBytes})
	require.NoError(t, err, "Failed to send new tx with rpc")
	t.Logf("Transaction sent with RPC: %s\n", resp)

	// Wait for transaction to be processed
	delay := tSettings.BlockAssembly.DoubleSpendWindow
	if delay != 0 {
		t.Logf("Waiting %dms [block assembly has delay processing txs to catch double spends]\n", delay)
		time.Sleep(time.Duration(delay) * time.Millisecond)
	}

	_, err = td.CallRPC(td.Ctx, "generate", []interface{}{1})
	require.NoError(t, err, "Failed to generate blocks")

	t.Logf("Resp: %s", resp)

	_, err = td.CallRPC(td.Ctx, "generate", []interface{}{100})
	require.NoError(t, err, "Failed to generate blocks")

	t.Logf("Resp: %s", resp)

	block102, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 102)
	require.NoError(t, err)

	err = block102.GetAndValidateSubtrees(td.Ctx, td.Logger, td.SubtreeStore, td.Settings.Block.GetAndValidateSubtreesConcurrency)
	require.NoError(t, err)

	err = block102.CheckMerkleRoot(td.Ctx)
	require.NoError(t, err)

	subtree, err := block102.GetSubtrees(td.Ctx, td.Logger, td.SubtreeStore, td.Settings.Block.GetAndValidateSubtreesConcurrency)
	require.NoError(t, err)

	blFound := false
	for i := 0; i < len(subtree); i++ {
		st := subtree[i]
		for _, node := range st.Nodes {
			t.Logf("node.Hash: %s", node.Hash.String())
			t.Logf("tx.TxIDChainHash().String(): %s", newTx.TxIDChainHash().String())

			if node.Hash.String() == newTx.TxIDChainHash().String() {
				blFound = true
				break
			}
		}
	}

	assert.True(t, blFound, "TX not found in the blockstore")

	resp, err = td.CallRPC(td.Ctx, "getblockchaininfo", []interface{}{})
	require.NoError(t, err)

	var blockchainInfo helper.BlockchainInfo
	errJSON := json.Unmarshal([]byte(resp), &blockchainInfo)

	if errJSON != nil {
		t.Errorf("JSON decoding error: %v", errJSON)
		return
	}

	block202, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 202)
	require.NoError(t, err)
	t.Logf("blockchainInfo: %+v", blockchainInfo)
	assert.Equal(t, int(202), blockchainInfo.Result.Blocks)
	assert.Equal(t, block202.Hash().String(), blockchainInfo.Result.BestBlockHash)
	assert.Equal(t, "regtest", blockchainInfo.Result.Chain)
	assert.Equal(t, "9601000000000000000000000000000000000000000000000000000000000000", blockchainInfo.Result.Chainwork)
	assert.Equal(t, float64(4.6565423739069247e-10), blockchainInfo.Result.Difficulty)
	assert.Equal(t, int(863341), blockchainInfo.Result.Headers)
	assert.Equal(t, int(0), blockchainInfo.Result.Mediantime)
	assert.False(t, blockchainInfo.Result.Pruned)
	assert.Empty(t, blockchainInfo.Result.Softforks)
	assert.Equal(t, float64(0), blockchainInfo.Result.VerificationProgress)
	assert.Nil(t, blockchainInfo.Error)
	assert.Nil(t, blockchainInfo.ID)

	resp, err = td.CallRPC(td.Ctx, "getinfo", []interface{}{})

	require.NoError(t, err)

	var getInfo helper.GetInfo
	errJSON = json.Unmarshal([]byte(resp), &getInfo)
	require.NoError(t, errJSON)
	require.NotNil(t, getInfo.Result)

	t.Logf("getInfo: %+v", getInfo)

	assert.Equal(t, int(202), getInfo.Result.Blocks)
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

	resp, err = td.CallRPC(td.Ctx, "getdifficulty", []interface{}{})

	require.NoError(t, err)

	errJSON = json.Unmarshal([]byte(resp), &getDifficulty)
	require.NoError(t, errJSON)

	t.Logf("getDifficulty: %+v", getDifficulty)
	assert.Equal(t, float64(4.6565423739069247e-10), getDifficulty.Result)

	resp, err = td.CallRPC(td.Ctx, "getblockhash", []interface{}{202})
	require.NoError(t, err, "Failed to generate blocks")

	var getBlockHash helper.GetBlockHashResponse
	errJSON = json.Unmarshal([]byte(resp), &getBlockHash)
	require.NoError(t, errJSON)

	t.Logf("getBlockHash: %+v", getBlockHash)
	assert.Equal(t, block202.Hash().String(), getBlockHash.Result)

	t.Logf("%s", resp)

	resp, err = td.CallRPC(td.Ctx, "getblockbyheight", []interface{}{102})
	require.NoError(t, err, "Failed to get block by height")

	var getBlockByHeightResp helper.GetBlockByHeightResponse
	errJSON = json.Unmarshal([]byte(resp), &getBlockByHeightResp)
	require.NoError(t, errJSON)

	t.Logf("getBlockByHeightResp: %+v", getBlockByHeightResp)

	t.Logf("Resp: %s", resp)

	block103, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 103)
	require.NoError(t, err)
	block101, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 101)
	require.NoError(t, err)
	// Assert block properties
	require.Equal(t, block102.Hash().String(), getBlockByHeightResp.Result.Hash)
	require.Equal(t, 101, getBlockByHeightResp.Result.Confirmations)
	// require.Equal(t, 229, getBlockByHeightResp.Result.Size)
	require.Equal(t, uint32(102), getBlockByHeightResp.Result.Height)
	require.Equal(t, 536870912, getBlockByHeightResp.Result.Version)
	require.Equal(t, "20000000", getBlockByHeightResp.Result.VersionHex)
	require.Equal(t, block102.Header.HashMerkleRoot.String(), getBlockByHeightResp.Result.Merkleroot)
	require.Equal(t, "207fffff", getBlockByHeightResp.Result.Bits)
	require.InDelta(t, 4.6565423739069247e-10, getBlockByHeightResp.Result.Difficulty, 1e-20)
	require.Equal(t, block101.Hash().String(), getBlockByHeightResp.Result.Previousblockhash)
	require.Equal(t, block103.Hash().String(), getBlockByHeightResp.Result.Nextblockhash)
}

func TestShouldNotProcessNonFinalTxWithPostgres(t *testing.T) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:            true,
		SettingsOverrideFunc: test.SystemTestSettings(),
	})

	defer td.Stop(t)

	tSettings := td.Settings

	// set run state
	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// Generate initial blocks
	// CSVHeight is the block height at which the CSV rules are activated including lock time
	td.MineAndWait(t, tSettings.ChainCfgParams.CSVHeight+1)

	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)

	coinbaseTx := block1.CoinbaseTx

	coinbasePrivKey := tSettings.BlockAssembly.MinerWalletPrivateKeys[0]
	coinbasePrivateKey, err := bec.PrivateKeyFromWif(coinbasePrivKey)
	require.NoError(t, err)

	_, err = bscript.NewAddressFromPublicKey(coinbasePrivateKey.PubKey(), true)
	require.NoError(t, err)

	privateKey, err := bec.NewPrivateKey()
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

	err = newTx.FillAllInputs(td.Ctx, &unlocker.Getter{PrivateKey: coinbasePrivateKey})
	require.NoError(t, err)

	// When a transaction's nLockTime is set (e.g., 500 for block height),
	// nSequence must be less than 0xffffffff for the locktime to be enforced.
	// Otherwise, the locktime is ignored.
	newTx.Inputs[0].SequenceNumber = 0x10000005
	newTx.LockTime = tSettings.ChainCfgParams.CSVHeight + 123

	t.Logf("Sending New Transaction with RPC: %s\n", newTx.TxIDChainHash())
	txBytes := hex.EncodeToString(newTx.ExtendedBytes())

	resp, err := td.CallRPC(td.Ctx, "sendrawtransaction", []any{txBytes})
	// "code: -8, message: TX rejected:
	// PROCESSING (4): error sending transaction 1b36fab6c9342373f0c45770f5e46f963e6d70a3f42a5e8fce003b03ed27f631 to 100.00% of the propagation servers: [SERVICE_ERROR (59): address localhost:8084
	//  -> UNKNOWN (0): SERVICE_ERROR (59): [ProcessTransaction][1b36fab6c9342373f0c45770f5e46f963e6d70a3f42a5e8fce003b03ed27f631] failed to validate transaction
	//  -> UTXO_NON_FINAL (61): [Validate][1b36fab6c9342373f0c45770f5e46f963e6d70a3f42a5e8fce003b03ed27f631] transaction is not final
	//  -> TX_LOCK_TIME (35): lock time (699) as block height is greater than block height (578)]"
	require.Error(t, err, "Failed to send new tx with rpc")
	require.Contains(t, err.Error(), "transaction is not final")
	require.Contains(t, err.Error(), "TX_LOCK_TIME")

	t.Logf("Transaction sent with RPC: %s\n", resp)
}
