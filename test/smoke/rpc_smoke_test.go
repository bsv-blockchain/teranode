//go:build test_all || test_smoke_rpc || test_rpc || debug

package smoke

import (
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/test/testdaemon"
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

func TestShouldAllowFairTxUseRpc(t *testing.T) {
	td := testdaemon.New(t, testdaemon.TestOptions{
		EnableRPC:        true,
		KillTeranode:     true,
		SettingsOverride: "dev.system.test",
	})

	t.Cleanup(func() {
		td.Stop()
	})

	// set run state
	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// Generate initial blocks
	_, err = td.CallRPC("generate", []interface{}{101})
	require.NoError(t, err)

	tSettings := td.Settings

	state := td.WaitForBlockHeight(t, 101, 5*time.Second)
	assert.Equal(t, uint32(101), state.CurrentHeight, "Expected block assembly to reach height 101")

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

	t.Logf("Sending New Transaction with RPC: %s\n", newTx.TxIDChainHash())
	txBytes := hex.EncodeToString(newTx.ExtendedBytes())

	resp, err := td.CallRPC("sendrawtransaction", []interface{}{txBytes})
	require.NoError(t, err, "Failed to send new tx with rpc")
	t.Logf("Transaction sent with RPC: %s\n", resp)

	// Wait for transaction to be processed
	delay := tSettings.BlockAssembly.DoubleSpendWindow
	if delay != 0 {
		t.Logf("Waiting %dms [block assembly has delay processing txs to catch double spends]\n", delay)
		time.Sleep(time.Duration(delay) * time.Millisecond)
	}

	_, err = td.CallRPC("generate", []interface{}{1})
	require.NoError(t, err, "Failed to generate blocks")

	t.Logf("Resp: %s", resp)

	_, err = td.CallRPC("generate", []interface{}{100})
	require.NoError(t, err, "Failed to generate blocks")

	t.Logf("Resp: %s", resp)

	state = td.WaitForBlockHeight(t, 202, 10*time.Second)
	assert.Equal(t, uint32(202), state.CurrentHeight, "Expected block assembly to reach height 202")

	block102, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 102)
	require.NoError(t, err)

	err = block102.GetAndValidateSubtrees(td.Ctx, td.Logger, td.SubtreeStore, nil)
	require.NoError(t, err)

	err = block102.CheckMerkleRoot(td.Ctx)
	require.NoError(t, err)

	fallbackGetFunc := func(subtreeHash chainhash.Hash) error {
		return block102.SubTreesFromBytes(subtreeHash[:])
	}

	subtree, err := block102.GetSubtrees(td.Ctx, td.Logger, td.SubtreeStore, fallbackGetFunc)
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

	resp, err = td.CallRPC("getblockchaininfo", []interface{}{})
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
	assert.Equal(t, string("4.6565423739069247246592908691514574469873245939403288293276821765520783128832e-10"), blockchainInfo.Result.Difficulty)
	assert.Equal(t, int(863341), blockchainInfo.Result.Headers)
	assert.Equal(t, int(0), blockchainInfo.Result.Mediantime)
	assert.False(t, blockchainInfo.Result.Pruned)
	assert.Empty(t, blockchainInfo.Result.Softforks)
	assert.Equal(t, float64(0), blockchainInfo.Result.VerificationProgress)
	assert.Nil(t, blockchainInfo.Error)
	assert.Nil(t, blockchainInfo.ID)

	resp, err = td.CallRPC("getinfo", []interface{}{})

	require.NoError(t, err)

	var getInfo helper.GetInfo
	errJSON = json.Unmarshal([]byte(resp), &getInfo)
	require.NoError(t, errJSON)
	require.NotNil(t, getInfo.Result)

	t.Logf("getInfo: %+v", getInfo)

	assert.Equal(t, int(202), getInfo.Result.Blocks)
	assert.Equal(t, int(1), getInfo.Result.Connections)
	assert.Equal(t, float64(1), getInfo.Result.Difficulty)
	assert.Equal(t, int(70016), getInfo.Result.ProtocolVersion)
	assert.Equal(t, "host:port", getInfo.Result.Proxy)
	assert.Equal(t, float64(100), getInfo.Result.RelayFee)
	assert.False(t, getInfo.Result.Stn)
	assert.False(t, getInfo.Result.TestNet)
	assert.Equal(t, int(1), getInfo.Result.TimeOffset)
	assert.Equal(t, int(1), getInfo.Result.Version)
	assert.Nil(t, getInfo.Error)
	assert.Equal(t, int(0), getInfo.ID)

	var getDifficulty helper.GetDifficultyResponse

	resp, err = td.CallRPC("getdifficulty", []interface{}{})

	require.NoError(t, err)

	errJSON = json.Unmarshal([]byte(resp), &getDifficulty)
	require.NoError(t, errJSON)

	t.Logf("getDifficulty: %+v", getDifficulty)
	assert.Equal(t, float64(4.6565423739069247e-10), getDifficulty.Result)

	resp, err = td.CallRPC("getblockhash", []interface{}{202})
	require.NoError(t, err, "Failed to generate blocks")

	var getBlockHash helper.GetBlockHashResponse
	errJSON = json.Unmarshal([]byte(resp), &getBlockHash)
	require.NoError(t, errJSON)

	t.Logf("getBlockHash: %+v", getBlockHash)
	assert.Equal(t, block202.Hash().String(), getBlockHash.Result)

	t.Logf("%s", resp)

	resp, err = td.CallRPC("getblockbyheight", []interface{}{102})
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
	require.Equal(t, 102, getBlockByHeightResp.Result.Height)
	require.Equal(t, 536870912, getBlockByHeightResp.Result.Version)
	require.Equal(t, "20000000", getBlockByHeightResp.Result.VersionHex)
	require.Equal(t, block102.Header.HashMerkleRoot.String(), getBlockByHeightResp.Result.Merkleroot)
	require.Equal(t, "207fffff", getBlockByHeightResp.Result.Bits)
	require.InDelta(t, 4.6565423739069247e-10, getBlockByHeightResp.Result.Difficulty, 1e-20)
	require.Equal(t, block101.Hash().String(), getBlockByHeightResp.Result.Previousblockhash)
	require.Equal(t, block103.Hash().String(), getBlockByHeightResp.Result.Nextblockhash)
}

func TestShouldNotProcessNonFinalTx(t *testing.T) {
	t.Skip("Test is disabled")
	td := testdaemon.New(t, testdaemon.TestOptions{
		EnableRPC:        true,
		KillTeranode:     true,
		SettingsOverride: "dev.system.test",
	})

	t.Cleanup(func() {
		td.Stop()
	})

	// set run state
	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// Generate initial blocks
	_, err = td.CallRPC("generate", []interface{}{101})
	require.NoError(t, err)

	tSettings := td.Settings

	state := td.WaitForBlockHeight(t, 101, 5*time.Second)
	assert.Equal(t, uint32(101), state.CurrentHeight, "Expected block assembly to reach height 101")

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

	newTx.LockTime = 350

	t.Logf("Sending New Transaction with RPC: %s\n", newTx.TxIDChainHash())
	txBytes := hex.EncodeToString(newTx.ExtendedBytes())

	resp, err := td.CallRPC("sendrawtransaction", []interface{}{txBytes})
	require.Error(t, err, "Failed to send new tx with rpc")
	t.Logf("Transaction sent with RPC: %s\n", resp)
}
