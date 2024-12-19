package doublespendtest

import (
	"context"
	"testing"

	"github.com/bitcoin-sv/ubsv/chaincfg"
	"github.com/bitcoin-sv/ubsv/daemon"
	"github.com/bitcoin-sv/ubsv/services/blockassembly"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/settings"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/stretchr/testify/require"
)

/*

THE PLAN

Blockchain - sqllite
BLockstore = file
SubtreeStore = lotr file



*/

// func TestDoubleSpend(t *testing.T) {
// 	logger := ulogger.NewVerboseTestLogger(t)

// 	tSettings := test.CreateBaseTestSettings()

// 	blockchainStore := blockchain.NewMockStore()
// 	blockStore := memory.New()
// 	subtreeStore := memory.New()
// 	txStore := memory.New()
// 	utxoStore := utxo_memory.New(logger)

// 	localValidator, err := validator.New(ctx,
// 		logger,
// 		tSettings,
// 		utxoStore,
// 		nil, // txMetaKafkaProducerClient,
// 		nil, // rejectedTxKafkaProducerClient,
// 	)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	blockvalidation.New(
// 		logger,
// 		tSettings,
// 		subtreeStore,
// 		txStore,
// 		utxoStore,
// 		localValidator,
// 		blockchainClient,
// 		blockchainStore,
// 		blockStore,
// 	)

// }

func TestDaemon(t *testing.T) {
	t.SkipNow()

	ctx := context.Background()
	logger := ulogger.TestLogger{}

	tSettings := settings.NewSettings()
	tSettings.ChainCfgParams = &chaincfg.RegressionNetParams

	readyCh := make(chan struct{})

	d := daemon.New()
	go d.Start(logger, []string{
		"-all=0",
		"-blockchain=1",
		"-subtreevalidation=1",
		"-blockvalidation=1",
		"-blockassembly=1",
		"-asset=1",
	}, tSettings, readyCh)

	<-readyCh

	bcClient, err := blockchain.NewClient(ctx, logger, tSettings, "test")
	require.NoError(t, err)

	err = bcClient.Run(ctx, "test")
	require.NoError(t, err)

	baClient, err := blockassembly.NewClient(ctx, logger, tSettings)
	require.NoError(t, err)

	err = baClient.GenerateBlocks(ctx, 101)
	require.NoError(t, err)

	block1, _, err := bcClient.GetBlockHeadersByHeight(ctx, 1, 1)
	require.NoError(t, err)

	t.Logf("Block 1: %s", block1[0].StringDump())

	d.Stop()
}

// time.Sleep(15 * time.Second)

// d.Stop()
// t.Log("Daemon stopped")
// }

// func createCoinbaseTx(height uint32) (*bt.Tx, error) {
// 	coinbaseTx := bt.NewTx()

// 	err := coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0)
// 	if err != nil {
// 		return nil, err
// 	}

// 	blockHeightBytes := make([]byte, 4)
// 	binary.LittleEndian.PutUint32(blockHeightBytes, height) // Block height

// 	arbitraryData := []byte{}
// 	arbitraryData = append(arbitraryData, 0x03)
// 	arbitraryData = append(arbitraryData, blockHeightBytes[:3]...)

// 	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes(arbitraryData)
// 	coinbaseTx.Inputs[0].SequenceNumber = 0

// 	err = coinbaseTx.AddP2PKHOutputFromPubKeyBytes(privkey.PubKey().SerialiseCompressed(), 50e8)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return coinbaseTx, nil
// }

// func createDoubleSpendData(t *testing.T) {
// 	// Start with funds
// 	coinbase1, err := createCoinbaseTx(1)
// 	require.NoError(t, err)

// 	// coinbase2, err := createCoinbaseTx(2)
// 	// require.NoError(t, err)

// 	tx1 := bt.NewTx()

// 	err = tx1.FromUTXOs(&bt.UTXO{
// 		TxIDHash:      coinbase1.TxIDChainHash(),
// 		Vout:          0,
// 		LockingScript: coinbase1.Outputs[0].LockingScript,
// 		Satoshis:      coinbase1.Outputs[0].Satoshis,
// 	})
// 	require.NoError(t, err)

// 	privkey, err = bec.NewPrivateKey(bec.S256())
// 	require.NoError(t, err)

// 	err = tx1.AddP2PKHOutputFromPubKeyBytes(privkey.PubKey().SerialiseCompressed(), 50e8)
// 	require.NoError(t, err)

// 	err = tx1.FillAllInputs(context.Background(), &unlocker.Getter{PrivateKey: privkey})
// 	require.NoError(t, err)

// 	t.Logf("tx1: %x", tx1.Bytes())
// }
