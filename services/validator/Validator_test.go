package validator_test

import (
	"bufio"
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/validator"
	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/stores/txmeta/memory"
	"github.com/bitcoin-sv/ubsv/stores/txmetacache"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	utxoMemorystore "github.com/bitcoin-sv/ubsv/stores/utxo/memory"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/test"
	"github.com/libsv/go-bt/v2"
)

var (
	tx, _        = bt.NewTxFromString("010000000152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")
	utxoHash0, _ = util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[0], 0)
	testSpend0   = &utxostore.Spend{
		TxID: tx.TxIDChainHash(),
		Vout: 0,
		Hash: utxoHash0,
	}
	Hash, _  = chainhash.NewHashFromStr("5e3bc5947f48cec766090aa17f309fd16259de029dcef5d306b514848c9687c7")
	Hash2, _ = chainhash.NewHashFromStr("663bc5947f48cec766090aa17f309fd16259de029dcef5d306b514848c9687c8")
	spends   = []*utxostore.Spend{{
		TxID:         tx.TxIDChainHash(),
		Vout:         0,
		Hash:         utxoHash0,
		SpendingTxID: Hash,
	}}
	spends2 = []*utxostore.Spend{{
		TxID:         tx.TxIDChainHash(),
		Vout:         0,
		Hash:         utxoHash0,
		SpendingTxID: Hash2,
	}}
	previousTxScript, _ = hex.DecodeString("76a914d687c76d6ee133c9cc42bd96e3947d8a84bdf60288ac")
	hash, _             = chainhash.NewHashFromStr("8ef53bb4c9c4b849c30ec75243bad8a7eafd83f407407a154a6d9ec80d83dd00")
)

type NullStore struct{}

func (ns *NullStore) SetBlockHeight(height uint32) error {
	return nil
}

func (ns *NullStore) GetBlockHeight() (uint32, error) {
	return 0, nil
}

func (ns *NullStore) Health(ctx context.Context) (int, string, error) {
	return 0, "Validator test Null Store", nil
}

func (ns *NullStore) DeleteSpends(deleteSpends bool) {
	// No nothing
}

func (ns *NullStore) Get(ctx context.Context, spend *utxostore.Spend) (*utxostore.Response, error) {
	// fmt.Printf("Get(%s)\n", hash.String())
	return nil, nil
}

func (ns *NullStore) Store(ctx context.Context, tx *bt.Tx, lockTime ...uint32) error {
	// fmt.Printf("Store(%s)\n", hash.String())
	return nil
}

func (ns *NullStore) StoreFromHashes(ctx context.Context, txID chainhash.Hash, utxoHashes []chainhash.Hash, lockTime uint32) error {
	return nil
}

func (ns *NullStore) Spend(ctx context.Context, spends []*utxostore.Spend) error {
	// fmt.Printf("Spend(%s, %s)\n", hash.String(), txID.String())
	return nil
}

func (ns *NullStore) UnSpend(ctx context.Context, spends []*utxostore.Spend) error {
	// fmt.Printf("MoveUpBlock(%s)\n", hash.String())
	return nil
}

func (ns *NullStore) Delete(ctx context.Context, tx *bt.Tx) error {
	// fmt.Printf("MoveUpBlock(%s)\n", hash.String())
	return nil
}

func BenchmarkValidator(b *testing.B) {
	tx, err := bt.NewTxFromString("010000000000000000ef01f3f0d33a5c5afd524043762f8b812999caa5a225e6e20ecdb71a7e0e1c207b43530000006a473044022049e20908f21bdcb901b5c5a9a93b238446606267e19db4e662df1a7c4a5bae08022036960a340515e2cfee79b9c194093f24f253d4243bf9d0baa97352983e2263fa412102a98c1a3be041da2591761fbef4b2ab0f147aef36c308aee66df0b9825218de23ffffffff10000000000000001976a914a8d6bd6648139d95dac35d411c592b05bc0973aa88ac01000000000000000070006a0963657274696861736822314c6d763150594d70387339594a556e374d3948565473446b64626155386b514e4a403263333934306361313334353331373035326334346630613861636362323162323165633131386465646330396538643764393064323166333935663063613000000000")
	if err != nil {
		panic(err)
	}

	ns := &NullStore{}

	v, err := validator.New(context.Background(), ulogger.TestLogger{}, ns, memory.New(ulogger.TestLogger{}), nil)
	if err != nil {
		panic(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := v.Validate(context.Background(), tx); err != nil {
			log.Printf("ERROR: %v\n", err)
		} else {
			fmt.Println("asd")
		}
	}
}

func TestValidate_CoinbaseTransaction(t *testing.T) {
	coinbaseHex := "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1703fb03002f6d322d75732f0cb6d7d459fb411ef3ac6d65ffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac00000000"
	coinbase, err := bt.NewTxFromString(coinbaseHex)
	require.NoError(t, err)

	// need to add spendable utxo to utxo store

	// delete spends set to false
	utxoStore := utxoMemorystore.New(false)
	txMetaStore := memory.New(ulogger.TestLogger{}, true)

	v, err := validator.New(context.Background(), ulogger.TestLogger{}, utxoStore, txMetaStore, nil)
	if err != nil {
		panic(err)
	}

	err = v.Validate(context.Background(), coinbase)
	require.Error(t, err)
}

func TestValidate_ValidTransaction(t *testing.T) {
	utxoStore := utxoMemorystore.New(false)
	err := utxoStore.Store(context.Background(), tx)
	require.NoError(t, err)

	height, err := utxoStore.GetBlockHeight()
	require.NoError(t, err)

	fmt.Println("utxoStore height: ", height)
	fmt.Println("transaction output len: ", len(tx.Outputs))
	fmt.Println("transaction satoshis: ", tx.Outputs[0].Satoshis)
	fmt.Println("transaction script: ", tx.Outputs[0].LockingScript)
	//fmt.Println("transaction sequence number: ", tx.Outputs[0].)

	for i, output := range tx.Outputs {
		fmt.Println("output ", i, " : ", output)
	}

	txHash := tx.TxIDChainHash()

	// create a new transaction using one of the outputs of the previous transaction
	newTx := bt.NewTx()
	newTx.Inputs = append(newTx.Inputs, &bt.Input{
		PreviousTxSatoshis: 201,
		PreviousTxScript:   bscript.NewFromBytes(previousTxScript),
		UnlockingScript:    bscript.NewFromBytes(previousTxScript),
		PreviousTxOutIndex: 0,
		SequenceNumber:     0,
	})

	err = newTx.Inputs[0].PreviousTxIDAdd(txHash)
	require.NoError(t, err)

	// add an output to the new transaction
	newTx.AddOutput(&bt.Output{
		Satoshis:      100,
		LockingScript: &bscript.Script{},
	})
	newTx.AddOutput(&bt.Output{
		Satoshis:      100,
		LockingScript: &bscript.Script{},
	})
	newTx.AddOutput(&bt.Output{
		Satoshis:      10,
		LockingScript: &bscript.Script{},
	})
	tx.LockTime = 0

	txMetaStore := memory.New(ulogger.TestLogger{}, true)

	fmt.Println("tx size: ", newTx.Size())

	// validate transaction
	v, err := validator.New(context.Background(), ulogger.TestLogger{}, utxoStore, txMetaStore, nil)
	if err != nil {
		panic(err)
	}

	err = v.Validate(context.Background(), newTx)
	require.NoError(t, err)

}

func TestValidate_ValidBlockTransaction(t *testing.T) {
	//ctx := context.Background()
	var cachedTxMetaStore txmeta.Store

	// newTx, err := bt.NewTxFromString("010000000000000000ef01f3f0d33a5c5afd524043762f8b812999caa5a225e6e20ecdb71a7e0e1c207b43530000006a473044022049e20908f21bdcb901b5c5a9a93b238446606267e19db4e662df1a7c4a5bae08022036960a340515e2cfee79b9c194093f24f253d4243bf9d0baa97352983e2263fa412102a98c1a3be041da2591761fbef4b2ab0f147aef36c308aee66df0b9825218de23ffffffff10000000000000001976a914a8d6bd6648139d95dac35d411c592b05bc0973aa88ac01000000000000000070006a0963657274696861736822314c6d763150594d70387339594a556e374d3948565473446b64626155386b514e4a403263333934306361313334353331373035326334346630613861636362323162323165633131386465646330396538643764393064323166333935663063613000000000")
	// if err != nil {
	// 	panic(err)
	// }
	subtreeStore := test.NewLocalSubtreeStore()
	utxoStore := utxoMemorystore.New(false)

	fileDir := "./test-generated_test_data/"
	config := test.TestConfig{
		FileDir:                      fileDir,
		FileNameTemplate:             fileDir + "subtree-%d.bin",
		FileNameTemplateMerkleHashes: fileDir + "subtree-merkle-hashes.bin",
		FileNameTemplateBlock:        fileDir + "block.bin",
		TxMetafileNameTemplate:       fileDir + "txMeta.bin",
		SubtreeSize:                  8,
		TxCount:                      8,
		GenerateNewTestData:          true,
	}
	block, err := test.GenerateTestBlock(subtreeStore, &config)
	require.NoError(t, err)

	txMetaStore := memory.New(ulogger.TestLogger{}, true)
	cachedTxMetaStore = txmetacache.NewTxMetaCache(context.Background(), ulogger.TestLogger{}, txMetaStore, 1024)
	file, err := os.Open(config.TxMetafileNameTemplate)
	require.NoError(t, err)
	defer file.Close()

	// create a buffered reader for the file
	bufReader := bufio.NewReaderSize(file, 55*1024*1024)

	err = test.ReadTxMeta(bufReader, cachedTxMetaStore.(*txmetacache.TxMetaCache))
	require.NoError(t, err)

	// check if the first txid is in the txMetaStore
	reqTxId, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)

	data, err := cachedTxMetaStore.Get(context.Background(), reqTxId)
	require.NoError(t, err)
	require.Equal(t, &txmeta.Data{
		Fee:            1,
		SizeInBytes:    1,
		ParentTxHashes: []chainhash.Hash{},
	}, data)

	for idx, subtreeHash := range block.Subtrees {
		subtreeStore.Files[*subtreeHash] = idx
	}

	currentChain := make([]*model.BlockHeader, 11)
	currentChainIDs := make([]uint32, 11)
	for i := 0; i < 11; i++ {
		currentChain[i] = &model.BlockHeader{
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			// set the last 11 block header timestamps to be less than the current timestamps
			Timestamp: 1231469665 - uint32(i),
		}
		currentChainIDs[i] = uint32(i)
	}
	currentChain[0].HashPrevBlock = &chainhash.Hash{}

	v, err := validator.New(context.Background(), ulogger.TestLogger{}, utxoStore, txMetaStore, nil)
	if err != nil {
		panic(err)
	}

	fmt.Println(v.GetBlockHeight())

	// create on memory utxo store
	//utxoStore := utxoMemorystore.New(false)

	// input1 := &bt.Input{
	// 	PreviousTxSatoshis: 11.4999616 * 1e8,
	// 	PreviousTxScript:   bscript.NewFromBytes(previousTxScript),
	// 	PreviousTxOutIndex: 0,
	// 	SequenceNumber:     4294967294,
	// }
	// err := input1.PreviousTxIDAddStr("2fb09ea4d1d282f55b4f4b5b1eec92fa314e1ba5a5a009e897f63d155b4dba82")
	// require.NoError(t, err)

	//utxoHash := UTXOHashFromInput(tt.args.input)

	// coinbaseHex := "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1703fb03002f6d322d75732f0cb6d7d459fb411ef3ac6d65ffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac00000000"
	// coinbaseTx, err := bt.NewTxFromString(coinbaseHex)
	// require.NoError(t, err)

	// utxoHashes, err := utxostore.GetUtxoHashes(tx)
	// require.NoError(t, err)
	// fmt.Println("utxoHashes of tx: ", utxoHashes)

	// utxoHashTx, _ := util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[0], 0)
	// fmt.Println("utxoHash of tx: ", utxoHashTx)

	//input := &bt.Input{}

	// transaction := bt.NewTx()
	// transaction.Inputs = append(transaction.Inputs)
	// err = transaction.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 5000000000)
	// require.NoError(t, err)

	// add spendable utxo to utxo store
	// utxoStore.Store(ctx, tx)

	// delete spends set to false
	//utxoStore.Spend(ctx, spends)

	// txMetaStore := memory.New(ulogger.TestLogger{}, true)

	// v, err := validator.New(context.Background(), ulogger.TestLogger{}, utxoStore, txMetaStore, nil)
	// if err != nil {
	// 	panic(err)
	// }

	// err = v.Validate(context.Background(), tx)
	// require.NoError(t, err)

}

func TestValidate_InValidDoubleSpendTx(t *testing.T) {

}

func TestValidate_TxMetaStoreError(t *testing.T) {

}

func TestValidate_BlockAssemblyError(t *testing.T) {

}

func newTransaction(utxoStore *utxoMemorystore.Memory) {
	parentTx := bt.NewTx()
	parentTx.LockTime = 1
	parentTxHash := parentTx.TxIDChainHash()

	tx := bt.NewTx()
	_ = bt.Input{
		PreviousTxSatoshis: 201,
		PreviousTxScript:   &bscript.Script{},
		UnlockingScript:    &bscript.Script{},
		PreviousTxOutIndex: 0,
		SequenceNumber:     0,
	}

	err := tx.Inputs[0].PreviousTxIDAdd(parentTxHash)
	if err != nil {
		panic(err)
	}

	tx.AddOutput(&bt.Output{
		Satoshis:      100,
		LockingScript: &bscript.Script{},
	})
	tx.LockTime = 0
	//hash := tx.TxIDChainHash()

	utxoStore.Store(context.Background(), tx)

}
