package main

import (
	"context"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/legacy/netsync"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/libsv/go-bt/v2/unlocker"
)

var (
	genesisBlockHash *chainhash.Hash
	nBits            *model.NBit
	privateKey       *bec.PrivateKey
	publicKey        *bec.PublicKey
	address          *bscript.Address
	coinbase1        *bt.Tx
	coinbase2        *bt.Tx
	coinbase3        *bt.Tx
	baseTime         uint32

	tx1 *bt.Tx
	tx2 *bt.Tx
	tx3 *bt.Tx
	tx4 *bt.Tx

	block1 *model.Block
	block2 *model.Block
	block3 *model.Block
)

func init() {
	var err error

	genesisBlockHash, err = chainhash.NewHashFromStr("0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206")
	if err != nil {
		panic(err)
	}

	nBits, err = model.NewNBitFromString("207fffff")
	if err != nil {
		panic(err)
	}

	privateKey, err = bec.NewPrivateKey(bec.S256())
	if err != nil {
		panic(err)
	}

	publicKey = privateKey.PubKey()

	address, err = bscript.NewAddressFromPublicKey(publicKey, true)
	if err != nil {
		panic(err)
	}

	coinbase1 = bt.NewTx()

	err = coinbase1.AddP2PKHOutputFromAddress(address.AddressString, 50e8)
	if err != nil {
		panic(err)
	}

	err = coinbase1.FillAllInputs(context.Background(), &unlocker.Getter{PrivateKey: privateKey})
	if err != nil {
		panic(err)
	}

	coinbase2 = bt.NewTx()

	err = coinbase2.AddP2PKHOutputFromAddress(address.AddressString, 50e8)
	if err != nil {
		panic(err)
	}

	err = coinbase2.FillAllInputs(context.Background(), &unlocker.Getter{PrivateKey: privateKey})
	if err != nil {
		panic(err)
	}

	coinbase3 = bt.NewTx()

	err = coinbase3.AddP2PKHOutputFromAddress(address.AddressString, 50e8)
	if err != nil {
		panic(err)
	}

	err = coinbase3.FillAllInputs(context.Background(), &unlocker.Getter{PrivateKey: privateKey})
	if err != nil {
		panic(err)
	}

	// nolint: gosec
	baseTime = uint32(time.Date(2024, time.September, 10, 9, 0, 0, 0, time.UTC).Unix())

	tx1, err = spendTx(coinbase1, 0, 40e8)
	if err != nil {
		panic(err)
	}

	tx2, err = spendExternalTx(tx1, 0)
	if err != nil {
		panic(err)
	}

	tx3, err = spendTx(tx1, 1, 10e8)
	if err != nil {
		panic(err)
	}

	tx4, err = spendTx(tx2, 0, 40e8/200)
	if err != nil {
		panic(err)
	}
}

func main() {
	err := createBlocks()
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := ulogger.New("sim")

	sm, err := netsync.New(
		ctx,
		logger,
		nil, // blockchain.ClientI,
		nil, // validator.Interface,
		nil, // utxo.Store,
		nil, // subtree store blob.Store,
		nil, // temp store blob.Store,
		nil, // subtreeValidation subtreevalidation.Interface,
		nil, // blockValidation blockvalidation.Interface,
		nil, // config *netsync.Config
		nil,
	)
	if err != nil {
		panic(err)
	}

	err = sm.ProcessBlock(ctx, block1)
	if err != nil {
		panic(err)
	}

	err = sm.ProcessBlock(ctx, block2)
	if err != nil {
		panic(err)
	}

	err = sm.ProcessBlock(ctx, block3)
	if err != nil {
		panic(err)
	}
}

func spendTx(parentTx *bt.Tx, vout int, satoshis uint64) (*bt.Tx, error) {
	tx := bt.NewTx()

	// nolint: gosec
	err := tx.FromUTXOs(&bt.UTXO{
		TxIDHash:      parentTx.TxIDChainHash(),
		Vout:          uint32(vout),
		LockingScript: parentTx.Outputs[vout].LockingScript,
		Satoshis:      parentTx.Outputs[vout].Satoshis,
	})
	if err != nil {
		return nil, err
	}

	err = tx.AddP2PKHOutputFromAddress(address.AddressString, satoshis)
	if err != nil {
		return nil, err
	}

	err = tx.AddP2PKHOutputFromAddress(address.AddressString, parentTx.Outputs[vout].Satoshis-satoshis)
	if err != nil {
		return nil, err
	}

	err = tx.FillAllInputs(context.Background(), &unlocker.Getter{PrivateKey: privateKey})
	if err != nil {
		return nil, err
	}

	return tx, nil
}

func spendExternalTx(parentTx *bt.Tx, vout int) (*bt.Tx, error) {
	tx := bt.NewTx()

	// nolint: gosec
	err := tx.FromUTXOs(&bt.UTXO{
		TxIDHash:      parentTx.TxIDChainHash(),
		Vout:          uint32(vout),
		LockingScript: parentTx.Outputs[vout].LockingScript,
		Satoshis:      parentTx.Outputs[vout].Satoshis,
	})
	if err != nil {
		return nil, err
	}

	for i := 0; i < 200; i++ {
		err = tx.AddP2PKHOutputFromAddress(address.AddressString, parentTx.Outputs[vout].Satoshis/200)
		if err != nil {
			return nil, err
		}
	}

	err = tx.FillAllInputs(context.Background(), &unlocker.Getter{PrivateKey: privateKey})
	if err != nil {
		return nil, err
	}

	return tx, nil
}

func createBlocks() error {
	var err error

	// nolint: gosec
	header1 := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  genesisBlockHash,
		HashMerkleRoot: coinbase1.TxIDChainHash(),
		Timestamp:      baseTime + 1,
		Bits:           *nBits,
	}

	mine(header1)

	ok, _, _ := header1.HasMetTargetDifficulty()
	if !ok {
		return errors.NewProcessingError("header1 does not have met target difficulty")
	}

	block1, err = model.NewBlock(header1, coinbase1, nil, 1, 0, 1, 1)
	if err != nil {
		return err
	}

	// Block 2
	subtree2, err := util.NewIncompleteTreeByLeafCount(4)
	if err != nil {
		return err
	}

	err = subtree2.AddCoinbaseNode()
	if err != nil {
		return err
	}

	err = subtree2.AddNode(*tx1.TxIDChainHash(), 0, uint64(tx1.Size()))
	if err != nil {
		return err
	}

	err = subtree2.AddNode(*tx2.TxIDChainHash(), 0, uint64(tx2.Size()))
	if err != nil {
		return err
	}

	merkleRoot2, err := subtree2.RootHashWithReplaceRootNode(coinbase2.TxIDChainHash(), 0, 0)
	if err != nil {
		return err
	}

	// nolint: gosec
	header2 := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  block1.Hash(),
		HashMerkleRoot: merkleRoot2,
		Timestamp:      baseTime + 2,
		Bits:           *nBits,
	}

	mine(header2)

	ok, _, _ = header2.HasMetTargetDifficulty()
	if !ok {
		return errors.NewProcessingError("header2 does not have met target difficulty")
	}

	block2, err = model.NewBlock(header2, coinbase2, []*chainhash.Hash{subtree2.RootHash()}, 3, 0, 2, 2)
	if err != nil {
		return err
	}

	// Block 3
	subtree3, err := util.NewIncompleteTreeByLeafCount(4)
	if err != nil {
		return err
	}

	err = subtree3.AddCoinbaseNode()
	if err != nil {
		return err
	}

	err = subtree3.AddNode(*tx3.TxIDChainHash(), 0, uint64(tx3.Size()))
	if err != nil {
		return err
	}

	err = subtree3.AddNode(*tx4.TxIDChainHash(), 0, uint64(tx4.Size()))
	if err != nil {
		return err
	}

	merkleRoot3, err := subtree3.RootHashWithReplaceRootNode(coinbase3.TxIDChainHash(), 0, 0)
	if err != nil {
		return err
	}

	// nolint: gosec
	header3 := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  block2.Hash(),
		HashMerkleRoot: merkleRoot3,
		Timestamp:      baseTime + 3,
		Bits:           *nBits,
	}

	mine(header3)

	ok, _, _ = header3.HasMetTargetDifficulty()
	if !ok {
		return errors.NewProcessingError("header3 does not have met target difficulty")
	}

	block3, err = model.NewBlock(header3, coinbase3, []*chainhash.Hash{subtree3.RootHash()}, 3, 0, 3, 3)
	if err != nil {
		return err
	}

	return nil
}

func mine(header *model.BlockHeader) {
	for {
		ok, _, _ := header.HasMetTargetDifficulty()
		if ok {
			return
		}

		header.Nonce++
	}
}
