package main

import (
	"context"
	"log"

	"github.com/bitcoin-sv/ubsv/services/propagation/propagation_api"
	"github.com/bitcoin-sv/ubsv/services/seeder/seeder_api"
	"github.com/bitcoin-sv/ubsv/services/utxo/utxostore_api"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

func main() {
	ctx := context.Background()

	seederGrpcAddress, ok := gocore.Config().Get("seeder_grpcAddress")
	if !ok {
		panic("no seeder_grpcAddress setting found")
	}

	sConn, err := utils.GetGRPCClient(context.Background(), seederGrpcAddress, &utils.ConnectionOptions{
		MaxRetries: 3,
	})
	if err != nil {
		panic(err)
	}

	utxostoreGrpcAddress, ok := gocore.Config().Get("utxostore_grpcAddress")
	if !ok {
		panic("no utxostore_grpcAddress setting found")
	}

	uConn, err := utils.GetGRPCClient(context.Background(), utxostoreGrpcAddress, &utils.ConnectionOptions{
		MaxRetries: 3,
	})
	if err != nil {
		panic(err)
	}

	propagationGrpcAddresses, ok := gocore.Config().GetMulti("propagation_grpcAddresses", "|")
	if !ok {
		panic("no propagation_grpcAddresses setting found")
	}

	// This code is expecting one address only.  Always use the first...
	pConn, err := utils.GetGRPCClient(context.Background(), propagationGrpcAddresses[0], &utils.ConnectionOptions{
		MaxRetries: 3,
	})
	if err != nil {
		panic(err)
	}

	seeder := seeder_api.NewSeederAPIClient(sConn)
	utxostore := utxostore_api.NewUtxoStoreAPIClient(uConn)
	propagator := propagation_api.NewPropagationAPIClient(pConn)

	if _, err := seeder.CreateSpendableTransactions(ctx, &seeder_api.CreateSpendableTransactionsRequest{
		NumberOfTransactions: 1,
		NumberOfOutputs:      1,
		SatoshisPerOutput:    1000,
	}); err != nil {
		panic(err)
	}

	res2, err := seeder.NextSpendableTransaction(ctx, &seeder_api.NextSpendableTransactionRequest{})
	if err != nil {
		panic(err)
	}
	if err != nil {
		panic(err)
	}

	log.Printf("Private key:         %x", res2.PrivateKey)
	log.Printf("TXID:                %x", res2.Txid)
	log.Printf("Number of outputs:   %d", res2.NumberOfOutputs)
	log.Printf("Satoshis per output: %d", res2.SatoshisPerOutput)
	log.Printf("Private key: %x", res2.PrivateKey)

	privateKey, publicKey := bec.PrivKeyFromBytes(bec.S256(), res2.PrivateKey)

	log.Printf("PrivateKey: %x", privateKey.Serialise())

	script, err := bscript.NewP2PKHFromPubKeyBytes(publicKey.SerialiseCompressed())
	if err != nil {
		panic(err)
	}

	txid, err := chainhash.NewHash(res2.Txid)
	if err != nil {
		panic(err)
	}

	utxoHash, err := util.UTXOHash(txid, 0, *script, 1000)
	if err != nil {
		panic(err)
	}

	log.Printf("Chainhash:  %x", utxoHash.CloneBytes())

	res3, err := utxostore.Get(ctx, &utxostore_api.GetRequest{
		UxtoHash: utxoHash.CloneBytes(),
	})
	if err != nil {
		panic(err)
	}

	log.Printf("UTXO found: %v", res3.Status)

	tx := bt.NewTx()

	txHash, err := chainhash.NewHash(res2.Txid)
	if err != nil {
		panic(err)
	}

	if err := tx.FromUTXOs(&bt.UTXO{
		TxIDHash:      txHash,
		Vout:          0,
		LockingScript: script,
		Satoshis:      res2.SatoshisPerOutput,
	}); err != nil {
		panic(err)
	}

	if err := tx.PayTo(script, res2.SatoshisPerOutput); err != nil {
		panic(err)
	}

	unlockerGetter := unlocker.Getter{PrivateKey: privateKey}

	if err := tx.FillAllInputs(context.Background(), &unlockerGetter); err != nil {
		panic(err)
	}

	if _, err := propagator.ProcessTransaction(ctx, &propagation_api.ProcessTransactionRequest{
		Tx: tx.ExtendedBytes(),
	}); err != nil {
		panic(err)
	}

	res4, err := utxostore.Spend(ctx, &utxostore_api.SpendRequest{
		UxtoHash:     utxoHash.CloneBytes(),
		SpendingTxid: utxoHash.CloneBytes(),
	})
	if err != nil {
		panic(err)
	}

	log.Printf("UTXO spent: %v", res4.Status)

}
