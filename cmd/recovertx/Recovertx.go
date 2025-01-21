package recovertx

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/settings"
	utxostore "github.com/bitcoin-sv/teranode/stores/utxo"
	utxofactory "github.com/bitcoin-sv/teranode/stores/utxo/_factory"
	utxo2 "github.com/bitcoin-sv/teranode/test/stores/utxo"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

var simulate *bool

func RecoverTransaction(txIDHex string, blockHeight64 uint64, simulateFlag bool, spendingTxIDsStr ...string) {
	ctx := context.Background()
	logger := ulogger.New("recover_tx")
	tSettings := settings.NewSettings()

	txID, err := chainhash.NewHashFromStr(txIDHex)
	if err != nil {
		fmt.Println("Error: invalid txID: ", err.Error())
		os.Exit(1)
	}

	if blockHeight64 > math.MaxUint32 {
		fmt.Println("Error: block height too large")
		os.Exit(1)
	}

	blockHeight := uint32(blockHeight64)

	// if simulateFlag is true, set simulate to true
	simulate := simulateFlag

	var spendingTxIDs []*chainhash.Hash

	if len(spendingTxIDsStr) > 0 {
		// split the comma separated list of spending tx ids
		spendingTxHexs := strings.Split(spendingTxIDsStr[0], ",")

		spendingTxIDs = make([]*chainhash.Hash, len(spendingTxHexs))

		for i, spendingTxHex := range spendingTxHexs {
			if len(spendingTxHex) == 0 {
				spendingTxIDs = append(spendingTxIDs, nil)
				continue
			}

			spendingTxID, err := chainhash.NewHashFromStr(spendingTxHex)
			if err != nil {
				fmt.Println("Error: invalid spending tx id: ", err.Error())
				os.Exit(1)
			}

			spendingTxIDs[i] = spendingTxID
		}
	}

	utxoStore, err := utxofactory.NewStore(ctx, logger, tSettings, "main", false)
	if err != nil {
		fmt.Println("error creating utxostore: ", err.Error())
		os.Exit(1)
	}

	if err = recoverTx(ctx, utxoStore, txID, blockHeight, spendingTxIDs); err != nil {
		fmt.Println("error recovering transaction: ", err.Error())
		return
	} else if simulate {
		fmt.Println("Transaction recovery simulation done")
		return
	}

	fmt.Println("Transaction recovered successfully")
}

func recoverTx(ctx context.Context, utxoStore utxostore.Store, txID *chainhash.Hash, blockHeight uint32, spendingTxIDs []*chainhash.Hash) error {
	if len(spendingTxIDs) > 0 {
		return errors.NewError("this function does not work anymore since the utxo changes. Please update the function if needed")
	}

	// get the transaction from whatsonchain
	txHex, err := fetchTransactionHex(txID.String())
	if err != nil {
		return errors.NewProcessingError("error fetching transaction: %s", err)
	}

	tx, err := bt.NewTxFromString(txHex)
	if err != nil {
		return errors.NewProcessingError("error creating transaction: %s", err)
	}

	if err = EnrichStandardWOC(tx); err != nil {
		return errors.NewProcessingError("error enriching transaction: %s", err)
	}

	// recover the transaction
	fmt.Printf("Recovering transaction %s\n", tx.TxIDChainHash())

	if simulate != nil && *simulate {
		fmt.Printf("Fetched transaction: %s\n", hex.EncodeToString(tx.ExtendedBytes()))
	}

	fmt.Printf("Spending transaction outputs: %v\n", spendingTxIDs)

	// check for existence of the transaction in the utxo store
	// if it exists, return an error
	exists, err := utxoStore.Get(ctx, tx.TxIDChainHash())
	if err != nil && !errors.Is(err, errors.ErrTxNotFound) {
		return errors.NewProcessingError("error checking for existing transaction: %s", err)
	}

	if exists != nil {
		return errors.NewProcessingError("transaction already exists in the utxo store")
	}

	// create a new transaction in the utxo store
	//nolint:gosec
	if simulate == nil || !*simulate {
		_, err = utxoStore.Create(ctx, tx, blockHeight)
		if err != nil {
			return errors.NewProcessingError("error creating transaction in utxo store: %s", err)
		}

		// set the spends for the transaction
		spends := make([]*utxostore.Spend, 0)

		for i, spendingTxID := range spendingTxIDs {
			if spendingTxID == nil {
				continue
			}

			utxoHash, err := util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[i], uint32(i))
			if err != nil {
				return errors.NewProcessingError("error calculating utxo hash: %s", err)
			}

			spends = append(spends, &utxostore.Spend{
				TxID:         tx.TxIDChainHash(),
				Vout:         uint32(i),
				SpendingTxID: spendingTxID,
				UTXOHash:     utxoHash,
			})
		}

		if len(spends) > 0 {
			// FAKE TX to pass linter
			spendingTx := utxo2.GetSpendingTx(tx, 0)

			// OLD if err = utxoStore.Spend(ctx, spends, blockHeight); err != nil {
			if _, err = utxoStore.Spend(ctx, spendingTx); err != nil {
				return errors.NewProcessingError("error setting spends for transaction: %s", err)
			}
		}
	}

	return nil
}

func EnrichStandardWOC(tx *bt.Tx) error {
	parentTxs := make(map[string]*bt.Tx)

	for i, input := range tx.Inputs {
		parentTx, ok := parentTxs[input.PreviousTxIDChainHash().String()]
		if !ok {
			// get the parent tx and store in the map
			parentTxID := input.PreviousTxIDChainHash().String()

			parentTxHex, err := fetchTransactionHex(parentTxID)
			if err != nil {
				return err
			}

			parentTx, err = bt.NewTxFromString(parentTxHex)
			if err != nil {
				return err
			}

			parentTxs[parentTxID] = parentTx
		}

		// add the parent tx output to the input
		previousScript, err := hex.DecodeString(parentTx.Outputs[input.PreviousTxOutIndex].LockingScript.String())
		if err != nil {
			return err
		}

		tx.Inputs[i].PreviousTxScript = bscript.NewFromBytes(previousScript)
		tx.Inputs[i].PreviousTxSatoshis = parentTx.Outputs[input.PreviousTxOutIndex].Satoshis
	}

	return nil
}

func fetchTransactionHex(txIDHex string) (string, error) {
	time.Sleep(500 * time.Millisecond) // overcome rate limit 3 RPS

	network, _ := gocore.Config().Get("network", "mainnet")

	networkForWoC := "main"
	if network == "testnet" {
		networkForWoC = "test"
	}

	resp, err := http.Get(fmt.Sprintf("https://api.whatsonchain.com/v1/bsv/%s/tx/%s/hex", networkForWoC, txIDHex))
	if err != nil {
		return "", err
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return string(body), nil
}
