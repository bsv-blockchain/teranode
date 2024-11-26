package recovertx

import (
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/settings"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	utxofactory "github.com/bitcoin-sv/ubsv/stores/utxo/_factory"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

var simulate *bool

func Start() {
	simulate = flag.Bool("simulate", false, "simulate the recovery without actually updating the utxo store")

	flag.Parse()

	args := flag.Args()
	fmt.Println(args)

	// read command line arguments
	if len(args) < 2 {
		fmt.Println("Usage: recover_tx <txID> <blockHeight> [comma separated list of spending tx ids]")
		os.Exit(1)
	}

	ctx := context.Background()
	logger := ulogger.New("recover_tx")
	tSettings := settings.NewSettings()
	blockHeightStr := args[1]

	blockHeight64, err := strconv.ParseUint(blockHeightStr, 10, 32)
	if err != nil {
		fmt.Println("Error: invalid block height: ", err.Error())
		os.Exit(1)
	}

	if blockHeight64 > math.MaxUint32 {
		fmt.Println("Error: block height too large")
		os.Exit(1)
	}

	blockHeight := uint32(blockHeight64)

	// read hex data from command line, or file from os if in the format @filename.hex
	txIDHex := args[0]

	txID, err := chainhash.NewHashFromStr(txIDHex)
	if err != nil {
		fmt.Println("Error: invalid txID: ", err.Error())
		os.Exit(1)
	}

	var spendingTxIDs []*chainhash.Hash

	if len(args) > 2 {
		spendingTxHexStr := args[2]

		// split the comma separated list of spending tx ids
		spendingTxHexs := strings.Split(spendingTxHexStr, ",")

		spendingTxIDs = make([]*chainhash.Hash, len(spendingTxHexs))

		for i, spendingTxHex := range spendingTxHexs {
			if len(spendingTxHex) == 0 {
				spendingTxIDs[i] = nil
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
	} else if simulate != nil && *simulate {
		fmt.Println("Transaction recovery simulation done")
		return
	}

	fmt.Println("Transaction recovered successfully")
}

func recoverTx(ctx context.Context, utxoStore utxostore.Store, txID *chainhash.Hash, blockHeight uint32, spendingTxIDs []*chainhash.Hash) error {
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
	if err != nil {
		if !errors.Is(err, errors.ErrTxNotFound) {
			return errors.NewProcessingError("error checking for existing transaction: %s", err)
		}
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
			if err = utxoStore.Spend(ctx, spends, blockHeight); err != nil {
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
