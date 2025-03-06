// Package alert implements the Bitcoin SV alert system server and related functionality.
package alert

import (
	"context"

	"github.com/bitcoin-sv/alert-system/app/config"
	"github.com/bitcoin-sv/go-sdk/script"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/services/blockassembly"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/stores/utxo/fields"
	"github.com/bitcoin-sv/teranode/tracing"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/libsv/go-bn/models"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
)

// Node implements the node interface for interacting with the Bitcoin network.
type Node struct {
	// logger handles logging operations
	logger ulogger.Logger

	// blockchainClient provides access to blockchain operations
	blockchainClient blockchain.ClientI

	// utxoStore manages UTXO operations
	utxoStore utxo.Store

	// blockassemblyClient handles block assembly operations
	blockassemblyClient *blockassembly.Client

	// settings contains node configuration
	settings *settings.Settings
}

// NewNodeConfig creates a new Node instance with the provided dependencies.
func NewNodeConfig(logger ulogger.Logger, blockchainClient blockchain.ClientI, utxoStore utxo.Store,
	blockassemblyClient *blockassembly.Client, tSettings *settings.Settings) config.NodeInterface {
	return &Node{
		logger:              logger,
		blockchainClient:    blockchainClient,
		utxoStore:           utxoStore,
		blockassemblyClient: blockassemblyClient,
		settings:            tSettings,
	}
}

// BestBlockHash returns the hash of the best block in the main chain.
func (n *Node) BestBlockHash(ctx context.Context) (string, error) {
	bestBlockHeader, _, err := n.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		return "", err
	}

	return bestBlockHeader.Hash().String(), nil
}

// GetRPCHost returns the RPC host address.
func (n *Node) GetRPCHost() string {
	return ""
}

// GetRPCPassword returns the RPC password.
func (n *Node) GetRPCPassword() string {
	return ""
}

// GetRPCUser returns the RPC username.
func (n *Node) GetRPCUser() string {
	return ""
}

// InvalidateBlock marks a block as invalid by its hash.
func (n *Node) InvalidateBlock(ctx context.Context, blockHashStr string) error {
	blockHash, err := chainhash.NewHashFromStr(blockHashStr)
	if err != nil {
		return err
	}

	return n.blockchainClient.InvalidateBlock(ctx, blockHash)
}

// BanPeer adds the peer's IP address to the ban list
// TODO depends on #913
func (n *Node) BanPeer(ctx context.Context, peer string) error {
	n.logger.Warnf("IMPLEMENT Banning peer %s", peer)
	return nil
}

// UnbanPeer removes the peer's IP address from the ban list
// TODO depends on #913
func (n *Node) UnbanPeer(ctx context.Context, peer string) error {
	n.logger.Warnf("IMPLEMENT Unbanning peer %s", peer)
	return nil
}

// AddToConsensusBlacklist adds funds to the consensus blacklist
// Sets a specified UTXO as un-spendable until further notice
func (n *Node) AddToConsensusBlacklist(ctx context.Context, funds []models.Fund) (*models.BlacklistResponse, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "AddToConsensusBlacklist",
		tracing.WithDebugLogMessage(n.logger, "[AddToConsensusBlacklist] called for %d funds", len(funds)),
	)
	defer deferFn()

	if len(funds) == 0 {
		return nil, errors.NewError("no funds to process")
	}

	// create a response object
	response := &models.BlacklistResponse{}

	// freeze one-by-one, getting the errors message for each one
	for _, fund := range funds {
		txHash, err := chainhash.NewHashFromStr(fund.TxOut.TxId)
		if err != nil {
			response.NotProcessed = append(response.NotProcessed, n.getAddToConsensusBlacklistResponse(fund, err))
			continue
		}

		// get the parent tx, to get the utxo hash of the spend
		parentTxMeta, err := n.utxoStore.Get(ctx, txHash, fields.Tx)
		if err != nil {
			response.NotProcessed = append(response.NotProcessed, n.getAddToConsensusBlacklistResponse(fund, err))
			continue
		}

		vout, err := util.SafeIntToUint32(fund.TxOut.Vout)
		if err != nil {
			response.NotProcessed = append(response.NotProcessed, n.getAddToConsensusBlacklistResponse(fund, err))
			continue
		}

		// calculate the utxo hash from the output script
		utxoHash, err := util.UTXOHashFromOutput(parentTxMeta.Tx.TxIDChainHash(), parentTxMeta.Tx.Outputs[fund.TxOut.Vout], vout)
		if err != nil {
			response.NotProcessed = append(response.NotProcessed, n.getAddToConsensusBlacklistResponse(fund, err))
			continue
		}

		// create a spend object
		spend := &utxo.Spend{
			TxID:     txHash,
			Vout:     vout,
			UTXOHash: utxoHash,
		}

		// check the height enforcement, if the height is below our current height, we are unfreezing, otherwise freezing
		if len(fund.EnforceAtHeight) > 0 && fund.EnforceAtHeight[0].Stop < int(n.utxoStore.GetBlockHeight()) {
			// unfreeze
			if err = n.utxoStore.UnFreezeUTXOs(ctx, []*utxo.Spend{spend}, n.settings); err != nil {
				response.NotProcessed = append(response.NotProcessed, n.getAddToConsensusBlacklistResponse(fund, err))
			}
		} else {
			// freeze
			if err = n.utxoStore.FreezeUTXOs(ctx, []*utxo.Spend{spend}, n.settings); err != nil {
				response.NotProcessed = append(response.NotProcessed, n.getAddToConsensusBlacklistResponse(fund, err))
			}
		}
	}

	return response, nil
}

func (n *Node) getAddToConsensusBlacklistResponse(fund models.Fund, err error) models.BlacklistNotProcessed {
	return models.BlacklistNotProcessed{
		TxOut: models.TxOut{
			TxId: fund.TxOut.TxId,
			Vout: fund.TxOut.Vout,
		},
		Reason: err.Error(),
	}
}

// AddToConfiscationTransactionWhitelist re-assigns a utxo to a confiscation transaction
// This allows the confiscation transaction to spend the utxo(s) in question
func (n *Node) AddToConfiscationTransactionWhitelist(ctx context.Context, txs []models.ConfiscationTransactionDetails) (*models.AddToConfiscationTransactionWhitelistResponse, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "AddToConfiscationTransactionWhitelist",
		tracing.WithDebugLogMessage(n.logger, "[AddToConfiscationTransactionWhitelist] called for %d txs", len(txs)),
	)
	defer deferFn()

	if len(txs) == 0 {
		return nil, errors.NewError("no transactions to process")
	}

	// create a response object
	response := &models.AddToConfiscationTransactionWhitelistResponse{}

	// re-assign one-by-one, getting the errors message for each one
	for _, txDetails := range txs {
		tx, err := bt.NewTxFromString(txDetails.ConfiscationTransaction.Hex)
		if err != nil {
			response.NotProcessed = append(response.NotProcessed, n.getAddToConfiscationTransactionWhitelistResponse("", err))
			continue
		}

		// get the parent txs of all the inputs of this transaction
		for _, txIn := range tx.Inputs {
			// get the parent tx, to get the utxo hash of the spend
			parentTxMeta, err := n.utxoStore.Get(ctx, txIn.PreviousTxIDChainHash(), fields.Tx)
			if err != nil {
				response.NotProcessed = append(response.NotProcessed, n.getAddToConfiscationTransactionWhitelistResponse(tx.TxIDChainHash().String(), err))
				continue
			}

			// check the satoshis are equal of the new input and the parent output
			if txIn.PreviousTxSatoshis > parentTxMeta.Tx.Outputs[txIn.PreviousTxOutIndex].Satoshis {
				response.NotProcessed = append(response.NotProcessed, n.getAddToConfiscationTransactionWhitelistResponse(tx.TxIDChainHash().String(), errors.NewError("new input satoshis are greater than parent output satoshis")))
				continue
			}

			// calculate the original utxo hash from the parent output script
			oldUtxoHash, err := util.UTXOHashFromOutput(txIn.PreviousTxIDChainHash(), parentTxMeta.Tx.Outputs[txIn.PreviousTxOutIndex], txIn.PreviousTxOutIndex)
			if err != nil {
				response.NotProcessed = append(response.NotProcessed, n.getAddToConfiscationTransactionWhitelistResponse(tx.TxIDChainHash().String(), err))
				continue
			}

			oldUtxo := &utxo.Spend{
				TxID:     txIn.PreviousTxIDChainHash(),
				Vout:     txIn.PreviousTxOutIndex,
				UTXOHash: oldUtxoHash,
			}

			// get the output script from the confiscation transaction input
			// the output script should be a generic P2PKH script, as the confiscation transaction is a special transaction

			// get the public key of the new input unlocking script
			publicKey, err := extractPublicKey(txIn.UnlockingScript.Bytes())
			if err != nil {
				response.NotProcessed = append(response.NotProcessed, n.getAddToConfiscationTransactionWhitelistResponse(tx.TxIDChainHash().String(), err))
				continue
			}

			// create a new locking script from the public key
			newLockingScript, err := bscript.NewP2PKHFromPubKeyBytes(publicKey)
			if err != nil {
				response.NotProcessed = append(response.NotProcessed, n.getAddToConfiscationTransactionWhitelistResponse(tx.TxIDChainHash().String(), err))
				continue
			}

			// create a new output with the same satoshis and the new locking script
			amendedOutputScript := &bt.Output{
				Satoshis:      parentTxMeta.Tx.Outputs[txIn.PreviousTxOutIndex].Satoshis, // same satoshis
				LockingScript: newLockingScript,                                          // new locking script
			}

			// the new utxo hash allows the original output to be spent by the confiscation transaction
			newUtxoHash, err := util.UTXOHashFromOutput(txIn.PreviousTxIDChainHash(), amendedOutputScript, txIn.PreviousTxOutIndex)
			if err != nil {
				response.NotProcessed = append(response.NotProcessed, n.getAddToConfiscationTransactionWhitelistResponse(tx.TxIDChainHash().String(), err))
				continue
			}

			newUtxo := &utxo.Spend{
				TxID:     txIn.PreviousTxIDChainHash(),
				Vout:     txIn.PreviousTxOutIndex,
				UTXOHash: newUtxoHash,
			}

			// re-assign the utxo
			if err = n.utxoStore.ReAssignUTXO(ctx, oldUtxo, newUtxo, n.settings); err != nil {
				response.NotProcessed = append(response.NotProcessed, n.getAddToConfiscationTransactionWhitelistResponse(tx.TxIDChainHash().String(), err))
			}
		}
	}

	return response, nil
}

func (n *Node) getAddToConfiscationTransactionWhitelistResponse(txID string, err error) models.WhitelistNotProcessed {
	return models.WhitelistNotProcessed{
		ConfiscationTransaction: models.WhitelistConfiscationTransaction{
			TxId: txID,
		},
		Reason: err.Error(),
	}
}

// extractPublicKey extracts the public key from a P2PKH scriptSig
// Assuming scriptSig is the unlocking script from the transaction input
func extractPublicKey(scriptSig []byte) ([]byte, error) {
	// Parse the scriptSig into an array of elements (OpCodes or Data pushes)
	parsedScript := script.NewFromBytes(scriptSig)

	elements, err := parsedScript.ParseOps()
	if err != nil {
		return nil, err
	}

	// For P2PKH, the last element is the public key (the second item in scriptSig)
	if len(elements) < 2 {
		return nil, errors.NewProcessingError("invalid P2PKH scriptSig")
	}

	publicKey := elements[len(elements)-1].Data // Last element is the public key

	return publicKey, nil
}
