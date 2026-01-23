// Package alert implements the Bitcoin SV alert system server and related functionality.
//
// The node.go file contains the Node implementation that provides integration
// with the Bitcoin network for the alert system. It enables interactions with
// blockchain data, UTXO management, and peer communication, supporting the
// alert system's ability to respond to and affect network conditions.
package alert

import (
	"context"
	"time"

	"github.com/bsv-blockchain/go-alert-system/app/config"
	"github.com/bsv-blockchain/go-bn/models"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/services/legacy/peer_api"
	"github.com/bsv-blockchain/teranode/services/p2p"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
)

// Node implements the config.NodeInterface for interacting with the Bitcoin network.
// It provides the alert system with access to blockchain data, UTXO management,
// peer communications, and block assembly operations. The Node encapsulates all
// the functionality needed for the alert system to gather network information and
// enforce consensus rules such as transaction blacklisting and confiscation.
//
// The Node acts as an adapter between the alert system and the underlying Bitcoin
// network components, isolating the alert system from the specific implementation
// details of blockchain access, UTXO management, and peer communication.
type Node struct {
	// logger handles logging operations for the Node instance
	// It provides consistent logging across all Node operations
	logger ulogger.Logger

	// blockchainClient provides access to blockchain operations such as
	// retrieving block headers, invalidating blocks, and querying chain state
	blockchainClient blockchain.ClientI

	// utxoStore manages UTXO operations including blacklisting UTXOs and
	// managing locked flags for confiscation transactions
	utxoStore utxo.Store

	// blockassemblyClient handles block assembly operations, allowing the
	// alert system to interact with block creation processes
	blockassemblyClient blockassembly.ClientI

	// peerClient handles peer operations such as banning and unbanning peers
	// based on alert system decisions
	peerClient peer.ClientI

	// p2pClient handles p2p network operations for communication with other nodes
	p2pClient p2p.ClientI

	// settings contains node configuration parameters from the teranode settings system
	settings *settings.Settings
}

// NewNodeConfig creates a new Node instance with the provided dependencies.
// This factory function creates and initializes a Node that implements the config.NodeInterface
// required by the alert system. All necessary dependencies are injected, making the Node
// testable and configurable with different implementations of its dependencies.
//
// Parameters:
//   - logger: The logger instance for Node operations
//   - blockchainClient: Interface for accessing blockchain operations
//   - utxoStore: Store for managing UTXO-related operations
//   - blockassemblyClient: Client for interacting with block assembly
//   - peerClient: Client for managing peer operations
//   - p2pClient: Client for P2P network communications
//   - tSettings: Configuration settings for the Node
//
// Returns:
//   - config.NodeInterface: A fully initialized Node instance that satisfies the required interface
func NewNodeConfig(logger ulogger.Logger, blockchainClient blockchain.ClientI, utxoStore utxo.Store,
	blockassemblyClient blockassembly.ClientI, peerClient peer.ClientI, p2pClient p2p.ClientI, tSettings *settings.Settings) config.NodeInterface {
	return &Node{
		logger:              logger,
		blockchainClient:    blockchainClient,
		utxoStore:           utxoStore,
		blockassemblyClient: blockassemblyClient,
		peerClient:          peerClient,
		p2pClient:           p2pClient,
		settings:            tSettings,
	}
}

// BestBlockHash returns the hash of the best block in the main chain.
// This method is part of the config.NodeInterface and provides the alert system
// with information about the current chain tip.
//
// Parameters:
//   - ctx: Context for the operation, allowing for cancellation and timeouts
//
// Returns:
//   - string: The hash of the best block in hex string format
//   - error: Any error encountered while retrieving the best block hash
func (n *Node) BestBlockHash(ctx context.Context) (string, error) {
	bestBlockHeader, _, err := n.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		return "", err
	}

	return bestBlockHeader.Hash().String(), nil
}

// GetRPCHost returns the RPC host address.
// This method is part of the config.NodeInterface but is not used in the current implementation.
// It exists for interface compatibility with systems that expect RPC connections.
//
// Returns:
//   - string: An empty string as direct RPC connections are not used in this implementation
func (n *Node) GetRPCHost() string {
	return ""
}

// GetRPCPassword returns the RPC password.
// This method is part of the config.NodeInterface but is not used in the current implementation.
// It exists for interface compatibility with systems that expect RPC connections.
//
// Returns:
//   - string: An empty string as direct RPC connections are not used in this implementation
func (n *Node) GetRPCPassword() string {
	return ""
}

// GetRPCUser returns the RPC username.
// This method is part of the config.NodeInterface but is not used in the current implementation.
// It exists for interface compatibility with systems that expect RPC connections.
//
// Returns:
//   - string: An empty string as direct RPC connections are not used in this implementation
func (n *Node) GetRPCUser() string {
	return ""
}

// InvalidateBlock marks a block as invalid by its hash.
// This method is part of the config.NodeInterface and allows the alert system
// to respond to certain conditions by invalidating blocks, which can trigger
// reorganization of the blockchain.
//
// Parameters:
//   - ctx: Context for the operation, allowing for cancellation and timeouts
//   - blockHashStr: The hash of the block to invalidate, in hex string format
//
// Returns:
//   - error: Any error encountered during the invalidation process
func (n *Node) InvalidateBlock(ctx context.Context, blockHashStr string) error {
	n.logger.Infof("[InvalidateBlock] Alert service invalidating block %s", blockHashStr)

	blockHash, err := chainhash.NewHashFromStr(blockHashStr)
	if err != nil {
		return err
	}

	invalidatedHashes, err := n.blockchainClient.InvalidateBlock(ctx, blockHash)
	if err != nil {
		return err
	}

	n.logger.Infof("[InvalidateBlock] Invalidated %d blocks: %v", len(invalidatedHashes), invalidatedHashes)
	return nil
}

// BanPeer adds the peer's IP address to the ban list for both P2P and legacy peers.
// This method is part of the config.NodeInterface and allows the alert system to
// enforce bans on malicious or misbehaving peers. It attempts to ban the peer in both
// the P2P and legacy peer subsystems, ensuring comprehensive coverage.
//
// Parameters:
//   - ctx: Context for the operation, allowing for cancellation and timeouts
//   - peer: The peer address to ban, in the format "IP:port" or "IP"
//
// Returns:
//   - error: Any error encountered during the ban process, or if the ban failed on all systems
func (n *Node) BanPeer(ctx context.Context, peer string) error {
	var banned bool

	// ban p2p peer for 100 years
	until := time.Now().Add(24 * 365 * 100 * time.Hour).Unix()
	err := n.p2pClient.BanPeer(ctx, peer, until)
	if err == nil {
		banned = true
	} else {
		return err
	}

	// ban legacy peer
	legacyResp, err := n.peerClient.BanPeer(ctx, &peer_api.BanPeerRequest{Addr: peer, Until: time.Now().Add(24 * 365 * 100 * time.Hour).Unix()})
	if err != nil {
		return err
	}

	if !legacyResp.Ok {
		banned = true
	}

	if !banned {
		return errors.NewError("failed to ban peer %s", peer)
	}

	return nil
}

// UnbanPeer removes the peer's IP address from the ban list for both P2P and legacy peers.
// This method is part of the config.NodeInterface and allows the alert system to
// lift previously applied bans. It attempts to unban the peer in both the P2P and legacy
// peer subsystems, ensuring comprehensive removal of the ban.
//
// Parameters:
//   - ctx: Context for the operation, allowing for cancellation and timeouts
//   - peer: The peer address to unban, in the format "IP:port" or "IP"
//
// Returns:
//   - error: Any error encountered during the unban process, or if the unban failed on all systems
func (n *Node) UnbanPeer(ctx context.Context, peer string) error {
	var unbanned bool

	// unban p2p peer
	err := n.p2pClient.UnbanPeer(ctx, peer)
	if err == nil {
		unbanned = true
	} else {
		return err
	}

	// unban legacy peer
	legacyResp, err := n.peerClient.UnbanPeer(ctx, &peer_api.UnbanPeerRequest{Addr: peer})
	if err != nil {
		return err
	}

	if !legacyResp.Ok {
		unbanned = true
	}

	if !unbanned {
		return errors.NewError("failed to unban peer %s", peer)
	}

	return nil
}

// AddToConsensusBlacklist adds funds to the consensus blacklist.
// This method is a critical enforcement mechanism that sets specified UTXOs as
// un-spendable until further notice. It implements a key function of the alert system,
// allowing authorities to freeze suspect funds or prevent unauthorized spending.
//
// The method processes each fund individually, ensuring thorough error handling and
// comprehensive status reporting for each UTXO. It creates a specialized marker in
// the UTXO store that prevents the inputs from being spent in future transactions.
//
// Parameters:
//   - ctx: Context for the operation, allowing for cancellation and timeouts
//   - funds: An array of Fund objects identifying UTXOs to blacklist, each containing
//     transaction ID, output index, and additional metadata
//
// Returns:
//   - *models.BlacklistResponse: Response containing processed and unprocessed funds with status
//   - error: Any error encountered during the overall blacklisting process
func (n *Node) AddToConsensusBlacklist(ctx context.Context, funds []models.Fund) (*models.AddToConsensusBlacklistResponse, error) {
	ctx, _, deferFn := tracing.Tracer("alert").Start(ctx, "AddToConsensusBlacklist",
		tracing.WithDebugLogMessage(n.logger, "[AddToConsensusBlacklist] called for %d funds", len(funds)),
	)
	defer deferFn()

	if len(funds) == 0 {
		return nil, errors.NewError("no funds to process")
	}

	// create a response object
	response := &models.AddToConsensusBlacklistResponse{}

	// freeze one-by-one, getting the errors message for each one
	for _, fund := range funds {
		txHash, err := chainhash.NewHashFromStr(fund.TxOut.TxId)
		if err != nil {
			response.NotProcessed = append(response.NotProcessed, n.getAddToConsensusBlacklistResponse(fund, err)...)
			continue
		}

		// get the parent tx, to get the utxo hash of the spend
		parentTxMeta, err := n.utxoStore.Get(ctx, txHash, fields.Tx)
		if err != nil {
			response.NotProcessed = append(response.NotProcessed, n.getAddToConsensusBlacklistResponse(fund, err)...)
			continue
		}

		vout, err := safeconversion.IntToUint32(fund.TxOut.Vout)
		if err != nil {
			response.NotProcessed = append(response.NotProcessed, n.getAddToConsensusBlacklistResponse(fund, err)...)
			continue
		}

		// calculate the utxo hash from the output script
		utxoHash, err := util.UTXOHashFromOutput(parentTxMeta.Tx.TxIDChainHash(), parentTxMeta.Tx.Outputs[fund.TxOut.Vout], vout)
		if err != nil {
			response.NotProcessed = append(response.NotProcessed, n.getAddToConsensusBlacklistResponse(fund, err)...)
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
				response.NotProcessed = append(response.NotProcessed, n.getAddToConsensusBlacklistResponse(fund, err)...)
			}
		} else {
			// freeze
			if err = n.utxoStore.FreezeUTXOs(ctx, []*utxo.Spend{spend}, n.settings); err != nil {
				response.NotProcessed = append(response.NotProcessed, n.getAddToConsensusBlacklistResponse(fund, err)...)
			}
		}
	}

	return response, nil
}

// getAddToConsensusBlacklistResponse creates a standardized response for failed blacklist operations.
// This helper method formats consistent response objects for UTXOs that could not be blacklisted,
// including the original fund details and the specific error that prevented processing.
//
// Parameters:
//   - fund: The Fund object that failed to be blacklisted
//   - err: The specific error that prevented blacklisting
//
// Returns:
//   - models.BlacklistNotProcessed: A structured response object containing the fund and error details
func (n *Node) getAddToConsensusBlacklistResponse(fund models.Fund, err error) models.AddToConsensusBlacklistNotProcessed {
	return models.AddToConsensusBlacklistNotProcessed{
		{
			TxOut: models.TxOut{
				TxId: fund.TxOut.TxId,
				Vout: fund.TxOut.Vout,
			},
			Reason: err.Error(),
		},
	}
}

// AddToConfiscationTransactionWhitelist re-assigns UTXOs to a confiscation transaction.
// This critical enforcement mechanism allows authorized confiscation transactions to spend
// previously frozen UTXOs. The method validates each transaction and its inputs, ensuring
// that the confiscation transaction is properly formatted and that the inputs refer to
// valid, frozen UTXOs.
//
// For each confiscation transaction, the method:
// - Validates the transaction format and structure
// - Verifies each input against its parent transaction
// - Extracts the public key from the unlocking script
// - Creates a new locking script for the UTXO based on the public key
// - Updates the UTXO store to allow the confiscation transaction to spend the UTXO
//
// Parameters:
//   - ctx: Context for the operation, allowing for cancellation and timeouts
//   - txs: Array of confiscation transaction details containing transaction hex data
//
// Returns:
//   - *models.AddToConfiscationTransactionWhitelistResponse: Response containing processing status for each transaction
//   - error: Any error encountered during the overall whitelisting process
func (n *Node) AddToConfiscationTransactionWhitelist(ctx context.Context, txs []models.ConfiscationTransactionDetails) (*models.AddToConfiscationTransactionWhitelistResponse, error) {
	ctx, _, deferFn := tracing.Tracer("alert").Start(ctx, "AddToConfiscationTransactionWhitelist",
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
			response.NotProcessed = append(response.NotProcessed, n.getAddToConfiscationTransactionWhitelistResponse("", err)...)
			continue
		}

		// get the parent txs of all the inputs of this transaction
		for _, txIn := range tx.Inputs {
			// get the parent tx, to get the utxo hash of the spend
			parentTxMeta, err := n.utxoStore.Get(ctx, txIn.PreviousTxIDChainHash(), fields.Tx)
			if err != nil {
				response.NotProcessed = append(response.NotProcessed, n.getAddToConfiscationTransactionWhitelistResponse(tx.TxIDChainHash().String(), err)...)
				continue
			}

			// check the satoshis are equal of the new input and the parent output
			if txIn.PreviousTxSatoshis > parentTxMeta.Tx.Outputs[txIn.PreviousTxOutIndex].Satoshis {
				response.NotProcessed = append(response.NotProcessed, n.getAddToConfiscationTransactionWhitelistResponse(tx.TxIDChainHash().String(), errors.NewError("new input satoshis are greater than parent output satoshis"))...)
				continue
			}

			// calculate the original utxo hash from the parent output script
			oldUtxoHash, err := util.UTXOHashFromOutput(txIn.PreviousTxIDChainHash(), parentTxMeta.Tx.Outputs[txIn.PreviousTxOutIndex], txIn.PreviousTxOutIndex)
			if err != nil {
				response.NotProcessed = append(response.NotProcessed, n.getAddToConfiscationTransactionWhitelistResponse(tx.TxIDChainHash().String(), err)...)
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
				response.NotProcessed = append(response.NotProcessed, n.getAddToConfiscationTransactionWhitelistResponse(tx.TxIDChainHash().String(), err)...)
				continue
			}

			// create a new locking script from the public key
			newLockingScript, err := bscript.NewP2PKHFromPubKeyBytes(publicKey)
			if err != nil {
				response.NotProcessed = append(response.NotProcessed, n.getAddToConfiscationTransactionWhitelistResponse(tx.TxIDChainHash().String(), err)...)
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
				response.NotProcessed = append(response.NotProcessed, n.getAddToConfiscationTransactionWhitelistResponse(tx.TxIDChainHash().String(), err)...)
				continue
			}

			newUtxo := &utxo.Spend{
				TxID:     txIn.PreviousTxIDChainHash(),
				Vout:     txIn.PreviousTxOutIndex,
				UTXOHash: newUtxoHash,
			}

			// re-assign the utxo
			if err = n.utxoStore.ReAssignUTXO(ctx, oldUtxo, newUtxo, n.settings); err != nil {
				response.NotProcessed = append(response.NotProcessed, n.getAddToConfiscationTransactionWhitelistResponse(tx.TxIDChainHash().String(), err)...)
			}
		}
	}

	return response, nil
}

// getAddToConfiscationTransactionWhitelistResponse creates a standardized response for failed whitelist operations.
// This helper method formats consistent response objects for confiscation transactions that could
// not be whitelisted, including the transaction ID and the specific error that prevented processing.
//
// Parameters:
//   - txID: The transaction ID of the confiscation transaction that failed to be whitelisted
//   - err: The specific error that prevented whitelisting
//
// Returns:
//   - models.WhitelistNotProcessed: A structured response object containing the transaction ID and error details
func (n *Node) getAddToConfiscationTransactionWhitelistResponse(txID string, err error) models.AddToConfiscationTransactionWhitelistNotProcessed {
	return models.AddToConfiscationTransactionWhitelistNotProcessed{
		{
			ConfiscationTransaction: models.WhitelistConfiscationTransaction{
				TxId: txID,
			},
			Reason: err.Error(),
		},
	}
}

// extractPublicKey extracts the public key from a P2PKH scriptSig.
// This helper function parses the unlocking script (scriptSig) from a transaction input
// to extract the public key used for signing. In P2PKH inputs, the scriptSig contains
// both the signature and the public key in a specific format.
//
// The function handles the standard P2PKH script format:
// [signature] [public key]
//
// Parameters:
//   - scriptSig: Raw bytes of the unlocking script from a transaction input
//
// Returns:
//   - []byte: The extracted public key if successful
//   - error: Any error encountered during extraction, such as malformed scripts
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
