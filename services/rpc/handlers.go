// Package rpc implements the Bitcoin JSON-RPC API service for Teranode.
package rpc

// The handlers.go file contains implementations of all the supported RPC command handlers
// that process client requests. Each handler follows a consistent pattern of:
//
// 1. Parameter validation and type assertion
// 2. Interaction with the appropriate backing services (blockchain, p2p, etc.)
// 3. Response formatting according to the Bitcoin RPC specification
// 4. Error handling with appropriate error codes and messages
//
// All handler functions conform to the commandHandler type signature, taking a context,
// server reference, command parameters, and a close notification channel. They return
// a properly formatted response object or an error to be encoded as a JSON-RPC response.
//
// The handlers connect to other Teranode services via the client interfaces established
// during RPCServer initialization, including:
// - blockchainClient: For blockchain and block data operations
// - blockAssemblyClient: For mining and block template creation
// - peerClient: For network peer management
// - p2pClient: For advanced P2P network operations
// - utxoStore: For UTXO set queries and transaction validation
//
// Performance considerations are implemented for many handlers, including:
// - Response caching for frequently requested data
// - Efficient serialization for large responses
// - Proper context handling for cancellation
// - Request tracing for operational visibility
import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	"github.com/bsv-blockchain/teranode/services/legacy/peer_api"
	"github.com/bsv-blockchain/teranode/services/legacy/txscript"
	"github.com/bsv-blockchain/teranode/services/p2p"
	"github.com/bsv-blockchain/teranode/services/rpc/bsvjson"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/ordishs/go-utils"
	cache "github.com/patrickmn/go-cache"
	"google.golang.org/protobuf/types/known/emptypb"
)

// live items expire after 10s, cleanup runs every minute
var rpcCallCache = cache.New(10*time.Second, time.Minute)

// handleGetBlock implements the getblock command, which retrieves information about a block
// from the blockchain based on its hash.
//
// The command supports three verbosity levels that control the amount of information returned:
// - 0: Returns the serialized block as a hex-encoded string
// - 1: Returns a JSON object with block header information
//
// This handler interfaces with the blockchain service to retrieve block data and performs
// format conversion appropriate to the requested verbosity level. Response size increases
// significantly with higher verbosity, especially for blocks with many transactions.
//
// Parameters:
//   - ctx: Context for cancellation and tracing
//   - s: The RPC server instance providing access to service clients
//   - cmd: The parsed command arguments (bsvjson.GetBlockCmd)
//   - _: Unused channel for close notification
//
// Returns:
//   - interface{}: Block data in the requested format (string or bsvjson.GetBlockVerboseResult)
//   - error: Any error encountered during processing
func handleGetBlock(ctx context.Context, s *RPCServer, cmd interface{}, _ <-chan struct{}) (interface{}, error) {
	ctx, _, deferFn := tracing.Tracer("rpc").Start(ctx, "handleGetBlock",
		tracing.WithParentStat(RPCStat),
		tracing.WithHistogram(prometheusHandleGetBlock),
		tracing.WithLogMessage(s.logger, "[handleGetBlock] called"),
	)
	defer deferFn()

	c := cmd.(*bsvjson.GetBlockCmd)

	ch, err := chainhash.NewHashFromStr(c.Hash)
	if err != nil {
		return nil, rpcDecodeHexError(c.Hash)
	}

	// Load the raw block bytes from the database.
	b, err := s.blockchainClient.GetBlock(ctx, ch)
	if err != nil {
		return nil, err
	}

	result, err := s.blockToJSON(ctx, b, *c.Verbosity)
	if err != nil {
		return nil, err
	}

	// If verbosity > 0, check if block is on main chain and adjust confirmations
	if *c.Verbosity > 0 {
		// Check if this block is on the main chain
		isOnMainChain, err := s.blockchainClient.CheckBlockIsInCurrentChain(ctx, []uint32{b.ID})
		if err != nil {
			return nil, err
		}

		if !isOnMainChain {
			// Type switch to modify confirmations field for orphaned blocks
			switch v := result.(type) {
			case *bsvjson.GetBlockVerboseTxResult:
				v.GetBlockBaseVerboseResult.Confirmations = -1
				return v, nil
			case *bsvjson.GetBlockVerboseResult:
				v.GetBlockBaseVerboseResult.Confirmations = -1
				return v, nil
			default:
				// Should not happen with current implementation, but be defensive
				return result, nil
			}
		}
	}

	return result, nil
}

// handleGetBlockByHeight implements the getblockbyheight command, which retrieves information
// about a block from the blockchain based on its height (block number).
//
// This command provides an alternative to getblock for retrieving block data when the
// block hash is not known but the height is available. It supports the same verbosity
// levels as getblock to control the amount of information returned:
// - 0: Returns the serialized block as a hex-encoded string
// - 1: Returns a JSON object with block header information and transaction details
//
// The handler interfaces with the blockchain service to locate the block at the specified
// height and performs format conversion appropriate to the requested verbosity level.
// This is particularly useful for blockchain explorers and applications that need to
// iterate through blocks sequentially.
//
// Parameters:
//   - ctx: Context for cancellation and tracing
//   - s: The RPC server instance providing access to service clients
//   - cmd: The parsed command arguments (bsvjson.GetBlockByHeightCmd)
//   - _: Unused channel for close notification
//
// Returns:
//   - interface{}: Block data in the requested format (string or bsvjson.GetBlockVerboseResult)
//   - error: Any error encountered during processing, including if height is invalid
func handleGetBlockByHeight(ctx context.Context, s *RPCServer, cmd interface{}, _ <-chan struct{}) (interface{}, error) {
	ctx, _, deferFn := tracing.Tracer("rpc").Start(ctx, "handleGetBlockByHeight",
		tracing.WithParentStat(RPCStat),
		tracing.WithHistogram(prometheusHandleGetBlockByHeight),
		tracing.WithLogMessage(s.logger, "[handleGetBlockByHeight] called"),
	)
	defer deferFn()

	c := cmd.(*bsvjson.GetBlockByHeightCmd)

	// Load the raw block bytes from the database.
	b, err := s.blockchainClient.GetBlockByHeight(ctx, c.Height)
	if err != nil {
		return nil, err
	}

	return s.blockToJSON(ctx, b, *c.Verbosity)
}

// handleGetBlockHash implements the getblockhash command, which returns the hash
// of the block at the specified height in the main blockchain.
//
// This command provides a simple way to retrieve the block hash when only the
// block height (index) is known. It's commonly used by blockchain explorers,
// wallets, and other applications that need to iterate through the blockchain
// or verify specific blocks by height.
//
// The command takes a single parameter (the block height) and returns the
// corresponding block hash as a hex-encoded string. This is often used in
// conjunction with getblock to first obtain the hash, then retrieve detailed
// block information.
//
// Parameters:
//   - ctx: Context for cancellation and tracing
//   - s: The RPC server instance providing access to service clients
//   - cmd: The parsed command arguments (bsvjson.GetBlockHashCmd)
//   - _: Unused channel for close notification
//
// Returns:
//   - interface{}: String containing the hash of the block at the specified height
//   - error: Any error encountered during processing, including if height is invalid or out of range
func handleGetBlockHash(ctx context.Context, s *RPCServer, cmd interface{}, _ <-chan struct{}) (interface{}, error) {
	ctx, _, deferFn := tracing.Tracer("rpc").Start(ctx, "handleGetBlockHash",
		tracing.WithParentStat(RPCStat),
		tracing.WithHistogram(prometheusHandleGetBlockHash),
		tracing.WithLogMessage(s.logger, "[handleGetBlockHash] called"),
	)
	defer deferFn()

	c := cmd.(*bsvjson.GetBlockHashCmd)

	indexUint32, err := safeconversion.Int64ToUint32(c.Index)
	if err != nil {
		return nil, err
	}

	// Load the raw block bytes from the database.
	b, err := s.blockchainClient.GetBlockByHeight(ctx, indexUint32)
	if err != nil {
		return nil, err
	}

	return b.Hash().String(), nil
}

// handleGetBlockHeader implements the getblockheader command.
func handleGetBlockHeader(ctx context.Context, s *RPCServer, cmd interface{}, _ <-chan struct{}) (interface{}, error) {
	ctx, _, deferFn := tracing.Tracer("rpc").Start(ctx, "handleGetBlockHeader",
		tracing.WithParentStat(RPCStat),
		tracing.WithHistogram(prometheusHandleGetBlockHeader),
		tracing.WithLogMessage(s.logger, "[handleGetBlockHeader] called"),
	)
	defer deferFn()

	c := cmd.(*bsvjson.GetBlockHeaderCmd)

	ch, err := chainhash.NewHashFromStr(c.Hash)
	if err != nil {
		return nil, rpcDecodeHexError(c.Hash)
	}
	// Load the raw block bytes from the database.
	b, meta, err := s.blockchainClient.GetBlockHeader(ctx, ch)
	if err != nil {
		return nil, err
	}

	if *c.Verbose {
		versionInt32, err := safeconversion.Uint32ToInt32(b.Version)
		if err != nil {
			return nil, err
		}

		nonceUint64, err := safeconversion.Uint32ToUint64(b.Nonce)
		if err != nil {
			return nil, err
		}

		timeInt64, err := safeconversion.Uint32ToInt64(b.Timestamp)
		if err != nil {
			return nil, err
		}

		heightInt32, err := safeconversion.Uint32ToInt32(meta.Height)
		if err != nil {
			return nil, err
		}

		diff := b.Bits.CalculateDifficulty()
		diffFloat, _ := diff.Float64()

		// Get best block header for confirmations calculation
		_, bestBlockMeta, err := s.blockchainClient.GetBestBlockHeader(ctx)
		if err != nil {
			return nil, err
		}

		// Calculate median time for this block
		medianTime, err := calculateMedianTime(ctx, s.blockchainClient, b.Hash())
		if err != nil {
			// If we can't calculate median time, use block time
			s.logger.Warnf("Failed to calculate median time for block %s: %v, falling back to block timestamp", b.Hash(), err)
			medianTime = b.Timestamp
		}

		// Handle previousblockhash for genesis block
		previousBlockHash := b.HashPrevBlock.String()
		if meta.Height == 0 {
			// For genesis block, return empty string instead of zeros
			previousBlockHash = ""
		}

		// Get next block hash unless there are none
		nextBlock, err := s.blockchainClient.GetBlockByHeight(ctx, meta.Height+1)
		var nextBlockHash string
		if err == nil && nextBlock != nil {
			nextBlockHash = nextBlock.Hash().String()
		}

		// Get block size
		blockBytes := b.Bytes()
		blockSizeInt32, err := safeconversion.IntToInt32(len(blockBytes))
		if err != nil {
			return nil, err
		}

		// Get transaction count from the block at this height
		block, err := s.blockchainClient.GetBlockByHeight(ctx, meta.Height)
		var numTx int
		if err == nil && block != nil {
			numTx = int(block.TransactionCount)
		}

		headerReply := &bsvjson.GetBlockHeaderVerboseResult{
			Hash:          b.Hash().String(),
			Version:       versionInt32,
			VersionHex:    fmt.Sprintf("%08x", b.Version),
			PreviousHash:  previousBlockHash,
			Nonce:         nonceUint64,
			Time:          timeInt64,
			Bits:          b.Bits.String(),
			Difficulty:    diffFloat,
			MerkleRoot:    b.HashMerkleRoot.String(),
			Confirmations: int64(1 + bestBlockMeta.Height - meta.Height),
			Height:        heightInt32,
			Size:          blockSizeInt32,
			NumTx:         numTx,
			MedianTime:    int64(medianTime),
			ChainWork:     hex.EncodeToString(meta.ChainWork),
			NextHash:      nextBlockHash,
			Status:        "active",
		}

		// Check if this block is on the main chain
		isOnMainChain, err := s.blockchainClient.CheckBlockIsInCurrentChain(ctx, []uint32{meta.ID})
		if err != nil {
			return nil, err
		}

		if !isOnMainChain {
			headerReply.Confirmations = -1
			return headerReply, nil
		}

		return headerReply, nil
	}

	return fmt.Sprintf("%x", b.Bytes()), nil
}

// blockToJSON converts a block to JSON format based on verbosity level.
func (s *RPCServer) blockToJSON(ctx context.Context, b *model.Block, verbosity uint32) (interface{}, error) {
	if b == nil {
		return nil, &bsvjson.RPCError{
			Code:    bsvjson.ErrRPCBlockNotFound,
			Message: "Block not found",
		}
	}

	// When the verbosity value set to 0, simply return the serialized block
	// as a hex-encoded string.
	blkBytes, err := b.Bytes()
	if err != nil {
		return nil, err
	}

	if verbosity == 0 {
		// Generate the JSON object and return it.
		return hex.EncodeToString(blkBytes), nil
	}

	// get best block header
	_, bestBlockMeta, err := s.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		return nil, err
	}

	// Get block metadata for this specific block to retrieve its chain work
	_, blockMeta, err := s.blockchainClient.GetBlockHeader(ctx, b.Hash())
	if err != nil {
		return nil, err
	}

	// Get next block hash unless there are none.
	nextBlock, err := s.blockchainClient.GetBlockByHeight(ctx, b.Height+1)
	if err != nil && !errors.Is(err, errors.ErrBlockNotFound) {
		return nil, err
	}

	var (
		blockReply    interface{}
		nextBlockHash string
		// 	params      = s.cfg.ChainParams
		// 	blockHeader = &blk.MsgBlock().Header
	)

	if nextBlock != nil {
		nextBlockHash = nextBlock.Hash().String()
	}

	diff, _ := b.Header.Bits.CalculateDifficulty().Float64()

	versionInt32, err := safeconversion.Uint32ToInt32(b.Header.Version)
	if err != nil {
		return nil, err
	}

	blkBytesInt32, err := safeconversion.IntToInt32(len(blkBytes))
	if err != nil {
		return nil, err
	}

	// Calculate median time for this block
	medianTime, err := calculateMedianTime(ctx, s.blockchainClient, b.Hash())
	if err != nil {
		// If we can't calculate median time, use block time
		s.logger.Warnf("Failed to calculate median time for block %s: %v, falling back to block timestamp", b.Hash(), err)
		medianTime = b.Header.Timestamp
	}

	// Handle previousblockhash for genesis block
	previousBlockHash := b.Header.HashPrevBlock.String()
	if b.Height == 0 {
		// For genesis block, return empty string instead of zeros
		previousBlockHash = ""
	}

	// Create the base block reply with all required fields
	baseBlockReply := &bsvjson.GetBlockBaseVerboseResult{
		Hash:          b.Hash().String(),
		Version:       versionInt32,
		VersionHex:    fmt.Sprintf("%08x", b.Header.Version),
		MerkleRoot:    b.Header.HashMerkleRoot.String(),
		PreviousHash:  previousBlockHash,
		Nonce:         b.Header.Nonce,
		Time:          int64(b.Header.Timestamp),
		Confirmations: 1 + int64(bestBlockMeta.Height) - int64(b.Height),
		Height:        int64(b.Height),
		Size:          blkBytesInt32,
		Bits:          b.Header.Bits.String(),
		Difficulty:    diff,
		NextHash:      nextBlockHash,
		NumTx:         int(b.TransactionCount),
		MedianTime:    int64(medianTime),
		ChainWork:     hex.EncodeToString(blockMeta.ChainWork),
	}

	// For verbosity 1 and above, return the JSON object
	// Note: Tx field is intentionally kept empty for performance reasons
	// to avoid large response bodies with potentially millions of transactions
	s.logger.Debugf("Returning block %s with empty tx array (num_tx=%d) for performance reasons", b.Hash(), b.TransactionCount)

	blockReply = &bsvjson.GetBlockVerboseResult{
		GetBlockBaseVerboseResult: baseBlockReply,
		Tx:                        []string{}, // Empty array for backward compatibility
	}

	return blockReply, nil
}

// handleGetBestBlockHash implements the getbestblockhash command, which returns the hash
// of the current best (tip) block in the longest blockchain.
//
// This handler provides a simple but crucial method for clients to determine the current
// blockchain state. It's commonly used by wallets and blockchain explorers to check
// for new blocks and synchronize their state with the network.
//
// The handler connects to the blockchain service to retrieve the current best block's
// information. It has minimal computational overhead and is optimized for frequent calls.
//
// Parameters:
//   - ctx: Context for cancellation and tracing
//   - s: The RPC server instance providing access to service clients
//   - _: Unused command arguments (command takes no parameters)
//   - _: Unused channel for close notification
//
// Returns:
//   - interface{}: String containing the hash of the best block in the main chain
//   - error: Any error encountered during processing
func handleGetBestBlockHash(ctx context.Context, s *RPCServer, _ interface{}, _ <-chan struct{}) (interface{}, error) {
	ctx, _, deferFn := tracing.Tracer("rpc").Start(ctx, "handleGetBestBlockHash",
		tracing.WithParentStat(RPCStat),
		tracing.WithHistogram(prometheusHandleGetBestBlockHash),
		tracing.WithLogMessage(s.logger, "[handleGetBestBlockHash] called"),
	)
	defer deferFn()

	if cached, found := rpcCallCache.Get("getbestblockhash"); found {
		return cached.(string), nil
	}

	bh, _, err := s.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		return nil, err
	}

	hash := bh.Hash()

	if s.settings.RPC.CacheEnabled {
		rpcCallCache.Set("getbestblockhash", hash.String(), cache.DefaultExpiration)
	}

	return hash.String(), nil
}

// handleGetRawTransaction implements the getrawtransaction command, which retrieves the raw
// transaction data for a specific transaction hash.
//
// This handler supports two modes of operation through the 'verbose' parameter:
// - When verbose=false: Returns the serialized, hex-encoded transaction data
// - When verbose=true: Returns a JSON object with detailed transaction information
//
// The handler can retrieve transactions from both the blockchain (confirmed transactions)
// and unconfirmed transactions that are being processed. For blockchain transactions, the
// transaction's confirmation status and block information are also provided.
//
// Security and performance considerations:
// - Resource intensive for servers with large transaction history
// - May be subject to rate limiting in production environments
// - Can be optimized with indexing for faster lookups
// - Performance critical for high-volume applications like payment processors
//
// Parameters:
//   - ctx: Context for cancellation and tracing
//   - s: The RPC server instance providing access to service clients
//   - cmd: The parsed command arguments (bsvjson.GetRawTransactionCmd)
//   - _: Unused channel for close notification
//
// Returns:
//   - interface{}: Either a string (raw transaction hex) or a GetRawTransactionResult object
//   - error: Any error encountered during processing, including if transaction is not found
func handleGetRawTransaction(ctx context.Context, s *RPCServer, cmd interface{}, _ <-chan struct{}) (interface{}, error) {
	_, _, deferFn := tracing.Tracer("rpc").Start(ctx, "handleGetRawTransaction",
		tracing.WithParentStat(RPCStat),
		tracing.WithHistogram(prometheusHandleGetRawTransaction),
		tracing.WithLogMessage(s.logger, "[handleGetRawTransaction] called"),
	)
	defer deferFn()

	c := cmd.(*bsvjson.GetRawTransactionCmd)

	if s.assetHTTPURL == nil {
		return nil, errors.NewConfigurationError("asset_httpURL is not set")
	}

	fullURL := s.assetHTTPURL.ResolveReference(&url.URL{Path: fmt.Sprintf("/api/v1/tx/%s/hex", c.Txid)})

	// Set up HTTP client with timeouts
	client := &http.Client{
		Timeout: time.Second * 10,
	}

	// Send an HTTP GET request to the URL
	resp, err := client.Get(fullURL.String())
	if err != nil {
		return nil, errors.NewServiceError("Error: " + err.Error())
	}
	defer resp.Body.Close()

	// Check the response status code
	if resp.StatusCode != http.StatusOK {
		return nil, errors.NewServiceError(fmt.Sprintf("Error: Unexpected status code %d", resp.StatusCode))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.NewServiceError("Error reading response body", err)
	}

	tx, err := bt.NewTxFromString(string(body))
	if err != nil {
		return nil, errors.NewServiceError("Error parsing transaction", err)
	}

	if *c.Verbose == 0 {
		return tx.String(), nil
	}

	// inputs
	inputs := make([]bsvjson.Vin, len(tx.Inputs))

	for i, txIn := range tx.Inputs {
		asm, err := txscript.DisasmString(txIn.UnlockingScript.Bytes())
		if err != nil {
			return nil, errors.NewServiceError("Error disassembling script", err)
		}

		inputs[i] = bsvjson.Vin{
			Txid: txIn.PreviousTxIDStr(),
			Vout: txIn.PreviousTxOutIndex,
			ScriptSig: &bsvjson.ScriptSig{
				Asm: asm,
				Hex: txIn.UnlockingScript.String(),
			},
			Sequence: txIn.SequenceNumber,
		}
	}

	// outputs
	outputs := make([]bsvjson.Vout, len(tx.Outputs))

	for i, txOut := range tx.Outputs {
		_, addrs, _, err := txscript.ExtractPkScriptAddrs(txOut.LockingScript.Bytes(), s.settings.ChainCfgParams)
		if err != nil {
			return nil, errors.NewServiceError("Error extracting script addresses", err)
		}

		addressStrings := make([]string, 0, len(addrs))
		for _, addr := range addrs {
			addressStrings = append(addressStrings, addr.EncodeAddress())
		}

		asm, err := txscript.DisasmString(txOut.LockingScript.Bytes())
		if err != nil {
			return nil, errors.NewServiceError("Error disassembling script", err)
		}

		outputs[i] = bsvjson.Vout{
			Value: float64(txOut.Satoshis),
			N:     uint32(i),
			ScriptPubKey: bsvjson.ScriptPubKeyResult{
				Addresses: addressStrings,
				Asm:       asm,
				Hex:       hex.EncodeToString(txOut.LockingScript.Bytes()),
				Type:      txOut.LockingScript.ScriptType(),
			},
		}
	}

	// return verbose transaction
	// do we want to send all the vin and vout data?
	return bsvjson.TxRawResult{
		Hex:      tx.String(),
		Txid:     tx.TxID(),
		Hash:     tx.TxID(),
		Size:     int32(tx.Size()),  //nolint:gosec
		Version:  int32(tx.Version), //nolint:gosec
		LockTime: tx.LockTime,
		Vin:      inputs,
		Vout:     outputs,
	}, nil
}

// handleCreateRawTransaction handles createrawtransaction commands.
func handleCreateRawTransaction(ctx context.Context, s *RPCServer, cmd interface{}, _ <-chan struct{}) (interface{}, error) {
	_, _, deferFn := tracing.Tracer("rpc").Start(ctx, "handleCreateRawTransaction",
		tracing.WithParentStat(RPCStat),
		tracing.WithHistogram(prometheusHandleCreateRawTransaction),
		tracing.WithLogMessage(s.logger, "[handleCreateRawTransaction] called"),
	)
	defer deferFn()

	c := cmd.(*bsvjson.CreateRawTransactionCmd)

	// Validate the locktime, if given.
	if c.LockTime != nil &&
		(*c.LockTime < 0 || *c.LockTime > int64(wire.MaxTxInSequenceNum)) {
		return nil, &bsvjson.RPCError{
			Code:    bsvjson.ErrRPCInvalidParameter,
			Message: "Locktime out of range",
		}
	}

	// Add all transaction inputs to a new transaction after performing
	// some validity checks.
	mtx := wire.NewMsgTx(wire.TxVersion)

	for _, input := range c.Inputs {
		txHash, err := chainhash.NewHashFromStr(input.Txid)
		if err != nil {
			return nil, rpcDecodeHexError(input.Txid)
		}

		prevOut := wire.NewOutPoint(txHash, input.Vout)
		txIn := wire.NewTxIn(prevOut, []byte{})

		if c.LockTime != nil && *c.LockTime != 0 {
			txIn.Sequence = wire.MaxTxInSequenceNum - 1
		}

		mtx.AddTxIn(txIn)
	}

	// Add all transaction outputs to the transaction after performing
	// some validity checks.
	// params := s.cfg.ChainParams
	for encodedAddr, amount := range c.Amounts {
		// Ensure amount is in the valid range for monetary amounts.
		if amount <= 0 || amount > bsvutil.MaxSatoshi {
			return nil, &bsvjson.RPCError{
				Code:    bsvjson.ErrRPCType,
				Message: "Invalid amount",
			}
		}

		// Decode the provided address.
		addr, err := bsvutil.DecodeAddress(encodedAddr, s.settings.ChainCfgParams)
		if err != nil {
			return nil, &bsvjson.RPCError{
				Code:    bsvjson.ErrRPCInvalidAddressOrKey,
				Message: "Invalid address or key: " + err.Error(),
			}
		}

		// Ensure the address is one of the supported types and that
		// the network encoded with the address matches the network the
		// server is currently on.
		switch addr.(type) {
		case *bsvutil.AddressPubKeyHash:
		case *bsvutil.AddressScriptHash:
		case *bsvutil.LegacyAddressPubKeyHash: // TODO: support legacy addresses?
		default:
			return nil, &bsvjson.RPCError{
				Code:    bsvjson.ErrRPCInvalidAddressOrKey,
				Message: `Invalid address or key`,
			}
		}

		if !addr.IsForNet(s.settings.ChainCfgParams) {
			return nil, &bsvjson.RPCError{
				Code: bsvjson.ErrRPCInvalidAddressOrKey,
				Message: "Invalid address: " + encodedAddr +
					" is for the wrong network",
			}
		}

		// Create a new script which pays to the provided address.
		pkScript, err := txscript.PayToAddrScript(addr)
		if err != nil {
			context := "Failed to generate pay-to-address script"
			return nil, s.internalRPCError(err.Error(), context)
		}

		// Convert the amount to satoshi.
		satoshi, err := bsvutil.NewAmount(amount)
		if err != nil {
			context := "Failed to convert amount"
			return nil, s.internalRPCError(err.Error(), context)
		}

		txOut := wire.NewTxOut(int64(satoshi), pkScript)
		mtx.AddTxOut(txOut)
	}

	// Set the Locktime, if given.
	if c.LockTime != nil {
		lockTimeUint32, err := safeconversion.Int64ToUint32(*c.LockTime)
		if err != nil {
			return nil, err
		}

		mtx.LockTime = lockTimeUint32
	}

	// Return the serialized and hex-encoded transaction.  Note that this
	// is intentionally not directly returning because the first return
	// value is a string and it would result in returning an empty string to
	// the client instead of nothing (nil) in the case of an error.
	mtxHex, err := s.messageToHex(mtx)
	if err != nil {
		return nil, err
	}

	return mtxHex, nil
}

// handleSendRawTransaction implements the sendrawtransaction command, which submits a
// raw transaction to the network for inclusion in the blockchain.
//
// This is one of the most critical RPC methods as it serves as the primary interface
// for clients to submit transactions to the Bitcoin network. The handler performs:
// 1. Basic validation of the transaction format and structure
// 2. Policy-based acceptance checks (fees, standardness, etc.)
// 3. Submission to the transaction processing system
// 4. Propagation to peers in the network
//
// The handler supports the 'allowhighfees' parameter to bypass the normal fee rate limits,
// which can be useful for testing or certain specialized applications. However, this
// should be used with caution as high-fee transactions might be interpreted as an error.
//
// Security and scaling considerations:
// - Rate limiting may be applied to prevent DoS attacks
// - Transaction size and complexity checks protect node resources
// - May trigger additional validation steps for non-standard transactions
// - Performance critical for high-volume applications like payment processors
//
// Parameters:
//   - ctx: Context for cancellation and tracing
//   - s: The RPC server instance providing access to service clients
//   - cmd: The parsed command arguments (bsvjson.SendRawTransactionCmd)
//   - _: Unused channel for close notification
//
// Returns:
//   - interface{}: String containing the transaction hash if successful
//   - error: Detailed error information if transaction submission fails
func handleSendRawTransaction(ctx context.Context, s *RPCServer, cmd interface{}, _ <-chan struct{}) (interface{}, error) {
	_, _, deferFn := tracing.Tracer("rpc").Start(ctx, "handleSendRawTransaction",
		tracing.WithParentStat(RPCStat),
		tracing.WithHistogram(prometheusHandleSendRawTransaction),
		tracing.WithLogMessage(s.logger, "[handleSendRawTransaction] called"),
	)
	defer deferFn()

	c := cmd.(*bsvjson.SendRawTransactionCmd)
	// Deserialize and send off to tx relay
	hexStr := c.HexTx
	if len(hexStr)%2 != 0 {
		hexStr = "0" + hexStr
	}

	serializedTx, err := hex.DecodeString(hexStr)

	if err != nil {
		return nil, rpcDecodeHexError(hexStr)
	}

	// Use 0 for the tag to represent local node.
	tx, err := bt.NewTxFromBytes(serializedTx)
	if err != nil {
		return nil, &bsvjson.RPCError{
			Code:    bsvjson.ErrRPCDeserialization,
			Message: "TX rejected: " + err.Error(),
		}
	}

	s.logger.Debugf("tx to send: %v", tx)

	// Store the transaction in blob store first (following the pattern from propagation service)
	if s.txStore != nil {
		err = s.txStore.Set(ctx, tx.TxIDChainHash().CloneBytes(), fileformat.FileTypeTx, tx.SerializeBytes())
		if err != nil {
			return nil, &bsvjson.RPCError{
				Code:    bsvjson.ErrRPCInternal.Code,
				Message: "Failed to store transaction: " + err.Error(),
			}
		}
	}

	// Validate the transaction synchronously
	// This will validate scripts, check UTXOs, spend them, create new UTXOs, and send to block assembly
	_, err = s.validatorClient.Validate(ctx, tx, 0)
	if err != nil {
		return nil, &bsvjson.RPCError{
			Code:    bsvjson.ErrRPCVerify,
			Message: "TX rejected: " + err.Error(),
		}
	}

	// Return the transaction ID as a hex string per Bitcoin RPC spec
	return tx.TxID(), nil
}

// handleGenerate implements the generate command, which instructs the node to
// immediately mine the specified number of blocks for testing purposes.
//
// This command is intended primarily for testing and development environments, not for
// production mining. It generates blocks using the node's block assembly service and
// submits them to the blockchain, effectively creating a private fork if used on mainnet.
//
// The command instructs the node to mine a specific number of blocks and returns the
// block hashes of the generated blocks. For each block, the node:
// 1. Creates a new block template with current unconfirmed transactions
// 2. Attempts to solve the proof-of-work (may use CPU mining)
// 3. Submits the solved block to the local blockchain
//
// Security and operational notes:
// - This command is typically disabled on production nodes
// - Resource-intensive operation that may temporarily reduce node responsiveness
// - Generated blocks will only be accepted by the network if valid
// - Not suitable for actual mining revenue generation
//
// Parameters:
//   - ctx: Context for cancellation and tracing
//   - s: The RPC server instance providing access to service clients
//   - cmd: The parsed command arguments (bsvjson.GenerateCmd)
//   - _: Unused channel for close notification
//
// Returns:
//   - interface{}: Array of strings containing the hashes of the generated blocks
//   - error: Any error encountered during block generation
func handleGenerate(ctx context.Context, s *RPCServer, cmd interface{}, _ <-chan struct{}) (interface{}, error) {
	c := cmd.(*bsvjson.GenerateCmd)
	_, _, deferFn := tracing.Tracer("rpc").Start(ctx, "handleGenerate",
		tracing.WithParentStat(RPCStat),
		tracing.WithHistogram(prometheusHandleGenerate),
		tracing.WithLogMessage(s.logger, "[handleGenerate] called for %d blocks", c.NumBlocks),
	)

	defer deferFn()

	// Respond with an error if there's virtually 0 chance of mining a block
	// with the CPU.
	if !s.settings.ChainCfgParams.GenerateSupported {
		return nil, &bsvjson.RPCError{
			Code: bsvjson.ErrRPCDifficulty,
			Message: fmt.Sprintf("No support for `generate` on "+
				"the current network, %s, as it's unlikely to "+
				"be possible to mine a block with the CPU.",
				s.settings.ChainCfgParams.Net),
		}
	}

	if c.NumBlocks <= 0 {
		return nil, &bsvjson.RPCError{
			Code:    bsvjson.ErrRPCInternal.Code,
			Message: "Please request a nonzero number of blocks to generate.",
		}
	}

	numblocksInt32, err := safeconversion.Uint32ToInt32(c.NumBlocks)
	if err != nil {
		return nil, err
	}

	// Generate blocks and return their hashes
	return s.generateBlocksAndReturnHashes(ctx, numblocksInt32, nil, nil)
}

// handleGenerateToAddress implements the generatetoaddress command, which instructs the node to
// immediately mine the specified number of blocks with coinbase rewards sent to a specified address.
//
// Similar to the 'generate' command, this is primarily intended for testing and development
// environments. The key difference is that it allows specifying the beneficiary address
// that will receive the block rewards, providing more flexibility for testing scenarios.
//
// For each requested block, the handler:
// 1. Creates a new block template with unconfirmed transactions
// 2. Sets the coinbase transaction to pay to the specified address
// 3. Attempts to solve the proof-of-work challenge
// 4. Submits the solved block to the blockchain
//
// Security and operational considerations:
// - Command typically disabled in production environments
// - Resource-intensive operation that could impact node performance
// - Requires proper address format validation
// - Block rewards will only be spendable after 100 blocks (coinbase maturity)
//
// Parameters:
//   - ctx: Context for cancellation and tracing
//   - s: The RPC server instance providing access to service clients
//   - cmd: The parsed command arguments (bsvjson.GenerateToAddressCmd)
//   - _: Unused channel for close notification
//
// Returns:
//   - interface{}: Array of strings containing the hashes of the generated blocks
//   - error: Any error encountered during block generation or address parsing
func handleGenerateToAddress(ctx context.Context, s *RPCServer, cmd interface{}, _ <-chan struct{}) (interface{}, error) {
	c := cmd.(*bsvjson.GenerateToAddressCmd)
	_, _, deferFn := tracing.Tracer("rpc").Start(ctx, "handleGenerateToAddress",
		tracing.WithParentStat(RPCStat),
		tracing.WithHistogram(prometheusHandleGenerateToAddress),
		tracing.WithLogMessage(s.logger, "[handleGenerateToAddress] called for %d blocks to %s", c.NumBlocks, c.Address),
	)

	defer deferFn()

	// Respond with an error if there's virtually 0 chance of mining a block
	// with the CPU.
	if !s.settings.ChainCfgParams.GenerateSupported {
		return nil, &bsvjson.RPCError{
			Code: bsvjson.ErrRPCDifficulty,
			Message: fmt.Sprintf("No support for `generatetoaddress` on "+
				"the current network, %s, as it's unlikely to "+
				"be possible to mine a block with the CPU.",
				s.settings.ChainCfgParams.Net),
		}
	}

	if c.NumBlocks <= 0 {
		return nil, &bsvjson.RPCError{
			Code:    bsvjson.ErrRPCInternal.Code,
			Message: "Please request a nonzero number of blocks to generate.",
		}
	}

	// check address
	_, err := bsvutil.DecodeAddress(c.Address, s.settings.ChainCfgParams)
	if err != nil {
		return nil, &bsvjson.RPCError{
			Code:    bsvjson.ErrRPCInvalidAddressOrKey,
			Message: err.Error(),
		}
	}

	// Generate blocks and return their hashes
	return s.generateBlocksAndReturnHashes(ctx, c.NumBlocks, &c.Address, c.MaxTries)
}

// handleGetMiningCandidate implements the getminingcandidate command, which provides
// external miners with the information needed to construct a block for mining.
//
// This command is a critical component of the mining interface, allowing miners to
// retrieve the current block template with all necessary data to begin hashing. Unlike
// the BIP22 getblocktemplate command, this uses a simplified format optimized for
// efficiency in high-performance mining operations.
//
// The handler generates and returns a mining candidate containing:
// - The previous block hash to build upon
// - The current block height being targeted
// - The current timestamp
// - The current difficulty target
// - Merkle root for the block
// - Pre-selected transactions to include
//
// Performance considerations:
// - Optimized for frequent polling by miners
// - Caching mechanisms reduce load on the node
// - Context timeouts prevent blocking during blockchain reorganizations
// - Designed for mining pool integration
//
// Parameters:
//   - ctx: Context for cancellation and tracing
//   - s: The RPC server instance providing access to service clients
//   - cmd: The parsed command arguments (optional coinbase value)
//   - _: Unused channel for close notification
//
// Returns:
//   - interface{}: A mining candidate object with all required fields for miners
//   - error: Any error encountered during template generation
func handleGetMiningCandidate(ctx context.Context, s *RPCServer, cmd interface{}, _ <-chan struct{}) (interface{}, error) {
	ctx, _, deferFn := tracing.Tracer("rpc").Start(ctx, "handleGetMiningCandidate",
		tracing.WithParentStat(RPCStat),
		tracing.WithHistogram(prometheusHandleGetMiningCandidate),
		tracing.WithDebugLogMessage(s.logger, "[handleGetMiningCandidate] called"),
	)
	defer deferFn()

	c := cmd.(*bsvjson.GetMiningCandidateCmd)

	mc, err := s.blockAssemblyClient.GetMiningCandidate(ctx)
	if err != nil {
		return nil, err
	}

	ph, err := chainhash.NewHash(mc.PreviousHash)
	if err != nil {
		return nil, err
	}

	nBits, err := model.NewNBitFromSlice(mc.NBits)
	if err != nil {
		return nil, err
	}

	// Calculate difficulty from nBits
	difficulty := nBits.CalculateDifficulty()
	difficultyFloat, _ := difficulty.Float64()

	merkleProofStrings := make([]string, len(mc.MerkleProof))

	for i, hash := range mc.MerkleProof {
		merkleProofStrings[i] = utils.ReverseAndHexEncodeSlice(hash)
	}

	jsonMap := map[string]interface{}{
		"id":                  utils.ReverseAndHexEncodeSlice(mc.Id),
		"prevhash":            ph.String(),
		"coinbaseValue":       mc.CoinbaseValue,
		"version":             mc.Version,
		"nBits":               nBits.String(),
		"time":                mc.Time,
		"height":              mc.Height,
		"num_tx":              mc.NumTxs,
		"sizeWithoutCoinbase": mc.SizeWithoutCoinbase,
		"merkleProof":         merkleProofStrings,
		"difficulty":          difficultyFloat,
	}

	if c.ProvideCoinbaseTx != nil && *c.ProvideCoinbaseTx {
		coinbaseTx, err := mc.CreateCoinbaseTxCandidate(s.settings, true)
		if err != nil {
			return nil, err
		}

		jsonMap["coinbase"] = hex.EncodeToString(coinbaseTx.Bytes())
	}

	if *c.Verbosity == uint32(1) {
		subtreeHashes := make([]string, len(mc.SubtreeHashes))
		for i, hash := range mc.SubtreeHashes {
			subtreeHashes[i] = utils.ReverseAndHexEncodeSlice(hash)
		}

		jsonMap["subtreeHashes"] = subtreeHashes
	}

	return jsonMap, nil
}

// handleGetpeerinfo implements the getpeerinfo command, which returns information
// about all currently connected peers in the network.
//
// This command provides detailed information about each peer connection, including:
// - Connection details (IP address, port, connection time)
// - Protocol information (version, user agent, services)
// - Network statistics (bytes sent/received, ping times)
// - Synchronization status (starting height, current height)
//
// The handler aggregates peer information from both the legacy peer service and
// the modern P2P service to provide a comprehensive view of all network connections.
// Results are cached briefly to improve performance when called frequently by
// monitoring tools.
//
// This command is commonly used by:
// - Network monitoring and debugging tools
// - Node operators checking connection health
// - Applications that need to understand network topology
// - Diagnostic tools for troubleshooting connectivity issues
//
// Parameters:
//   - ctx: Context for cancellation and tracing
//   - s: The RPC server instance providing access to service clients
//   - cmd: The parsed command arguments (no specific parameters)
//   - _: Unused channel for close notification
//
// Returns:
//   - interface{}: Array of peer information objects with detailed connection data
//   - error: Any error encountered while retrieving peer information
func handleGetpeerinfo(ctx context.Context, s *RPCServer, cmd interface{}, _ <-chan struct{}) (interface{}, error) {
	ctx, _, deferFn := tracing.Tracer("rpc").Start(ctx, "handleGetpeerinfo",
		tracing.WithParentStat(RPCStat),
		tracing.WithHistogram(prometheusHandleGetpeerinfo),
		tracing.WithLogMessage(s.logger, "[handleGetpeerinfo] called"),
	)
	defer deferFn()

	if cached, found := rpcCallCache.Get("getpeerinfo"); found {
		return cached, nil
	}

	// use a goroutine with select to handle timeouts more reliably
	type legacyPeerResult struct {
		resp *peer_api.GetPeersResponse
		err  error
	}

	legacyResultCh := make(chan legacyPeerResult, 1)

	type newPeerResult struct {
		resp []*p2p.PeerInfo
		err  error
	}
	newPeerResultCh := make(chan newPeerResult, 1)

	// create a timeout context to prevent hanging if legacy peer service is not responding
	peerCtx, cancel := context.WithTimeout(ctx, s.settings.RPC.ClientCallTimeout)
	defer cancel()

	wg := sync.WaitGroup{}

	hasLegacyClient := s.legacyP2PClient != nil
	hasP2PClient := s.p2pClient != nil

	// get legacy peer info
	if hasLegacyClient {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := s.legacyP2PClient.GetPeers(peerCtx)
			legacyResultCh <- legacyPeerResult{resp: resp, err: err}
		}()
	}

	// get new peer info from p2p service
	if hasP2PClient {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := s.p2pClient.GetPeerRegistry(peerCtx)
			newPeerResultCh <- newPeerResult{resp: resp, err: err}
		}()
	}

	wg.Wait()

	infos := make([]*bsvjson.GetPeerInfoResult, 0, 32)

	if hasLegacyClient {
		legacyResult := <-legacyResultCh
		if legacyResult.err != nil {
			// not critical - legacy service may not be running, so log as info
			s.logger.Infof("error getting legacy peer info: %v", legacyResult.err)
		} else {
			for _, p := range legacyResult.resp.Peers {
				info := &bsvjson.GetPeerInfoResult{
					ID:        p.Id,
					Addr:      p.Addr,
					AddrLocal: p.AddrLocal,
					// Services:       fmt.Sprintf("%08d", uint64(statsSnap.Services)),
					ServicesStr: p.Services,
					// RelayTxes:      !p.IsTxRelayDisabled(),
					LastSend:       p.LastSend,
					LastRecv:       p.LastRecv,
					BytesSent:      p.BytesSent,
					BytesRecv:      p.BytesReceived,
					ConnTime:       p.ConnTime,
					PingTime:       float64(p.PingTime),
					TimeOffset:     p.TimeOffset,
					Version:        p.Version,
					SubVer:         p.SubVer,
					Inbound:        p.Inbound,
					StartingHeight: p.StartingHeight,
					CurrentHeight:  p.CurrentHeight,
					BanScore:       p.Banscore,
					Whitelisted:    p.Whitelisted,
					FeeFilter:      p.FeeFilter,
					// SyncNode:       p.ID == syncPeerID,
				}
				// if p.ToPeer().LastPingNonce() != 0 {
				// 	wait := float64(time.Since(p.LastPingTime).Nanoseconds())
				// 	// We actually want microseconds.
				// 	info.PingWait = wait / 1000
				// }
				for _, s := range p.Streams {
					info.Streams = append(info.Streams, bsvjson.StreamInfoResult{
						StreamType: s.StreamType,
						BytesSent:  s.BytesSent,
						BytesRecv:  s.BytesRecv,
					})
				}
				infos = append(infos, info)
			}
		}
	}

	if hasP2PClient {
		newResult := <-newPeerResultCh
		if newResult.err != nil {
			// not critical - p2p service may not be running, so log as info
			s.logger.Infof("error getting new peer info: %v", newResult.err)
		} else {
			for _, np := range newResult.resp {
				s.logger.Debugf("new peer: %v", np)
			}

			for _, p := range newResult.resp {
				info := &bsvjson.GetPeerInfoResult{
					PeerID:         p.ID.String(),
					Addr:           p.DataHubURL, // Use DataHub URL as address
					SubVer:         p.ClientName,
					CurrentHeight:  int32(p.Height),
					StartingHeight: int32(p.Height), // Use current height as starting height
					BanScore:       int32(p.BanScore),
					BytesRecv:      p.BytesReceived,
					BytesSent:      0, // P2P doesn't track bytes sent currently
					ConnTime:       p.ConnectedAt.Unix(),
					TimeOffset:     0, // P2P doesn't track time offset
					PingTime:       p.AvgResponseTime.Seconds(),
					Version:        0,                        // P2P doesn't track protocol version
					LastSend:       p.LastMessageTime.Unix(), // Last time we sent/received any message
					LastRecv:       p.LastBlockTime.Unix(),   // Last time we received a block
					Inbound:        p.IsConnected,            // Whether peer is currently connected
				}
				infos = append(infos, info)
			}
		}
	}

	if s.settings.RPC.CacheEnabled {
		rpcCallCache.Set("getpeerinfo", infos, 10*time.Second)
	}

	// return peerInfo, nil
	return infos, nil
}

// handleGetRawMempool implements the getrawmempool command.
// Returns transaction IDs currently in the memory pool.
func handleGetRawMempool(ctx context.Context, s *RPCServer, cmd interface{}, _ <-chan struct{}) (interface{}, error) {
	ctx, _, deferFn := tracing.Tracer("rpc").Start(ctx, "handleGetRawMempool",
		tracing.WithParentStat(RPCStat),
		tracing.WithHistogram(prometheusHandleGetRawmempool),
		tracing.WithLogMessage(s.logger, "[handleGetRawMempool] called"),
	)
	defer deferFn()

	verbose := cmd.(*bsvjson.GetRawMempoolCmd).Verbose

	txs, err := s.blockAssemblyClient.GetTransactionHashes(ctx)
	if err != nil {
		return nil, &bsvjson.RPCError{
			Code:    bsvjson.ErrRPCInternal.Code,
			Message: "Error retrieving raw mempool: " + err.Error(),
		}
	}

	if verbose != nil && *verbose {
		miningCandidate, err := s.blockAssemblyClient.GetMiningCandidate(ctx)
		if err != nil {
			return nil, &bsvjson.RPCError{
				Code:    bsvjson.ErrRPCInternal.Code,
				Message: "Error retrieving mining candidate: " + err.Error(),
			}
		}

		result := bsvjson.GetRawMempoolVerboseResult{
			Size:    int32(len(txs)),                        //nolint:gosec
			Fee:     float64(miningCandidate.CoinbaseValue), //nolint:gosec
			Time:    int64(miningCandidate.Time),            //nolint:gosec
			Height:  int64(miningCandidate.Height),          //nolint:gosec
			Depends: txs,
		}

		return result, nil
	}

	return txs, nil
}

// handleGetDifficulty implements the getdifficulty command, which returns the current
// proof-of-work difficulty as a multiple of the minimum difficulty.
//
// This command provides the current network difficulty target, which represents how
// difficult it is to find a hash below the target threshold. The difficulty is
// expressed as a floating-point number that indicates how many times more difficult
// the current target is compared to the minimum possible difficulty.
//
// The difficulty value is crucial for:
// - Miners to understand the current network hash rate and competition level
// - Applications calculating expected mining times and profitability
// - Network monitoring tools tracking blockchain security and stability
// - Block explorers displaying network statistics
//
// The difficulty adjusts automatically every 2016 blocks (approximately every 2 weeks)
// to maintain an average block time of 10 minutes, regardless of changes in total
// network hash rate.
//
// Parameters:
//   - ctx: Context for cancellation and tracing
//   - s: The RPC server instance providing access to service clients
//   - cmd: The parsed command arguments (no specific parameters)
//   - _: Unused channel for close notification
//
// Returns:
//   - interface{}: Float64 representing the current difficulty as a multiple of minimum difficulty
//   - error: Any error encountered while retrieving difficulty information
func handleGetDifficulty(ctx context.Context, s *RPCServer, cmd interface{}, _ <-chan struct{}) (interface{}, error) {
	ctx, _, deferFn := tracing.Tracer("rpc").Start(ctx, "handleGetDifficulty",
		tracing.WithParentStat(RPCStat),
		tracing.WithHistogram(prometheusHandleGetDifficulty),
		tracing.WithLogMessage(s.logger, "[handleGetDifficulty] called"),
	)
	defer deferFn()

	difficulty, err := s.blockAssemblyClient.GetCurrentDifficulty(ctx)
	if err != nil {
		return nil, err
	}

	return difficulty, nil
}

// handleGetblockchaininfo implements the getblockchaininfo command, which returns
// information about the current state of the blockchain.
//
// This command provides a comprehensive overview of the blockchain, including:
// - The current chain name (main, test, regtest)
// - The current block count
// - The current block header count
// - The best block hash
// - The current difficulty
// - The median time of the last 11 blocks
// - The verification progress (not implemented)
// - The chain work (not implemented)
// - Whether the blockchain is pruned (not implemented)
//
// The command is commonly used by:
// - Blockchain explorers to display network statistics
// - Node operators to monitor blockchain health
// - Applications that need to understand the current blockchain state
//
// Parameters:
//   - ctx: Context for cancellation and tracing
//   - s: The RPC server instance providing access to service clients
//   - cmd: The parsed command arguments (no specific parameters)
//   - _: Unused channel for close notification
//
// Returns:
//   - interface{}: A map of blockchain information
//   - error: Any error encountered while retrieving blockchain information
func handleGetblockchaininfo(ctx context.Context, s *RPCServer, cmd interface{}, _ <-chan struct{}) (interface{}, error) {
	ctx, _, deferFn := tracing.Tracer("rpc").Start(ctx, "handleGetblockchaininfo",
		tracing.WithParentStat(RPCStat),
		tracing.WithHistogram(prometheusHandleGetblockchaininfo),
		tracing.WithLogMessage(s.logger, "[handleGetblockchaininfo] called"),
	)
	defer deferFn()

	if cached, found := rpcCallCache.Get("getblockchaininfo"); found {
		return cached.(map[string]interface{}), nil
	}

	bestBlockHeader, bestBlockMeta, err := s.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		return map[string]interface{}{}, errors.NewProcessingError("error getting best block header: %v", err)
	}

	// Calculate median time from the last 11 blocks
	medianTime, err := calculateMedianTime(ctx, s.blockchainClient, bestBlockHeader.Hash())
	if err != nil {
		return map[string]interface{}{}, errors.NewProcessingError("error calculating median time: %v", err)
	}

	// Calculate verification progress based on Bitcoin SV's GuessVerificationProgress function
	verificationProgress, err := calculateVerificationProgress(ctx, s.blockchainClient, bestBlockMeta.Height)
	if err != nil {
		// If we can't calculate verification progress, default to 1.0 (assume fully synced)
		verificationProgress = 1.0
	}

	// Convert chainwork bytes to little endian hex string
	chainWorkHex := hex.EncodeToString(bestBlockMeta.ChainWork)

	difficultyBigFloat := bestBlockHeader.Bits.CalculateDifficulty()
	difficulty, _ := difficultyBigFloat.Float64()

	jsonMap := map[string]interface{}{
		"chain":                s.settings.ChainCfgParams.Name,
		"blocks":               bestBlockMeta.Height,
		"headers":              bestBlockMeta.Height,
		"bestblockhash":        bestBlockHeader.Hash().String(),
		"difficulty":           difficulty, // Return as float64 to match Bitcoin SV
		"mediantime":           medianTime,
		"verificationprogress": verificationProgress,
		"chainwork":            chainWorkHex,
		"pruned":               false, // the minimum relay fee for non-free transactions in BSV/KB
		"softforks":            []interface{}{},
	}

	if s.settings.RPC.CacheEnabled {
		rpcCallCache.Set("getblockchaininfo", jsonMap, 10*time.Second)
	}

	return jsonMap, nil
}

// calculateVerificationProgress follows the pattern of Bitcoin SV's GuessVerificationProgress function.
// This is a translation of the Bitcoin SV code from validation.cpp:
//
//	double GuessVerificationProgress(const ChainTxData &data, const CBlockIndex *pindex) {
//	    if (pindex == nullptr) return 0.0;
//	    int64_t nNow = time(nullptr);
//	    double fTxTotal;
//	    if (pindex->GetChainTx() <= data.nTxCount) {
//	        fTxTotal = data.nTxCount + (nNow - data.nTime) * data.dTxRate;
//	    } else {
//	        fTxTotal = pindex->GetChainTx() + (nNow - pindex->GetBlockTime()) * data.dTxRate;
//	    }
//	    return pindex->GetChainTx() / fTxTotal;
//	}
//
// Since we don't have hardcoded ChainTxData like Bitcoin SV, we'll use dynamic calculation
// based on our current blockchain statistics.
func calculateVerificationProgress(ctx context.Context, blockchainClient blockchain.ClientI, currentHeight uint32) (float64, error) {
	// Equivalent to: if (pindex == nullptr) return 0.0;
	if currentHeight == 0 {
		return 0.0, nil
	}

	// Get current block stats to get pindex->GetChainTx() equivalent
	blockStats, err := blockchainClient.GetBlockStats(ctx)
	if err != nil {
		return 0.0, err
	}

	// Equivalent to: if (pindex == nullptr) return 0.0;
	if blockStats.TxCount == 0 {
		return 0.0, nil
	}

	// Equivalent to: int64_t nNow = time(nullptr);
	nNow := time.Now().Unix()

	// Since we don't have hardcoded ChainTxData like Bitcoin SV, we'll use a different approach
	// We'll estimate sync progress based on how recent our last block is

	// If our last block is very recent (within 10 minutes), assume we're synced
	timeSinceLastBlock := nNow - int64(blockStats.LastBlockTime)
	if timeSinceLastBlock <= 600 { // 10 minutes
		return 1.0, nil
	}

	// If our last block is very old (more than 24 hours), we're definitely not synced
	if timeSinceLastBlock >= 86400 { // 24 hours
		return 0.1, nil // Very behind
	}

	// For blocks between 10 minutes and 24 hours old, calculate progress
	// Progress decreases as time since last block increases
	maxTimeBehind := int64(86400) // 24 hours
	minTimeBehind := int64(600)   // 10 minutes

	// Linear interpolation between 1.0 (recent) and 0.1 (old)
	progress := 1.0 - 0.9*float64(timeSinceLastBlock-minTimeBehind)/float64(maxTimeBehind-minTimeBehind)

	if progress < 0.1 {
		progress = 0.1
	}
	if progress > 1.0 {
		progress = 1.0
	}

	return progress, nil
}

// calculateMedianTime calculates the median time from the last 11 blocks.
// This follows the Bitcoin consensus rules for calculating median time.
func calculateMedianTime(ctx context.Context, blockchainClient blockchain.ClientI, bestBlockHash *chainhash.Hash) (uint32, error) {
	// Get the last 11 block headers starting from the best block
	headers, _, err := blockchainClient.GetBlockHeaders(ctx, bestBlockHash, 11)
	if err != nil {
		return 0, err
	}

	if len(headers) == 0 {
		return 0, errors.NewProcessingError("no block headers found")
	}

	// Extract timestamps from headers
	timestamps := make([]time.Time, len(headers))
	for i, header := range headers {
		timestamps[i] = time.Unix(int64(header.Timestamp), 0)
	}

	// Calculate median timestamp using the existing model function
	medianTimestamp, err := model.CalculateMedianTimestamp(timestamps)
	if err != nil {
		return 0, err
	}

	// Convert back to uint32 timestamp
	medianTimestampUint32, err := safeconversion.TimeToUint32(*medianTimestamp)
	if err != nil {
		return 0, err
	}

	return medianTimestampUint32, nil
}

// isSubscriberActive checks whether a blockchain subscriber whose source
// contains substr is currently registered. This lets RPC handlers skip
// expensive calls to services that are not running.
// isSubscriberActive checks whether source is present in the blockchain
// subscriber list. This lets RPC handlers skip expensive calls to services
// that are not running.
func isSubscriberActive(ctx context.Context, s *RPCServer, source string) bool {
	subs, err := s.blockchainClient.GetSubscribers(ctx)
	if err != nil {
		return false
	}
	for _, src := range subs {
		if src == source {
			return true
		}
	}
	return false
}

// handleGetInfo returns a JSON object containing various state info.
func handleGetInfo(ctx context.Context, s *RPCServer, cmd interface{}, _ <-chan struct{}) (interface{}, error) {
	ctx, _, deferFn := tracing.Tracer("rpc").Start(ctx, "handleGetInfo",
		tracing.WithParentStat(RPCStat),
		tracing.WithHistogram(prometheusHandleGetinfo),
		tracing.WithLogMessage(s.logger, "[handleGetInfo] called"),
	)
	defer deferFn()

	// use a cache that expires after 10 seconds
	if cached, found := rpcCallCache.Get("getinfo"); found {
		return cached.(map[string]interface{}), nil
	}

	bestBlockHeader, bestBlockMeta, err := s.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		s.logger.Errorf("error getting best block header: %v", err)
		return nil, err
	}

	difficultyBigFloat := bestBlockHeader.Bits.CalculateDifficulty()
	difficulty, _ := difficultyBigFloat.Float64()

	var p2pConnections []*p2p.PeerInfo
	if s.p2pClient != nil {
		// create a timeout context to prevent hanging if p2p service is not responding
		peerCtx, cancel := context.WithTimeout(ctx, s.settings.RPC.ClientCallTimeout)
		defer cancel()

		// use a goroutine with select to handle timeouts more reliably
		type peerResult struct {
			resp []*p2p.PeerInfo
			err  error
		}
		resultCh := make(chan peerResult, 1)

		go func() {
			resp, err := s.p2pClient.GetPeers(peerCtx)
			resultCh <- peerResult{resp: resp, err: err}
		}()

		select {
		case result := <-resultCh:
			if result.err != nil {
				// not critical - p2p service may not be running, so log as info
				s.logger.Infof("error getting p2p peer info: %v", result.err)
			} else {
				p2pConnections = result.resp
			}
		case <-peerCtx.Done():
			// timeout reached
			s.logger.Infof("timeout getting p2p peer info from p2p service")
		}
	}

	var legacyConnections *peer_api.GetPeersResponse
	if s.legacyP2PClient != nil && isSubscriberActive(ctx, s, blockchain.SubscriberLegacy) {
		peerCtx, cancel := context.WithTimeout(ctx, s.settings.RPC.ClientCallTimeout)
		defer cancel()

		type peerResult struct {
			resp *peer_api.GetPeersResponse
			err  error
		}
		resultCh := make(chan peerResult, 1)

		go func() {
			resp, err := s.legacyP2PClient.GetPeers(peerCtx)
			resultCh <- peerResult{resp: resp, err: err}
		}()

		select {
		case result := <-resultCh:
			if result.err != nil {
				s.logger.Infof("error getting legacy peer info: %v", result.err)
			} else {
				legacyConnections = result.resp
			}
		case <-peerCtx.Done():
			s.logger.Infof("timeout getting legacy peer info from peer service")
		}
	}

	connectionCount := 0
	if p2pConnections != nil {
		connectionCount += len(p2pConnections)
	}

	if legacyConnections != nil {
		connectionCount += len(legacyConnections.Peers)
	}

	jsonMap := map[string]interface{}{
		"version":         1,                                             // the version of the server
		"protocolversion": wire.ProtocolVersion,                          // the latest supported protocol version
		"blocks":          bestBlockMeta.Height,                          // the number of blocks processed
		"connections":     connectionCount,                               // the number of connected peers
		"difficulty":      difficulty,                                    // the current target difficulty
		"testnet":         s.settings.ChainCfgParams.Net == wire.TestNet, // whether or not server is using testnet
		"stn":             s.settings.ChainCfgParams.Net == wire.STN,     // whether or not server is using stn
		"policy": map[string]interface{}{
			"maxblocksize":                 s.settings.Policy.ExcessiveBlockSize,           // maximum block size
			"maxminedblocksize":            s.settings.Policy.BlockMaxSize,                 // maximum mined block size
			"maxstackmemoryusagepolicy":    s.settings.Policy.MaxStackMemoryUsagePolicy,    // max stack memory usage policy
			"maxstackmemoryusageconsensus": s.settings.Policy.MaxStackMemoryUsageConsensus, // max stack memory usage consensus
		},
	}

	if s.settings.RPC.CacheEnabled {
		rpcCallCache.Set("getinfo", jsonMap, cache.DefaultExpiration)
	}

	return jsonMap, nil
}

// handleSubmitMiningSolution implements the submitminingsolution command, which allows
// external miners to submit solved blocks to the network.
//
// This command is the counterpart to getminingcandidate, completing the mining workflow
// by allowing miners to submit their proof-of-work solutions. When a miner successfully
// finds a nonce that produces a valid hash, they submit the solution containing:
// - The block hash being solved
// - The nonce value that produces a valid proof-of-work
// - The coinbase transaction
// - The timestamp used (may differ from the template if sufficient time has passed)
//
// The handler validates the submission and, if valid, propagates the new block to the
// network. This command is optimized for high-frequency submission attempts typical in
// modern mining operations.
//
// Security and validation considerations:
// - Thorough validation prevents invalid block submissions
// - Anti-DoS measures may limit submission rates
// - Performance critical - must process quickly to minimize orphan risk
// - Success guarantees only that the block was accepted locally
//
// Parameters:
//   - ctx: Context for cancellation and tracing
//   - s: The RPC server instance providing access to service clients
//   - cmd: The parsed command arguments (bsvjson.SubmitMiningSolutionCmd)
//   - _: Unused channel for close notification
//
// Returns:
//   - interface{}: Boolean true if the solution was accepted, error otherwise
//   - error: Detailed error if the solution was rejected
func handleSubmitMiningSolution(ctx context.Context, s *RPCServer, cmd interface{}, _ <-chan struct{}) (interface{}, error) {
	ctx, _, deferFn := tracing.Tracer("rpc").Start(ctx, "handleSubmitMiningSolution",
		tracing.WithParentStat(RPCStat),
		tracing.WithHistogram(prometheusHandleSubmitMiningSolution),
		tracing.WithLogMessage(s.logger, "[handleSubmitMiningSolution] called"),
	)
	defer deferFn()

	c := cmd.(*bsvjson.SubmitMiningSolutionCmd)

	s.logger.Debugf("in handleSubmitMiningSolution: cmd: %s", c.MiningSolution.String())

	id, err := utils.DecodeAndReverseHexString(c.MiningSolution.ID)
	if err != nil {
		return nil, rpcDecodeHexError(c.MiningSolution.ID)
	}

	coinbase, err := hex.DecodeString(c.MiningSolution.Coinbase)
	if err != nil {
		return nil, rpcDecodeHexError(c.MiningSolution.Coinbase)
	}

	ms := &model.MiningSolution{
		Id:       id,
		Coinbase: coinbase,
		Time:     c.MiningSolution.Time,
		Nonce:    c.MiningSolution.Nonce,
		Version:  c.MiningSolution.Version,
	}

	s.logger.Debugf("in handleSubmitMiningSolution: ms: %s", ms.Stringify(true))

	if err = s.blockAssemblyClient.SubmitMiningSolution(ctx, ms); err != nil {
		// Handle "job not found" as an expected condition - miners often submit stale solutions
		if terr, ok := err.(*errors.Error); ok && terr.Code() == errors.ERR_NOT_FOUND {
			s.logger.Infof("Mining solution rejected: %s (job expired or block already found)", err.Error())
			return nil, bsvjson.NewRPCError(bsvjson.ErrRPCMisc, err.Error())
		}
		return nil, err
	}

	return true, nil
}

// handleInvalidateBlock implements the invalidateblock command, which marks a block
// as invalid, forcing a chain reorganization.
//
// This command allows manual intervention in blockchain validation by instructing the node
// to consider a specific block as invalid, even if it passes all consensus rules. When a block
// is invalidated, the blockchain service will:
// 1. Mark the specified block as invalid in the block index
// 2. Disconnect it and all blocks built on top of it from the active chain
// 3. Rewind the chain state to the last valid block before the invalidation point
// 4. Attempt to find an alternative chain to follow
//
// This is a privileged command intended primarily for testing and emergency recovery
// scenarios, such as responding to a consensus failure or attack. It should be used
// with extreme caution as it can cause the node to diverge from network consensus.
//
// Security considerations:
// - Requires admin privileges to execute
// - Can cause chain splits if used on blocks accepted by other nodes
// - May result in transaction reordering in the transaction processing system
// - Changes persist across node restarts until explicitly reconsidered
//
// Parameters:
//   - ctx: Context for cancellation and tracing
//   - s: The RPC server instance providing access to service clients
//   - cmd: The parsed command arguments (bsvjson.InvalidateBlockCmd)
//   - _: Unused channel for close notification
//
// Returns:
//   - interface{}: Null on success
//   - error: Any error encountered during block invalidation
func handleInvalidateBlock(ctx context.Context, s *RPCServer, cmd interface{}, _ <-chan struct{}) (interface{}, error) {
	ctx, _, deferFn := tracing.Tracer("rpc").Start(ctx, "handleInvalidateBlock",
		tracing.WithParentStat(RPCStat),
		tracing.WithHistogram(prometheusHandleInvalidateBlock),
		tracing.WithLogMessage(s.logger, "[handleInvalidateBlock] called"),
	)
	defer deferFn()

	c := cmd.(*bsvjson.InvalidateBlockCmd)

	// Log the RPC request
	s.logger.Infof("[handleInvalidateBlock] RPC request to invalidate block %s", c.BlockHash)

	ch, err := chainhash.NewHashFromStr(c.BlockHash)
	if err != nil {
		return nil, rpcDecodeHexError(c.BlockHash)
	}

	_, err = s.blockchainClient.InvalidateBlock(ctx, ch)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

// handleReconsiderBlock implements the reconsiderblock command, which removes the
// invalid status from a previously invalidated block, allowing it to be considered
// for inclusion in the blockchain again.
//
// This command reverses the effect of invalidateblock by undoing the manual
// invalidation of a block. This allows the node to reconsider the block during
// its normal validation process and potentially accept it if it follows consensus
// rules. The blockchain service will:
// 1. Remove the specified block from the invalid block list
// 2. Trigger revalidation of the block and its descendants
// 3. Possibly reorganize the chain if the reconsidered block belongs to a stronger chain
//
// Like invalidateblock, this is a privileged command intended for testing and recovery
// scenarios. It provides a mechanism to correct mistaken manual interventions in
// the blockchain validation process.
//
// Security considerations:
// - Requires admin privileges to execute
// - May trigger chain reorganization if the block is part of the best chain
// - Could potentially cause the node to switch to a chain that others consider invalid
// - May result in transaction reordering in the transaction processing system
//
// Parameters:
//   - ctx: Context for cancellation and tracing
//   - s: The RPC server instance providing access to service clients
//   - cmd: The parsed command arguments (bsvjson.ReconsiderBlockCmd)
//   - _: Unused channel for close notification
//
// Returns:
//   - interface{}: Null on success
//   - error: Any error encountered during block reconsideration
func handleReconsiderBlock(ctx context.Context, s *RPCServer, cmd interface{}, _ <-chan struct{}) (interface{}, error) {
	ctx, _, deferFn := tracing.Tracer("rpc").Start(ctx, "handleReconsiderBlock",
		tracing.WithParentStat(RPCStat),
		tracing.WithHistogram(prometheusHandleReconsiderBlock),
		tracing.WithLogMessage(s.logger, "[handleReconsiderBlock] called"),
	)
	defer deferFn()

	c := cmd.(*bsvjson.ReconsiderBlockCmd)

	ch, err := chainhash.NewHashFromStr(c.BlockHash)
	if err != nil {
		return nil, rpcDecodeHexError(c.BlockHash)
	}

	// Check if block validation client is available
	if s.blockValidationClient == nil {
		s.logger.Errorf("[handleReconsiderBlock] block validation client not available")
		return nil, &bsvjson.RPCError{
			Code:    bsvjson.ErrRPCInternal.Code,
			Message: "Block validation service not available",
		}
	}

	// Revalidate the block - this will fetch it, validate it, and update its status
	s.logger.Infof("[handleReconsiderBlock] revalidating block %s", ch)
	err = s.blockValidationClient.RevalidateBlock(ctx, *ch)
	if err != nil {
		s.logger.Errorf("[handleReconsiderBlock] block revalidation failed for %s: %v", ch, err)
		return nil, &bsvjson.RPCError{
			Code:    bsvjson.ErrRPCVerify,
			Message: "Block failed revalidation: " + err.Error(),
		}
	}

	s.logger.Infof("[handleReconsiderBlock] block %s successfully reconsidered and validated", ch)

	// Now recursively clear invalid status for all invalid child blocks
	err = s.reconsiderInvalidChildren(ctx, ch)
	if err != nil {
		s.logger.Errorf("[handleReconsiderBlock] failed to reconsider child blocks: %v", err)
		return nil, &bsvjson.RPCError{
			Code:    bsvjson.ErrRPCInternal.Code,
			Message: fmt.Sprintf("Block %s was reconsidered but failed to reconsider children: %v", ch, err),
		}
	}

	return nil, nil
}

// reconsiderInvalidChildren recursively finds and fully revalidates all invalid child
// blocks of the given parent block hash. This ensures that when a block is reconsidered,
// any children that were marked invalid are also properly revalidated through the full
// validation pipeline, preventing consensus-violating blocks from re-entering the chain.
// Uses an iterative queue-based approach to avoid redundant queries.
func (s *RPCServer) reconsiderInvalidChildren(ctx context.Context, parentHash *chainhash.Hash) error {
	// Fetch all invalid blocks once at the start to avoid O(depth × N) queries
	allInvalidBlocks, err := s.blockchainClient.GetLastNInvalidBlocks(ctx, 10000)
	if err != nil {
		return errors.NewServiceError("failed to get invalid blocks", err)
	}

	// Build a map for efficient parent->children lookups
	invalidBlocksByParent := make(map[chainhash.Hash][]*chainhash.Hash)
	for _, blockInfo := range allInvalidBlocks {
		header, err := model.NewBlockHeaderFromBytes(blockInfo.BlockHeader)
		if err != nil {
			s.logger.Warnf("[reconsiderInvalidChildren] failed to parse block header: %v", err)
			continue
		}

		childHash := header.Hash()
		prevHash := *header.HashPrevBlock
		invalidBlocksByParent[prevHash] = append(invalidBlocksByParent[prevHash], childHash)
	}

	// Use a queue for breadth-first traversal (more efficient than depth-first recursion)
	queue := []*chainhash.Hash{parentHash}

	for len(queue) > 0 {
		currentParent := queue[0]
		queue = queue[1:]

		// Find children of current parent
		children, exists := invalidBlocksByParent[*currentParent]
		if !exists {
			continue
		}

		// Revalidate each child
		for _, childHash := range children {
			s.logger.Infof("[reconsiderInvalidChildren] revalidating child block %s", childHash)

			// Use blockValidationClient.RevalidateBlock for full validation
			// This ensures consensus rules are properly checked before clearing the invalid flag
			err = s.blockValidationClient.RevalidateBlock(ctx, *childHash)
			if err != nil {
				s.logger.Errorf("[reconsiderInvalidChildren] failed to revalidate child block %s: %v", childHash, err)
				return errors.NewServiceError("failed to revalidate child block %s", childHash.String(), err)
			}

			s.logger.Infof("[reconsiderInvalidChildren] successfully revalidated child block %s", childHash)

			// Add to queue to process its children
			queue = append(queue, childHash)
		}
	}

	return nil
}

func handleHelp(ctx context.Context, s *RPCServer, cmd interface{}, _ <-chan struct{}) (interface{}, error) {
	_, _, deferFn := tracing.Tracer("rpc").Start(ctx, "handleHelp",
		tracing.WithParentStat(RPCStat),
		tracing.WithHistogram(prometheusHandleHelp),
		tracing.WithLogMessage(s.logger, "[handleHelp] called"),
	)
	defer deferFn()

	c := cmd.(*bsvjson.HelpCmd)

	// Provide a usage overview of all commands when no specific command
	// was specified.
	var command string
	if c.Command != nil {
		command = *c.Command
	}

	if command == "" {
		usage, err := s.helpCacher.rpcUsage()
		if err != nil {
			context := "Failed to generate RPC usage"
			return nil, s.internalRPCError(err.Error(), context)
		}

		return usage, nil
	}

	// Check that the command asked for is supported and implemented.  Only
	// search the main list of handlers since help should not be provided
	// for commands that are unimplemented or related to wallet
	// functionality.
	if _, ok := rpcHandlers[command]; !ok {
		return nil, &bsvjson.RPCError{
			Code:    bsvjson.ErrRPCInvalidParameter,
			Message: "Unknown command: " + command,
		}
	}

	// Get the help for the command.
	help, err := s.helpCacher.rpcMethodHelp(command)
	if err != nil {
		context := "Failed to generate help"
		return nil, s.internalRPCError(err.Error(), context)
	}

	return help, nil
}

// handleIsBanned implements the isbanned command, which checks if a specific IP address
// or subnet is currently banned from connecting to the node.
//
// This command provides a simple query interface to determine the ban status of a given
// network address. It's useful for administrators to verify ban list contents without
// having to retrieve and search through the entire ban list, especially when the list
// is large.
//
// The handler checks the given address against all active bans, considering both:
// - Direct IP address matches
// - Subnet bans that would include the specified address
//
// This command can be used as part of automated network security monitoring and
// for troubleshooting connectivity issues with specific peers.
//
// Parameters:
//   - ctx: Context for cancellation and tracing
//   - s: The RPC server instance providing access to service clients
//   - cmd: The parsed command arguments (bsvjson.IsBannedCmd)
//   - _: Unused channel for close notification
//
// Returns:
//   - interface{}: Boolean indicating whether the address is banned (true) or not (false)
//   - error: Any error encountered during ban check, such as invalid address format
func handleIsBanned(ctx context.Context, s *RPCServer, cmd interface{}, _ <-chan struct{}) (interface{}, error) {
	_, _, deferFn := tracing.Tracer("rpc").Start(ctx, "handleIsBanned",
		tracing.WithParentStat(RPCStat),
		tracing.WithHistogram(prometheusHandleIsBanned),
		tracing.WithLogMessage(s.logger, "[handleIsBanned] called"),
	)
	defer deferFn()

	c := cmd.(*bsvjson.IsBannedCmd)

	s.logger.Debugf("in handleIsBanned: c: %+v", *c)

	if c.IPOrSubnet == "" {
		return nil, &bsvjson.RPCError{
			Code:    bsvjson.ErrRPCInvalidParameter,
			Message: "IPOrSubnet is required",
		}
	}

	// validate ip or subnet
	if !isIPOrSubnet(c.IPOrSubnet) {
		return nil, &bsvjson.RPCError{
			Code:    bsvjson.ErrRPCInvalidParameter,
			Message: "Invalid IP or subnet",
		}
	}

	// check if P2P service is available
	var p2pBanned bool

	if s.p2pClient != nil {
		isBanned, err := s.p2pClient.IsBanned(ctx, c.IPOrSubnet)
		if err != nil {
			s.logger.Warnf("Failed to check if banned in P2P service: %v", err)
		} else {
			p2pBanned = isBanned
		}
	}

	// check if legacy peer service is available
	var peerBanned bool

	if s.legacyP2PClient != nil {
		isBannedLegacy, err := s.legacyP2PClient.IsBanned(ctx, &peer_api.IsBannedRequest{IpOrSubnet: c.IPOrSubnet})
		if err != nil {
			s.logger.Warnf("Failed to check if banned in legacy peer service: %v", err)
		} else {
			peerBanned = isBannedLegacy.IsBanned
		}
	}

	return p2pBanned || peerBanned, nil
}

// handleListBanned implements the listbanned command, which returns a list of all
// currently banned IP addresses and subnets along with ban metadata.
//
// This command provides comprehensive information about the node's active ban list,
// enabling administrators to audit and manage network access controls. For each ban
// entry, the command returns detailed information including:
// - The banned IP address or subnet
// - When the ban was created
// - When the ban expires (or if it's permanent)
// - The ban reason, if available
//
// The returned information is useful for security auditing, troubleshooting
// connection issues, and planning ban management strategies. It also serves as
// a reference when using other ban-related commands like setban and clearbanned.
//
// Parameters:
//   - ctx: Context for cancellation and tracing
//   - s: The RPC server instance providing access to service clients
//   - cmd: The parsed command arguments (no specific parameters)
//   - _: Unused channel for close notification
//
// Returns:
//   - interface{}: Array of ban information objects
//   - error: Any error encountered while retrieving ban information
func handleListBanned(ctx context.Context, s *RPCServer, cmd interface{}, _ <-chan struct{}) (interface{}, error) {
	_, _, deferFn := tracing.Tracer("rpc").Start(ctx, "handleListBanned",
		tracing.WithParentStat(RPCStat),
		tracing.WithHistogram(prometheusHandleListBanned),
		tracing.WithLogMessage(s.logger, "[handleListBanned] called"),
	)
	defer deferFn()

	s.logger.Debugf("in handleListBanned")

	// Define a constant for the client call timeout
	const clientCallTimeout = 5 * time.Second

	// check if P2P service is available
	var bannedList []string

	if s.p2pClient != nil {
		// Create a timeout context for the P2P client call
		p2pCtx, cancel := context.WithTimeout(ctx, clientCallTimeout)
		defer cancel()

		// Use a goroutine with select to handle timeouts more reliably
		type p2pResult struct {
			resp []string
			err  error
		}
		resultCh := make(chan p2pResult, 1)

		go func() {
			resp, err := s.p2pClient.ListBanned(p2pCtx)
			resultCh <- p2pResult{resp: resp, err: err}
		}()

		select {
		case result := <-resultCh:
			if result.err != nil {
				s.logger.Warnf("Failed to get banned list in P2P service: %v", result.err)
			} else {
				bannedList = result.resp
			}
		case <-p2pCtx.Done():
			// Timeout reached
			s.logger.Warnf("Timeout getting banned list from P2P service after %v seconds", clientCallTimeout.Seconds())
		}
	}

	// check if legacy peer service is available
	if s.legacyP2PClient != nil {
		// Create a timeout context for the legacy peer client call
		legacyCtx, cancel := context.WithTimeout(ctx, clientCallTimeout)
		defer cancel()

		// Use a goroutine with select to handle timeouts more reliably
		type legacyResult struct {
			resp *peer_api.ListBannedResponse
			err  error
		}
		resultCh := make(chan legacyResult, 1)

		go func() {
			resp, err := s.legacyP2PClient.ListBanned(legacyCtx, &emptypb.Empty{})
			resultCh <- legacyResult{resp: resp, err: err}
		}()

		select {
		case result := <-resultCh:
			if result.err != nil {
				s.logger.Warnf("Failed to get banned list in legacy peer service: %v", result.err)
			} else {
				bannedList = append(bannedList, result.resp.Banned...)
			}
		case <-legacyCtx.Done():
			// Timeout reached
			s.logger.Warnf("Timeout getting banned list from legacy peer service after %v seconds", clientCallTimeout.Seconds())
		}
	}

	return bannedList, nil
}

// handleClearBanned implements the clearbanned command, which removes all IP address
// and subnet bans from the node.
//
// This command provides a way to completely reset the node's ban list, effectively
// allowing all previously banned addresses to reconnect (subject to other connection
// policies). This is useful in scenarios such as:
// - When recovering from a misconfiguration that caused excessive banning
// - After an attack is mitigated and normal operation should resume
// - During testing and debugging of network connectivity
// - When implementing a new ban policy from scratch
//
// Security considerations:
// - Requires admin privileges to execute
// - Immediately allows previously banned peers to reconnect
// - Should be used with caution in production environments
// - Consider using setban with specific removals for more targeted approaches
//
// Parameters:
//   - ctx: Context for cancellation and tracing
//   - s: The RPC server instance providing access to service clients
//   - cmd: The parsed command arguments (no specific parameters)
//   - _: Unused channel for close notification
//
// Returns:
//   - interface{}: Null on success
//   - error: Any error encountered while clearing the ban list
func handleClearBanned(ctx context.Context, s *RPCServer, cmd interface{}, _ <-chan struct{}) (interface{}, error) {
	_, _, deferFn := tracing.Tracer("rpc").Start(ctx, "handleClearBanned",
		tracing.WithParentStat(RPCStat),
		tracing.WithHistogram(prometheusHandleClearBanned),
		tracing.WithLogMessage(s.logger, "[handleClearBanned] called"),
	)
	defer deferFn()

	s.logger.Debugf("in handleClearBanned")

	// check if P2P service is available
	if s.p2pClient != nil {
		err := s.p2pClient.ClearBanned(ctx)
		if err != nil {
			s.logger.Warnf("Failed to clear banned list in P2P service: %v", err)
		}
	}
	// check if legacy peer service is available
	if s.legacyP2PClient != nil {
		_, err := s.legacyP2PClient.ClearBanned(ctx, &emptypb.Empty{})
		if err != nil {
			s.logger.Warnf("Failed to clear banned list in legacy peer service: %v", err)
		}
	}

	return true, nil
}

// handleSetBan implements the setban command, which adds or removes an IP address or subnet
// from the node's ban list, controlling which peers can connect to the node.
//
// This command is an essential network security feature that allows administrators to
// manage connections to potential malicious actors. The command supports two operations:
// - 'add': Adds an IP or subnet to the ban list for a specified time (or permanently)
// - 'remove': Removes an IP or subnet from the ban list, allowing connections again
//
// When adding a ban, the duration can be specified in seconds, or an 'absolute' flag
// can be set to interpret the time as an absolute unix timestamp. This provides
// flexibility in ban management for different operational scenarios.
//
// Security considerations:
// - Requires admin privileges to execute
// - Critical for protecting against DoS attacks and malicious peers
// - Bans persist across node restarts by default
// - Can ban entire subnets using CIDR notation
// - Changes apply immediately, disconnecting currently connected peers if banned
//
// Parameters:
//   - ctx: Context for cancellation and tracing
//   - s: The RPC server instance providing access to service clients
//   - cmd: The parsed command arguments (bsvjson.SetBanCmd)
//   - _: Unused channel for close notification
//
// Returns:
//   - interface{}: Null on success
//   - error: Any error encountered during ban list modification
func handleSetBan(ctx context.Context, s *RPCServer, cmd interface{}, _ <-chan struct{}) (interface{}, error) {
	_, _, deferFn := tracing.Tracer("rpc").Start(ctx, "handleSetBan",
		tracing.WithParentStat(RPCStat),
		tracing.WithHistogram(prometheusHandleSetBan),
		tracing.WithLogMessage(s.logger, "[handleSetBan] called"),
	)
	defer deferFn()

	c := cmd.(*bsvjson.SetBanCmd)

	s.logger.Debugf("in handleSetBan: c: %+v", *c)

	if c.IPOrSubnet == "" {
		return nil, &bsvjson.RPCError{
			Code:    bsvjson.ErrRPCInvalidParameter,
			Message: "IPOrSubnet is required",
		}
	}

	// validate ip or subnet
	if !isIPOrSubnet(c.IPOrSubnet) {
		return nil, &bsvjson.RPCError{
			Code:    bsvjson.ErrRPCInvalidParameter,
			Message: "Invalid IP or subnet",
		}
	}

	// Handle the command
	switch c.Command {
	case "add":
		var success bool

		var expirationTime time.Time

		// If BanTime is nil or 0, use a default ban time (e.g., 24 hours)
		expirationTime = time.Now().Add(24 * time.Hour)

		if c.Absolute != nil && *c.Absolute {
			if c.BanTime == nil {
				return nil, &bsvjson.RPCError{
					Code:    bsvjson.ErrRPCInvalidParameter,
					Message: "BanTime is required when absolute is true",
				}
			}
			expirationTime = time.Unix(*c.BanTime, 0)
		} else if c.BanTime != nil && *c.BanTime != 0 {
			expirationTime = time.Now().Add(time.Duration(*c.BanTime) * time.Second)
		}

		expirationTimeInt64 := expirationTime.Unix()

		// ban teranode peers
		if s.p2pClient != nil {
			err := s.p2pClient.BanPeer(ctx, c.IPOrSubnet, expirationTimeInt64)
			if err == nil {
				success = true
				s.logger.Debugf("Added ban for %s until %v", c.IPOrSubnet, expirationTime)
			} else {
				s.logger.Warnf("Error while trying to ban teranode peer: %v", err)
			}
		}

		// and ban legacy peers
		if s.legacyP2PClient != nil {
			until := expirationTimeInt64

			resp, err := s.legacyP2PClient.BanPeer(ctx, &peer_api.BanPeerRequest{
				Addr:  c.IPOrSubnet,
				Until: until,
			})

			if err != nil {
				s.logger.Warnf("Error while trying to ban legacy peer: %v", err)

				if !success {
					return nil, &bsvjson.RPCError{
						Code:    bsvjson.ErrRPCInvalidParameter,
						Message: "Failed to add ban",
					}
				}
			} else if resp == nil || !resp.Ok {
				if !success {
					return nil, &bsvjson.RPCError{
						Code:    bsvjson.ErrRPCInvalidParameter,
						Message: "Failed to ban peer",
					}
				}
			} else {
				s.logger.Debugf("Added ban for %s until %v", c.IPOrSubnet, expirationTime)
			}
		}
	case "remove":
		var success bool

		if s.p2pClient != nil {
			err := s.p2pClient.UnbanPeer(ctx, c.IPOrSubnet)
			if err == nil {
				success = true
			} else {
				s.logger.Errorf("Error while trying to unban teranode peer: %v", err)
			}
		}

		// unban legacy peer
		if s.legacyP2PClient != nil {
			resp, err := s.legacyP2PClient.UnbanPeer(ctx, &peer_api.UnbanPeerRequest{
				Addr: c.IPOrSubnet,
			})
			if err != nil {
				s.logger.Errorf("Error while trying to unban legacy peer: %v", err)

				if !success {
					return nil, &bsvjson.RPCError{
						Code:    bsvjson.ErrRPCInvalidParameter,
						Message: "Error while trying to unban peer",
					}
				}
			} else if resp == nil || !resp.Ok {
				if !success {
					return nil, &bsvjson.RPCError{
						Code:    bsvjson.ErrRPCInvalidParameter,
						Message: "Failed to unban peer",
					}
				}
			} else {
				s.logger.Debugf("Removed ban for %s", c.IPOrSubnet)
			}
		}
	default:
		return nil, &bsvjson.RPCError{
			Code:    bsvjson.ErrRPCInvalidParameter,
			Message: "Invalid command. Must be 'add' or 'remove'.",
		}
	}

	return nil, nil
}

// handleGetMiningInfo implements the getmininginfo command, which returns information
// about the current mining state and network statistics.
//
// This command provides comprehensive mining-related information including:
// - Current block count and blockchain height
// - Current difficulty target for proof-of-work
// - Estimated network hash rate (hashes per second)
// - Number of transactions in the memory pool
// - Current block size and transaction count being assembled
// - Any errors or warnings related to mining operations
//
// The information returned is essential for:
// - Miners monitoring network conditions and profitability
// - Mining pool operators tracking network statistics
// - Applications calculating mining difficulty and expected returns
// - Network monitoring tools assessing blockchain health and security
//
// The command aggregates data from multiple sources including the blockchain service,
// block assembly service, and network statistics to provide a complete mining overview.
//
// Parameters:
//   - ctx: Context for cancellation and tracing
//   - s: The RPC server instance providing access to service clients
//   - cmd: The parsed command arguments (no specific parameters)
//   - _: Unused channel for close notification
//
// Returns:
//   - interface{}: Object containing comprehensive mining information and network statistics
//   - error: Any error encountered while retrieving mining information
func handleGetMiningInfo(ctx context.Context, s *RPCServer, cmd interface{}, _ <-chan struct{}) (interface{}, error) {
	_, _, deferFn := tracing.Tracer("rpc").Start(ctx, "handleGetMiningInfo",
		tracing.WithParentStat(RPCStat),
		tracing.WithHistogram(prometheusHandleGetMiningInfo),
		tracing.WithLogMessage(s.logger, "[handleGetMiningInfo] called"),
	)
	defer deferFn()

	bestBlockHeader, bestBlockMeta, err := s.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		return nil, err
	}
	// {
	//   "blocks": 868496,
	//   "currentblocksize": 0,
	//   "currentblocktx": 0,
	//   "difficulty": 97415240192.16336,
	//   "errors": "",
	//   "networkhashps": 7.08831367103262e+17,
	//   "pooledtx": 935,
	//   "chain": "main"
	// }

	difficulty, _ := bestBlockHeader.Bits.CalculateDifficulty().Float64()

	return &bsvjson.GetMiningInfoResult{
		Blocks:           int64(bestBlockMeta.Height),                                                           // The current block
		CurrentBlockSize: bestBlockMeta.SizeInBytes,                                                             // The last block size
		CurrentBlockTx:   bestBlockMeta.TxCount,                                                                 // The last block transaction
		Difficulty:       difficulty,                                                                            // The current difficulty
		Errors:           "",                                                                                    // Current errors
		NetworkHashPS:    calculateHashRate(difficulty, s.settings.ChainCfgParams.TargetTimePerBlock.Seconds()), // The network hashes per second
		// PooledTx:      0,                           // The size of the mempool - we don't have a mempool
		Chain: s.settings.ChainCfgParams.Name, // current network name as defined in BIP70 (main, test, regtest)
	}, nil
}

// handleFreeze implements the freeze command, which marks a specific UTXO as frozen,
// preventing it from being spent in future transactions.
//
// This command provides UTXO management functionality by allowing administrators to
// temporarily or permanently prevent specific transaction outputs from being spent.
// When a UTXO is frozen, it remains in the UTXO set but cannot be used as an input
// for new transactions until it is explicitly unfrozen.
//
// Common use cases for freezing UTXOs include:
// - Compliance requirements for suspicious or disputed funds
// - Temporary holds during investigation or audit processes
// - Risk management for high-value outputs
// - Testing and debugging scenarios
// - Regulatory compliance and law enforcement cooperation
//
// The freeze operation is reversible through the unfreeze command, allowing for
// flexible UTXO management based on changing circumstances or investigation outcomes.
//
// Security considerations:
// - Requires admin privileges to execute
// - Changes persist across node restarts
// - Affects transaction validation and mempool acceptance
// - Should be used carefully to avoid disrupting legitimate transactions
//
// Parameters:
//   - ctx: Context for cancellation and tracing
//   - s: The RPC server instance providing access to service clients
//   - cmd: The parsed command arguments (bsvjson.FreezeCmd with TxID and Vout)
//   - _: Unused channel for close notification
//
// Returns:
//   - interface{}: Null on success
//   - error: Any error encountered during the freeze operation
func handleFreeze(ctx context.Context, s *RPCServer, cmd interface{}, _ <-chan struct{}) (interface{}, error) {
	_, _, deferFn := tracing.Tracer("rpc").Start(ctx, "handleFreeze",
		tracing.WithParentStat(RPCStat),
		tracing.WithHistogram(prometheusHandleFreeze),
		tracing.WithLogMessage(s.logger, "[handleFreeze] called"),
	)
	defer deferFn()

	c := cmd.(*bsvjson.FreezeCmd)

	var err error

	h, err := chainhash.NewHashFromStr(c.TxID)
	if err != nil {
		return nil, err
	}

	if err = s.utxoStore.FreezeUTXOs(ctx, []*utxo.Spend{{TxID: h, Vout: uint32(c.Vout), UTXOHash: h}}, s.settings); err != nil { // nolint:gosec
		return nil, err
	}

	return nil, nil
}

// handleUnfreeze implements the unfreeze command, which removes the frozen status from
// a previously frozen UTXO, allowing it to be spent in future transactions.
//
// This command reverses the effect of freeze by undoing the manual freezing of a UTXO.
// This allows the node to consider the UTXO for spending during its normal transaction
// validation process and potentially accept it if it follows consensus rules.
//
// The unfreeze operation is used to correct mistaken manual interventions in the UTXO
// management process.
//
// Security considerations:
// - Requires admin privileges to execute
// - May trigger transaction reordering in the transaction processing system
// - Changes persist across node restarts
// - Should be used carefully to avoid disrupting legitimate transactions
//
// Parameters:
//   - ctx: Context for cancellation and tracing
//   - s: The RPC server instance providing access to service clients
//   - cmd: The parsed command arguments (bsvjson.UnfreezeCmd with TxID and Vout)
//   - _: Unused channel for close notification
//
// Returns:
//   - interface{}: Null on success
//   - error: Any error encountered during the unfreeze operation
func handleUnfreeze(ctx context.Context, s *RPCServer, cmd interface{}, _ <-chan struct{}) (interface{}, error) {
	_, _, deferFn := tracing.Tracer("rpc").Start(ctx, "handleUnfreeze",
		tracing.WithParentStat(RPCStat),
		tracing.WithHistogram(prometheusHandleUnfreeze),
		tracing.WithLogMessage(s.logger, "[handleUnfreeze] called"),
	)
	defer deferFn()

	c := cmd.(*bsvjson.UnfreezeCmd)

	var err error

	h, err := chainhash.NewHashFromStr(c.TxID)
	if err != nil {
		return nil, err
	}

	if err = s.utxoStore.UnFreezeUTXOs(ctx, []*utxo.Spend{{TxID: h, Vout: uint32(c.Vout), UTXOHash: h}}, s.settings); err != nil { // nolint:gosec
		return nil, err
	}

	return nil, nil
}

// handleReassign implements the reassign command, which reassigns a specific UTXO to a
// new UTXO hash, allowing for flexible UTXO management.
//
// This command provides a way to update the UTXO set by reassigning a UTXO to a new
// UTXO hash. This is useful in scenarios such as:
// - Correcting errors in the UTXO set
// - Updating the UTXO set after a chain reorganization
// - Managing UTXOs during testing and debugging
//
// The reassign operation is used to correct mistakes in the UTXO management process.
//
// Security considerations:
// - Requires admin privileges to execute
// - May trigger transaction reordering in the transaction processing system
// - Changes persist across node restarts
// - Should be used carefully to avoid disrupting legitimate transactions
//
// Parameters:
//   - ctx: Context for cancellation and tracing
//   - s: The RPC server instance providing access to service clients
//   - cmd: The parsed command arguments (bsvjson.ReassignCmd with OldTxID, OldVout, and NewUTXOHash)
//   - _: Unused channel for close notification
//
// Returns:
//   - interface{}: Null on success
//   - error: Any error encountered during the reassign operation
func handleReassign(ctx context.Context, s *RPCServer, cmd interface{}, _ <-chan struct{}) (interface{}, error) {
	_, _, deferFn := tracing.Tracer("rpc").Start(ctx, "handleReassign",
		tracing.WithParentStat(RPCStat),
		tracing.WithHistogram(prometheusHandleReassign),
		tracing.WithLogMessage(s.logger, "[handleReassign] called"),
	)
	defer deferFn()

	c := cmd.(*bsvjson.ReassignCmd)

	var err error

	oldTXIDHash, err := chainhash.NewHashFromStr(c.OldTxID)
	if err != nil {
		return nil, err
	}

	oldUTXOHash, err := chainhash.NewHashFromStr(c.OldUTXOHash)
	if err != nil {
		return nil, err
	}

	newUTXOHash, err := chainhash.NewHashFromStr(c.NewUTXOHash)
	if err != nil {
		return nil, err
	}

	if err = s.utxoStore.ReAssignUTXO(ctx,
		&utxo.Spend{TxID: oldTXIDHash, Vout: uint32(c.OldVout), UTXOHash: oldUTXOHash}, // nolint:gosec
		&utxo.Spend{UTXOHash: newUTXOHash}, s.settings); err != nil {
		return nil, err
	}

	return nil, nil
}

// messageToHex serializes a wire protocol message to its binary representation
// and returns it as a hex-encoded string.
//
// This utility method provides consistent serialization of Bitcoin protocol
// messages to a standardized format suitable for transmission over the RPC
// interface. It ensures that all messages are encoded using the latest
// protocol version for maximum compatibility.
//
// The method is used by various RPC handlers to convert internal message
// structures to the string representation expected by RPC clients, maintaining
// consistency in how binary data is presented in the JSON-RPC responses.
//
// Parameters:
//   - msg: The wire.Message to serialize
//
// Returns:
//   - string: Hex-encoded string representation of the serialized message
//   - error: Any error encountered during serialization
func (s *RPCServer) messageToHex(msg wire.Message) (string, error) {
	var buf bytes.Buffer
	if err := msg.BsvEncode(&buf, wire.ProtocolVersion, wire.BaseEncoding); err != nil {
		context := fmt.Sprintf("Failed to encode msg of type %T", msg)
		return "", s.internalRPCError(err.Error(), context)
	}

	return hex.EncodeToString(buf.Bytes()), nil
}

// calculateHashRate computes the estimated network hash rate based on
// the current difficulty and average block time.
//
// This utility function implements the standard formula for estimating the
// Bitcoin network's total hash rate:
//
//	hashRate = difficulty * 2^32 / blockTimeInSeconds
//
// The result represents the estimated hashes per second being performed
// by all miners on the network combined. This is a key metric reported
// in various RPC commands for monitoring network strength and mining
// competitiveness.
//
// Parameters:
//   - difficulty: The current network difficulty value
//   - blockTime: The average time between blocks in seconds
//
// Returns:
//   - float64: Estimated network hash rate in hashes per second
func calculateHashRate(difficulty float64, blockTime float64) float64 {
	return (difficulty * math.Pow(2, 32)) / blockTime
}

// isIPOrSubnet validates whether a string is a valid IP address or subnet in CIDR notation.
//
// This utility function is used primarily by the network ban management RPC commands
// to validate input parameters before attempting to ban or unban addresses. It supports
// both IPv4 and IPv6 addresses, and can validate subnet specifications in CIDR notation
// (e.g., 192.168.1.0/24 or 2001:db8::/32).
//
// The function is designed to be conservative, returning false for any input that
// cannot be definitively determined to be a valid IP address or subnet, helping
// prevent accidental banning of invalid network specifications.
//
// Parameters:
//   - ipOrSubnet: String representation of an IP address or subnet to validate
//
// Returns:
//   - bool: true if the string is a valid IP or subnet, false otherwise
func isIPOrSubnet(ipOrSubnet string) bool {
	// no slash means ip
	if !strings.Contains(ipOrSubnet, "/") {
		_, err := net.ResolveIPAddr("ip", ipOrSubnet)
		return err == nil
	}

	if strings.Contains(ipOrSubnet, ":") {
		// remove port
		ipOrSubnet = strings.Split(ipOrSubnet, ":")[0]
	}

	_, _, err := net.ParseCIDR(ipOrSubnet)

	return err == nil
}

// handleGetchaintips implements the getchaintips command.
func handleGetchaintips(ctx context.Context, s *RPCServer, cmd interface{}, _ <-chan struct{}) (interface{}, error) {
	ctx, _, deferFn := tracing.Tracer("rpc").Start(ctx, "handleGetchaintips",
		tracing.WithParentStat(RPCStat),
		tracing.WithHistogram(prometheusHandleGetchaintips),
		tracing.WithLogMessage(s.logger, "[handleGetchaintips] called"),
	)
	defer deferFn()

	if cached, found := rpcCallCache.Get("getchaintips"); found {
		return cached, nil
	}

	_, ok := cmd.(*bsvjson.GetChainTipsCmd)
	if !ok {
		return nil, bsvjson.ErrRPCInternal
	}

	// Get chain tips from the blockchain client
	chainTipsData, err := s.blockchainClient.GetChainTips(ctx)
	if err != nil {
		s.logger.Errorf("Failed to get chain tips: %v", err)
		return nil, bsvjson.ErrRPCInternal
	}

	// Convert to the expected JSON-RPC response format
	result := make([]map[string]interface{}, 0, len(chainTipsData))
	for _, tipInfo := range chainTipsData {
		result = append(result, map[string]interface{}{
			"height":    int64(tipInfo.Height),
			"hash":      tipInfo.Hash,
			"branchlen": int(tipInfo.Branchlen),
			"status":    tipInfo.Status,
		})
	}

	if s.settings.RPC.CacheEnabled {
		rpcCallCache.Set("getchaintips", result, 300*time.Second)
	}

	return result, nil
}

// generateBlocksAndReturnHashes is a shared helper method that generates blocks and returns their hashes.
// This method is used by both handleGenerate and handleGenerateToAddress to avoid code duplication.
//
// Parameters:
//   - ctx: Context for cancellation and tracing
//   - s: The RPC server instance providing access to service clients
//   - count: Number of blocks to generate
//   - address: Optional address for mining rewards (nil for generate command)
//   - maxTries: Optional maximum number of attempts (nil for generate command)
//
// Returns:
//   - []string: Array of block hashes of the generated blocks
//   - error: Any error encountered during block generation
func (s *RPCServer) generateBlocksAndReturnHashes(ctx context.Context, count int32, address *string, maxTries *int32) ([]string, error) {
	// Get the current best block hash before generation
	_, bestBlockMeta, err := s.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		return nil, &bsvjson.RPCError{
			Code:    bsvjson.ErrRPCInternal.Code,
			Message: errors.NewServiceError("RPC blockchain client", err).Error(),
		}
	}
	startHeight := bestBlockMeta.Height

	// Generate the blocks
	err = s.blockAssemblyClient.GenerateBlocks(ctx, &blockassembly_api.GenerateBlocksRequest{
		Count:    count,
		Address:  address,
		MaxTries: maxTries,
	})
	if err != nil {
		return nil, &bsvjson.RPCError{
			Code:    bsvjson.ErrRPCInternal.Code,
			Message: errors.NewServiceError("RPC blockassembly client", err).Error(),
		}
	}

	// Collect the block hashes of the generated blocks
	// Note: There may be a brief delay between block generation and availability in blockchain client
	var blockHashes []string
	for i := int32(1); i <= count; i++ {
		targetHeight := startHeight + uint32(i)

		// Get the block at the target height
		block, err := s.blockchainClient.GetBlockByHeight(ctx, targetHeight)
		if err != nil {
			return nil, &bsvjson.RPCError{
				Code:    bsvjson.ErrRPCInternal.Code,
				Message: errors.NewServiceError("RPC blockchain client", err).Error(),
			}
		}

		blockHashes = append(blockHashes, block.Header.Hash().String())
	}

	return blockHashes, nil
}
