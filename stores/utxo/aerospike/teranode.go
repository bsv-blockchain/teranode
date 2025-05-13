// Package aerospike provides an Aerospike-based implementation of the UTXO store interface.
// It offers high performance, distributed storage capabilities with support for large-scale
// UTXO sets and complex operations like freezing, reassignment, and batch processing.
//
// # Architecture
//
// The implementation uses a combination of Aerospike Key-Value store and Lua scripts
// for atomic operations. Transactions are stored with the following structure:
//   - Main Record: Contains transaction metadata and up to 20,000 UTXOs
//   - Pagination Records: Additional records for transactions with >20,000 outputs
//   - External Storage: Optional blob storage for large transactions
//
// # Features
//
//   - Efficient UTXO lifecycle management (create, spend, unspend)
//   - Support for batched operations with LUA scripting
//   - Automatic cleanup of spent UTXOs through DAH
//   - Alert system integration for freezing/unfreezing UTXOs
//   - Metrics tracking via Prometheus
//   - Support for large transactions through external blob storage
//
// # Usage
//
//	store, err := aerospike.New(ctx, logger, settings, &url.URL{
//	    Scheme: "aerospike",
//	    Host:   "localhost:3000",
//	    Path:   "/test/utxos",
//	    RawQuery: "expiration=3600&set=txmeta",
//	})
//
// # Database Structure
//
// Normal Transaction:
//   - inputs: Transaction input data
//   - outputs: Transaction output data
//   - utxos: List of UTXO hashes
//   - totalUtxos: Total number of UTXOs in the transaction
//   - recordUtxos: Total number of UTXO in this record
//   - spentUtxos: Number of spent UTXOs in this record
//   - blockIDs: Block references
//   - isCoinbase: Coinbase flag
//   - spendingHeight: Coinbase maturity height
//   - frozen: Frozen status
//
// Large Transaction with External Storage:
//   - Same as normal but with external=true
//   - Transaction data stored in blob storage
//   - Multiple records for >20k outputs
//
// # Thread Safety
//
// The implementation is fully thread-safe and supports concurrent access through:
//   - Atomic operations via Lua scripts
//   - Batched operations for better performance
//   - Lock-free reads with optimistic concurrency
package aerospike

import (
	_ "embed"
	"strconv"
	"strings"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/uaerospike"
	"github.com/libsv/go-bt/v2/chainhash"
)

//go:embed teranode.lua
var teranodeLUA []byte

var LuaPackage = "teranode_v34" // N.B. Do not have any "." in this string

// frozenUTXOBytes which is FF...FF, which is equivalent to a coinbase placeholder
var frozenUTXOBytes = util.CoinbasePlaceholder[:]

// LuaReturnValue represents the status code returned from Lua scripts.
type LuaReturnValue string

// GetFrozenUTXOBytes exposes frozenUTXOBytes to public for testing
func GetFrozenUTXOBytes() []byte {
	return frozenUTXOBytes
}

func (l *LuaReturnValue) ToString() string {
	if l == nil {
		return ""
	}

	return string(*l)
}

// LuaReturnMessage encapsulates the complete response from a Lua script,
// including status, additional signals, and transaction details.
type LuaReturnMessage struct {
	// ReturnValue indicates the primary operation result
	ReturnValue LuaReturnValue

	// Signal provides additional operation context
	Signal LuaReturnValue

	// SpendingTxID contains the spending transaction hash if relevant
	SpendingTxID *chainhash.Hash

	ChildCount int

	BlockIDs []int
}

func (l *LuaReturnMessage) String() string {
	if l == nil {
		return "<nil>"
	}

	sb := strings.Builder{}

	sb.WriteString(string(l.ReturnValue))

	if len(l.BlockIDs) > 0 {
		sb.WriteString(":[")

		for i, blockID := range l.BlockIDs {
			if i > 0 {
				sb.WriteString(",")
			}

			sb.WriteString(strconv.Itoa(blockID))
		}

		sb.WriteString("]")
	}

	if len(l.Signal) > 0 {
		sb.WriteString(":")
		sb.WriteString(string(l.Signal))
	}

	if l.SpendingTxID != nil {
		sb.WriteString(":")
		sb.WriteString(l.SpendingTxID.String())
	}

	if l.ChildCount > 0 {
		sb.WriteString(":")
		sb.WriteString(strconv.Itoa(l.ChildCount))
	}

	return sb.String()
}

// Lua Return Values
const (
	// LuaOk indicates successful operation
	LuaOk LuaReturnValue = "OK"

	// LuaDAHSet indicates deleteAtHeight was set on the record
	LuaDAHSet LuaReturnValue = "DAHSET"

	// LuaDAHUnset indicates deleteAtHeight was unset on the record
	LuaDAHUnset LuaReturnValue = "DAHUNSET"

	// LuaSpent indicates UTXO is already spent
	LuaSpent LuaReturnValue = "SPENT"

	// LuaAllSpent indicates all UTXOs in transaction are spent
	LuaAllSpent LuaReturnValue = "ALLSPENT"

	// LuaNotAllSpent indicates some UTXOs remain unspent
	LuaNotAllSpent LuaReturnValue = "NOTALLSPENT"

	// LuaFrozen indicates UTXO is frozen
	LuaFrozen LuaReturnValue = "FROZEN"

	// LuaTxNotFound indicates transaction doesn't exist
	LuaTxNotFound LuaReturnValue = "TX not found"

	// LuaConflicting indicates conflicting transaction
	LuaConflicting LuaReturnValue = "CONFLICTING"

	// LuaUnspendable indicates transaction is unspendable
	LuaUnspendable LuaReturnValue = "UNSPENDABLE"

	// LuaError indicates operation failed
	LuaError LuaReturnValue = "ERROR"

	// LuaCoinbaseImmature indicates coinbase is not spendable yet
	LuaCoinbaseImmature LuaReturnValue = "COINBASE_IMMATURE"
)

// registerLuaIfNecessary ensures required Lua scripts are registered with Aerospike.
// It checks for existing scripts and registers new ones if needed.
//
// Parameters:
//   - logger: For operation logging
//   - client: Aerospike client
//   - funcName: Name for the Lua package
//   - funcBytes: Lua script content
//
// Returns error if registration fails.
func registerLuaIfNecessary(logger ulogger.Logger, client *uaerospike.Client, funcName string, funcBytes []byte) error {
	var (
		udfs    []*aerospike.UDF
		listErr error
	)

	const (
		maxRetries = 5
		retryDelay = 1 * time.Second
	)

	for attempt := 1; attempt <= maxRetries; attempt++ {
		udfs, listErr = client.ListUDF(nil)
		if listErr == nil {
			// Success!
			break
		}

		// Check if the error is a known transient one using errors.As and Matches() with ResultCodes from types package
		var asErr aerospike.Error
		isTransientError := errors.As(listErr, &asErr) && asErr.Matches(types.INVALID_NODE_ERROR, types.TIMEOUT, types.NO_RESPONSE, types.NETWORK_ERROR, types.SERVER_NOT_AVAILABLE, types.NO_AVAILABLE_CONNECTIONS_TO_NODE)

		if isTransientError && attempt < maxRetries {
			logger.Warnf("Failed to list UDFs on attempt %d (cluster initializing?): %v. Retrying in %v...", attempt, listErr, retryDelay)
			time.Sleep(retryDelay)
		} else {
			// Not a transient error, or last attempt failed
			logger.Errorf("Failed to list UDFs after %d attempts: %v", attempt, listErr)
			return listErr
		}
	}
	// If loop finished without error, listErr is nil
	if listErr != nil {
		return listErr
	}

	foundScript := false

	for _, udf := range udfs {
		if udf.Filename == funcName+".lua" {
			logger.Infof("LUA script %s already registered", funcName)

			foundScript = true

			break
		}
	}

	if !foundScript {
		logger.Infof("LUA script %s not registered - registering", funcName)

		registerLua, err := client.RegisterUDF(nil, funcBytes, funcName+".lua", aerospike.LUA)
		if err != nil {
			return err
		}

		err = <-registerLua.OnComplete()
		if err != nil {
			return err
		}

		logger.Infof("LUA script %s registered successfully", funcName)
	}

	return nil
}

// ParseLuaReturnValue parses the colon-delimited response from Lua scripts.
// Format: "ReturnValue:Signal:External"
// Special case: If Signal is 64 chars, it's treated as a transaction hash.
//
// Example responses:
//
//	"OK:[]:ALLSPENT"
//	"OK:[1,2,3]:ALLSPENT"
//	"OK:[]:DAHSET:n"
//	"OK:[3,4]:DAHUNSET:n"
//	"SPENT:1234...5678" (where 1234...5678 is a 64-char tx hash)
//	"ERROR:TX not found"
//
// Parameters:
//   - returnValue: Raw response string from Lua
//
// Returns:
//   - Parsed LuaReturnMessage
//   - Error if parsing fails
func (s *Store) ParseLuaReturnValue(returnValue string) (LuaReturnMessage, error) {
	var err error

	rets := LuaReturnMessage{}

	r := strings.Split(returnValue, ":")

	if len(r) > 0 {
		rets.ReturnValue = LuaReturnValue(r[0])

		if rets.ReturnValue == LuaOk && len(r) > 1 && r[1][0] == '[' && r[1][len(r[1])-1] == ']' {
			// Assuming blockIDs are comma-separated in r[1]
			blockIDs := strings.Split(r[1][1:len(r[1])-1], ",")

			// Check if blockIDs is empty and skip the loop if it is
			if len(blockIDs) > 0 && blockIDs[0] != "" {
				// Process blockIDs as needed
				for _, blockID := range blockIDs {
					id, err := strconv.Atoi(blockID)
					if err != nil {
						return rets, errors.NewProcessingError("error parsing blockID %s", blockID, err)
					}

					rets.BlockIDs = append(rets.BlockIDs, id)
				}
			}

			// Remove r[1] from the slice
			r = append(r[:1], r[2:]...)
		}
	}

	if len(r) > 1 {
		if len(r[1]) == 64 {
			hash, err := chainhash.NewHashFromStr(r[1])
			if err != nil {
				return rets, errors.NewProcessingError("error parsing spendingTxID %s", r[1], err)
			}

			rets.SpendingTxID = hash
		} else {
			rets.Signal = LuaReturnValue(r[1])
		}
	}

	if len(r) > 2 {
		rets.ChildCount, err = strconv.Atoi(r[2])
		if err != nil {
			return rets, errors.NewProcessingError("error parsing child count %s", r[2], err)
		}
	}

	return rets, nil
}
