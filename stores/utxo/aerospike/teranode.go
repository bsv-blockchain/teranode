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
//   - Automatic cleanup of spent UTXOs through TTL
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
//   - nrUtxos: Total number of UTXOs
//   - spentUtxos: Number of spent UTXOs
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
	"strings"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/uaerospike"
	"github.com/libsv/go-bt/v2/chainhash"
)

//go:embed teranode.lua
var teranodeLUA []byte

var LuaPackage = "teranode_v19" // N.B. Do not have any "." in this string

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

	// External indicates if the transaction uses external storage
	External bool
}

// Lua Return Values
const (
	// LuaOk indicates successful operation
	LuaOk LuaReturnValue = "OK"

	// LuaTTLSet indicates TTL was set on the record
	LuaTTLSet LuaReturnValue = "TTLSET"

	// LuaSpent indicates UTXO is already spent
	LuaSpent LuaReturnValue = "SPENT"

	// LuaExternal indicates transaction uses external storage
	LuaExternal LuaReturnValue = "EXTERNAL"

	// LuaAllSpent indicates all UTXOs in transaction are spent
	LuaAllSpent LuaReturnValue = "ALLSPENT"

	// LuaNotAllSpent indicates some UTXOs remain unspent
	LuaNotAllSpent LuaReturnValue = "NOTALLSPENT"

	// LuaFrozen indicates UTXO is frozen
	LuaFrozen LuaReturnValue = "FROZEN"

	// LuaTxNotFound indicates transaction doesn't exist
	LuaTxNotFound LuaReturnValue = "TX not found"

	// LuaError indicates operation failed
	LuaError LuaReturnValue = "ERROR"
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
	udfs, err := client.ListUDF(nil)
	if err != nil {
		return err
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
	}

	return nil
}

// ParseLuaReturnValue parses the colon-delimited response from Lua scripts.
// Format: "ReturnValue:Signal:External"
// Special case: If Signal is 64 chars, it's treated as a transaction hash.
//
// Example responses:
//
//	"OK:ALLSPENT"
//	"OK:TTLSET:EXTERNAL"
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
	rets := LuaReturnMessage{}

	r := strings.Split(returnValue, ":")

	if len(r) > 0 {
		rets.ReturnValue = LuaReturnValue(r[0])
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
		rets.External = r[2] == string(LuaExternal)
	}

	return rets, nil
}
