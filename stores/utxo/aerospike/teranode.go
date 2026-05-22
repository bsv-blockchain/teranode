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
	"sync"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
)

//go:embed teranode.lua
var teranodeLUA []byte

var (
	LuaPackage      = "teranode_v59" // N.B. Do not have any "." in this string
	LuaPackageMined = LuaPackage + "_mined"
)

// frozenUTXOBytes which is FF...FF, which is equivalent to a coinbase placeholder
var frozenUTXOBytes = subtree.FrozenBytes[:]

// LuaReturnValue represents the status code returned from Lua scripts.
type LuaReturnValue string

func (lr LuaReturnValue) String() string {
	return string(lr)
}

// GetFrozenUTXOBytes exposes frozenUTXOBytes to public for testing
func GetFrozenUTXOBytes() []byte {
	return frozenUTXOBytes
}

// Lua Return Values
const (
	// LuaSuccess indicates successful operation
	LuaSuccess LuaReturnValue = "SUCCESS"

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

	// LuaLocked indicates transaction is locked
	LuaLocked LuaReturnValue = "LOCKED"

	// LuaError indicates operation failed
	LuaError LuaReturnValue = "ERROR"

	// LuaCoinbaseImmature indicates coinbase is not spendable yet
	LuaCoinbaseImmature LuaReturnValue = "COINBASE_IMMATURE"

	// LuaPreserve indicates external files need preservation
	LuaPreserve LuaReturnValue = "PRESERVE"
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

// LuaStatus represents the status values in LuaMapResponse
type LuaStatus string

// Status constants for LuaMapResponse
const (
	LuaStatusOK    LuaStatus = "OK"
	LuaStatusError LuaStatus = "ERROR"
)

// LuaSignal represents the signal values in LuaMapResponse
type LuaSignal string

// Signal constants for LuaMapResponse
const (
	LuaSignalDAHSet      LuaSignal = "DAHSET"
	LuaSignalDAHUnset    LuaSignal = "DAHUNSET"
	LuaSignalAllSpent    LuaSignal = "ALLSPENT"
	LuaSignalNotAllSpent LuaSignal = "NOTALLSPENT"
	LuaSignalPreserve    LuaSignal = "PRESERVE"
)

// LuaErrorCode represents the error code values in LuaMapResponse
type LuaErrorCode string

// Error code constants for LuaMapResponse
const (
	LuaErrorCodeTxNotFound       LuaErrorCode = "TX_NOT_FOUND"
	LuaErrorCodeConflicting      LuaErrorCode = "CONFLICTING"
	LuaErrorCodeLocked           LuaErrorCode = "LOCKED"
	LuaErrorCodeCreating         LuaErrorCode = "CREATING"
	LuaErrorCodeFrozen           LuaErrorCode = "FROZEN"
	LuaErrorCodeAlreadyFrozen    LuaErrorCode = "ALREADY_FROZEN"
	LuaErrorCodeFrozenUntil      LuaErrorCode = "FROZEN_UNTIL"
	LuaErrorCodeCoinbaseImmature LuaErrorCode = "COINBASE_IMMATURE"
	LuaErrorCodeSpent            LuaErrorCode = "SPENT"
	LuaErrorCodeInvalidSpend     LuaErrorCode = "INVALID_SPEND"
	LuaErrorCodeUtxosNotFound    LuaErrorCode = "UTXOS_NOT_FOUND"
	LuaErrorCodeUtxoNotFound     LuaErrorCode = "UTXO_NOT_FOUND"
	LuaErrorCodeUtxoInvalidSize  LuaErrorCode = "UTXO_INVALID_SIZE"
	LuaErrorCodeUtxoHashMismatch LuaErrorCode = "UTXO_HASH_MISMATCH"
	LuaErrorCodeUtxoNotFrozen    LuaErrorCode = "UTXO_NOT_FROZEN"
	LuaErrorCodeInvalidParameter LuaErrorCode = "INVALID_PARAMETER"
)

// LuaErrorInfo represents an individual error from Lua functions
type LuaErrorInfo struct {
	ErrorCode    LuaErrorCode `json:"errorCode"`
	Message      string       `json:"message"`
	SpendingData string       `json:"spendingData,omitempty"`
}

// LuaMapResponse represents the structured response from Lua functions
type LuaMapResponse struct {
	Status     LuaStatus            `json:"status"`
	ErrorCode  LuaErrorCode         `json:"errorCode,omitempty"`
	Message    string               `json:"message,omitempty"`
	Signal     LuaSignal            `json:"signal,omitempty"`
	BlockIDs   []int                `json:"blockIDs,omitempty"`
	Errors     map[int]LuaErrorInfo `json:"errors,omitempty"`
	ChildCount int                  `json:"childCount,omitempty"`
	// Debug      string               `json:"debug,omitempty"`
}

// Reset clears all fields of LuaMapResponse so it can be reused via a sync.Pool.
//
// BlockIDs MUST be nil-ed (not truncated to [:0]) because the public contract
// observed by processBatchResultsForSetMined is "BlockIDs != nil iff the Lua
// response actually contained a blockIDs field". parseLuaMapResponseInto only
// writes BlockIDs when the response includes that key — error responses (e.g.
// TX_NOT_FOUND) leave BlockIDs untouched. Truncating to [:0] across pool
// iterations would carry forward a non-nil empty slice from a successful
// previous record and cause the caller to insert the failed record's hash
// into the returned map with an empty value. (See test
// TestAerospike/aerospike_setmined_multi_partial_failure.)
//
// The Errors map is cleared (entries deleted) rather than nil-ed so its backing
// buckets can be reused. parseLuaMapResponseInto explicitly checks the
// respMap["errors"] key existence before writing, so the same nil/empty
// contract concern does not apply — the loop body checks res.Signal and other
// scalar fields rather than relying on r.Errors == nil semantics.
func (r *LuaMapResponse) Reset() {
	r.Status = ""
	r.ErrorCode = ""
	r.Message = ""
	r.Signal = ""
	r.BlockIDs = nil
	if r.Errors != nil {
		for k := range r.Errors {
			delete(r.Errors, k)
		}
	}
	r.ChildCount = 0
}

// luaMapResponsePool reuses LuaMapResponse structs across set_mined batch records,
// where each record allocates one response. The pool is scoped to internal hot-loop
// callers (parseLuaMapResponseInto); the public ParseLuaMapResponse still allocates
// fresh structs for the 12+ other callers that don't release back to the pool.
var luaMapResponsePool = sync.Pool{
	New: func() interface{} { return &LuaMapResponse{} },
}

// getLuaMapResponse returns a zeroed LuaMapResponse from the pool.
func getLuaMapResponse() *LuaMapResponse {
	return luaMapResponsePool.Get().(*LuaMapResponse)
}

// putLuaMapResponse returns a LuaMapResponse to the pool after resetting it.
// Callers must not retain references to r or any of its fields after this call.
func putLuaMapResponse(r *LuaMapResponse) {
	if r == nil {
		return
	}
	r.Reset()
	luaMapResponsePool.Put(r)
}

// ParseLuaMapResponse parses the map response from Lua scripts.
// This handles the new structured response format where Lua returns a map.
// The returned struct is freshly allocated; callers that wish to participate
// in pooling should use parseLuaMapResponseInto with a pool-owned destination.
func (s *Store) ParseLuaMapResponse(response interface{}) (*LuaMapResponse, error) {
	result := &LuaMapResponse{}
	if err := s.parseLuaMapResponseInto(response, result); err != nil {
		return nil, err
	}
	return result, nil
}

// parseLuaMapResponseInto parses the Lua map response into the caller-provided
// destination. The destination must be zeroed (e.g. via Reset) before the call.
// This permits sync.Pool-backed reuse of LuaMapResponse across hot batch loops.
func (s *Store) parseLuaMapResponseInto(response interface{}, result *LuaMapResponse) error {
	// Handle the expected map response
	respMap, ok := response.(map[interface{}]interface{})
	if !ok {
		return errors.NewProcessingError("expected map response but got %T", response)
	}

	// Parse status
	if status, ok := respMap["status"].(string); ok {
		result.Status = LuaStatus(status)
	} else {
		return errors.NewProcessingError("missing or invalid status in response")
	}

	// Add debug field if it exists
	// if debug, ok := respMap["debug"].(string); ok {
	// 	fmt.Printf("*******************\ndebug: %s\n************************\n", debug)
	// 	result.Debug = debug
	// }

	// Parse optional fields
	if errorCode, ok := respMap["errorCode"].(string); ok {
		result.ErrorCode = LuaErrorCode(errorCode)
	}

	if msg, ok := respMap["message"].(string); ok {
		result.Message = msg
	}

	if signal, ok := respMap["signal"].(string); ok {
		result.Signal = LuaSignal(signal)
	}

	// Parse blockIDs (can be list or []interface{}). Reuse the existing slice
	// capacity if the caller obtained `result` from a pool and it had been used
	// previously (Reset truncates to [:0]). The presence of the blockIDs key —
	// even an empty list — must yield a non-nil result.BlockIDs to preserve the
	// existing observable contract.
	if blockIDs, ok := respMap["blockIDs"]; ok {
		switch v := blockIDs.(type) {
		case []interface{}:
			switch {
			case cap(result.BlockIDs) >= len(v) && result.BlockIDs != nil:
				result.BlockIDs = result.BlockIDs[:len(v)]
			case len(v) == 0:
				result.BlockIDs = make([]int, 0)
			default:
				result.BlockIDs = make([]int, len(v))
			}
			for i, id := range v {
				if idInt, ok := id.(int); ok {
					result.BlockIDs[i] = idInt
				} else {
					return errors.NewProcessingError("invalid blockID at index %d", i)
				}
			}
		default:
			return errors.NewProcessingError("invalid blockIDs type: %T", blockIDs)
		}
	}

	// Parse errors map for spendMulti
	if errorsField, ok := respMap["errors"]; ok {
		errMap, ok := errorsField.(map[interface{}]interface{})
		if !ok {
			return errors.NewProcessingError("invalid errors type: %T", errorsField)
		}

		if result.Errors == nil {
			result.Errors = make(map[int]LuaErrorInfo, len(errMap))
		}
		for k, v := range errMap {
			offset, ok := k.(int)
			if !ok {
				return errors.NewProcessingError("invalid error offset type: %T", k)
			}

			errorObj, ok := v.(map[interface{}]interface{})
			if !ok {
				return errors.NewProcessingError("invalid error object type: %T", v)
			}

			errorCode, ok := errorObj["errorCode"].(string)
			if !ok {
				return errors.NewProcessingError("invalid errorCode type in error object")
			}

			errorMessage, ok := errorObj["message"].(string)
			if !ok {
				return errors.NewProcessingError("invalid message type in error object")
			}

			errorInfo := LuaErrorInfo{
				ErrorCode: LuaErrorCode(errorCode),
				Message:   errorMessage,
			}

			// Parse optional spendingData field
			if spendingData, ok := errorObj["spendingData"].(string); ok {
				errorInfo.SpendingData = spendingData
			}

			result.Errors[offset] = errorInfo
		}
	}

	// Parse childCount
	if childCount, ok := respMap["childCount"]; ok {
		if count, ok := childCount.(int); ok {
			result.ChildCount = count
		}
	}

	return nil
}
