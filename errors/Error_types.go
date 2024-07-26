package errors

/// Error type usage guidelines:
/// storage error -> error returned from processing a file on s3, aerospike.
///	service error -> error when performing an operation with one of the services, i.e. if it is using our GRPC of one of our services, error when
/// processing error -> error when manipulating data, i.e. when processing a block, a transaction, etc., inside the method/function.

var (
	ErrUnknown                    = New(ERR_UNKNOWN, "unknown error")
	ErrInvalidArgument            = New(ERR_INVALID_ARGUMENT, "invalid argument")
	ErrThresholdExceeded          = New(ERR_THRESHOLD_EXCEEDED, "threshold exceeded")
	ErrNotFound                   = New(ERR_NOT_FOUND, "not found")
	ErrProcessing                 = New(ERR_PROCESSING, "error processing")
	ErrConfiguration              = New(ERR_CONFIGURATION, "configuration error")
	ErrError                      = New(ERR_ERROR, "generic error")
	ErrBlockNotFound              = New(ERR_BLOCK_NOT_FOUND, "block not found")
	ErrBlockInvalid               = New(ERR_BLOCK_INVALID, "block invalid")
	ErrBlockExists                = New(ERR_BLOCK_EXISTS, "block exists")
	ErrBlockError                 = New(ERR_BLOCK_ERROR, "block error")
	ErrSubtreeNotFound            = New(ERR_SUBTREE_NOT_FOUND, "subtree not found")
	ErrSubtreeInvalid             = New(ERR_SUBTREE_INVALID, "subtree invalid")
	ErrSubtreeInvalidFormat       = New(ERR_SUBTREE_INVALID_FORMAT, "subtree format is invalid")
	ErrSubtreeError               = New(ERR_SUBTREE_ERROR, "subtree error")
	ErrSubtreeExists              = New(ERR_SUBTREE_EXISTS, "subtree exists")
	ErrTxNotFound                 = New(ERR_TX_NOT_FOUND, "tx not found")
	ErrTxInvalid                  = New(ERR_TX_INVALID, "tx invalid")
	ErrTxInvalidDoubleSpend       = New(ERR_TX_INVALID_DOUBLE_SPEND, "tx invalid double spend")
	ErrTxAlreadyExists            = New(ERR_TX_ALREADY_EXISTS, "tx already exists")
	ErrTxError                    = New(ERR_TX_ERROR, "tx error")
	ErrServiceUnavailable         = New(ERR_SERVICE_UNAVAILABLE, "service unavailable")
	ErrServiceNotStarted          = New(ERR_SERVICE_NOT_STARTED, "service not started")
	ErrServiceError               = New(ERR_SERVICE_ERROR, "service error")
	ErrStorageUnavailable         = New(ERR_STORAGE_UNAVAILABLE, "storage unavailable")
	ErrStorageNotStarted          = New(ERR_STORAGE_NOT_STARTED, "storage not started")
	ErrStorageError               = New(ERR_STORAGE_ERROR, "storage error")
	ErrCoinbaseMissingBlockHeight = New(ERR_COINBASE_MISSING_BLOCK_HEIGHT, "the coinbase signature script doesn't have the block height")
	ErrSpent                      = New(ERR_SPENT, "utxo already spent")
	ErrLockTime                   = New(ERR_LOCKTIME, "Bad lock time")
	ErrKafkaDecode                = New(ERR_KAFKA_DECODE_ERROR, "error decoding kafka message")
)
