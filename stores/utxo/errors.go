package utxo

import "errors"

var (
	ErrNotFound      = errors.New("utxo not found")
	ErrAlreadyExists = errors.New("utxo already exists")
	ErrSpent         = errors.New("utxo already spent")
	ErrLockTime      = errors.New("utxo not spendable yet, due to lock time")
	ErrChainHash     = errors.New("utxo chain hash could not be calculated")
	ErrStore         = errors.New("utxo store error")
)
