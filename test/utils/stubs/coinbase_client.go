package stubs

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2"
)

// CoinbaseClient is a stub implementation of the coinbase client for testing
type CoinbaseClient struct {
	balance uint64
}

type MalformationType int32

const (
	ZeroSatoshis MalformationType = 0
)

// NewCoinbaseClient creates a new stub coinbase client
func NewCoinbaseClient() *CoinbaseClient {
	return &CoinbaseClient{
		balance: 100000000, // 1 BSV in satoshis as default balance
	}
}

// GetBalance returns a mock balance
func (c *CoinbaseClient) GetBalance(ctx context.Context) (uint64, uint64, error) {
	return c.balance, 0, nil
}

// RequestFunds creates a mock transaction with the requested funds
func (c *CoinbaseClient) RequestFunds(ctx context.Context, address string, wait bool) (*bt.Tx, error) {
	// Create a new transaction
	tx := bt.NewTx()

	return tx, nil
}

func (c *CoinbaseClient) SetMalformedUTXOConfig(ctx context.Context, percentage int32, malformationType MalformationType) error {
	return nil
}
