package svnode

import (
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/sighash"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/errors"
)

// TxCreator helps create and fund transactions for testing
// It manages a private key and uses an SVNode to create funding transactions
//
// How it works:
//  1. TxCreator generates/holds a private key and derives a Bitcoin address from it
//  2. It uses SVNode RPC calls (sendtoaddress) to request the node's wallet to send
//     coins to this address, creating a funding transaction
//  3. It calls generate(1) to mine a block containing the funding transaction,
//     confirming it and making the funds spendable
//  4. Since TxCreator holds the private key for the receiving address, it has full
//     control over these funds and can create spending transactions
//  5. This gives tests the "freedom" to spend these funds however needed for testing
//
// Example flow:
//
//	txCreator := NewTxCreator(svNode, privateKey)
//	utxo := txCreator.CreateConfirmedFunding(10.0)  // Get 10 BSV we can spend
//	tx := txCreator.CreateSpendingTransaction([]*FundingUTXO{utxo}, builder)
type TxCreator struct {
	privKey *bec.PrivateKey
	svNode  SVNodeI
	address string
}

// FundingUTXO represents a funded UTXO that can be spent in tests
type FundingUTXO struct {
	TxID          string
	Vout          uint32
	Amount        uint64
	LockingScript *bscript.Script
	Tx            *bt.Tx // Full funding transaction for reference
}

// TxBuilder is a callback function that constructs a transaction from funded UTXOs
// It receives the UTXOs to spend and should return a complete unsigned transaction
// The TxCreator will handle signing afterwards
type TxBuilder func(utxos []*FundingUTXO) (*bt.Tx, error)

// NewTxCreator creates a transaction creator with the given private key and SVNode
func NewTxCreator(svNode SVNodeI, privKey *bec.PrivateKey) (*TxCreator, error) {
	// Generate address from private key (use testnet format for regtest)
	address, err := bscript.NewAddressFromPublicKey(privKey.PubKey(), false)
	if err != nil {
		return nil, errors.NewProcessingError("failed to create address from private key", err)
	}

	return &TxCreator{
		privKey: privKey,
		svNode:  svNode,
		address: address.AddressString,
	}, nil
}

// NewTxCreatorWithGeneratedKey creates a transaction creator with an auto-generated key
func NewTxCreatorWithGeneratedKey(svNode SVNodeI) (*TxCreator, error) {
	privKey, err := bec.NewPrivateKey()
	if err != nil {
		return nil, errors.NewProcessingError("failed to generate private key", err)
	}

	return NewTxCreator(svNode, privKey)
}

// Address returns the Bitcoin address associated with this creator's private key
func (tc *TxCreator) Address() string {
	return tc.address
}

// CreateFunds requests the SVNode to send funds to this creator's address
// Returns the funding UTXO that can be spent in subsequent transactions
//
// How it works:
//  1. Calls SVNode RPC method 'sendtoaddress' with our address and amount
//  2. SVNode's wallet creates a transaction sending coins to our address
//  3. The transaction is broadcast to the mempool but NOT yet mined
//  4. We retrieve the transaction and identify our output (the funding UTXO)
//  5. Since we hold the private key for this address, we can later spend this UTXO
//
// Note: The funding transaction is NOT mined - it's only in the mempool.
// Call CreateConfirmedFunding() or manually call svNode.Generate(1) to mine it.
func (tc *TxCreator) CreateFunds(amount float64) (*FundingUTXO, error) {
	// Have SVNode send funds to our address via RPC call
	txid, err := tc.svNode.SendToAddress(tc.address, amount)
	if err != nil {
		return nil, errors.NewProcessingError("failed to send funds to address", err)
	}

	return tc.getFundingUTXO(txid)
}

// CreateConfirmedFunding creates a funding transaction and mines 1 block to confirm it
// This makes the funding UTXO spendable by satisfying the coinbase maturity requirement
// Returns the confirmed funding UTXO that can be spent in subsequent transactions
//
// Complete flow:
//  1. Calls CreateFunds() which uses 'sendtoaddress' RPC to send coins to our address
//  2. The funding transaction is created and broadcast to mempool
//  3. Calls 'generate(1)' RPC to mine 1 block containing our funding transaction
//  4. The funding transaction is now confirmed and the UTXO is spendable
//  5. We return the UTXO which we can freely spend (because we hold the private key)
//
// This is the most common pattern for test setup: get confirmed funds we can spend.
func (tc *TxCreator) CreateConfirmedFunding(amount float64) (*FundingUTXO, error) {
	// Create funding transaction via sendtoaddress RPC
	utxo, err := tc.CreateFunds(amount)
	if err != nil {
		return nil, err
	}

	// Mine 1 block to confirm the funding via generate(1) RPC
	_, err = tc.svNode.Generate(1)
	if err != nil {
		return nil, errors.NewProcessingError("failed to mine confirmation block", err)
	}

	return utxo, nil
}

// getFundingUTXO retrieves the funding transaction and extracts our UTXO
func (tc *TxCreator) getFundingUTXO(txid string) (*FundingUTXO, error) {
	// Get the funding transaction
	fundingTx, err := tc.svNode.GetRawTransaction(txid)
	if err != nil {
		return nil, errors.NewProcessingError("failed to get funding transaction", err)
	}

	// Find which output is ours
	expectedScript, err := bscript.NewP2PKHFromAddress(tc.address)
	if err != nil {
		return nil, errors.NewProcessingError("failed to create expected script", err)
	}

	// Search for our output
	for i, output := range fundingTx.Outputs {
		if output.LockingScript != nil && output.LockingScript.String() == expectedScript.String() {
			return &FundingUTXO{
				TxID:          txid,
				Vout:          uint32(i),
				Amount:        output.Satoshis,
				LockingScript: output.LockingScript,
				Tx:            fundingTx,
			}, nil
		}
	}

	return nil, errors.NewProcessingError("funding output not found in transaction %s", txid)
}

// CreateSpendingTransaction creates a transaction spending the given UTXOs
// The builder callback constructs the transaction, and this method signs it
func (tc *TxCreator) CreateSpendingTransaction(utxos []*FundingUTXO, builder TxBuilder) (*bt.Tx, error) {
	if len(utxos) == 0 {
		return nil, errors.NewInvalidArgumentError("no UTXOs provided")
	}

	// Let the builder construct the transaction
	tx, err := builder(utxos)
	if err != nil {
		return nil, errors.NewProcessingError("transaction builder failed", err)
	}

	// Sign all inputs
	for i := range tx.Inputs {
		if i >= len(utxos) {
			return nil, errors.NewProcessingError("transaction has more inputs than UTXOs provided")
		}

		// Calculate signature hash
		sigHash, err := tx.CalcInputSignatureHash(uint32(i), sighash.AllForkID)
		if err != nil {
			return nil, errors.NewProcessingError("failed to calculate signature hash for input %d", i, err)
		}

		// Sign
		sig, err := tc.privKey.Sign(sigHash)
		if err != nil {
			return nil, errors.NewProcessingError("failed to sign input %d", i, err)
		}

		// Create unlocking script for P2PKH: <signature> <pubkey>
		unlockScript := &bscript.Script{}
		sigBytes := append(sig.Serialize(), byte(sighash.AllForkID))
		_ = unlockScript.AppendPushData(sigBytes)
		_ = unlockScript.AppendPushData(tc.privKey.PubKey().Compressed())

		tx.Inputs[i].UnlockingScript = unlockScript
	}

	return tx, nil
}

// DefaultTxBuilder creates a simple transaction builder that sends funds to a specified address
// This is the default "Alice pays Bob" pattern
func DefaultTxBuilder(toAddress string, fee uint64) TxBuilder {
	return func(utxos []*FundingUTXO) (*bt.Tx, error) {
		tx := bt.NewTx()

		// Calculate total input amount
		var totalInput uint64
		for _, utxo := range utxos {
			totalInput += utxo.Amount

			// Add input - use the funding transaction's TxIDChainHash
			btUtxo := &bt.UTXO{
				TxIDHash:      utxo.Tx.TxIDChainHash(),
				Vout:          utxo.Vout,
				LockingScript: utxo.LockingScript,
				Satoshis:      utxo.Amount,
			}
			err := tx.FromUTXOs(btUtxo)
			if err != nil {
				return nil, errors.NewProcessingError("failed to add input from UTXO", err)
			}
		}

		// Calculate output amount (total - fee)
		if totalInput <= fee {
			return nil, errors.NewInvalidArgumentError("total input %d is less than or equal to fee %d", totalInput, fee)
		}
		outputAmount := totalInput - fee

		// Add output to destination address
		err := tx.AddP2PKHOutputFromAddress(toAddress, outputAmount)
		if err != nil {
			return nil, errors.NewProcessingError("failed to add output", err)
		}

		return tx, nil
	}
}

// SelfPaymentBuilder creates a builder that sends funds back to this creator's address
// Useful for testing transaction flow without needing a second address
func (tc *TxCreator) SelfPaymentBuilder(fee uint64) TxBuilder {
	return DefaultTxBuilder(tc.address, fee)
}
