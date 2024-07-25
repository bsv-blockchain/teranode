package validator

import (
	"encoding/hex"
	"fmt"

	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/bscript/interpreter"
)

type TxValidator struct {
	policy *PolicySettings
}

func (tv *TxValidator) ValidateTransaction(tx *bt.Tx, blockHeight uint32) error {
	//
	// Each node will verify every transaction against a long checklist of criteria:
	//
	txSize := tx.Size()

	// 1) Neither lists of inputs or outputs are empty
	if len(tx.Inputs) == 0 || len(tx.Outputs) == 0 {
		return fmt.Errorf("transaction has no inputs or outputs")
	}

	// 2) The transaction size in bytes is less than maxtxsizepolicy.
	if err := tv.checkTxSize(txSize, tv.policy); err != nil {
		return err
	}

	// 3) check that each input value, as well as the sum, are in the allowed range of values (less than 21m coins)
	// 5) None of the inputs have hash=0, N=–1 (coinbase transactions should not be relayed)
	if err := tv.checkInputs(tx, blockHeight); err != nil {
		return err
	}

	// 4) Each output value, as well as the total, must be within the allowed range of values (less than 21m coins,
	//    more than the dust threshold if 1 unless it's OP_RETURN, which is allowed to be 0)
	if err := tv.checkOutputs(tx, blockHeight); err != nil {
		return err
	}

	// 6) nLocktime is equal to INT_MAX, or nLocktime and nSequence values are satisfied according to MedianTimePast
	//    => checked by the node, we do not want to have to know the current block height

	// 7) The transaction size in bytes is greater than or equal to 100
	// There are many examples in the chain up to height 422559 where this rule was not in place
	if blockHeight > 422559 && txSize < 100 {
		return fmt.Errorf("transaction size in bytes is less than 100 bytes")
	}

	// 8) The number of signature operations (SIGOPS) contained in the transaction is less than the signature operation limit
	if err := tv.sigOpsCheck(tx, tv.policy); err != nil {
		return err
	}

	// SAO - https://bitcoin.stackexchange.com/questions/83805/did-the-introduction-of-verifyscript-cause-a-backwards-incompatible-change-to-co
	// SAO - The rule enforcing that unlocking scripts must be "push only" became more relevant and started being enforced with the
	//       introduction of Segregated Witness (SegWit) which activated at height 481824.  BCH Forked before this at height 478559
	//       and therefore let's not enforce this check until then.
	if blockHeight >= util.ForkIDActivationHeight {
		// 9) The unlocking script (scriptSig) can only push numbers on the stack
		if err := tv.pushDataCheck(tx); err != nil {
			return err
		}
	}

	// 10) Reject if the sum of input values is less than sum of output values
	// 11) Reject if transaction fee would be too low (minRelayTxFee) to get into an empty block.
	if err := tv.checkFees(tx, feesToBtFeeQuote(tv.policy.GetMinMiningTxFee())); err != nil {
		return err
	}

	// 12) The unlocking scripts for each input must validate against the corresponding output locking scripts
	if err := tv.checkScripts(tx, blockHeight); err != nil {
		return err
	}

	// everything checks out
	return nil
}

func (tv *TxValidator) checkTxSize(txSize int, policy *PolicySettings) error {
	maxTxSizePolicy := policy.GetMaxTxSizePolicy()
	if maxTxSizePolicy == 0 {
		// no policy found for tx size, use max block size
		maxTxSizePolicy = MaxBlockSize
	}
	if txSize > maxTxSizePolicy {
		return fmt.Errorf("transaction size in bytes is greater than max tx size policy %d", maxTxSizePolicy)
	}

	return nil
}

func (tv *TxValidator) checkOutputs(tx *bt.Tx, blockHeight uint32) error {
	total := uint64(0)

	minOutput := uint64(0)
	if blockHeight >= util.GenesisActivationHeight {
		minOutput = bt.DustLimit
	}

	for index, output := range tx.Outputs {
		isData := output.LockingScript.IsData()
		switch {
		case !isData && (output.Satoshis > MaxSatoshis || output.Satoshis < minOutput):
			return fmt.Errorf("transaction output %d satoshis is invalid", index)
		case isData && output.Satoshis != 0 && blockHeight >= util.GenesisActivationHeight:
			return fmt.Errorf("transaction output %d has non 0 value op return (height=%d)", index, blockHeight)
		}
		total += output.Satoshis
	}

	if total > MaxSatoshis {
		return fmt.Errorf("transaction output total satoshis is too high")
	}

	return nil
}

func (tv *TxValidator) checkInputs(tx *bt.Tx, blockHeight uint32) error {
	total := uint64(0)
	for index, input := range tx.Inputs {
		if hex.EncodeToString(input.PreviousTxID()) == coinbaseTxID {
			return fmt.Errorf("transaction input %d is a coinbase input", index)
		}
		/* lots of our valid test transactions have this sequence number, is this not allowed?
		if input.SequenceNumber == 0xffffffff {
			fmt.Printf("input %d has sequence number 0xffffffff, txid = %s", index, tx.TxID())
			return fmt.Errorf("transaction input %d sequence number is invalid", index)
		}
		*/
		// if input.PreviousTxSatoshis == 0 && !input.PreviousTxScript.IsData() {
		// 	return fmt.Errorf("transaction input %d satoshis cannot be zero", index)
		// }
		if input.PreviousTxSatoshis > MaxSatoshis {
			return fmt.Errorf("transaction input %d satoshis is too high", index)
		}
		total += input.PreviousTxSatoshis
	}
	if total == 0 && blockHeight >= util.ForkIDActivationHeight {
		return fmt.Errorf("transaction input total satoshis cannot be zero")
	}
	if total > MaxSatoshis {
		return fmt.Errorf("transaction input total satoshis is too high")
	}

	return nil
}

func (tv *TxValidator) checkFees(tx *bt.Tx, feeQuote *bt.FeeQuote) error {
	feesOK, err := tx.IsFeePaidEnough(feeQuote)
	if err != nil {
		return err
	}

	if !feesOK {
		return fmt.Errorf("transaction fee is too low")
	}

	return nil
}

func (tv *TxValidator) sigOpsCheck(tx *bt.Tx, policy *PolicySettings) error {
	maxSigOps := policy.GetMaxTxSigopsCountsPolicy()

	if maxSigOps == 0 {
		maxSigOps = int64(MaxTxSigopsCountPolicyAfterGenesis)
	}

	numSigOps := int64(0)
	for _, input := range tx.Inputs {
		parser := interpreter.DefaultOpcodeParser{}
		parsedUnlockingScript, err := parser.Parse(input.PreviousTxScript)
		if err != nil {
			return err
		}

		for _, op := range parsedUnlockingScript {
			if op.Value() == bscript.OpCHECKSIG || op.Value() == bscript.OpCHECKSIGVERIFY {
				numSigOps++
				if numSigOps > maxSigOps {
					return fmt.Errorf("transaction unlocking scripts have too many sigops (%d)", numSigOps)
				}
			}
		}
	}

	return nil
}

func (tv *TxValidator) pushDataCheck(tx *bt.Tx) error {
	for index, input := range tx.Inputs {
		if input.UnlockingScript == nil {
			return fmt.Errorf("transaction input %d unlocking script is empty", index)
		}
		parser := interpreter.DefaultOpcodeParser{}
		parsedUnlockingScript, err := parser.Parse(input.UnlockingScript)
		if err != nil {
			return err
		}
		if !parsedUnlockingScript.IsPushOnly() {
			return fmt.Errorf("transaction input %d unlocking script is not push only", index)
		}
	}

	return nil
}

func (tv *TxValidator) checkScripts(tx *bt.Tx, blockHeight uint32) error {
	for i, in := range tx.Inputs {
		prevOutput := &bt.Output{
			Satoshis:      in.PreviousTxSatoshis,
			LockingScript: in.PreviousTxScript,
		}

		opts := make([]interpreter.ExecutionOptionFunc, 0, 3)
		opts = append(opts, interpreter.WithTx(tx, i, prevOutput))

		if blockHeight >= util.ForkIDActivationHeight {
			opts = append(opts, interpreter.WithForkID())
		}

		if blockHeight >= util.GenesisActivationHeight {
			opts = append(opts, interpreter.WithAfterGenesis())
		}

		// opts = append(opts, interpreter.WithDebugger(&LogDebugger{}),

		if err := interpreter.NewEngine().Execute(opts...); err != nil {
			return fmt.Errorf("script execution failed: %w", err)
		}
	}

	return nil
}
