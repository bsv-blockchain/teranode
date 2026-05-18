//go:build !bdk

package consensus

import (
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
)

func (vi *ValidatorIntegration) verifyGoBDKScriptOnly(tx *bt.Tx, blockHeight uint32, utxoHeights []uint32) error {
	return errors.NewProcessingError("go-bdk validator not registered")
}
