package validator

import "github.com/bsv-blockchain/go-bt/v2"

type noopBDKValidator struct{}

func (noopBDKValidator) ValidateTransaction(_ *bt.Tx, _ uint32, _ bool, _ []uint32) error {
	return nil
}
