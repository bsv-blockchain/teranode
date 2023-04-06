package validator

import "github.com/libsv/go-bt/v2"

type Interface interface {
	Validate(tx *bt.Tx) error
}
