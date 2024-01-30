package redis

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/libsv/go-bt/v2/chainhash"
)

type Value struct {
	LockTime     uint32
	SpendingTxID *chainhash.Hash
}

func NewValueFromString(s string) *Value {
	v := &Value{}

	parts := strings.Split(s, ",")

	locktime, err := strconv.Atoi(parts[0])
	if err != nil {
		panic(err)
	}
	v.LockTime = uint32(locktime)

	if len(parts) > 1 {
		h, err := chainhash.NewHashFromStr(parts[1])
		if err != nil {
			panic(err)
		}
		v.SpendingTxID = h
	}

	return v
}

func (v *Value) String() string {
	if v.SpendingTxID == nil {
		return fmt.Sprintf("%d", v.LockTime)
	}
	return fmt.Sprintf("%d,%s", v.LockTime, v.SpendingTxID.String())
}
