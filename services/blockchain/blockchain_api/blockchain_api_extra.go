package blockchain_api

import (
	"fmt"

	"github.com/ordishs/go-utils"
)

func (n *Notification) Stringify() string {
	return fmt.Sprintf("%s: %s, metadata: %s", n.Type.String(), utils.ReverseAndHexEncodeSlice(n.Hash), n.Metadata)
}
