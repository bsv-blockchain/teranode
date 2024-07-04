package main

import (
	"fmt"

	helper "github.com/bitcoin-sv/ubsv/test/utils"
)

func main() {
	hashStr := "003e8c9abde82685fdacfd6594d9de14801c4964e1dbe79397afa6299360b521"
	getBlock, err := helper.CallRPC("http://localhost:19292", "getblock", []interface{}{hashStr, 1})
	if err != nil {
		fmt.Printf("error getting block: %v", err)
	}
	fmt.Println(getBlock)
}
