package aerospike2

import (
	_ "embed"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/bitcoin-sv/ubsv/util/uaerospike"
)

//go:embed ubsv.lua
var ubsvLUA []byte

var luaPackage = "ubsv_v7" // N.B. Do not have any "." in this string

func registerLuaIfNecessary(client *uaerospike.Client, funcName string, funcBytes []byte) error {
	udfs, err := client.ListUDF(nil)
	if err != nil {
		return err
	}

	foundScript := false

	for _, udf := range udfs {
		if udf.Filename == funcName+".lua" {

			foundScript = true
			break
		}
	}

	if !foundScript {
		registerLua, err := client.RegisterUDF(nil, funcBytes, funcName+".lua", aerospike.LUA)
		if err != nil {
			return err
		}

		err = <-registerLua.OnComplete()
		if err != nil {
			return err
		}
	}
	return nil
}
