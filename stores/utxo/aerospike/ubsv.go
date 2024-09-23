package aerospike

import (
	_ "embed"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/uaerospike"
)

//go:embed ubsv.lua
var ubsvLUA []byte

var luaPackage = "ubsv_v14" // N.B. Do not have any "." in this string

// frozenUTXOBytes which is FF...FF, which is equivalent to a coinbase placeholder
var frozenUTXOBytes = util.CoinbasePlaceholder[:]

func registerLuaIfNecessary(logger ulogger.Logger, client *uaerospike.Client, funcName string, funcBytes []byte) error {
	udfs, err := client.ListUDF(nil)
	if err != nil {
		return err
	}

	foundScript := false

	for _, udf := range udfs {
		if udf.Filename == funcName+".lua" {
			logger.Infof("LUA script %s already registered", funcName)

			foundScript = true

			break
		}
	}

	if !foundScript {
		logger.Infof("LUA script %s not registered - registering", funcName)

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
