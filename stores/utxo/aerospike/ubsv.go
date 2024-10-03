package aerospike

import (
	_ "embed"
	"strings"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/uaerospike"
	"github.com/libsv/go-bt/v2/chainhash"
)

//go:embed ubsv.lua
var ubsvLUA []byte

var luaPackage = "ubsv_v17" // N.B. Do not have any "." in this string

// frozenUTXOBytes which is FF...FF, which is equivalent to a coinbase placeholder
var frozenUTXOBytes = util.CoinbasePlaceholder[:]

type luaReturnValue string

func (l *luaReturnValue) ToString() string {
	if l == nil {
		return ""
	}

	return string(*l)
}

type luaReturnMessage struct {
	returnValue  luaReturnValue
	signal       luaReturnValue
	spendingTxID *chainhash.Hash
	external     bool
}

const (
	LuaOk          luaReturnValue = "OK"
	LuaTTLSet      luaReturnValue = "TTLSET"
	LuaSpent       luaReturnValue = "SPENT"
	LuaExternal    luaReturnValue = "EXTERNAL"
	LuaAllSpent    luaReturnValue = "ALLSPENT"
	LuaNotAllSpent luaReturnValue = "NOTALLSPENT"
	LuaFrozen      luaReturnValue = "FROZEN"
	LuaTxNotFound  luaReturnValue = "TX not found"
	LuaError       luaReturnValue = "ERROR"
)

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

func (s *Store) parseLuaReturnValue(returnValue string) (luaReturnMessage, error) {
	rets := luaReturnMessage{}

	r := strings.Split(returnValue, ":")

	if len(r) > 0 {
		rets.returnValue = luaReturnValue(r[0])
	}

	if len(r) > 1 {
		if len(r[1]) == 32 {
			hash, err := chainhash.NewHashFromStr(r[1])
			if err != nil {
				return rets, errors.NewProcessingError("error parsing spendingTxID %s", r[1], err)
			}

			rets.spendingTxID = hash
		} else {
			rets.signal = luaReturnValue(r[1])
		}
	}

	if len(r) > 2 {
		rets.external = r[2] == string(LuaExternal)
	}

	return rets, nil
}
