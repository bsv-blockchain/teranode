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

var LuaPackage = "ubsv_v19" // N.B. Do not have any "." in this string

// frozenUTXOBytes which is FF...FF, which is equivalent to a coinbase placeholder
var frozenUTXOBytes = util.CoinbasePlaceholder[:]

type LuaReturnValue string

// GetFrozenUTXOBytes exposes frozenUTXOBytes to public for testing
func GetFrozenUTXOBytes() []byte {
	return frozenUTXOBytes
}

func (l *LuaReturnValue) ToString() string {
	if l == nil {
		return ""
	}

	return string(*l)
}

type LuaReturnMessage struct {
	ReturnValue  LuaReturnValue
	Signal       LuaReturnValue
	SpendingTxID *chainhash.Hash
	External     bool
}

const (
	LuaOk          LuaReturnValue = "OK"
	LuaTTLSet      LuaReturnValue = "TTLSET"
	LuaSpent       LuaReturnValue = "SPENT"
	LuaExternal    LuaReturnValue = "EXTERNAL"
	LuaAllSpent    LuaReturnValue = "ALLSPENT"
	LuaNotAllSpent LuaReturnValue = "NOTALLSPENT"
	LuaFrozen      LuaReturnValue = "FROZEN"
	LuaTxNotFound  LuaReturnValue = "TX not found"
	LuaError       LuaReturnValue = "ERROR"
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

func (s *Store) ParseLuaReturnValue(returnValue string) (LuaReturnMessage, error) {
	rets := LuaReturnMessage{}

	r := strings.Split(returnValue, ":")

	if len(r) > 0 {
		rets.ReturnValue = LuaReturnValue(r[0])
	}

	if len(r) > 1 {
		if len(r[1]) == 64 {
			hash, err := chainhash.NewHashFromStr(r[1])
			if err != nil {
				return rets, errors.NewProcessingError("error parsing spendingTxID %s", r[1], err)
			}

			rets.SpendingTxID = hash
		} else {
			rets.Signal = LuaReturnValue(r[1])
		}
	}

	if len(r) > 2 {
		rets.External = r[2] == string(LuaExternal)
	}

	return rets, nil
}
