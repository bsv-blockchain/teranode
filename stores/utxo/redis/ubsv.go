package redis

import (
	"context"
	"crypto/sha1" //nolint:gosec // used for generating a random version string
	_ "embed"
	"fmt"
	"strings"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	redis_db "github.com/redis/go-redis/v9"
)

//go:embed ubsv.lua
var ubsvLUA string

const luaScriptVersion = "v1"

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

func registerLuaIfNecessary(ctx context.Context, logger ulogger.Logger, rdb *redis_db.Client, version ...string) error {
	v := luaScriptVersion
	if len(version) > 0 {
		v = version[0]
	}

	// Create the script name
	scriptName := fmt.Sprintf("ubsv_%s", v)

	// Get list of currently registered functions
	cmd := rdb.Do(ctx, "FUNCTION", "LIST")
	if err := cmd.Err(); err != nil {
		return err
	}

	funcList, err := cmd.Slice()
	if err != nil {
		return err
	}

	// Check if our function exists
	foundFunc := false

	for _, f := range funcList {
		fMap, ok := f.(map[interface{}]interface{})
		if !ok {
			continue
		}

		name, _ := fMap["library_name"].(string)
		if name == scriptName {
			foundFunc = true
			break
		}
	}

	// Register if function doesn't exist
	if !foundFunc {
		logger.Infof("Registering new LUA script %s", scriptName)

		// Replace all instances of ___VERSION___ with the actual version
		cmd = rdb.Do(ctx, "FUNCTION", "LOAD", strings.ReplaceAll(ubsvLUA, "___VERSION___", v))
		if err := cmd.Err(); err != nil {
			return err
		}
	} else {
		logger.Infof("LUA script %s already registered", scriptName)
	}

	return nil
}

func registerLuaForTesting(rdb *redis_db.Client) (string, func() error, error) {
	ctx := context.Background()

	// Create a random sha1 hash of the current time to use as the version string
	hash := sha1.New() //nolint:gosec // used for generating a random version string
	hash.Write([]byte(time.Now().UTC().String()))
	randomVersion := fmt.Sprintf("test_%x", hash.Sum(nil))

	err := registerLuaIfNecessary(ctx, ulogger.TestLogger{}, rdb, randomVersion)
	if err != nil {
		return "", nil, err
	}

	return randomVersion, func() error {
		cmd := rdb.Do(ctx, "FUNCTION", "DELETE", fmt.Sprintf("ubsv_%s", randomVersion))
		if err := cmd.Err(); err != nil {
			return errors.NewProcessingError("Failed to delete function", err)
		}

		return nil
	}, nil
}

func parseLuaReturnValue(returnValue string) (luaReturnMessage, error) {
	rets := luaReturnMessage{}

	r := strings.Split(returnValue, ":")

	if len(r) > 0 {
		rets.returnValue = luaReturnValue(r[0])
	}

	if len(r) > 1 {
		if len(r[1]) == 64 {
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
