package p2p

import (
	"context"
	"sync"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/internal/banlist"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// Type aliases for backward compatibility with existing code (legacy service, tests, etc.).
type BanListI = banlist.Interface
type BanInfo = banlist.BanInfo
type BanEvent = banlist.BanEvent
type BanList = banlist.BanList

var (
	banListInstance *BanList
	banListOnce     sync.Once
)

// GetBanList retrieves or creates a singleton instance of BanList.
// The singleton ensures that the P2P service and legacy service share
// the same in-process ban list instance.
func GetBanList(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings) (*BanList, chan BanEvent, error) {
	var eventChan chan BanEvent

	banListOnce.Do(func() {
		var err error

		banListInstance, err = banlist.NewFromSettings(logger, tSettings)
		if err != nil {
			logger.Errorf("Failed to create BanList: %v", err)

			banListInstance = nil

			return
		}

		err = banListInstance.Init(ctx)
		if err != nil {
			logger.Errorf("Failed to initialise BanList: %v", err)

			banListInstance = nil
		}
	})

	if banListInstance == nil {
		return nil, nil, errors.New(errors.ERR_ERROR, "Failed to initialise BanList")
	}

	eventChan = banListInstance.Subscribe()

	return banListInstance, eventChan, nil
}

// ResetBanListSingleton resets the singleton for testing purposes.
func ResetBanListSingleton() {
	banListInstance = nil
	banListOnce = sync.Once{}
}
