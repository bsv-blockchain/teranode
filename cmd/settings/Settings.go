package settings

import (
	"encoding/json"
	"fmt"

	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/ordishs/gocore"
)

func CmdSettings(logger ulogger.Logger, tSettings *settings.Settings, version string, commit string) {
	stats := gocore.Config().Stats()
	fmt.Printf("STATS\n%s\nVERSION\n-------\n%s (%s)\n\n", stats, version, commit)

	settingsJSON, err := json.MarshalIndent(tSettings, "", "  ")
	if err != nil {
		logger.Errorf("Failed to marshal settings: %v", err)
		return
	} else {
		fmt.Printf("SETTINGS JSON\n-------------\n%s\n\n", string(settingsJSON))
	}
}
