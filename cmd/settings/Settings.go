package settings

import (
	"fmt"

	"github.com/ordishs/gocore"
)

func CmdSettings(version string, commit string) {
	stats := gocore.Config().Stats()
	fmt.Printf("STATS\n%s\nVERSION\n-------\n%s (%s)\n\n", stats, version, commit)
}
