package settings

import (
	"fmt"

	"github.com/ordishs/gocore"
)

func Start() {
	stats := gocore.Config().Stats()
	fmt.Printf("STATS\n%s\nVERSION\n-------\n\n", stats)
}
