package util

import (
	"context"

	"github.com/ordishs/gocore"
)

type statsKey struct{}

func ContextWithStat(ctx context.Context, stat *gocore.Stat) context.Context {
	return context.WithValue(ctx, statsKey{}, stat)
}

func StatFromContext(ctx context.Context, defaultStat *gocore.Stat) *gocore.Stat {
	stat, ok := ctx.Value(statsKey{}).(*gocore.Stat)
	if !ok {
		// panic("No stat in context")
		return defaultStat
	}
	return stat
}
