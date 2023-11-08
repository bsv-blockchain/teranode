package util

import (
	"context"
	"time"

	"github.com/ordishs/gocore"
)

type statsKey struct{}

var defaultStat = gocore.NewStat("no root", true)

func ContextWithStat(ctx context.Context, stat *gocore.Stat) context.Context {
	return context.WithValue(ctx, statsKey{}, stat)
}

func NewStatFromContext(ctx context.Context, key string, defaultParent *gocore.Stat, options ...bool) (int64, *gocore.Stat, context.Context) {
	parentStat, ok := ctx.Value(statsKey{}).(*gocore.Stat)
	if !ok {
		// panic("No stat in context")
		parentStat = defaultParent
	}
	ignoreChildren := true
	if len(options) > 0 {
		ignoreChildren = options[0]
	}
	stat := parentStat.NewStat(key, ignoreChildren)
	return gocore.CurrentNanos(), stat, context.WithValue(ctx, statsKey{}, stat)
}

func StartStatFromContext(ctx context.Context, key string, options ...bool) (int64, *gocore.Stat, context.Context) {
	return NewStatFromContext(ctx, key, defaultStat, options...)
}

func TimeSince(start int64) float64 {
	return float64(time.Since(time.UnixMicro(start)))
}

func Stat(stat *gocore.Stat, func1 func() interface{}) interface{} {
	start := gocore.CurrentNanos()
	defer func() {
		stat.AddTime(gocore.CurrentNanos() - start)
	}()
	return func1()
}
