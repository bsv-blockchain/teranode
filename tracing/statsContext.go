package tracing

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

func NewStatFromContextWithCancel(ctx context.Context, key string, defaultParent *gocore.Stat, options ...bool) (time.Time, *gocore.Stat, context.Context, context.CancelFunc) {
	t, s, ctx := NewStatFromContext(ctx, key, defaultParent, options...)
	ctx, cancel := context.WithCancel(ctx)
	return t, s, ctx, cancel
}

func NewStatFromContext(ctx context.Context, key string, defaultParent *gocore.Stat, options ...bool) (time.Time, *gocore.Stat, context.Context) {
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
	return gocore.CurrentTime(), stat, context.WithValue(ctx, statsKey{}, stat)
}

func StartStatFromContext(ctx context.Context, key string, options ...bool) (time.Time, *gocore.Stat, context.Context) {
	return NewStatFromContext(ctx, key, defaultStat, options...)
}

func CopyStatFromContext(ctxFrom context.Context, ctxTo context.Context) context.Context {
	stat, ok := ctxFrom.Value(statsKey{}).(*gocore.Stat)
	if !ok {
		// panic("No stat in context")
		stat = defaultStat
	}
	return context.WithValue(ctxTo, statsKey{}, stat)
}

func TimeSince(start time.Time) float64 {
	return float64(time.Since(start))
}

func Stat(stat *gocore.Stat, func1 func() interface{}) interface{} {
	start := gocore.CurrentTime()
	defer func() {
		stat.AddTime(start)
	}()
	return func1()
}
