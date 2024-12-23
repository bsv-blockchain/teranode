package daemon

import (
	"github.com/ordishs/gocore"
	"gopkg.in/DataDog/dd-trace-go.v1/profiler"
)

func datadogProfiler() func() {
	if err := profiler.Start(
		profiler.WithService("teranode"),
		profiler.WithEnv(gocore.Config().GetContext()),
		profiler.WithVersion("0.1.0"),
		// profiler.WithTags("context:"+gocore.Config().GetContext()),
		profiler.WithProfileTypes(
			profiler.CPUProfile,
			profiler.HeapProfile,
			profiler.BlockProfile,
			profiler.MutexProfile,
			profiler.GoroutineProfile,
		),
	); err != nil {
		panic(err)
	}

	// return defer function
	return func() {
		profiler.Stop()
	}
}
