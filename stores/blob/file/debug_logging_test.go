package file

import (
	"testing"

	"github.com/bsv-blockchain/teranode/util/debugflags"
	"github.com/bsv-blockchain/teranode/util/test/mocklogger"
)

func TestFileDebugfRespectsFlags(t *testing.T) {
	logger := mocklogger.NewTestLogger()
	s := &File{logger: logger}

	debugflags.Init(debugflags.Flags{})
	t.Cleanup(func() { debugflags.Init(debugflags.Flags{}) })

	s.debugf("should be silent")
	logger.AssertNumberOfCalls(t, "Debugf", 0)

	logger.Reset()
	debugflags.Init(debugflags.Flags{File: true})

	s.debugf("should log")
	logger.AssertNumberOfCalls(t, "Debugf", 1)
}
