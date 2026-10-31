package guard

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	timeout = 2 * time.Second
	tick    = time.Millisecond
)

type recording struct {
	mu        sync.Mutex
	component string
	value     any
}

func (r *recording) Panicked(component string, value any) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.component = component
	r.value = value
}

func TestRecoverReportsComponentAndValue(t *testing.T) {
	rec := &recording{}
	done := make(chan struct{})

	go func() {
		defer Recover(rec, "worker")
		defer close(done)

		panic("boom")
	}()

	<-done

	require.Eventually(t, func() bool {
		rec.mu.Lock()
		defer rec.mu.Unlock()

		return rec.component == "worker" && rec.value == "boom"
	}, timeout, tick)
}

func TestRecoverIgnoresCleanReturn(t *testing.T) {
	called := false

	func() {
		defer Recover(RecorderFunc(func(string, any) { called = true }), "clean")
	}()

	require.False(t, called)
}

func TestRecorderFuncReceivesPanic(t *testing.T) {
	var got string

	func() {
		defer Recover(RecorderFunc(func(component string, value any) { got = component + ":" + value.(string) }), "fn")

		panic("x")
	}()

	require.Equal(t, "fn:x", got)
}
