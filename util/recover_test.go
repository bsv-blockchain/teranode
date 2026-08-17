package util

import (
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestRecoverToError_NoPanicLeavesTheErrorAlone(t *testing.T) {
	logger := ulogger.TestLogger{}

	run := func() (err error) {
		defer RecoverToError(logger, &err, nil, "worker %d", 7)()

		return errors.NewProcessingError("the real error")
	}

	err := run()
	require.ErrorContains(t, err, "the real error", "a normal return must not be rewritten")
}

func TestRecoverToError_PanicBecomesAnErrorWithoutLeakingThePanicValue(t *testing.T) {
	logger := ulogger.TestLogger{}

	run := func() (err error) {
		defer RecoverToError(logger, &err, nil, "worker %d", 7)()

		panic("runtime error: invalid memory address or nil pointer dereference")
	}

	err := run()
	require.Error(t, err, "the panic must be converted into an error, not escape the goroutine")
	require.ErrorContains(t, err, "worker 7", "the error must identify the fan-out site")
	require.NotContains(t, err.Error(), "nil pointer dereference", "the panic value belongs in the log only")
}

func TestRecoverToError_OnPanicRunsWithTheSameError(t *testing.T) {
	logger := ulogger.TestLogger{}

	var cleanedUpWith error

	run := func() (err error) {
		defer RecoverToError(logger, &err, func(e error) { cleanedUpWith = e }, "streamer")()

		panic("boom")
	}

	err := run()
	require.Error(t, err)
	require.Equal(t, err, cleanedUpWith, "onPanic must receive the error the caller will see, so it can fail a pipe with it")
}

func TestRecoverToError_OnPanicIsNotRunOnTheHappyPath(t *testing.T) {
	logger := ulogger.TestLogger{}

	called := false

	run := func() (err error) {
		defer RecoverToError(logger, &err, func(error) { called = true }, "streamer")()

		return nil
	}

	require.NoError(t, run())
	require.False(t, called, "onPanic is panic-only cleanup; a clean return must not trigger it")
}

// args are format parameters for format, nothing else. errors.New extracts a
// trailing error as the wrapped cause and renders it into the message via " -> ",
// which would fold internal detail into the very error this helper exists to keep
// clean.
func TestRecoverToError_TrailingErrorArgumentDoesNotLeakIntoTheError(t *testing.T) {
	logger := ulogger.TestLogger{}

	cause := errors.NewProcessingError("aerospike said no for record 0xdeadbeef")

	run := func() (err error) {
		defer RecoverToError(logger, &err, nil, "batch %d", 7, cause)()

		panic("boom")
	}

	err := run()
	require.Error(t, err)
	require.ErrorContains(t, err, "batch 7", "the format parameters must still be applied")
	require.NotContains(t, err.Error(), "aerospike said no", "a trailing error must not reach the client-facing message")
	require.NotContains(t, err.Error(), "%!", "dropping it must not mangle the format")
}

// The log args must be built on a fresh slice: append into a caller-supplied
// slice with spare capacity would write the panic value past the length it handed
// in, mutating an array the caller still owns.
func TestRecoverToError_DoesNotWriteIntoTheCallersBackingArray(t *testing.T) {
	logger := ulogger.TestLogger{}

	args := make([]any, 1, 4)
	args[0] = 7

	run := func() (err error) {
		defer RecoverToError(logger, &err, nil, "batch %d", args...)()

		panic("boom")
	}

	require.Error(t, run())
	require.Nil(t, args[:cap(args)][1], "the panic value must not be appended into the caller's backing array")
}

// The reason the helper exists: errgroup does not propagate panics from its
// children, so a bare g.Go takes the process down with it. Pin that contract so
// nobody "simplifies" the guards away on the assumption errgroup handles it.
func TestRecoverToError_ErrgroupChildPanicIsContained(t *testing.T) {
	logger := ulogger.TestLogger{}

	g := &errgroup.Group{}
	g.Go(func() (err error) {
		defer RecoverToError(logger, &err, nil, "child")()

		panic("boom")
	})

	require.Error(t, g.Wait(), "the group must report the child's panic as an error")
}
