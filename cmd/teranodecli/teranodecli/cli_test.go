package teranodecli

import (
	"flag"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

func TestUint32Flag(t *testing.T) {
	t.Run("accepts valid heights", func(t *testing.T) {
		for _, in := range []struct {
			arg  string
			want uint32
		}{
			{"0", 0},
			{"300", 300},
			{"4294967295", 4294967295}, // math.MaxUint32
		} {
			fs := flag.NewFlagSet("test", flag.ContinueOnError)
			h := uint32Flag(fs, "end-height", 0, "")

			require.NoError(t, fs.Parse([]string{"--end-height", in.arg}))
			require.Equal(t, in.want, *h)
		}
	})

	t.Run("rejects out-of-range instead of truncating", func(t *testing.T) {
		// 4294967297 would silently truncate to 1 with a plain uint flag.
		for _, arg := range []string{"4294967296", "4294967297", "-1", "abc"} {
			fs := flag.NewFlagSet("test", flag.ContinueOnError)
			fs.SetOutput(&nopWriter{})
			uint32Flag(fs, "end-height", 0, "")

			require.Error(t, fs.Parse([]string{"--end-height", arg}), "arg %q must be rejected", arg)
		}
	})

	t.Run("default is used when flag absent", func(t *testing.T) {
		fs := flag.NewFlagSet("test", flag.ContinueOnError)
		h := uint32Flag(fs, "end-height", 7, "")

		require.NoError(t, fs.Parse(nil))
		require.Equal(t, uint32(7), *h)
	})
}

type nopWriter struct{}

func (*nopWriter) Write(p []byte) (int, error) { return len(p), nil }

// TestRewindblockchainRegistration guards the subcommand's discoverability and
// its flag surface. It deliberately never calls cmd.Execute: that opens real
// blockchain, UTXO and subtree stores.
func TestRewindblockchainRegistration(t *testing.T) {
	t.Run("listed in commandHelp so printUsage shows it", func(t *testing.T) {
		desc, ok := commandHelp["rewindblockchain"]
		require.True(t, ok, "rewindblockchain must be registered in commandHelp")
		require.Contains(t, desc, "DESTRUCTIVE",
			"the help line must warn that the command is destructive")
	})

	t.Run("not gated pre-parse as a dangerous command", func(t *testing.T) {
		// dangerousCommands is consumed before FlagSet.Parse, so an entry here
		// would demand typed confirmation before --help or --dry-run could run.
		require.False(t, dangerousCommands["rewindblockchain"],
			"rewindblockchain must not be in dangerousCommands: the prompt fires pre-parse")
	})

	t.Run("registers the documented flags with the documented defaults", func(t *testing.T) {
		fs := flag.NewFlagSet("rewindblockchain", flag.ContinueOnError)
		fs.SetOutput(&nopWriter{})

		f := registerRewindFlags(fs)

		require.NoError(t, fs.Parse(nil))

		opts := f.options()
		require.Equal(t, int64(-1), opts.TargetHeight,
			"default must be -1 so Rewind reads state[\"BlockAssembler\"]")
		require.False(t, opts.DryRun)
		require.False(t, opts.AssumeYes)
		require.False(t, opts.ForceNotIdle)
		require.False(t, opts.ForceDeep)
		require.False(t, opts.Verify)
		require.Zero(t, opts.Concurrency)
		require.NotNil(t, opts.Stdin, "the confirmation prompt needs stdin wired")
		require.NotNil(t, opts.Stdout, "the confirmation prompt needs stdout wired")
	})

	t.Run("parses every flag into Options", func(t *testing.T) {
		fs := flag.NewFlagSet("rewindblockchain", flag.ContinueOnError)
		fs.SetOutput(&nopWriter{})

		f := registerRewindFlags(fs)

		require.NoError(t, fs.Parse([]string{
			"--target-height", "1749330",
			"--dry-run",
			"--assume-yes",
			"--force-not-idle",
			"--force-deep",
			"--verify",
			"--concurrency", "8",
		}))

		opts := f.options()
		require.Equal(t, int64(1749330), opts.TargetHeight)
		require.True(t, opts.DryRun)
		require.True(t, opts.AssumeYes)
		require.True(t, opts.ForceNotIdle)
		require.True(t, opts.ForceDeep)
		require.True(t, opts.Verify)
		require.Equal(t, 8, opts.Concurrency)
	})

	t.Run("Execute rejects the swallowed-flag invocation", func(t *testing.T) {
		// Go's flag package stops parsing at the first non-flag argument, so
		// "--assume-yes 1749330 --force-deep" parses --assume-yes, then leaves
		// "1749330" and "--force-deep" as positionals: TargetHeight stays at
		// its -1 default and ForceDeep is never set, while AssumeYes (parsed
		// before the positional) would silently skip the confirmation prompt.
		// resolveTarget then falls back to state["BlockAssembler"] — an
		// irreversible rewind to a height the operator never asked for.
		fs := flag.NewFlagSet("rewindblockchain", flag.ContinueOnError)
		fs.SetOutput(&nopWriter{})

		f := registerRewindFlags(fs)

		require.NoError(t, fs.Parse([]string{"--assume-yes", "1749330", "--force-deep"}))

		// Establish the dangerous input: the height and the trailing flag were
		// both swallowed, and --assume-yes survived.
		require.Equal(t, int64(-1), f.options().TargetHeight,
			"the positional height must NOT have been parsed into --target-height")
		require.True(t, f.options().AssumeYes)
		require.False(t, f.options().ForceDeep,
			"--force-deep after the positional must NOT have been parsed")
		require.NotEmpty(t, fs.Args(), "the swallowed arguments remain as positionals")

		// Now drive the guard itself. The settings stub carries a deliberately
		// unsupported store scheme: if the guard is ever removed, execution falls
		// through to resolveStores, blockchain.NewStore rejects the scheme, and
		// this subtest fails on the assertion below rather than panicking or
		// touching a real store. (A nil *settings.Settings would panic in
		// NewStore on storeURL.Scheme and take the sibling subtests with it; a
		// real one would open actual stores.)
		stub := &settings.Settings{}
		stub.BlockChain.StoreURL = &url.URL{Scheme: "guard-must-reject-before-this"}

		err := rewindExecute(ulogger.TestLogger{}, stub, f)(fs.Args())
		require.Error(t, err, "positional arguments must be rejected before any store is opened")
		require.Contains(t, err.Error(), "takes no positional arguments",
			"the guard must reject the invocation; reaching the store layer means it is gone")
		require.Contains(t, err.Error(), "--target-height",
			"the error must point the operator at the flag they meant to use")
	})

	t.Run("Execute accepts an invocation with no positional arguments", func(t *testing.T) {
		// The guard must not reject a well-formed invocation. Verified via
		// FlagSet.Args() being empty after a clean parse; Execute itself is not
		// called here because it would proceed to open real stores.
		fs := flag.NewFlagSet("rewindblockchain", flag.ContinueOnError)
		fs.SetOutput(&nopWriter{})

		registerRewindFlags(fs)

		require.NoError(t, fs.Parse([]string{"--target-height", "1749330", "--dry-run"}))
		require.Empty(t, fs.Args(), "a well-formed invocation leaves no positionals for the guard to reject")
	})
}
