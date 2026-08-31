// Package fdlimit reports how many open file descriptors the process can spare
// for a bounded workload, so a caller sizing a concurrency limit can fit under
// what the operating system actually allows.
package fdlimit

// Headroom is the number of descriptors held back for everything the caller's
// budget does not cover — sockets, connection pools, log files, and the Go
// runtime's own handles. Without it a caller could size itself to the whole
// limit and starve them, hitting EMFILE on a descriptor it never counted.
//
// Where the limit is smaller than twice this, Budget reserves half of it
// instead: a fixed reserve there would leave nothing to work with.
const Headroom uint64 = 512

// get is indirected through a variable so tests can pose synthetic limits.
// Driving the real process limit is not an option: the cases worth testing
// involve lowering it, which an unprivileged process cannot undo, so it would
// leak into every later test in the binary.
var get = getRlimit

// Budget reports the descriptors available for a bounded workload: the soft
// open-file limit, less a reserve of Headroom — or of half the limit, where
// that is the smaller of the two.
//
// It deliberately does not raise the limit, for two reasons. The Go runtime
// already raises RLIMIT_NOFILE to the hard limit during startup, before main
// runs (see syscall/rlimit.go), including the Darwin cap at
// kern.maxfilesperproc — so by the time any application code runs there is at
// most one descriptor left to gain. And the hard limit is the operator's to
// set: it is where ulimit -Hn, systemd LimitNOFILE and container runtimes
// express policy, and this package's job is to respect that, not to negotiate
// with it.
//
// It also does not fail on a limit that is too small, because the caller is
// expected to bound its own concurrency rather than refuse to run. An error
// means the limit could not be read at all (a platform with no RLIMIT_NOFILE);
// the budget is then 0, which the caller should read as "unknown, carry on".
func Budget() (uint64, error) {
	soft, err := get()
	if err != nil {
		return 0, err
	}

	// Never reserve the whole limit. On a host whose limit is at or below
	// Headroom a fixed reserve leaves a budget of zero, which a caller reads as
	// "no limit worth fitting under" — no protection at all on the host that can
	// least afford it. Reserving half keeps a small limit honest and still
	// usable. Subtracting cannot underflow: the reserve is at most soft/2.
	return soft - min(Headroom, soft/2), nil
}
