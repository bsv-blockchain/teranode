//go:build unix

package fdlimit

import "syscall"

// getRlimit reads the process's soft open-file limit.
//
// The kernel's rlim_t is unsigned on Linux, Darwin, Solaris, NetBSD, OpenBSD,
// AIX and illumos, but signed on FreeBSD and DragonFly, and Go's syscall.Rlimit
// mirrors that — both of which the unix build tag covers. Widening either kind
// to uint64 is the only conversion this package needs, and it is confined here.
func getRlimit() (soft uint64, err error) {
	var lim syscall.Rlimit
	if err = syscall.Getrlimit(syscall.RLIMIT_NOFILE, &lim); err != nil {
		return 0, err
	}

	//nolint:gosec,unconvert // rlim_t signedness varies by platform; see above
	return uint64(lim.Cur), nil
}
