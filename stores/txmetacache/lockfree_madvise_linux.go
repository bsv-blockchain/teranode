//go:build linux

package txmetacache

import "golang.org/x/sys/unix"

// adviseHugePages asks for transparent huge pages on the cache region. Lookups
// land on random cache lines across tens to hundreds of GB; with 4 KB pages
// nearly every one also misses the TLB. Failure only costs performance.
func adviseHugePages(region []byte) {
	_ = unix.Madvise(region, unix.MADV_HUGEPAGE)
}
