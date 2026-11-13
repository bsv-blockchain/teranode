//go:build !linux

package txmetacache

// adviseHugePages does nothing where transparent huge pages are not available.
func adviseHugePages(_ []byte) {
	// Transparent huge pages are a Linux feature; the cache works without them.
}
