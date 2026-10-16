//go:build amd64

package txmetacache

// storeFence and loadFence are no-ops on amd64. Under TSO stores are not
// reordered with earlier stores and loads are not reordered with earlier
// loads, and the writer's tail publish (atomic.Uint64.Store, an XCHG) is
// already a full fence. The Go compiler does not move memory accesses across
// the atomic operations around these calls.
func storeFence() {
	// Nothing to do: see above.
}

func loadFence() {
	// Nothing to do: see above.
}
