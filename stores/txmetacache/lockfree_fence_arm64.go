//go:build arm64

package txmetacache

// storeFence orders every earlier store (the lane tail publish included)
// before every later store. A writer issues it right after publishing the
// lane's new tail, so none of the record stores that follow can become visible
// to a reader before the tail that lets the reader detect the overwrite. The
// tail store (STLR) is only a release, which orders what came before it, not
// what follows.
//
//go:noescape
func storeFence()

// loadFence orders every earlier load before every later load. A reader issues
// it after copying a record and before re-reading the lane's tail, so the copy
// cannot be performed after the check that validates it.
//
//go:noescape
func loadFence()
