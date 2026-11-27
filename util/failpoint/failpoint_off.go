//go:build !failpoints

package failpoint

// Inject is a no-op in builds without the "failpoints" tag.
func Inject(string) {}
