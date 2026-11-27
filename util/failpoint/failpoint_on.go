//go:build failpoints

package failpoint

import (
	"fmt"
	"os"
	"strings"
	"sync"
)

var (
	mu       sync.RWMutex
	loadOnce sync.Once
	armed    = map[string]struct{}{}
)

func load() {
	loadOnce.Do(func() {
		mu.Lock()
		defer mu.Unlock()

		for _, name := range strings.Split(os.Getenv(EnvVar), ",") {
			if name = strings.TrimSpace(name); name != "" {
				armed[name] = struct{}{}
			}
		}
	})
}

// Enable arms the named failpoint in-process.
func Enable(name string) {
	load()
	mu.Lock()
	armed[name] = struct{}{}
	mu.Unlock()
}

// Disable disarms the named failpoint in-process.
func Disable(name string) {
	load()
	mu.Lock()
	delete(armed, name)
	mu.Unlock()
}

// Inject terminates the process with exit code 1 if the named failpoint is armed.
func Inject(name string) {
	load()
	mu.RLock()
	_, ok := armed[name]
	mu.RUnlock()

	if !ok {
		return
	}

	_, _ = fmt.Fprintf(os.Stderr, "%s%s\n", HitMarker, name)

	os.Exit(1)
}
