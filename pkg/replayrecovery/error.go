package replayrecovery

import "fmt"

// Native sentinels retain identity; project error codes deliberately compare
// whole classes of errors, which would conflate pending and failed recovery.
type sentinelError string

func (e sentinelError) Error() string { return string(e) }

func failure(format string, args ...any) error {
	//nolint:forbidigo // Standard wrapping preserves recovery sentinels and context cancellation across CLI boundaries.
	return fmt.Errorf(format, args...)
}

func combineErrors(primary, secondary error) error {
	if secondary == nil {
		return primary
	}
	if primary == nil {
		return secondary
	}
	// A durability/close failure takes precedence over a merely pending audit.
	return failure("%w (earlier result: %v)", secondary, primary)
}
