package redis

import "errors"

var (
	errWatchFailed = errors.New("WATCH failed")
	errNotFound    = errors.New("NOT_FOUND")
	errSpent       = errors.New("SPENT")
	errLocked      = errors.New("LOCKED")
)
