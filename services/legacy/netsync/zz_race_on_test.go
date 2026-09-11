//go:build race

package netsync

// raceDetectorEnabled is what lets a test opt out when the race detector would
// invalidate it. The detector allocates shadow memory for every access and
// changes allocation behaviour throughout, so a test asserting a proportion of
// the heap cannot hold under it. Correctness tests must never use this; it is
// for measurements only.
const raceDetectorEnabled = true
