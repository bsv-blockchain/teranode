//go:build !race

package netsync

// raceDetectorEnabled is false in an ordinary build. See its counterpart in
// zz_race_on_test.go for why a measurement may need to know.
const raceDetectorEnabled = false
