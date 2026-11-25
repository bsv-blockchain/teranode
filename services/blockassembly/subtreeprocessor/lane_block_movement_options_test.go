package subtreeprocessor

// laneMoveBenchOptions are the processor options the block-movement benchmarks run with at
// shadow: a keeper for the lane bitmaps it builds as it seals subtrees.
func laneMoveBenchOptions(shadow bool) []Options {
	if !shadow {
		return nil
	}

	return []Options{WithLaneBitmapsKeeper(newRecordingLaneKeeper())}
}
