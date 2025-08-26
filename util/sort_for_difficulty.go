package util

// SortForDifficultyAdjustment sorts a slice of exactly 3 elements by their timestamp
// in ascending order using a sorting network implementation. This is optimized for
// Bitcoin's difficulty adjustment algorithm which requires sorting exactly 3 block
// headers by timestamp to find the median.
//
// The function uses a sorting network with 3 comparisons, which is optimal for
// sorting 3 elements. The sorting network approach provides deterministic performance
// with a fixed number of comparisons regardless of input order.
//
// Parameters:
//   - s: A slice containing exactly 3 elements that implement GetTime() uint32
//
// Note: The function assumes the slice has exactly 3 elements. Using with a different
// number of elements will cause index out of bounds panics.
func SortForDifficultyAdjustment[T interface{ GetTime() uint32 }](s []T) {
	// Sorting network implementation
	if s[0].GetTime() > s[2].GetTime() {
		s[0], s[2] = s[2], s[0]
	}

	if s[0].GetTime() > s[1].GetTime() {
		s[0], s[1] = s[1], s[0]
	}

	if s[1].GetTime() > s[2].GetTime() {
		s[1], s[2] = s[2], s[1]
	}
}
