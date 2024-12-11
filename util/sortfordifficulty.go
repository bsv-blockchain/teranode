package util

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
