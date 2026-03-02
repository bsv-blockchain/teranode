package util

import "strconv"

// FormatComma produces a string with commas as thousands separators.
func FormatComma(n int64) string {
	s := strconv.FormatInt(n, 10)
	if n < 0 {
		return "-" + insertCommas(s[1:])
	}
	return insertCommas(s)
}

func insertCommas(s string) string {
	if len(s) <= 3 {
		return s
	}
	return insertCommas(s[:len(s)-3]) + "," + s[len(s)-3:]
}
