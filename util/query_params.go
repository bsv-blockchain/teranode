package util

import (
	"net/url"
	"strconv"
	"time"
)

// GetQueryParam extracts a string query parameter from the URL.
// If the parameter is not present or is empty, it returns defValue.
func GetQueryParam(u *url.URL, key string, defValue string) string {
	s := u.Query().Get(key)
	if s == "" {
		return defValue
	}

	return s
}

// GetQueryParamBool extracts a boolean query parameter from the URL.
// Returns true only if the parameter value is exactly "true" (case-sensitive).
// If the parameter is not present, empty, or any other value, it returns defValue.
func GetQueryParamBool(u *url.URL, key string, defValue bool) bool {
	s := u.Query().Get(key)
	if s == "" {
		return defValue
	}

	if s == "true" {
		return true
	}

	return false
}

// GetQueryParamInt extracts an integer query parameter from the URL.
// If the parameter is not present, empty, or cannot be parsed as an integer,
// it returns defValue.
func GetQueryParamInt(u *url.URL, key string, defValue int) int {
	s := GetQueryParam(u, key, "")
	if s == "" {
		return defValue
	}

	i, err := strconv.Atoi(s)
	if err != nil {
		return defValue
	}

	return i
}

// GetQueryParamDuration extracts a time.Duration query parameter from the URL.
// The parameter value is parsed using time.ParseDuration.
// If the parameter is not present, empty, or cannot be parsed as a duration,
// it returns defValue.
func GetQueryParamDuration(u *url.URL, key string, defValue time.Duration) time.Duration {
	s := GetQueryParam(u, key, "")
	if s == "" {
		return defValue
	}

	d, err := time.ParseDuration(s)
	if err != nil {
		return defValue
	}

	return d
}
