package util

import (
	"net/url"
	"strconv"
	"time"
)

func GetQueryParam(u *url.URL, key string, defValue string) string {
	s := u.Query().Get(key)
	if s == "" {
		return defValue
	}

	return s
}

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
