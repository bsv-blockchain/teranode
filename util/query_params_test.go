package util

import (
	"net/url"
	"testing"
	"time"
)

func TestGetQueryParam(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		key      string
		defValue string
		expected string
	}{
		{
			name:     "parameter exists with value",
			url:      "http://example.com?name=value",
			key:      "name",
			defValue: "default",
			expected: "value",
		},
		{
			name:     "parameter exists but empty",
			url:      "http://example.com?name=",
			key:      "name",
			defValue: "default",
			expected: "default",
		},
		{
			name:     "parameter doesn't exist",
			url:      "http://example.com",
			key:      "name",
			defValue: "default",
			expected: "default",
		},
		{
			name:     "multiple values for same key returns first",
			url:      "http://example.com?name=first&name=second",
			key:      "name",
			defValue: "default",
			expected: "first",
		},
		{
			name:     "empty default value",
			url:      "http://example.com",
			key:      "name",
			defValue: "",
			expected: "",
		},
		{
			name:     "parameter with special characters",
			url:      "http://example.com?name=hello%20world",
			key:      "name",
			defValue: "default",
			expected: "hello world",
		},
		{
			name:     "parameter with spaces in URL encoded form",
			url:      "http://example.com?message=hello+world",
			key:      "message",
			defValue: "default",
			expected: "hello world",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, err := url.Parse(tt.url)
			if err != nil {
				t.Fatalf("Failed to parse URL: %v", err)
			}

			result := GetQueryParam(u, tt.key, tt.defValue)
			if result != tt.expected {
				t.Errorf("GetQueryParam() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestGetQueryParamBool(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		key      string
		defValue bool
		expected bool
	}{
		{
			name:     "parameter is true",
			url:      "http://example.com?enabled=true",
			key:      "enabled",
			defValue: false,
			expected: true,
		},
		{
			name:     "parameter is false",
			url:      "http://example.com?enabled=false",
			key:      "enabled",
			defValue: true,
			expected: false,
		},
		{
			name:     "parameter doesn't exist returns default true",
			url:      "http://example.com",
			key:      "enabled",
			defValue: true,
			expected: true,
		},
		{
			name:     "parameter doesn't exist returns default false",
			url:      "http://example.com",
			key:      "enabled",
			defValue: false,
			expected: false,
		},
		{
			name:     "parameter exists but empty",
			url:      "http://example.com?enabled=",
			key:      "enabled",
			defValue: true,
			expected: true,
		},
		{
			name:     "parameter is True (case sensitive)",
			url:      "http://example.com?enabled=True",
			key:      "enabled",
			defValue: true,
			expected: false,
		},
		{
			name:     "parameter is TRUE (case sensitive)",
			url:      "http://example.com?enabled=TRUE",
			key:      "enabled",
			defValue: true,
			expected: false,
		},
		{
			name:     "parameter is 1",
			url:      "http://example.com?enabled=1",
			key:      "enabled",
			defValue: true,
			expected: false,
		},
		{
			name:     "parameter is 0",
			url:      "http://example.com?enabled=0",
			key:      "enabled",
			defValue: true,
			expected: false,
		},
		{
			name:     "parameter is random string",
			url:      "http://example.com?enabled=randomvalue",
			key:      "enabled",
			defValue: true,
			expected: false,
		},
		{
			name:     "multiple true values returns first",
			url:      "http://example.com?enabled=true&enabled=false",
			key:      "enabled",
			defValue: false,
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, err := url.Parse(tt.url)
			if err != nil {
				t.Fatalf("Failed to parse URL: %v", err)
			}

			result := GetQueryParamBool(u, tt.key, tt.defValue)
			if result != tt.expected {
				t.Errorf("GetQueryParamBool() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestGetQueryParamInt(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		key      string
		defValue int
		expected int
	}{
		{
			name:     "parameter is valid positive integer",
			url:      "http://example.com?count=42",
			key:      "count",
			defValue: 0,
			expected: 42,
		},
		{
			name:     "parameter is valid negative integer",
			url:      "http://example.com?count=-10",
			key:      "count",
			defValue: 0,
			expected: -10,
		},
		{
			name:     "parameter is zero",
			url:      "http://example.com?count=0",
			key:      "count",
			defValue: 5,
			expected: 0,
		},
		{
			name:     "parameter doesn't exist",
			url:      "http://example.com",
			key:      "count",
			defValue: 100,
			expected: 100,
		},
		{
			name:     "parameter exists but empty",
			url:      "http://example.com?count=",
			key:      "count",
			defValue: 50,
			expected: 50,
		},
		{
			name:     "parameter is not a valid integer",
			url:      "http://example.com?count=abc",
			key:      "count",
			defValue: 25,
			expected: 25,
		},
		{
			name:     "parameter is decimal number",
			url:      "http://example.com?count=3.14",
			key:      "count",
			defValue: 1,
			expected: 1,
		},
		{
			name:     "parameter has leading/trailing spaces",
			url:      "http://example.com?count=%20123%20",
			key:      "count",
			defValue: 0,
			expected: 0, // strconv.Atoi doesn't trim spaces, so this should fail
		},
		{
			name:     "parameter is very large integer",
			url:      "http://example.com?count=2147483647",
			key:      "count",
			defValue: 0,
			expected: 2147483647,
		},
		{
			name:     "parameter is too large for int",
			url:      "http://example.com?count=9223372036854775808",
			key:      "count",
			defValue: 42,
			expected: 42, // Should overflow and return default
		},
		{
			name:     "multiple values returns first valid",
			url:      "http://example.com?count=123&count=456",
			key:      "count",
			defValue: 0,
			expected: 123,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, err := url.Parse(tt.url)
			if err != nil {
				t.Fatalf("Failed to parse URL: %v", err)
			}

			result := GetQueryParamInt(u, tt.key, tt.defValue)
			if result != tt.expected {
				t.Errorf("GetQueryParamInt() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestGetQueryParamDuration(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		key      string
		defValue time.Duration
		expected time.Duration
	}{
		{
			name:     "parameter is valid duration in seconds",
			url:      "http://example.com?timeout=30s",
			key:      "timeout",
			defValue: time.Minute,
			expected: 30 * time.Second,
		},
		{
			name:     "parameter is valid duration in minutes",
			url:      "http://example.com?timeout=5m",
			key:      "timeout",
			defValue: time.Second,
			expected: 5 * time.Minute,
		},
		{
			name:     "parameter is valid duration in hours",
			url:      "http://example.com?timeout=2h",
			key:      "timeout",
			defValue: time.Second,
			expected: 2 * time.Hour,
		},
		{
			name:     "parameter is complex duration",
			url:      "http://example.com?timeout=1h30m15s",
			key:      "timeout",
			defValue: time.Second,
			expected: time.Hour + 30*time.Minute + 15*time.Second,
		},
		{
			name:     "parameter is valid duration in nanoseconds",
			url:      "http://example.com?timeout=500ns",
			key:      "timeout",
			defValue: time.Second,
			expected: 500 * time.Nanosecond,
		},
		{
			name:     "parameter is valid duration in microseconds",
			url:      "http://example.com?timeout=100us",
			key:      "timeout",
			defValue: time.Second,
			expected: 100 * time.Microsecond,
		},
		{
			name:     "parameter is valid duration in milliseconds",
			url:      "http://example.com?timeout=250ms",
			key:      "timeout",
			defValue: time.Second,
			expected: 250 * time.Millisecond,
		},
		{
			name:     "parameter doesn't exist",
			url:      "http://example.com",
			key:      "timeout",
			defValue: 10 * time.Second,
			expected: 10 * time.Second,
		},
		{
			name:     "parameter exists but empty",
			url:      "http://example.com?timeout=",
			key:      "timeout",
			defValue: 5 * time.Minute,
			expected: 5 * time.Minute,
		},
		{
			name:     "parameter is not a valid duration",
			url:      "http://example.com?timeout=invalid",
			key:      "timeout",
			defValue: time.Hour,
			expected: time.Hour,
		},
		{
			name:     "parameter is number without unit",
			url:      "http://example.com?timeout=30",
			key:      "timeout",
			defValue: time.Minute,
			expected: time.Minute,
		},
		{
			name:     "parameter is zero duration",
			url:      "http://example.com?timeout=0s",
			key:      "timeout",
			defValue: time.Minute,
			expected: 0,
		},
		{
			name:     "parameter is negative duration",
			url:      "http://example.com?timeout=-30s",
			key:      "timeout",
			defValue: time.Minute,
			expected: -30 * time.Second,
		},
		{
			name:     "multiple values returns first valid",
			url:      "http://example.com?timeout=1m&timeout=2m",
			key:      "timeout",
			defValue: time.Second,
			expected: time.Minute,
		},
		{
			name:     "parameter with fractional seconds",
			url:      "http://example.com?timeout=1.5s",
			key:      "timeout",
			defValue: time.Second,
			expected: time.Second + 500*time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, err := url.Parse(tt.url)
			if err != nil {
				t.Fatalf("Failed to parse URL: %v", err)
			}

			result := GetQueryParamDuration(u, tt.key, tt.defValue)
			if result != tt.expected {
				t.Errorf("GetQueryParamDuration() = %v, want %v", result, tt.expected)
			}
		})
	}
}
