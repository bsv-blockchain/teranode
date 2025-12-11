package logs

import (
	"sort"
	"strings"
	"sync"
	"time"
)

// ErrorStats tracks error/warning counts per service over a time window
type ErrorStats struct {
	mu      sync.RWMutex
	window  time.Duration
	entries []errorEntry
}

// errorEntry represents a single error/warning event
type errorEntry struct {
	Service   string
	Level     LogLevel
	Timestamp time.Time
}

// ServiceErrorCount holds error/warning counts for a service
type ServiceErrorCount struct {
	Service  string
	Errors   int
	Warnings int
}

// NewErrorStats creates a new error stats tracker with the given window duration
func NewErrorStats(window time.Duration) *ErrorStats {
	return &ErrorStats{
		window:  window,
		entries: make([]errorEntry, 0),
	}
}

// Track records an error or warning from a log entry
func (es *ErrorStats) Track(entry LogEntry) {
	if entry.Level != LevelError && entry.Level != LevelWarn && entry.Level != LevelFatal {
		return
	}

	es.mu.Lock()
	defer es.mu.Unlock()

	es.entries = append(es.entries, errorEntry{
		Service:   strings.ToLower(entry.Service),
		Level:     entry.Level,
		Timestamp: entry.Timestamp,
	})
}

// TrackMany records multiple log entries
func (es *ErrorStats) TrackMany(entries []LogEntry) {
	es.mu.Lock()
	defer es.mu.Unlock()

	for _, entry := range entries {
		if entry.Level != LevelError && entry.Level != LevelWarn && entry.Level != LevelFatal {
			continue
		}
		es.entries = append(es.entries, errorEntry{
			Service:   strings.ToLower(entry.Service),
			Level:     entry.Level,
			Timestamp: entry.Timestamp,
		})
	}
}

// prune removes entries older than the window (must be called with lock held)
func (es *ErrorStats) prune() {
	cutoff := time.Now().Add(-es.window)
	newEntries := make([]errorEntry, 0, len(es.entries))
	for _, e := range es.entries {
		if e.Timestamp.After(cutoff) {
			newEntries = append(newEntries, e)
		}
	}
	es.entries = newEntries
}

// Summary returns error/warning counts per service, sorted by total count (descending)
func (es *ErrorStats) Summary() []ServiceErrorCount {
	es.mu.Lock()
	es.prune()
	es.mu.Unlock()

	es.mu.RLock()
	defer es.mu.RUnlock()

	counts := make(map[string]*ServiceErrorCount)
	for _, e := range es.entries {
		service := e.Service
		if service == "" {
			service = "unknown"
		}
		if counts[service] == nil {
			counts[service] = &ServiceErrorCount{Service: service}
		}
		if e.Level == LevelError || e.Level == LevelFatal {
			counts[service].Errors++
		} else if e.Level == LevelWarn {
			counts[service].Warnings++
		}
	}

	result := make([]ServiceErrorCount, 0, len(counts))
	for _, c := range counts {
		result = append(result, *c)
	}

	// Sort by total count (errors + warnings) descending
	sort.Slice(result, func(i, j int) bool {
		totalI := result[i].Errors + result[i].Warnings
		totalJ := result[j].Errors + result[j].Warnings
		if totalI != totalJ {
			return totalI > totalJ
		}
		return result[i].Service < result[j].Service
	})

	return result
}

// TotalErrors returns the total error count across all services
func (es *ErrorStats) TotalErrors() int {
	es.mu.Lock()
	es.prune()
	es.mu.Unlock()

	es.mu.RLock()
	defer es.mu.RUnlock()

	count := 0
	for _, e := range es.entries {
		if e.Level == LevelError || e.Level == LevelFatal {
			count++
		}
	}
	return count
}

// TotalWarnings returns the total warning count across all services
func (es *ErrorStats) TotalWarnings() int {
	es.mu.Lock()
	es.prune()
	es.mu.Unlock()

	es.mu.RLock()
	defer es.mu.RUnlock()

	count := 0
	for _, e := range es.entries {
		if e.Level == LevelWarn {
			count++
		}
	}
	return count
}

// RateTracker tracks log rate over time using a sliding window
type RateTracker struct {
	mu       sync.RWMutex
	buckets  []int     // count per second for last N seconds
	width    int       // number of buckets
	current  int       // current bucket index
	lastTick time.Time // last time we advanced
}

// NewRateTracker creates a new rate tracker with the given width (seconds of history)
func NewRateTracker(width int) *RateTracker {
	return &RateTracker{
		buckets:  make([]int, width),
		width:    width,
		current:  0,
		lastTick: time.Now(),
	}
}

// Add adds log entries to the current bucket
func (rt *RateTracker) Add(count int) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.buckets[rt.current] += count
}

// Tick advances time and moves to next bucket if a second has passed
func (rt *RateTracker) Tick() {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(rt.lastTick)

	// Advance buckets for each second that has passed
	for elapsed >= time.Second {
		rt.current = (rt.current + 1) % rt.width
		rt.buckets[rt.current] = 0
		elapsed -= time.Second
		rt.lastTick = rt.lastTick.Add(time.Second)
	}
}

// Sparkline returns an ASCII sparkline representation of the rate history
func (rt *RateTracker) Sparkline() string {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	// Sparkline characters from lowest to highest
	chars := []rune{'▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'}

	// Find max value for scaling
	maxVal := 1 // avoid division by zero
	for _, v := range rt.buckets {
		if v > maxVal {
			maxVal = v
		}
	}

	// Build sparkline starting from oldest bucket
	var sb strings.Builder
	for i := 0; i < rt.width; i++ {
		// Start from the bucket after current (oldest) and go forward
		idx := (rt.current + 1 + i) % rt.width
		val := rt.buckets[idx]

		// Scale to 0-7 range
		level := (val * 7) / maxVal
		if level > 7 {
			level = 7
		}
		sb.WriteRune(chars[level])
	}

	return sb.String()
}

// CurrentRate returns the rate in the current (most recent) bucket
func (rt *RateTracker) CurrentRate() int {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	// Return the previous bucket since current is still accumulating
	prev := (rt.current - 1 + rt.width) % rt.width
	return rt.buckets[prev]
}

// MaxRate returns the maximum rate in the window
func (rt *RateTracker) MaxRate() int {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	maxVal := 0
	for _, v := range rt.buckets {
		if v > maxVal {
			maxVal = v
		}
	}
	return maxVal
}

// AvgRate returns the average rate over the window
func (rt *RateTracker) AvgRate() float64 {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	total := 0
	for _, v := range rt.buckets {
		total += v
	}
	return float64(total) / float64(rt.width)
}
