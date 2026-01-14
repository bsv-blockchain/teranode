package profiling

import (
	"fmt"
	"net/http"
	"strconv"
)

// MemoryProfileHandler is an HTTP handler that returns complete memory analysis.
func MemoryProfileHandler(w http.ResponseWriter, r *http.Request) {
	breakdown, err := GetCompleteMemoryProfile()
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get memory profile: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")

	// Parse query parameter for number of top regions
	topN := 20
	if topStr := r.URL.Query().Get("top"); topStr != "" {
		if n, err := strconv.Atoi(topStr); err == nil && n > 0 {
			topN = n
		}
	}

	// Write formatted breakdown
	_, _ = fmt.Fprint(w, breakdown.FormatBreakdown())

	// Write top regions
	_, _ = fmt.Fprint(w, breakdown.FormatTopRegions(topN))
}
