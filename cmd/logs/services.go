package logs

import (
	"sort"
	"strings"
	"sync"
)

// ServiceTracker tracks discovered services from log entries
type ServiceTracker struct {
	services map[string]int // service name -> count of log entries
	mu       sync.RWMutex
}

// NewServiceTracker creates a new service tracker
func NewServiceTracker() *ServiceTracker {
	return &ServiceTracker{
		services: make(map[string]int),
	}
}

// Track adds a service to the tracker (if not empty)
func (st *ServiceTracker) Track(service string) {
	service = strings.ToLower(strings.TrimSpace(service))
	if service == "" {
		return
	}

	st.mu.Lock()
	defer st.mu.Unlock()
	st.services[service]++
}

// TrackMany tracks multiple entries at once
func (st *ServiceTracker) TrackMany(entries []LogEntry) {
	st.mu.Lock()
	defer st.mu.Unlock()

	for _, entry := range entries {
		service := strings.ToLower(strings.TrimSpace(entry.Service))
		if service != "" {
			st.services[service]++
		}
	}
}

// Services returns all discovered services sorted by frequency (most common first)
func (st *ServiceTracker) Services() []string {
	st.mu.RLock()
	defer st.mu.RUnlock()

	type serviceCount struct {
		name  string
		count int
	}

	// Collect services with counts
	sc := make([]serviceCount, 0, len(st.services))
	for name, count := range st.services {
		sc = append(sc, serviceCount{name, count})
	}

	// Sort by count (descending), then by name (ascending)
	sort.Slice(sc, func(i, j int) bool {
		if sc[i].count != sc[j].count {
			return sc[i].count > sc[j].count
		}
		return sc[i].name < sc[j].name
	})

	// Extract names
	result := make([]string, len(sc))
	for i, s := range sc {
		result[i] = s.name
	}
	return result
}

// Suggest returns services that match the given prefix
func (st *ServiceTracker) Suggest(prefix string) []string {
	prefix = strings.ToLower(strings.TrimSpace(prefix))
	services := st.Services()

	if prefix == "" {
		return services
	}

	var matches []string
	for _, s := range services {
		if strings.HasPrefix(s, prefix) {
			matches = append(matches, s)
		}
	}
	return matches
}

// Count returns the number of discovered services
func (st *ServiceTracker) Count() int {
	st.mu.RLock()
	defer st.mu.RUnlock()
	return len(st.services)
}

// FormatServiceList formats the services as a display string
func (st *ServiceTracker) FormatServiceList() string {
	services := st.Services()
	if len(services) == 0 {
		return "(no services discovered yet)"
	}
	return strings.Join(services, ", ")
}
