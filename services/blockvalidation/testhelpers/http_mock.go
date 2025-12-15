package testhelpers

import (
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/jarcoal/httpmock"
)

// HTTPMockResponse represents a mock HTTP response configuration
type HTTPMockResponse struct {
	StatusCode int
	Headers    map[string]string
	Body       []byte
	Error      error
	Delay      time.Duration
	Validator  func(req *http.Request) error
}

// HTTPMockSetup provides HTTP mocking functionality for tests
type HTTPMockSetup struct {
	t         *testing.T
	responses map[string]*HTTPMockResponse
	counters  map[string]*atomic.Int32
	mu        sync.RWMutex
	activated bool
}

// NewHTTPMockSetup creates a new HTTP mock setup
func NewHTTPMockSetup(t *testing.T) *HTTPMockSetup {
	return &HTTPMockSetup{
		t:         t,
		responses: make(map[string]*HTTPMockResponse),
		counters:  make(map[string]*atomic.Int32),
	}
}

// RegisterResponse registers a mock response for a URL pattern
func (m *HTTPMockSetup) RegisterResponse(pattern string, response *HTTPMockResponse) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.responses[pattern] = response
	m.counters[pattern] = &atomic.Int32{}
}

// RegisterHeaderResponse registers a response that returns headers as bytes
func (m *HTTPMockSetup) RegisterHeaderResponse(url string, headers interface{}) {
	var body []byte

	// Handle both []byte and []*model.BlockHeader
	switch v := headers.(type) {
	case []byte:
		body = v
	case []*model.BlockHeader:
		body = HeadersToBytes(v)
	default:
		m.t.Fatalf("RegisterHeaderResponse: unsupported type %T", headers)
	}

	// Register with a pattern that matches the headers_from_common_ancestor endpoint
	pattern := fmt.Sprintf(`=~^%s/headers_from_common_ancestor/.*`, url)
	m.RegisterResponse(pattern, &HTTPMockResponse{
		StatusCode: 200,
		Body:       body,
	})
}

// RegisterFlakeyResponse registers a response that fails N times before succeeding
func (m *HTTPMockSetup) RegisterFlakeyResponse(url string, failCount int, successData interface{}) {
	var body []byte

	// Handle both []byte and []*model.BlockHeader
	switch v := successData.(type) {
	case []byte:
		body = v
	case []*model.BlockHeader:
		body = HeadersToBytes(v)
	default:
		m.t.Fatalf("RegisterFlakeyResponse: unsupported type %T", successData)
	}

	counter := &atomic.Int32{}
	// Register with a pattern that matches the headers_from_common_ancestor endpoint
	pattern := fmt.Sprintf(`=~^%s/headers_from_common_ancestor/.*`, url)
	m.RegisterResponse(pattern, &HTTPMockResponse{
		StatusCode: 200,
		Body:       body,
		Validator: func(req *http.Request) error {
			if counter.Add(1) <= int32(failCount) {
				return errors.ErrError
			}
			return nil
		},
	})
}

// RegisterPaginatedHeaderResponse registers paginated responses
func (m *HTTPMockSetup) RegisterPaginatedHeaderResponse(url string, data []byte, pageSize int) {
	requestCount := &atomic.Int32{}

	m.RegisterResponse(url, &HTTPMockResponse{
		StatusCode: 200,
		Validator: func(req *http.Request) error {
			return nil
		},
		Body: func() []byte {
			page := int(requestCount.Add(1)) - 1
			start := page * pageSize
			end := start + pageSize

			if start >= len(data) {
				return []byte{}
			}

			if end > len(data) {
				end = len(data)
			}

			return data[start:end]
		}(),
	})
}

// Activate activates the HTTP mock
func (m *HTTPMockSetup) Activate() {
	if m.activated {
		return
	}

	httpmock.ActivateNonDefault(util.HTTPClient())
	m.activated = true

	// Register all configured responses
	for pattern, response := range m.responses {
		resp := response // capture for closure
		counter := m.counters[pattern]

		httpmock.RegisterResponder(
			"GET",
			pattern,
			func(req *http.Request) (*http.Response, error) {
				counter.Add(1)

				if resp.Delay > 0 {
					time.Sleep(resp.Delay)
				}

				if resp.Validator != nil {
					if err := resp.Validator(req); err != nil {
						return nil, err
					}
				}

				if resp.Error != nil {
					return nil, resp.Error
				}

				return httpmock.NewBytesResponder(resp.StatusCode, resp.Body)(req)
			},
		)
	}
}

// Deactivate deactivates the HTTP mock
func (m *HTTPMockSetup) Deactivate() {
	if m.activated {
		httpmock.DeactivateAndReset()
		m.activated = false
	}
}

// GetCallCount returns the number of times a pattern was called
func (m *HTTPMockSetup) GetCallCount(pattern string) int32 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if counter, exists := m.counters[pattern]; exists {
		return counter.Load()
	}
	return 0
}

// RegisterErrorResponse registers a response that returns an error
func (m *HTTPMockSetup) RegisterErrorResponse(url string, err error) {
	// Register with a pattern that matches the headers_from_common_ancestor endpoint
	pattern := fmt.Sprintf(`=~^%s/headers_from_common_ancestor/.*`, url)
	m.RegisterResponse(pattern, &HTTPMockResponse{
		Error: err,
	})
}
