package uaerospike

import (
	"sync"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
)

// MockAerospikeClient is a mock implementation of the aerospike.IClient interface
// for testing purposes without requiring a real Aerospike server
type MockAerospikeClient struct {
	// Control behavior
	ShouldError    bool
	ErrorToReturn  aerospike.Error
	RecordToReturn *aerospike.Record
	DeleteResult   bool

	// Track calls
	PutCalled          int
	PutBinsCalled      int
	DeleteCalled       int
	GetCalled          int
	OperateCalled      int
	BatchOperateCalled int
	ExecuteCalled      int
	CloseCalled        int

	// Store last call parameters for verification
	LastKey          *aerospike.Key
	LastBinMap       aerospike.BinMap
	LastBins         []*aerospike.Bin
	LastBinNames     []string
	LastOperations   []*aerospike.Operation
	LastRecords      []aerospike.BatchRecordIfc
	LastPackageName  string
	LastFunctionName string

	// Synchronization for concurrent access
	mu sync.RWMutex
}

// NewMockAerospikeClient creates a new mock aerospike client
func NewMockAerospikeClient() *MockAerospikeClient {
	return &MockAerospikeClient{
		RecordToReturn: &aerospike.Record{},
		DeleteResult:   true,
	}
}

// Put implements the aerospike Put operation
func (m *MockAerospikeClient) Put(policy *aerospike.WritePolicy, key *aerospike.Key, binMap aerospike.BinMap) aerospike.Error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.PutCalled++
	m.LastKey = key
	m.LastBinMap = binMap

	if m.ShouldError {
		return m.ErrorToReturn
	}
	return nil
}

// PutBins implements the aerospike PutBins operation
func (m *MockAerospikeClient) PutBins(policy *aerospike.WritePolicy, key *aerospike.Key, bins ...*aerospike.Bin) aerospike.Error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.PutBinsCalled++
	m.LastKey = key
	m.LastBins = bins

	if m.ShouldError {
		return m.ErrorToReturn
	}
	return nil
}

// Delete implements the aerospike Delete operation
func (m *MockAerospikeClient) Delete(policy *aerospike.WritePolicy, key *aerospike.Key) (bool, aerospike.Error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.DeleteCalled++
	m.LastKey = key

	if m.ShouldError {
		return false, m.ErrorToReturn
	}
	return m.DeleteResult, nil
}

// Get implements the aerospike Get operation
func (m *MockAerospikeClient) Get(policy *aerospike.BasePolicy, key *aerospike.Key, binNames ...string) (*aerospike.Record, aerospike.Error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.GetCalled++
	m.LastKey = key
	m.LastBinNames = binNames

	if m.ShouldError {
		return nil, m.ErrorToReturn
	}
	return m.RecordToReturn, nil
}

// Operate implements the aerospike Operate operation
func (m *MockAerospikeClient) Operate(policy *aerospike.WritePolicy, key *aerospike.Key, operations ...*aerospike.Operation) (*aerospike.Record, aerospike.Error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.OperateCalled++
	m.LastKey = key
	m.LastOperations = operations

	if m.ShouldError {
		return nil, m.ErrorToReturn
	}
	return m.RecordToReturn, nil
}

// BatchOperate implements the aerospike BatchOperate operation
func (m *MockAerospikeClient) BatchOperate(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.BatchOperateCalled++
	m.LastRecords = records

	if m.ShouldError {
		return m.ErrorToReturn
	}
	return nil
}

// Execute implements the aerospike Execute (UDF) operation
func (m *MockAerospikeClient) Execute(policy *aerospike.WritePolicy, key *aerospike.Key, packageName string, functionName string, args ...aerospike.Value) (any, aerospike.Error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.ExecuteCalled++
	m.LastKey = key
	m.LastPackageName = packageName
	m.LastFunctionName = functionName

	if m.ShouldError {
		return nil, m.ErrorToReturn
	}
	return nil, nil
}

// Close implements the aerospike Close operation
func (m *MockAerospikeClient) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.CloseCalled++
}

// SetError configures the mock to return an error
func (m *MockAerospikeClient) SetError(shouldError bool, err aerospike.Error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.ShouldError = shouldError
	m.ErrorToReturn = err
}

// SetRecord configures the mock to return a specific record
func (m *MockAerospikeClient) SetRecord(record *aerospike.Record) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.RecordToReturn = record
}

// SetDeleteResult configures the mock to return a specific delete result
func (m *MockAerospikeClient) SetDeleteResult(result bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.DeleteResult = result
}

// Reset clears all call counts and stored parameters
func (m *MockAerospikeClient) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.PutCalled = 0
	m.PutBinsCalled = 0
	m.DeleteCalled = 0
	m.GetCalled = 0
	m.OperateCalled = 0
	m.BatchOperateCalled = 0
	m.ExecuteCalled = 0
	m.CloseCalled = 0

	m.LastKey = nil
	m.LastBinMap = nil
	m.LastBins = nil
	m.LastBinNames = nil
	m.LastOperations = nil
	m.LastRecords = nil
	m.LastPackageName = ""
	m.LastFunctionName = ""
}

// MockAerospikeError is a mock implementation of aerospike.Error for testing
type MockAerospikeError struct {
	ResultCodeValue types.ResultCode
	MessageValue    string
}

// NewMockAerospikeError creates a new mock aerospike error
func NewMockAerospikeError(code types.ResultCode, message string) *MockAerospikeError {
	return &MockAerospikeError{
		ResultCodeValue: code,
		MessageValue:    message,
	}
}

// ResultCode implements aerospike.Error
func (e *MockAerospikeError) ResultCode() types.ResultCode {
	return e.ResultCodeValue
}

// Error implements aerospike.Error and the standard error interface
func (e *MockAerospikeError) Error() string {
	return e.MessageValue
}

// Matches implements aerospike.Error
func (e *MockAerospikeError) Matches(codes ...types.ResultCode) bool {
	for _, code := range codes {
		if e.ResultCodeValue == code {
			return true
		}
	}
	return false
}

// InDoubt implements aerospike.Error
func (e *MockAerospikeError) InDoubt() bool {
	return false
}

// IsInDoubt implements aerospike.Error (deprecated method name, kept for compatibility)
func (e *MockAerospikeError) IsInDoubt() bool {
	return false
}

// Trace implements aerospike.Error
func (e *MockAerospikeError) Trace() string {
	return e.MessageValue
}

// Unwrap implements aerospike.Error
func (e *MockAerospikeError) Unwrap() error {
	return nil
}

// iter implements aerospike.Error (internal iterator method)
func (e *MockAerospikeError) iter() {
	// No-op implementation
}

// MockHost creates a mock aerospike.Host for testing
func MockHost(name string, port int) *aerospike.Host {
	return aerospike.NewHost(name, port)
}
