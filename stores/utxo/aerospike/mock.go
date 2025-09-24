package aerospike

import (
	"context"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/mock"
)

// MockAerospikeClient mocks the Aerospike client interface
type MockAerospikeClient struct {
	mock.Mock
}

func (m *MockAerospikeClient) Query(policy *as.QueryPolicy, statement *as.Statement) (*as.Recordset, error) {
	args := m.Called(policy, statement)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*as.Recordset), args.Error(1)
}

// MockRecordset mocks the Aerospike recordset
type MockRecordset struct {
	mock.Mock
	resultChan chan *as.Result
}

func (m *MockRecordset) Results() <-chan *as.Result {
	if m.resultChan == nil {
		m.resultChan = make(chan *as.Result, 10) // Buffered channel for testing
	}
	return m.resultChan
}

func (m *MockRecordset) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockRecordset) IsClosed() bool {
	args := m.Called()
	return args.Bool(0)
}

// AddResult adds a result to the mock recordset for testing
func (m *MockRecordset) AddResult(result *as.Result) {
	if m.resultChan == nil {
		m.resultChan = make(chan *as.Result, 10)
	}
	m.resultChan <- result
}

// CloseResults closes the results channel to simulate end of iteration
func (m *MockRecordset) CloseResults() {
	if m.resultChan != nil {
		close(m.resultChan)
	}
}

// MockStore mocks the Store for testing
type MockStore struct {
	mock.Mock
	namespace string
	setName   string
	client    interface{} // Will hold our mock client
	logger    MockLogger
}

func (m *MockStore) GetTxFromExternalStore(ctx context.Context, hash chainhash.Hash) (*bt.Tx, error) {
	args := m.Called(ctx, hash)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*bt.Tx), args.Error(1)
}

// MockLogger mocks the logger interface
type MockLogger struct {
	mock.Mock
}

func (m *MockLogger) Warnf(format string, args ...interface{}) {
	m.Called(format, args)
}

func (m *MockLogger) Errorf(format string, args ...interface{}) {
	m.Called(format, args)
}

func (m *MockLogger) Infof(format string, args ...interface{}) {
	m.Called(format, args)
}

func (m *MockLogger) Debugf(format string, args ...interface{}) {
	m.Called(format, args)
}

func (m *MockLogger) Printf(format string, args ...interface{}) {
	m.Called(format, args)
}

func (m *MockLogger) Logf(format string, args ...interface{}) {
	m.Called(format, args)
}

func (m *MockLogger) Fatalf(format string, args ...interface{}) {
	m.Called(format, args)
}

func (m *MockLogger) Duplicate(options ...ulogger.Option) ulogger.Logger {
	args := m.Called(options)
	return args.Get(0).(ulogger.Logger)
}

func (m *MockLogger) WithField(key, value interface{}) ulogger.Logger {
	args := m.Called(key, value)
	return args.Get(0).(ulogger.Logger)
}

func (m *MockLogger) WithFields(fields map[string]interface{}) ulogger.Logger {
	args := m.Called(fields)
	return args.Get(0).(ulogger.Logger)
}

func (m *MockLogger) LogLevel() int {
	args := m.Called()
	return args.Int(0)
}

func (m *MockLogger) SetLogLevel(level string) {
	m.Called(level)
}

func (m *MockLogger) New(service string, options ...ulogger.Option) ulogger.Logger {
	args := m.Called(service, options)
	return args.Get(0).(ulogger.Logger)
}
