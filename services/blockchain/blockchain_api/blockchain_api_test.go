package blockchain_api

import (
	"strings"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Helper function to create a test block hash
func createTestHash(val byte) []byte {
	hash := make([]byte, 32)
	for i := range hash {
		hash[i] = val
	}

	return hash
}

func TestNotification_Stringify(t *testing.T) {
	tests := []struct {
		name     string
		notif    *Notification
		expected string
	}{
		{
			name: "block notification",
			notif: &Notification{
				Type: model.NotificationType_Block,
				Hash: []byte{0x01, 0x02, 0x03},
				Metadata: &NotificationMetadata{
					Metadata: map[string]string{"height": "12345"},
				},
			},
			expected: "Block: 030201, metadata: metadata:{key:\"height\" value:\"12345\"}",
		},
		{
			name: "subtree notification",
			notif: &Notification{
				Type: model.NotificationType_Subtree,
				Hash: []byte{0xFF, 0xEE, 0xDD},
				Metadata: &NotificationMetadata{
					Metadata: map[string]string{"size": "1000"},
				},
			},
			expected: "Subtree: ddeeff, metadata: metadata:{key:\"size\" value:\"1000\"}",
		},
		{
			name: "notification with empty metadata",
			notif: &Notification{
				Type: model.NotificationType_Block,
				Hash: []byte{0x01, 0x02, 0x03},
				Metadata: &NotificationMetadata{
					Metadata: map[string]string{},
				},
			},
			expected: "Block: 030201, metadata: ",
		},
		{
			name: "notification with nil metadata",
			notif: &Notification{
				Type: model.NotificationType_Block,
				Hash: []byte{0x01, 0x02, 0x03},
			},
			expected: "Block: 030201, metadata: <nil>",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.notif.Stringify()
			// Normalize spaces in metadata formatting
			result = strings.ReplaceAll(result, "  ", " ")
			tt.expected = strings.ReplaceAll(tt.expected, "  ", " ")
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestHealthResponse_Fields(t *testing.T) {
	tests := []struct {
		name     string
		response *HealthResponse
		check    func(*testing.T, *HealthResponse)
	}{
		{
			name: "all fields set",
			response: &HealthResponse{
				Ok:        true,
				Details:   "all systems operational",
				Timestamp: timestamppb.Now(),
			},
			check: func(t *testing.T, hr *HealthResponse) {
				assert.True(t, hr.Ok)
				assert.NotEmpty(t, hr.Details)
				assert.NotNil(t, hr.Timestamp)
			},
		},
		{
			name: "minimal fields",
			response: &HealthResponse{
				Ok: true,
			},
			check: func(t *testing.T, hr *HealthResponse) {
				assert.True(t, hr.Ok)
				assert.Empty(t, hr.Details)
				assert.Nil(t, hr.Timestamp)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.check(t, tt.response)
		})
	}
}

func TestAddBlockRequest_Fields(t *testing.T) {
	tests := []struct {
		name    string
		request *AddBlockRequest
		check   func(*testing.T, *AddBlockRequest)
	}{
		{
			name: "complete request",
			request: &AddBlockRequest{
				Header:           createTestHash(0x01),
				SubtreeHashes:    [][]byte{createTestHash(0x02)},
				CoinbaseTx:       createTestHash(0x03),
				TransactionCount: 100,
				SizeInBytes:      1000000,
				External:         false,
				PeerId:           "peer1",
			},
			check: func(t *testing.T, r *AddBlockRequest) {
				assert.Len(t, r.Header, 32)
				assert.NotEmpty(t, r.SubtreeHashes)
				assert.Len(t, r.CoinbaseTx, 32)
				assert.Greater(t, r.TransactionCount, uint64(0))
				assert.Greater(t, r.SizeInBytes, uint64(0))
				assert.NotEmpty(t, r.PeerId)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.check(t, tt.request)
		})
	}
}

func TestGetBlockRequest_Fields(t *testing.T) {
	tests := []struct {
		name    string
		request *GetBlockRequest
		check   func(*testing.T, *GetBlockRequest)
	}{
		{
			name: "valid hash",
			request: &GetBlockRequest{
				Hash: createTestHash(0x01),
			},
			check: func(t *testing.T, r *GetBlockRequest) {
				assert.Len(t, r.Hash, 32)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.check(t, tt.request)
		})
	}
}

func TestGetBlockHeadersRequest_Fields(t *testing.T) {
	tests := []struct {
		name    string
		request *GetBlockHeadersRequest
		check   func(*testing.T, *GetBlockHeadersRequest)
	}{
		{
			name: "valid request",
			request: &GetBlockHeadersRequest{
				StartHash:       createTestHash(0x01),
				NumberOfHeaders: 100,
			},
			check: func(t *testing.T, r *GetBlockHeadersRequest) {
				assert.Len(t, r.StartHash, 32)
				assert.Greater(t, r.NumberOfHeaders, uint64(0))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.check(t, tt.request)
		})
	}
}

func TestFSMEventType_Values(t *testing.T) {
	// Test that FSM event types are defined correctly
	assert.Equal(t, int32(0), int32(FSMEventType_STOP))
	assert.Equal(t, int32(1), int32(FSMEventType_RUN))
	assert.Equal(t, int32(2), int32(FSMEventType_CATCHUPBLOCKS))
	assert.Equal(t, int32(3), int32(FSMEventType_LEGACYSYNC))
}

func TestFSMStateType_Values(t *testing.T) {
	// Test that FSM state types are defined correctly
	assert.Equal(t, int32(0), int32(FSMStateType_IDLE))
	assert.Equal(t, int32(1), int32(FSMStateType_RUNNING))
	assert.Equal(t, int32(2), int32(FSMStateType_CATCHINGBLOCKS))
	assert.Equal(t, int32(3), int32(FSMStateType_LEGACYSYNCING))
}

func TestGetLastNBlocksRequest_Fields(t *testing.T) {
	tests := []struct {
		name    string
		request *GetLastNBlocksRequest
		check   func(*testing.T, *GetLastNBlocksRequest)
	}{
		{
			name: "valid request",
			request: &GetLastNBlocksRequest{
				NumberOfBlocks: 10,
				IncludeOrphans: true,
				FromHeight:     1000,
			},
			check: func(t *testing.T, r *GetLastNBlocksRequest) {
				assert.Greater(t, r.NumberOfBlocks, int64(0))
				assert.True(t, r.IncludeOrphans)
				assert.Greater(t, r.FromHeight, uint32(0))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.check(t, tt.request)
		})
	}
}

func TestGetBlockHeaderResponse_Fields(t *testing.T) {
	tests := []struct {
		name     string
		response *GetBlockHeaderResponse
		check    func(*testing.T, *GetBlockHeaderResponse)
	}{
		{
			name: "complete response",
			response: &GetBlockHeaderResponse{
				BlockHeader: createTestHash(0x01),
				Height:      12345,
				TxCount:     100,
				SizeInBytes: 1000000,
				Miner:       "test_miner",
				BlockTime:   uint32(time.Now().Unix()), // nolint:gosec
				Timestamp:   uint32(time.Now().Unix()), // nolint:gosec
				ChainWork:   []byte{0x01, 0x02, 0x03},
			},
			check: func(t *testing.T, r *GetBlockHeaderResponse) {
				assert.Len(t, r.BlockHeader, 32)
				assert.Greater(t, r.Height, uint32(0))
				assert.Greater(t, r.TxCount, uint64(0))
				assert.Greater(t, r.SizeInBytes, uint64(0))
				assert.NotEmpty(t, r.Miner)
				assert.Greater(t, r.BlockTime, uint32(0))
				assert.Greater(t, r.Timestamp, uint32(0))
				assert.NotEmpty(t, r.ChainWork)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.check(t, tt.response)
		})
	}
}

func TestSubscribeRequest_Fields(t *testing.T) {
	tests := []struct {
		name    string
		request *SubscribeRequest
		check   func(*testing.T, *SubscribeRequest)
	}{
		{
			name: "valid source",
			request: &SubscribeRequest{
				Source: "test_client",
			},
			check: func(t *testing.T, r *SubscribeRequest) {
				assert.NotEmpty(t, r.Source)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.check(t, tt.request)
		})
	}
}
