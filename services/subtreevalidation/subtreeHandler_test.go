package subtreevalidation

import (
	"context"
	"os"
	"testing"

	"github.com/IBM/sarama"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/services/validator"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/kafka"
	kafkamessage "github.com/bitcoin-sv/teranode/util/kafka/kafka_message"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

type MockExister struct{}

func (m MockExister) Exists(_ context.Context, _ []byte, _ ...options.FileOption) (bool, error) {
	return false, nil
}

func TestLock(t *testing.T) {
	exister := MockExister{}

	tSettings := test.CreateBaseTestSettings()

	tSettings.SubtreeValidation.QuorumPath = "./data/subtree_quorum"

	defer func() {
		_ = os.RemoveAll(tSettings.SubtreeValidation.QuorumPath)
	}()

	q, err := NewQuorum(ulogger.TestLogger{}, exister, tSettings.SubtreeValidation.QuorumPath)
	require.NoError(t, err)

	hash := chainhash.HashH([]byte("test"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gotLock, _, releaseFn, err := q.TryLockIfNotExists(ctx, &hash)
	require.NoError(t, err)
	assert.True(t, gotLock)

	defer releaseFn()

	gotLock, _, releaseFn, err = q.TryLockIfNotExists(ctx, &hash)
	require.NoError(t, err)
	assert.False(t, gotLock)

	defer releaseFn()
}

type testServer struct {
	Server
	validateSubtreeInternalFn func(ctx context.Context, v ValidateSubtree, blockHeight uint32, validationOptions ...validator.Option) error
}

func (s *testServer) ValidateSubtreeInternal(ctx context.Context, v ValidateSubtree, blockHeight uint32, validationOptions ...validator.Option) error {
	if s.validateSubtreeInternalFn != nil {
		return s.validateSubtreeInternalFn(ctx, v, blockHeight, validationOptions...)
	}

	return nil
}

func TestSubtreesHandler(t *testing.T) {
	tests := []struct {
		name    string
		msg     *kafka.KafkaMessage
		setup   func(*testServer)
		wantErr bool
	}{
		{
			name: "valid message",
			msg: &kafka.KafkaMessage{
				ConsumerMessage: sarama.ConsumerMessage{
					Value: func() []byte {
						msg := &kafkamessage.KafkaSubtreeTopicMessage{
							Hash: make([]byte, 32),
							URL:  "http://example.com",
						}
						data, _ := proto.Marshal(msg)
						return data
					}(),
				},
			},
			setup: func(s *testServer) {
				s.validateSubtreeInternalFn = func(ctx context.Context, v ValidateSubtree, blockHeight uint32, validationOptions ...validator.Option) error {
					return nil
				}
			},
			wantErr: false,
		},
		{
			name: "invalid message unmarshal",
			msg: &kafka.KafkaMessage{
				ConsumerMessage: sarama.ConsumerMessage{
					Value: []byte("invalid data"),
				},
			},
			wantErr: true,
		},
		{
			name: "invalid hash",
			msg: &kafka.KafkaMessage{
				ConsumerMessage: sarama.ConsumerMessage{
					Value: func() []byte {
						msg := &kafkamessage.KafkaSubtreeTopicMessage{
							Hash: []byte("invalid"),
							URL:  "http://example.com",
						}
						data, _ := proto.Marshal(msg)
						return data
					}(),
				},
			},
			wantErr: true,
		},
		{
			name: "invalid URL",
			msg: &kafka.KafkaMessage{
				ConsumerMessage: sarama.ConsumerMessage{
					Value: func() []byte {
						msg := &kafkamessage.KafkaSubtreeTopicMessage{
							Hash: make([]byte, 32),
							URL:  "://invalid",
						}
						data, _ := proto.Marshal(msg)
						return data
					}(),
				},
			},
			wantErr: true,
		},
		{
			name: "validation error",
			msg: &kafka.KafkaMessage{
				ConsumerMessage: sarama.ConsumerMessage{
					Value: func() []byte {
						msg := &kafkamessage.KafkaSubtreeTopicMessage{
							Hash: make([]byte, 32),
							URL:  "http://example.com",
						}
						data, _ := proto.Marshal(msg)
						return data
					}(),
				},
			},
			setup: func(s *testServer) {
				s.validateSubtreeInternalFn = func(ctx context.Context, v ValidateSubtree, blockHeight uint32, validationOptions ...validator.Option) error {
					return errors.New(errors.ERR_INVALID_ARGUMENT, "validation failed")
				}
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tSettings := test.CreateBaseTestSettings()
			tSettings.SubtreeValidation.QuorumPath = "./data/subtree_quorum"

			defer func() {
				_ = os.RemoveAll(tSettings.SubtreeValidation.QuorumPath)
			}()

			server := &testServer{
				Server: Server{
					logger: ulogger.TestLogger{},
				},
			}

			if tt.setup != nil {
				tt.setup(server)
			}

			err := server.subtreesHandler(tt.msg)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
