package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_bytes2Uint16(t *testing.T) {
	type args struct {
		b   [32]byte
		mod uint16
	}
	tests := []struct {
		name string
		args args
		want uint16
	}{
		{
			name: "bytes2Uint16",
			args: args{
				b:   [32]byte{0x00, 0x01},
				mod: 256,
			},
			want: 1,
		},
		{
			name: "bytes2Uint16",
			args: args{
				b:   [32]byte{0x01, 0xff},
				mod: 256,
			},
			want: 255,
		},
		{
			name: "bytes2Uint16",
			args: args{
				b:   [32]byte{0xff, 0x01},
				mod: 256,
			},
			want: 1,
		},
		{
			name: "bytes2Uint16",
			args: args{
				b:   [32]byte{0xff, 0xff},
				mod: 256,
			},
			want: 255,
		},
		{
			name: "bytes2Uint16",
			args: args{
				b:   [32]byte{0xdd, 0xdd},
				mod: 256,
			},
			want: 221,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, Bytes2Uint16Buckets(tt.args.b, tt.args.mod), "bytes2Uint16(%v)", tt.args.b)
		})
	}
}

func TestNewSplitSwissMap(t *testing.T) {
	t.Run("NewSplitSwissMap", func(t *testing.T) {
		m := NewSplitSwissMap(100)
		assert.NotNil(t, m)

		err := m.Put([32]byte{0x00, 0x01}, 1)
		require.NoError(t, err)

		ok := m.Exists([32]byte{0x00, 0x01})
		assert.True(t, ok)

		ok = m.Exists([32]byte{0x01, 0x01})
		assert.False(t, ok)
	})
}

func TestNewSwissMapKVUint64(t *testing.T) {
	t.Run("NewSplitSwissMap", func(t *testing.T) {
		m := NewSwissMapKVUint64(100)
		assert.NotNil(t, m)

		err := m.Put(1, 1)
		require.NoError(t, err)

		ok := m.Exists(1)
		assert.True(t, ok)

		ok = m.Exists(2)
		assert.False(t, ok)
	})
}

func TestNewSplitSwissMapKVUint64(t *testing.T) {
	t.Run("NewSplitSwissMap", func(t *testing.T) {
		m := NewSplitSwissMapKVUint64(100)
		assert.NotNil(t, m)

		err := m.Put(1, 1)
		require.NoError(t, err)

		ok := m.Exists(1)
		assert.True(t, ok)

		ok = m.Exists(2)
		assert.False(t, ok)
	})
}
