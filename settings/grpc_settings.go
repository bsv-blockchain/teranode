package settings

// GRPCSettings configures gRPC client and server connection parameters.
// These settings control throughput optimization, keepalive behavior, and connection management.
type GRPCSettings struct {
	// HighThroughputMode enables optimized settings for high-throughput scenarios.
	// When enabled, increases buffer sizes, window sizes, and concurrent stream limits.
	// When disabled (default), uses conservative settings suitable for normal workloads.
	HighThroughputMode bool `key:"grpc_high_throughput_mode" desc:"Enable high-throughput gRPC optimizations" default:"false" category:"GRPC" usage:"Enable for high-volume transaction processing" type:"bool"`

	// InitialWindowSize is the per-stream flow control window size in bytes.
	// Larger values allow more data in-flight before requiring ACKs.
	// Default: 65536 (64KB) for normal mode, 1048576 (1MB) for high-throughput mode.
	InitialWindowSize int32 `key:"grpc_initial_window_size" desc:"Per-stream flow control window size in bytes" default:"0" category:"GRPC" usage:"0 uses mode-based default" type:"int32"`

	// InitialConnWindowSize is the per-connection flow control window size in bytes.
	// This is the aggregate window across all streams on a connection.
	// Default: 65536 (64KB) for normal mode, 4194304 (4MB) for high-throughput mode.
	InitialConnWindowSize int32 `key:"grpc_initial_conn_window_size" desc:"Per-connection flow control window size in bytes" default:"0" category:"GRPC" usage:"0 uses mode-based default" type:"int32"`

	// MaxConcurrentStreams is the maximum number of concurrent streams per connection (server-side).
	// Default: 100 for normal mode, 1000 for high-throughput mode.
	MaxConcurrentStreams uint32 `key:"grpc_max_concurrent_streams" desc:"Maximum concurrent streams per connection" default:"0" category:"GRPC" usage:"0 uses mode-based default" type:"uint32"`

	// ReadBufferSize is the read buffer size for the transport in bytes.
	// Larger buffers reduce syscall overhead for high-volume traffic.
	// Default: 32768 (32KB) for normal mode, 1048576 (1MB) for high-throughput mode.
	ReadBufferSize int `key:"grpc_read_buffer_size" desc:"Transport read buffer size in bytes" default:"0" category:"GRPC" usage:"0 uses mode-based default" type:"int"`

	// WriteBufferSize is the write buffer size for the transport in bytes.
	// Larger buffers reduce syscall overhead for high-volume traffic.
	// Default: 32768 (32KB) for normal mode, 1048576 (1MB) for high-throughput mode.
	WriteBufferSize int `key:"grpc_write_buffer_size" desc:"Transport write buffer size in bytes" default:"0" category:"GRPC" usage:"0 uses mode-based default" type:"int"`

	// KeepaliveTime is the interval for client keepalive pings when there's no activity.
	// Must be >= server's MinTime enforcement policy to avoid ENHANCE_YOUR_CALM errors.
	// Default: 30 seconds.
	KeepaliveTime int `key:"grpc_keepalive_time_seconds" desc:"Client keepalive ping interval in seconds" default:"30" category:"GRPC" usage:"Interval for detecting dead connections" type:"int"`

	// KeepaliveTimeout is the time to wait for a keepalive ping acknowledgment.
	// Default: 20 seconds.
	KeepaliveTimeout int `key:"grpc_keepalive_timeout_seconds" desc:"Keepalive ping acknowledgment timeout in seconds" default:"20" category:"GRPC" usage:"Timeout before considering connection dead" type:"int"`

	// ServerMinPingTime is the minimum allowed interval between client pings (server-side enforcement).
	// Clients sending pings more frequently will receive ENHANCE_YOUR_CALM.
	// Default: 30 seconds.
	ServerMinPingTime int `key:"grpc_server_min_ping_time_seconds" desc:"Minimum interval between client pings in seconds" default:"30" category:"GRPC" usage:"Server enforcement policy for client pings" type:"int"`

	// PermitWithoutStream allows keepalive pings even when there are no active streams.
	// Default: true (pings allowed on idle connections).
	PermitWithoutStream bool `key:"grpc_permit_without_stream" desc:"Allow keepalive pings without active streams" default:"true" category:"GRPC" usage:"Enable for better idle connection health" type:"bool"`

	// MaxConnectionIdle is the maximum time a connection can be idle before being closed (server-side).
	// Default: 5 minutes.
	MaxConnectionIdleSeconds int `key:"grpc_max_connection_idle_seconds" desc:"Maximum idle time before closing connection in seconds" default:"300" category:"GRPC" usage:"Server closes idle connections after this time" type:"int"`
}

// GetInitialWindowSize returns the effective per-stream window size based on settings.
func (g *GRPCSettings) GetInitialWindowSize() int32 {
	if g.InitialWindowSize > 0 {
		return g.InitialWindowSize
	}
	if g.HighThroughputMode {
		return 1 << 20 // 1 MB
	}
	return 64 << 10 // 64 KB (gRPC default)
}

// GetInitialConnWindowSize returns the effective per-connection window size based on settings.
func (g *GRPCSettings) GetInitialConnWindowSize() int32 {
	if g.InitialConnWindowSize > 0 {
		return g.InitialConnWindowSize
	}
	if g.HighThroughputMode {
		return 4 << 20 // 4 MB
	}
	return 64 << 10 // 64 KB (gRPC default)
}

// GetMaxConcurrentStreams returns the effective max concurrent streams based on settings.
func (g *GRPCSettings) GetMaxConcurrentStreams() uint32 {
	if g.MaxConcurrentStreams > 0 {
		return g.MaxConcurrentStreams
	}
	if g.HighThroughputMode {
		return 1000
	}
	return 100 // gRPC default
}

// GetReadBufferSize returns the effective read buffer size based on settings.
func (g *GRPCSettings) GetReadBufferSize() int {
	if g.ReadBufferSize > 0 {
		return g.ReadBufferSize
	}
	if g.HighThroughputMode {
		return 1 << 20 // 1 MB
	}
	return 32 << 10 // 32 KB (gRPC default)
}

// GetWriteBufferSize returns the effective write buffer size based on settings.
func (g *GRPCSettings) GetWriteBufferSize() int {
	if g.WriteBufferSize > 0 {
		return g.WriteBufferSize
	}
	if g.HighThroughputMode {
		return 1 << 20 // 1 MB
	}
	return 32 << 10 // 32 KB (gRPC default)
}
