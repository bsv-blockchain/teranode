package tracing

import (
	"github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"
	"github.com/opentracing/opentracing-go"
	"google.golang.org/grpc"
)

func GetGRPCClientTracerOptions(opts []grpc.DialOption, unaryClientInterceptors []grpc.UnaryClientInterceptor, streamClientInterceptors []grpc.StreamClientInterceptor) ([]grpc.DialOption, []grpc.UnaryClientInterceptor, []grpc.StreamClientInterceptor) {
	if opentracing.IsGlobalTracerRegistered() {
		tracer := opentracing.GlobalTracer()
		unaryClientInterceptors = append(unaryClientInterceptors, otgrpc.OpenTracingClientInterceptor(tracer))
		streamClientInterceptors = append(streamClientInterceptors, otgrpc.OpenTracingStreamClientInterceptor(tracer))
	}

	return opts, unaryClientInterceptors, streamClientInterceptors
}

func GetGRPCServerTracerOptions(opts []grpc.ServerOption, unaryInterceptors []grpc.UnaryServerInterceptor, streamInterceptors []grpc.StreamServerInterceptor) ([]grpc.ServerOption, []grpc.UnaryServerInterceptor, []grpc.StreamServerInterceptor) {
	if opentracing.IsGlobalTracerRegistered() {
		tracer := opentracing.GlobalTracer()
		unaryInterceptors = append(unaryInterceptors, otgrpc.OpenTracingServerInterceptor(tracer))
		streamInterceptors = append(streamInterceptors, otgrpc.OpenTracingStreamServerInterceptor(tracer))
	}

	return opts, unaryInterceptors, streamInterceptors
}
