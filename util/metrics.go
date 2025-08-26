package util

// MetricsBucketsMicroSeconds defines histogram buckets for microsecond-level latency measurements.
// Buckets range from 128μs to 262ms in exponential progression.
var MetricsBucketsMicroSeconds = []float64{
	128e-6, 256e-6, 512e-6, 1024e-6, 2048e-6, 4096e-6, 8192e-6, 16384e-6, 32768e-6, 65536e-6, 131072e-6, 262144e-6,
}

// MetricsBucketsMilliSeconds defines histogram buckets for millisecond-level latency measurements.
// Buckets range from 1ms to 4s in exponential progression.
var MetricsBucketsMilliSeconds = []float64{
	1e-3, 2e-3, 4e-3, 16e-3, 32e-3, 64e-3, 128e-3, 256e-3, 512e-3, 1024e-3, 2048e-3, 4096e-3,
}

// MetricsBucketsMilliLongSeconds defines histogram buckets for longer millisecond-level measurements.
// Buckets range from 64ms to 131s in exponential progression.
var MetricsBucketsMilliLongSeconds = []float64{
	64e-3, 128e-3, 256e-3, 512e-3, 1024e-3, 2048e-3, 4096e-3, 8192e-3, 16384e-3, 32768e-3, 65536e-3, 131072e-3,
}

// MetricsBucketsSeconds defines histogram buckets for second-level duration measurements.
// Buckets range from 1s to 2048s (~34 minutes) in exponential progression.
var MetricsBucketsSeconds = []float64{
	1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048,
}

// MetricsBucketsSizeSmall defines histogram buckets for small data size measurements.
// Buckets range from 1 byte to 32KB in exponential progression.
var MetricsBucketsSizeSmall = []float64{
	1, 16, 32, 64, 128, 256, 1024, 2048, 4096, 8192, 16384, 32768,
}

// MetricsBucketsSize defines histogram buckets for general data size measurements.
// Buckets range from 128 bytes to 256KB in exponential progression.
var MetricsBucketsSize = []float64{
	128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144,
}
