package util

var MetricsBucketsMicroSeconds = []float64{
	128e-6, 256e-6, 512e-6, 1024e-6, 2048e-6, 4096e-6, 8192e-6, 16384e-6, 32768e-6, 65536e-6, 131072e-6, 262144e-6,
}

var MetricsBucketsMilliSeconds = []float64{
	1e-3, 2e-3, 4e-3, 16e-3, 32e-3, 64e-3, 128e-3, 256e-3, 512e-3, 1024e-3, 2048e-3, 4096e-3,
}

var MetricsBucketsMilliLongSeconds = []float64{
	64e-3, 128e-3, 256e-3, 512e-3, 1024e-3, 2048e-3, 4096e-3, 8192e-3, 16384e-3, 32768e-3, 65536e-3, 131072e-3,
}

var MetricsBucketsSeconds = []float64{
	1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048,
}

var MetricsBucketsSize = []float64{
	128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144,
}
