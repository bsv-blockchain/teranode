package util

var MetricsBuckets = []float64{
	4e-6, 16e-6, 64e-6, 256e-6, 1e-3, 10,
}

var MetricsBucketsMilliSeconds = []float64{
	4e-3, 16e-3, 64e-3, 256e-3, 1, 10,
}

var MetricsBucketsSeconds = []float64{
	1, 2, 4, 8, 16, 32,
}
