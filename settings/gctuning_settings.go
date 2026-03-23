package settings

// GCTuningSettings configures Go runtime garbage collection tuning.
// When enabled, automatically detects the container's cgroup memory limit
// and sets GOMEMLIMIT accordingly, preventing OOM kills while allowing
// efficient memory utilization.
type GCTuningSettings struct {
	Enabled  bool    `key:"gc_tuning_enabled" desc:"Enable automatic GOMEMLIMIT configuration from cgroup memory limits" default:"true" category:"Global" usage:"Set to false to disable automatic GC tuning" type:"bool"`
	Ratio    float64 `key:"gc_tuning_ratio" desc:"Fraction of cgroup memory limit to set as GOMEMLIMIT (0.0-1.0)" default:"0.9" category:"Global" usage:"0.9 reserves 10% headroom for non-Go memory (cgo, mmap, kernel)" type:"float64"`
	GCTarget int     `key:"gc_tuning_gogc" desc:"GOGC target percentage (0=off, 100=default). Only applied when gc_tuning_enabled=true" default:"100" category:"Global" usage:"Higher values reduce GC frequency but increase memory usage. Safe when paired with GOMEMLIMIT" type:"int"`
}
