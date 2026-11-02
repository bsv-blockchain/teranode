package alarm

import "github.com/bsv-blockchain/teranode/services/probe/snapshot"

const (
	ChainThroughput = "throughput"
	ChainCatchup    = "catchup"

	CatchupInProgressID = "catchup-in-progress"
	SourceUnreachableID = "source-unreachable-"
)

type Rule struct {
	ID           string
	Stage        string
	Reading      string
	BadWhenLow   bool
	Chain        string
	ChainOrder   int
	CatchupOrder int
	MinTolerance float64
	FloorKey     string
	TextID       string
	Drilldowns   []snapshot.Link
}

var (
	linkIngress   = snapshot.Link{Label: "Open ingress details", Path: "#/stage/ingress"}
	linkValidator = snapshot.Link{Label: "Open Validator details", Path: "#/stage/validator"}
	linkCache     = snapshot.Link{Label: "Open cache details", Path: "#/stage/cache"}
	linkAssembly  = snapshot.Link{Label: "Open Block Assembly details", Path: "#/stage/assembly"}
	linkChain     = snapshot.Link{Label: "Open Blockchain details", Path: "#/stage/chain"}
	linkStores    = snapshot.Link{Label: "Open Aerospike details", Path: "#/stage/stores"}
)

var stageLinks = map[string]snapshot.Link{
	"ingress":   linkIngress,
	"validator": linkValidator,
	"cache":     linkCache,
	"assembly":  linkAssembly,
	"chain":     linkChain,
	"stores":    {Label: "Open data store details", Path: "#/stage/stores"},
}

var stageOrder = []string{"ingress", "validator", "cache", "assembly", "chain", "stores"}

var DefaultRules = append([]Rule{
	{
		ID: "cache-hit-rate", Stage: "cache", Reading: "cache.hitRate", BadWhenLow: true,
		Chain: ChainThroughput, ChainOrder: 1, FloorKey: "cache.hitRate", TextID: "cache-hit-rate",
		Drilldowns: []snapshot.Link{linkCache, linkStores},
	},
	{
		ID: "aerospike-read-p99", Stage: "stores", Reading: "stores.aerospike.readP99Sec",
		Chain: ChainThroughput, ChainOrder: 2, FloorKey: "stores.aerospike.readP99Sec", TextID: "aerospike-read-p99",
		Drilldowns: []snapshot.Link{linkStores, linkCache},
	},
	{
		ID: "validator-latency-p99", Stage: "validator", Reading: "validator.latencyP99Sec",
		Chain: ChainThroughput, ChainOrder: 3, CatchupOrder: 4, FloorKey: "validator.latencyP99Sec", TextID: "validator-latency-p99",
		Drilldowns: []snapshot.Link{linkValidator, linkCache, linkStores},
	},
	{
		ID: "validator-queue-fill", Stage: "validator", Reading: "validator.queueFill",
		Chain: ChainThroughput, ChainOrder: 3, CatchupOrder: 4, MinTolerance: 0.05, FloorKey: "validator.queueFill", TextID: "validator-queue-fill",
		Drilldowns: []snapshot.Link{linkValidator, linkCache},
	},
	{
		ID: "assembly-queue-head-age", Stage: "assembly", Reading: "assembly.queueHeadAgeSec",
		Chain: ChainThroughput, ChainOrder: 4, MinTolerance: 5, FloorKey: "assembly.queueHeadAgeSec", TextID: "assembly-queue-head-age",
		Drilldowns: []snapshot.Link{linkAssembly, linkValidator},
	},
	{
		ID: "assembly-tip-lag", Stage: "assembly", Reading: "assembly.tipLagBlocks",
		Chain: ChainThroughput, ChainOrder: 4, MinTolerance: 1, FloorKey: "assembly.tipLagBlocks", TextID: "assembly-tip-lag",
		Drilldowns: []snapshot.Link{linkAssembly, linkChain},
	},
	{
		ID: "subtree-queue-age", Stage: "ingress", Reading: "ingress.subtreeQueueOldestSec",
		Chain: ChainCatchup, ChainOrder: 3, MinTolerance: 5, FloorKey: "ingress.subtreeQueueOldestSec", TextID: "subtree-queue-age",
		Drilldowns: []snapshot.Link{linkIngress, linkValidator},
	},
	{
		ID: "persister-behind", Stage: "chain", Reading: "chain.persisterBehind",
		Chain: ChainCatchup, ChainOrder: 2, MinTolerance: 2, FloorKey: "chain.persisterBehind", TextID: "persister-behind",
		Drilldowns: []snapshot.Link{linkChain, linkStores},
	},
}, sourceRules()...)

var catchupRule = Rule{
	ID: CatchupInProgressID, Stage: "chain", Chain: ChainCatchup, ChainOrder: 1,
	TextID: CatchupInProgressID, Drilldowns: []snapshot.Link{linkChain},
}

func sourceRules() []Rule {
	out := make([]Rule, 0, len(stageOrder))

	for _, stage := range stageOrder {
		out = append(out, Rule{
			ID:         SourceUnreachableID + stage,
			Stage:      stage,
			TextID:     SourceUnreachableID + stage,
			Drilldowns: []snapshot.Link{stageLinks[stage]},
		})
	}

	return out
}

func (r Rule) orderIn(chain string) (int, bool) {
	switch {
	case r.Chain == chain:
		return r.ChainOrder, true
	case chain == ChainCatchup && r.Chain == ChainThroughput && r.CatchupOrder > 0:
		return r.CatchupOrder, true
	default:
		return 0, false
	}
}
