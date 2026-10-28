package alarm

type text struct {
	title string
	body  string
}

var stageNames = map[string]string{
	"ingress":   "Ingress",
	"validator": "Validator",
	"cache":     "Tx meta cache",
	"assembly":  "Block Assembly",
	"chain":     "Blockchain",
	"stores":    "Data stores",
}

var texts = buildTexts()

func buildTexts() map[string]text {
	out := map[string]text{
		"cache-hit-rate": {
			title: "Tx meta cache hit rate is low",
			body: "Fewer lookups hit the tx meta cache than normal. " +
				"Each miss reads the UTXO store, so Aerospike load and Validator latency rise. " +
				"A cache that is full is normal, because ring caches stay full after their first pass. " +
				"The live signals are the hit rate and the eviction rate. " +
				"Look at the eviction rate and the share of entries from the previous generation.",
		},
		"aerospike-read-p99": {
			title: "Aerospike read latency is high",
			body: "Aerospike read p99 is above its normal value. " +
				"Slow reads delay each cache miss and each spend in the Validator. " +
				"Look at Aerospike disk, network and operations per second. " +
				"This reading needs enable-benchmarks-read on the Aerospike namespace. " +
				"Without it, the panel shows no data.",
		},
		"validator-latency-p99": {
			title: "Validator latency is high",
			body: "The Validator takes longer than normal to validate a transaction. " +
				"Look at the tx meta cache hit rate and Aerospike read latency first. " +
				"A slow Validator fills its queue and starves Block Assembly.",
		},
		"validator-queue-fill": {
			title: "Validator queue is filling",
			body: "The Validator cannot process transactions as fast as they arrive. " +
				"Its internal queue grows. " +
				"Look at the tx meta cache hit rate and Aerospike read latency. " +
				"During catch-up, the Validator also checks the transactions of each queued block.",
		},
		"assembly-queue-head-age": {
			title: "Block Assembly input is old",
			body: "The oldest transaction in the Block Assembly queue waits longer than normal. " +
				"Block Assembly does not keep up with its input. " +
				"Look at the Validator queue and the Block Assembly state.",
		},
		"assembly-tip-lag": {
			title: "Block Assembly is behind the chain tip",
			body: "Block Assembly works on a block below the current chain tip. " +
				"It does not move to new blocks as fast as they arrive. " +
				"Look at the Blockchain panel and the Block Assembly state.",
		},
		"subtree-queue-age": {
			title: "Subtrees wait too long",
			body: "The oldest queued subtree waits longer than normal. " +
				"Subtree validation does not keep up with the subtrees that peers announce. " +
				"Look at the Validator queue and the tx meta cache hit rate.",
		},
		"persister-behind": {
			title: "Block persister is behind",
			body: "The block persister has not written all blocks up to the tip. " +
				"During catch-up, this backlog grows first. " +
				"Look at the block store and the Blockchain panel.",
		},
		CatchupInProgressID: {
			title: "Node is catching up",
			body: "The node is in the CATCHINGBLOCKS state. " +
				"It downloads and validates blocks to reach the network tip. " +
				"Transaction throughput stays low until catch-up ends. " +
				"Look at the queued blocks and the block persister.",
		},
	}

	for _, stage := range stageOrder {
		name := stageNames[stage]
		out[SourceUnreachableID+stage] = text{
			title: name + " source has no data",
			body: "The probe has had no fresh data for the " + name + " panel for more than one minute. " +
				"The source can be down, or the probe cannot reach it. " +
				"Look at the health of the service and the probe settings for this source.",
		}
	}

	return out
}

func Text(textID string) (title, body string, ok bool) {
	t, ok := texts[textID]
	if !ok {
		return "", "", false
	}

	return t.title, t.body, true
}
