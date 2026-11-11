package snapshot

import (
	"reflect"
	"strings"
	"time"
)

type Status string

const (
	StatusOK     Status = "ok"
	StatusWarn   Status = "warn"
	StatusCrit   Status = "crit"
	StatusIdle   Status = "idle"
	StatusNoData Status = "nodata"
)

const (
	FlowTxInput     = "tx-input"
	FlowSubtrees    = "subtrees"
	FlowTxMeta      = "txmeta"
	FlowLookups     = "lookups"
	FlowValidated   = "validated"
	FlowBlocks      = "blocks"
	FlowBlocksFinal = "blocks-final"
	FlowUTXOOps     = "utxo-ops"
	FlowBlockWrites = "block-writes"
)

type Reading struct {
	Value  float64  `json:"v"`
	Status Status   `json:"st"`
	Normal *float64 `json:"normal,omitempty"`
	Note   string   `json:"note,omitempty"`
	AgeSec float64  `json:"age,omitempty"`
}

type IngressStage struct {
	PropagationTxPerSec   Reading `json:"propagationTxPerSec"`
	SubtreeIntakePerSec   Reading `json:"subtreeIntakePerSec"`
	Peers                 Reading `json:"peers"`
	RejectedPerSec        Reading `json:"rejectedPerSec"`
	SubtreeQueue          Reading `json:"subtreeQueue"`
	SubtreeQueueOldestSec Reading `json:"subtreeQueueOldestSec"`
}

type ValidatorStage struct {
	ValidatedPerSec Reading `json:"validatedPerSec"`
	LatencyP99Sec   Reading `json:"latencyP99Sec"`
	QueueFill       Reading `json:"queueFill"`
	SpendBatchFill  Reading `json:"spendBatchFill"`
	ErrorsPerSec    Reading `json:"errorsPerSec"`
}

type CacheStage struct {
	HitRate          Reading `json:"hitRate"`
	MissesPerSec     Reading `json:"missesPerSec"`
	EvictionsPerSec  Reading `json:"evictionsPerSec"`
	InsertionsPerSec Reading `json:"insertionsPerSec"`
	Fill             Reading `json:"fill"`
	CurrentGenFrac   Reading `json:"currentGenFrac"`
	PreviousGenFrac  Reading `json:"previousGenFrac"`
	EntriesMillions  Reading `json:"entriesMillions"`
	SizeGB           Reading `json:"sizeGB"`
	CapacityGB       Reading `json:"capacityGB"`
}

type AssemblyStage struct {
	Paused          Reading `json:"paused"`
	TipLagBlocks    Reading `json:"tipLagBlocks"`
	QueueHeadAgeSec Reading `json:"queueHeadAgeSec"`
	Transactions    Reading `json:"transactions"`
	IngestQueueFill Reading `json:"ingestQueueFill"`
	SubtreeFill     Reading `json:"subtreeFill"`
}

type BlobStoreView struct {
	ReadsPerSec  Reading `json:"readsPerSec"`
	WritesPerSec Reading `json:"writesPerSec"`
	ErrorsPerSec Reading `json:"errorsPerSec"`
	ReadP99Sec   Reading `json:"readP99Sec"`
	WriteP99Sec  Reading `json:"writeP99Sec"`
}

type BlobsView struct {
	Subtree BlobStoreView `json:"subtree"`
	Block   BlobStoreView `json:"block"`
}

type TopicLag struct {
	Topic string `json:"topic"`
	Lag   *int64 `json:"lag"`
}

type TxLane struct {
	AlreadySeenPerSec Reading `json:"alreadySeenPerSec"`
}

type SubtreeLane struct {
	AlreadyKnownPerSec Reading `json:"alreadyKnownPerSec"`
	CompletedPerSec    Reading `json:"completedPerSec"`
	MissingTxsPerSec   Reading `json:"missingTxsPerSec"`
	TxsFetchedAvg      Reading `json:"txsFetchedAvg"`
}

type BlockLane struct {
	AnnouncedTotal        Reading `json:"announcedTotal"`
	ValidatedTotal        Reading `json:"validatedTotal"`
	MissingSubtrees       Reading `json:"missingSubtrees"`
	MissingSubtreesPerSec Reading `json:"missingSubtreesPerSec"`
}

type MiningLane struct {
	CandidatesPerSec Reading `json:"candidatesPerSec"`
	CandidateAgeSec  Reading `json:"candidateAgeSec"`
	SolutionsTotal   Reading `json:"solutionsTotal"`
}

type LanesView struct {
	Tx       TxLane      `json:"tx"`
	Subtrees SubtreeLane `json:"subtrees"`
	Blocks   BlockLane   `json:"blocks"`
	Mining   MiningLane  `json:"mining"`
}

type ChainStage struct {
	ForkDepth          Reading  `json:"forkDepth"`
	PersisterBehind    Reading  `json:"persisterBehind"`
	SubscribersDropped Reading  `json:"subscribersDropped"`
	PeerHeight         Reading  `json:"peerHeight"`
	BlocksWaitingCount Reading  `json:"blocksWaitingCount"`
	BlocksWaiting      []uint32 `json:"blocksWaiting"`
}

type StoreStats struct {
	Configured    bool    `json:"configured"`
	DiskFill      Reading `json:"diskFill"`
	SizeGB        Reading `json:"sizeGB"`
	ReadP99Sec    Reading `json:"readP99Sec"`
	ReadsPerSec   Reading `json:"readsPerSec"`
	WritesPerSec  Reading `json:"writesPerSec"`
	CommitsPerSec Reading `json:"commitsPerSec"`
}

type StoresStage struct {
	Aerospike StoreStats `json:"aerospike"`
	Postgres  StoreStats `json:"postgres"`
}

type Flow struct {
	ID    string  `json:"id"`
	From  string  `json:"from"`
	To    string  `json:"to"`
	Label string  `json:"label"`
	Rate  Reading `json:"rate"`
	Lag   *int64  `json:"lag,omitempty"`
}

type Link struct {
	Label string `json:"label"`
	Path  string `json:"path"`
}

type Alarm struct {
	ID           string    `json:"id"`
	Stage        string    `json:"stage"`
	Title        string    `json:"title"`
	Body         string    `json:"body"`
	TextID       string    `json:"textId"`
	Severity     string    `json:"severity"`
	RaisedAt     time.Time `json:"raisedAt"`
	StartOfChain bool      `json:"startOfChain"`
	Snoozed      bool      `json:"snoozed"`
	Drilldowns   []Link    `json:"drilldowns"`
}

type Calibration struct {
	Ready    bool      `json:"ready"`
	Until    time.Time `json:"until"`
	Fraction float64   `json:"fraction"`
}

type SourceHealth struct {
	Name   string  `json:"name"`
	OK     bool    `json:"ok"`
	AgeSec float64 `json:"age,omitempty"`
}

type Snapshot struct {
	Time         time.Time      `json:"time"`
	Node         string         `json:"node"`
	FSM          string         `json:"fsm"`
	Tip          uint32         `json:"tip"`
	PeerTip      uint32         `json:"peerTip"`
	Ingress      IngressStage   `json:"ingress"`
	Validator    ValidatorStage `json:"validator"`
	Cache        CacheStage     `json:"cache"`
	Assembly     AssemblyStage  `json:"assembly"`
	Chain        ChainStage     `json:"chain"`
	Stores       StoresStage    `json:"stores"`
	Lanes        LanesView      `json:"lanes"`
	Blobs        BlobsView      `json:"blobs"`
	Kafka        []TopicLag     `json:"kafka"`
	Flows        []Flow         `json:"flows"`
	Alarms       []Alarm        `json:"alarms"`
	Calibration  Calibration    `json:"calibration"`
	DrainSeconds *float64       `json:"drainSeconds,omitempty"`
	DrainGrowing bool           `json:"drainGrowing"`
	Sources      []SourceHealth `json:"sources"`
}

type Patch func(*Snapshot)

var readingType = reflect.TypeOf(Reading{})

func NewSnapshot(node string) Snapshot {
	flows := []struct{ id, from, to string }{
		{FlowTxInput, "ingress", "validator"},
		{FlowSubtrees, "ingress", "assembly"},
		{FlowTxMeta, "validator", "cache"},
		{FlowLookups, "cache", "stores"},
		{FlowValidated, "validator", "assembly"},
		{FlowBlocks, "assembly", "chain"},
		{FlowBlocksFinal, "chain", "stores"},
		{FlowUTXOOps, "validator", "stores"},
		{FlowBlockWrites, "chain", "stores"},
	}

	s := Snapshot{Node: node, Flows: make([]Flow, len(flows)), Sources: []SourceHealth{}, Kafka: []TopicLag{}}

	for i, f := range flows {
		s.Flows[i] = Flow{ID: f.id, From: f.from, To: f.to, Label: f.id, Rate: Reading{Status: StatusNoData}}
	}

	return s
}

const flowsKey = "flows"

func (s *Snapshot) Lookup(key string) (*Reading, bool) {
	v := reflect.ValueOf(s).Elem()
	names := strings.Split(key, ".")

	if names[0] == flowsKey {
		if len(names) < 3 {
			return nil, false
		}

		f := s.Flow(names[1])
		if f == nil {
			return nil, false
		}

		v = reflect.ValueOf(f).Elem()
		names = names[2:]
	}

	for _, name := range names {
		if v.Kind() != reflect.Struct || v.Type() == readingType {
			return nil, false
		}

		next, found := fieldByJSONName(v, name)
		if !found {
			return nil, false
		}

		v = next
	}

	if v.Type() != readingType {
		return nil, false
	}

	return v.Addr().Interface().(*Reading), true
}

func (s *Snapshot) EachReading(fn func(key string, r *Reading)) {
	eachReading(reflect.ValueOf(s).Elem(), "", fn)

	for i := range s.Flows {
		eachReading(reflect.ValueOf(&s.Flows[i]).Elem(), flowsKey+"."+s.Flows[i].ID+".", fn)
	}
}

func eachReading(v reflect.Value, prefix string, fn func(key string, r *Reading)) {
	t := v.Type()

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if !field.IsExported() {
			continue
		}

		name := strings.Split(field.Tag.Get("json"), ",")[0]
		if name == "" || name == "-" {
			continue
		}

		fv := v.Field(i)

		switch {
		case field.Type == readingType:
			fn(prefix+name, fv.Addr().Interface().(*Reading))
		case field.Type.Kind() == reflect.Struct:
			eachReading(fv, prefix+name+".", fn)
		}
	}
}

func fieldByJSONName(v reflect.Value, name string) (reflect.Value, bool) {
	t := v.Type()

	for i := 0; i < t.NumField(); i++ {
		if strings.Split(t.Field(i).Tag.Get("json"), ",")[0] == name {
			return v.Field(i), true
		}
	}

	return reflect.Value{}, false
}

func (s *Snapshot) Flow(id string) *Flow {
	for i := range s.Flows {
		if s.Flows[i].ID == id {
			return &s.Flows[i]
		}
	}

	return nil
}
