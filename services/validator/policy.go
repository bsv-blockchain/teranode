package validator

type PolicySettings struct {
	ExcessiveBlockSize              int     `json:"excessiveblocksize"`
	BlockMaxSize                    int     `json:"blockmaxsize"`
	MaxTxSizePolicy                 int     `json:"maxtxsizepolicy"`
	MaxOrphanTxSize                 int     `json:"maxorphantxsize"`
	DataCarrierSize                 int64   `json:"datacarriersize"`
	MaxScriptSizePolicy             int     `json:"maxscriptsizepolicy"`
	MaxOpsPerScriptPolicy           int64   `json:"maxopsperscriptpolicy"`
	MaxScriptNumLengthPolicy        int     `json:"maxscriptnumlengthpolicy"`
	MaxPubKeysPerMultisigPolicy     int64   `json:"maxpubkeyspermultisigpolicy"`
	MaxTxSigopsCountsPolicy         int64   `json:"maxtxsigopscountspolicy"`
	MaxStackMemoryUsagePolicy       int     `json:"maxstackmemoryusagepolicy"`
	MaxStackMemoryUsageConsensus    int     `json:"maxstackmemoryusageconsensus"`
	LimitAncestorCount              int     `json:"limitancestorcount"`
	LimitCPFPGroupMembersCount      int     `json:"limitcpfpgroupmemberscount"`
	MaxMempool                      int     `json:"maxmempool"`
	MaxMempoolSizedisk              int     `json:"maxmempoolsizedisk"`
	MempoolMaxPercentCPFP           int     `json:"mempoolmaxpercentcpfp"`
	AcceptNonStdOutputs             bool    `json:"acceptnonstdoutputs"`
	DataCarrier                     bool    `json:"datacarrier"`
	MinMiningTxFee                  float64 `json:"minminingtxfee"`
	MaxStdTxValidationDuration      int     `json:"maxstdtxvalidationduration"`
	MaxNonStdTxValidationDuration   int     `json:"maxnonstdtxvalidationduration"`
	MaxTxChainValidationBudget      int     `json:"maxtxchainvalidationbudget"`
	ValidationClockCpu              bool    `json:"validationclockcpu"`
	MinConsolidationFactor          int     `json:"minconsolidationfactor"`
	MaxConsolidationInputScriptSize int     `json:"maxconsolidationinputscriptsize"`
	MinConfConsolidationInput       int     `json:"minconfconsolidationinput"`
	MinConsolidationInputMaturity   int     `json:"minconsolidationinputmaturity"`
	AcceptNonStdConsolidationInput  bool    `json:"acceptnonstdconsolidationinput"`
}

func NewPolicySettings() *PolicySettings {
	return &PolicySettings{
		// TODO set defaults
	}
}

func (ps *PolicySettings) SetExcessiveBlockSize(size int) {
	ps.ExcessiveBlockSize = size
}

func (ps *PolicySettings) SetBlockMaxSize(size int) {
	ps.BlockMaxSize = size
}

func (ps *PolicySettings) SetMaxTxSizePolicy(size int) {
	ps.MaxTxSizePolicy = size
}

func (ps *PolicySettings) SetMaxOrphanTxSize(size int) {
	ps.MaxOrphanTxSize = size
}

func (ps *PolicySettings) SetDataCarrierSize(size int64) {
	ps.DataCarrierSize = size
}

func (ps *PolicySettings) SetMaxScriptSizePolicy(size int) {
	ps.MaxScriptSizePolicy = size
}

func (ps *PolicySettings) SetMaxOpsPerScriptPolicy(size int64) {
	ps.MaxOpsPerScriptPolicy = size
}

func (ps *PolicySettings) SetMaxScriptNumLengthPolicy(size int) {
	ps.MaxScriptNumLengthPolicy = size
}

func (ps *PolicySettings) SetMaxPubKeysPerMultisigPolicy(size int64) {
	ps.MaxPubKeysPerMultisigPolicy = size
}

func (ps *PolicySettings) SetMaxTxSigopsCountsPolicy(size int64) {
	ps.MaxTxSigopsCountsPolicy = size
}

func (ps *PolicySettings) SetMaxStackMemoryUsagePolicy(size int) {
	ps.MaxStackMemoryUsagePolicy = size
}

func (ps *PolicySettings) SetMaxStackMemoryUsageConsensus(size int) {
	ps.MaxStackMemoryUsageConsensus = size
}

func (ps *PolicySettings) SetLimitAncestorCount(size int) {
	ps.LimitAncestorCount = size
}

func (ps *PolicySettings) SetLimitCPFPGroupMembersCount(size int) {
	ps.LimitCPFPGroupMembersCount = size
}

func (ps *PolicySettings) SetMaxMempool(size int) {
	ps.MaxMempool = size
}

func (ps *PolicySettings) SetMaxMempoolSizedisk(size int) {
	ps.MaxMempoolSizedisk = size
}

func (ps *PolicySettings) SetMempoolMaxPercentCPFP(size int) {
	ps.MempoolMaxPercentCPFP = size
}

func (ps *PolicySettings) SetAcceptNonStdOutputs(accept bool) {
	ps.AcceptNonStdOutputs = accept
}

func (ps *PolicySettings) SetDataCarrier(accept bool) {
	ps.DataCarrier = accept
}

func (ps *PolicySettings) SetMinMiningTxFee(fee float64) {
	ps.MinMiningTxFee = fee
}

func (ps *PolicySettings) SetMaxStdTxValidationDuration(duration int) {
	ps.MaxStdTxValidationDuration = duration
}

func (ps *PolicySettings) SetMaxNonStdTxValidationDuration(duration int) {
	ps.MaxNonStdTxValidationDuration = duration
}

func (ps *PolicySettings) SetMaxTxChainValidationBudget(budget int) {
	ps.MaxTxChainValidationBudget = budget
}

func (ps *PolicySettings) SetValidationClockCpu(use bool) {
	ps.ValidationClockCpu = use
}

func (ps *PolicySettings) SetMinConsolidationFactor(factor int) {
	ps.MinConsolidationFactor = factor
}

func (ps *PolicySettings) SetMaxConsolidationInputScriptSize(size int) {
	ps.MaxConsolidationInputScriptSize = size
}

func (ps *PolicySettings) SetMinConfConsolidationInput(conf int) {
	ps.MinConfConsolidationInput = conf
}

func (ps *PolicySettings) SetMinConsolidationInputMaturity(maturity int) {
	ps.MinConsolidationInputMaturity = maturity
}

func (ps *PolicySettings) SetAcceptNonStdConsolidationInput(accept bool) {
	ps.AcceptNonStdConsolidationInput = accept
}

func (ps *PolicySettings) GetExcessiveBlockSize() int {
	return ps.ExcessiveBlockSize
}

func (ps *PolicySettings) GetBlockMaxSize() int {
	return ps.BlockMaxSize
}

func (ps *PolicySettings) GetMaxTxSizePolicy() int {
	return ps.MaxTxSizePolicy
}

func (ps *PolicySettings) GetMaxOrphanTxSize() int {
	return ps.MaxOrphanTxSize
}

func (ps *PolicySettings) GetDataCarrierSize() int64 {
	return ps.DataCarrierSize
}

func (ps *PolicySettings) GetMaxScriptSizePolicy() int {
	return ps.MaxScriptSizePolicy
}

func (ps *PolicySettings) GetMaxOpsPerScriptPolicy() int64 {
	return ps.MaxOpsPerScriptPolicy
}

func (ps *PolicySettings) GetMaxScriptNumLengthPolicy() int {
	return ps.MaxScriptNumLengthPolicy
}

func (ps *PolicySettings) GetMaxPubKeysPerMultisigPolicy() int64 {
	return ps.MaxPubKeysPerMultisigPolicy
}

func (ps *PolicySettings) GetMaxTxSigopsCountsPolicy() int64 {
	return ps.MaxTxSigopsCountsPolicy
}

func (ps *PolicySettings) GetMaxStackMemoryUsagePolicy() int {
	return ps.MaxStackMemoryUsagePolicy
}

func (ps *PolicySettings) GetMaxStackMemoryUsageConsensus() int {
	return ps.MaxStackMemoryUsageConsensus
}

func (ps *PolicySettings) GetLimitAncestorCount() int {
	return ps.LimitAncestorCount
}

func (ps *PolicySettings) GetLimitCPFPGroupMembersCount() int {
	return ps.LimitCPFPGroupMembersCount
}

func (ps *PolicySettings) GetMaxMempool() int {
	return ps.MaxMempool
}

func (ps *PolicySettings) GetMaxMempoolSizedisk() int {
	return ps.MaxMempoolSizedisk
}

func (ps *PolicySettings) GetMempoolMaxPercentCPFP() int {
	return ps.MempoolMaxPercentCPFP
}

func (ps *PolicySettings) GetAcceptNonStdOutputs() bool {
	return ps.AcceptNonStdOutputs
}

func (ps *PolicySettings) GetDataCarrier() bool {
	return ps.DataCarrier
}

func (ps *PolicySettings) GetMinMiningTxFee() float64 {
	return ps.MinMiningTxFee
}

func (ps *PolicySettings) GetMaxStdTxValidationDuration() int {
	return ps.MaxStdTxValidationDuration
}

func (ps *PolicySettings) GetMaxNonStdTxValidationDuration() int {
	return ps.MaxNonStdTxValidationDuration
}

func (ps *PolicySettings) GetMaxTxChainValidationBudget() int {
	return ps.MaxTxChainValidationBudget
}

func (ps *PolicySettings) GetValidationClockCpu() bool {
	return ps.ValidationClockCpu
}

func (ps *PolicySettings) GetMinConsolidationFactor() int {
	return ps.MinConsolidationFactor
}

func (ps *PolicySettings) GetMaxConsolidationInputScriptSize() int {
	return ps.MaxConsolidationInputScriptSize
}

func (ps *PolicySettings) GetMinConfConsolidationInput() int {
	return ps.MinConfConsolidationInput
}

func (ps *PolicySettings) GetMinConsolidationInputMaturity() int {
	return ps.MinConsolidationInputMaturity
}

func (ps *PolicySettings) GetAcceptNonStdConsolidationInput() bool {
	return ps.AcceptNonStdConsolidationInput
}
