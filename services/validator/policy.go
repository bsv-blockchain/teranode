/*
Package validator implements Bitcoin SV transaction validation functionality.

This package provides comprehensive transaction validation for Bitcoin SV nodes,
including script verification, UTXO management, and policy enforcement. It supports
multiple script interpreters (GoBT, GoSDK, GoBDK) and implements the full Bitcoin
transaction validation ruleset.

Key features:
  - Transaction validation against Bitcoin consensus rules
  - UTXO spending and creation
  - Script verification using multiple interpreters
  - Policy enforcement
  - Block assembly integration
  - Kafka integration for transaction metadata

Usage:

	validator := NewTxValidator(logger, policy, params)
	err := validator.ValidateTransaction(tx, blockHeight)
*/
package validator

// PolicySettings defines the validation and consensus policy parameters for Bitcoin transactions and blocks
type PolicySettings struct {
	// ExcessiveBlockSize defines the maximum block size allowed by the node
	// Blocks larger than this size will be rejected
	ExcessiveBlockSize int `json:"excessiveblocksize"`

	// BlockMaxSize defines the maximum size of blocks that this node will mine
	// Must be less than or equal to ExcessiveBlockSize
	BlockMaxSize int `json:"blockmaxsize"`

	// MaxTxSizePolicy defines the maximum allowed size for a single transaction
	// Transactions larger than this size will be rejected by policy
	MaxTxSizePolicy int `json:"maxtxsizepolicy"`

	// MaxOrphanTxSize defines the maximum size for transactions held in the orphan pool
	MaxOrphanTxSize int `json:"maxorphantxsize"`

	// DataCarrierSize defines the maximum size of OP_RETURN data carrier outputs
	DataCarrierSize int64 `json:"datacarriersize"`

	// MaxScriptSizePolicy defines the maximum allowed script size in bytes
	MaxScriptSizePolicy int `json:"maxscriptsizepolicy"`

	// MaxOpsPerScriptPolicy defines the maximum number of operations allowed in a script
	MaxOpsPerScriptPolicy int64 `json:"maxopsperscriptpolicy"`

	// MaxScriptNumLengthPolicy defines the maximum length for numeric operands in scripts
	MaxScriptNumLengthPolicy int `json:"maxscriptnumlengthpolicy"`

	// MaxPubKeysPerMultisigPolicy defines the maximum number of public keys allowed in a multi-signature script
	MaxPubKeysPerMultisigPolicy int64 `json:"maxpubkeyspermultisigpolicy"`

	// MaxTxSigopsCountsPolicy defines the maximum number of signature operations allowed in a transaction
	MaxTxSigopsCountsPolicy int64 `json:"maxtxsigopscountspolicy"`

	// MaxStackMemoryUsagePolicy defines the maximum memory usage allowed for script execution stack
	MaxStackMemoryUsagePolicy int `json:"maxstackmemoryusagepolicy"`

	// MaxStackMemoryUsageConsensus defines the consensus limit for script stack memory usage
	MaxStackMemoryUsageConsensus int `json:"maxstackmemoryusageconsensus"`

	// LimitAncestorCount defines the maximum number of ancestor transactions allowed
	LimitAncestorCount int `json:"limitancestorcount"`

	// LimitCPFPGroupMembersCount defines the maximum number of transactions in a CPFP group
	LimitCPFPGroupMembersCount int `json:"limitcpfpgroupmemberscount"`

	// MaxMempool defines the maximum size of the mempool in bytes
	MaxMempool int `json:"maxmempool"`

	// MaxMempoolSizedisk defines the maximum size of the mempool on disk
	MaxMempoolSizedisk int `json:"maxmempoolsizedisk"`

	// MempoolMaxPercentCPFP defines the maximum percentage of mempool that can be used for CPFP
	MempoolMaxPercentCPFP int `json:"mempoolmaxpercentcpfp"`

	// AcceptNonStdOutputs determines if non-standard outputs are accepted
	AcceptNonStdOutputs bool `json:"acceptnonstdoutputs"`

	// DataCarrier determines if OP_RETURN outputs are allowed
	DataCarrier bool `json:"datacarrier"`

	// MinMiningTxFee defines the minimum fee rate for transaction mining
	MinMiningTxFee float64 `json:"minminingtxfee"`

	// MaxStdTxValidationDuration defines the maximum time allowed for standard transaction validation
	MaxStdTxValidationDuration int `json:"maxstdtxvalidationduration"`

	// MaxNonStdTxValidationDuration defines the maximum time allowed for non-standard transaction validation
	MaxNonStdTxValidationDuration int `json:"maxnonstdtxvalidationduration"`

	// MaxTxChainValidationBudget defines the maximum time budget for validating a chain of transactions
	MaxTxChainValidationBudget int `json:"maxtxchainvalidationbudget"`

	// ValidationClockCPU determines if CPU time should be used for validation timing
	ValidationClockCPU bool `json:"validationclockcpu"`

	// MinConsolidationFactor defines the minimum consolidation factor for transactions
	MinConsolidationFactor int `json:"minconsolidationfactor"`

	// MaxConsolidationInputScriptSize defines the maximum script size for consolidation inputs
	MaxConsolidationInputScriptSize int `json:"maxconsolidationinputscriptsize"`

	// MinConfConsolidationInput defines the minimum confirmations required for consolidation inputs
	MinConfConsolidationInput int `json:"minconfconsolidationinput"`

	// MinConsolidationInputMaturity defines the minimum maturity for consolidation inputs
	MinConsolidationInputMaturity int `json:"minconsolidationinputmaturity"`

	// AcceptNonStdConsolidationInput determines if non-standard inputs are accepted in consolidation
	AcceptNonStdConsolidationInput bool `json:"acceptnonstdconsolidationinput"`
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

func (ps *PolicySettings) SetValidationClockCPU(use bool) {
	ps.ValidationClockCPU = use
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

func (ps *PolicySettings) GetValidationClockCPU() bool {
	return ps.ValidationClockCPU
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
