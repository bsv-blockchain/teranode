# Policy Settings

**Related Topics**: [Network Consensus Rules](../networkConsensusRules.md), [Transaction Validation](../../topics/services/validator.md)

Policy settings control BSV Blockchain consensus rules and transaction validation behavior in Teranode. These settings determine what transactions and blocks are considered valid on the BSV Blockchain according to the Bitcoin Protocol.

## Configuration Settings

### Block Size Limits

| Setting | Type | Default | Environment Variable | Usage |
| --------- | ------ | --------- | --------------------- | ------- |
| BlockMaxSize | int | 0 (unlimited) | blockmaxsize | **CRITICAL** - Maximum block size policy limit |
| ExcessiveBlockSize | int | 4294967296 (4GB) | excessiveblocksize | Excessive block size threshold |

### Transaction Size and Script Limits

| Setting | Type | Default | Environment Variable | Usage |
| --------- | ------ | --------- | --------------------- | ------- |
| MaxTxSizePolicy | int | 10485760 (10MB) | maxtxsizepolicy | **CRITICAL** - Maximum transaction size policy |
| MaxOrphanTxSize | int | 1000000 (1MB) | maxorphantxsize | Maximum orphan transaction size |
| MaxScriptSizePolicy | int | 100000000 (100MB) | maxscriptsizepolicy | **CRITICAL** - Maximum script size policy |
| MaxScriptNumLengthPolicy | int | 10000 | maxscriptnumlengthpolicy | Maximum script number length |
| MaxOpsPerScriptPolicy | int64 | 1000000 | maxopsperscriptpolicy | Maximum operations per script |

### Multisig and Signature Limits

| Setting | Type | Default | Environment Variable | Usage |
| --------- | ------ | --------- | --------------------- | ------- |
| MaxPubKeysPerMultisigPolicy | int64 | 0 (unlimited) | maxpubkeyspermultisigpolicy | Maximum public keys per multisig |
| MaxTxSigopsCountsPolicy | int64 | 0 (unlimited) | maxtxsigopscountspolicy | Maximum signature operations per transaction |

### Memory and Stack Limits

| Setting | Type | Default | Environment Variable | Usage |
| --------- | ------ | --------- | --------------------- | ------- |
| MaxStackMemoryUsagePolicy | int | 104857600 (100MB) | maxstackmemoryusagepolicy | **CRITICAL** - Maximum stack memory usage (policy) |
| MaxStackMemoryUsageConsensus | int | 0 (unlimited) | maxstackmemoryusageconsensus | **CRITICAL** - Maximum stack memory usage (consensus) |

### Validation Timeout Settings

| Setting | Type | Default | Environment Variable | Usage |
| --------- | ------ | --------- | --------------------- | ------- |
| MaxStdTxValidationDuration | int | 3 | maxstdtxvalidationduration | Maximum validation time for standard transactions (ms) |
| MaxNonStdTxValidationDuration | int | 1000 | maxnonstdtxvalidationduration | Maximum validation time for non-standard transactions (ms) |
| MaxTxChainValidationBudget | int | 50 | maxtxchainvalidationbudget | Total time budget for chain validation (ms) |
| ValidationClockCPU | bool | false | validationclockcpu | Use CPU time instead of wall-clock for validation timeouts |

### Data Carrier Settings

| Setting | Type | Default | Environment Variable | Usage |
| --------- | ------ | --------- | --------------------- | ------- |
| DataCarrier | bool | false | datacarrier | Enable relaying of OP_RETURN data carrier transactions |
| DataCarrierSize | int64 | 1000000 (1MB) | datacarriersize | Maximum OP_RETURN data size when DataCarrier is enabled |

### Transaction Chain Limits

| Setting | Type | Default | Environment Variable | Usage |
| --------- | ------ | --------- | --------------------- | ------- |
| LimitAncestorCount | int | 1000000 | limitancestorcount | Maximum unconfirmed ancestor count in mempool |
| LimitCPFPGroupMembersCount | int | 1000000 | limitcpfpgroupmemberscount | Maximum CPFP group members |

### Mining and Fee Settings

| Setting | Type | Default | Environment Variable | Usage |
| --------- | ------ | --------- | --------------------- | ------- |
| MinMiningTxFee | float64 | 0.00000500 | minminingtxfee | Minimum transaction fee for mining |
| MinMiningTxFeeByScriptSize | []FeeTier | (empty) | minminingtxfeebyscriptsize | Optional marginal fee tiers for large executed scripts |
| MinMiningTxFeeByScriptOps | []FeeTier | (empty) | minminingtxfeebyscriptops | Optional marginal fee tiers for op-dense executed scripts |
| AcceptNonStdOutputs | bool | true | acceptnonstdoutputs | **CRITICAL** - Accept non-standard output scripts |

The two fee-tier settings price the metrics their policy caps gate: `minminingtxfeebyscriptsize` pairs with `maxscriptsizepolicy` (script bytes) and `minminingtxfeebyscriptops` pairs with `maxopsperscriptpolicy` (counted ops: every opcode above OP_16, pushes free). Each is a pipe-separated list of `<threshold>:<satoshisPerK>` pairs (for example `500000:10`), applied marginally per executed script, like tax brackets: units below the first threshold cost nothing extra, units beyond each threshold must pay at least that tier's rate per 1000 units. Executed scripts are each input's unlocking script, the locking script it spends, and a legacy P2SH redeem script (only for coins created before Genesis, matching BDK's era gate); a transaction's own output scripts are priced when they are later spent. The surcharges add on top of the `minminingtxfee` floor, which BDK keeps enforcing unchanged. Empty (the default) disables a setting, leaving fee policy exactly as before.

The counted-ops metric matches svnode's executed op count for every script that reaches BDK. That includes the key count svnode charges on top of `OP_CHECKMULTISIG` and `OP_CHECKMULTISIGVERIFY`, which is added wherever it is statically certain: the opcode must be reached with execution still active (at IF-depth zero, and before any `OP_RETURN` nested in a conditional, which stops svnode executing while it keeps counting), and the key count must come from the literal push immediately before it. That covers a bare n-of-m multisig lock, the densest shape this setting exists to price, which would otherwise be charged for a single operation.

Where the key count is not statically certain, inside a conditional or computed at runtime, the opcode counts as one. That under-counts, which is the containment direction: the metric can only ever charge less than svnode, never reject a script svnode would accept cheaply.

A script whose count exceeds `maxopsperscriptpolicy` (or whose size exceeds `maxscriptsizepolicy`) is left unpriced, so BDK's own cap rejection is reported rather than a misleading insufficient-fee error.

The free-consolidation exemption is honoured for the `minminingtxfee` floor only; the per-script surcharge is always due. The exemption exists to encourage cleanup of many small UTXOs, and a genuine such consolidation never triggers a surcharge (its scripts are far below any threshold), so a large-script output cannot be created cheaply and then "consolidated" to escape the surcharge.

Neither schedule is advertised on `/v1/policy` (the ARC schema is closed to additional properties) or the P2P `fee_policy` message, so wallets and propagation peers cannot discover the rule: a node can reject a transaction for a fee schedule its peers cannot see. An operator enabling this must communicate the schedule out of band; extending the Teranode-owned P2P policy message is a planned follow-up.

### Consolidation Transaction Settings

| Setting | Type | Default | Environment Variable | Usage |
| --------- | ------ | --------- | --------------------- | ------- |
| MinConsolidationFactor | int | 20 | minconsolidationfactor | Minimum consolidation factor |
| MaxConsolidationInputScriptSize | int | 150 | maxconsolidationinputscriptsize | Maximum input script size for consolidation |
| MinConfConsolidationInput | int | 6 | minconfconsolidationinput | Minimum confirmations for consolidation input |
| MinConsolidationInputMaturity | int | 6 | minconsolidationinputmaturity | Minimum input maturity for consolidation |
| AcceptNonStdConsolidationInput | bool | false | acceptnonstdconsolidationinput | Accept non-standard consolidation inputs |

## Configuration Dependencies

### Block Size Policy

- `BlockMaxSize = 0` means unlimited block size (default behavior for BSV)
- `ExcessiveBlockSize` defines the threshold for considering blocks "excessive"
- Both settings work together to enforce BSV Blockchain's unbounded block size philosophy

### Script Validation

- `MaxScriptSizePolicy` controls script size limits during validation
- `MaxScriptNumLengthPolicy` limits the length of script numbers
- `MaxStackMemoryUsagePolicy` vs `MaxStackMemoryUsageConsensus`:

    - Policy: Enforced during transaction validation
    - Consensus: Enforced during block validation
    - Setting consensus to 0 (unlimited) aligns with BSV's restored protocol

### Multisig and Signature Operations

- `MaxPubKeysPerMultisigPolicy = 0` means unlimited public keys (BSV default)
- `MaxTxSigopsCountsPolicy = 0` means unlimited signature operations (BSV default)
- These unlimited defaults reflect BSV Blockchain's restoration of original Bitcoin capabilities

### Non-Standard Transactions

- `AcceptNonStdOutputs = true` enables acceptance of non-standard output scripts
- Required for many BSV applications that use custom script templates
- Aligns with BSV's philosophy of not restricting valid script types

### Consolidation Transactions

- Consolidation transactions allow efficient UTXO management
- `MinConsolidationFactor` requires minimum input-to-output ratio
- `MinConfConsolidationInput` and `MinConsolidationInputMaturity` prevent immature UTXO consolidation
- `MaxConsolidationInputScriptSize` limits input script complexity
- `AcceptNonStdConsolidationInput` controls whether non-standard inputs can be consolidated

## BSV Blockchain Specifics

### Restored Protocol Features

BSV Blockchain restores the original Bitcoin protocol, which is reflected in these policy settings:

1. **Unlimited Block Size**: `BlockMaxSize = 0` (unlimited)
2. **Unlimited Script Capabilities**: Most limits set to 0 (unlimited)
3. **Original Opcodes**: All original Bitcoin opcodes restored and functional
4. **No Artificial Restrictions**: Non-standard transactions accepted by default

### Policy vs Consensus

Teranode distinguishes between policy and consensus rules:

- **Policy Rules**: Local node preferences (can be more restrictive)
- **Consensus Rules**: Network-wide agreement (must match the BSV Blockchain protocol)

The settings allow operators to configure policy rules while maintaining consensus compatibility.

## Validation Rules

| Setting | Validation | Impact |
| --------- | ------------ | -------- |
| BlockMaxSize | 0 means unlimited | Block acceptance criteria |
| MaxTxSizePolicy | Must be positive or 0 | Transaction size validation |
| MaxStackMemoryUsagePolicy | Policy enforcement | Script execution limits |
| MaxStackMemoryUsageConsensus | Consensus enforcement | Block validation limits |
| MinMiningTxFee | Minimum fee threshold | Mining inclusion criteria |
| MinMiningTxFeeByScriptSize | Sorted, unique thresholds; non-decreasing rates | Marginal fee requirement for large scripts |
| MinMiningTxFeeByScriptOps | Sorted, unique thresholds; non-decreasing rates | Marginal fee requirement for op-dense scripts |

## Configuration Examples

### Default BSV Configuration

```text
blockmaxsize = 0
excessiveblocksize = 4294967296
maxtxsizepolicy = 10485760
maxscriptsizepolicy = 100000000
maxpubkeyspermultisigpolicy = 0
maxtxsigopscountspolicy = 0
maxstackmemoryusagepolicy = 104857600
maxstackmemoryusageconsensus = 0
acceptnonstdoutputs = true
```

### Conservative Policy Configuration

```text
blockmaxsize = 2147483648
maxtxsizepolicy = 5242880
maxscriptsizepolicy = 250000
maxstackmemoryusagepolicy = 52428800
acceptnonstdoutputs = false
```

### Mining-Focused Configuration

```text
minminingtxfee = 0.00001000
acceptnonstdoutputs = true
minconsolidationfactor = 30
```

## Related Documentation

- [Transaction Validation](../../topics/services/validator.md)
