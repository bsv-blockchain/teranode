package settings

type PolicySettings struct {
	ExcessiveBlockSize              int     `json:"excessiveblocksize" key:"excessiveblocksize" desc:"Maximum block size to accept (4GB default)" default:"4294967296" category:"Policy" usage:"Blocks larger than this are rejected" type:"int" longdesc:"### Purpose\nDefines the maximum block size this node will accept. This is a POLICY setting, not a consensus rule.\n\n### How It Works\n- Blocks exceeding this threshold are rejected even if otherwise valid\n- Enforced in BlockValidation.ValidateBlock() before any transaction processing\n- Blocks from other miners already proven valid by PoW may still be rejected if they exceed this nodes threshold\n- Different nodes can have different limits without breaking consensus\n\n### Values\n- **4294967296** (default) - 4GB, supports BSVs large block scaling philosophy\n- **0** - Disables the limit entirely\n\n### Trade-offs\n| Setting | Benefit | Drawback |\n|---------|---------|----------|\n| Higher | Accept larger blocks | More resource usage, potential DoS risk |\n| Lower | Resource protection | May reject valid blocks from network |\n\n### Recommendations\n- Use default 4GB for standard BSV operation\n- Set based on node hardware capacity and network conditions\n- Teranode is designed for unbounded blocks where economic limits determine actual block sizes"`
	BlockMaxSize                    int     `json:"blockmaxsize" key:"blockmaxsize" desc:"Maximum block size to create when mining" default:"0" category:"Policy" usage:"0 means unlimited (use excessiveblocksize)" type:"int" longdesc:"### Purpose\nControls the maximum size of blocks this node will CREATE when mining. Distinct from ExcessiveBlockSize which controls acceptance.\n\n### How It Works\n- Used by Block Assembly service to limit block template size\n- Miners optimize based on their infrastructure capacity\n- Does not affect what blocks the node accepts from others\n\n### Values\n- **0** (default) - Unlimited, miners can create blocks of any size\n- **N > 0** - Fixed maximum block size in bytes\n\n### Trade-offs\n| Setting | Benefit | Drawback |\n|---------|---------|----------|\n| Larger/Unlimited | More transaction fees per block | Slower propagation, higher orphan risk |\n| Smaller | Faster propagation, lower orphan risk | Fewer fees, less throughput |\n\n### Recommendations\n- Use 0 (unlimited) and let economic incentives balance block size\n- Self-impose limits based on network conditions and propagation capacity\n- Consider your infrastructure capacity when setting explicit limits"`
	MaxTxSizePolicy                 int     `json:"maxtxsizepolicy" key:"maxtxsizepolicy" desc:"Maximum transaction size (10MB default)" default:"10485760" category:"Policy" usage:"Reject transactions larger than this" type:"int" longdesc:"### Purpose\nMaximum transaction size accepted for relay and mempool inclusion. This is a POLICY limit only, not consensus - BSV supports unbounded transaction sizes at the protocol level.\n\n### How It Works\n- Enforced in TxValidator.checkTxSize(), called only when validationOptions.SkipPolicyChecks is false\n- When validating transactions in blocks mined by others (SkipPolicyChecks=true), this limit is bypassed\n- Blocks proven valid by PoW must be accepted for consensus regardless of this setting\n\n### Values\n- **10485760** (default) - 10MB, enables large data storage transactions\n- **0** - Unlimited acceptance\n\n### Trade-offs\n| Setting | Benefit | Drawback |\n|---------|---------|----------|\n| Higher | Support larger data transactions | More mempool resource usage |\n| Lower | Better mempool protection | May reject valid BSV use cases |\n\n### Recommendations\n- Use default 10MB for standard BSV operation with data carrier support\n- Increase for specialized nodes handling large data transactions\n- Protects mempool from spam while allowing BSVs data storage use cases"`
	MaxOrphanTxSize                 int     `json:"maxorphantxsize" key:"maxorphantxsize" desc:"Maximum orphan transaction size" default:"1000000" category:"Policy" usage:"Orphan transactions larger than this are rejected" type:"int" longdesc:"### Purpose\nMaximum size for orphan transactions (transactions whose parent transactions are not yet known to this node). These are held in a temporary pool until parents arrive.\n\n### How It Works\n- Orphan transactions are held until their parent transactions arrive\n- Smaller than MaxTxSizePolicy (1MB vs 10MB) to conserve memory\n- Used in transaction pool management to limit orphan cache size\n- Essential for Child-Pays-For-Parent (CPFP) patterns where child may arrive before parent\n\n### Values\n- **1000000** (default) - 1MB, supports reasonable transaction chains\n- Smaller values provide stronger DoS protection\n\n### Trade-offs\n| Setting | Benefit | Drawback |\n|---------|---------|----------|\n| Higher | Accept larger orphan chains | More memory usage, DoS risk |\n| Lower | Better memory protection | May reject valid CPFP patterns |\n\n### Recommendations\n- Use default 1MB for balanced operation\n- Orphan pools are prime attack vectors - keep limits conservative\n- Balances responsiveness with resource protection"`
	DataCarrierSize                 int64   `json:"datacarriersize" key:"datacarriersize" desc:"Maximum data carrier size" default:"1000000" category:"Policy" usage:"Maximum size for OP_RETURN data" type:"int64" longdesc:"### Purpose\nMaximum size for OP_RETURN data carrier outputs when the DataCarrier setting is enabled. OP_RETURN outputs are provably unspendable and used to store arbitrary data on-chain.\n\n### How It Works\n- Applies only when DataCarrier setting is enabled\n- Used in legacy script validation code\n- BSV generally doesnt artificially limit OP_RETURN size at protocol level\n\n### Values\n- **1000000** (default) - 1MB, vastly more generous than BTC (80 bytes) or BCH (220 bytes)\n- Higher values for specialized data storage nodes\n\n### Use Cases\n- Document timestamping\n- Supply chain tracking\n- On-chain data storage\n- Certificate verification\n\n### Recommendations\n- Use default 1MB for standard BSV operation\n- Increase for specialized data storage nodes\n- Reflects BSVs philosophy that blockchain is a global data ledger, not just a payment system"`
	MaxScriptSizePolicy             int     `json:"maxscriptsizepolicy" key:"maxscriptsizepolicy" desc:"Maximum script size (500KB default)" default:"500000" category:"Policy" usage:"Reject scripts larger than this" type:"int" longdesc:"### Purpose\nMaximum size of a locking script (scriptPubKey) or unlocking script (scriptSig) during validation. BSV removed script size limits at consensus level - this is a POLICY limit to prevent resource exhaustion.\n\n### How It Works\n- Passed to BDK script engine via SetMaxScriptSizePolicy() during initialization\n- Enforced during script execution in ScriptVerifierGoBDK\n- Much more permissive than BTCs 10KB limit\n\n### Values\n- **500000** (default) - 500KB, allows sophisticated smart contracts\n- **0** - Unlimited (for specialized nodes handling complex scripts)\n- Higher values for advanced scripting use cases\n\n### Trade-offs\n| Setting | Benefit | Drawback |\n|---------|---------|----------|\n| Higher | Support complex smart contracts | More validation resource usage |\n| Lower | Better DoS protection | May reject valid BSV scripts |\n\n### Recommendations\n- Use default 500KB for standard operation\n- Critical for BSVs restored opcodes functionality\n- Enables tokens, complex contracts, and data processing use cases"`
	MaxOpsPerScriptPolicy           int64   `json:"maxopsperscriptpolicy" key:"maxopsperscriptpolicy" desc:"Maximum operations per script" default:"1000000" category:"Policy" usage:"Limits script complexity" type:"int64" longdesc:"### Purpose\nMaximum number of opcodes that can execute in a script. Original Bitcoin had 201 op limit which was removed in BSV to restore full scripting capability.\n\n### How It Works\n- Set via BDK script engine SetMaxOpsPerScriptPolicy() and enforced during script execution\n- Op counting includes all executed operations, not just total opcodes in script\n- Loops count each iteration toward the limit\n- Economic limits also apply: larger scripts require higher transaction fees\n\n### Values\n- **1000000** (default) - 1 million operations, allows very complex on-chain computation\n- **0** - Unlimited (use with caution)\n\n### Trade-offs\n| Setting | Benefit | Drawback |\n|---------|---------|----------|\n| Higher | Support complex smart contracts | More CPU usage during validation |\n| Lower | Better DoS protection | May reject valid complex scripts |\n\n### Recommendations\n- Use default 1 million for standard operation\n- Essential for BSVs vision of programmable money and data\n- Enables complex smart contracts, token protocols, and on-chain computation"`
	MaxScriptNumLengthPolicy        int     `json:"maxscriptnumlengthpolicy" key:"maxscriptnumlengthpolicy" desc:"Maximum script number length (10K default)" default:"10000" category:"Policy" usage:"Maximum size of numbers in scripts" type:"int" longdesc:"### Purpose\nMaximum size (in bytes) of numbers used in Bitcoin Script arithmetic operations. BSV supports arbitrary precision integers (bignums), restoring original Bitcoin capability. Original Bitcoin limited numbers to 4 bytes; BSV removed this restriction.\n\n### How It Works\n- Enforced via BDK script engine SetMaxScriptNumLengthPolicy()\n- Scripts attempting to create larger numbers fail with SCRIPT_ERR_SCRIPTNUM_OVERFLOW error\n\n### Values\n- **10000** (default) - 10KB, allows very large numbers for complex computations\n- Higher values for specialized cryptographic applications\n\n### Use Cases\n- Zero-knowledge proofs\n- Large integer arithmetic\n- Complex financial calculations\n- Cryptographic protocols\n\n### Recommendations\n- Use default 10KB for standard operation\n- Critical for BSVs advanced scripting capabilities\n- Enables computations previously impossible on blockchain"`
	MaxPubKeysPerMultisigPolicy     int64   `json:"maxpubkeyspermultisigpolicy" key:"maxpubkeyspermultisigpolicy" desc:"Maximum public keys per multisig" default:"0" category:"Policy" usage:"0 is unlimited" type:"int64" longdesc:"### Purpose\nMaximum public keys allowed in a multisig script (OP_CHECKMULTISIG / OP_CHECKMULTISIGVERIFY). BTC limits to 20 keys; BSV removed this artificial restriction.\n\n### How It Works\n- Set via BDK script engine SetMaxPubKeysPerMultiSigPolicy()\n- When 0, no limit is enforced\n- Multisig verification is computationally tractable even with thousands of keys (signature checking is parallelizable)\n- Economic limits apply: more keys = larger transaction = higher fees\n\n### Values\n- **0** (default) - UNLIMITED, allowing M-of-N multisig with any N value\n- **N > 0** - Fixed maximum number of public keys\n\n### Use Cases\n- Large multi-party escrow\n- Corporate governance structures\n- Complex signing requirements\n- Distributed authorization systems\n\n### Recommendations\n- Use 0 (unlimited) for standard BSV operation\n- Fundamental to BSVs philosophy of removing artificial protocol restrictions\n- Trust economic incentives to prevent abuse"`
	MaxTxSigopsCountsPolicy         int64   `json:"maxtxsigopscountspolicy" key:"maxtxsigopscountspolicy" desc:"Maximum signature operations per transaction" default:"0" category:"Policy" usage:"0 is unlimited" type:"int64" longdesc:"### Purpose\nMaximum number of signature verification operations per transaction. BTC/BCH impose sigops limits per block and transaction; BSV removed this restriction.\n\n### How It Works\n- Signature verification is highly parallelizable and fast on modern hardware\n- Economic incentives (fees scale with transaction size/complexity) prevent abuse\n- Currently DISABLED in Teranode code (sigOpsCheck function exists but is not called)\n\n### Values\n- **0** (default) - UNLIMITED, reflecting BSVs philosophy of unlimited script capability\n- **N > 0** - Fixed maximum sigops per transaction\n\n### Use Cases\n- Complex multi-signature protocols\n- Batch verification schemes\n- Advanced cryptographic applications\n\n### Recommendations\n- Use 0 (unlimited) for standard BSV operation\n- Removes bottleneck on signature-heavy transactions\n- Trust economic incentives rather than arbitrary protocol limits"`
	MaxStackMemoryUsagePolicy       int     `json:"maxstackmemoryusagepolicy" key:"maxstackmemoryusagepolicy" desc:"Maximum stack memory usage (100MB default)" default:"104857600" category:"Policy" usage:"Limits memory during script execution" type:"int" longdesc:"### Purpose\nMaximum stack memory (in bytes) for script execution during MEMPOOL validation. This is separate from MaxStackMemoryUsageConsensus which controls block validation.\n\n### How It Works\n- Passed to BDK script engine via SetMaxStackMemoryUsage()\n- Stack memory accumulates as values are pushed during script execution\n- Exceeding this during mempool validation returns SCRIPT_ERR_STACK_SIZE with TxPolicyError wrapper\n- Large data operations (string manipulation, cryptographic operations) can consume significant memory\n\n### Values\n- **104857600** (default) - 100MB, allows complex data processing scripts\n- Higher values for specialized nodes handling complex scripts\n\n### Trade-offs\n| Setting | Benefit | Drawback |\n|---------|---------|----------|\n| Higher | Support complex data processing | More memory usage, DoS risk |\n| Lower | Better memory protection | May reject valid complex scripts |\n\n### Recommendations\n- Use default 100MB for standard operation\n- Increase for nodes specializing in complex script validation\n- Protects node resources while enabling BSVs data processing use cases"`
	MaxStackMemoryUsageConsensus    int     `json:"maxstackmemoryusageconsensus" key:"maxstackmemoryusageconsensus" desc:"Maximum stack memory usage for consensus" default:"0" category:"Policy" usage:"0 is unlimited" type:"int" longdesc:"### Purpose\nMaximum stack memory for BLOCK validation. Separate from MaxStackMemoryUsagePolicy which is for mempool.\n\n### How It Works\n- Both values passed to BDK script engine SetMaxStackMemoryUsage(consensus, policy)\n- During block validation (SkipPolicyChecks=true), consensus limit applies\n- Blocks proven valid by PoW must be accepted - this ensures blocks can contain transactions exceeding policy limits\n- SCRIPT_ERR_STACK_SIZE errors are treated as policy errors if they exceed consensus limit\n\n### Values\n- **0** (default) - UNLIMITED, aligning with BSVs restored protocol philosophy\n- **N > 0** - Fixed maximum in bytes\n\n### Two-Tier Design\n| Tier | Setting | Purpose |\n|------|---------|----------|\n| Policy | 100MB | Protect mempool resources |\n| Consensus | Unlimited | Accept all valid blocks |\n\n### Recommendations\n- Use 0 (unlimited) for consensus to accept all valid blocks\n- Essential for BSVs vision of blockchain as global data ledger\n- Enables large data processing transactions in blocks"`
	LimitAncestorCount              int     `json:"limitancestorcount" key:"limitancestorcount" desc:"Maximum ancestor count limit" default:"1000000" category:"Policy" usage:"Chain depth limit for unconfirmed transactions" type:"int" longdesc:"### Purpose\nMaximum number of unconfirmed ancestor transactions allowed for a transaction in mempool. Prevents excessively long chains that consume resources and complicate validation.\n\n### How It Works\n- Tracks transaction dependencies in mempool\n- When a transaction has parents in mempool (not yet confirmed), those parents are ancestors\n- Limits chain depth to prevent resource exhaustion and make eviction policies tractable\n- Setting exists but ancestor tracking not actively enforced in current Teranode implementation\n\n### Values\n- **1000000** (default) - 1 million, effectively unlimited for normal use cases\n- Lower values for stricter mempool management\n\n### Use Cases\n- Transaction batching\n- Child-Pays-For-Parent (CPFP) patterns\n- Complex multi-transaction protocols\n\n### Recommendations\n- Use default for standard operation\n- Very long chains can complicate fee estimation and block template building\n- Placeholder for future mempool chain management features"`
	LimitCPFPGroupMembersCount      int     `json:"limitcpfpgroupmemberscount" key:"limitcpfpgroupmemberscount" desc:"Maximum CPFP group members" default:"1000000" category:"Policy" usage:"Limits size of CPFP transaction groups" type:"int64" longdesc:"### Purpose\nMaximum number of transactions in a Child-Pays-For-Parent (CPFP) group. CPFP allows a child transaction to pay higher fees to incentivize mining of its parent transactions.\n\n### How It Works\n- When child transaction has high fee rate, miners are incentivized to include low-fee parents\n- Groups transactions for fee calculation purposes\n- Large groups can complicate block template optimization\n- CPFP fee calculation not actively implemented in current Teranode (placeholder for future support)\n\n### Values\n- **1000000** (default) - 1 million, allows complex transaction batches\n- Lower values for stricter group size limits\n\n### Trade-offs\n| Setting | Benefit | Drawback |\n|---------|---------|----------|\n| Higher | Support complex transaction batches | More complex block template optimization |\n| Lower | Simpler block templates | May reject valid CPFP patterns |\n\n### Recommendations\n- Use default for standard operation\n- BSVs low fees make CPFP less critical than in high-fee environments\n- Will enable fee bumping strategies when fully implemented"`
	AcceptNonStdOutputs             bool    `json:"acceptnonstdoutputs" key:"acceptnonstdoutputs" desc:"Accept transactions with non-standard output scripts" default:"true" category:"Policy" usage:"Enable for full BSV script support" type:"bool" longdesc:"### Purpose\nWhen true, accepts transactions with ANY valid script in outputs, not just standard templates. BTC only accepts standard templates (P2PKH, P2SH, P2WPKH, etc.); BSV removes this restriction.\n\n### Warning\n**CRITICAL FOR BSV** - Setting this to false severely limits BSVs capabilities and creates BTC-like restrictions.\n\n### How It Works\n- When true, script validation checks execution validity only, not template matching\n- When false, only recognized patterns are accepted\n- Non-standard is a misnomer - these are perfectly valid Bitcoin scripts that dont match common patterns\n\n### Values\n- **true** (default) - ESSENTIAL for BSV operation, enables full scripting capability\n- **false** - BTC-like restrictions, significantly limits BSV features\n\n### Use Cases\n- Custom escrow scripts\n- Token protocols\n- Oracle integration\n- Data commitments\n- Complex multi-party contracts\n- Covenant scripts\n\n### Recommendations\n- Always use true for standard BSV operation\n- Fundamental to BSVs permissionless innovation model\n- Bitcoin Script should not be artificially restricted"`
	DataCarrier                     bool    `json:"datacarrier" key:"datacarrier" desc:"Enable data carrier transactions" default:"false" category:"Policy" usage:"Allows OP_RETURN for data storage" type:"bool" longdesc:"### Purpose\nEnable relaying of OP_RETURN data carrier transactions. OP_RETURN creates provably unspendable outputs used for storing arbitrary data on-chain.\n\n### How It Works\n- When true, OP_RETURN transactions up to DataCarrierSize are accepted\n- When false, OP_RETURN transactions are rejected from mempool\n- Transactions with OP_RETURN are still accepted in blocks (consensus requirement) regardless of this setting\n- Separate from DataCarrierSize which controls size limit when enabled\n\n### Values\n- **false** (default) - Conservative setting, rejects OP_RETURN from mempool\n- **true** - Enables BSVs data storage economy (many BSV nodes use this)\n\n### Use Cases\n- Document timestamping\n- Certificate verification\n- Supply chain provenance\n- IoT data logging\n- Public commitments\n- Immutable data storage with Bitcoins security guarantees\n\n### Recommendations\n- Set to true to participate in BSVs data storage economy\n- BSV embraces blockchain as data ledger\n- Disabling reduces mempool size but limits BSVs data applications"`
	MinMiningTxFee                  float64 `json:"minminingtxfee" key:"minminingtxfee" desc:"Minimum fee rate for mining (BSV/kilobyte)" default:"0.000005" category:"Policy" usage:"Transactions below this fee are not mined" type:"float64" longdesc:"### Purpose\nMinimum transaction fee rate (in BSV per kilobyte) for including transactions in mined blocks. Provides economic spam prevention without artificial restrictions.\n\n### How It Works\n- Converted to satoshis/byte: (BSV/kB * 1e8 satoshis/BSV) / 1000 bytes/kB\n- Transaction must pay at least (feeRate * txSizeInBytes) in fees\n- Fee checks are POLICY only, bypassed during block validation (SkipPolicyChecks=true)\n- Enforced in TxValidator.checkFees()\n- **Consolidation Exception**: Consolidation transactions are COMPLETELY EXEMPT from fee checks (see MinConsolidationFactor)\n\n### Values\n- **0.000005** (default) - 0.5 satoshis/byte, extremely low for micropayments\n- **0** - Disables fee requirement entirely\n\n### Trade-offs\n| Setting | Benefit | Drawback |\n|---------|---------|----------|\n| Higher | Better spam protection | May reject micropayments |\n| Lower | Enable micropayments | Less spam protection |\n\n### Recommendations\n- Use default 0.000005 BSV/kB for standard BSV operation\n- Enables sub-cent transactions economically viable on BSV\n- Consolidation transactions are exempt to incentivize UTXO cleanup"`
	MaxStdTxValidationDuration      int     `json:"maxstdtxvalidationduration" key:"maxstdtxvalidationduration" desc:"Maximum validation duration for standard transactions (ms)" default:"3" category:"Policy" usage:"Timeout for standard script validation" type:"int" longdesc:"### Purpose\nMaximum time (milliseconds) allowed for validating standard transactions (simple scripts like P2PKH). Prevents DoS attacks via intentionally slow script execution.\n\n### How It Works\n- Measured as wall-clock time (default) or CPU time if ValidationClockCPU is true\n- CPU time is more accurate for computational cost and unaffected by I/O waits\n- Exceeded timeouts cause transaction rejection\n- Much shorter than MaxNonStdTxValidationDuration (1000ms) because standard scripts are simple\n- Setting exists but timeout enforcement NOT actively implemented in current Teranode (placeholder for future)\n\n### Values\n- **3** (default) - 3ms, very fast for simple signature verification\n- Higher values for more tolerance\n\n### Trade-offs\n| Setting | Benefit | Drawback |\n|---------|---------|----------|\n| Higher | More tolerance for slow validation | DoS risk from slow scripts |\n| Lower | Better DoS protection | May reject valid transactions |\n\n### Recommendations\n- Use default 3ms for standard operation\n- Primary DoS protection is currently economic (fees scale with complexity)\n- Will protect against validation-time DoS when fully implemented"`
	MaxNonStdTxValidationDuration   int     `json:"maxnonstdtxvalidationduration" key:"maxnonstdtxvalidationduration" desc:"Maximum validation duration for non-standard transactions (ms)" default:"1000" category:"Policy" usage:"Timeout for complex script validation" type:"int" longdesc:"### Purpose\nMaximum time (milliseconds) allowed for validating non-standard transactions with complex scripts. Supports BSVs advanced scripting while preventing indefinite validation time.\n\n### How It Works\n- 333x longer than MaxStdTxValidationDuration (3ms) to support complex scripts\n- Uses wall-clock or CPU time based on ValidationClockCPU setting\n- CPU time more resistant to gaming via I/O operations\n- Validation time includes script parsing, execution, signature verification, and stack operations\n- Setting exists but timeout NOT actively enforced in current code (placeholder for future)\n\n### Values\n- **1000** (default) - 1 second, allows sophisticated on-chain computation\n- Higher values for more complex script support\n\n### Trade-offs\n| Setting | Benefit | Drawback |\n|---------|---------|----------|\n| Higher | Support more complex scripts | DoS risk from slow validation |\n| Lower | Better DoS protection | May reject valid complex scripts |\n\n### Recommendations\n- Use default 1000ms for standard operation\n- Essential for BSVs restored opcodes (OP_MUL, OP_SUBSTR, etc.)\n- Economic limits (higher fees for complex scripts) are primary DoS protection"`
	MaxTxChainValidationBudget      int     `json:"maxtxchainvalidationbudget" key:"maxtxchainvalidationbudget" desc:"Maximum validation budget for transaction chains (ms)" default:"50" category:"Policy" usage:"Total time budget for chain validation" type:"int" longdesc:"### Purpose\nTotal time budget (milliseconds) for validating a chain of dependent transactions. Prevents DoS via long chains where each transaction is fast but cumulative time is excessive.\n\n### How It Works\n- When transaction depends on unconfirmed parents, validator must check entire ancestor chain\n- Each ancestors validation time counts toward budget\n- When budget exhausted, chain is rejected as too expensive to validate\n- Distinct from MaxStdTxValidationDuration which is per-transaction\n- Setting exists but chain budget tracking NOT actively implemented (placeholder for future)\n\n### Values\n- **50** (default) - 50ms, allows reasonable parent-child chains\n- Higher values for longer chain support\n\n### Trade-offs\n| Setting | Benefit | Drawback |\n|---------|---------|----------|\n| Higher | Support longer transaction chains | DoS risk from chain validation |\n| Lower | Better DoS protection | May reject valid chain patterns |\n\n### Recommendations\n- Use default 50ms for standard operation\n- Long chains can accumulate significant validation time\n- Will protect against chain-based validation DoS when fully implemented"`
	ValidationClockCPU              bool    `json:"validationclockcpu" key:"validationclockcpu" desc:"Use CPU time for validation clock" default:"false" category:"Policy" usage:"More accurate than wall time for validation limits" type:"bool" longdesc:"### Purpose\nChoose between wall-clock time or CPU time for measuring validation timeouts. Affects MaxStdTxValidationDuration, MaxNonStdTxValidationDuration, and MaxTxChainValidationBudget.\n\n### How It Works\n- **Wall clock (false)**: Measures real-world elapsed time. Simple and predictable but affected by I/O waits, system load, and context switches.\n- **CPU time (true)**: Measures actual CPU cycles consumed. More accurate for computational cost, unaffected by I/O, fairer in multi-threaded environments.\n- Setting exists but timeout enforcement NOT currently active (placeholder for future)\n\n### Values\n- **false** (default) - Wall-clock time, simpler for debugging\n- **true** - CPU time, more accurate and secure\n\n### Trade-offs\n| Setting | Benefit | Drawback |\n|---------|---------|----------|\n| Wall clock | Simpler, predictable | Affected by I/O, system load |\n| CPU time | Accurate, DoS resistant | More complex measurement |\n\n### Recommendations\n- Use CPU time (true) in production for better DoS resistance\n- CPU time prevents gaming timeouts via I/O operations\n- Wall clock is simpler for debugging and reasoning about performance"`
	MinConsolidationFactor          int     `json:"minconsolidationfactor" key:"minconsolidationfactor" desc:"Minimum consolidation factor" default:"20" category:"Policy" usage:"Minimum input-to-output ratio for consolidation" type:"int" longdesc:"### Purpose\nMinimum ratio of inputs to outputs for a transaction to qualify as consolidation. Consolidation transactions are EXEMPT FROM FEE CHECKS to incentivize UTXO cleanup.\n\n### How It Works\nTransaction must pass TWO tests:\n1. numInputs >= MinConsolidationFactor * numOutputs\n2. totalInputScriptSize >= MinConsolidationFactor * totalOutputScriptSize\n\nAdditional requirements:\n- Input confirmations (MinConfConsolidationInput)\n- Script size limits (MaxConsolidationInputScriptSize)\n- Script standards (AcceptNonStdConsolidationInput)\n\n**Special Case**: Dust return transactions (single output, 0 satoshis, OP_FALSE OP_RETURN) automatically qualify.\n\n### Values\n- **20** (default) - 20 inputs + 1 output qualifies; 100 inputs + 5 outputs qualifies\n- **0 or negative** - Disables consolidation entirely\n\n### Examples\n| Inputs | Outputs | Factor 20 | Qualifies? |\n|--------|---------|-----------|------------|\n| 20 | 1 | 20 >= 20 | Yes |\n| 100 | 5 | 100 >= 100 | Yes |\n| 15 | 1 | 15 < 20 | No |\n\n### Recommendations\n- Use default 20 for good balance\n- Too low allows fee gaming; too high discourages cleanup\n- Critical for UTXO set management and network health"`
	MaxConsolidationInputScriptSize int     `json:"maxconsolidationinputscriptsize" key:"maxconsolidationinputscriptsize" desc:"Maximum consolidation input script size" default:"150" category:"Policy" usage:"Script size limit for consolidation inputs" type:"int" longdesc:"### Purpose\nMaximum size (bytes) of unlocking scripts (scriptSig) for inputs in consolidation transactions. Ensures consolidation is for UTXO cleanup, not complex transactions.\n\n### How It Works\n- Checked in TxValidator.isConsolidationTx() during qualification\n- Each inputs UnlockingScript length must be <= this value\n- If exceeded, transaction doesnt qualify as consolidation and normal fees apply\n- Prevents fee gaming via complex multi-signature or script-heavy consolidation\n\n### Values\n- **150** (default) - Allows standard P2PKH scripts with margin\n\n### Standard Script Sizes\n| Script Type | Size | Fits 150? |\n|-------------|------|----------|\n| P2PKH | ~107 bytes (73 sig + 33 pubkey) | Yes |\n| P2PK | ~106 bytes (73 sig + 33 pubkey) | Yes |\n| Complex multisig | Varies, often >150 | No |\n\n### Recommendations\n- Use default 150 bytes for standard operation\n- Consolidation should be simple cleanup, not complex operations\n- Increase only if legitimate complex consolidation use cases emerge"`
	MinConfConsolidationInput       int     `json:"minconfconsolidationinput" key:"minconfconsolidationinput" desc:"Minimum confirmations for consolidation input" default:"6" category:"Policy" usage:"Required confirmations before consolidation" type:"int" longdesc:"### Purpose\nMinimum number of confirmations required for UTXOs being consolidated. Ensures consolidated UTXOs are stable and unlikely to be reversed by chain reorganization.\n\n### How It Works\n- Enforced in TxValidator.isConsolidationTx() by comparing currentHeight - utxoHeight >= this value\n- Requires utxoHeights parameter during validation\n- If confirmation requirement not met, transaction doesnt qualify as consolidation\n\n**Special Case**: Dust return transactions (single 0-satoshi OP_FALSE OP_RETURN output) are exempt and can consolidate unconfirmed inputs.\n\n### Values\n- **6** (default) - Approximately 1 hour of PoW, high confidence in finality\n- Lower values for faster consolidation\n- Higher values for extra safety\n\n### Trade-offs\n| Setting | Benefit | Drawback |\n|---------|---------|----------|\n| Higher | More stable UTXOs, safer against reorgs | Slower consolidation |\n| Lower | Faster consolidation | Risk of consolidating reversed UTXOs |\n\n### Recommendations\n- Use default 6 confirmations for standard operation\n- Ensures only stable, confirmed UTXOs are consolidated\n- Prevents complications from chain reorganizations"`
	MinConsolidationInputMaturity   int     `json:"minconsolidationinputmaturity" key:"minconsolidationinputmaturity" desc:"Minimum consolidation input maturity" default:"6" category:"Policy" usage:"Blocks required before input is consolidatable" type:"int" longdesc:"### Purpose\nAlternate maturity requirement for consolidation inputs. Currently redundant with MinConfConsolidationInput but exists for potential future differentiation.\n\n### How It Works\n- Setting exists and has getter/setter methods\n- NOT actively used differently from MinConfConsolidationInput in current implementation\n- Code only checks MinConfConsolidationInput for confirmations\n- Placeholder for future expansion\n\n### Values\n- **6** (default) - Matches MinConfConsolidationInput\n\n### Potential Future Use Cases\n- Different maturity criteria for different input types\n- MinConfConsolidationInput for regular transactions\n- MinConsolidationInputMaturity for coinbase consolidation (100 blocks maturity)\n- Differentiation between confirmation count vs absolute maturity\n\n### Recommendations\n- Keep aligned with MinConfConsolidationInput for now\n- Provides flexibility for future consolidation maturity rules\n- May be used for coinbase vs regular tx consolidation differentiation"`
	AcceptNonStdConsolidationInput  bool    `json:"acceptnonstdconsolidationinput" key:"acceptnonstdconsolidationinput" desc:"Accept non-standard consolidation inputs" default:"false" category:"Policy" usage:"Allow non-standard scripts in consolidation" type:"bool" longdesc:"### Purpose\nControls whether consolidation transactions can use non-standard input scripts. Default false prevents abuse of consolidation fee exemption via complex scripts.\n\n### How It Works\n- Enforced in TxValidator.isConsolidationTx() by calling isStandardInputScript() for each input\n- After UAHF fork height, standard input scripts must be push-only (no executable opcodes in scriptSig)\n- Push-only requirement improves security and validation performance\n- Uses same interpreter as pushDataCheck for consistency\n\n### Values\n- **false** (default) - Only standard (push-only) input scripts qualify for consolidation\n- **true** - Allows non-standard input scripts (risks fee exemption abuse)\n\n### Trade-offs\n| Setting | Benefit | Drawback |\n|---------|---------|----------|\n| false | Prevents fee gaming, simple auditing | Cannot consolidate non-standard UTXOs |\n| true | Can consolidate any UTXO | Fee exemption abuse risk |\n\n### Recommendations\n- Keep false (default) for standard operation\n- Consolidation should be simple UTXO cleanup, not complex operations\n- Prevents complex script consolidation fee gaming"`
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
