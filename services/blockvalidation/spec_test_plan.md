# Block Validation: Spec Test Coverage and Test Plan

**Companion to:** [`spec.md`](spec.md) (requirement prefix `BV`)
**Assessed at:** branch `docs/blockvalidation-spec`, base `upstream/main` 71c5aff48, on 2026-10-19
**Status:** Draft for review

This document answers two questions for each requirement in the Block Validation spec:

1. **Is it tested today?** Which test asserts it, and does that test run in CI?
2. **What else should test it?** Beyond example-based unit tests: invariant and property tests, fail-point (crash and error injection) tests, chaos tests, race tests, fuzzing, metamorphic tests, and differential tests against bitcoin-sv.

It ends with a prioritised test backlog and the test infrastructure needed to build it.

> **Status update, 2026-10-30.** Ten gaps this plan marks "fails today" have since been fixed on `main`, each with a regression test (see the spec's Conformance Gaps for the test names):
>
> - GAP-BV-003 (bsv-blockchain/teranode#2010)
> - GAP-BV-005 (bsv-blockchain/teranode#2021)
> - GAP-BV-020 and GAP-BV-021 (bsv-blockchain/teranode#2034)
> - GAP-BV-029 and GAP-BV-056 (bsv-blockchain/teranode#2011)
> - GAP-BV-043 (bsv-blockchain/teranode#2033)
> - GAP-BV-045 (bsv-blockchain/teranode#2017)
> - GAP-BV-054 (bsv-blockchain/teranode#2012, bsv-blockchain/teranode#2035)
> - GAP-BV-057 (bsv-blockchain/teranode#2016)
>
> GAP-BV-031 and GAP-BV-055 were resolved by bsv-blockchain/teranode#1909. The coverage rows and backlog items below still describe the 2026-10-19 assessment. For a backlog item on a resolved gap, the expected result is now "passes" rather than "fails today".

---

## Contents

1. [Summary](#1-summary)
2. [How coverage was judged](#2-how-coverage-was-judged)
3. [Where tests run today](#3-where-tests-run-today)
4. [Coverage by spec section](#4-coverage-by-spec-section)
5. [Requirement coverage matrix](#5-requirement-coverage-matrix)
6. [Tests that do not test what they appear to](#6-tests-that-do-not-test-what-they-appear-to)
7. [Crash-point analysis](#7-crash-point-analysis)
8. [Invariants to check after every test](#8-invariants-to-check-after-every-test)
9. [Test backlog](#9-test-backlog)
10. [Test infrastructure roadmap](#10-test-infrastructure-roadmap)
11. [Spec changes needed for testability](#11-spec-changes-needed-for-testability)
12. [Defects found during this assessment](#12-defects-found-during-this-assessment)

---

## 1. Summary

Of the 208 requirements in the spec:

| Verdict | Count | Share |
|---|---|---|
| **Covered**: a test asserts the requirement's observable outcome | 47 | 23% |
| **Partial**: exercised, or only some clauses asserted | 104 | 50% |
| **Pins a gap**: a test asserts the behaviour the spec records as a defect | 16 | 8% |
| **None**: no test | 40 | 19% |
| Not applicable (definitional) | 1 | — |

The package has a large unit suite (121 test files, about 57,000 lines), and many recent tests are careful, real-store regression tests. The weaknesses are in what kind of testing exists, not in volume:

- **Almost all coverage is unit-level.** No e2e test anywhere asserts a block-level error code (`ERR_BLOCK_INVALID`, `ERR_BLOCK_CORRUPT`). They check only that some error occurred.
- **Crash consistency is untested.** The commit sequence (AddBlock → `subtrees_set` → mined-tagging → `mined_set`) has about 25 distinct crash or failure windows (section 7). No test kills or fails the service in any of them. There is no fail-point mechanism, and the e2e daemon's `Stop` is graceful, so it completes the very work a crash would interrupt.
- **The nightly chaos suite never touches Teranode.** Scenarios 01–07 exercise the Postgres and Kafka client libraries through Toxiproxy; none imports a Teranode package. The multinode chaos harness (container kill, iptables, netem), the `wip/` e2e tests, chaos scenario 08 and `test/consensus` run in no CI workflow.
- **Consensus rules have no independent oracle.** No test compares Teranode's verdicts with bitcoin-sv's. The bitcoin-sv transaction and script vectors run only in `test/consensus`, which is not in CI, and 127 of those cases are skipped because "go-bdk behaviour differs".
- **The newest consensus requirements are untested.** BV-BODY-019 to 022 (coinbase finality, context reuse, block sigops, BIP30) have no tests. These came from the bitcoin-sv audit.
- **Adversarial peers are simulated, not real.** Peer HTTP is faked with `httpmock`, which replaces the transport. Truncated bodies, a lying `Content-Length`, slow-loris and mid-body resets cannot be expressed.
- **16 tests pin known defects.** They will fail when the defect is fixed, which is useful, but they must be identified first (section 6). Several further tests are vacuous.

This assessment also found **six new defects**, now recorded in the spec as GAP-BV-054 to 059 (section 12). The most serious: three worker paths that crash the process on a panic, and a race that can leave an invalidated block marked as mined with stale tags.

---

## 2. How coverage was judged

Each requirement was mapped to the tests that verify it. The test bodies were read, not just the names. Verdicts are strict:

- **Covered:** a test asserts the requirement's observable outcome, including the error code and the persistence or peer effects the requirement states.
- **Partial:** the code path is exercised, but the assertion is missing or covers only some clauses (for example: tests the rule but not the error code, or one network or store backend only).
- **Pins a gap:** a test asserts the current behaviour that the spec records as a GAP. That test must be inverted when the gap is fixed.
- **None:** no test.

A test that runs in no CI tier does not count as coverage.

**Method and confidence.** The mapping was done by six parallel reviews: five by requirement section, one on test infrastructure. The headline findings in this document were spot-checked against the code, including every "new defect" in section 12 and every test called vacuous in section 6. Individual matrix rows were not all re-verified. Test file and line references are approximate (`~`) and reflect the tree on the date above.

---

## 3. Where tests run today

| Tier | Trigger | What runs | Relevant to Block Validation |
|---|---|---|---|
| PR unit | every PR (`make test`) | every package outside `test/`, with `-race` and tag `testtxmetacache` | the whole `services/blockvalidation` suite, `model`, `services/subtreevalidation`, `services/validator`, `stores` |
| PR smoke | every PR, 3 shards | `test/e2e/daemon/ready` (minus legacy and pruner subsets) | `catchup_test.go`, `reorg_test.go`, `fork_navigation_test.go`, `bip68_test.go`, `blockvalidation_test.go` |
| PR legacy-sync, pruner | every PR | named `ready` subsets | legacy sync against svnode |
| PR sequential | every PR, 7 shards | `test/sequentialtest/*` | `double_spend/*`, `longest_chain/*` |
| PR chainintegrity | every PR | 3-node docker compose | tip agreement only |
| Long | `long_tests.yaml` | `make testall` | `test/longtest`, `TestLegacyHistoricalTestnetSync` |
| Nightly | `nightly_tests.yaml` | full `ready` (no `-race`), chaos 01–07, soak (2 h) | chaos 01–07: **none** (see below) |
| Fuzz | nightly, 5 min per target | 10 targets in `Makefile` `FUZZ_TARGETS` | `model` block parsers only; nothing in `services/blockvalidation` |

**Not run by any workflow:**

- `test/e2e/daemon/wip/` (18 files), which includes the invalid-block, ban-score and unmined-reorg e2e tests;
- `test/multinode` and `test/multinode_split` (build tag `network_chaos`), the only tests that kill nodes or partition the network;
- `test/chaos/scenario_08_block_validation_memory_test.go`, which in any case uses simulated functions, not the service;
- `test/consensus` (svnode script, transaction and sighash vectors through the BDK engine);
- `test/fsm`, and the unmerged chain-reconciliation oracle's own tests under `test/utils`.

**Chaos scenarios 01–07 do not exercise Teranode.** Their only imports are `lib/pq` and `sarama`. They test client-library behaviour under Toxiproxy faults, so they provide no Block Validation coverage.

---

## 4. Coverage by spec section

| Section | Reqs | Covered | Partial | Pins gap | None | Main weakness |
|---|---|---|---|---|---|---|
| Ingress (`INGRESS`) | 19 | 4 | 10 | 3 | 2 | `wait_to_complete` tested with a fake worker; Kafka redelivery never driven through a real consumer |
| Queue (`QUEUE`) | 11 | 1 | 7 | 2 | 1 | no model-based test of ordering, ageing or the fork limit |
| Header (`HEADER`) | 11 | 3 | 7 | 1 | 0 | notification side effects never asserted; future-time pinned as INVALID |
| Body (`BODY`) | 22 | 6 | 9 | 3 | 4 | BODY-019–022 untested; delegated rules partly verified only through a non-CI package |
| Verdict (`VERDICT`) | 11 | 5 | 4 | 1 | 0 (1 n/a) | no table-driven test of "exactly one class with these effects" |
| Optimistic (`OPTIMISTIC`) | 9 | 4 | 4 | 0 | 1 | unit tests pre-cancel the lifecycle, so the end state is never observed; no e2e test enables optimistic mining |
| Commit (`COMMIT`) | 12 | 2 | 5 | 1 | 4 | no crash or failure injected anywhere in the sequence |
| Revalidation (`REVALIDATE`) | 10 | 0 | 6 | 0 | 4 | `Server.RevalidateBlock` has no direct test |
| Catchup (`CATCHUP`) | 27 | 6 | 16 | 1 | 4 | lying-header rules tested at function level only; no real-socket peer |
| Quick validation (`QUICK`) | 31 | 8 | 14 | 0 | 9 | no crash or replay across the multi-wave UTXO apply; no quick-vs-full comparison |
| Peer (`PEER`) | 10 | 5 | 4 | 1 | 0 | malicious-peer refusal untested at 7 of its call sites |
| Startup (`STARTUP`) | 9 | 0 | 4 | 0 | 5 | `Init` failures, panic recovery and resubscription untested |
| Config (`CONFIG`) | 7 | 1 | 4 | 1 | 1 | nothing detects settings that have no effect |
| Observability (`OBSERVABILITY`) | 9 | 1 | 4 | 1 | 3 | FSM health check and metric registration untested |
| State (`STATE`) | 3 | 0 | 2 | 1 | 0 | matrix cells mostly untested |
| Non-functional (`NFR`) | 7 | 1 | 4 | 0 | 2 | no crash-convergence or honest-tip-wedge test |
| **Total** | **208** | **47** | **104** | **16** | **40** | |

---

## 5. Requirement coverage matrix

Status codes: **C** covered, **P** partial, **G** pins a gap, **N** none. Unless noted, cited tests are in `services/blockvalidation/` and run in PR unit.

### Ingress

| ID | Status | Evidence | Missing |
|---|---|---|---|
| INGRESS-001 | P | `Test_BlockFound`, `Test_consumerMessageHandler` (Server_test.go) | no test that both paths give the same outcome |
| INGRESS-002 | P | `Test_BlockFound/invalid hash` | error code, nothing-queued, 31/33-byte boundaries |
| INGRESS-003 | P | `Test_BlockFound/block already exists` (cache hit only) | block in store, block stored invalid, no-fetch assertion |
| INGRESS-004 | P | `TestBlockFoundChannelErrorHandling`, `TestKafkaConsumerMessageHandling/Invalid_URL_Scheme` | full message text, error code, `ProcessBlock` `invalid base URL` branch |
| INGRESS-005 | P | `Test_BlockFound/new block with wait` (a test goroutine plays the worker) | real pipeline; never waits for validation; GAP-BV-001 has no failing test |
| INGRESS-006 | N | — | bound on pending work (GAP-BV-002) |
| INGRESS-007 | P | `Test_Start*` (FSM wait mocked) | consumer not started before leaving IDLE |
| INGRESS-008 | C | `Test_consumerMessageHandler/recoverable error` (misnamed: tests a decode failure) | — |
| INGRESS-009 | G | `TestKafkaConsumerMessageHandling/Recoverable_Error_Handling` | handler contract only; real redelivery (GAP-BV-003) |
| INGRESS-010 | G | `TestBlockHandlerWithMaliciousPeer` (peer ID copied) | client cannot send `peer_id` (GAP-BV-004); `Client_test.go` compiles against it |
| INGRESS-011 | C | `Test_ProcessBlock/invalid block bytes` | error code |
| INGRESS-012 | P | `Test_ProcessBlock` height subtests | derived height value, `invalid height: 0`, error codes |
| INGRESS-013 | P | `TestStoreInvalidBlock_KeepsTheReservedBlockID` (direct call) | through `ProcessBlock`; valid-block case; GAP-BV-039 |
| INGRESS-014 | N | — | empty `base_url` means `legacy` |
| INGRESS-015 | C | `Test_Server_processBlockFound_SettlesPeerSuppliedHeight` | only one wrong claim tested; property test |
| INGRESS-016 | C | `TestProcessBlockFound_NonLegacyBlockKeepsTheCatchupDivert` | full catchup channel |
| INGRESS-017 | G | `...NonLegacyBlockKeepsTheCatchupDivert` asserts `NoError` on divert (GAP-BV-005) | legacy route with unified route off |
| INGRESS-018 | P | `TestWaitForBlockAssemblyReady` (util), unified-route BA-behind test | default of 20; give-up behaviour |
| INGRESS-019 | P | `TestIsParentFinalizing` (predicate only) | the wait loop, cancellation, ordering |

### Queue

| ID | Status | Evidence | Missing |
|---|---|---|---|
| QUEUE-001 | P | `TestBlockClassifier` (block_priority_test.go) | boundary `parent+1 == best−threshold`; classification error → DeepFork |
| QUEUE-002 | N | — | threshold derivation; underflow (GAP-BV-009) |
| QUEUE-003 | P | `TestBlockPriorityQueue/Priority ordering`, `/Height ordering` | FIFO tie-break |
| QUEUE-004 | P | `TestBlockPriorityQueue_GetEffectivePriority` | boundaries (60 s, 10 skips); lazy evaluation (GAP-BV-041) |
| QUEUE-005 | P | `Duplicate prevention`, `TestAlternativeSourceTracking` | cap of 3 never exceeded in tests (GAP-BV-007) |
| QUEUE-006 | G | `TestBlockPriorityQueueRetry/RequeueForRetry_New_Block` asserts the `retry` mechanism (GAP-BV-006) | primary fails then alternative fetched |
| QUEUE-007 | G | `TestBlockProcessingWorkerRetry` asserts empty queue after failure (GAP-BV-007) | configurable bound |
| QUEUE-008 | C | `TestAddBlockToPriorityQueue_RateLimitedFetchDoesNotChargeAnnouncer` | 429 on alternative and retry paths |
| QUEUE-009 | P | `TestForkManager_BlockProcessing` (parallel cap) | one-per-fork at start not enforced (GAP-BV-008) |
| QUEUE-010 | P | `TestForkManager_BlockProcessing/Already processing block` | refused duplicate dropped with its sources (GAP-BV-008) |
| QUEUE-011 | P | `TestForkManager_PerformCleanup` (custom 100 ms config) | default 1 h; 1000 cap; GAP-BV-037 |

### Header

| ID | Status | Evidence | Missing |
|---|---|---|---|
| HEADER-001 | P | `TestValidateBlock_ZeroWorkHeader_IsNotPersisted`, `model` nBits tests | negative/zero/overflow nBits through `ValidateBlock`; notification |
| HEADER-002 | C | `...ZeroWorkHeader...` second subtest, `TestValidateBlock_DoesNotMeetTargetDifficulty` | hash equal to target; notification |
| HEADER-003 | C | `TestValidateBlock_Difficulty1Header_IsNotPersisted`, `TestCheckpointValidationCallSites` | notification |
| HEADER-004 | P | `TestValidateBlock_BelowCheckpoint_ExpectedNBitsAlwaysEnforced`, `services/blockchain` difficulty tests | quick and unified routes; fixtures are Teranode's own, not svnode `pow_tests.cpp` |
| HEADER-005 | P | `TestValidateBlock_CheapHeader_RejectedBeforeCheckpointAndParentChecks`, `TestValidateBlock_PoWCheckedBeforeSubtreeValidation` | no UTXO-store access assertion |
| HEADER-006 | G | `model` `TestBlock_CheckHeaderContextual` future subtest and `cheap_header_no_persist_test.go` assert `ERR_BLOCK_INVALID` | not-a-verdict outcome; no notification; later acceptance (GAP-BV-050) |
| HEADER-007 | P | `TestCheckHeaderContextualMedianWindow`, `TestMedianTimestamp` | regtest exemption; upper median on even count |
| HEADER-008 | C | `TestCheckBlockVersion*`, `TestBlockValidRejectsOutdatedVersion`, `TestQuickValidateBlockRejectsOutdatedVersion` | BIP65/66 boundaries; negative nVersion |
| HEADER-009 | P | no-persist asserted throughout | no test observes the notification for any header verdict |
| HEADER-010 | P | `TestValidateBlock_HeaderContextRetryBudgetIsBounded`, `TestServerValidateBlock_UnrepairableRunIsNotAVerdictOnTheBlock` | "exactly once"; no-persist, no-charge |
| HEADER-011 | P | `TestPreviousBlockHeaderCountFloor` (settings) | default 100; clamping end to end |

### Body

| ID | Status | Evidence | Missing |
|---|---|---|---|
| BODY-001 | C | `TestEarlyBodyBindingMatchesCheckMerkleRoot`, `TestValidateBlock_SameHeaderDifferentBody_HonestBodyStillAccepted` | honest body after a corrupt body with subtrees |
| BODY-002 | P | `TestEarlyBodyBinding_NonPlaceholderFirstLeaf` | placeholder in other slots or the last subtree |
| BODY-003 | P | `TestGetAndValidateSubtrees_SizeMismatchIsCorrupt` | power-of-two and larger-final cases asserted only relative to a reference loader |
| BODY-004 | G | `TestCVE20122459_MerklePassesDedupCatches` and dedup tests assert CORRUPT for every duplicate (GAP-BV-051) | non-merkle duplicate in a bound body → INVALID |
| BODY-005 | G | `BlockValidation_test.go` ~1618 and `quick_validate_test.go` ~528 assert INCOMPLETE for an empty-input coinbase (GAP-BV-046) | INVALID expected |
| BODY-006 | P | `TestBlockValid_InvalidWhenCoinbaseIsOnlyLooselyACoinbase` | two inputs; wrong index |
| BODY-007 | C | `TestBlock_Valid_CoinbaseCommonRules`, `TestCoinbaseCommonRuleViolation` | post-Genesis 1 GB bound |
| BODY-008 | C | `TestBlock_Valid_CoinbaseScriptSigLength` and service/quick variants | — |
| BODY-009 | P | `TestBlock_Valid_BIP34Reorder`, `TestBlock_ExtractCoinbaseHeight` | property test against `CScript() << h`; non-minimal pushes |
| BODY-010 | P | `TestBlock_CheckBlockRewardAndFees*`, `TestValidateBlock_BoundInvalidBody_IsPersistedInvalid` (height 1) | above-checkpoint case; halving boundaries; notification |
| BODY-011 | C | bound/unbound pairs in the coinbase rule tests | reward rule unbound |
| BODY-012 | C | `TestParentSpends_RandomBlocksVsReference` (property test), `FuzzInpointDedup` (seed replay only) | end-to-end code and persisted record |
| BODY-013 | P | `test/sequentialtest/double_spend/13_*` (PR sequential, `require.Error` only) | error code, persisted record, notification |
| BODY-014 | C | `floater_fsm_gated_test.go` (RUNNING and CATCHINGBLOCKS) | IDLE |
| BODY-015 | P | structure, value, BIP68 and finality-selector tests in `services/validator`; svnode vectors only in `test/consensus` (not CI) | null prevout, empty in/out, input-total and fee range, block-level maturity, block-level non-final rejection, consensus sigops |
| BODY-016 | P | `TestBlockValidation_SubtreeError_Classification`, `TestValidateBlock_SubtreeTxInvalid_IsCorruptNotPersisted` | legacy branch; each transient cause |
| BODY-017 | G | `TestUpdateTxMinedStatus_BlockIDCollisionDetection` asserts post-commit detection (GAP-BV-011) | "never becomes tip" |
| BODY-018 | P | `TestValidateBlock_ExcessiveBlockSize_PlainPolicyDecline`, decline-cap tests | error code; under-declared size (GAP-BV-010); 1 MB floor |
| BODY-019 | N | — | coinbase finality (GAP-BV-045) |
| BODY-020 | N | — | context reuse (GAP-BV-047) |
| BODY-021 | N | — | pre-Genesis block sigops (GAP-BV-048) |
| BODY-022 | N | — | BIP30 (GAP-BV-049) |

### Verdict

| ID | Status | Evidence | Missing |
|---|---|---|---|
| VERDICT-001 | P | exercised piecemeal | table-driven "exactly one class with these effects" |
| VERDICT-002 | C | same-header-different-body, subtree tx-invalid tests | gating cache after CORRUPT |
| VERDICT-003 | C | `errors.Is(err, ErrBlockInvalid)` false checks across corrupt tests | — |
| VERDICT-004 | C | `TestValidateBlock_BoundInvalidBody_IsPersistedInvalid`, reserved-ID tests | — |
| VERDICT-005 | P | `TestValidateBlock_ParentInvalid_UnboundBodyPersistsNothing` | notification |
| VERDICT-006 | P | `TestValidateBlock_CorruptBody_NotWrappedInvalidOverRPC` | INVALID across gRPC; wrapping of other outcomes |
| VERDICT-007 | C | `corrupt_subtree_cleanup_test.go` | INVALID and LOCAL outcomes |
| VERDICT-008 | P | infrastructure-error and classification tests | storage or gRPC fault at each pre-AddBlock step |
| VERDICT-009 | G | one positive (duplicate tx), one negative (corrupt) | header-final, parent-invalid, optimistic and revalidation paths (GAP-BV-012) |
| VERDICT-010 | C | `TestKafkaNotifyBlockInvalid_SanitisesLegacyPeerID` | through a real rejected legacy block |
| VERDICT-011 | — | definitional | — |

### Optimistic mining

| ID | Status | Evidence | Missing |
|---|---|---|---|
| OPTIMISTIC-001 | C | `TestOptimisticMiningDisabledForPeerPath`, `...GlobalFlagAloneIsNotOptimistic` | — |
| OPTIMISTIC-002 | P | `TestValidateBlocksOnChannel_CatchupIsNeverOptimistic` | legacy and `RevalidateBlock` end to end |
| OPTIMISTIC-003 | P | non-optimistic corrupt and no-persist tests | AddBlock strictly last |
| OPTIMISTIC-004 | C | `...RejectsFutureTimestampSynchronously`, `...SubtreeLessUnboundBodyRejectedBeforeAdd` | other header rules |
| OPTIMISTIC-005 | P | `TestBlockValidation_OptimisticFloaterInvalidatedWhenCaughtUp` | notification (GAP-BV-012, 043); end state never observed |
| OPTIMISTIC-006 | C | `TestBlockValidation_OptimisticMining_CorruptBody_InvalidateRoute`, escalation tests | — |
| OPTIMISTIC-007 | P | `TestOptimisticAdded_*` | UTXO timeout inside background validation |
| OPTIMISTIC-008 | N | — | restart before background validation (GAP-BV-013) |
| OPTIMISTIC-009 | C | `TestReValidateBlock_CorruptConvergesOnlyWhenOptimisticallyAdded` | — |

### Commit and mined state

| ID | Status | Evidence | Missing |
|---|---|---|---|
| COMMIT-001 | P | `TestBlockValidation_SetMined_UpdatesTxMeta` (happy path) | step order; best-effort failures |
| COMMIT-002 | C | `TestProcessSubtreesNotSet_*` (sweep path) | inline path |
| COMMIT-003 | N | — | failure after AddBlock (GAP-BV-015) |
| COMMIT-004 | P | `TestBlockValidation_BlockchainSubscription_TriggersSetMined` (weak: passes even if the worker never runs) | `Block` notification must not trigger tagging |
| COMMIT-005 | P | `TestBlockValidation_SetMined_UpdatesTxMeta`, `model` coverage-gap tests | `mined_set` stays false on failure |
| COMMIT-006 | C | `TestEnqueueSetMined_DoesNotBlockWhenChannelFull` | real notification loop under stall |
| COMMIT-007 | P | `TestSetMinedRetryBackoff`, retry no-stall tests | 10th-attempt drop; `manual_intervention_required`; not-found drop |
| COMMIT-008 | P | sweep unit tests | single-flight for (a) (GAP-BV-059); startup ordering |
| COMMIT-009 | N | the two relevant tests are `t.Skip`'d | backoff, re-trigger, final code (GAP-BV-014) |
| COMMIT-010 | N | quick-path option test only | non-zero `block_id` on other routes (GAP-BV-039) |
| COMMIT-011 | G | `model/update-tx-mined_test.go` ~1396 asserts revert errors are swallowed (GAP-BV-057) | revert verified in UTXO store |
| COMMIT-012 | N | — | finite delete-at-height; never promoted by Block Validation |

### Revalidation and `ValidateBlock`

| ID | Status | Evidence | Missing |
|---|---|---|---|
| REVALIDATE-001 | P | `TestValidateBlock_ParentInvalid_ChildKeepsFullBodyAndReconsiders` (via options, not the RPC) | `Server.RevalidateBlock`; peer re-fetch |
| REVALIDATE-002 | N | — | error for a block not marked invalid |
| REVALIDATE-003 | P | store `TestSQL_RevalidateBlock` | pruned-data case; re-tagging after success |
| REVALIDATE-004 | P | strike-gating tests (corrupt only) | no new invalid record |
| REVALIDATE-005 | N | — | server wrapper message |
| REVALIDATE-006 | P | `TestBlockValidation_RevalidateBlockChan_Retries` | "no 5th attempt" |
| REVALIDATE-007 | P | `Test_ValidateBlock` | spy proving no chain mutation |
| REVALIDATE-008 | N | — | header rules in `ValidateBlock` (GAP-BV-017) |
| REVALIDATE-009 | P | `Test_ValidateBlock`, transient missing-parent test | most mapping rows; success message ("success" subtest is misnamed) |
| REVALIDATE-010 | N | — | height derivation |

### Catchup

| ID | Status | Evidence | Missing |
|---|---|---|---|
| CATCHUP-001 | P | `TestServer_blockFoundCh_triggersCatchupCh` | malicious-primary refusal at server level |
| CATCHUP-002 | G | `TestCatchup_ConcurrentCatchupLock`, "catchup-in-progress keeps the guard" (assert the drop, GAP-BV-019) | retained request runs later |
| CATCHUP-003 | P | `TestChainExtendingBlocksNotSentToCatchup` | thresholds and boundaries |
| CATCHUP-004 | P | `TestFetchHeadersWithRetry_EnforcesByteCap` | 10,000-header request; locator cap |
| CATCHUP-005 | P | `TimeoutVerySlowPeer` (httpmock delay) | 300 s bound; exact retry count; real byte drip |
| CATCHUP-006 | C | `TestCatchup_MemoryLimitAfterDuplicateRemoval` | — |
| CATCHUP-007 | P | `catchup/header_validation_test.go` (function level) | abort plus malicious report through `catchup()`; future-dated not malicious |
| CATCHUP-008 | P | `TestHeaderChainCache_BuildFromHeaders_BrokenChain` | first header linked to ancestor (GAP-BV-020) |
| CATCHUP-009 | P | `TestValidateHeaderChainDifficulty_*` (function level) | abort plus report through `catchup()` |
| CATCHUP-010 | P | `TestFindCommonAncestor_*` | ancestor stored invalid |
| CATCHUP-011 | P | `TestValidateForkDepth_Boundary` | log and metric for a refused heavier chain |
| CATCHUP-012 | C | `TestCheckSecretMiningWorkGating` | end to end |
| CATCHUP-013 | P | `TestCatchup_NoMinedSetChurnOnRejectedFork` (depth and secret mining only) | link, checkpoint and DAA failures (GAP-BV-021) |
| CATCHUP-014 | P | `TestCheckpointValidation*`, `TestQuickValidationBoundToActuallyVerifiedCheckpoint` | mismatch-then-abort confirmed |
| CATCHUP-015 | C | `TestCatchupEntry_OperatorIdleDoesNotStartValidation`, `TestProcessCatchupChItem_OperatorIdleIsNotPeerFailure` | no-fetch assertion |
| CATCHUP-016 | P | `TestRestoreFSMState_*` (direct calls) | RUN after a failed block phase; GAP-BV-028 is pinned |
| CATCHUP-017 | P | empty-batch, hash-mismatch and byte-cap tests | 99-of-100 batch |
| CATCHUP-018 | C | prefetch-budget tests (`get_blocks_test.go`) | — |
| CATCHUP-019 | P | `TestOrderedDelivery_StrictOrdering` | validator receives parent first; BA wait |
| CATCHUP-020 | C | `TestValidateBlocksOnChannel_CatchupIsNeverOptimistic` | — |
| CATCHUP-021 | C | `TestCatchupAttemptCap*`, `TestCatchupCap_BoundsReentry` | real clock |
| CATCHUP-022 | N | — | a fourth alternative dropped |
| CATCHUP-023 | N | — | parallel-fetch eligibility; worker limit (GAP-BV-024) |
| CATCHUP-024 | N | — | throughput switching (GAP-BV-024) |
| CATCHUP-025 | P | `catchup/circuit_breaker_test.go` (config with 3 half-open slots) | production config never closes (GAP-BV-022); see section 6 |
| CATCHUP-026 | N | — | local faults recorded against the peer (GAP-BV-023) |
| CATCHUP-027 | P | "Already Synchronized" subtest | no-fetch assertion |

### Quick validation

| ID | Status | Evidence | Missing |
|---|---|---|---|
| QUICK-001 | P | `TestQuickValidationBoundToActuallyVerifiedCheckpoint`, `TestTryQuickValidation` | allow-flag off, fork depth > 0, no checkpoint matched |
| QUICK-002 | N | — | BA-not-ready fallback; see section 11 for a conflict with catchup |
| QUICK-003 | C | `TestServer_legacyUnifiedRoute` (full truth table) | real store capability; legacy and BV route logic not cross-checked |
| QUICK-004 | C | `TestProcessBlockFound_UnprovenBelowCheckpointBlockNeverReachesSpendAndCreate` | script validation actually running |
| QUICK-005 | C | `TestProcessBlockFound_UnifiedRouteBlockWithNoStoredParentIsALocalFault` | — |
| QUICK-006 | P | `TestOperatorOverrideFence` (outpoint-only only) | lock skip and unspendable skip under the override; above-checkpoint refusal never fired |
| QUICK-007 | C | `TestBelowCheckpoint`, `TestQuickValidateSkipsUtxoLock` | — |
| QUICK-008 | C | version and nil-coinbase quick tests | version error code |
| QUICK-009 | C | `TestQuickValidate_MismatchedBody_NoUTXOMutation` and many prebind variants | — |
| QUICK-010 | P | forged-root and wrong-key tests | transaction body swapped at a slot |
| QUICK-011 | C | quick coinbase rule tests | — |
| QUICK-012 | N | — | forged previous-output data re-resolved |
| QUICK-013 | P | `TestQuickValidate_OutpointOnly_NoDecorate_ZeroFees` (mock store) | refusal above the checkpoint |
| QUICK-014 | P | lock-gating tests (call flags only) | unlock strictly after AddBlock; real-store lock state |
| QUICK-015 | N | — | unlock after crash (GAP-BV-030, 056) |
| QUICK-016 | P | `TestBatchList_ChainedAndIndependentMix`, `TestBatchList_RetryKeepsAFailedChildsParent` | positive ordering under delay |
| QUICK-017 | P | `TestSpendBatchWithRetry` (spend wave, mock store); `TestBatchList_StoreFaultRetriesTheList`, `TestBatchList_RetryWithNoProgressGivesUp` | cap of 10; `ErrSpent` |
| QUICK-018 | P | `TestBatchList_CancelStopsTheBackoff`, `TestBatchList_CallBoundExpiryIsARetryNotAVerdict` | sibling failure during an in-flight store call (GAP-BV-055, resolved by #1909) |
| QUICK-019 | P | `TestBatchList_ReplayStampsAndCreatesNothingTwice` (uses two different block IDs) | same ID on replay; block-ID reuse path |
| QUICK-020 | P | `TestQuickValidateBlock` (three options) | no separate notification; single insert on a real store |
| QUICK-021 | N | — | files durable before AddBlock (GAP-BV-029, 056) |
| QUICK-022 | N | — | DAH value; no subtree-meta file |
| QUICK-023 | C | `TestSkipUnspendableTxStorageDuringCatchup_EndToEnd` (SQL) | chained and replayed skipped transactions |
| QUICK-024 | N | — | startup refusal (GAP-BV-032) |
| QUICK-025 | P | unified-route missing-parent test | catchup-route ordering |
| QUICK-026 | P | `TestQuickValidate_TwoForgedBlobs_BothQuarantined` and others | legacy unconfirmed deletion → `ERR_SERVICE_ERROR` |
| QUICK-027 | P | late-corrupt tests | INVALID re-judged; INCOMPLETE keeps files |
| QUICK-028 | N | — | settings disagreement degrades to full validation |
| QUICK-029 | P | `TestCheckpointHeight_WriteReadAgree` | cross-service decision grid |
| QUICK-030 | N | — | characterisation of what is trusted |
| QUICK-031 | N | — | override-range rules (GAP-BV-045) |

No quick-validation test runs against Aerospike. Quick validation in normal (non-outpoint-only) mode on Aerospike is therefore untested.

### Peer accountability

| ID | Status | Evidence | Missing |
|---|---|---|---|
| PEER-001 | C | `corrupt_cap_test.go`, `corrupt_cap_integration_test.go` | real clock; concurrent hammer |
| PEER-002 | C | policy-decline cap and terminal-decline tests | skip-and-try-alternatives in catchup |
| PEER-003 | P | `TestCatchup_MaliciousPeerAbortsCatchup` (header path), fail-open tests | 7 other call sites; see section 6 |
| PEER-004 | C | `TestPenalizeCorruptBlockPeer_*`, strike-gating tests | — |
| PEER-005 | G | positive control in `catchup_corrupt_branch_test.go`; unbound-list test walks alternatives (GAP-BV-044) | exactly once (GAP-BV-025) |
| PEER-006 | P | `TestReleaseCatchupLock_*Local*`, `TestIsLocalCatchupFault` | persistent 429; BA behind (GAP-BV-026); `legacy` (GAP-BV-027) |
| PEER-007 | C | `TestFetchSubtreeDataForBlock_StrikesTheServingPeerNotThePrimary`, `TestReleaseCatchupLock_ExternalErrorChargesOnlyTheFailingPeers` | one test accepts a double charge of the primary |
| PEER-008 | C | cache-bypass retry tests | — |
| PEER-009 | P | header-progress credit tests | catchup-success and header-delivery credit |
| PEER-010 | P | `ReportPeerFailure` assertions for external errors | INVALID, header-phase, timeout, breaker-open |

### Startup, configuration, observability, state, NFR

| ID | Status | Evidence | Missing |
|---|---|---|---|
| STARTUP-001 | N | — | `Init` failure paths |
| STARTUP-002 | N | — | gRPC served while FSM is IDLE |
| STARTUP-003 | P | `Test_Start_FSMTransitionError`, `..._FSMContextCancellation` | nil-consumer error |
| STARTUP-004 | P | `pkg/adaptivefetch` tests | server fallback; arm on first RUNNING |
| STARTUP-005 | P | `Test_Stop*` (mocks return instantly) | wedged Kafka close (GAP-BV-042) |
| STARTUP-006 | N | — | producer readiness (GAP-BV-034) |
| STARTUP-007 | N | — | panic recovery (GAP-BV-054) |
| STARTUP-008 | N | — | resubscription |
| STARTUP-009 | P | `multinode_split` isolation scenarios (not CI); local-classification tests | each dependency's transient failure never persists invalid |
| CONFIG-001 | P | `TestPreviousBlockHeaderCountFloor` | warning; exact value |
| CONFIG-002 | P | `TestSubtreeDataFetchTimeout_FailsClosed` | meta timeout |
| CONFIG-003 | P | `TestCorruptBodySettings_LoaderReadsKeys` | `OptimisticMining` default; warning |
| CONFIG-004 | N | — | secret-mining default |
| CONFIG-005 | P | `TestOperatorOverrideFence` | replacement of the checkpoint list |
| CONFIG-006 | G | — | nothing detects no-effect settings (GAP-BV-024, 033) |
| CONFIG-007 | C | cap-disabled tests | loader distinguishes literal 0 |
| OBSERVABILITY-001 | C | `Test_HealthLiveness` | — |
| OBSERVABILITY-002 | P | `Test_HealthReadiness*` | each dependency individually |
| OBSERVABILITY-003 | N | — | `blockchain.CheckFSM` has no test anywhere |
| OBSERVABILITY-004 | P | `Test_HealthGRPC*` | no-recursion |
| OBSERVABILITY-005 | P | `TestGetCatchupStatusPopulatesCurrentHeightWhenNotCatchingUp` | active fields; `blocks_fetched` (GAP-BV-035) |
| OBSERVABILITY-006 | P | `PreviousAttempt.ErrorType` assertions | persistence across later success |
| OBSERVABILITY-007 | N | — | metric registration |
| OBSERVABILITY-008 | G | — | dead metrics and wrong labels (GAP-BV-036) |
| OBSERVABILITY-009 | N | — | sub-second histograms |
| STATE-001 | P | IDLE catchup-refusal and floater cells | most matrix cells |
| STATE-002 | P | release-clears-flag tests | the iff property |
| STATE-003 | G | `TestRestoreFSMState_ExhaustedPersistenceRetriesRemainCatching` (GAP-BV-028) | eventual RUN |
| NFR-001 | P | cheap-header tests (subtree fetch counted) | no UTXO-store access |
| NFR-002 | P | byte-cap and prefetch-budget tests | tracked-forks bound; see section 6 |
| NFR-003 | C | `TestCorruptAttemptCap_MultipleBadPeersIndependent` (counters) | end-to-end re-download count |
| NFR-004 | N | — | crash convergence |
| NFR-005 | P | honest-peer-not-wedged counter tests | mixed multi-peer attack |
| NFR-006 | P | `Test_Stop*` | hung dependency |
| NFR-007 | N | — | structural lint |

---

## 6. Tests that do not test what they appear to

These should be fixed or deleted before anyone relies on them. Items marked ✓ were checked by reading the test.

### Vacuous or wired wrong

- ✓ `CircuitBreakerStabilizes` (`catchup_network_resilience_test.go` ~296). It is the only test using the production circuit-breaker configuration. It sends requests as `peer-flapping-002`, reads the breaker for `peer-flapping-001`, and only logs the final state without asserting it. It therefore cannot detect GAP-BV-022.
- ✓ `TestMaliciousPeerFailover` and `TestBlockHandlerWithMaliciousPeer` (`malicious_peer_handling_test.go`). No P2P client is wired, so no peer is ever flagged malicious; a comment in the file says the tests should be updated. Neither tests PEER-003.
- `TestCatchup_MemoryLimitPreCheck/"TruncatesHeadersToFitLimit"` (`catchup_test.go` ~2402) re-implements the truncation arithmetic inline and never calls production code.
- `DetectBrokenHeaderChain`, `DetectLargeHiddenChain` and `Malicious Response Detection` (`catchup_malicious_peer_test.go`) assert only that an error occurred. `DetectBrokenHeaderChain` fails on the ancestor lookup, not the mid-chain break it is named for.
- The "parent transaction ordering error" subtests in `model/Block_test.go` (~4300, ~4920) log or discard the error.
- `test/e2e/daemon/bsv/bsv_invalidblockrequest_test.go` accepts either outcome. It also runs in no CI tier.
- `TestBlockValidation_BlockchainSubscription_TriggersSetMined` (`BlockValidation_test.go` ~4755) checks a condition that holds even if the worker never runs. Its comment names a different notification type from the one the test sends.
- `Test_ValidateBlock`'s "success" subtest asserts an error.

### Named for a mode they do not run

- ✓ `testLongestChainInvalidateBlockWithOldTx` and `testLongestChainInvalidateFork` (`test/sequentialtest/longest_chain`) set only `OptimisticMining = true`. Optimistic mining also requires `OptimisticMiningPeerBlocks`, which defaults to false, so both run the non-optimistic path. **No e2e test exercises optimistic mining.**
- The optimistic unit tests (`floater_fsm_gated_test.go`, `optimistic_requeue_flag_test.go`) pass a pre-cancelled lifecycle context, so the sweeps and workers never run and the end state after an optimistic add is never observed.
- `test/sequentialtest/double_spend/13_*` `...DuplicateTxidAcrossSubtrees...` exercises a double-spend conflict, not a duplicate transaction ID.

### Tests that pin a known defect (invert when the gap is fixed)

| Test | Gap |
|---|---|
| `unified_route_server_test.go` `...NonLegacyBlockKeepsTheCatchupDivert` (`NoError` on divert) | GAP-BV-005 |
| `block_processing_retry_test.go` `RequeueForRetry_New_Block`, `TestBlockProcessingWorkerRetry` | GAP-BV-006, 007 |
| `Client_test.go` `BlockFound` signature | GAP-BV-004 |
| `catchup_promotion_test.go` `TestRestoreFSMState_ExhaustedPersistenceRetriesRemainCatching` | GAP-BV-028 |
| `TestCatchup_ConcurrentCatchupLock` drop assertion; "catchup-in-progress keeps the guard" | GAP-BV-019 |
| `block_processing_retry_test.go` "unbound invalid-transaction verdict recovers via a cached alternative" | GAP-BV-044 |
| `check_duplicate_txs_test.go`, `block_dedup_during_load_test.go` (every duplicate CORRUPT) | GAP-BV-051 |
| `BlockValidation_test.go` ~1618, `quick_validate_test.go` ~528 (empty-input coinbase INCOMPLETE) | GAP-BV-046 |
| `model/Block_test.go` ~603, `cheap_header_no_persist_test.go` ~345 (future-dated INVALID) | GAP-BV-050 |
| `model/update-tx-mined_test.go` `BlockIDCollisionDetection` (post-commit detection) | GAP-BV-011 |
| `model/update-tx-mined_test.go` ~1396 (revert errors swallowed) | GAP-BV-057 |
| `TestBlockValidation_RevalidateIsCalledOnHeaderError` | GAP-BV-016 |
| `catchup_release_policy_test.go` ~65 (expects `ERR_BLOCK_ERROR` from the parent-mined wait) | GAP-BV-014 |
| `longest_chain_test.go` `testLongestChainInvalidateBlockWithOldTx` (post-commit invalidation) | GAP-BV-011 |

### Hidden divergences

- `test/consensus` passes 726 cases and skips 127, mostly with "go-bdk behaviour differs from test expectations". The skips hide divergences from the bitcoin-sv vectors, and the package runs in no CI tier.

---

## 7. Crash-point analysis

A crash point is a moment where the process can stop, or a call can fail, between two writes that the spec treats as a unit. For each one the table gives the state left behind, the mechanism the spec relies on to repair it, and the invariant that must hold once recovery finishes.

**No row in either table is covered by a test that crashes or fails the service at that point.** The only related tests repair synthetic states built by hand (the sweep tests) or cancel a single store call.

### Full validation commit sequence

| # | Point | State left | Repair (spec) | Invariant after recovery |
|---|---|---|---|---|
| N0 | after validation, before AddBlock | no block row; subtree blobs and unmined UTXO records | re-announcement (OPTIMISTIC-003) | no row; no transaction tagged with a nonexistent block ID |
| N1 | AddBlock committed but client timed out | row stored; caller sees an error; re-announcement returns "already exists" | sweep (a) | flags converge |
| N2 | AddBlock → mark existing | cache only | not needed | store is the authority |
| N3 | after AddBlock, before `subtrees_set` | row with both flags false; coinbase possibly missing from TX store (never repaired) | sweep (a) (COMMIT-008, NFR-004) | both flags true within two processing intervals |
| N4 | setting `subtrees_set` fails | stored, caller told failure | sweep (a); COMMIT-003 says report success (GAP-BV-015) | flag converges; caller saw success |
| N5 | `subtrees_set` written, notification lost | `subtrees_set` true, `mined_set` false | sweep (b) | `mined_set` true within one interval |
| N6 | subscription gap during resubscribe | as N5 | sweep (b) | as N5 |
| N7 | queued for tagging, process dies | queue lost; as N5 | sweep (b) at startup | as N5 |
| N8 | tagging worker fails to read the header | skipped with no retry | sweep (b) only | eventually tagged |
| N9 | tagging fails repeatedly | 10 retries, then dropped | sweep (b) re-drives every interval | tagged once the fault clears |
| N10 | between `SetMinedMulti` batches | some transactions tagged | sweep (b) re-runs (idempotent) | every transaction tagged once, no duplicates |
| N11 | inside one batch | SQL: atomic per chunk; Aerospike: per record | as N10 | as N10 |
| N12 | store reports success but a transaction is untagged | coverage error | retry / sweep | no `mined_set` until covered |
| N13 | re-presented transaction found after commit | block tagged and valid | sweep re-detects, then invalidates (GAP-BV-011) | invalid, tags reverted |
| N14 | all tagged, before `mined_set` | children wait in the parent-mined loop | sweep (b) | `mined_set` true |
| N15 | tagging disabled by setting | `mined_set` set with nothing tagged | none (GAP-BV-011) | `mined_set` must not be set |
| N16 | child validated while parent unmined | child waits about 160 s, then fails | re-announcement | child never accepted before parent is mined |
| O1 | optimistic: added, before background validation | block on chain, unvalidated | **sweep accepts it without validating** (GAP-BV-013) | not marked until validated |
| O2 | optimistic: validated, before `subtrees_set` | valid, flag false | sweep (a) | as N3 |
| O3 | optimistic: revalidation queue lost or exhausted | on chain, unvalidated | none (GAP-BV-013) | ends invalid or validated |
| O4 | optimistic: INVALID found, invalidation lost | on chain, `invalid` false | none (GAP-BV-013, 043) | not mined |
| O5 | sweep runs while background validation is in progress, which then invalidates | race | none | invalid ⇒ no tags |
| I1 | invalidation written, before tag revert | invalid block still tagged | sweep (b) if `subtrees_set` | tags reverted, `mined_set` true |
| I2 | descendants of an invalidated block | only the root gets a notification | sweep (b) | every descendant reverted |
| I3 | invalidation concurrent with in-flight tagging | `mined_set` overwritten to true; unset skipped | **none** (GAP-BV-058) | invalid ⇒ no tags |
| I4 | revert fails or a subtree file is missing | errors swallowed, `mined_set` set | **none** (GAP-BV-057) | invalid block with `mined_set` has no tags |
| V1 | revalidation written, before re-tagging | valid, `mined_set` false | sweep (b) if `subtrees_set` | re-tagged |
| V2 | revalidation concurrent with in-flight revert | `mined_set` true with tags gone | **none** (GAP-BV-058) | `mined_set` ⇒ every transaction tagged |

### Quick validation of one block

| # | Point | State left | Repair | Invariant after recovery |
|---|---|---|---|---|
| F0 | before block ID assigned | nothing | re-fetch | unchanged stores |
| F1 | ID assigned, before first wave | ID reserved | retry reuses the same ID | same ID every retry |
| F2 | inside a one-wave call (spend then create, not atomic) | parent spent by a transaction that does not exist yet | replay | each input spent by exactly one existing transaction |
| F3 | inside the store's create wave (`SpendAndCreateMulti`) | some creates done | replay | as F2 |
| F4 | inside the chained spend wave | some spends done | replay | as F2 |
| F5 | between batches | earlier batches applied | replay, reusing the ID from the first transaction | same ID; correct even if that transaction was mined elsewhere before |
| F6 | all batches applied, before AddBlock | locked outputs, no block row | replay, or none if never retried | no orphaned locked outputs |
| F7 | AddBlock fails | as F6; full validation then runs over partial state | full validation | same result as a clean full validation |
| F8 | after AddBlock, before or during unlock | committed, outputs locked; on a non-crash error, subtree files deleted (GAP-BV-056) | **none** (GAP-BV-030) | outputs unlocked; files present |
| F9 | after unlock, before cache update | cache only | not needed | — |
| F10 | async subtree write fails | committed with `subtrees_set`, files never written (GAP-BV-029) | **none** | every `subtrees_set` block has its files |

---

## 8. Invariants to check after every test

These hold at any quiescent point: at least two processing intervals after the last event, with no work in flight. They are checkable from the three stores (blockchain, UTXO, subtree). The proposal is to run them as an oracle in the teardown of every e2e, sequential, chaos and fail-point test, so that every existing test also becomes an invariant test.

The chain-reconciliation oracle on branch `test/chain-reconciliation-oracle` (commit bcae9c0d9, unmerged) already checks UTXO and blob state against the best chain. It needs to be extended with block flags, non-best-chain blocks and locks.

| # | Invariant | Spec IDs | Violated by |
|---|---|---|---|
| I-1 | `subtrees_set` and not invalid ⇒ every subtree file exists (within retention) | COMMIT-002, QUICK-021 | GAP-BV-029, 056 |
| I-2 | `mined_set` and not invalid ⇒ every transaction carries the block ID and height | COMMIT-005 | GAP-BV-011 (setting off), 039, 058 |
| I-3 | invalid and `mined_set` ⇒ no transaction carries the block ID | COMMIT-011 | GAP-BV-057, 058 |
| I-4 | not `subtrees_set` (full-validation route) ⇒ no transaction carries the block ID | COMMIT-004 | — |
| I-5 | `mined_set` ⇒ `subtrees_set` (except pre-assigned-ID blocks) | COMMIT-004, 005 | — |
| I-6 | no block awaits mined-tagging; only blocks with a missing file await `subtrees_set` | COMMIT-008, NFR-004 | sweep failures |
| I-7 | `mined_set(B)` ⇒ `mined_set(parent(B))` | COMMIT-009 | — |
| I-8 | no transaction is mined in two blocks on the best chain | BODY-017 | GAP-BV-011 |
| I-9 | a transaction's longest-chain flag matches whether any of its blocks is on the best chain | COMMIT-005 | reorg timing |
| I-10 | every descendant of an invalid block is invalid; no invalid block is on the main chain | VERDICT-004 | — |
| I-11 | every stored non-invalid block passes `ValidateBlock` (wall-clock rules excepted) | OPTIMISTIC-008 | GAP-BV-013, 043 |
| I-12 | in-memory queues are empty | — | stuck work |
| I-13 | every subtree file written by Block Validation has a finite delete-at-height until persisted | COMMIT-012, QUICK-022 | — |
| I-14 | no output created by a stored block is still locked | QUICK-015 | GAP-BV-030, 056 |
| I-15 | every input is spent by a transaction that exists | QUICK-016, 019 | spend-before-create window |
| I-16 | every stored-invalid block has a retrievable body whose merkle root matches | VERDICT-004 | — |
| I-17 (soft) | each non-invalid `subtrees_set` block's coinbase is in the TX store | COMMIT-001 | best-effort write never repaired |

---

## 9. Test backlog

Priorities: **P0** catches a consensus divergence, data loss or state corruption, or turns an unpinned P0 gap into a failing test. **P1** catches a liveness, DoS or accountability failure. **P2** observability, configuration and hygiene.

"Fails today" marks tests expected to be red until the named gap is fixed. Write them first and mark them as expected failures, so the fix lands with a red-to-green test.

### 9.1 Consensus and differential

| # | P | Type | Test | Asserts | IDs |
|---|---|---|---|---|---|
| 1 | P0 | unit | Coinbase finality (fails today, GAP-BV-045) | nLockTime = H with sequence 0 → INVALID; sequence 0xFFFFFFFF passes; time-based lock; boundaries H and H−1 | BODY-019 |
| 2 | P0 | unit | Context reuse across forks (fails today, GAP-BV-047) | a transaction validated at one tip, re-presented in a sibling block where it is non-final or spends an immature coinbase, is rejected; also a cached subtree re-used across a changed median-time-past | BODY-020 |
| 3 | P0 | unit | Block sigops and BIP30 (fail today, GAP-BV-048, 049) | 20,001 sigops in a 900 KB pre-Genesis block → INVALID; BIP30 with the 91842/91880 exemptions | BODY-021, 022 |
| 4 | P0 | property | BIP34 encoding | for sampled heights and every boundary (1–16, 127/128, 255/256, 32767/32768, 8388607/8388608), the checker accepts exactly `CScript() << h` as a prefix and rejects non-minimal pushes | BODY-009 |
| 5 | P0 | differential | svnode block-vector corpus | blocks recorded from `p2p-fullblocktest.py` and `abc-p2p-fullblocktest.py` with svnode's verdict, replayed through `ValidateBlockWithOptions` on sqlitememory; accept ⇔ accept; known deviations in an allow-list keyed by GAP ID (plan in 9.8) | HEADER-*, BODY-* |
| 6 | P0 | differential | svnode transaction vectors in CI | `test/consensus` in a CI tier, with its 127 skips replaced by an allow-list keyed by vector and reason | BODY-015 |
| 7 | P0 | unit | Wrong-class pins inverted (fail today) | non-merkle duplicate in a bound body → INVALID (GAP-BV-051); empty-input coinbase → INVALID (GAP-BV-046) | BODY-004, 005 |
| 8 | P0 | unit | Future timestamp is not a verdict (fails today, GAP-BV-050) | now + 3 h: not accepted, not persisted, no notification, no strike; accepted after the clock advances. Needs an injectable clock | HEADER-006 |
| 9 | P0 | unit | Re-presentation never becomes tip (fails today, GAP-BV-011) | block N+2 re-including a transaction from block N is never the best tip, including with tagging disabled | BODY-017 |
| 10 | P1 | differential | Difficulty vectors | port svnode `pow_tests.cpp` (retarget, EDA, DAA, testnet minimum) into `services/blockchain` | HEADER-004 |
| 11 | P1 | metamorphic | Body re-chunking | for a random transaction list, every legal subtree split gives the same verdict; swapping two transactions → CORRUPT; duplicating the last → CORRUPT | BODY-001–004 |
| 12 | P1 | invariant | Honest body after any corrupt body | for every CORRUPT sub-class in sequence against one store, an honest body for the same header then commits | VERDICT-002, BODY-001 |
| 13 | P1 | unit | Double-spend e2e asserts the class | upgrade `double_spend/13_*` to assert `ERR_BLOCK_INVALID`, the persisted record and the notification | BODY-013 |
| 14 | P1 | race | Competing forks spending one parent | two fork blocks validated concurrently: one commits, the other is rejected with the right class | BODY-012, 013 |
| 15 | P1 | unit | Header rules on every route | wrong nBits rejected on the quick and unified routes (test checkpoint table) | HEADER-004 |

### 9.2 Verdicts and notifications

| # | P | Type | Test | Asserts | IDs |
|---|---|---|---|---|---|
| 16 | P0 | invariant | Verdict effects table | inject each failure at each validation step; exactly one class, with the table's persist, strike, notify and retry effects | VERDICT-001, 008 |
| 17 | P0 | unit | Notification observed | a capturing producer sees one message per header verdict, parent-invalid child, optimistic old-block-ID INVALID and revalidation INVALID (the last two fail today, GAP-BV-012, 043); keyed by block hash | HEADER-009, VERDICT-005, 009 |
| 18 | P1 | race | Corrupt and honest delivery of one hash concurrently | honest commits; corrupt neither poisons nor deletes the honest body's blobs | VERDICT-002, 007 |
| 19 | P1 | unit | Codes across gRPC | `ProcessBlock` round trip preserves CORRUPT and INVALID, wraps the rest | VERDICT-006 |

### 9.3 Commit sequence and crash consistency

| # | P | Type | Test | Asserts | IDs |
|---|---|---|---|---|---|
| 20 | P0 | property + fail-point | Sweep convergence generator | random fault step (AddBlock, `subtrees_set`, `SetMinedMulti` batch k, `mined_set`, invalidate, revalidate) × kill position × restart; after two intervals all section 8 invariants hold. Covers N3, N5, N10, N14, I1, V1 | COMMIT-*, NFR-004 |
| 21 | P0 | race | Invalidate or revalidate vs in-flight tagging (fails today, GAP-BV-058) | block tagging inside the store; invalidate; release. Invalid ⇒ no tags; revalidated ⇒ `mined_set` implies tags | COMMIT-011, REVALIDATE-003 |
| 22 | P0 | fail-point | Revert failure (fails today, GAP-BV-057) | `SetMinedMulti` fails during revert ⇒ `mined_set` stays false and the sweep retries | COMMIT-011 |
| 23 | P0 | fail-point | Optimistic restart (fails today, GAP-BV-013) | optimistic add, crash before background validation, restart; block is not marked until validated | OPTIMISTIC-008 |
| 24 | P0 | e2e | Optimistic mining end to end | daemon with both optimistic flags true: a peer block that fails in the background (double spend, floater, bad merkle) is reorged out, tags reverted, notification published | OPTIMISTIC-005–007 |
| 25 | P0 | fail-point | `subtrees_set` fails or notification lost (first fails today, GAP-BV-015) | caller gets success; the sweep tags | COMMIT-003, 004 |
| 26 | P0 | unit | Parent-mined wait | un-skip and rewrite the two skipped tests: backoff schedule, re-trigger, final code `ERR_BLOCK_PARENT_NOT_MINED` (fails today, GAP-BV-014) | COMMIT-009 |
| 27 | P1 | fail-point | Tagging retries | 10th failure drops with the `manual_intervention_required` signal; not-found drops at once; sweep re-attempts. Needs the retry constants to become fields | COMMIT-007 |
| 28 | P1 | unit | Non-zero `block_id` above the checkpoint (fails today, GAP-BV-039) | mined-tagging still runs | COMMIT-010 |
| 29 | P1 | unit | `Block` notification does not trigger tagging | drive the real subscription loop with `Block` only; zero tagging calls | COMMIT-004 |
| 30 | P1 | race | Overlapping subtree sweeps (fails today, GAP-BV-059) | at most one pass in flight | COMMIT-008 |
| 31 | P1 | chaos | Daemon killed at each commit log marker | restart on the same data directory; invariants hold. Nightly | NFR-004 |

### 9.4 Quick validation

| # | P | Type | Test | Asserts | IDs |
|---|---|---|---|---|---|
| 32 | P0 | fail-point | Crash and replay at every cut point of the multi-wave apply | replay equals a clean run (the `SpendAndCreateMultiRepeatAtCutPoints` pattern), both outpoint modes | QUICK-016, 017, 019 |
| 33 | P0 | fail-point | Post-AddBlock failures (fail today, GAP-BV-029, 030, 056) | unlock fails, process dies before unlock, async write fails: outputs end unlocked and files present | QUICK-015, 020, 021 |
| 34 | P0 | metamorphic | Quick vs full validation | one generated chain (in-block parents, chained transactions, OP_RETURN, coinbase spends) through quick outpoint-only, quick normal and full validation: same verdict, identical UTXO state; also on single-fault mutations | QUICK-003, 013, 023, 027 |
| 35 | P0 | property | Whole-block idempotent replay | apply twice, N times, or after a random prefix: same UTXO set, same block ID, nothing created twice | QUICK-019 |
| 36 | P0 | race | In-flight store call not cancelled by a sibling failure (regression guard for GAP-BV-055, resolved by #1909) | a sibling failure does not cancel a create in flight; no call starts after cancellation | QUICK-018 |
| 37 | P0 | differential | Route-decision grid | every combination of height, checkpoints, override, flags, store capability, base URL and proof gives the same decision in Block Validation's and legacy netsync's route logic; the override never widens any fast path | QUICK-003, 006, 007, 028, 029 |
| 38 | P1 | unit | Retry semantics, all waves | cap of 10, 2 s backoff, no-progress stop, `ErrSpent` and conflicts fail hard; create wave retries (fails today, GAP-BV-031) | QUICK-017 |
| 39 | P1 | unit | Ordering and files | unlock after AddBlock; no separate notification; DAH = height + retention; no subtree-meta file | QUICK-014, 020, 022 |
| 40 | P1 | unit | Forged previous-output data | inflated satoshis and forged script are re-resolved in normal mode | QUICK-012 |
| 41 | P1 | property | Block-ID reuse | a block whose first transaction was mined elsewhere before must not reuse that block's ID | QUICK-019 |
| 42 | P1 | unit | Startup refusal (fails today, GAP-BV-032) | unspendable-skip with Block Persister enabled refuses to start | QUICK-024 |
| 43 | P1 | differential | Aerospike | replay of F2–F4 against Aerospike (testcontainers, tagged) | QUICK-016–019 |

### 9.5 Catchup and peers

| # | P | Type | Test | Asserts | IDs |
|---|---|---|---|---|---|
| 44 | P0 | unit + property | Circuit breaker under production config (fails today, GAP-BV-022) | with 1 half-open slot and success threshold 2, the breaker closes after two good calls; liveness property over random call sequences; also assert the server's configuration satisfies slots ≥ threshold | CATCHUP-025 |
| 45 | P0 | adversarial peer | Lying-header suite through `catchup()` | bad PoW, nBits, DAA bits, broken link, first header unlinked (fails today, GAP-BV-020), future-dated (no report; fails today, GAP-BV-050), zero merkle, pre-2009, duplicates, 10,001 headers, exact byte cap | CATCHUP-004–009 |
| 46 | P0 | unit | No `mined_set` cleared on any header failure (fails today, GAP-BV-021) | link, checkpoint, DAA and PoW failures clear nothing | CATCHUP-013 |
| 47 | P0 | adversarial peer | Malicious primary refused everywhere | a fake P2P client flags the peer at each of the 7 call sites and mid-block-phase: no outbound request, no strike, caller answered. Replaces the vacuous tests | PEER-003, CATCHUP-001 |
| 48 | P0 | adversarial peer | Truncated and inflated batches | 99 of 100, mid-block cut, duplicated, reordered, substituted hash; inflated or zero declared size: fetch fails, right peer charged once, memory bounded | CATCHUP-017, 018 |
| 49 | P0 | unit + race | Retained catchup request (fails today, GAP-BV-019) | a second request runs after the first ends; announce-storm under `-race` with a goroutine-leak check | CATCHUP-002, 021, 022 |
| 50 | P0 | unit | Malicious reported exactly once (fails today, GAP-BV-025) | across the three report sites; no alternative for a bound INVALID | PEER-005 |
| 51 | P1 | fail-point | Local faults never trip the breaker (fails today, GAP-BV-023) | 5 local store failures during header fetches leave the peer's breaker closed and the peer uncharged | CATCHUP-026, PEER-006 |
| 52 | P1 | unit | Local-fault classification | persistent 429, Block Assembly behind (GAP-BV-026), `legacy` base URL (GAP-BV-027) are never charged | PEER-006 |
| 53 | P1 | fail-point | RUN after the block phase (fails today, GAP-BV-028) | RUN requested after a failed block phase; retried on a later pass after 3 failures | CATCHUP-016, STATE-003 |
| 54 | P1 | adversarial peer | Flip-flopping tip and deep-reorg matrix | a peer switching branches mid-catchup; an honest heavier chain at depths T−1 … 101 | CATCHUP-006, 008, 011, 012 |
| 55 | P1 | chaos | Real-socket slow-loris and resets | 1 byte/s with `Content-Length`; mid-body TCP reset: iteration timeout fires, retries bounded, no goroutine or descriptor leak | CATCHUP-005 |
| 56 | P1 | chaos | Peer churn and partition | primary dies mid-batch and returns; node partitioned to a malicious minority during a reorg | PEER-007, 010, CATCHUP-011 |
| 57 | P1 | property | Corrupt and decline caps | random deliveries, successes and time advances across peers: re-downloads ≤ peers × cap; empty ID never capped; peers independent | PEER-001, 002, NFR-003, 005 |
| 58 | P2 | fuzz | Header response and block-batch readers | arbitrary bytes into the header parser and `/blocks` batch reader: no panic, bounded allocation, correct classification | CATCHUP-004, 017 |
| 59 | P2 | soak | 24 h catchup against a scripted peer cycling honest, withholding and lying | goroutines, descriptors, memory and every cache stay bounded | NFR-002 |

### 9.6 Ingress, queue, lifecycle, observability

| # | P | Type | Test | Asserts | IDs |
|---|---|---|---|---|---|
| 60 | P0 | fail-point | `wait_to_complete` returns the real outcome (fails today, GAP-BV-001, 005) | real workers; does not return before validation ends; returns INVALID; `ERR_SERVICE_ERROR` on a catchup divert | INGRESS-005, 017 |
| 61 | P0 | fail-point | Kafka redelivery (fails today, GAP-BV-003) | in-memory Kafka; message 1 fails locally, message 2 succeeds: message 1 is reprocessed; an unparseable hash does not panic | INGRESS-009 |
| 62 | P0 | chaos | Panic recovery on every worker path (fails today, GAP-BV-054) | a panic in the priority worker, catchup consumer and Kafka handler: process survives, caller gets `ERR_PROCESSING`, next block processed | STARTUP-007 |
| 63 | P0 | invariant | Announcement paths agree | one random set of announcements through `BlockFound`, `ProcessBlock` and Kafka gives identical stored state, queue state, charges and error class; announcing twice equals once | INGRESS-001, 003, 004 |
| 64 | P0 | race | Fork limit and duplicate merge (fails today, GAP-BV-008) | real workers, blocking validator: one block per fork, one validation per hash, refused duplicate's sources merged | QUEUE-009, 010 |
| 65 | P0 | property | Classifier never underflows (fails today, GAP-BV-009) | random best, parent and threshold, including best < threshold | QUEUE-001, 002 |
| 66 | P0 | fail-point | Alternatives then bounded retries (fails today, GAP-BV-006, 007) | primary fails, alternative serves; retries bounded; at most 3 sources | QUEUE-005–007 |
| 67 | P0 | unit | State matrix as a table | each FSM state (real blockchain service on sqlite) × each operation asserts its cell, generated from the spec table | STATE-001 |
| 68 | P1 | model-based | Priority queue | random operations against a reference model of the spec ordering, with an injectable clock: ageing boundaries, skip promotion, FIFO, liveness | QUEUE-003, 004 |
| 69 | P1 | fuzz | `BlockFound` and `ProcessBlock` requests | arbitrary hash length, base URL, height, block ID and bytes: no panic, error from the allowed set | INGRESS-002, 004, 011, 012 |
| 70 | P1 | fail-point | Dependency outages never persist invalid | each of the five hard dependencies fails at call N through the validation path: never `invalid = true`, always LOCAL | STARTUP-009, VERDICT-008 |
| 71 | P1 | unit | Lifecycle | `Init` failures; consumer not started before IDLE exit; gRPC served while IDLE; wedged Kafka close bounded (GAP-BV-042); resubscription | STARTUP-001–005, 008 |
| 72 | P2 | unit | Guard tests | every `BlockValidation` setting is read by non-test code (fails today, GAP-BV-024, 033); every registered metric is updated, with correct labels and sub-second precision (fails today, GAP-BV-036) | CONFIG-006, OBSERVABILITY-007–009 |
| 73 | P2 | unit | Health | `CheckFSM` table; each readiness dependency; no gRPC self-recursion | OBSERVABILITY-002–004 |

### 9.7 Run existing tests that are not running

| # | P | Action |
|---|---|---|
| 74 | P0 | Put `test/consensus` in a CI tier (see item 6). |
| 75 | P0 | Merge the chain-reconciliation oracle, move it where `make test` runs its tests, and call it from teardown in every e2e, sequential and chaos test. |
| 76 | P1 | Triage `test/e2e/daemon/wip/` and promote the viable invalid-block, ban-score and unmined-reorg tests to `ready/`. |
| 77 | P1 | Add a nightly job for `make network-chaos-test` (multinode kill, partition, latency) and a weekly job for the split-service variant. |
| 78 | P1 | Replace chaos scenario 08's simulated functions with a real daemon, and point Toxiproxy at Teranode's own connections (see 10.6). |
| 79 | P2 | Add the six existing but unlisted fuzz targets (including `FuzzInpointDedup`) to `FUZZ_TARGETS` and `fuzz.yaml`. |

### 9.8 Differential-testing plan

**What exists locally** (`/Users/sg/bsv-blockchain/bitcoin-sv`):

- **Static JSON vectors:** `src/test/data/{script_tests,tx_valid,tx_invalid,sighash}.json`.
- **Block vectors, generated rather than stored:** `test/functional/p2p-fullblocktest.py` and `abc-p2p-fullblocktest.py`. Each case carries an expected reject reason, covering:
    - header: `bad-diffbits`, `bad-txnmrklroot`;
    - coinbase: `bad-cb-amount`, `bad-cb-length`, `bad-cb-missing`, `bad-tx-coinbase`;
    - sigops: `bad-blk-sigops`, `bad-txn-sigops`;
    - transactions: `bad-txns-duplicate`, `bad-txns-in-belowout`, `bad-txns-inputs-missingorspent`, `bad-txns-nonfinal`, `bad-txns-premature-spend-of-coinbase`.
- **Further functional tests:** sigops limits, BIP68, BIP65, BIP66, CSV activation and coinbase P2SH.
- **C++ unit vectors to port:** `pow_tests.cpp`, `checkpoints_tests.cpp`, `blockcheck_tests.cpp`, `merkle_tests.cpp`, `excessiveblock_tests.cpp`, `sigopcount_tests.cpp`.

**Stages:**

1. **Record (offline, occasional).** Patch the Python block tests to dump each block with its parent chain, svnode's verdict and reject reason into a JSON corpus under `test/consensus/blockvectors/`. Run them against svnode regtest with checkpoints disabled, using the svnode docker image already used by legacy-sync.
2. **Replay (PR unit).** Feed each vector to `ValidateBlockWithOptions` on sqlitememory with regtest parameters and no checkpoints, after applying its parent blocks. This needs no daemon.
3. **Verdict mapping.** Pass if svnode accepts ⇔ Teranode accepts, and a svnode rejection gives `ERR_BLOCK_INVALID`, `ERR_BLOCK_CORRUPT` or `ERR_BLOCK_POLICY_DECLINED`, never success. Known deviations go in an allow-list keyed by GAP ID, so that fixing a gap fails the test until its entry is removed. Expected entries today: `bad-txns-nonfinal` for the coinbase (GAP-BV-045), `bad-blk-sigops` pre-Genesis (GAP-BV-048), `bad-blk-length` (a declared deviation, BODY-018), and `time-too-new` (GAP-BV-050).
4. **Reverse direction (nightly).** Un-skip `TestSVNodeValidatesTeranodeBlocks` and have svnode ingest blocks Teranode produced, including blocks from the quick-vs-full generator in item 34.

---

## 10. Test infrastructure roadmap

Most of the backlog depends on a handful of shared pieces. In order:

| # | Build | Why | Unlocks | Size |
|---|---|---|---|---|
| 1 | **Shared fault-injecting store and client wrappers** (`test/utils/faultstore`): UTXO, blob, blockchain client and store. One rule plan: fail on call N, every N, with probability p; delay; block until released; panic; **apply then return an error** ("applied but acknowledgement lost"). Records the call log. Plus a "real stack" builder in `testhelpers` (sqlitememory blockchain and UTXO, memory blobs, in-memory Kafka, `httptest` peer) | Replaces about 30 one-off wrappers copied into individual test files; makes fault tests one-liners; fixes the remaining mocked-blockchain and mocked-Kafka usage that AGENTS.md forbids | 9.2, 9.3, 9.4, 9.6; sweep test over "fail call k" | M |
| 2 | **Chain oracle merged and extended** with the section 8 invariants (block flags, non-best-chain and invalid blocks, locks, files), runnable in-process against a test daemon and called from teardown | Turns every existing test into an invariant test | every crash, chaos and e2e test | M |
| 3 | **Fail-point package** (`util/failpoint`). Build-tag gated so production binaries carry no cost. Named sites, e.g. `bv/setTxMinedStatus/before-SetBlockMinedSet`. Actions: return error, sleep, block, panic, exit. Enabled from tests, an environment variable, or a tag-gated debug endpoint. A consistency test fails if a name has no site. About 25 sites | The only way to reach crash windows inside a function (between two statements), rather than at interface boundaries | section 7 crash points | M |
| 4 | **Hard crash for the test daemon.** In-process `Crash()` that skips the graceful shutdown path; later a subprocess mode that kills with signal 9 | Today `Stop` is graceful and completes the work a crash would interrupt | crash-then-restart-then-oracle tests | M, then L |
| 5 | **Scripted peer** (`testhelpers/scriptedpeer`): a real `httptest` DataHub serving headers, blocks, subtrees and subtree data from a fork-able chain model. Per-request scripts: serve, truncate, lie about length, drip, stall, reset, switch chain. Request log; one shared fake P2P client with recorders; fault-injecting `net.Listener` | `httpmock` cannot express socket-level faults; 7 fake P2P clients are duplicated today | 9.5 | M |
| 6 | **Block Validation fuzz targets**: header response, block batch, subtree data, Kafka message, `ProcessBlock` request, BIP34 coinbase, body binding. Generate the `fuzz.yaml` matrix from `FUZZ_TARGETS` | The service has no fuzz target | 9.5, 9.6 | M |
| 7 | **Metamorphic helpers**: generator, independent reference model, normalised store snapshot | Catches ordering and batching divergences no example test finds | 9.1, 9.4 | M |
| 8 | **Injectable clock** at the timestamp rule, retry backoff, circuit breaker and TTL caches; retry constants turned into fields | Time-dependent rules and drop paths are untestable or take minutes today | 8, 27, 44, 57, 68 | M |
| 9 | **CI wiring**: `test/consensus` and the non-docker `test/` packages; a fail-point build pass; multinode chaos nightly; `wip/` triage | Existing tests that never run | 9.7 | S–M |
| 10 | **Differential corpus** (9.8) | The only independent oracle for consensus rules | 9.1 | M, then L for live |

---

## 11. Spec changes needed for testability

Writing the tests showed places where the spec is ambiguous, untestable or wrong. Suggested amendments:

| ID | Problem | Suggested change |
|---|---|---|
| INGRESS-005 | Silent on caller timeout | "Bounded by the request context; on expiry the block stays queued and the caller receives a context error." |
| INGRESS-009 | Unachievable with the current consumer (GAP-BV-003) | Restate as an observable contract: "an uncommitted offset is eventually reprocessed, even when a later offset on the same partition succeeds." |
| QUEUE-004 | "Whether or not the queue was modified" leaks implementation | "The order of two consecutive dequeues depends only on queue state and the clock at each dequeue." |
| QUEUE-011 | Does not say what happens to a block over the fork cap | State reject or queue. Today: logged and queued. |
| HEADER-006 | No outcome class or error code | Assign one (the new CLOCK-DEFERRED row in VERDICT-001), with its error code. |
| HEADER-010 | "Exactly once" is not observable | Name a counter or log field that tests can read. |
| BODY-005 | "Absent" is undefined | Define it precisely: nil coinbase, as distinct from a coinbase with an empty input list. |
| BODY-017 | Second sentence conflicts with Open Question 3 | Resolve Open Question 3 first. |
| BODY-020 | "Change of that context" has no key | "A validation result is keyed by (height, parent median-time-past)." |
| QUICK-002 | Possibly conflicts with catchup: the catchup loop waits for Block Assembly and fails the cycle before quick validation can fall back (reported, not verified) | Verify, then align QUICK-002 with BV-CATCHUP-019. |
| QUICK-030 | Listed coinbase maturity as trusted in outpoint-only mode, but the store enforces it | **Corrected in the spec.** |
| STARTUP-006 | Conflicts with observed behaviour: producer creation failure exits the process | Decide "fail startup" or "report not ready" (GAP-BV-034). |
| CATCHUP-025 | "Always able to return to closed" needs a time model | Pair with the injectable clock; state liveness as "from any state, a finite run of successes returns to closed". |
| CONFIG-006 | "Has the effect its documentation describes" is not testable | "Every setting is read by non-test code, and its documentation is generated from a test-verified table." |
| OBSERVABILITY-007 | Lists categories, not names | Give the normative metric names. |

---

## 12. Defects found during this assessment

Each was confirmed by reading the code, and is now recorded in the spec's Conformance Gaps.

| Gap | Defect | Severity |
|---|---|---|
| GAP-BV-054 | The priority-queue workers, the catchup consumer and the Kafka handler goroutine have no panic recovery. A panic on any of them terminates the node. Resolved by bsv-blockchain/teranode#2012. | High |
| GAP-BV-055 | The chained create wave of quick validation can be cancelled while a store call is in flight. Resolved by bsv-blockchain/teranode#1909 after this assessment. | Medium |
| GAP-BV-056 | On the quick route, an error after `AddBlock` (e.g. unlock fails) deletes the committed block's subtree files and leaves its outputs locked. Resolved by bsv-blockchain/teranode#2011. | High |
| GAP-BV-057 | Errors while reverting an invalid block's mined tags are swallowed, and `mined_set` is set anyway, so stale tags are never revisited. Resolved by bsv-blockchain/teranode#2016. | High |
| GAP-BV-058 | `SetBlockMinedSet` and `SetBlockSubtreesSet` are unconditional updates. A tagging run in flight can overwrite an invalidation or revalidation. Derived from the code; not reproduced. | High |
| GAP-BV-059 | The subtrees sweep is not single-flight, unlike the mined sweep. | Low |
| GAP-BV-060 | On the default non-optimistic path, a contextual header failure (median time past, version floor) is not published as an invalid-block notification, so the peer is never struck. Found in review; proven by `TestGapBV060_UnboundHeaderFinalVerdictPublishesNotification`. | Medium |
| GAP-BV-061 | Catchup logs and continues when reading or clearing `mined_set` on the old branch fails, leaving stale mined tags after the reorg. Found in review; proven by `TestGapBV061_CatchupAbortsWhenOldBranchMinedSetClearFails`. | High |

The spec also gained:

- a CLOCK-DEFERRED row in BV-VERDICT-001;
- a correction to BV-QUICK-030;
- an extension of GAP-BV-013, recording that optimistic revalidation exhaustion leaves a block unvalidated even without a crash;
- an extension of GAP-BV-050, recording the block-path behaviour and the tests that pin it.

Smaller findings not raised as gaps:

- descendants of an invalidated block are reverted only by the sweep, not by notification;
- a tagging worker's header-read failure is retried only by the sweep;
- the model package's in-flight block map is global and keyed by block ID alone, so parallel tests can collide.
