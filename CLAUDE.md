# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Teranode is a horizontally scalable BSV Blockchain node implementation using a microservices architecture. It achieves over 1 million transactions per second through distributed processing across multiple machines.

## Common Development Commands

### Build Commands
```bash
# Build main teranode binary with dashboard
make build

# Build without dashboard
make build-teranode

# Build for CI with race detection
make build-teranode-ci

# Build teranode CLI tool
make build-teranode-cli

# Build specific components
make build-chainintegrity
make build-tx-blaster
```

### Testing Commands
```bash
# Run all tests except integration tests
make test

# Run long-running tests
make longtest

# Run sequential tests
make sequentialtest

# Run smoke tests (basic functionality)
make smoketest

# Run all tests
make testall

# Run a single test
go test -v -race -tags "testtxmetacache" -run TestNameHere ./path/to/package

# Test Retry Support (for flaky tests)
# Automatically retry failed tests to handle timing/race issues
# Default: 3 retries for all test targets

# Unit tests with retry (gotestsum native --rerun-fails)
make test TEST_RETRY_COUNT=3
make longtest TEST_RETRY_COUNT=5

# E2E tests with retry (custom retry wrapper with timeout extension)
make smoketest TEST_RETRY_COUNT=3
make sequentialtest TEST_RETRY_COUNT=5 TEST_RETRY_DELAY=3

# Disable retries (set to 0 or 1)
make test TEST_RETRY_COUNT=0
make smoketest TEST_RETRY_COUNT=1

# Database-specific sequential tests with retry
make sequentialtest-aerospike TEST_RETRY_COUNT=5
make sequentialtest-postgres TEST_RETRY_COUNT=3
make sequentialtest-sqlite TEST_RETRY_COUNT=3

# Flaky test reports (JSON format):
# - Unit tests: /tmp/teranode-test-results/unit-test-flaky.json
# - Long tests: /tmp/teranode-test-results/longtest-flaky.json
# - Sequential tests: console output with flaky test summary

# See docs/testing/test-retry-mechanism.md for full documentation
```

### Linting Commands
```bash
# Check only changed files vs main branch
make lint

# Check only unstaged/untracked changes
make lint-new

# Check all files
make lint-full

# Fix gci linting for Go files
gci write --skip-generated -s standard -s default <filename>
```

### Development Mode
```bash
# Run teranode and dashboard in development mode
make dev

# Run only teranode
make dev-teranode

# Run only dashboard
make dev-dashboard
```

## High-Level Architecture

### Microservices Structure
Teranode consists of multiple specialized services communicating via gRPC and Kafka:

**Core Services:**
- **Asset Server** (`services/asset/`): HTTP/WebSocket interface to blockchain data stores
- **Propagation** (`services/propagation/`): Receives and forwards transactions (gRPC/UDP/HTTP)
- **Validator** (`services/validator/`): Validates transactions against consensus rules
- **Block Validation** (`services/blockvalidation/`): Validates complete blocks
- **Block Assembly** (`services/blockassembly/`): Assembles new blocks from validated transactions
- **Blockchain** (`services/blockchain/`): Manages blockchain state and FSM
- **Subtree Validation** (`services/subtreevalidation/`): Validates merkle subtrees

**Overlay Services:**
- **P2P** (`services/p2p/`): Peer-to-peer network communication
- **RPC** (`services/rpc/`): Bitcoin-compatible JSON-RPC interface
- **Legacy** (`services/legacy/`): Backward compatibility with existing Bitcoin nodes

**Data Stores:**
- **UTXO Store** (`stores/utxo/`): Manages unspent transaction outputs (Aerospike-backed)
- **Blob Store** (`stores/blob/`): Stores transactions and subtrees (S3/filesystem)
- **Blockchain Store** (`stores/blockchain/`): Block header and chain state (PostgreSQL)

### Communication Patterns
- **gRPC**: Service-to-service synchronous communication
- **Kafka**: Asynchronous event streaming between services
- **HTTP/WebSocket**: External client interfaces
- **UDP/IPv6 Multicast**: High-performance transaction propagation

### Key Design Patterns
1. **Horizontal Scaling**: Services can be deployed across multiple machines
2. **Event-Driven**: Kafka topics for decoupled communication
3. **UTXO Model**: Bitcoin's unspent transaction output tracking
4. **Merkle Trees**: Binary hash trees for transaction inclusion proofs
5. **Two-Phase Commit**: Distributed transaction consistency

## Configuration

### Settings Files
- `settings.conf`: Default settings and environment-specific overrides
- `settings_local.conf`: Local development overrides (not committed)
- Environment contexts: `dev`, `test`, `docker`, `operator`

### Port Configuration
Services use standardized ports with optional prefixes for multi-node setups:
- Asset Server: 8090
- RPC: 9292
- P2P: 9905
- Blockchain gRPC: 8087
- Validator gRPC: 8081

## Available Agents

Claude will automatically use specialized agents in `.claude/agents/` when appropriate:

- **bitcoin-expert**: Bitcoin protocol, consensus rules, cryptography (automatically consulted for Bitcoin-specific tasks)
- **test-writer-fixer**: Automatically runs tests after code changes
- **api-tester**: API load testing and contract validation
- **backend-architect**: System design and architecture decisions

These agents work together - for example, when implementing a new Bitcoin feature:
1. bitcoin-expert provides protocol guidance
2. backend-architect designs the implementation
3. test-writer-fixer ensures tests pass
4. performance-benchmarker validates performance

## Bitcoin-Specific Context

### Bitcoin Expert Agent
The project includes a Bitcoin expert agent (`.claude/agents/bitcoin-expert.md`) that should be consulted for:
- **Protocol Questions**: Bitcoin consensus rules, script validation, transaction structure
- **Cryptography**: ECDSA signatures, hash functions, Merkle trees
- **BSV-Specific Features**: Restored opcodes, unbounded script sizes, large block handling
- **Implementation Guidance**: When porting bitcoin-sv functionality to teranode

**Usage**: Reference `.claude/agents/bitcoin-expert.md` when working on:
- Transaction validation logic
- Script interpreter implementation
- Consensus rule enforcement
- UTXO management
- Block validation
- Any Bitcoin protocol-specific features

### Key Bitcoin Concepts in Teranode
- ECDSA signatures on secp256k1 curve
- Bitcoin Script (stack-based, Forth-like)
- UTXO transaction model
- Merkle tree block structure
- Proof-of-Work consensus
- BSV's restored opcodes (OP_SUBSTR, OP_LEFT, OP_RIGHT, etc.)
- Unbounded transaction and script sizes

## Testing Strategy

### Test Categories
1. **Unit Tests**: Package-level tests with mocks
2. **Integration Tests**: Multi-service interaction tests
3. **Consensus Tests** (`test/consensus/`): Bitcoin script validation
4. **E2E Tests** (`test/e2e/`): Full system tests with containers
5. **Sequential Tests**: Order-dependent test scenarios

### Test Tags
- `testtxmetacache`: Small cache for testing
- `largetxmetacache`: Production cache size
- `aerospike`: Tests requiring Aerospike

## Coding Conventions

**All code must follow the standards defined in [`docs/references/codingConventions.md`](docs/references/codingConventions.md).**

This includes:
- Naming conventions (packages, variables, functions, interfaces, types, files)
- Error handling patterns
- Concurrency best practices
- Testing standards (testify/require, table-driven tests)
- Code formatting and linting rules
- Commenting guidelines

**Key highlights:**
- File names: `snake_case.go` format (enforced by CI)
- Variables: CamelCase for exported, camelCase for internal
- Functions: VerbNoun pattern (CalculateTotal, ReadFile)
- Getters: No "Get" prefix - use `Name()` not `GetName()`
- Interfaces: Single-method ends in "-er" (Reader, Writer)
- Tests: Use `require` from testify, avoid `t.Parallel()` unless testing concurrency
- Error handling: Always check `if err != nil`, use `errors.New` not `fmt.Errorf`
- Comments: Explain "why" not "what"

## Development Notes

- Always run linting before commits: `make lint`
- Use `gci` for import formatting in Go files
- Follow existing patterns for new services (check similar services first)
- Protobuf files generate Go code via `make gen`
- Dashboard is a Svelte application in `ui/dashboard/`
- Use TestContainers for integration tests requiring external services
- Don't use mock blockchain client/store - you can use a real one using the sqlitememory store
- Don't use mock kafka - you can use in_memory_kafka.go
- **Log messages must always be on a single line** - never use multi-line log statements

### Service Interface Design Pattern

When creating or updating service interfaces and clients, follow this pattern to avoid exposing protobuf/gRPC types:

**Interface Layer** (`Interface.go`):
- Define interfaces using native Go types and existing domain types (e.g., `*PeerInfo`, `[]string`, `bool`, `error`)
- Do NOT expose protobuf types (e.g., `*p2p_api.GetPeersResponse`) in interface signatures
- Use simple, idiomatic Go return types: `error` for success/fail, `bool` for yes/no, `[]string` for lists
- Prefer existing domain structs over creating new minimal types

**Client Layer** (`Client.go`):
- Keep the protobuf/gRPC import for internal use (e.g., `import "github.com/bsv-blockchain/teranode/services/p2p/p2p_api"`)
- Maintain internal gRPC client field (e.g., `client p2p_api.PeerServiceClient`)
- Public methods match the interface signatures (native types)
- Convert between native types and protobuf types internally using helper functions

**Benefits**:
- Cleaner API boundaries between services
- Reduces coupling to gRPC implementation details
- Makes interfaces more testable (no protobuf dependencies needed for mocks)
- Uses idiomatic Go types that are easier to work with

**Example**:
```go
// Interface.go - Clean, no protobuf types
type ClientI interface {
    GetPeers(ctx context.Context) ([]*PeerInfo, error)
    BanPeer(ctx context.Context, peerID string, duration int64, reason string) error
    IsBanned(ctx context.Context, peerID string) (bool, error)
    ListBanned(ctx context.Context) ([]string, error)
}

// Client.go - Internal conversion
type Client struct {
    client p2p_api.PeerServiceClient // gRPC client
}

func (c *Client) GetPeers(ctx context.Context) ([]*PeerInfo, error) {
    resp, err := c.client.GetPeers(ctx, &emptypb.Empty{})
    if err != nil {
        return nil, err
    }
    // Convert p2p_api types to native PeerInfo
    return convertFromAPIResponse(resp), nil
}
```

## Git Workflow (Fork Mode)

All developers work in forked repositories with `upstream` remote pointing to the original repo.

### Pushing Work
```bash
# Always sync with upstream first
git fetch upstream
git reset --hard upstream/main

# If conflicts occur: STOP and ask user for resolution guidance
# After resolving (or if no conflicts):
git push origin <current-branch>

# Display push result message including PR creation links
```

**Important**: Never auto-resolve merge conflicts. Always show conflicting files and wait for user approval on resolution strategy.

### Creating New Branches
```bash
# Always branch from synced main
git checkout main
git fetch upstream
git reset --hard upstream/main
git checkout -b <new-branch-name>
```

### Quick Reference
- **Push work**: Sync upstream → resolve conflicts (with user approval) → push to fork → show PR link
- **New branch**: Switch to main → sync upstream → create branch
- **Sync with upstream**: `git checkout main && git fetch upstream && git reset --hard upstream/main`

<!-- gitnexus:start -->
# GitNexus — Code Intelligence

This project is indexed by GitNexus as **teranode** (27349 symbols, 94911 relationships, 300 execution flows). Use the GitNexus MCP tools to understand code, assess impact, and navigate safely.

> If any GitNexus tool warns the index is stale, run `npx gitnexus analyze` in terminal first.

## Always Do

- **MUST run impact analysis before editing any symbol.** Before modifying a function, class, or method, run `gitnexus_impact({target: "symbolName", direction: "upstream"})` and report the blast radius (direct callers, affected processes, risk level) to the user.
- **MUST run `gitnexus_detect_changes()` before committing** to verify your changes only affect expected symbols and execution flows.
- **MUST warn the user** if impact analysis returns HIGH or CRITICAL risk before proceeding with edits.
- When exploring unfamiliar code, use `gitnexus_query({query: "concept"})` to find execution flows instead of grepping. It returns process-grouped results ranked by relevance.
- When you need full context on a specific symbol — callers, callees, which execution flows it participates in — use `gitnexus_context({name: "symbolName"})`.

## When Debugging

1. `gitnexus_query({query: "<error or symptom>"})` — find execution flows related to the issue
2. `gitnexus_context({name: "<suspect function>"})` — see all callers, callees, and process participation
3. `READ gitnexus://repo/teranode/process/{processName}` — trace the full execution flow step by step
4. For regressions: `gitnexus_detect_changes({scope: "compare", base_ref: "main"})` — see what your branch changed

## When Refactoring

- **Renaming**: MUST use `gitnexus_rename({symbol_name: "old", new_name: "new", dry_run: true})` first. Review the preview — graph edits are safe, text_search edits need manual review. Then run with `dry_run: false`.
- **Extracting/Splitting**: MUST run `gitnexus_context({name: "target"})` to see all incoming/outgoing refs, then `gitnexus_impact({target: "target", direction: "upstream"})` to find all external callers before moving code.
- After any refactor: run `gitnexus_detect_changes({scope: "all"})` to verify only expected files changed.

## Never Do

- NEVER edit a function, class, or method without first running `gitnexus_impact` on it.
- NEVER ignore HIGH or CRITICAL risk warnings from impact analysis.
- NEVER rename symbols with find-and-replace — use `gitnexus_rename` which understands the call graph.
- NEVER commit changes without running `gitnexus_detect_changes()` to check affected scope.

## Tools Quick Reference

| Tool | When to use | Command |
|------|-------------|---------|
| `query` | Find code by concept | `gitnexus_query({query: "auth validation"})` |
| `context` | 360-degree view of one symbol | `gitnexus_context({name: "validateUser"})` |
| `impact` | Blast radius before editing | `gitnexus_impact({target: "X", direction: "upstream"})` |
| `detect_changes` | Pre-commit scope check | `gitnexus_detect_changes({scope: "staged"})` |
| `rename` | Safe multi-file rename | `gitnexus_rename({symbol_name: "old", new_name: "new", dry_run: true})` |
| `cypher` | Custom graph queries | `gitnexus_cypher({query: "MATCH ..."})` |

## Impact Risk Levels

| Depth | Meaning | Action |
|-------|---------|--------|
| d=1 | WILL BREAK — direct callers/importers | MUST update these |
| d=2 | LIKELY AFFECTED — indirect deps | Should test |
| d=3 | MAY NEED TESTING — transitive | Test if critical path |

## Resources

| Resource | Use for |
|----------|---------|
| `gitnexus://repo/teranode/context` | Codebase overview, check index freshness |
| `gitnexus://repo/teranode/clusters` | All functional areas |
| `gitnexus://repo/teranode/processes` | All execution flows |
| `gitnexus://repo/teranode/process/{name}` | Step-by-step execution trace |

## Self-Check Before Finishing

Before completing any code modification task, verify:
1. `gitnexus_impact` was run for all modified symbols
2. No HIGH/CRITICAL risk warnings were ignored
3. `gitnexus_detect_changes()` confirms changes match expected scope
4. All d=1 (WILL BREAK) dependents were updated

## CLI

- Re-index: `npx gitnexus analyze`
- Check freshness: `npx gitnexus status`
- Generate docs: `npx gitnexus wiki`

<!-- gitnexus:end -->
