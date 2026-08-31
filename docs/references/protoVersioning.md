# Protobuf Package Versioning Policy

Teranode's `.proto` files (the `*_api` service definitions under `services/*/*_api/`,
plus `pruner_api`) do not carry a version suffix in their `package` declaration
today. That's a deliberate, temporary state, not a claim that these packages have
never changed shape.

Several of them have already taken breaking changes without a package version
bump:

- `blockassembly_api.AddTxBatchColumnarRequest` — the parent-vout layout moved
  from fields 8/9 to fields 10/11 (`reserved 8, 9;`).
- `validator_api.ValidateTransactionRequest` — field 10 (`parent_metadata`) was
  removed (`reserved 10;`), superseded by field 12.
- `blockchain_api.FSMEventType` / `FSMStateType` — enum value 3 was removed
  (`reserved 3;`).

`reserved` is corruption hygiene, not a versioning strategy: it stops the old
number/name being reused so an old sender's bytes can't be silently decoded as
the new field, but it does not make the two shapes interoperable. An old client
still sending the old field to a new server just has that data dropped as
unknown. Each of the cases above was handled by a deliberate, hand-reasoned
mechanism instead (a documented upgrade-ordering requirement, in the validator
case) rather than by a version bump. This policy is about the case that hasn't
come up yet: a change that `reserved` can't express -- an RPC whose
request/response shape changes, or streaming semantics that flip -- where old
and new must coexist on the wire during a rollout.

## The rule

- A package with no version suffix (`package blockchain_api;`) is implicitly
  **v1** -- the unsuffixed name IS v1, not a placeholder for it. Nothing needs
  to change about it as long as every change to that `.proto` stays
  wire-compatible (new optional fields, new RPCs, new enum values appended at
  the end, `reserved` on removed/renumbered fields, etc.).
- A version suffix is only added **at the moment a breaking change is
  introduced that `reserved` can't express**, and the first suffixed version is
  **`.v2`**, not `.v1` -- the unsuffixed package already occupies `v1`, and every
  node still speaking the old format is a `v1` peer. Renaming the incompatible
  format to `.v1` would claim a label the deployed format already owns.
- Do not pre-emptively add a version suffix to a package that hasn't had a
  breaking change. The proto package name is part of every gRPC method path
  (`/<package>.<Service>/<Method>`) and every message's fully-qualified name, so
  renaming it breaks every RPC on the service, not just the message that
  changed -- that cost is paid once, when it's actually needed. (It does not
  change Go imports: every file here sets `option go_package` explicitly, so
  `protoc-gen-go` derives the Go package/import path from that, not from the
  proto package name.)

Each `.proto` file's `package` declaration carries a short comment recording
this:

```proto
// Version: v1 (implicit). No version suffix; the unsuffixed name IS v1. A
// breaking change adds the next suffixed version (v2) alongside it -- see
// docs/references/protoVersioning.md.
package blockchain_api;
```

## When a breaking change lands

1. Decide the coexistence question first -- it decides the layout:
   - **Cutover** (every caller upgrades atomically, no mixed-version window):
     rename the package in place (`blockchain_api` → `blockchain_api.v2`),
     update the comment, regenerate (`make gen`).
   - **Side by side** (the common case -- Teranode is a multi-node network with
     independently upgraded services, so a breaking wire-format change
     generally can't assume every peer/service upgrades atomically): do **not**
     rename in place, since that removes the v1 service entirely and leaves
     nothing for old peers to talk to. Instead add a new `.proto` in its own
     directory with its own package and `go_package`
     (e.g. `services/blockchain/blockchain_api/v2/blockchain_api.proto`,
     `package blockchain_api.v2;`), give it its own `make gen` stanza, and
     register both services on the server so `v1` and `v2` are both reachable
     during the rollout window. A distinct `go_package` is required --
     `--go_opt=paths=source_relative` emits next to the source file, so two
     versions sharing a directory and Go package name would redeclare every
     generated symbol.

     See how `validator_txmeta_wireFormat` lets two Kafka tx-meta batch formats
     (`v1`/`v2`) coexist on the same topic during a rollout
     ([kafkaMessageFormat.md](kafkaMessageFormat.md)), and how the P2P/legacy
     admin gRPC auth gate (`adminProtectedMethods()` in
     `services/p2p/Server.go`, the `protectedMethods` map in
     `services/legacy/Server.go`) was rolled out method-by-method rather than
     as an atomic cutover -- the same "old and new must coexist mid-rollout"
     concern applies to a versioned proto package.

2. Sweep for hardcoded fully-qualified names that a package rename or addition
   doesn't update automatically:
   - The admin-auth allowlists -- `adminProtectedMethods()` in
     `services/p2p/Server.go` and the `protectedMethods` map in
     `services/legacy/Server.go` -- key on literal method path strings
     (`"/p2p_api.PeerService/BanPeer"`). `util/grpc_helper.go`'s
     `CreateAuthInterceptor` **fails open**: a method path not present in the
     map bypasses authentication entirely. Under a side-by-side `v2`, the new
     package's method paths need their own entries or the `v2` RPCs ship
     unauthenticated. P2P has `TestAdminProtectedMethodsCoverAllRPCs`
     (`services/p2p/server_auth_test.go`) deriving expected paths from
     `ServiceDesc.Methods` to catch omissions; the legacy map has no equivalent
     coverage test today -- add one if you touch that map.
   - `grpcurl`/tooling invocations that hardcode the qualified service name,
     e.g. `scripts/docker_host_fire_run_event.sh`.
   - Qualified type references in other `.proto` files, and in non-Go
     deployment config that names generated types by qualified name, e.g.
     `deploy/docker/base/kafka-console-config.yml`'s `kafkamessage.*` entries.
3. Only remove the old version's code path once the rollout window has closed
   and no node in the network can still be sending the old format.

Today, detecting that a change is breaking is left to reviewer attention --
there's no `buf.yaml` or equivalent compatibility gate in this repo, and
`make gen` invokes `protoc` directly with no schema-diff check. The `reserved`
blocks above are evidence that breaks do land; until an automated gate exists,
treat this policy as review discipline, not an enforced rule.

## Scope

This policy covers any `.proto` whose messages cross a process boundary:

- the service contracts in `services/*/*_api/*.proto` and
  `services/pruner/pruner_api/pruner_api.proto`;
- `model/model.proto` and `errors/error.proto` -- these are wire-facing, not
  internal-only: `model.MiningCandidate` is the return type of
  `blockassembly_api.GetMiningCandidate`, and `errors.TError` is a response
  field in `propagation_api.SendResponse` / `validator_api` batch responses,
  so a breaking change to either breaks the API that embeds it;
- `util/kafka/kafka_message/kafka_messages.proto` (`package kafkamessage`) --
  cross-node Kafka payloads referenced by qualified name in
  `deploy/docker/base/kafka-console-config.yml`; see
  [kafkaMessageFormat.md](kafkaMessageFormat.md) for how its `v1`/`v2` tx-meta
  formats already coexist on one topic.

It does not cover `.proto` files that never leave the process
(e.g. `stores/utxo/status.proto`).
