---
name: chaos-scenario
description: Run chaos-test (galt-tr/chaos-test) scenarios on the active chaos stack and report what happened — one scenario or a batch, optionally under blaster load (--under-load [preset]) and with a chain reset between scenarios. Follows each run step by step, summarises passes, failures and findings, saves the result and compares it with the previous run of the same scenario. Use whenever the user asks to run, rerun or batch a scenario (freeze-skew-rpc, freeze-timing-skew, freeze-fork-remine, svnode-dust-policy, arcade-same-height-flip-flop, wallet-sustained-flow, multinode-smoke, multinode-freeze-skew-rpc), "run galt's scenarios", "run X under load", "what did the last run find", or wants a screenshot of a run.
license: MIT
metadata:
  audience: devs, qa
  layer: test-scenarios
---

# chaos-test scenarios

```bash
compose/chaostest/chaosctl.sh scenario list
compose/chaostest/chaosctl.sh scenario run <id>... [--under-load [preset]] [--reset] [--step] [--workers N]
compose/chaostest/chaosctl.sh scenario last <id>
```

A stack must be up (chaos-stack). Runs go through the orchestrator's API, not the browser.

## Which scenarios run where

| Scenario | Stack A | Stack B |
|---|---|---|
| `multinode-smoke`, `multinode-freeze-skew-rpc` | yes | no (not loaded) |
| `freeze-skew-rpc` | no (needs arcade) | yes |
| `freeze-timing-skew`, `freeze-fork-remine` | no (alert hub, partition) | yes |
| `svnode-dust-policy` | no (SV nodes) | yes |
| `arcade-same-height-flip-flop` | no (arcade) | yes |
| `wallet-sustained-flow` | no (wallet) | yes |

If the user asks for one on the wrong stack, say so and offer to switch (chaos-stack).

## Reset or not

chaos-test expects each scenario to start on a converged fleet with clean alert history, and
a scenario that leaves a split (every freeze scenario does) cannot be healed by mining.

- **Stack B, more than one scenario, or the fleet is SPLIT** → `--reset`. It wipes the chain
  first; with `--under-load` it also stops the blaster and restarts it `--fresh`.
- A single scenario on a converged fleet → no reset needed.
- `--reset` deletes the chain: if the user did not ask for a clean chain, mention it once.

## Under load

`--under-load [preset]` starts the blaster with that preset (default `default`) and funds it
before the first scenario, or keeps one that is already running. It starts it **without
auto-mine**: the auto-miner mines on teranode1, which is a scenario role, and scenarios assert
exact heights (`freeze-skew-rpc` failed at "A refuses the block" with A at 222 instead of
219 when the blaster was mining). The 110 blocks mined at funding keep the blaster going, and
scenarios mine what they need. A blaster started separately with auto-mine triggers a warning;
stop it first or use `--reset`. The result records the
preset and the blaster's stats before and after, so a new finding can be tied to the load.
To compare with and without load, run the same scenario both ways and look at the "vs"
line. Presets and worker guidance: chaos-blast.

## Reading the output

Each step prints as it finishes, then:

```
   result: passed, 16/16 steps, 5 findings, blaster on (ds-storm, [01:20] tx=… tx/s=…)
   finding: step "…": event (…)
   vs <previous run> (<image>, blaster off): new finding: … | finding gone: … | same result
   saved: data/chaosctl/runs/<run>.json
```

- **passed with findings** is a result, not a failure: a `should: true` check recorded a
  disagreement. Report each finding in one line, in the scenario's own words.
- **failed** = a hard check or an action failed; the failed step and its error print. Report
  the step and what it checked.
- **vs** compares with the last run of the same scenario on the same stack: status changes
  and findings that appeared or went away (keyed by step and check, ignoring counts).
- Known results (23-26 Oct 2026, image built from `pruner-unmined-tx-loss-repro`):
  `freeze-skew-rpc` on B passes with 5 findings (no invalid-block verdict, re-validation
  loop); `multinode-freeze-skew-rpc` on A fails at the freeze step (the RPC freeze is a no-op
  on Aerospike).

To dig into a failure (container logs around the step, Kafka verdicts, code), hand over to
the `debug-teranode-docker` skill with the run id and timestamps from the saved JSON.

## Injecting faults between steps

`--step` pauses before every step; release each with the UI or
`curl -X POST localhost:8600/api/runs/<run>/next`. On Stack A, apply
`compose/multinode.sh chaos isolate|slow|kill|pause <n> [svc]` while paused, and heal before
the closing checks.

## Screenshot of a run

The UI is at `http://localhost:8600/#/scenarios`. For a picture of a finished run, use the
`browser-automation` skill (headless; no Chrome extension needed):

A fresh page shows no run ("Select a run from the history"), so open a session, click the
run's row under **Run history** (it lists the run id), then screenshot:

```bash
B=<browser-automation dir>/browser.mjs
node $B "http://localhost:8600/#/scenarios" --session runshot --wait button --snapshot
# click the history row whose text contains the run id, e.g. with --script and
#   page.getByText('<run id>').first().click()
node $B --session runshot --screenshot run.png
node $B --session runshot --close
```

Read the PNG before describing it.
