#!/usr/bin/env python3
"""Turn a nightly.sh output directory into junit.xml and summary.md, and decide the exit code.

  nightly_report.py <out dir> <known-failures.txt> [<previous out dir>] [--error <message>]

A run fails the night when it failed and is not in known-failures.txt, when a phase ran out of
its time box, or when an audit found a chain divergence or a wrong merkle proof. Integrity
failures are never quarantined.
"""
import glob
import json
import os
import sys
from xml.sax.saxutils import escape, quoteattr


def load_known(path):
    known = []
    for line in open(path):
        line = line.split("#", 1)[0].strip()
        if line:
            sid, _, step = line.partition(":")
            known.append((sid.strip(), step.strip()))
    return known


def is_known(known, run):
    for sid, step in known:
        if sid != run["scenario"]:
            continue
        if not step or any(f.startswith(step) for f in run.get("failed") or []):
            return True
    return False


def load_runs(out):
    runs = []
    for d in sorted(glob.glob(os.path.join(out, "runs", "phase*"))):
        phase = os.path.basename(d)
        for f in sorted(glob.glob(os.path.join(d, "*.json"))):
            r = json.load(open(f))
            r["phase"] = phase
            runs.append(r)
    return runs


def audits(out):
    """chain_audit / proof_audit outputs per orchestrator run, keyed by run id."""
    found = {}
    for f in glob.glob(os.path.join(out, "orchestrator-runs", "*.json")):
        r = json.load(open(f))
        for s in r.get("steps") or []:
            o = s.get("output")
            if not isinstance(o, dict):
                continue
            if "divergences" in o:
                # The first chain_audit covers the teranodes over the whole chain; a later one
                # cross-checks the SV nodes over the heights they hold.
                a = found.setdefault(r.get("id"), {})
                a["sv" if "chain" in a else "chain"] = o
            elif "mismatched" in o:
                found.setdefault(r.get("id"), {})["proofs"] = o
    return found


def parse_time(s):
    """RFC3339 with optional nanoseconds, which datetime.fromisoformat (3.9) cannot read."""
    from datetime import datetime, timezone
    s = s.rstrip("Z")
    if "." in s:
        head, frac = s.split(".", 1)
        s = head + "." + frac[:6].ljust(6, "0")
        return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=timezone.utc)
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)


def seconds(r):
    try:
        return max(0.0, (parse_time(r["finishedAt"]) - parse_time(r["startedAt"])).total_seconds())
    except (KeyError, ValueError):
        return 0.0


def orchestrator_runs(out):
    """Orchestrator run records (step by step, with outputs), keyed by run id."""
    found = {}
    for f in glob.glob(os.path.join(out, "orchestrator-runs", "*.json")):
        r = json.load(open(f))
        found[r.get("id")] = r
    return found


def clock(seconds_):
    s = int(round(seconds_))
    return "%d:%02d" % (s // 60, s % 60)


def node_name(roles, x):
    return roles.get(x, x) if isinstance(x, str) else ", ".join(roles.get(n, n) for n in x or [])


def step_result(step, roles):
    """One short, plain line for what a step did."""
    a, o, w = step.get("action"), step.get("output"), step.get("resolved") or {}
    if not isinstance(o, dict):
        return "" if a in ("wait", "note") else str(o or "")[:120]
    if a == "mine":
        return "%s block(s) on %s" % (o.get("count", "?"), o.get("node", "?"))
    if a == "automine":
        if not o.get("enabled"):
            return "auto-mining off"
        return "%s auto-mines a block every %ss" % (o.get("node", "?"), o.get("intervalSeconds", "?"))
    if a == "wallet_topup":
        return "wallet funded with %s coins" % o.get("coins", "?")
    if a == "wallet_send_start":
        return "wallet starts sending, %s tx/s" % o.get("tps", "?")
    if a == "wallet_send_stop":
        return "%s sends, %s returned an error, %.1f tx/s" % (o.get("attempted", "?"), o.get("failed", "?"), o.get("measuredTps") or 0)
    if a == "partition":
        who = node_name(roles, w.get("nodes"))
        return ("%s cut off the %s network" if o.get("on") else "%s back on the %s network") % (who, o.get("plane", "p2p"))
    if a == "chaos":
        return "%s %sd" % (node_name(roles, w.get("node")), w.get("action", "chaos"))
    if a == "proof_audit":
        return "%s checked: %s proven, %s unproven, %s mismatched" % (o.get("checked", "?"), o.get("proven", "?"), o.get("unproven", "?"), o.get("mismatched", "?"))
    if "summary" in o:
        return str(o["summary"]).split(": ", 1)[0][:200]
    return ""


def mined_by_node(steps):
    """Blocks each node mined during a run, from the mine and automine steps. Blocks mined by a
    node while it was partitioned are counted apart: they were a fork the node had to drop."""
    mined, fork = {}, {}
    auto_node, auto_count = None, 0
    cut = set()
    for s in steps:
        a, o, w = s.get("action"), s.get("output"), s.get("resolved") or {}
        if not isinstance(o, dict):
            continue
        if a == "partition":
            nodes = set(w.get("nodes") or [])
            cut = cut | nodes if o.get("on") else cut - nodes
        elif a == "mine":
            node = o.get("node")
            target = fork if (w.get("node") in cut) else mined
            target[node] = target.get(node, 0) + int(o.get("count") or 0)
        elif a == "automine":
            count = int(o.get("blocksMined") or 0)
            if auto_node:
                mined[auto_node] = mined.get(auto_node, 0) + max(0, count - auto_count)
            auto_node, auto_count = (o.get("node") if o.get("enabled") else None), count
    return mined, fork


def scenario_story(r, roles_run):
    """A details block telling what happened in one orchestrator run."""
    steps = roles_run.get("steps") or []
    roles = roles_run.get("roles") or {}
    try:
        t0 = parse_time(roles_run["startedAt"])
    except (KeyError, ValueError):
        return []
    md = ["<details><summary><b>%s</b>: what happened</summary>" % r["scenario"], ""]
    send = next((s.get("output") for s in steps if s.get("action") == "wallet_send_stop" and isinstance(s.get("output"), dict)), None)
    if send:
        md.append("- **Load:** the wallet sent %s transactions through the broadcaster over %d min (%.1f tx/s); %s of the send calls returned an error to the sender. The proof audit below checks every transaction the wallet holds." % (
            send.get("attempted", "?"), int((send.get("elapsedSeconds") or 0) // 60), send.get("measuredTps") or 0, send.get("failed", "?")))
    mined, fork = mined_by_node(steps)
    if mined:
        line = ", ".join("%s %d" % (n, c) for n, c in sorted(mined.items()))
        if fork:
            line += "; on a fork while cut off, then dropped: " + ", ".join("%s %d" % (n, c) for n, c in sorted(fork.items()))
        md.append("- **Mining:** blocks mined by " + line + ". The SV nodes only follow.")
    md += ["", "| At | Step | Took | Result |", "|---|---|---|---|"]
    for s in steps:
        try:
            start, end = parse_time(s["startedAt"]), parse_time(s["finishedAt"])
        except (KeyError, ValueError):
            continue
        status = s.get("status", "")
        result = step_result(s, roles)
        if status not in ("passed", ""):
            result = ("**%s** %s" % (status, result)).strip()
        md.append("| +%s | %s | %s | %s |" % (clock((start - t0).total_seconds()), s.get("name", ""), clock((end - start).total_seconds()), result.replace("|", "\\|")))
    md += ["", "</details>", ""]
    return md


ROLES = [
    ("teranode", "Teranode, all-in-one, built from this commit. Mines, validates and serves blocks."),
    ("aerospike", "Aerospike UTXO store, one per teranode."),
    ("postgres", "PostgreSQL: each teranode's blockchain store."),
    ("kafka", "Kafka (Redpanda), shared by the teranodes and merkle-service."),
    ("svnode", "Bitcoin SV node following the teranodes over the legacy p2p protocol; never mines."),
    ("alert-svnode", "Alert sidecar of an SV node."),
    ("alert-system", "Alert hub (alert-system network)."),
    ("arcade", "BSV Broadcaster: takes the wallet's transactions, broadcasts them to the teranodes, tracks their status and keeps the merkle proofs."),
    ("merkle-service", "merkle-service: follows the teranodes' blocks and subtrees and sends the broadcaster a merkle proof for every transaction it tracks."),
    ("wallet-infra", "Wallet storage server (go-wallet-toolbox), broadcasting through the broadcaster."),
    ("wallet-db", "Wallet storage database."),
    ("walletd", "BRC-100 wallet that the scenarios drive to send transactions."),
    ("chaos-orchestrator", "chaos-test orchestrator: runs the scenarios, injects the faults and runs the audits."),
    ("jaeger", "Tracing."),
]


def stack_table(out):
    path = os.path.join(out, "containers.tsv")
    if not os.path.exists(path):
        return []
    groups = {}
    for line in open(path):
        name, _, image = line.rstrip("\n").partition("\t")
        base = name[: -len("-multinode")] if name.endswith("-multinode") else name
        role = next((r for r in ROLES if base.startswith(r[0])), (base, ""))
        g = groups.setdefault(role[0], {"names": [], "images": set(), "what": role[1]})
        g["names"].append(base)
        g["images"].add(image)
    md = ["| Component | Containers | Image | Role |", "|---|---|---|---|"]
    order = [r[0] for r in ROLES]
    for key in sorted(groups, key=lambda k: order.index(k) if k in order else len(order)):
        g = groups[key]
        md.append("| %s | %s | `%s` | %s |" % (key, ", ".join(sorted(g["names"])), "`, `".join(sorted(g["images"])), g["what"]))
    return md


ABOUT = """\
<details><summary>What this run does</summary>

1. **Builds** teranode from the commit above, and pulls the latest BSV Broadcaster. merkle-service and the wallet are pinned.
2. **Starts Stack A** on a new regtest chain: 3 teranodes and 2 Bitcoin SV nodes on a p2p network that can be partitioned, plus the broadcaster, merkle-service, a wallet and the chaos-test orchestrator (see *The stack* below).
3. **Phase 1, chain integrity.** Each scenario starts from a fresh chain. The wallet sends about 3 transactions a second through the broadcaster while the teranodes mine:
   - **steady** hands mining from teranode1 to teranode2 to teranode3, 8 minutes each, with no faults.
   - **chaos** keeps teranode1 mining, cuts teranode3 off the p2p network while it mines a fork of its own, pauses teranode2, then pauses merkle-service.

   Each scenario ends with two audits. `chain_audit` compares the block hash and merkle root at every height on every node, SV nodes included. `proof_audit` checks each wallet transaction's merkle proof, built independently by merkle-service, against every teranode's block at that height.
4. **Phase 2** (if run): the broadcaster's end-to-end suite. **Phase 3** (if run): chaos-test's scenario library.
5. **Verdict.** The night fails on any chain divergence or wrong proof, any failed scenario not listed in `suites/known-failures.txt`, or a phase that runs out of time. *Findings* (such as wallet send calls that returned an error) are recorded but do not fail the night.

</details>
"""

GLOSSARY = """\
<details><summary>Reading the numbers</summary>

- **Heights / Divergences**: how many block heights were compared across the nodes, and at how many of them any two nodes held a different block. Any divergence fails the night.
- **Stale tips (deepest)**: competing blocks that lost (for example two nodes mining at the same moment, or a partitioned node's fork) and the longest such branch. Expected when mining moves between nodes or a node is cut off; the audit checks they were all abandoned.
- **Proofs checked / Proven / Unproven / Mismatched**: wallet transactions sent during the scenario; those with a merkle proof; those still without one at the end; those whose proof does not match the block every node holds at that height. Mismatched fails the night.
- **Latency p50 / p95**: time from the broadcaster accepting a transaction to it holding the merkle proof. Pausing merkle-service in the chaos scenario raises the p95 on purpose.
- **Finding**: a check marked "should" in the scenario that did not hold. Recorded, not a failure. The usual one, "no send failed", counts wallet send calls that returned an error; those transactions still reach the chain and are covered by the proof audit.

</details>
"""

DATA = """\
<details><summary>What is in the artifact</summary>

| File | Contents |
|---|---|
| `summary.md`, `junit.xml` | this summary; per-scenario results for test reporting |
| `meta.json`, `images.txt`, `containers.tsv` | commits, runner, image digests, the stack's containers |
| `runs/phaseN/*.json` | each scenario's result: status, failed steps, findings |
| `orchestrator-runs/*.json` | each scenario step by step, with every step's output, including the full audit reports |
| `phaseN.log`, `stack-up.log`, `build.log`, `pull.log`, `teardown.log` | console output of each stage |
| `logs/` | every container's log for the night |
| `diagnostics.txt`, `merkle-groups.txt` | node state at the end; merkle-service's Kafka consumer groups |

</details>
"""


def main(argv):
    error = ""
    if "--error" in argv:
        i = argv.index("--error")
        error = argv[i + 1]
        argv = argv[:i] + argv[i + 2:]
    out, known_file = argv[1], argv[2]
    previous = argv[3] if len(argv) > 3 else ""
    known = load_known(known_file)
    runs = load_runs(out)
    aud = audits(out)
    meta = json.load(open(os.path.join(out, "meta.json"))) if os.path.exists(os.path.join(out, "meta.json")) else {}

    timed_out = []
    if os.path.exists(os.path.join(out, "phases.txt")):
        for line in open(os.path.join(out, "phases.txt")):
            n, _, st = line.strip().partition("=")
            if st == "124":
                timed_out.append(n)

    rows, new_failures, recovered = [], [], []
    integrity = []
    for r in runs:
        a = aud.get(r.get("run"), {})
        chain, proofs, sv = a.get("chain"), a.get("proofs"), a.get("sv")
        bad_integrity = any(x and x.get("divergences", 0) > 0 for x in (chain, sv)) or bool(proofs and proofs.get("mismatched", 0) > 0)
        if chain or proofs:
            integrity.append((r, chain, proofs, sv))
        listed = is_known(known, r) or any(sid == r["scenario"] for sid, _ in known)
        if r["status"] == "passed" and not bad_integrity:
            verdict = "passed"
            if listed:
                recovered.append(r["scenario"])
        elif not bad_integrity and is_known(known, r):
            verdict = "known"
        else:
            verdict = "FAILED"
            new_failures.append(r)
        rows.append((r, verdict))

    prev_status = {}
    if previous and os.path.isdir(previous):
        for r in load_runs(previous):
            prev_status[r["scenario"]] = r["status"]

    # junit.xml
    cases = []
    for r, verdict in rows:
        name = quoteattr(r["scenario"])
        body = ""
        detail = "; ".join(r.get("failed") or []) or "audit mismatch"
        if verdict == "FAILED":
            body = "<failure message=%s>%s</failure>" % (quoteattr(detail[:500]), escape(json.dumps(r, indent=2)[:20000]))
        elif verdict == "known":
            body = "<skipped message=%s/>" % quoteattr("known failure: " + detail[:400])
        for f in r.get("findings") or []:
            body += "<system-out>%s</system-out>" % escape("finding: " + f)
        cases.append('<testcase classname="%s" name=%s time="%.1f">%s</testcase>' % (r["phase"], name, seconds(r), body))
    for n in timed_out:
        cases.append('<testcase classname="phase%s" name="phase time box"><failure message="phase %s ran out of its time box"/></testcase>' % (n, n))
    if error:
        cases.append('<testcase classname="setup" name="setup"><failure message=%s/></testcase>' % quoteattr(error))
    failures = len(new_failures) + len(timed_out) + (1 if error else 0)
    skipped = sum(1 for _, v in rows if v == "known")
    with open(os.path.join(out, "junit.xml"), "w") as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        f.write('<testsuites name="chain-integrity-stack"><testsuite name="chain-integrity-stack" tests="%d" failures="%d" skipped="%d">\n' % (len(cases), failures, skipped))
        f.write("\n".join(cases))
        f.write("\n</testsuite></testsuites>\n")

    # summary.md
    ok = failures == 0
    md = ["## Chain integrity on the multinode stack: %s" % ("passed" if ok else "FAILED"), ""]
    if integrity and not error:
        heights = sum((c or {}).get("heights", 0) for _, c, _, _ in integrity)
        divergences = sum((c or {}).get("divergences", 0) + (v or {}).get("divergences", 0) for _, c, _, v in integrity)
        checked = sum((p or {}).get("checked", 0) for _, _, p, _ in integrity)
        proven = sum((p or {}).get("proven", 0) for _, _, p, _ in integrity)
        mismatched = sum((p or {}).get("mismatched", 0) for _, _, p, _ in integrity)
        nodes = max(len((v or c or {}).get("nodes") or []) for _, c, _, v in integrity)
        md.append(("**In short:** %s %s %s" % (
            "all %d nodes agreed on every block (%d heights compared across %d scenarios)." % (nodes, heights, len(integrity)) if divergences == 0
            else "**%d height(s) where nodes held different blocks.**" % divergences,
            "%d of %d wallet transactions got a merkle proof%s." % (proven, checked, ", all of them correct" if mismatched == 0 else ""),
            "" if mismatched == 0 else "**%d proof(s) did not match the chain.**" % mismatched)).rstrip())
        if new_failures or timed_out:
            md.append("Failed: %s." % ", ".join([r["scenario"] for r in new_failures] + ["phase %s time box" % n for n in timed_out]))
        md.append("")
    md.append("Run `%s` on %s. teranode `%s`%s, chaos-test `%s`, blaster %s%s." % (
        meta.get("run", "?"), meta.get("host") or "?", (meta.get("teranode") or "?")[:12],
        (" (%s)" % meta["teranodeSubject"]) if meta.get("teranodeSubject") else "",
        (meta.get("chaosTest") or "?")[:12],
        "on" if meta.get("blaster") else "off (wallet load only)", ", quick" if meta.get("quick") else ""))
    if previous and prev_status:
        md.append("Compared with the last successful night, `%s`." % os.path.basename(os.path.normpath(previous)))
    if error:
        md += ["", "**Setup failed:** " + error]
    if integrity:
        md += ["", "### Integrity audits", "",
               "| Scenario | Heights | Divergences | Stale tips (deepest) | All nodes incl. SV: heights, divergences | Proofs checked | Proven | Unproven | Mismatched | Latency p50 / p95 |",
               "|---|---|---|---|---|---|---|---|---|---|"]
        for r, c, p, v in integrity:
            c, p, v = c or {}, p or {}, v or {}
            md.append("| %s | %s | %s | %s (%s) | %s, %s | %s | %s | %s | %s | %s / %s |" % (
                r["scenario"], c.get("heights", "-"), c.get("divergences", "-"), c.get("staleTips", "-"), c.get("deepestFork", "-"),
                v.get("heights", "-"), v.get("divergences", "-"),
                p.get("checked", "-"), p.get("proven", "-"), p.get("unproven", "-"), p.get("mismatched", "-"),
                p.get("latencyP50") or "-", p.get("latencyP95") or "-"))
    md += ["", "### Scenarios", "", "| Phase | Scenario | Result | Steps | Detail | vs previous |", "|---|---|---|---|---|---|"]
    for r, verdict in rows:
        detail = "; ".join((r.get("failed") or [])[:1] + ["finding: " + f for f in (r.get("findings") or [])][:2])
        prev = prev_status.get(r["scenario"])
        change = "" if prev is None else ("same" if prev == r["status"] else "%s -> %s" % (prev, r["status"]))
        md.append("| %s | %s | %s | %s | %s | %s |" % (r["phase"], r["scenario"], verdict, r.get("steps", ""), detail.replace("|", "\\|")[:300], change))
    for n in timed_out:
        md.append("| phase%s | (time box) | FAILED | | ran out of its time box | |" % n)
    if recovered:
        md += ["", "Listed in known-failures.txt but passed: " + ", ".join(sorted(set(recovered))) + ". Remove them from the list."]
    md += ["", ABOUT]
    orch = orchestrator_runs(out)
    for r, _, _, _ in integrity:
        if r.get("run") in orch:
            md += scenario_story(r, orch[r["run"]])
    stack = stack_table(out)
    if stack:
        md += ["<details><summary>The stack</summary>", ""] + stack + ["", "</details>", ""]
    md += [GLOSSARY, DATA]
    text = "\n".join(md) + "\n"
    open(os.path.join(out, "summary.md"), "w").write(text)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        open(os.environ["GITHUB_STEP_SUMMARY"], "a").write(text)
    print(text)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
