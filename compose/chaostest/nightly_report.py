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
    md.append("Run `%s`, teranode `%s`, chaos-test `%s`, blaster %s%s." % (
        meta.get("run", "?"), (meta.get("teranode") or "?")[:12], (meta.get("chaosTest") or "?")[:12],
        "on" if meta.get("blaster") else "off (wallet load only)", ", quick" if meta.get("quick") else ""))
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
    text = "\n".join(md) + "\n"
    open(os.path.join(out, "summary.md"), "w").write(text)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        open(os.environ["GITHUB_STEP_SUMMARY"], "a").write(text)
    print(text)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
