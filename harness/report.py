import json
from pathlib import Path

ICON = {"PASS": "PASS", "FAIL": "**FAIL**", "N/A": "N/A"}


def _metrics(m: dict) -> str:
    parts = []
    for k, v in m.items():
        if isinstance(v, (list, dict)):
            if not v:
                continue
            v = json.dumps(v, default=str)
            if len(v) > 80:
                v = v[:77] + "..."
        parts.append(f"{k}={v}")
    return ", ".join(parts).replace("|", "\\|")


def overall(checks) -> str:
    return "FAIL" if any(c["status"] == "FAIL" for c in checks) else "PASS"


def write(run_dir: Path, data: dict) -> str:
    status = overall(data["checks"])
    data["status"] = status
    (run_dir / "parity-report.json").write_text(json.dumps(data, indent=2, default=str) + "\n")

    fails = [c["id"] for c in data["checks"] if c["status"] == "FAIL"]
    head = (f"**{status}**: `{data['job']}`, {data['mode']}, fixture `{data['fixture']}`"
            + (f". Failed: {', '.join(fails)}." if fails else f". {len(data['checks'])} checks, none failed."))
    lines = [f"# Parity report {data['run_id']}", "", head, "",
             "| Check | Status | Metrics |", "|---|---|---|"]
    for c in data["checks"]:
        lines.append(f"| {c['title']} | {ICON[c['status']]} | {_metrics(c['metrics'])} |")
    for c in data["checks"]:
        if c["status"] == "FAIL" and c.get("explanation"):
            lines += ["", f"- **{c['id']}**: {c['explanation']}"]

    lines += ["", "## Inputs", "",
              f"- Fixture `{data['fixture']}`, MANIFEST.json sha256 `{data['manifest_sha256']}`",
              f"- Input records: {data.get('input_records')}"]
    for k, v in data["deps"].items():
        lines.append(f"- {k}: `{v or 'n/a'}`")
    if data.get("rust_image"):
        lines.append(f"- Rust image: `{data['rust_image']}`")

    chaos = data.get("chaos")
    if chaos:
        lines += ["", "## Chaos timeline", "", "| Event | Detail |", "|---|---|"]
        for e in chaos["timeline"]:
            lines.append(f"| {e['event']} | {e['detail']} |")

    priv = data.get("privacy")
    if priv:
        lines += ["", "## Privacy", "", "| Store | Kind | Sensitive fields |", "|---|---|---|"]
        for s in priv:
            fields = ", ".join(f"`{f}` ({t})" for f, t in s["sensitive_fields"].items()) or "none"
            lines.append(f"| `{s['name']}` | {s['kind']} | {fields} |")

    diffs = [(c["id"], d) for c in data["checks"] if c["status"] == "FAIL" for d in c["details"]][:20]
    if diffs:
        lines += ["", "## First diffs", "", "| Check | Detail |", "|---|---|"]
        for cid, d in diffs:
            lines.append(f"| {cid} | {json.dumps(d, default=str).replace('|', chr(92) + '|')} |")

    lines += ["", "## Durations", ""] + [f"- {k}: {v:.1f} s" for k, v in data["durations"].items()]
    lines += ["", "## Reproduce", "", "```", data["command"], "```", ""]
    md = "\n".join(lines)
    (run_dir / "parity-report.md").write_text(md)
    return status
