#!/usr/bin/env python3
"""
live_control_monitor.py

Small realtime monitor for cutup live telemetry JSONL.
Prints rolling summaries by event type + current override values.
"""

from __future__ import annotations

import argparse
import json
import time
from collections import Counter
from pathlib import Path
from typing import Dict


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Monitor cutup live telemetry JSONL stream")
    p.add_argument("--telemetry", required=True, help="Path to live telemetry JSONL file")
    p.add_argument("--refresh-ms", type=int, default=750, help="Screen refresh interval")
    p.add_argument("--tail", type=int, default=25, help="How many recent events to summarize")
    return p.parse_args()


def _clear() -> None:
    print("\033[2J\033[H", end="")


def main() -> None:
    args = parse_args()
    path = Path(args.telemetry).expanduser().resolve()

    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")

    pos = 0
    rows = []
    by_where = Counter()
    by_section = Counter()
    last_overrides: Dict[str, float] = {}
    last_filter_severity = ""
    last_section_arc = ""
    last_source_score = ""

    while True:
        try:
            with path.open("r", encoding="utf-8") as f:
                f.seek(pos)
                for line in f:
                    pos = f.tell()
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        row = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if not isinstance(row, dict):
                        continue
                    rows.append(row)
                    where = str(row.get("where", "unknown"))
                    by_where[where] += 1
                    section = str(row.get("section", ""))
                    if section:
                        by_section[section] += 1
                    ov = row.get("overrides", {})
                    if isinstance(ov, dict):
                        last_overrides = {k: float(v) for k, v in ov.items() if isinstance(v, (int, float))}
                    filter_severity = row.get("filter_severity", "")
                    if isinstance(filter_severity, str) and filter_severity:
                        last_filter_severity = filter_severity
                    section_arc = row.get("section_arc", "")
                    if isinstance(section_arc, str) and section_arc:
                        last_section_arc = section_arc
                    source_score = row.get("source_score", "")
                    if isinstance(source_score, str) and source_score:
                        last_source_score = source_score
        except OSError:
            pass

        if len(rows) > args.tail:
            rows = rows[-args.tail :]

        _clear()
        print("cutup live telemetry monitor")
        print(f"file: {path}")
        print(f"events seen: {sum(by_where.values())}")
        print()

        print("where counts:")
        for k, c in by_where.most_common(8):
            print(f"  - {k:14s} {c}")
        if not by_where:
            print("  - (none yet)")

        print("\nsection counts:")
        for k, c in by_section.most_common(8):
            print(f"  - {k:10s} {c}")
        if not by_section:
            print("  - (none yet)")

        print("\nlast overrides:")
        if last_overrides or last_filter_severity or last_section_arc or last_source_score:
            for k in sorted(last_overrides):
                print(f"  - {k:18s} {last_overrides[k]:.3f}")
            if last_filter_severity:
                print(f"  - {'filter_severity':18s} {last_filter_severity}")
            if last_section_arc:
                print(f"  - {'section_arc':18s} {last_section_arc}")
            if last_source_score:
                print(f"  - {'source_score':18s} {last_source_score}")
        else:
            print("  - (none yet)")

        print("\nrecent events:")
        for row in rows[-8:]:
            where = str(row.get("where", "?"))
            ts = row.get("ts_ms", "?")
            sec = row.get("section", "")
            extra = f" section={sec}" if sec else ""
            print(f"  - {ts} {where}{extra}")

        time.sleep(max(0.1, args.refresh_ms / 1000.0))


if __name__ == "__main__":
    main()
