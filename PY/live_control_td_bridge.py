#!/usr/bin/env python3
"""
live_control_td_bridge.py

TouchDesigner bridge for cutup.py live-control.

Runs a UDP listener that accepts small JSON payloads and writes a control file
compatible with:
  python PY/cutup.py --live-control-file <path>

This is designed to be fed from TouchDesigner via UDP Out DAT/CHOP.
"""

from __future__ import annotations

import argparse
import json
import socket
from pathlib import Path
from typing import Dict, Tuple


ALLOWED: Dict[str, Tuple[float, float]] = {
    "absurd_seriousness": (0.0, 1.0),
    "text_chaos": (0.0, 1.5),
    "rupture_prob": (0.0, 1.0),
    "stutter_prob": (0.0, 1.0),
    "recurrence_prob": (0.0, 0.95),
    "ghost_prob": (0.0, 0.95),
    "silence_prob": (0.0, 0.95),
}


def clamp(v: float, low: float, high: float) -> float:
    return max(low, min(high, v))


def clamp_payload(raw: Dict[str, object]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for key, (low, high) in ALLOWED.items():
        val = raw.get(key)
        if isinstance(val, (int, float)):
            out[key] = clamp(float(val), low, high)
    return out


def extract_conductor_controls(raw: Dict[str, object]) -> Dict[str, object]:
    out: Dict[str, object] = {}
    sec = str(raw.get("force_section", "")).strip().upper()
    if sec in {"", "ENTRY", "BUILD", "PRESSURE", "COLLAPSE", "AFTERIMAGE"}:
        out["force_section"] = sec
    for key in ("hold_section", "burst_now", "panic_silence"):
        if key in raw:
            out[key] = bool(raw.get(key))
    return out


def atomic_write(path: Path, text: str) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="TouchDesigner UDP bridge for cutup live-control JSON")
    p.add_argument("--host", default="127.0.0.1", help="UDP bind host")
    p.add_argument("--port", type=int, default=9988, help="UDP bind port")
    p.add_argument("--control-file", default="live_control.json", help="Control JSON file to write")
    p.add_argument("--verbose", action="store_true", help="Print received updates")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    control_file = Path(args.control_file).expanduser().resolve()
    control_file.parent.mkdir(parents=True, exist_ok=True)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((args.host, args.port))
    print(f"[td-bridge] listening on udp://{args.host}:{args.port}")
    print(f"[td-bridge] writing control file: {control_file}")

    current: Dict[str, float] = {}
    while True:
        data, src = sock.recvfrom(65535)
        try:
            payload = json.loads(data.decode("utf-8", errors="strict"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            if args.verbose:
                print(f"[td-bridge] ignored non-JSON packet from {src}")
            continue
        if not isinstance(payload, dict):
            if args.verbose:
                print(f"[td-bridge] ignored non-object payload from {src}")
            continue

        payload_version = payload.get("version", 1)
        if payload_version not in {1, 2}:
            if args.verbose:
                print(f"[td-bridge] ignored unsupported version from {src}: {payload_version}")
            continue

        controls = payload.get("controls", payload) if isinstance(payload.get("controls", payload), dict) else payload
        if not isinstance(controls, dict):
            continue

        update: Dict[str, object] = {}
        update.update(clamp_payload(controls))
        update.update(extract_conductor_controls(controls))
        if not update:
            if args.verbose:
                print(f"[td-bridge] ignored packet without supported keys from {src}")
            continue

        current.update(update)
        wrapped = {"version": 2, "controls": current}
        atomic_write(control_file, json.dumps(wrapped, indent=2) + "\n")
        if args.verbose:
            print(f"[td-bridge] {src} -> {update}")


if __name__ == "__main__":
    main()
