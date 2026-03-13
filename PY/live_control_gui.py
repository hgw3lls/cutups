#!/usr/bin/env python3
"""
live_control_gui.py

Minimal real-time GUI for cutup.py live-control MVP.
Writes a JSON control file that cutup.py can poll via --live-control-file.
"""

from __future__ import annotations

import argparse
import json
import tkinter as tk
from dataclasses import dataclass
from pathlib import Path
from tkinter import ttk
from typing import Dict, Tuple


RANGES: Dict[str, Tuple[float, float, float]] = {
    "absurd_seriousness": (0.0, 1.0, 0.62),
    "text_chaos": (0.0, 1.5, 0.60),
    "rupture_prob": (0.0, 1.0, 0.35),
    "stutter_prob": (0.0, 1.0, 0.32),
    "recurrence_prob": (0.0, 0.95, 0.28),
    "ghost_prob": (0.0, 0.95, 0.22),
    "silence_prob": (0.0, 0.95, 0.15),
}

PRESETS: Dict[str, Dict[str, float]] = {
    "Default": {k: v[2] for k, v in RANGES.items()},
    "Bureaucratic Pressure": {
        "absurd_seriousness": 0.92,
        "text_chaos": 0.95,
        "rupture_prob": 0.54,
        "stutter_prob": 0.42,
        "recurrence_prob": 0.58,
        "ghost_prob": 0.45,
        "silence_prob": 0.20,
    },
    "Ghost Broadcast": {
        "absurd_seriousness": 0.76,
        "text_chaos": 0.84,
        "rupture_prob": 0.30,
        "stutter_prob": 0.36,
        "recurrence_prob": 0.70,
        "ghost_prob": 0.75,
        "silence_prob": 0.34,
    },
    "Collapse Ritual": {
        "absurd_seriousness": 1.00,
        "text_chaos": 1.20,
        "rupture_prob": 0.82,
        "stutter_prob": 0.68,
        "recurrence_prob": 0.66,
        "ghost_prob": 0.56,
        "silence_prob": 0.41,
    },
}


@dataclass
class ControlGUI:
    root: tk.Tk
    control_file: Path
    vars: Dict[str, tk.DoubleVar]
    status_var: tk.StringVar
    last_payload: Dict[str, object]
    section_var: tk.StringVar
    hold_var: tk.BooleanVar
    burst_var: tk.BooleanVar
    panic_var: tk.BooleanVar

    @staticmethod
    def _atomic_write(path: Path, text: str) -> None:
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(text, encoding="utf-8")
        tmp.replace(path)

    def write_payload(self) -> None:
        payload = {k: round(v.get(), 4) for k, v in self.vars.items()}
        controls = dict(payload)
        controls["force_section"] = self.section_var.get().strip().upper()
        controls["hold_section"] = bool(self.hold_var.get())
        controls["burst_now"] = bool(self.burst_var.get())
        controls["panic_silence"] = bool(self.panic_var.get())
        if controls == self.last_payload:
            return
        self.control_file.parent.mkdir(parents=True, exist_ok=True)
        wrapped = {"version": 2, "controls": controls}
        self._atomic_write(self.control_file, json.dumps(wrapped, indent=2) + "\n")
        self.last_payload = controls
        self.status_var.set(f"Wrote: {self.control_file}")

    def apply_preset(self, preset_name: str) -> None:
        data = PRESETS.get(preset_name, PRESETS["Default"])
        for key, val in data.items():
            if key in self.vars:
                self.vars[key].set(val)
        self.write_payload()

    def reset_defaults(self) -> None:
        self.apply_preset("Default")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Realtime GUI for cutup.py live-control JSON file")
    p.add_argument("--control-file", default="live_control.json", help="Path to write live control JSON")
    p.add_argument("--title", default="Cutup Live Control", help="Window title")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    control_file = Path(args.control_file).expanduser().resolve()

    root = tk.Tk()
    root.title(args.title)
    root.geometry("640x500")

    frame = ttk.Frame(root, padding=12)
    frame.pack(fill=tk.BOTH, expand=True)

    ttk.Label(frame, text="cutup.py realtime control", font=("TkDefaultFont", 14, "bold")).pack(anchor="w")
    ttk.Label(
        frame,
        text="Move sliders while cutup.py is running with --live-control-file to update generation in-flight.",
    ).pack(anchor="w", pady=(0, 10))

    status_var = tk.StringVar(value=f"Control file: {control_file}")

    preset_row = ttk.Frame(frame)
    preset_row.pack(fill=tk.X, pady=(0, 8))
    ttk.Label(preset_row, text="Preset:").pack(side=tk.LEFT)
    preset_var = tk.StringVar(value="Default")
    preset_combo = ttk.Combobox(preset_row, values=list(PRESETS.keys()), textvariable=preset_var, state="readonly", width=28)
    preset_combo.pack(side=tk.LEFT, padx=(8, 10))

    vars_map: Dict[str, tk.DoubleVar] = {k: tk.DoubleVar(value=default) for k, (_, _, default) in RANGES.items()}
    section_var = tk.StringVar(value="")
    hold_var = tk.BooleanVar(value=False)
    burst_var = tk.BooleanVar(value=False)
    panic_var = tk.BooleanVar(value=False)
    gui = ControlGUI(
        root=root,
        control_file=control_file,
        vars=vars_map,
        status_var=status_var,
        last_payload={},
        section_var=section_var,
        hold_var=hold_var,
        burst_var=burst_var,
        panic_var=panic_var,
    )

    def on_slide(_: str = "") -> None:
        gui.write_payload()

    for key, (low, high, _) in RANGES.items():
        row = ttk.Frame(frame)
        row.pack(fill=tk.X, pady=4)
        ttk.Label(row, text=key, width=22).pack(side=tk.LEFT)
        scale = ttk.Scale(row, from_=low, to=high, variable=vars_map[key], command=on_slide)
        scale.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(8, 8))
        value_label = ttk.Label(row, width=8)
        value_label.pack(side=tk.LEFT)

        def bind_value(var: tk.DoubleVar, label: ttk.Label) -> None:
            def update_label(*_: object) -> None:
                label.configure(text=f"{var.get():.3f}")
            var.trace_add("write", update_label)
            update_label()

        bind_value(vars_map[key], value_label)

    btns = ttk.Frame(frame)
    btns.pack(fill=tk.X, pady=(10, 8))
    ttk.Button(btns, text="Apply preset", command=lambda: gui.apply_preset(preset_var.get())).pack(side=tk.LEFT)
    ttk.Button(btns, text="Reset defaults", command=gui.reset_defaults).pack(side=tk.LEFT, padx=(8, 0))
    ttk.Button(btns, text="Write now", command=gui.write_payload).pack(side=tk.LEFT, padx=(8, 0))

    conductor = ttk.LabelFrame(frame, text="Conductor controls", padding=8)
    conductor.pack(fill=tk.X, pady=(8, 8))
    ttk.Label(conductor, text="Force section").grid(row=0, column=0, sticky="w")
    sec_combo = ttk.Combobox(conductor, values=["", "ENTRY", "BUILD", "PRESSURE", "COLLAPSE", "AFTERIMAGE"], textvariable=section_var, state="readonly", width=16)
    sec_combo.grid(row=0, column=1, sticky="w", padx=(8, 12))
    ttk.Checkbutton(conductor, text="Hold section", variable=hold_var, command=gui.write_payload).grid(row=0, column=2, sticky="w")
    ttk.Checkbutton(conductor, text="Burst now", variable=burst_var, command=gui.write_payload).grid(row=1, column=0, sticky="w", pady=(6, 0))
    ttk.Checkbutton(conductor, text="Panic silence", variable=panic_var, command=gui.write_payload).grid(row=1, column=1, sticky="w", pady=(6, 0))

    cmd = (
        f"python PY/cutup.py --mode both --input ./samples --sectional "
        f"--live-control-file {control_file} --live-control-poll-ms 120"
    )
    ttk.Label(frame, text="Run cutup with:", font=("TkDefaultFont", 10, "bold")).pack(anchor="w", pady=(8, 2))
    cmd_box = tk.Text(frame, height=2, wrap="word")
    cmd_box.insert("1.0", cmd)
    cmd_box.configure(state="disabled")
    cmd_box.pack(fill=tk.X)

    status = ttk.Label(frame, textvariable=status_var)
    status.pack(anchor="w", pady=(10, 0))

    preset_combo.bind("<<ComboboxSelected>>", lambda _e: gui.apply_preset(preset_var.get()))
    sec_combo.bind("<<ComboboxSelected>>", lambda _e: gui.write_payload())

    gui.write_payload()
    root.mainloop()


if __name__ == "__main__":
    main()
