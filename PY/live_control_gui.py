#!/usr/bin/env python3
"""
live_control_gui.py

Minimal real-time GUI for cutup.py live-control MVP.
Writes a JSON control file that cutup.py can poll via --live-control-file.
"""

from __future__ import annotations

import argparse
import json
import math
import shlex
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


tk: Any = None
ttk: Any = None
filedialog: Any = None


def ensure_tk() -> None:
    global tk, ttk, filedialog
    if tk is not None and ttk is not None and filedialog is not None:
        return
    try:
        import tkinter as tk_module
        from tkinter import ttk as ttk_module
        from tkinter import filedialog as filedialog_module
    except ModuleNotFoundError as exc:
        raise SystemExit(
            "Tk GUI unavailable: this Python environment does not provide tkinter/_tkinter. "
            "Use cutups-live-monitor or install a Python build with Tk support."
        ) from exc
    tk = tk_module
    ttk = ttk_module
    filedialog = filedialog_module


RANGES: Dict[str, Tuple[float, float, float]] = {
    "absurd_seriousness": (0.0, 1.0, 0.62),
    "text_chaos": (0.0, 1.5, 0.60),
    "rupture_prob": (0.0, 1.0, 0.35),
    "stutter_prob": (0.0, 1.0, 0.32),
    "recurrence_prob": (0.0, 0.95, 0.28),
    "ghost_prob": (0.0, 0.95, 0.22),
    "silence_prob": (0.0, 0.95, 0.15),
    "burst_rate": (0.0, 1.0, 0.0),
    "dropout_rate": (0.0, 1.0, 0.0),
    "reverse_shard_rate": (0.0, 1.0, 0.0),
    "stutter_rate": (0.0, 1.0, 0.0),
    "mute_rate": (0.0, 1.0, 0.0),
    "repeat_rate": (0.0, 1.0, 0.0),
    "beat_dropout_rate": (0.0, 1.0, 0.0),
    "source_diversity": (0.0, 1.0, 0.0),
}

SECTION_ARCS = ["classic", "spoken", "breach", "pulse", "ghost"]
SOURCE_SCORES = ["off", "spoken", "beat", "breach"]
BASELINE_PLACEMENTS = ["any", "accent", "gap", "offbeat"]
SLICE_GRIDS = ["off", "1/4", "1/8", "1/16", "1/32", "1/8t", "1/16t"]


def format_eta(seconds: object) -> str:
    if not isinstance(seconds, (int, float)) or not math.isfinite(float(seconds)) or seconds < 0:
        return "--:--"
    total = int(round(float(seconds)))
    minutes, secs = divmod(total, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours:d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


def cutup_script_path() -> Path:
    return Path(__file__).resolve().with_name("cutup.py")


def build_render_command(
    python_executable: str,
    script_path: Path,
    input_path: str,
    output_path: str,
    preset: str,
    duration: str,
    control_file: Path,
    telemetry_file: Path,
    bpm: str = "",
    slice_grid: str = "off",
    baseline_beat: str = "",
    semi_live: bool = False,
    semi_live_chunk_sec: str = "8",
) -> List[str]:
    cmd = [
        python_executable,
        str(script_path),
        "--mode",
        "audio",
        "--input",
        input_path,
        "--output",
        output_path,
        "--duration",
        duration,
        "--preview-duration",
        "10",
        "--live-control-file",
        str(control_file),
        "--live-telemetry-jsonl",
        str(telemetry_file),
        "--live-control-poll-ms",
        "120",
    ]
    if preset and preset != "Default":
        cmd.extend(["--preset", preset])
    if bpm.strip() and bpm.strip() not in {"0", "0.0"}:
        cmd.extend(["--bpm", bpm.strip()])
    if slice_grid and slice_grid != "off":
        cmd.extend(["--slice-grid", slice_grid])
    if baseline_beat.strip():
        cmd.extend(["--baseline-beat", baseline_beat.strip()])
    if semi_live:
        cmd.extend(["--semi-live", "--semi-live-chunk-sec", semi_live_chunk_sec.strip() or "8"])
    return cmd


PRESETS: Dict[str, Dict[str, object]] = {
    "Default": {k: v[2] for k, v in RANGES.items()},
    "signal-breach": {
        "absurd_seriousness": 0.78,
        "text_chaos": 1.00,
        "rupture_prob": 0.75,
        "stutter_prob": 0.62,
        "recurrence_prob": 0.48,
        "ghost_prob": 0.58,
        "silence_prob": 0.42,
        "burst_rate": 0.58,
        "dropout_rate": 0.64,
        "reverse_shard_rate": 0.46,
        "filter_severity": "hard",
        "section_arc": "breach",
        "source_score": "breach",
        "baseline_placement": "gap",
        "source_diversity": 0.22,
    },
    "spoken-word-cutup": {
        "absurd_seriousness": 0.62,
        "text_chaos": 0.55,
        "rupture_prob": 0.28,
        "stutter_prob": 0.20,
        "recurrence_prob": 0.32,
        "ghost_prob": 0.18,
        "silence_prob": 0.18,
        "section_arc": "spoken",
        "source_score": "spoken",
        "baseline_placement": "gap",
        "source_diversity": 0.65,
    },
    "beat-cutup": {
        "absurd_seriousness": 0.62,
        "text_chaos": 0.60,
        "rupture_prob": 0.50,
        "stutter_prob": 0.75,
        "recurrence_prob": 0.55,
        "ghost_prob": 0.20,
        "silence_prob": 0.24,
        "stutter_rate": 0.48,
        "mute_rate": 0.18,
        "repeat_rate": 0.38,
        "beat_dropout_rate": 0.16,
        "section_arc": "pulse",
        "source_score": "beat",
        "baseline_placement": "accent",
        "source_diversity": 0.35,
    },
    "radio-intrusion": {
        "absurd_seriousness": 0.72,
        "text_chaos": 0.78,
        "rupture_prob": 0.55,
        "stutter_prob": 0.38,
        "recurrence_prob": 0.46,
        "ghost_prob": 0.68,
        "silence_prob": 0.33,
        "burst_rate": 0.24,
        "dropout_rate": 0.28,
        "filter_severity": "hard",
        "section_arc": "ghost",
        "source_score": "breach",
        "baseline_placement": "gap",
        "source_diversity": 0.45,
    },
    "hard-stutter": {
        "absurd_seriousness": 0.74,
        "text_chaos": 0.88,
        "rupture_prob": 0.85,
        "stutter_prob": 0.90,
        "recurrence_prob": 0.62,
        "ghost_prob": 0.32,
        "silence_prob": 0.30,
        "dropout_rate": 0.42,
        "reverse_shard_rate": 0.24,
        "stutter_rate": 0.72,
        "mute_rate": 0.26,
        "repeat_rate": 0.52,
        "beat_dropout_rate": 0.24,
        "section_arc": "pulse",
        "source_score": "beat",
        "baseline_placement": "accent",
        "source_diversity": 0.25,
    },
    "ghost-transmission": {
        "absurd_seriousness": 0.66,
        "text_chaos": 0.72,
        "rupture_prob": 0.34,
        "stutter_prob": 0.34,
        "recurrence_prob": 0.74,
        "ghost_prob": 0.82,
        "silence_prob": 0.38,
        "section_arc": "ghost",
        "source_score": "spoken",
        "baseline_placement": "offbeat",
        "source_diversity": 0.18,
    },
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
        "section_arc": "ghost",
        "source_score": "spoken",
        "baseline_placement": "offbeat",
        "source_diversity": 0.25,
    },
    "Collapse Ritual": {
        "absurd_seriousness": 1.00,
        "text_chaos": 1.20,
        "rupture_prob": 0.82,
        "stutter_prob": 0.68,
        "recurrence_prob": 0.66,
        "ghost_prob": 0.56,
        "silence_prob": 0.41,
        "section_arc": "breach",
        "source_score": "breach",
        "baseline_placement": "gap",
        "source_diversity": 0.3,
    },
}


@dataclass
class ControlGUI:
    root: tk.Tk
    control_file: Path
    telemetry_file: Path
    telemetry_pos: int
    vars: Dict[str, tk.DoubleVar]
    status_var: tk.StringVar
    progress_var: tk.DoubleVar
    progress_text_var: tk.StringVar
    progress_detail_var: tk.StringVar
    input_var: tk.StringVar
    output_var: tk.StringVar
    render_preset_var: tk.StringVar
    duration_var: tk.StringVar
    bpm_var: tk.StringVar
    slice_grid_var: tk.StringVar
    baseline_var: tk.StringVar
    semi_live_var: tk.BooleanVar
    chunk_sec_var: tk.StringVar
    track_path_var: tk.StringVar
    command_box: Any
    last_payload: Dict[str, object]
    section_var: tk.StringVar
    filter_var: tk.StringVar
    arc_var: tk.StringVar
    score_var: tk.StringVar
    placement_var: tk.StringVar
    hold_var: tk.BooleanVar
    burst_var: tk.BooleanVar
    panic_var: tk.BooleanVar
    render_process: Optional[subprocess.Popen] = None
    render_log_handle: Any = None

    @staticmethod
    def _atomic_write(path: Path, text: str) -> None:
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(text, encoding="utf-8")
        tmp.replace(path)

    def write_payload(self) -> None:
        payload = {k: round(v.get(), 4) for k, v in self.vars.items()}
        controls = dict(payload)
        controls["force_section"] = self.section_var.get().strip().upper()
        controls["filter_severity"] = self.filter_var.get().strip().lower()
        controls["section_arc"] = self.arc_var.get().strip().lower()
        controls["source_score"] = self.score_var.get().strip().lower()
        controls["baseline_placement"] = self.placement_var.get().strip().lower()
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

    def poll_telemetry(self) -> None:
        try:
            if not self.telemetry_file.exists():
                self.telemetry_file.parent.mkdir(parents=True, exist_ok=True)
                self.telemetry_file.write_text("", encoding="utf-8")
            with self.telemetry_file.open("r", encoding="utf-8") as f:
                f.seek(self.telemetry_pos)
                while True:
                    line = f.readline()
                    if not line:
                        break
                    self.telemetry_pos = f.tell()
                    try:
                        row = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if not isinstance(row, dict):
                        continue
                    if row.get("where") == "progress":
                        percent = row.get("percent", 0.0)
                        if isinstance(percent, (int, float)):
                            self.progress_var.set(max(0.0, min(100.0, float(percent))))
                        stage = str(row.get("stage", ""))
                        eta = format_eta(row.get("eta_sec"))
                        self.progress_text_var.set(f"{self.progress_var.get():5.1f}%  {stage}  ETA {eta}")
                        detail = str(row.get("detail", ""))
                        self.progress_detail_var.set(detail if detail else f"Telemetry: {self.telemetry_file}")
                    elif row.get("where") == "semi_live_chunk":
                        track_path = str(row.get("live_track_path", row.get("track_path", "")) or "")
                        chunk_index = row.get("chunk_index", 0)
                        chunk_count = row.get("chunk_count", 0)
                        rendered_ms = row.get("rendered_ms", 0)
                        if track_path:
                            self.track_path_var.set(track_path)
                        self.progress_detail_var.set(f"Semi-live chunk {chunk_index}/{chunk_count} rendered {rendered_ms} ms")
        except OSError as exc:
            self.progress_detail_var.set(f"Telemetry unavailable: {exc}")
        self.root.after(500, self.poll_telemetry)

    def current_render_command(self) -> List[str]:
        return build_render_command(
            sys.executable,
            cutup_script_path(),
            self.input_var.get().strip() or "./samples",
            self.output_var.get().strip() or "out/gui_render",
            self.render_preset_var.get().strip(),
            self.duration_var.get().strip() or "45",
            self.control_file,
            self.telemetry_file,
            bpm=self.bpm_var.get().strip(),
            slice_grid=self.slice_grid_var.get().strip() or "off",
            baseline_beat=self.baseline_var.get().strip(),
            semi_live=bool(self.semi_live_var.get()),
            semi_live_chunk_sec=self.chunk_sec_var.get().strip() or "8",
        )

    def update_command_preview(self, *_: object) -> None:
        if self.command_box is None:
            return
        command = " ".join(shlex.quote(part) for part in self.current_render_command())
        self.command_box.configure(state="normal")
        self.command_box.delete("1.0", tk.END)
        self.command_box.insert("1.0", command)
        self.command_box.configure(state="disabled")

    def choose_input(self) -> None:
        chosen = filedialog.askdirectory(title="Choose audio source folder")
        if chosen:
            self.input_var.set(chosen)

    def choose_output(self) -> None:
        chosen = filedialog.askdirectory(title="Choose output folder")
        if chosen:
            self.output_var.set(chosen)

    def choose_baseline(self) -> None:
        chosen = filedialog.askopenfilename(title="Choose baseline beat", filetypes=[("Audio files", "*.wav *.mp3 *.flac *.aiff *.ogg *.m4a"), ("All files", "*.*")])
        if chosen:
            self.baseline_var.set(chosen)

    def open_track(self) -> None:
        raw = self.track_path_var.get().strip()
        if not raw or raw == "Track: waiting":
            self.status_var.set("No semi-live track available yet")
            return
        path = Path(raw)
        if not path.exists():
            self.status_var.set(f"Track not written yet: {path}")
            return
        try:
            if sys.platform == "darwin":
                subprocess.Popen(["open", str(path)])
            elif sys.platform.startswith("win"):
                subprocess.Popen(["cmd", "/c", "start", "", str(path)])
            else:
                subprocess.Popen(["xdg-open", str(path)])
        except OSError as exc:
            self.status_var.set(f"Could not open track: {exc}")

    def start_render(self) -> None:
        if self.render_process and self.render_process.poll() is None:
            self.status_var.set("Render already running")
            return
        self.write_payload()
        self.progress_var.set(0.0)
        self.progress_text_var.set("  0.0%  starting  ETA --:--")
        self.progress_detail_var.set(f"Telemetry: {self.telemetry_file}")
        self.track_path_var.set("Track: waiting")
        self.telemetry_pos = 0
        self.telemetry_file.parent.mkdir(parents=True, exist_ok=True)
        self.telemetry_file.write_text("", encoding="utf-8")
        log_path = self.telemetry_file.with_suffix(".log")
        self.render_log_handle = log_path.open("w", encoding="utf-8")
        cmd = self.current_render_command()
        try:
            self.render_process = subprocess.Popen(
                cmd,
                cwd=str(cutup_script_path().parents[1]),
                stdout=self.render_log_handle,
                stderr=subprocess.STDOUT,
            )
        except OSError as exc:
            self.render_log_handle.close()
            self.render_log_handle = None
            self.status_var.set(f"Render failed to start: {exc}")
            return
        self.status_var.set(f"Render running. Log: {log_path}")
        self.root.after(500, self.poll_render)

    def stop_render(self) -> None:
        if self.render_process and self.render_process.poll() is None:
            self.render_process.terminate()
            self.status_var.set("Stopping render")
        else:
            self.status_var.set("No render is running")

    def poll_render(self) -> None:
        if not self.render_process:
            return
        code = self.render_process.poll()
        if code is None:
            self.root.after(500, self.poll_render)
            return
        if self.render_log_handle:
            self.render_log_handle.close()
            self.render_log_handle = None
        self.status_var.set("Render complete" if code == 0 else f"Render exited with code {code}")

    def apply_preset(self, preset_name: str) -> None:
        data = PRESETS.get(preset_name, PRESETS["Default"])
        for key, var in self.vars.items():
            var.set(float(data.get(key, RANGES[key][2])))
        self.filter_var.set(str(data.get("filter_severity", "auto")))
        self.arc_var.set(str(data.get("section_arc", "classic")))
        self.score_var.set(str(data.get("source_score", "off")))
        self.placement_var.set(str(data.get("baseline_placement", "any")))
        self.write_payload()

    def reset_defaults(self) -> None:
        self.apply_preset("Default")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Realtime GUI for cutup.py live-control JSON file")
    p.add_argument("--control-file", default="live_control.json", help="Path to write live control JSON")
    p.add_argument("--telemetry-file", default="", help="Path to read live progress telemetry JSONL. Defaults beside --control-file.")
    p.add_argument("--title", default="Cutup Live Control", help="Window title")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    ensure_tk()
    control_file = Path(args.control_file).expanduser().resolve()
    telemetry_file = Path(args.telemetry_file).expanduser().resolve() if args.telemetry_file else control_file.with_name(f"{control_file.stem}_telemetry.jsonl")

    root = tk.Tk()
    root.title(args.title)
    root.geometry("780x1040")

    frame = ttk.Frame(root, padding=12)
    frame.pack(fill=tk.BOTH, expand=True)

    ttk.Label(frame, text="cutup.py realtime control", font=("TkDefaultFont", 14, "bold")).pack(anchor="w")
    ttk.Label(
        frame,
        text="Move sliders while cutup.py is running with --live-control-file to update generation in-flight.",
    ).pack(anchor="w", pady=(0, 10))

    status_var = tk.StringVar(value=f"Control file: {control_file}")
    progress_var = tk.DoubleVar(value=0.0)
    progress_text_var = tk.StringVar(value="  0.0%  waiting  ETA --:--")
    progress_detail_var = tk.StringVar(value=f"Telemetry: {telemetry_file}")
    input_var = tk.StringVar(value="./samples")
    output_var = tk.StringVar(value="out/gui_render")
    duration_var = tk.StringVar(value="45")
    bpm_var = tk.StringVar(value="")
    slice_grid_var = tk.StringVar(value="off")
    baseline_var = tk.StringVar(value="")
    semi_live_var = tk.BooleanVar(value=True)
    chunk_sec_var = tk.StringVar(value="8")
    track_path_var = tk.StringVar(value="Track: waiting")

    preset_row = ttk.Frame(frame)
    preset_row.pack(fill=tk.X, pady=(0, 8))
    ttk.Label(preset_row, text="Preset:").pack(side=tk.LEFT)
    preset_var = tk.StringVar(value="Default")
    preset_combo = ttk.Combobox(preset_row, values=list(PRESETS.keys()), textvariable=preset_var, state="readonly", width=28)
    preset_combo.pack(side=tk.LEFT, padx=(8, 10))

    vars_map: Dict[str, tk.DoubleVar] = {k: tk.DoubleVar(value=default) for k, (_, _, default) in RANGES.items()}
    section_var = tk.StringVar(value="")
    filter_var = tk.StringVar(value="auto")
    arc_var = tk.StringVar(value="classic")
    score_var = tk.StringVar(value="off")
    placement_var = tk.StringVar(value="any")
    hold_var = tk.BooleanVar(value=False)
    burst_var = tk.BooleanVar(value=False)
    panic_var = tk.BooleanVar(value=False)
    gui = ControlGUI(
        root=root,
        control_file=control_file,
        telemetry_file=telemetry_file,
        telemetry_pos=0,
        vars=vars_map,
        status_var=status_var,
        progress_var=progress_var,
        progress_text_var=progress_text_var,
        progress_detail_var=progress_detail_var,
        input_var=input_var,
        output_var=output_var,
        render_preset_var=preset_var,
        duration_var=duration_var,
        bpm_var=bpm_var,
        slice_grid_var=slice_grid_var,
        baseline_var=baseline_var,
        semi_live_var=semi_live_var,
        chunk_sec_var=chunk_sec_var,
        track_path_var=track_path_var,
        command_box=None,
        last_payload={},
        section_var=section_var,
        filter_var=filter_var,
        arc_var=arc_var,
        score_var=score_var,
        placement_var=placement_var,
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

    filter_row = ttk.Frame(frame)
    filter_row.pack(fill=tk.X, pady=(8, 4))
    ttk.Label(filter_row, text="filter_severity", width=22).pack(side=tk.LEFT)
    filter_combo = ttk.Combobox(filter_row, values=["auto", "light", "medium", "hard"], textvariable=filter_var, state="readonly", width=14)
    filter_combo.pack(side=tk.LEFT, padx=(8, 10))
    filter_combo.bind("<<ComboboxSelected>>", lambda _e: gui.write_payload())

    planner_row = ttk.Frame(frame)
    planner_row.pack(fill=tk.X, pady=(4, 4))
    ttk.Label(planner_row, text="section_arc", width=22).pack(side=tk.LEFT)
    arc_combo = ttk.Combobox(planner_row, values=SECTION_ARCS, textvariable=arc_var, state="readonly", width=14)
    arc_combo.pack(side=tk.LEFT, padx=(8, 16))
    ttk.Label(planner_row, text="source_score").pack(side=tk.LEFT)
    score_combo = ttk.Combobox(planner_row, values=SOURCE_SCORES, textvariable=score_var, state="readonly", width=12)
    score_combo.pack(side=tk.LEFT, padx=(8, 10))
    arc_combo.bind("<<ComboboxSelected>>", lambda _e: gui.write_payload())
    score_combo.bind("<<ComboboxSelected>>", lambda _e: gui.write_payload())

    placement_row = ttk.Frame(frame)
    placement_row.pack(fill=tk.X, pady=(4, 4))
    ttk.Label(placement_row, text="baseline_placement", width=22).pack(side=tk.LEFT)
    placement_combo = ttk.Combobox(placement_row, values=BASELINE_PLACEMENTS, textvariable=placement_var, state="readonly", width=14)
    placement_combo.pack(side=tk.LEFT, padx=(8, 10))
    placement_combo.bind("<<ComboboxSelected>>", lambda _e: gui.write_payload())

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

    render_frame = ttk.LabelFrame(frame, text="Render launcher", padding=8)
    render_frame.pack(fill=tk.X, pady=(8, 8))

    ttk.Label(render_frame, text="Input").grid(row=0, column=0, sticky="w")
    ttk.Entry(render_frame, textvariable=input_var).grid(row=0, column=1, sticky="ew", padx=(8, 8))
    ttk.Button(render_frame, text="Choose", command=gui.choose_input).grid(row=0, column=2, sticky="e")

    ttk.Label(render_frame, text="Output").grid(row=1, column=0, sticky="w", pady=(6, 0))
    ttk.Entry(render_frame, textvariable=output_var).grid(row=1, column=1, sticky="ew", padx=(8, 8), pady=(6, 0))
    ttk.Button(render_frame, text="Choose", command=gui.choose_output).grid(row=1, column=2, sticky="e", pady=(6, 0))

    ttk.Label(render_frame, text="Duration").grid(row=2, column=0, sticky="w", pady=(6, 0))
    ttk.Entry(render_frame, textvariable=duration_var, width=10).grid(row=2, column=1, sticky="w", padx=(8, 8), pady=(6, 0))
    ttk.Label(render_frame, text="BPM").grid(row=2, column=1, sticky="w", padx=(96, 0), pady=(6, 0))
    ttk.Entry(render_frame, textvariable=bpm_var, width=10).grid(row=2, column=1, sticky="w", padx=(136, 0), pady=(6, 0))
    ttk.Label(render_frame, text="Grid").grid(row=2, column=1, sticky="w", padx=(224, 0), pady=(6, 0))
    ttk.Combobox(render_frame, values=SLICE_GRIDS, textvariable=slice_grid_var, state="readonly", width=8).grid(row=2, column=1, sticky="w", padx=(264, 0), pady=(6, 0))

    ttk.Label(render_frame, text="Baseline").grid(row=3, column=0, sticky="w", pady=(6, 0))
    ttk.Entry(render_frame, textvariable=baseline_var).grid(row=3, column=1, sticky="ew", padx=(8, 8), pady=(6, 0))
    ttk.Button(render_frame, text="Choose", command=gui.choose_baseline).grid(row=3, column=2, sticky="e", pady=(6, 0))
    render_frame.columnconfigure(1, weight=1)

    semi_live_row = ttk.Frame(render_frame)
    semi_live_row.grid(row=4, column=1, columnspan=2, sticky="w", padx=(8, 0), pady=(8, 0))
    ttk.Checkbutton(semi_live_row, text="Semi-live track", variable=semi_live_var, command=gui.update_command_preview).pack(side=tk.LEFT)
    ttk.Label(semi_live_row, text="Chunk sec").pack(side=tk.LEFT, padx=(16, 4))
    ttk.Entry(semi_live_row, textvariable=chunk_sec_var, width=8).pack(side=tk.LEFT)

    ttk.Label(render_frame, text="Command", font=("TkDefaultFont", 10, "bold")).grid(row=5, column=0, sticky="nw", pady=(8, 0))
    command_box = tk.Text(render_frame, height=3, wrap="word")
    gui.command_box = command_box
    command_box.grid(row=5, column=1, columnspan=2, sticky="ew", padx=(8, 0), pady=(8, 0))
    command_box.configure(state="disabled")

    launch_row = ttk.Frame(render_frame)
    launch_row.grid(row=6, column=1, columnspan=2, sticky="w", padx=(8, 0), pady=(8, 0))
    ttk.Button(launch_row, text="Start render", command=gui.start_render).pack(side=tk.LEFT)
    ttk.Button(launch_row, text="Stop", command=gui.stop_render).pack(side=tk.LEFT, padx=(8, 0))

    progress_frame = ttk.LabelFrame(frame, text="Render progress", padding=8)
    progress_frame.pack(fill=tk.X, pady=(8, 8))
    progress_bar = ttk.Progressbar(progress_frame, variable=progress_var, maximum=100.0)
    progress_bar.pack(fill=tk.X)
    ttk.Label(progress_frame, textvariable=progress_text_var).pack(anchor="w", pady=(4, 0))
    ttk.Label(progress_frame, textvariable=progress_detail_var).pack(anchor="w")
    ttk.Label(progress_frame, textvariable=track_path_var).pack(anchor="w")
    ttk.Button(progress_frame, text="Open track", command=gui.open_track).pack(anchor="w", pady=(4, 0))

    status = ttk.Label(frame, textvariable=status_var)
    status.pack(anchor="w", pady=(10, 0))

    preset_combo.bind("<<ComboboxSelected>>", lambda _e: gui.apply_preset(preset_var.get()))
    sec_combo.bind("<<ComboboxSelected>>", lambda _e: gui.write_payload())
    for var in (input_var, output_var, preset_var, duration_var, bpm_var, slice_grid_var, baseline_var, chunk_sec_var):
        var.trace_add("write", gui.update_command_preview)

    gui.write_payload()
    gui.update_command_preview()
    gui.poll_telemetry()
    root.mainloop()


if __name__ == "__main__":
    main()
