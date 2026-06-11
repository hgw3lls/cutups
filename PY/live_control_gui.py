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
import socket
import struct
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


def analyze_script_path() -> Path:
    return Path(__file__).resolve().with_name("analyze_audio.py")


def open_with_system(path: Path) -> None:
    if sys.platform == "darwin":
        subprocess.Popen(["open", str(path)])
    elif sys.platform.startswith("win"):
        subprocess.Popen(["cmd", "/c", "start", "", str(path)])
    else:
        subprocess.Popen(["xdg-open", str(path)])


def osc_string(value: str) -> bytes:
    data = value.encode("utf-8") + b"\0"
    return data + (b"\0" * ((4 - (len(data) % 4)) % 4))


def build_osc_message(address: str, args: List[object] | Tuple[object, ...] = ()) -> bytes:
    if not address.startswith("/"):
        raise ValueError("OSC address must start with /")
    typetags = ","
    payload = bytearray()
    for arg in args:
        if isinstance(arg, bool):
            typetags += "i"
            payload.extend(struct.pack(">i", int(arg)))
        elif isinstance(arg, int):
            typetags += "i"
            payload.extend(struct.pack(">i", arg))
        elif isinstance(arg, float):
            typetags += "f"
            payload.extend(struct.pack(">f", arg))
        else:
            typetags += "s"
            payload.extend(osc_string(str(arg)))
    return osc_string(address) + osc_string(typetags) + bytes(payload)


def send_osc_message(host: str, port: int, address: str, args: List[object] | Tuple[object, ...] = ()) -> None:
    packet = build_osc_message(address, args)
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.sendto(packet, (host, port))


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
    analysis_cache: str = "",
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
    if analysis_cache.strip():
        cmd.extend(["--analysis-cache", analysis_cache.strip(), "--analysis-cache-readonly"])
    if semi_live:
        cmd.extend(["--semi-live", "--semi-live-chunk-sec", semi_live_chunk_sec.strip() or "8"])
    return cmd


def build_analyze_command(
    python_executable: str,
    script_path: Path,
    input_path: str,
    output_path: str,
    bpm: str = "",
    slice_grid: str = "off",
    progress_jsonl: str = "",
) -> List[str]:
    cmd = [
        python_executable,
        str(script_path),
        "--input",
        input_path,
        "--output",
        output_path,
        "--overwrite",
    ]
    if bpm.strip() and bpm.strip() not in {"0", "0.0"}:
        cmd.extend(["--bpm", bpm.strip()])
    if slice_grid and slice_grid != "off":
        cmd.extend(["--slice-grid", slice_grid])
    if progress_jsonl.strip():
        cmd.extend(["--progress-jsonl", progress_jsonl.strip(), "--no-progress"])
    return cmd


def resolve_render_path(raw: str, base_dir: Path) -> Path:
    path = Path(raw).expanduser()
    if path.is_absolute():
        return path
    return base_dir / path


def default_analysis_cache_path(input_path: str, base_dir: Path) -> Path:
    resolved = resolve_render_path(input_path.strip() or "./samples", base_dir)
    if resolved.is_dir():
        return resolved / "audio_analysis_cache.json"
    return resolved.with_name(f"{resolved.stem}_audio_analysis_cache.json")


def validate_analysis_settings(
    input_path: str,
    output_path: str,
    bpm: str,
    slice_grid: str,
    base_dir: Path,
) -> str:
    resolved_input = resolve_render_path(input_path.strip() or "./samples", base_dir)
    if not resolved_input.exists():
        return f"Input does not exist: {resolved_input}"
    resolved_output = resolve_render_path(output_path.strip(), base_dir)
    if resolved_output.exists() and resolved_output.is_dir():
        return f"Analysis cache path is a folder: {resolved_output}"
    grid = (slice_grid or "off").strip()
    bpm_value = (bpm or "").strip()
    if grid and grid != "off":
        try:
            parsed_bpm = float(bpm_value)
        except ValueError:
            return "Grid analysis requires a BPM. Enter BPM or set Grid to off."
        if parsed_bpm <= 0:
            return "Grid analysis requires a BPM. Enter BPM or set Grid to off."
    elif bpm_value:
        try:
            if float(bpm_value) <= 0:
                return "BPM must be greater than 0."
        except ValueError:
            return "BPM must be a number."
    return ""


def validate_render_settings(
    input_path: str,
    duration: str,
    bpm: str,
    slice_grid: str,
    baseline_beat: str,
    semi_live: bool,
    semi_live_chunk_sec: str,
    base_dir: Path,
    analysis_cache: str = "",
) -> str:
    resolved_input = resolve_render_path(input_path.strip() or "./samples", base_dir)
    if not resolved_input.exists():
        return f"Input does not exist: {resolved_input}"
    try:
        if float(duration.strip() or "45") <= 0:
            return "Duration must be greater than 0."
    except ValueError:
        return "Duration must be a number."
    grid = (slice_grid or "off").strip()
    bpm_value = (bpm or "").strip()
    if grid and grid != "off":
        try:
            parsed_bpm = float(bpm_value)
        except ValueError:
            return "Grid slicing requires a BPM. Enter BPM or set Grid to off."
        if parsed_bpm <= 0:
            return "Grid slicing requires a BPM. Enter BPM or set Grid to off."
    elif bpm_value:
        try:
            if float(bpm_value) <= 0:
                return "BPM must be greater than 0."
        except ValueError:
            return "BPM must be a number."
    baseline_value = baseline_beat.strip()
    if baseline_value and not resolve_render_path(baseline_value, base_dir).exists():
        return f"Baseline does not exist: {resolve_render_path(baseline_value, base_dir)}"
    cache_value = analysis_cache.strip()
    if cache_value and cache_value.lower() != "auto":
        cache_path = resolve_render_path(cache_value, base_dir)
        if cache_path.exists() and cache_path.is_dir():
            return f"Analysis cache path is a folder: {cache_path}"
        if not cache_path.exists():
            return f"Analysis cache not found: {cache_path}. Run Analyze sources or clear cache."
    if semi_live:
        try:
            if float(semi_live_chunk_sec.strip() or "8") < 1.0:
                return "Chunk sec must be at least 1."
        except ValueError:
            return "Chunk sec must be a number."
    return ""


def make_scrollable_frame(parent: Any) -> Tuple[Any, Any]:
    container = ttk.Frame(parent)
    canvas = tk.Canvas(container, highlightthickness=0)
    scrollbar = ttk.Scrollbar(container, orient="vertical", command=canvas.yview)
    inner = ttk.Frame(canvas, padding=12)
    window_id = canvas.create_window((0, 0), window=inner, anchor="nw")

    def update_scrollregion(_: object) -> None:
        canvas.configure(scrollregion=canvas.bbox("all"))

    def update_inner_width(event: object) -> None:
        width = getattr(event, "width", 0)
        if width:
            canvas.itemconfigure(window_id, width=width)

    inner.bind("<Configure>", update_scrollregion)
    canvas.bind("<Configure>", update_inner_width)
    canvas.configure(yscrollcommand=scrollbar.set)
    canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
    scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
    container.pack(fill=tk.BOTH, expand=True)
    return container, inner


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
    validation_var: tk.StringVar
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
    analysis_cache_var: tk.StringVar
    semi_live_var: tk.BooleanVar
    chunk_sec_var: tk.StringVar
    track_path_var: tk.StringVar
    sc_host_var: tk.StringVar
    sc_port_var: tk.StringVar
    sc_age_var: tk.StringVar
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
    render_log_path: Optional[Path] = None
    analysis_process: Optional[subprocess.Popen] = None
    analysis_log_handle: Any = None
    analysis_log_path: Optional[Path] = None
    analysis_telemetry_file: Optional[Path] = None
    analysis_telemetry_pos: int = 0

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

    def apply_telemetry_row(self, row: Dict[str, object]) -> None:
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
                    self.apply_telemetry_row(row)
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
            analysis_cache=self.analysis_cache_var.get().strip(),
        )

    def current_analysis_cache_path(self) -> Path:
        base_dir = cutup_script_path().parents[1]
        raw = self.analysis_cache_var.get().strip()
        if not raw or raw.lower() == "auto":
            return default_analysis_cache_path(self.input_var.get().strip() or "./samples", base_dir)
        return resolve_render_path(raw, base_dir)

    def current_analyze_command(self) -> List[str]:
        cache_path = self.current_analysis_cache_path()
        progress_path = self.analysis_telemetry_file or cache_path.with_suffix(".progress.jsonl")
        return build_analyze_command(
            sys.executable,
            analyze_script_path(),
            self.input_var.get().strip() or "./samples",
            str(cache_path),
            bpm=self.bpm_var.get().strip(),
            slice_grid=self.slice_grid_var.get().strip() or "off",
            progress_jsonl=str(progress_path),
        )

    def update_command_preview(self, *_: object) -> None:
        if self.command_box is None:
            return
        command = " ".join(shlex.quote(part) for part in self.current_render_command())
        self.command_box.configure(state="normal")
        self.command_box.delete("1.0", tk.END)
        self.command_box.insert("1.0", command)
        self.command_box.configure(state="disabled")
        self.update_launch_check()

    def current_validation_error(self) -> str:
        return validate_render_settings(
            self.input_var.get(),
            self.duration_var.get(),
            self.bpm_var.get(),
            self.slice_grid_var.get(),
            self.baseline_var.get(),
            bool(self.semi_live_var.get()),
            self.chunk_sec_var.get(),
            cutup_script_path().parents[1],
            analysis_cache=self.analysis_cache_var.get(),
        )

    def update_launch_check(self) -> None:
        error = self.current_validation_error()
        self.validation_var.set(error if error else "Ready to render")

    def choose_input_folder(self) -> None:
        chosen = filedialog.askdirectory(title="Choose audio source folder")
        if chosen:
            self.input_var.set(chosen)

    def choose_input_file(self) -> None:
        chosen = filedialog.askopenfilename(title="Choose audio source file", filetypes=[("Audio files", "*.wav *.mp3 *.flac *.aiff *.ogg *.m4a"), ("All files", "*.*")])
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

    def set_analysis_cache_auto(self) -> None:
        self.analysis_cache_var.set(str(self.current_analysis_cache_path()))

    def choose_analysis_cache(self) -> None:
        chosen = filedialog.asksaveasfilename(
            title="Choose analysis cache",
            defaultextension=".json",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")],
        )
        if chosen:
            self.analysis_cache_var.set(chosen)

    def clear_analysis_cache(self) -> None:
        self.analysis_cache_var.set("")

    def open_track(self) -> None:
        raw = self.track_path_var.get().strip()
        if not raw or raw in {"Track: waiting", "Waiting for first chunk"}:
            self.status_var.set("No semi-live track available yet")
            return
        path = Path(raw)
        if not path.exists():
            self.status_var.set(f"Track not written yet: {path}")
            return
        try:
            open_with_system(path)
        except OSError as exc:
            self.status_var.set(f"Could not open track: {exc}")

    def open_output(self) -> None:
        output = resolve_render_path(self.output_var.get().strip() or "out/gui_render", cutup_script_path().parents[1])
        target = output if output.exists() else output.parent
        if not target.exists():
            self.status_var.set(f"Output folder not available yet: {output}")
            return
        try:
            open_with_system(target)
        except OSError as exc:
            self.status_var.set(f"Could not open output: {exc}")

    def open_log(self) -> None:
        log_path = self.render_log_path or self.telemetry_file.with_suffix(".log")
        if not log_path.exists():
            self.status_var.set(f"Log not written yet: {log_path}")
            return
        try:
            open_with_system(log_path)
        except OSError as exc:
            self.status_var.set(f"Could not open log: {exc}")

    def copy_command(self) -> None:
        command = " ".join(shlex.quote(part) for part in self.current_render_command())
        self.root.clipboard_clear()
        self.root.clipboard_append(command)
        self.status_var.set("Copied render command")

    def log_tail(self, path: Optional[Path], max_lines: int = 6) -> str:
        if not path or not path.exists():
            return ""
        try:
            lines = [line.strip() for line in path.read_text(encoding="utf-8", errors="replace").splitlines() if line.strip()]
        except OSError:
            return ""
        return " | ".join(lines[-max_lines:])

    def start_analysis(self) -> None:
        if self.analysis_process and self.analysis_process.poll() is None:
            self.status_var.set("Analysis already running")
            return
        render_cwd = cutup_script_path().parents[1]
        cache_path = self.current_analysis_cache_path()
        if not self.analysis_cache_var.get().strip():
            self.analysis_cache_var.set(str(cache_path))
        validation_error = validate_analysis_settings(
            self.input_var.get(),
            str(cache_path),
            self.bpm_var.get(),
            self.slice_grid_var.get(),
            render_cwd,
        )
        if validation_error:
            self.status_var.set(validation_error)
            self.progress_detail_var.set(validation_error)
            return
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        log_path = cache_path.with_suffix(".log")
        progress_path = cache_path.with_suffix(".progress.jsonl")
        self.analysis_log_path = log_path
        self.analysis_telemetry_file = progress_path
        self.analysis_telemetry_pos = 0
        progress_path.write_text("", encoding="utf-8")
        self.analysis_log_handle = log_path.open("w", encoding="utf-8")
        cmd = self.current_analyze_command()
        try:
            self.analysis_process = subprocess.Popen(
                cmd,
                cwd=str(render_cwd),
                stdout=self.analysis_log_handle,
                stderr=subprocess.STDOUT,
            )
        except OSError as exc:
            self.analysis_log_handle.close()
            self.analysis_log_handle = None
            self.status_var.set(f"Analysis failed to start: {exc}")
            return
        self.status_var.set(f"Analysis running. Log: {log_path}")
        self.progress_var.set(0.0)
        self.progress_text_var.set("  0.0%  analysis  ETA --:--")
        self.progress_detail_var.set(f"Analysis cache: {cache_path}")
        self.root.after(500, self.poll_analysis)

    def poll_analysis_telemetry(self) -> None:
        if not self.analysis_telemetry_file:
            return
        try:
            if not self.analysis_telemetry_file.exists():
                return
            with self.analysis_telemetry_file.open("r", encoding="utf-8") as f:
                f.seek(self.analysis_telemetry_pos)
                while True:
                    line = f.readline()
                    if not line:
                        break
                    self.analysis_telemetry_pos = f.tell()
                    try:
                        row = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(row, dict):
                        self.apply_telemetry_row(row)
        except OSError as exc:
            self.progress_detail_var.set(f"Analysis telemetry unavailable: {exc}")

    def poll_analysis(self) -> None:
        if not self.analysis_process:
            return
        code = self.analysis_process.poll()
        self.poll_analysis_telemetry()
        if code is None:
            self.root.after(500, self.poll_analysis)
            return
        if self.analysis_log_handle:
            self.analysis_log_handle.close()
            self.analysis_log_handle = None
        cache_path = self.current_analysis_cache_path()
        if code == 0:
            self.status_var.set(f"Analysis cache ready: {cache_path}")
            self.progress_detail_var.set(f"Analysis cache ready: {cache_path}")
            self.update_command_preview()
        else:
            tail = self.log_tail(self.analysis_log_path)
            message = f"Analysis failed with code {code}: {tail}" if tail else f"Analysis exited with code {code}"
            self.status_var.set(message)
            self.progress_detail_var.set(message)

    def sc_variant_path(self) -> Path:
        track_raw = self.track_path_var.get().strip()
        if track_raw and track_raw not in {"Track: waiting", "Waiting for first chunk"}:
            track_path = Path(track_raw).expanduser()
            if track_path.suffix.lower() == ".wav":
                return track_path.parent
        output = resolve_render_path(self.output_var.get().strip() or "out/gui_render", cutup_script_path().parents[1])
        return output / "audio_cutups" / "cutup_01"

    def sc_live_source(self) -> Path:
        track_raw = self.track_path_var.get().strip()
        if track_raw and track_raw not in {"Track: waiting", "Waiting for first chunk"}:
            track_path = Path(track_raw).expanduser()
            if track_path.suffix.lower() == ".wav":
                return track_path
        return self.sc_variant_path()

    def send_sc(self, address: str, *args: object) -> None:
        host = self.sc_host_var.get().strip() or "127.0.0.1"
        try:
            port = int(self.sc_port_var.get().strip() or "57120")
        except ValueError:
            self.status_var.set("SC port must be an integer")
            return
        try:
            send_osc_message(host, port, address, args)
        except (OSError, ValueError) as exc:
            self.status_var.set(f"OSC send failed: {exc}")
            return
        rendered_args = " ".join(str(arg) for arg in args)
        suffix = f" {rendered_args}" if rendered_args else ""
        self.status_var.set(f"Sent OSC {address}{suffix} -> {host}:{port}")

    def send_sc_load_live(self) -> None:
        self.send_sc("/cutups/loadLive", str(self.sc_live_source()))

    def send_sc_load_chunks(self) -> None:
        self.send_sc("/cutups/loadChunks", str(self.sc_variant_path()))

    def send_sc_refresh_chunks(self) -> None:
        self.send_sc("/cutups/chunks/refresh")

    def send_sc_live_start(self) -> None:
        self.send_sc("/cutups/live/start", str(self.sc_live_source()))

    def send_sc_live_stop(self) -> None:
        self.send_sc("/cutups/live/stop")

    def send_sc_native_input_start(self) -> None:
        self.send_sc("/cutups/scLive/input/start")

    def send_sc_native_input_stop(self) -> None:
        self.send_sc("/cutups/scLive/input/stop")

    def send_sc_native_cuts_start(self) -> None:
        self.send_sc("/cutups/scLive/cuts/start")

    def send_sc_native_cuts_stop(self) -> None:
        self.send_sc("/cutups/scLive/cuts/stop")

    def send_sc_native_source_material(self) -> None:
        self.send_sc("/cutups/scLive/source", "material")

    def send_sc_native_source_live(self) -> None:
        self.send_sc("/cutups/scLive/source", "live")

    def send_sc_native_source_blend(self) -> None:
        self.send_sc("/cutups/scLive/source", "blend")

    def send_sc_preset_balanced(self) -> None:
        self.send_sc("/cutups/scLive/preset", "balanced")

    def send_sc_preset_breach(self) -> None:
        self.send_sc("/cutups/scLive/preset", "signal-breach")

    def send_sc_preset_ghost(self) -> None:
        self.send_sc("/cutups/scLive/preset", "ghost-transmission")

    def send_sc_preset_beat(self) -> None:
        self.send_sc("/cutups/scLive/preset", "beat-stutter")

    def send_sc_preset_spoken(self) -> None:
        self.send_sc("/cutups/scLive/preset", "spoken-word")

    def send_sc_cue_balanced(self) -> None:
        self.send_sc("/cutups/scLive/cue", "balanced")

    def send_sc_cue_breach(self) -> None:
        self.send_sc("/cutups/scLive/cue", "signal-breach")

    def send_sc_cue_ghost(self) -> None:
        self.send_sc("/cutups/scLive/cue", "ghost-transmission")

    def send_sc_cue_beat(self) -> None:
        self.send_sc("/cutups/scLive/cue", "beat-stutter")

    def send_sc_cue_spoken(self) -> None:
        self.send_sc("/cutups/scLive/cue", "spoken-word")

    def send_sc_bed_start(self) -> None:
        self.send_sc("/cutups/scLive/bed/start")

    def send_sc_bed_stop(self) -> None:
        self.send_sc("/cutups/scLive/bed/stop")

    def send_sc_bed_reverse(self) -> None:
        self.send_sc("/cutups/scLive/bed/reverse")

    def send_sc_bed_slot_a(self) -> None:
        self.send_sc("/cutups/scLive/bed/slot", "A")

    def send_sc_bed_slot_b(self) -> None:
        self.send_sc("/cutups/scLive/bed/slot", "B")

    def send_sc_bed_slot_c(self) -> None:
        self.send_sc("/cutups/scLive/bed/slot", "C")

    def send_sc_bed_slots_clear(self) -> None:
        self.send_sc("/cutups/scLive/bed/slot/clear")

    def send_sc_native_stab(self) -> None:
        self.send_sc("/cutups/scLive/stab")

    def send_sc_native_glitch(self) -> None:
        self.send_sc("/cutups/scLive/glitch")

    def send_sc_overdub_off(self) -> None:
        self.send_sc("/cutups/scLive/overdub", 0.0)

    def send_sc_overdub_low(self) -> None:
        self.send_sc("/cutups/scLive/overdub", 0.18)

    def send_sc_overdub_high(self) -> None:
        self.send_sc("/cutups/scLive/overdub", 0.45)

    def send_sc_decay_tight(self) -> None:
        self.send_sc("/cutups/scLive/decay", 0.72)

    def send_sc_decay_smear(self) -> None:
        self.send_sc("/cutups/scLive/decay", 0.92)

    def send_sc_stab(self) -> None:
        self.send_sc("/cutups/stab")

    def send_sc_phrase(self) -> None:
        self.send_sc("/cutups/phrase")

    def send_sc_concrete(self) -> None:
        self.send_sc("/cutups/concrete")

    def send_sc_scene_on(self) -> None:
        self.send_sc("/cutups/scene", 1)

    def send_sc_scene_off(self) -> None:
        self.send_sc("/cutups/scene", 0)

    def send_sc_stop(self) -> None:
        self.send_sc("/cutups/stop")

    def send_sc_age(self) -> None:
        try:
            age = float(self.sc_age_var.get().strip() or "0")
        except ValueError:
            self.status_var.set("SC age must be a number from 0 to 1")
            return
        self.send_sc("/cutups/master/age", max(0.0, min(1.0, age)))

    def render_log_tail(self, max_lines: int = 6) -> str:
        return self.log_tail(self.render_log_path, max_lines=max_lines)

    def start_render(self) -> None:
        if self.render_process and self.render_process.poll() is None:
            self.status_var.set("Render already running")
            return
        render_cwd = cutup_script_path().parents[1]
        validation_error = self.current_validation_error()
        if validation_error:
            self.status_var.set(validation_error)
            self.progress_detail_var.set(validation_error)
            self.validation_var.set(validation_error)
            return
        self.write_payload()
        self.progress_var.set(0.0)
        self.progress_text_var.set("  0.0%  starting  ETA --:--")
        self.progress_detail_var.set(f"Telemetry: {self.telemetry_file}")
        self.track_path_var.set("Waiting for first chunk")
        self.telemetry_pos = 0
        self.telemetry_file.parent.mkdir(parents=True, exist_ok=True)
        self.telemetry_file.write_text("", encoding="utf-8")
        log_path = self.telemetry_file.with_suffix(".log")
        self.render_log_path = log_path
        self.render_log_handle = log_path.open("w", encoding="utf-8")
        cmd = self.current_render_command()
        try:
            self.render_process = subprocess.Popen(
                cmd,
                cwd=str(render_cwd),
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
        if code == 0:
            self.status_var.set("Render complete")
        else:
            tail = self.render_log_tail()
            message = f"Render failed with code {code}: {tail}" if tail else f"Render exited with code {code}"
            self.status_var.set(message)
            self.progress_detail_var.set(message)

    def apply_preset(self, preset_name: str) -> None:
        data = PRESETS.get(preset_name, PRESETS["Default"])
        for key, var in self.vars.items():
            var.set(float(data.get(key, RANGES[key][2])))
        self.filter_var.set(str(data.get("filter_severity", "auto")))
        self.arc_var.set(str(data.get("section_arc", "classic")))
        self.score_var.set(str(data.get("source_score", "off")))
        self.placement_var.set(str(data.get("baseline_placement", "any")))
        if preset_name in {"beat-cutup", "hard-stutter"}:
            if not self.bpm_var.get().strip():
                self.bpm_var.set("120")
            if self.slice_grid_var.get().strip() == "off":
                self.slice_grid_var.set("1/16")
        self.write_payload()
        self.update_command_preview()

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
    root.geometry("1080x760")
    root.minsize(820, 580)

    _scroll_container, frame = make_scrollable_frame(root)

    ttk.Label(frame, text="cutup.py realtime control", font=("TkDefaultFont", 14, "bold")).pack(anchor="w")
    ttk.Label(
        frame,
        text="Move sliders while cutup.py is running with --live-control-file to update generation in-flight.",
    ).pack(anchor="w", pady=(0, 10))

    status_var = tk.StringVar(value=f"Control file: {control_file}")
    validation_var = tk.StringVar(value="Checking launch settings")
    progress_var = tk.DoubleVar(value=0.0)
    progress_text_var = tk.StringVar(value="  0.0%  waiting  ETA --:--")
    progress_detail_var = tk.StringVar(value=f"Telemetry: {telemetry_file}")
    input_var = tk.StringVar(value="./samples")
    output_var = tk.StringVar(value="out/gui_render")
    duration_var = tk.StringVar(value="45")
    bpm_var = tk.StringVar(value="")
    slice_grid_var = tk.StringVar(value="off")
    baseline_var = tk.StringVar(value="")
    analysis_cache_var = tk.StringVar(value="")
    semi_live_var = tk.BooleanVar(value=True)
    chunk_sec_var = tk.StringVar(value="8")
    track_path_var = tk.StringVar(value="Waiting for first chunk")
    sc_host_var = tk.StringVar(value="127.0.0.1")
    sc_port_var = tk.StringVar(value="57120")
    sc_age_var = tk.StringVar(value="0.25")

    tabs = ttk.Notebook(frame)
    tabs.pack(fill=tk.BOTH, expand=True, pady=(0, 8))
    render_tab = ttk.Frame(tabs, padding=4)
    sc_tab = ttk.Frame(tabs, padding=4)
    params_tab = ttk.Frame(tabs, padding=4)
    tabs.add(render_tab, text="Render")
    tabs.add(sc_tab, text="SuperCollider")
    tabs.add(params_tab, text="Parameters")

    preset_row = ttk.Frame(render_tab)
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
        validation_var=validation_var,
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
        analysis_cache_var=analysis_cache_var,
        semi_live_var=semi_live_var,
        chunk_sec_var=chunk_sec_var,
        track_path_var=track_path_var,
        sc_host_var=sc_host_var,
        sc_port_var=sc_port_var,
        sc_age_var=sc_age_var,
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

    render_frame = ttk.LabelFrame(render_tab, text="Render launcher", padding=8)
    render_frame.pack(fill=tk.X, pady=(8, 8))

    ttk.Label(render_frame, text="Input").grid(row=0, column=0, sticky="w")
    ttk.Entry(render_frame, textvariable=input_var).grid(row=0, column=1, sticky="ew", padx=(8, 8))
    input_buttons = ttk.Frame(render_frame)
    input_buttons.grid(row=0, column=2, sticky="e")
    ttk.Button(input_buttons, text="Folder", command=gui.choose_input_folder).pack(side=tk.LEFT)
    ttk.Button(input_buttons, text="File", command=gui.choose_input_file).pack(side=tk.LEFT, padx=(6, 0))

    ttk.Label(render_frame, text="Output").grid(row=1, column=0, sticky="w", pady=(6, 0))
    ttk.Entry(render_frame, textvariable=output_var).grid(row=1, column=1, sticky="ew", padx=(8, 8), pady=(6, 0))
    output_buttons = ttk.Frame(render_frame)
    output_buttons.grid(row=1, column=2, sticky="e", pady=(6, 0))
    ttk.Button(output_buttons, text="Choose", command=gui.choose_output).pack(side=tk.LEFT)
    ttk.Button(output_buttons, text="Open", command=gui.open_output).pack(side=tk.LEFT, padx=(6, 0))

    timing_row = ttk.Frame(render_frame)
    timing_row.grid(row=2, column=1, columnspan=2, sticky="w", padx=(8, 0), pady=(6, 0))
    ttk.Label(render_frame, text="Timing").grid(row=2, column=0, sticky="w", pady=(6, 0))
    ttk.Label(timing_row, text="Duration").pack(side=tk.LEFT)
    ttk.Entry(timing_row, textvariable=duration_var, width=10).pack(side=tk.LEFT, padx=(4, 14))
    ttk.Label(timing_row, text="BPM").pack(side=tk.LEFT)
    ttk.Entry(timing_row, textvariable=bpm_var, width=10).pack(side=tk.LEFT, padx=(4, 14))
    ttk.Label(timing_row, text="Grid").pack(side=tk.LEFT)
    ttk.Combobox(timing_row, values=SLICE_GRIDS, textvariable=slice_grid_var, state="readonly", width=8).pack(side=tk.LEFT, padx=(4, 0))

    ttk.Label(render_frame, text="Baseline").grid(row=3, column=0, sticky="w", pady=(6, 0))
    ttk.Entry(render_frame, textvariable=baseline_var).grid(row=3, column=1, sticky="ew", padx=(8, 8), pady=(6, 0))
    ttk.Button(render_frame, text="Choose", command=gui.choose_baseline).grid(row=3, column=2, sticky="e", pady=(6, 0))
    render_frame.columnconfigure(1, weight=1)

    ttk.Label(render_frame, text="Analysis cache").grid(row=4, column=0, sticky="w", pady=(6, 0))
    ttk.Entry(render_frame, textvariable=analysis_cache_var).grid(row=4, column=1, sticky="ew", padx=(8, 8), pady=(6, 0))
    analysis_buttons = ttk.Frame(render_frame)
    analysis_buttons.grid(row=4, column=2, sticky="e", pady=(6, 0))
    ttk.Button(analysis_buttons, text="Auto", command=gui.set_analysis_cache_auto).pack(side=tk.LEFT)
    ttk.Button(analysis_buttons, text="Choose", command=gui.choose_analysis_cache).pack(side=tk.LEFT, padx=(6, 0))
    ttk.Button(analysis_buttons, text="Analyze", command=gui.start_analysis).pack(side=tk.LEFT, padx=(6, 0))
    ttk.Button(analysis_buttons, text="Clear", command=gui.clear_analysis_cache).pack(side=tk.LEFT, padx=(6, 0))

    semi_live_row = ttk.Frame(render_frame)
    semi_live_row.grid(row=5, column=1, columnspan=2, sticky="w", padx=(8, 0), pady=(8, 0))
    ttk.Checkbutton(semi_live_row, text="Semi-live track", variable=semi_live_var, command=gui.update_command_preview).pack(side=tk.LEFT)
    ttk.Label(semi_live_row, text="Chunk sec").pack(side=tk.LEFT, padx=(16, 4))
    ttk.Entry(semi_live_row, textvariable=chunk_sec_var, width=8).pack(side=tk.LEFT)

    ttk.Label(render_frame, text="Command", font=("TkDefaultFont", 10, "bold")).grid(row=6, column=0, sticky="nw", pady=(8, 0))
    command_box = tk.Text(render_frame, height=3, wrap="word")
    gui.command_box = command_box
    command_box.grid(row=6, column=1, columnspan=2, sticky="ew", padx=(8, 0), pady=(8, 0))
    command_box.configure(state="disabled")

    launch_row = ttk.Frame(render_frame)
    launch_row.grid(row=7, column=1, columnspan=2, sticky="w", padx=(8, 0), pady=(8, 0))
    ttk.Button(launch_row, text="Start render", command=gui.start_render).pack(side=tk.LEFT)
    ttk.Button(launch_row, text="Stop", command=gui.stop_render).pack(side=tk.LEFT, padx=(8, 0))
    ttk.Button(launch_row, text="Copy command", command=gui.copy_command).pack(side=tk.LEFT, padx=(8, 0))
    ttk.Button(launch_row, text="Open log", command=gui.open_log).pack(side=tk.LEFT, padx=(8, 0))
    ttk.Label(render_frame, textvariable=validation_var).grid(row=8, column=1, columnspan=2, sticky="w", padx=(8, 0), pady=(4, 0))

    progress_frame = ttk.LabelFrame(render_tab, text="Render progress / playable track", padding=8)
    progress_frame.pack(fill=tk.X, pady=(8, 8))
    progress_frame.columnconfigure(1, weight=1)
    progress_bar = ttk.Progressbar(progress_frame, variable=progress_var, maximum=100.0)
    progress_bar.grid(row=0, column=0, columnspan=3, sticky="ew")
    ttk.Label(progress_frame, textvariable=progress_text_var).grid(row=1, column=0, columnspan=3, sticky="w", pady=(4, 0))
    ttk.Label(progress_frame, textvariable=progress_detail_var).grid(row=2, column=0, columnspan=3, sticky="w")
    ttk.Label(progress_frame, text="Playable track").grid(row=3, column=0, sticky="w", pady=(6, 0))
    ttk.Entry(progress_frame, textvariable=track_path_var, state="readonly").grid(row=3, column=1, sticky="ew", padx=(8, 8), pady=(6, 0))
    ttk.Button(progress_frame, text="Open track", command=gui.open_track).grid(row=3, column=2, sticky="e", pady=(6, 0))

    sc_frame = ttk.LabelFrame(sc_tab, text="Send to SuperCollider", padding=8)
    sc_frame.pack(fill=tk.X, pady=(0, 8))
    sc_frame.columnconfigure(5, weight=1)

    ttk.Label(sc_frame, text="Host").grid(row=0, column=0, sticky="w")
    ttk.Entry(sc_frame, textvariable=sc_host_var, width=15).grid(row=0, column=1, sticky="w", padx=(6, 12))
    ttk.Label(sc_frame, text="Port").grid(row=0, column=2, sticky="w")
    ttk.Entry(sc_frame, textvariable=sc_port_var, width=8).grid(row=0, column=3, sticky="w", padx=(6, 12))
    ttk.Label(sc_frame, text="Age").grid(row=0, column=4, sticky="w")
    ttk.Entry(sc_frame, textvariable=sc_age_var, width=7).grid(row=0, column=5, sticky="w", padx=(6, 0))

    sc_row_a = ttk.Frame(sc_frame)
    sc_row_a.grid(row=1, column=0, columnspan=6, sticky="w", pady=(8, 0))
    ttk.Button(sc_row_a, text="Load live", command=gui.send_sc_load_live).pack(side=tk.LEFT)
    ttk.Button(sc_row_a, text="Load chunks", command=gui.send_sc_load_chunks).pack(side=tk.LEFT, padx=(6, 0))
    ttk.Button(sc_row_a, text="Refresh chunks", command=gui.send_sc_refresh_chunks).pack(side=tk.LEFT, padx=(6, 0))
    ttk.Button(sc_row_a, text="Live start", command=gui.send_sc_live_start).pack(side=tk.LEFT, padx=(12, 0))
    ttk.Button(sc_row_a, text="Live stop", command=gui.send_sc_live_stop).pack(side=tk.LEFT, padx=(6, 0))
    ttk.Button(sc_row_a, text="Scene on", command=gui.send_sc_scene_on).pack(side=tk.LEFT, padx=(12, 0))
    ttk.Button(sc_row_a, text="Scene off", command=gui.send_sc_scene_off).pack(side=tk.LEFT, padx=(6, 0))

    sc_row_b = ttk.Frame(sc_frame)
    sc_row_b.grid(row=2, column=0, columnspan=6, sticky="w", pady=(6, 0))
    ttk.Button(sc_row_b, text="Stab", command=gui.send_sc_stab).pack(side=tk.LEFT)
    ttk.Button(sc_row_b, text="Phrase", command=gui.send_sc_phrase).pack(side=tk.LEFT, padx=(6, 0))
    ttk.Button(sc_row_b, text="Concrete", command=gui.send_sc_concrete).pack(side=tk.LEFT, padx=(6, 0))
    ttk.Button(sc_row_b, text="Send age", command=gui.send_sc_age).pack(side=tk.LEFT, padx=(12, 0))
    ttk.Button(sc_row_b, text="Stop all", command=gui.send_sc_stop).pack(side=tk.LEFT, padx=(6, 0))

    sc_row_c = ttk.Frame(sc_frame)
    sc_row_c.grid(row=3, column=0, columnspan=6, sticky="w", pady=(6, 0))
    ttk.Button(sc_row_c, text="SC input on", command=gui.send_sc_native_input_start).pack(side=tk.LEFT)
    ttk.Button(sc_row_c, text="SC input off", command=gui.send_sc_native_input_stop).pack(side=tk.LEFT, padx=(6, 0))
    ttk.Button(sc_row_c, text="SC cuts on", command=gui.send_sc_native_cuts_start).pack(side=tk.LEFT, padx=(12, 0))
    ttk.Button(sc_row_c, text="SC cuts off", command=gui.send_sc_native_cuts_stop).pack(side=tk.LEFT, padx=(6, 0))
    ttk.Button(sc_row_c, text="SC stab", command=gui.send_sc_native_stab).pack(side=tk.LEFT, padx=(12, 0))
    ttk.Button(sc_row_c, text="SC glitch", command=gui.send_sc_native_glitch).pack(side=tk.LEFT, padx=(6, 0))

    sc_row_d = ttk.Frame(sc_frame)
    sc_row_d.grid(row=4, column=0, columnspan=6, sticky="w", pady=(6, 0))
    ttk.Label(sc_row_d, text="SC source").pack(side=tk.LEFT)
    ttk.Button(sc_row_d, text="Material", command=gui.send_sc_native_source_material).pack(side=tk.LEFT, padx=(8, 0))
    ttk.Button(sc_row_d, text="Live", command=gui.send_sc_native_source_live).pack(side=tk.LEFT, padx=(6, 0))
    ttk.Button(sc_row_d, text="Blend", command=gui.send_sc_native_source_blend).pack(side=tk.LEFT, padx=(6, 0))
    ttk.Button(sc_row_d, text="Bed on", command=gui.send_sc_bed_start).pack(side=tk.LEFT, padx=(12, 0))
    ttk.Button(sc_row_d, text="Bed off", command=gui.send_sc_bed_stop).pack(side=tk.LEFT, padx=(6, 0))
    ttk.Button(sc_row_d, text="Bed rev", command=gui.send_sc_bed_reverse).pack(side=tk.LEFT, padx=(6, 0))

    sc_row_e = ttk.Frame(sc_frame)
    sc_row_e.grid(row=5, column=0, columnspan=6, sticky="w", pady=(6, 0))
    ttk.Label(sc_row_e, text="SC preset").pack(side=tk.LEFT)
    ttk.Button(sc_row_e, text="Balanced", command=gui.send_sc_preset_balanced).pack(side=tk.LEFT, padx=(8, 0))
    ttk.Button(sc_row_e, text="Breach", command=gui.send_sc_preset_breach).pack(side=tk.LEFT, padx=(6, 0))
    ttk.Button(sc_row_e, text="Ghost", command=gui.send_sc_preset_ghost).pack(side=tk.LEFT, padx=(6, 0))
    ttk.Button(sc_row_e, text="Beat", command=gui.send_sc_preset_beat).pack(side=tk.LEFT, padx=(6, 0))
    ttk.Button(sc_row_e, text="Spoken", command=gui.send_sc_preset_spoken).pack(side=tk.LEFT, padx=(6, 0))

    sc_row_f = ttk.Frame(sc_frame)
    sc_row_f.grid(row=6, column=0, columnspan=6, sticky="w", pady=(6, 0))
    ttk.Label(sc_row_f, text="SC cue").pack(side=tk.LEFT)
    ttk.Button(sc_row_f, text="Balanced", command=gui.send_sc_cue_balanced).pack(side=tk.LEFT, padx=(8, 0))
    ttk.Button(sc_row_f, text="Breach", command=gui.send_sc_cue_breach).pack(side=tk.LEFT, padx=(6, 0))
    ttk.Button(sc_row_f, text="Ghost", command=gui.send_sc_cue_ghost).pack(side=tk.LEFT, padx=(6, 0))
    ttk.Button(sc_row_f, text="Beat", command=gui.send_sc_cue_beat).pack(side=tk.LEFT, padx=(6, 0))
    ttk.Button(sc_row_f, text="Spoken", command=gui.send_sc_cue_spoken).pack(side=tk.LEFT, padx=(6, 0))

    sc_row_g = ttk.Frame(sc_frame)
    sc_row_g.grid(row=7, column=0, columnspan=6, sticky="w", pady=(6, 0))
    ttk.Label(sc_row_g, text="Bed slots").pack(side=tk.LEFT)
    ttk.Button(sc_row_g, text="A", command=gui.send_sc_bed_slot_a, width=4).pack(side=tk.LEFT, padx=(8, 0))
    ttk.Button(sc_row_g, text="B", command=gui.send_sc_bed_slot_b, width=4).pack(side=tk.LEFT, padx=(6, 0))
    ttk.Button(sc_row_g, text="C", command=gui.send_sc_bed_slot_c, width=4).pack(side=tk.LEFT, padx=(6, 0))
    ttk.Button(sc_row_g, text="Clear", command=gui.send_sc_bed_slots_clear).pack(side=tk.LEFT, padx=(6, 0))

    sc_row_h = ttk.Frame(sc_frame)
    sc_row_h.grid(row=8, column=0, columnspan=6, sticky="w", pady=(6, 0))
    ttk.Label(sc_row_h, text="Feedback").pack(side=tk.LEFT)
    ttk.Button(sc_row_h, text="Overdub off", command=gui.send_sc_overdub_off).pack(side=tk.LEFT, padx=(8, 0))
    ttk.Button(sc_row_h, text="Low", command=gui.send_sc_overdub_low, width=5).pack(side=tk.LEFT, padx=(6, 0))
    ttk.Button(sc_row_h, text="High", command=gui.send_sc_overdub_high, width=5).pack(side=tk.LEFT, padx=(6, 0))
    ttk.Button(sc_row_h, text="Decay tight", command=gui.send_sc_decay_tight).pack(side=tk.LEFT, padx=(12, 0))
    ttk.Button(sc_row_h, text="Decay smear", command=gui.send_sc_decay_smear).pack(side=tk.LEFT, padx=(6, 0))

    status = ttk.Label(frame, textvariable=status_var)
    status.pack(anchor="w", pady=(2, 10))

    def on_slide(_: str = "") -> None:
        gui.write_payload()

    for key, (low, high, _) in RANGES.items():
        row = ttk.Frame(params_tab)
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

    filter_row = ttk.Frame(params_tab)
    filter_row.pack(fill=tk.X, pady=(8, 4))
    ttk.Label(filter_row, text="filter_severity", width=22).pack(side=tk.LEFT)
    filter_combo = ttk.Combobox(filter_row, values=["auto", "light", "medium", "hard"], textvariable=filter_var, state="readonly", width=14)
    filter_combo.pack(side=tk.LEFT, padx=(8, 10))
    filter_combo.bind("<<ComboboxSelected>>", lambda _e: gui.write_payload())

    planner_row = ttk.Frame(params_tab)
    planner_row.pack(fill=tk.X, pady=(4, 4))
    ttk.Label(planner_row, text="section_arc", width=22).pack(side=tk.LEFT)
    arc_combo = ttk.Combobox(planner_row, values=SECTION_ARCS, textvariable=arc_var, state="readonly", width=14)
    arc_combo.pack(side=tk.LEFT, padx=(8, 16))
    ttk.Label(planner_row, text="source_score").pack(side=tk.LEFT)
    score_combo = ttk.Combobox(planner_row, values=SOURCE_SCORES, textvariable=score_var, state="readonly", width=12)
    score_combo.pack(side=tk.LEFT, padx=(8, 10))
    arc_combo.bind("<<ComboboxSelected>>", lambda _e: gui.write_payload())
    score_combo.bind("<<ComboboxSelected>>", lambda _e: gui.write_payload())

    placement_row = ttk.Frame(params_tab)
    placement_row.pack(fill=tk.X, pady=(4, 4))
    ttk.Label(placement_row, text="baseline_placement", width=22).pack(side=tk.LEFT)
    placement_combo = ttk.Combobox(placement_row, values=BASELINE_PLACEMENTS, textvariable=placement_var, state="readonly", width=14)
    placement_combo.pack(side=tk.LEFT, padx=(8, 10))
    placement_combo.bind("<<ComboboxSelected>>", lambda _e: gui.write_payload())

    btns = ttk.Frame(params_tab)
    btns.pack(fill=tk.X, pady=(10, 8))
    ttk.Button(btns, text="Apply preset", command=lambda: gui.apply_preset(preset_var.get())).pack(side=tk.LEFT)
    ttk.Button(btns, text="Reset defaults", command=gui.reset_defaults).pack(side=tk.LEFT, padx=(8, 0))
    ttk.Button(btns, text="Write now", command=gui.write_payload).pack(side=tk.LEFT, padx=(8, 0))

    conductor = ttk.LabelFrame(params_tab, text="Conductor controls", padding=8)
    conductor.pack(fill=tk.X, pady=(8, 8))
    ttk.Label(conductor, text="Force section").grid(row=0, column=0, sticky="w")
    sec_combo = ttk.Combobox(conductor, values=["", "ENTRY", "BUILD", "PRESSURE", "COLLAPSE", "AFTERIMAGE"], textvariable=section_var, state="readonly", width=16)
    sec_combo.grid(row=0, column=1, sticky="w", padx=(8, 12))
    ttk.Checkbutton(conductor, text="Hold section", variable=hold_var, command=gui.write_payload).grid(row=0, column=2, sticky="w")
    ttk.Checkbutton(conductor, text="Burst now", variable=burst_var, command=gui.write_payload).grid(row=1, column=0, sticky="w", pady=(6, 0))
    ttk.Checkbutton(conductor, text="Panic silence", variable=panic_var, command=gui.write_payload).grid(row=1, column=1, sticky="w", pady=(6, 0))

    preset_combo.bind("<<ComboboxSelected>>", lambda _e: gui.apply_preset(preset_var.get()))
    sec_combo.bind("<<ComboboxSelected>>", lambda _e: gui.write_payload())
    for var in (input_var, output_var, preset_var, duration_var, bpm_var, slice_grid_var, baseline_var, analysis_cache_var, chunk_sec_var):
        var.trace_add("write", gui.update_command_preview)

    gui.write_payload()
    gui.update_command_preview()
    gui.poll_telemetry()
    root.mainloop()


if __name__ == "__main__":
    main()
