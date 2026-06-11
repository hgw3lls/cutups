#!/usr/bin/env python3
"""
cutup.py

Integrated TRANSMISSIONS workflow:
- audio
- agitprop
- cuttargets
- both
- all

An unstable composition instrument for political cut-up and musique concrète
speech collage.
"""

from __future__ import annotations

import argparse
import csv
import importlib
import importlib.util
import json
import math
import platform
import random
import re
import shutil
import struct
import sys
import time
import wave
from collections import Counter, deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Deque, Dict, Iterable, List, Optional, Sequence, Set, Tuple

AudioSegment: Any = None
WhiteNoise: Any = None
compress_dynamic_range: Any = None
high_pass_filter: Any = None
low_pass_filter: Any = None


def ensure_audio_backend() -> None:
    """Load pydub lazily so non-audio flows and --help work without it."""
    global AudioSegment, WhiteNoise, compress_dynamic_range, high_pass_filter, low_pass_filter
    if AudioSegment is not None:
        return
    if "pydub" not in sys.modules and importlib.util.find_spec("pydub") is None:
        raise SystemExit(
            "Audio backend unavailable: install 'pydub' to use --mode audio/both/all. "
            "Try: python3 -m pip install -r requirements.txt"
        )
    if shutil.which("ffmpeg") is None and shutil.which("avconv") is None:
        raise SystemExit(
            "Audio backend unavailable: ffmpeg was not found on PATH. Install ffmpeg and rerun audio mode."
        )
    try:
        pydub = importlib.import_module("pydub")
        effects = importlib.import_module("pydub.effects")
        generators = importlib.import_module("pydub.generators")
    except ModuleNotFoundError as exc:
        if exc.name in {"audioop", "pyaudioop"}:
            raise SystemExit(
                "Audio backend unavailable: pydub needs 'audioop-lts' on Python 3.13+. "
                "Try: python3 -m pip install -r requirements.txt"
            ) from exc
        raise
    AudioSegment = pydub.AudioSegment
    WhiteNoise = generators.WhiteNoise
    compress_dynamic_range = effects.compress_dynamic_range
    high_pass_filter = effects.high_pass_filter
    low_pass_filter = effects.low_pass_filter

# -------------------------------------------------------------------
# CONFIG / CONSTANTS
# -------------------------------------------------------------------

AUDIO_EXTS = {".wav", ".mp3", ".flac", ".aiff", ".ogg", ".m4a"}
TOKEN_RE = re.compile(r"[A-Za-z']+")
SECTION_NAMES = ("ENTRY", "BUILD", "PRESSURE", "COLLAPSE", "AFTERIMAGE")
SECTION_PROGRESS = {
    "ENTRY": 0.1,
    "BUILD": 0.32,
    "PRESSURE": 0.56,
    "COLLAPSE": 0.76,
    "AFTERIMAGE": 0.93,
}
SECTION_ARCS = ("classic", "spoken", "breach", "pulse", "ghost")
SECTION_PROFILE_KEYS = ("dens", "frag_mul", "repeat", "reverse", "filt", "silence", "ghost")
SOURCE_SCORE_MODES = ("off", "spoken", "beat", "breach")
PLANNER_PROFILES = ("auto", "classic", "phrase", "beat", "breach")
BASELINE_PLACEMENT_MODES = ("any", "accent", "gap", "offbeat")
SPOKEN_SOURCE_KEYWORDS = ("voice", "speech", "spoken", "phrase", "dialog", "dialogue", "interview", "reading", "narration")
BEAT_SOURCE_KEYWORDS = ("beat", "loop", "drum", "kick", "snare", "hat", "bass", "perc", "pulse", "groove", "rhythm")
BREACH_SOURCE_KEYWORDS = ("noise", "static", "radio", "scan", "dropout", "glitch", "burst", "carrier", "corrupt", "warning", "collapse")
SECTION_ARC_MODIFIERS: Dict[str, Dict[str, Dict[str, float]]] = {
    "classic": {},
    "spoken": {
        "ENTRY": {"dens": 0.8, "frag_mul": 1.25, "repeat": 0.5, "reverse": 0.4, "filt": 0.55, "silence": 0.8, "ghost": 0.8},
        "BUILD": {"dens": 0.95, "frag_mul": 1.2, "repeat": 0.55, "reverse": 0.45, "filt": 0.65, "silence": 0.7, "ghost": 0.85},
        "PRESSURE": {"dens": 1.0, "frag_mul": 0.95, "repeat": 0.65, "reverse": 0.55, "filt": 0.75, "silence": 0.85, "ghost": 1.0},
        "COLLAPSE": {"dens": 0.55, "frag_mul": 0.8, "repeat": 0.75, "reverse": 0.7, "filt": 0.8, "silence": 1.1, "ghost": 1.1},
        "AFTERIMAGE": {"dens": 0.45, "frag_mul": 0.75, "repeat": 0.8, "reverse": 0.75, "filt": 0.85, "silence": 1.15, "ghost": 1.3},
    },
    "breach": {
        "ENTRY": {"dens": 0.55, "frag_mul": 1.4, "repeat": 0.75, "reverse": 0.8, "filt": 1.1, "silence": 1.35, "ghost": 0.8},
        "BUILD": {"dens": 1.05, "frag_mul": 0.9, "repeat": 1.1, "reverse": 1.0, "filt": 1.0, "silence": 0.9, "ghost": 1.0},
        "PRESSURE": {"dens": 1.35, "frag_mul": 0.7, "repeat": 1.15, "reverse": 1.15, "filt": 1.1, "silence": 0.75, "ghost": 1.1},
        "COLLAPSE": {"dens": 1.45, "frag_mul": 0.55, "repeat": 1.2, "reverse": 1.15, "filt": 1.05, "silence": 1.25, "ghost": 1.25},
        "AFTERIMAGE": {"dens": 0.55, "frag_mul": 0.8, "repeat": 1.0, "reverse": 1.1, "filt": 1.05, "silence": 1.35, "ghost": 1.3},
    },
    "pulse": {
        "ENTRY": {"dens": 0.9, "frag_mul": 1.0, "repeat": 0.9, "reverse": 0.45, "filt": 0.75, "silence": 0.75, "ghost": 0.6},
        "BUILD": {"dens": 1.15, "frag_mul": 0.9, "repeat": 1.15, "reverse": 0.55, "filt": 0.8, "silence": 0.65, "ghost": 0.75},
        "PRESSURE": {"dens": 1.35, "frag_mul": 0.75, "repeat": 1.25, "reverse": 0.7, "filt": 0.9, "silence": 0.55, "ghost": 0.85},
        "COLLAPSE": {"dens": 1.05, "frag_mul": 0.6, "repeat": 1.35, "reverse": 0.8, "filt": 1.0, "silence": 0.85, "ghost": 1.0},
        "AFTERIMAGE": {"dens": 0.7, "frag_mul": 0.65, "repeat": 1.2, "reverse": 0.8, "filt": 0.9, "silence": 0.95, "ghost": 1.1},
    },
    "ghost": {
        "ENTRY": {"dens": 0.45, "frag_mul": 1.45, "repeat": 0.6, "reverse": 0.6, "filt": 0.9, "silence": 1.35, "ghost": 1.4},
        "BUILD": {"dens": 0.65, "frag_mul": 1.2, "repeat": 0.75, "reverse": 0.7, "filt": 1.0, "silence": 1.2, "ghost": 1.5},
        "PRESSURE": {"dens": 0.85, "frag_mul": 0.9, "repeat": 0.95, "reverse": 0.85, "filt": 1.05, "silence": 1.0, "ghost": 1.55},
        "COLLAPSE": {"dens": 0.55, "frag_mul": 0.65, "repeat": 1.05, "reverse": 1.05, "filt": 1.05, "silence": 1.45, "ghost": 1.65},
        "AFTERIMAGE": {"dens": 0.35, "frag_mul": 0.75, "repeat": 1.1, "reverse": 1.15, "filt": 1.1, "silence": 1.5, "ghost": 1.8},
    },
}
SLICE_GRID_FACTORS: Dict[str, float] = {
    "off": 0.0,
    "1/4": 1.0,
    "1/8": 0.5,
    "1/16": 0.25,
    "1/32": 0.125,
    "1/8t": 1.0 / 3.0,
    "1/16t": 1.0 / 6.0,
}
PHRASE_LENGTH_RANGES: Dict[str, Tuple[float, float]] = {
    "auto": (0.0, 0.0),
    "micro": (0.04, 0.45),
    "short": (0.20, 1.35),
    "medium": (0.65, 3.40),
    "long": (1.60, 6.80),
}
SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_TOP300_CSV = SCRIPT_DIR / "transmissions_top300_sample_candidates.csv"
DEFAULT_FULL_CSV = SCRIPT_DIR / "transmissions_full_subtitles.csv"

TEXT_COLUMN_CANDIDATES = ["text", "subtitle", "line", "transcript", "content"]
FILE_COLUMN_CANDIDATES = ["file", "filename", "source_file"]
MANIFEST_FILE_COLUMN_CANDIDATES = ["file", "filename", "source_file", "path", "source", "audio"]
MANIFEST_ROLE_COLUMN_CANDIDATES = ["role", "type", "kind", "category", "source_type"]
MANIFEST_TAG_COLUMN_CANDIDATES = ["tags", "tag", "labels", "label", "keywords"]
MANIFEST_WEIGHT_COLUMN_CANDIDATES = ["weight", "source_weight", "priority"]
MANIFEST_LOOP_COLUMN_CANDIDATES = ["loop_hint", "loop", "loop_score", "rhythm"]
MANIFEST_WORD_COLUMN_CANDIDATES = ["words", "word_count", "phrase_words"]
CLIP_ID_COLUMN_CANDIDATES = ["clip_id", "clip", "id"]
CUE_COLUMN_CANDIDATES = ["cue_index", "cue", "index"]
START_TC_COLUMN_CANDIDATES = ["start_tc", "start", "in", "time_in"]
END_TC_COLUMN_CANDIDATES = ["end_tc", "end", "out", "time_out"]
DURATION_COLUMN_CANDIDATES = ["duration_sec", "duration", "dur"]
SCORE_COLUMN_CANDIDATES = ["score", "rank_score", "weight"]
LOOP_BIN_COLUMN_CANDIDATES = ["loop_bin", "loop", "size_bin"]
INTENSITY_COLUMN_CANDIDATES = ["intensity", "level", "energy"]

LIVE_CONTROL_LIMITS: Dict[str, Tuple[float, float]] = {
    "absurd_seriousness": (0.0, 1.0),
    "text_chaos": (0.0, 1.5),
    "rupture_prob": (0.0, 1.0),
    "stutter_prob": (0.0, 1.0),
    "recurrence_prob": (0.0, 0.95),
    "ghost_prob": (0.0, 0.95),
    "silence_prob": (0.0, 0.95),
    "burst_rate": (0.0, 1.0),
    "dropout_rate": (0.0, 1.0),
    "reverse_shard_rate": (0.0, 1.0),
    "stutter_rate": (0.0, 1.0),
    "mute_rate": (0.0, 1.0),
    "repeat_rate": (0.0, 1.0),
    "beat_dropout_rate": (0.0, 1.0),
    "source_diversity": (0.0, 1.0),
}
BEAT_RATE_KEYS = ("stutter_rate", "mute_rate", "repeat_rate", "beat_dropout_rate")
OPTIONAL_ANALYSIS_MODULES = (("librosa", "librosa"), ("scikit-learn", "sklearn"))
ANALYSIS_CACHE_VERSION = 8
AUDIO_PLAN_VERSION = 3
ANALYSIS_CACHE_REQUIRED_SAMPLE_KEYS = ("zero_crossing_rate", "grid_cell_summary", "similarity_vector")
ANALYSIS_GRID_CELL_MAX_CELLS = 512
BASELINE_GRID_SUMMARY_MAX_CELLS = 128
ANALYSIS_SIMILARITY_VECTOR_FIELDS = (
    "duration",
    "loudness",
    "zero_crossing",
    "grid_loudness_mean",
    "grid_loudness_variation",
    "grid_zcr_mean",
    "grid_zcr_variation",
)
DATASET_MANIFEST_FIELDS = [
    "file", "role", "tags", "intensity", "loop_hint", "words", "weight",
    "duration_ms", "duration_sec", "dbfs", "zero_crossing_rate",
    "recommended_preset", "recommended_flags", "notes",
]
QA_SOURCE_SPECS: Dict[str, Tuple[Tuple[str, float, str], ...]] = {
    "loops": (
        ("drum_pulse_120.wav", 8.0, "loop_drums"),
        ("bass_gate_120.wav", 8.0, "loop_bass"),
        ("metal_tick_120.wav", 8.0, "loop_metal"),
        ("noise_hat_120.wav", 8.0, "loop_noise_hat"),
    ),
    "voice": (
        ("voice_phrase_a.wav", 6.0, "voice_a"),
        ("voice_phrase_b.wav", 6.0, "voice_b"),
        ("voice_gap_phrase.wav", 6.0, "voice_gap"),
    ),
    "signal": (
        ("radio_noise_bursts.wav", 6.0, "signal_bursts"),
        ("dropout_carrier.wav", 6.0, "signal_dropouts"),
        ("scanline_hash.wav", 6.0, "signal_scanline"),
    ),
}
QA_SRT_CUES: Tuple[Tuple[int, int, int, str], ...] = (
    (1, 400, 1600, "signal check one begins in fragments"),
    (2, 2050, 3350, "the voice holds through the interruption"),
    (3, 3900, 5400, "repeat the message until the carrier fails"),
)
QA_CSV_CUES: Tuple[Tuple[str, int, int, str], ...] = (
    ("voice_phrase_a.wav", 400, 1600, "signal check one begins in fragments"),
    ("voice_phrase_a.wav", 2050, 3350, "the voice holds through the interruption"),
    ("voice_phrase_b.wav", 600, 1950, "another speaker enters the transmission"),
    ("voice_gap_phrase.wav", 300, 1500, "silence opens inside the phrase"),
    ("voice_gap_phrase.wav", 3100, 4950, "the last words return as broken memory"),
)

PRESET_VALUES: Dict[str, Dict[str, object]] = {
    "signal-breach": {
        "description": "Glitchy corrupted-transmission interruptions, noise bed, hard cuts, and dropouts.",
        "values": {
            "density": "dense",
            "sectional": True,
            "section_arc": "breach",
            "planner_profile": "breach",
            "source_score": "breach",
            "concrete": True,
            "bed_noise": True,
            "arrangement_style": "collapse",
            "memory_depth": 18,
            "silence_prob": 0.42,
            "recurrence_prob": 0.48,
            "ghost_prob": 0.58,
            "rupture_prob": 0.75,
            "stutter_prob": 0.62,
            "text_chaos": 1.0,
            "absurd_seriousness": 0.78,
            "min_frag": 0.025,
            "max_frag": 0.75,
            "burst_rate": 0.58,
            "dropout_rate": 0.64,
            "reverse_shard_rate": 0.46,
            "filter_severity": "hard",
            "source_diversity": 0.22,
        },
    },
    "spoken-word-cutup": {
        "description": "Voice-first cutups with longer fragments and lower destruction for intelligibility.",
        "values": {
            "density": "medium",
            "sectional": True,
            "section_arc": "spoken",
            "planner_profile": "phrase",
            "source_score": "spoken",
            "concrete": False,
            "arrangement_style": "sequential",
            "memory_depth": 12,
            "silence_prob": 0.18,
            "recurrence_prob": 0.32,
            "ghost_prob": 0.18,
            "rupture_prob": 0.28,
            "stutter_prob": 0.2,
            "text_chaos": 0.55,
            "absurd_seriousness": 0.62,
            "min_frag": 0.45,
            "max_frag": 3.6,
            "phrase_length": "medium",
            "intelligibility": "high",
            "interruption_density": "low",
            "silence_insert_ms": "120:420",
            "max_words_slogan": 14,
            "source_diversity": 0.65,
        },
    },
    "beat-cutup": {
        "description": "Short rhythmic slices, repeats, mutes, and memory-driven beat disruptions.",
        "values": {
            "density": "dense",
            "sectional": False,
            "section_arc": "pulse",
            "planner_profile": "beat",
            "source_score": "beat",
            "concrete": True,
            "arrangement_style": "swarm",
            "memory_depth": 16,
            "silence_prob": 0.24,
            "recurrence_prob": 0.55,
            "ghost_prob": 0.2,
            "rupture_prob": 0.5,
            "stutter_prob": 0.75,
            "min_frag": 0.08,
            "max_frag": 0.65,
            "slice_grid": "1/16",
            "stutter_rate": 0.48,
            "mute_rate": 0.18,
            "repeat_rate": 0.38,
            "beat_dropout_rate": 0.16,
            "source_diversity": 0.35,
        },
    },
    "radio-intrusion": {
        "description": "Filtered voice intrusions with hiss, ghosts, and unstable broadcast texture.",
        "values": {
            "density": "medium",
            "sectional": True,
            "section_arc": "ghost",
            "planner_profile": "breach",
            "source_score": "breach",
            "concrete": True,
            "bed_noise": True,
            "arrangement_style": "swarm",
            "memory_depth": 20,
            "silence_prob": 0.33,
            "recurrence_prob": 0.46,
            "ghost_prob": 0.68,
            "rupture_prob": 0.55,
            "stutter_prob": 0.38,
            "min_frag": 0.06,
            "max_frag": 1.2,
            "master_gain": -4.0,
            "burst_rate": 0.24,
            "dropout_rate": 0.28,
            "filter_severity": "hard",
            "source_diversity": 0.45,
        },
    },
    "hard-stutter": {
        "description": "Aggressive micro-fragment repetition, abrupt mutes, and collapse-forward pressure.",
        "values": {
            "density": "dense",
            "sectional": True,
            "section_arc": "pulse",
            "planner_profile": "beat",
            "source_score": "beat",
            "concrete": True,
            "arrangement_style": "collapse",
            "memory_depth": 10,
            "silence_prob": 0.3,
            "recurrence_prob": 0.62,
            "ghost_prob": 0.32,
            "rupture_prob": 0.85,
            "stutter_prob": 0.9,
            "min_frag": 0.025,
            "max_frag": 0.4,
            "dropout_rate": 0.42,
            "reverse_shard_rate": 0.24,
            "stutter_rate": 0.72,
            "mute_rate": 0.26,
            "repeat_rate": 0.52,
            "beat_dropout_rate": 0.24,
            "source_diversity": 0.25,
        },
    },
    "ghost-transmission": {
        "description": "Faint recurring voices, afterimages, dead air, and memory echoes.",
        "values": {
            "density": "medium",
            "sectional": True,
            "section_arc": "ghost",
            "planner_profile": "phrase",
            "source_score": "spoken",
            "concrete": False,
            "bed_noise": True,
            "arrangement_style": "collapse",
            "memory_depth": 28,
            "silence_prob": 0.38,
            "recurrence_prob": 0.74,
            "ghost_prob": 0.82,
            "rupture_prob": 0.34,
            "stutter_prob": 0.34,
            "min_frag": 0.25,
            "max_frag": 2.8,
            "master_gain": -5.5,
            "source_diversity": 0.18,
        },
    },
}

RECIPE_COMMANDS: Dict[str, Tuple[str, str]] = {
    "qa-sources": (
        "Create starter local WAV and cue sources outside the repo.",
        "cutups --init-qa-sources ../cutups_qa_sources",
    ),
    "scan-dataset": (
        "Inspect a local audio folder and write a starter source manifest plus JSON report.",
        "cutups \\\n"
        "  --scan-dataset ../cutups_qa_sources \\\n"
        "  --write-source-manifest ../cutups_qa_sources/source_manifest.csv \\\n"
        "  --write-dataset-report ../cutups_qa_sources/dataset_report.json \\\n"
        "  --overwrite",
    ),
    "signal-breach": (
        "Glitchy interruptions, static bursts, dropouts, reverse shards, and hard transmission filtering.",
        "cutups \\\n"
        "  --mode audio \\\n"
        "  --preset signal-breach \\\n"
        "  --input ../cutups_qa_sources/signal \\\n"
        "  --burst-rate 0.75 \\\n"
        "  --dropout-rate 0.65 \\\n"
        "  --reverse-shard-rate 0.50 \\\n"
        "  --filter-severity hard \\\n"
        "  --output out/signal_breach \\\n"
        "  --duration 30 \\\n"
        "  --preview-duration 10 \\\n"
        "  --seed 701",
    ),
    "spoken-word-cutup": (
        "Voice-first phrase cutups with higher intelligibility and editorial silence.",
        "cutups \\\n"
        "  --mode audio \\\n"
        "  --preset spoken-word-cutup \\\n"
        "  --input ../cutups_qa_sources/voice \\\n"
        "  --phrase-length medium \\\n"
        "  --intelligibility high \\\n"
        "  --interruption-density low \\\n"
        "  --silence-insert-ms 120:420 \\\n"
        "  --output out/spoken_word \\\n"
        "  --duration 45 \\\n"
        "  --preview-duration 12 \\\n"
        "  --seed 311",
    ),
    "spoken-word-cues": (
        "Phrase-boundary spoken-word render using generated SRT cues.",
        "cutups \\\n"
        "  --mode audio \\\n"
        "  --preset spoken-word-cutup \\\n"
        "  --input ../cutups_qa_sources/voice/voice_phrase_a.wav \\\n"
        "  --cue-file ../cutups_qa_sources/voice/voice_phrase_a.srt \\\n"
        "  --cue-slice-mode full \\\n"
        "  --output out/spoken_cued \\\n"
        "  --duration 30 \\\n"
        "  --preview-duration 10 \\\n"
        "  --seed 313",
    ),
    "beat-cutup": (
        "Grid-sliced loop cutup with stutters, repeats, mutes, and beat dropouts.",
        "cutups \\\n"
        "  --mode audio \\\n"
        "  --preset beat-cutup \\\n"
        "  --input ../cutups_qa_sources/loops \\\n"
        "  --bpm 120 \\\n"
        "  --slice-grid 1/16 \\\n"
        "  --stutter-rate 0.55 \\\n"
        "  --repeat-rate 0.45 \\\n"
        "  --mute-rate 0.20 \\\n"
        "  --beat-dropout-rate 0.15 \\\n"
        "  --output out/beat_cutup \\\n"
        "  --duration 32 \\\n"
        "  --preview-duration 12 \\\n"
        "  --seed 230",
    ),
    "beat-baseline": (
        "Use one beat loop as the timing bed while cutting voice, noise, or mixed sources against it.",
        "cutups \\\n"
        "  --mode audio \\\n"
        "  --preset beat-cutup \\\n"
        "  --input ../cutups_qa_sources/voice \\\n"
        "  --baseline-beat ../cutups_qa_sources/loops/drum_pulse_120.wav \\\n"
        "  --baseline-beat-bars 4 \\\n"
        "  --baseline-beat-gain -10 \\\n"
        "  --baseline-beat-duck-db 4 \\\n"
        "  --baseline-placement gap \\\n"
        "  --slice-grid 1/16 \\\n"
        "  --stutter-rate 0.45 \\\n"
        "  --repeat-rate 0.30 \\\n"
        "  --mute-rate 0.12 \\\n"
        "  --output out/beat_baseline \\\n"
        "  --duration 32 \\\n"
        "  --preview-duration 12 \\\n"
        "  --seed 233",
    ),
    "beat-similarity": (
        "Beat-grid render with source analysis, similarity jumps, and novelty bias.",
        "cutups \\\n"
        "  --mode audio \\\n"
        "  --preset beat-cutup \\\n"
        "  --input ../cutups_qa_sources/loops \\\n"
        "  --bpm 120 \\\n"
        "  --slice-grid 1/16 \\\n"
        "  --beat-jump-mode similarity \\\n"
        "  --beat-similarity-weight 1.0 \\\n"
        "  --beat-novelty 0.35 \\\n"
        "  --analysis-cache auto \\\n"
        "  --output out/beat_similarity \\\n"
        "  --duration 32 \\\n"
        "  --preview-duration 12 \\\n"
        "  --seed 230",
    ),
    "radio-intrusion": (
        "Filtered voice intrusions with hiss, ghosts, and unstable broadcast texture.",
        "cutups \\\n"
        "  --mode audio \\\n"
        "  --preset radio-intrusion \\\n"
        "  --input ../cutups_qa_sources/voice \\\n"
        "  --output out/radio_intrusion \\\n"
        "  --duration 45 \\\n"
        "  --preview-duration 12 \\\n"
        "  --seed 31",
    ),
    "hard-stutter": (
        "Aggressive micro-fragment repetition and abrupt grid mutes.",
        "cutups \\\n"
        "  --mode audio \\\n"
        "  --preset hard-stutter \\\n"
        "  --input ../cutups_qa_sources/loops \\\n"
        "  --bpm 120 \\\n"
        "  --slice-grid 1/32 \\\n"
        "  --stutter-rate 0.85 \\\n"
        "  --repeat-rate 0.65 \\\n"
        "  --mute-rate 0.30 \\\n"
        "  --output out/hard_stutter \\\n"
        "  --duration 24 \\\n"
        "  --preview-duration 10 \\\n"
        "  --seed 241",
    ),
    "ghost-transmission": (
        "Faint recurring voices, afterimages, dead air, and memory echoes.",
        "cutups \\\n"
        "  --mode audio \\\n"
        "  --preset ghost-transmission \\\n"
        "  --input ../cutups_qa_sources/voice \\\n"
        "  --output out/ghost_transmission \\\n"
        "  --duration 60 \\\n"
        "  --preview-duration 12 \\\n"
        "  --seed 53",
    ),
}

KEYWORD_WEIGHTS: Dict[str, float] = {
    "official": 1.3,
    "authority": 1.4,
    "federal": 1.1,
    "freedom": 1.2,
    "speech": 1.1,
    "command": 1.4,
    "must": 1.2,
    "warning": 1.3,
    "threat": 1.4,
    "collapse": 1.3,
    "silence": 1.0,
    "license": 1.1,
}

AGITPROP_MODE_PROFILES: Dict[str, Dict[str, float]] = {
    "POSTER": {"stack": 0.55, "escalation": 0.5, "contradiction": 0.25, "decree": 0.4, "chant": 0.55},
    "DECREE": {"stack": 0.75, "escalation": 0.68, "contradiction": 0.38, "decree": 0.9, "chant": 0.3},
    "COLLAPSE": {"stack": 0.38, "escalation": 0.8, "contradiction": 0.66, "decree": 0.3, "chant": 0.64},
    "PRESS BRIEFING FROM HELL": {"stack": 0.62, "escalation": 0.8, "contradiction": 0.58, "decree": 0.66, "chant": 0.44},
    "ADMINISTRATIVE CHANT": {"stack": 0.7, "escalation": 0.58, "contradiction": 0.3, "decree": 0.56, "chant": 0.9},
    "FALSE PATRIOTIC": {"stack": 0.52, "escalation": 0.74, "contradiction": 0.49, "decree": 0.52, "chant": 0.6},
    "GHOST BUREAU": {"stack": 0.67, "escalation": 0.63, "contradiction": 0.72, "decree": 0.5, "chant": 0.48},
    "PUBLIC INTEREST FEVER": {"stack": 0.78, "escalation": 0.82, "contradiction": 0.47, "decree": 0.64, "chant": 0.74},
}

OFFICIAL_NOUNS = [
    "PUBLIC", "INTEREST", "PROTOCOL", "AUTHORIZATION", "COMPLIANCE", "DIRECTIVE", "MANDATE",
    "ACCOUNTABILITY", "COMMITTEE", "CLARIFICATION", "LICENSING", "EMERGENCY", "PATRIOTISM", "MANAGEMENT",
]
PROCEDURAL_FILLERS = ["UNDER", "PURSUANT TO", "IN ACCORDANCE WITH", "SUBJECT TO", "PENDING", "WITHOUT PREJUDICE TO"]
BANAL_CONNECTORS = ["and also", "for now", "as needed", "until further feeling", "in this weather", "for administrative calm"]

# -------------------------------------------------------------------
# DATA MODELS
# -------------------------------------------------------------------


@dataclass
class SampleFile:
    path: Path
    duration_ms: int
    words: int
    intensity_hint: int
    loop_hint: int
    cue_start_ms: int = 0
    cue_end_ms: int = 0
    cue_text: str = ""
    cue_index: int = 0
    manifest_tags: str = ""
    manifest_role: str = ""
    manifest_weight: float = 1.0

    def has_cue(self) -> bool:
        return self.cue_end_ms > self.cue_start_ms


@dataclass(frozen=True)
class BaselineBeat:
    path: Path
    audio: Any
    source_duration_ms: int
    gain_db: float
    inferred_bpm: float = 0.0


@dataclass
class BeatJumpState:
    active: bool = False
    neighbor_keys: Dict[str, List[str]] = field(default_factory=dict)
    samples_by_key: Dict[str, SampleFile] = field(default_factory=dict)
    selections: int = 0
    fallbacks: int = 0


@dataclass
class Event:
    layer: str
    section: str
    source: str
    source_basename: str
    source_duration_ms: int
    source_cue_start_ms: int
    source_cue_end_ms: int
    source_cue_text: str
    source_manifest_tags: str
    source_manifest_role: str
    source_manifest_weight: float
    start_ms: int
    end_ms: int
    fragment_duration_ms: int
    gain_db: float
    reversed: bool
    speed: float
    repeated: int
    hp_hz: int
    lp_hz: int
    grain_mode: bool
    from_memory: bool
    transformation: str
    layer_role: str
    recurrence_index: int
    selection_reason: str = "unknown"
    source_score_mode: str = "off"
    source_base_weight: float = 0.0
    source_material_score: float = 1.0
    source_diversity_multiplier: float = 1.0
    source_final_weight: float = 0.0
    source_use_count_before: int = 0
    source_recent_hits_before: int = 0
    source_immediate_repeat: bool = False
    section_density_target: float = 0.0
    section_fragment_multiplier: float = 0.0
    section_repeat_probability: float = 0.0
    section_ghost_probability: float = 0.0
    baseline_placement_mode: str = "any"
    baseline_placement_original_start_ms: int = 0
    baseline_placement_cell_index: int = -1
    baseline_placement_cell_energy: float = 0.0
    planner_profile: str = "classic"
    planner_intent: str = ""
    phrase_protected: bool = False
    beat_grid_ms: int = 0
    beat_grid_cell_index: int = -1
    beat_grid_offset_ms: int = 0


EVENT_CSV_FIELDS = [
    "layer", "section", "source", "source_basename", "source_duration_ms",
    "source_cue_start_ms", "source_cue_end_ms", "source_cue_text",
    "source_manifest_tags", "source_manifest_role", "source_manifest_weight",
    "start_ms", "end_ms", "fragment_duration_ms", "gain_db",
    "reversed", "speed", "repeated", "hp_hz", "lp_hz",
    "grain_mode", "from_memory", "transformation", "layer_role",
    "recurrence_index", "selection_reason", "source_score_mode",
    "source_base_weight", "source_material_score",
    "source_diversity_multiplier", "source_final_weight",
    "source_use_count_before", "source_recent_hits_before",
    "source_immediate_repeat", "section_density_target",
    "section_fragment_multiplier", "section_repeat_probability",
    "section_ghost_probability", "baseline_placement_mode",
    "baseline_placement_original_start_ms", "baseline_placement_cell_index",
    "baseline_placement_cell_energy", "planner_profile", "planner_intent",
    "phrase_protected", "beat_grid_ms", "beat_grid_cell_index",
    "beat_grid_offset_ms",
]


@dataclass
class Line:
    text: str
    file: str = ""
    clip_id: str = ""
    cue_index: int = 0
    start_tc: str = ""
    end_tc: str = ""
    duration_sec: float = 0.0
    source_bank: str = ""
    score: float = 0.0
    loop_bin: str = ""
    intensity: str = ""
    word_count: int = 0
    tags: List[str] = field(default_factory=list)


@dataclass
class SourceRow:
    text: str
    file: str
    clip_id: str
    cue_index: str
    start_tc: str
    end_tc: str
    duration_sec: str
    source_bank: str


@dataclass
class CSVLoadStats:
    loaded: int = 0
    skipped_empty: int = 0
    skipped_unusable: int = 0


@dataclass
class RunSummary:
    top300_loaded: int = 0
    top300_skipped: int = 0
    full_loaded: int = 0
    full_skipped: int = 0
    slogans: int = 0
    broadcasts: int = 0
    chants: int = 0
    cut_matches: int = 0
    audio_events: int = 0
    beat_similarity_jumps: int = 0
    beat_similarity_fallbacks: int = 0
    section_distribution: Counter = field(default_factory=Counter)
    recurring_sources: Counter = field(default_factory=Counter)
    output_paths: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class RuntimeParams:
    absurd_seriousness: float
    text_chaos: float
    rupture_prob: float
    stutter_prob: float
    recurrence_prob: float
    ghost_prob: float
    silence_prob: float
    burst_rate: float = 0.0
    dropout_rate: float = 0.0
    reverse_shard_rate: float = 0.0
    filter_severity: str = ""
    stutter_rate: float = 0.0
    mute_rate: float = 0.0
    repeat_rate: float = 0.0
    beat_dropout_rate: float = 0.0
    source_diversity: float = 0.0
    section_arc: str = ""
    source_score: str = ""
    baseline_placement: str = ""
    force_section: str = ""
    hold_section: bool = False
    burst_now: bool = False
    panic_silence: bool = False


@dataclass
class LiveControlState:
    enabled: bool = False
    control_file: Optional[Path] = None
    poll_ms: int = 250
    telemetry_path: Optional[Path] = None
    last_poll_ms: int = 0
    last_mtime_ns: int = -1
    overrides: Dict[str, float] = field(default_factory=dict)
    section_override: str = ""
    filter_severity_override: str = ""
    section_arc_override: str = ""
    source_score_override: str = ""
    baseline_placement_override: str = ""
    hold_section: bool = False
    burst_now: bool = False
    panic_silence: bool = False

    def poll(self) -> None:
        if not self.enabled or not self.control_file:
            return
        now_ms = int(time.time() * 1000)
        if now_ms - self.last_poll_ms < self.poll_ms:
            return
        self.last_poll_ms = now_ms
        try:
            stat = self.control_file.stat()
        except OSError:
            return
        if stat.st_mtime_ns == self.last_mtime_ns:
            return
        self.last_mtime_ns = stat.st_mtime_ns
        try:
            payload = json.loads(self.control_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return
        if not isinstance(payload, dict):
            return

        payload_version = payload.get("version", 1)
        if payload_version not in {1, 2}:
            return
        controls = payload.get("controls", payload)
        if not isinstance(controls, dict):
            return

        for key, (low, high) in LIVE_CONTROL_LIMITS.items():
            val = controls.get(key)
            if isinstance(val, (int, float)):
                self.overrides[key] = clamp(float(val), low, high)

        sec = str(controls.get("force_section", "")).strip().upper()
        self.section_override = sec if sec in SECTION_NAMES else ""
        if "filter_severity" in controls:
            filter_severity = str(controls.get("filter_severity", "")).strip().lower()
            if filter_severity in {"", "auto"}:
                self.filter_severity_override = ""
            elif filter_severity in {"light", "medium", "hard"}:
                self.filter_severity_override = filter_severity
        if "section_arc" in controls:
            section_arc = str(controls.get("section_arc", "")).strip().lower()
            self.section_arc_override = section_arc if section_arc in SECTION_ARCS else ""
        if "source_score" in controls:
            source_score = str(controls.get("source_score", "")).strip().lower()
            self.source_score_override = source_score if source_score in SOURCE_SCORE_MODES else ""
        if "baseline_placement" in controls:
            baseline_placement = str(controls.get("baseline_placement", "")).strip().lower()
            self.baseline_placement_override = baseline_placement if baseline_placement in BASELINE_PLACEMENT_MODES else ""
        self.hold_section = bool(controls.get("hold_section", False))
        self.burst_now = bool(controls.get("burst_now", False))
        self.panic_silence = bool(controls.get("panic_silence", False))

    def value(self, args: argparse.Namespace, key: str) -> float:
        self.poll()
        base = getattr(args, key)
        return float(self.overrides.get(key, base))

    def telemetry(self, where: str, **fields: object) -> None:
        if not self.enabled or not self.telemetry_path:
            return
        row = {
            "ts_ms": int(time.time() * 1000),
            "where": where,
            "overrides": self.overrides,
        }
        if self.filter_severity_override:
            row["filter_severity"] = self.filter_severity_override
        if self.section_arc_override:
            row["section_arc"] = self.section_arc_override
        if self.source_score_override:
            row["source_score"] = self.source_score_override
        if self.baseline_placement_override:
            row["baseline_placement"] = self.baseline_placement_override
        row.update(fields)
        try:
            with self.telemetry_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
        except OSError:
            pass


def format_eta(seconds: Optional[float]) -> str:
    if seconds is None or not math.isfinite(float(seconds)) or seconds < 0:
        return "--:--"
    total = int(round(float(seconds)))
    minutes, secs = divmod(total, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours:d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


def progress_child_span(span: Tuple[float, float], start: float, end: float) -> Tuple[float, float]:
    a, b = span
    width = max(0.0, b - a)
    return a + width * clamp(float(start), 0.0, 1.0), a + width * clamp(float(end), 0.0, 1.0)


def progress_spans(mode: str) -> Dict[str, Tuple[float, float]]:
    if mode == "audio":
        return {"audio": (0.0, 1.0)}
    if mode == "agitprop":
        return {"agitprop": (0.0, 1.0)}
    if mode == "cuttargets":
        return {"cuttargets": (0.0, 1.0)}
    if mode == "both":
        return {"agitprop": (0.0, 0.18), "audio": (0.18, 1.0)}
    if mode == "all":
        return {"agitprop": (0.0, 0.12), "cuttargets": (0.12, 0.22), "audio": (0.22, 1.0)}
    return {}


@dataclass
class ProgressReporter:
    enabled: bool = False
    live: Optional[LiveControlState] = None
    start_time: float = field(default_factory=time.time)
    last_emit: float = 0.0
    last_line_len: int = 0
    min_interval: float = 0.25

    def update(self, progress: float, stage: str, detail: str = "", force: bool = False) -> None:
        now = time.time()
        progress = clamp(float(progress), 0.0, 1.0)
        if not force and now - self.last_emit < self.min_interval and progress < 1.0:
            return
        self.last_emit = now
        elapsed = max(0.0, now - self.start_time)
        eta = (elapsed / progress) * (1.0 - progress) if progress > 0 else None
        percent = progress * 100.0
        if self.live and self.live.enabled:
            self.live.telemetry(
                "progress",
                progress=round(progress, 6),
                percent=round(percent, 2),
                stage=stage,
                detail=detail,
                elapsed_sec=round(elapsed, 2),
                eta_sec=round(float(eta), 2) if eta is not None else None,
            )
        if not self.enabled:
            return
        width = 28
        filled = int(round(width * progress))
        bar = "#" * filled + "-" * (width - filled)
        message = f"\r[{bar}] {percent:6.2f}% {stage}"
        if detail:
            message += f" - {detail}"
        message += f" ETA {format_eta(eta)}"
        pad = max(0, self.last_line_len - len(message))
        sys.stderr.write(message + (" " * pad))
        sys.stderr.flush()
        self.last_line_len = len(message)

    def update_span(self, span: Tuple[float, float], fraction: float, stage: str, detail: str = "", force: bool = False) -> None:
        a, b = span
        self.update(a + (b - a) * clamp(float(fraction), 0.0, 1.0), stage, detail, force=force)

    def finish(self) -> None:
        self.update(1.0, "complete", "done", force=True)
        if self.enabled:
            sys.stderr.write("\n")
            sys.stderr.flush()


def build_progress_reporter(args: argparse.Namespace, live: Optional[LiveControlState]) -> ProgressReporter:
    enabled = bool(not getattr(args, "no_progress", False) and sys.stderr.isatty())
    return ProgressReporter(enabled=enabled, live=live)


# -------------------------------------------------------------------
# CLI
# -------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="TRANSMISSIONS cut-up instrument for speech, beats, signal breaches, and agitprop text.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--mode", choices=["audio", "agitprop", "cuttargets", "both", "all"], default="audio")
    p.add_argument("--output", default="transmissions_cutups", help="Output root folder.")
    p.add_argument("--seed", type=int, default=7, help="Deterministic random seed.")
    p.add_argument("--preset", choices=sorted(PRESET_VALUES), default="", help="Named TRANSMISSIONS recipe.")
    p.add_argument("--list-presets", action="store_true", help="Print available TRANSMISSIONS presets and exit.")
    p.add_argument("--show-recipe", choices=["all", *sorted(RECIPE_COMMANDS)], default="", help="Print a copy-ready TRANSMISSIONS command recipe and exit.")
    p.add_argument("--doctor", action="store_true", help="Check local Python/audio dependencies and bundled data, then exit.")
    p.add_argument("--init-qa-sources", default="", help="Write synthetic local QA WAV/cue sources under this folder, then exit.")
    p.add_argument("--scan-dataset", default="", help="Scan an audio file/folder, print role/preset recommendations, and exit.")
    p.add_argument("--write-source-manifest", default="", help="With --scan-dataset, write a starter source manifest CSV to this path.")
    p.add_argument("--write-dataset-report", default="", help="With --scan-dataset, write a JSON dataset report to this path.")
    p.add_argument("--dataset-max-files", type=int, default=0, help="With --scan-dataset, limit decoded audio files; 0 scans all candidates.")
    p.add_argument("--dry-run", action="store_true", help="Print resolved inputs/configuration without rendering outputs.")
    p.add_argument("--overwrite", action="store_true", help="Allow writing into a non-empty output folder.")
    p.add_argument("--no-progress", action="store_true", help="Disable terminal progress bar output.")
    p.add_argument("--preview-duration", type=float, default=0.0, help="Also export a short preview WAV from the start of each audio master; 0 disables.")
    p.add_argument("--semi-live", action="store_true", help="Render audio variants as short chunks and update a cumulative playable WAV track after each chunk.")
    p.add_argument("--semi-live-chunk-sec", type=float, default=8.0, help="Chunk length in seconds for --semi-live audio rendering.")
    p.add_argument("--semi-live-track", default="", help="Optional output path for the updating semi-live WAV track. Defaults inside each variant folder.")
    p.add_argument("--analysis-cache", default="", help="Write/update a lightweight JSON source analysis cache for audio mode. Use 'auto' for output/audio_analysis_cache.json.")
    p.add_argument("--analysis-cache-readonly", action="store_true", help="Use an existing analysis cache for source discovery/planning without updating it during render.")

    p.add_argument("--input", help="Audio sample file or folder (required for audio/both/all).")
    p.add_argument("--cue-file", default="", help="Optional SRT or CSV cue file for phrase-aware audio slicing.")
    p.add_argument("--cue-slice-mode", choices=["full", "fragment"], default="full", help="Use full cue spans or random fragments inside each cue when --cue-file is set.")
    p.add_argument("--source-manifest", default="", help="Optional CSV/JSON file labeling audio sources with tags, role/type, intensity, loop hints, words, or weight.")
    p.add_argument("--duration", type=float, default=90.0, help="Composition duration in seconds.")
    p.add_argument("--variants", type=int, default=1, help="Number of rendered variants.")
    p.add_argument("--sample-rate", type=int, default=44100, help="Export sample rate.")
    p.add_argument("--master-gain", type=float, default=-3.0, help="Master gain in dB.")
    p.add_argument("--bed-noise", action=argparse.BooleanOptionalAction, default=False, help="Add synthetic hiss bed.")
    p.add_argument("--baseline-beat", default="", help="Optional beat/loop audio file to use as a continuous timing bed; it is not added to the cutup source pool.")
    p.add_argument("--baseline-beat-gain", type=float, default=-9.0, help="Gain in dB applied to the baseline beat bed before mixing.")
    p.add_argument("--baseline-beat-bars", type=float, default=0.0, help="If >0 and --bpm is unset, infer BPM from the baseline beat length as this many 4/4 bars.")
    p.add_argument("--baseline-beat-duck-db", type=float, default=0.0, help="Positive dB attenuation applied to the baseline beat around cutup events; 0 disables.")
    p.add_argument("--baseline-beat-duck-ms", type=int, default=80, help="Padding in milliseconds around cutup events for baseline beat ducking.")
    p.add_argument("--baseline-placement", choices=BASELINE_PLACEMENT_MODES, default="any", help="Bias grid placement against the baseline beat: any keeps current behavior, accent favors loud cells, gap favors quiet cells, offbeat favors quiet cells next to accents.")
    p.add_argument("--min-frag", type=float, default=0.05, help="Minimum fragment size in seconds.")
    p.add_argument("--max-frag", type=float, default=4.2, help="Maximum fragment size in seconds.")
    p.add_argument("--phrase-length", choices=sorted(PHRASE_LENGTH_RANGES), default="auto", help="Voice-oriented fragment length profile.")
    p.add_argument("--intelligibility", choices=["auto", "high", "medium", "low"], default="auto", help="Voice clarity bias for speed, reverse, grain, and filtering.")
    p.add_argument("--interruption-density", choices=["auto", "low", "medium", "high"], default="auto", help="How often spoken fragments get cut, gapped, or disrupted.")
    p.add_argument("--silence-insert-ms", default="", help="Optional silence insertion range as min:max milliseconds.")
    p.add_argument("--burst-rate", type=float, default=0.0, help="Probability of inserting static/noise bursts into shaped fragments.")
    p.add_argument("--dropout-rate", type=float, default=0.0, help="Probability of hard signal dropouts inside shaped fragments.")
    p.add_argument("--reverse-shard-rate", type=float, default=0.0, help="Probability of reversing tiny shards inside shaped fragments.")
    p.add_argument("--filter-severity", choices=["auto", "light", "medium", "hard"], default="auto", help="Transmission filter severity for shaped fragments.")
    p.add_argument("--density", choices=["sparse", "medium", "dense"], default="medium")
    p.add_argument("--concrete", action=argparse.BooleanOptionalAction, default=False, help="Bias toward harsher concrete transformations.")
    p.add_argument("--sectional", action=argparse.BooleanOptionalAction, default=False, help="Enable section-aware timeline behavior.")
    p.add_argument("--section-arc", choices=SECTION_ARCS, default="classic", help="Named section energy curve used when --sectional is active.")
    p.add_argument("--planner-profile", choices=PLANNER_PROFILES, default="auto", help="High-level audio planner bias. Auto follows preset/source-score.")
    p.add_argument("--arrangement-style", choices=["sequential", "swarm", "collapse"], default="swarm")
    p.add_argument("--source-score", choices=SOURCE_SCORE_MODES, default="off", help="Material-aware source weighting before placement.")
    p.add_argument("--source-diversity", type=float, default=0.0, help="Source balancing 0..1; higher values penalize immediate and repeated source reuse.")
    p.add_argument("--bpm", type=float, default=0.0, help="Manual tempo for beat-grid slicing and placement. Use 0 to disable.")
    p.add_argument("--slice-grid", choices=sorted(SLICE_GRID_FACTORS), default="off", help="Beat grid unit for source slicing and event starts when --bpm is set.")
    p.add_argument("--beat-jump-mode", choices=["random", "similarity"], default="random", help="Beat source jump planner. Similarity mode uses cache planning metadata when available and falls back to weighted random selection.")
    p.add_argument("--beat-similarity-weight", type=float, default=1.0, help="Probability 0..1 of following a similarity neighbor when --beat-jump-mode similarity has an active plan.")
    p.add_argument("--beat-novelty", type=float, default=0.0, help="Probability bias 0..1 toward farther, more disruptive similarity neighbors.")
    p.add_argument("--stutter-rate", type=float, default=0.0, help="Beat-grid probability for retriggered stutter cells; requires active --bpm/--slice-grid.")
    p.add_argument("--mute-rate", type=float, default=0.0, help="Beat-grid probability for replacing cells with silence; requires active --bpm/--slice-grid.")
    p.add_argument("--repeat-rate", type=float, default=0.0, help="Beat-grid probability for repeating cells; requires active --bpm/--slice-grid.")
    p.add_argument("--beat-dropout-rate", type=float, default=0.0, help="Beat-grid probability for longer grid-aligned dropouts; requires active --bpm/--slice-grid.")
    p.add_argument("--memory-depth", type=int, default=10, help="Rolling memory depth for ghost recurrence.")
    p.add_argument("--silence-prob", type=float, default=0.15, help="Probability of dead-air insertion.")
    p.add_argument("--recurrence-prob", type=float, default=0.28, help="Probability to reuse previous source memory.")
    p.add_argument("--ghost-prob", type=float, default=0.22, help="Probability to force ghost-layer behavior.")

    p.add_argument("--top300-csv", default=str(DEFAULT_TOP300_CSV))
    p.add_argument("--full-csv", default=str(DEFAULT_FULL_CSV))
    p.add_argument("--agitprop-count", type=int, default=40)
    p.add_argument("--broadcast-count", type=int, default=16)
    p.add_argument("--chant-count", type=int, default=120)
    p.add_argument("--chant-cells-csv", default="")
    p.add_argument("--cut-match-count", type=int, default=3)
    p.add_argument("--rupture-prob", type=float, default=0.35)
    p.add_argument("--stutter-prob", type=float, default=0.32)
    p.add_argument("--text-chaos", type=float, default=0.6)
    p.add_argument("--absurd-seriousness", type=float, default=0.62, help="Bias toward institutional absurdity and deadpan escalation.")
    p.add_argument("--agitprop-personality", default="auto", help="Comma-separated modes or auto/all (POSTER, DECREE, COLLAPSE, PRESS BRIEFING FROM HELL, ADMINISTRATIVE CHANT, FALSE PATRIOTIC, GHOST BUREAU, PUBLIC INTEREST FEVER).")
    p.add_argument("--max-words-slogan", type=int, default=11)
    p.add_argument("--export-debug-summary", action="store_true", help="Write run_summary.txt.")
    p.add_argument("--live-control-file", default="", help="Optional JSON control file for live parameter overrides.")
    p.add_argument("--live-control-poll-ms", type=int, default=250, help="Poll interval for live control file updates.")
    p.add_argument("--live-telemetry-jsonl", default="", help="Optional JSONL file for live control telemetry.")

    parsed = p.parse_args()
    parsed._explicit_args = explicit_arg_dests(p, sys.argv[1:])
    if parsed.list_presets:
        print_presets()
        raise SystemExit(0)
    if parsed.show_recipe:
        print_recipe(parsed.show_recipe)
        raise SystemExit(0)
    if parsed.doctor:
        print_doctor()
        raise SystemExit(0)
    if parsed.init_qa_sources:
        written = write_qa_sources(Path(parsed.init_qa_sources), overwrite=parsed.overwrite)
        root = Path(parsed.init_qa_sources).expanduser().resolve()
        print(f"QA sources written under: {root}")
        for group in sorted(QA_SOURCE_SPECS):
            count = len([path for path in written if path.parent.name == group and path.suffix.lower() == ".wav"])
            print(f"  {group}: {count} wav")
        cue_count = len([path for path in written if path.suffix.lower() in {".srt", ".csv"}])
        if cue_count:
            print(f"  cues: {cue_count} files")
        raise SystemExit(0)
    if parsed.scan_dataset:
        run_dataset_scan_cli(parsed)
        raise SystemExit(0)
    return parsed


def explicit_arg_dests(parser: argparse.ArgumentParser, argv: Sequence[str]) -> Set[str]:
    option_to_dest = {
        option: action.dest
        for action in parser._actions
        for option in action.option_strings
        if action.dest != argparse.SUPPRESS
    }
    explicit: Set[str] = set()
    for raw in argv:
        option = raw.split("=", 1)[0]
        dest = option_to_dest.get(option)
        if dest:
            explicit.add(dest)
    return explicit


def print_presets() -> None:
    print("TRANSMISSIONS presets:")
    for name in sorted(PRESET_VALUES):
        print(f"  {name}: {PRESET_VALUES[name]['description']}")


def print_recipe(name: str) -> None:
    names = list(RECIPE_COMMANDS) if name == "all" else [name]
    print("TRANSMISSIONS recipes:")
    for recipe_name in names:
        if recipe_name not in RECIPE_COMMANDS:
            raise SystemExit(f"Unknown recipe: {recipe_name}")
        description, command = RECIPE_COMMANDS[recipe_name]
        print(f"\n## {recipe_name}")
        print(description)
        print()
        print(command)


def format_check(ok: bool, detail: str) -> str:
    return f"{'ok' if ok else 'missing'} - {detail}"


def module_status(module_name: str) -> Tuple[bool, str]:
    try:
        spec = importlib.util.find_spec(module_name)
    except ValueError:
        spec = None
    if spec is None:
        return False, module_name
    try:
        module = importlib.import_module(module_name)
    except Exception as exc:
        return False, f"{module_name} import failed: {exc}"
    version = getattr(module, "__version__", "")
    return True, f"{module_name}{f' {version}' if version else ''}"


def optional_analysis_checks() -> List[Tuple[str, bool, str]]:
    checks = []
    for label, module_name in OPTIONAL_ANALYSIS_MODULES:
        ok, detail = module_status(module_name)
        checks.append((label, ok, detail))
    return checks


def print_doctor() -> None:
    print("CUTUP DOCTOR")
    print(f"python: {platform.python_version()} ({sys.executable})")

    checks: List[Tuple[str, bool, str]] = []
    py_ok = sys.version_info >= (3, 10)
    checks.append(("python>=3.10", py_ok, f"{platform.python_version()}"))

    pydub_ok, pydub_detail = module_status("pydub")
    checks.append(("pydub", pydub_ok, pydub_detail))

    audioop_ok = bool(importlib.util.find_spec("audioop") or importlib.util.find_spec("pyaudioop"))
    if sys.version_info >= (3, 13):
        checks.append(("audioop-lts", audioop_ok, "audioop/pyaudioop compatibility module for Python 3.13+"))

    ffmpeg_path = shutil.which("ffmpeg") or shutil.which("avconv")
    checks.append(("ffmpeg/avconv", bool(ffmpeg_path), ffmpeg_path or "install ffmpeg before audio rendering"))

    checks.append(("top300 csv", DEFAULT_TOP300_CSV.exists(), str(DEFAULT_TOP300_CSV)))
    checks.append(("full csv", DEFAULT_FULL_CSV.exists(), str(DEFAULT_FULL_CSV)))

    for name, ok, detail in checks:
        print(f"{name}: {format_check(ok, detail)}")

    analysis_checks = optional_analysis_checks()
    print("optional analysis:")
    for name, ok, detail in analysis_checks:
        print(f"  {name}: {format_check(ok, detail)}")
    if all(ok for _, ok, _ in analysis_checks):
        print("analysis_status: ready")
    else:
        print("analysis_status: optional dependencies not installed")
        print("analysis_install: python3 -m pip install -e '.[analysis]' or python3 -m pip install -r requirements-analysis.txt")

    presets = ", ".join(sorted(PRESET_VALUES))
    print(f"presets: {presets}")

    if all(ok for _, ok, _ in checks):
        print("status: ready")
    else:
        print("status: action needed")


def apply_preset(args: argparse.Namespace) -> argparse.Namespace:
    if not args.preset:
        return args
    explicit = getattr(args, "_explicit_args", set())
    preset = PRESET_VALUES[args.preset]["values"]
    for key, value in preset.items():
        if key not in explicit:
            setattr(args, key, value)
    return args


def apply_phrase_length(args: argparse.Namespace) -> argparse.Namespace:
    if args.phrase_length == "auto":
        return args
    explicit = getattr(args, "_explicit_args", set())
    min_s, max_s = PHRASE_LENGTH_RANGES[args.phrase_length]
    if "min_frag" not in explicit:
        args.min_frag = min_s
    if "max_frag" not in explicit:
        args.max_frag = max_s
    return args


def validate_args(args: argparse.Namespace) -> argparse.Namespace:
    """Validate and normalize CLI arguments with clear failure reasons."""
    apply_preset(args)
    apply_phrase_length(args)
    if args.write_source_manifest and not args.scan_dataset:
        raise SystemExit("--write-source-manifest requires --scan-dataset")
    if args.write_dataset_report and not args.scan_dataset:
        raise SystemExit("--write-dataset-report requires --scan-dataset")
    if args.dataset_max_files < 0:
        raise SystemExit("--dataset-max-files must be >= 0")
    if args.variants < 1:
        raise SystemExit("--variants must be >= 1")
    if args.sample_rate < 8000:
        raise SystemExit("--sample-rate must be >= 8000")
    if args.duration <= 0:
        raise SystemExit("--duration must be > 0")
    if args.preview_duration < 0:
        raise SystemExit("--preview-duration must be >= 0")
    if args.semi_live_chunk_sec <= 0:
        raise SystemExit("--semi-live-chunk-sec must be > 0")
    if args.semi_live_chunk_sec < 1.0:
        raise SystemExit("--semi-live-chunk-sec must be >= 1.0")
    if args.semi_live_track and Path(args.semi_live_track).expanduser().resolve().suffix.lower() != ".wav":
        raise SystemExit("--semi-live-track must be a .wav path")
    if args.baseline_beat:
        baseline_path = Path(args.baseline_beat).expanduser().resolve()
        if not baseline_path.exists():
            raise SystemExit(f"--baseline-beat not found: {baseline_path}")
        if not baseline_path.is_file() or baseline_path.suffix.lower() not in AUDIO_EXTS:
            raise SystemExit(f"--baseline-beat must be an audio file: {baseline_path}")
    elif args.baseline_beat_bars > 0:
        raise SystemExit("--baseline-beat-bars requires --baseline-beat")
    elif args.baseline_beat_duck_db > 0:
        raise SystemExit("--baseline-beat-duck-db requires --baseline-beat")
    if not math.isfinite(float(args.baseline_beat_gain)):
        raise SystemExit("--baseline-beat-gain must be a finite dB value")
    if args.baseline_beat_bars < 0:
        raise SystemExit("--baseline-beat-bars must be >= 0")
    if not math.isfinite(float(args.baseline_beat_duck_db)) or args.baseline_beat_duck_db < 0:
        raise SystemExit("--baseline-beat-duck-db must be a finite value >= 0")
    if args.baseline_beat_duck_ms < 0:
        raise SystemExit("--baseline-beat-duck-ms must be >= 0")
    if args.bpm < 0:
        raise SystemExit("--bpm must be >= 0")
    if args.bpm and not 20 <= args.bpm <= 300:
        raise SystemExit("--bpm must be 20..300, or 0 to disable beat-grid behavior")
    baseline_can_supply_bpm = bool(args.baseline_beat and args.baseline_beat_bars > 0)
    if args.bpm <= 0 and args.slice_grid != "off" and "slice_grid" in getattr(args, "_explicit_args", set()) and not baseline_can_supply_bpm:
        raise SystemExit("--slice-grid requires --bpm")
    if args.baseline_placement != "any":
        if not args.baseline_beat:
            raise SystemExit("--baseline-placement requires --baseline-beat")
        if args.bpm <= 0 and not baseline_can_supply_bpm:
            raise SystemExit("--baseline-placement requires --bpm or --baseline-beat-bars")
        if args.slice_grid == "off" and ("slice_grid" in getattr(args, "_explicit_args", set()) or args.bpm > 0):
            raise SystemExit("--baseline-placement requires an active --slice-grid")
    if not 0.0 <= args.source_diversity <= 1.0:
        raise SystemExit("--source-diversity must be 0..1")
    if not 0.0 <= args.beat_similarity_weight <= 1.0:
        raise SystemExit("--beat-similarity-weight must be 0..1")
    if not 0.0 <= args.beat_novelty <= 1.0:
        raise SystemExit("--beat-novelty must be 0..1")
    if args.memory_depth < 1:
        raise SystemExit("--memory-depth must be >= 1")
    if args.cut_match_count < 1:
        raise SystemExit("--cut-match-count must be >= 1")
    if args.max_words_slogan < 1:
        raise SystemExit("--max-words-slogan must be >= 1")
    if args.agitprop_count < 1 or args.broadcast_count < 1 or args.chant_count < 1:
        raise SystemExit("--agitprop-count, --broadcast-count, and --chant-count must be >= 1")
    if args.live_control_poll_ms < 30:
        raise SystemExit("--live-control-poll-ms must be >= 30")
    if args.source_manifest:
        manifest_path = Path(args.source_manifest).expanduser().resolve()
        if not manifest_path.exists():
            raise SystemExit(f"--source-manifest not found: {manifest_path}")
        if not manifest_path.is_file():
            raise SystemExit(f"--source-manifest must be a CSV or JSON file: {manifest_path}")
        if manifest_path.suffix.lower() not in {".csv", ".json"}:
            raise SystemExit("--source-manifest must be a .csv or .json file")

    args.silence_insert_range_ms = parse_silence_insert_ms(args.silence_insert_ms)
    args.min_frag = max(0.01, args.min_frag)
    args.max_frag = max(args.min_frag, args.max_frag)
    args.silence_prob = clamp(args.silence_prob, 0.0, 0.95)
    args.recurrence_prob = clamp(args.recurrence_prob, 0.0, 0.95)
    args.rupture_prob = clamp(args.rupture_prob, 0.0, 1.0)
    args.stutter_prob = clamp(args.stutter_prob, 0.0, 1.0)
    args.ghost_prob = clamp(args.ghost_prob, 0.0, 0.95)
    args.burst_rate = clamp(args.burst_rate, 0.0, 1.0)
    args.dropout_rate = clamp(args.dropout_rate, 0.0, 1.0)
    args.reverse_shard_rate = clamp(args.reverse_shard_rate, 0.0, 1.0)
    for key, value in beat_control_rates(args).items():
        setattr(args, key, value)
    args.text_chaos = clamp(args.text_chaos, 0.0, 1.5)
    args.absurd_seriousness = clamp(args.absurd_seriousness, 0.0, 1.0)
    return args


# -------------------------------------------------------------------
# QA SOURCE GENERATION
# -------------------------------------------------------------------


def pulse_decay(t: float, rate_hz: float, decay: float = 18.0) -> float:
    position = (t * rate_hz) % 1.0
    return math.exp(-position * decay)


def pulse_gate(t: float, rate_hz: float, width: float = 0.5) -> float:
    return 1.0 if (t * rate_hz) % 1.0 < width else 0.0


def qa_voice_value(t: float, rng: random.Random, base_shift: float = 0.0, gaps: bool = False) -> float:
    syllable_len = 0.28
    syllable = int(t / syllable_len)
    syllable_pos = (t / syllable_len) % 1.0
    phrase_gate = pulse_gate(t, 0.55, 0.72)
    if gaps and int(t * 0.8) % 3 == 1:
        phrase_gate = 0.0
    freqs = (142.0, 176.0, 164.0, 213.0, 188.0, 151.0, 231.0)
    freq = freqs[syllable % len(freqs)] + base_shift
    envelope = math.sin(math.pi * syllable_pos) ** 0.45
    shimmer = 1.0 + 0.015 * math.sin(2.0 * math.pi * 5.2 * t)
    voice = (
        0.42 * math.sin(2.0 * math.pi * freq * shimmer * t)
        + 0.20 * math.sin(2.0 * math.pi * freq * 2.3 * t)
        + 0.11 * math.sin(2.0 * math.pi * freq * 3.7 * t)
    )
    breath = 0.035 * rng.uniform(-1.0, 1.0)
    return (voice + breath) * envelope * phrase_gate


def qa_source_value(profile: str, t: float, idx: int, rng: random.Random) -> float:
    noise = rng.uniform(-1.0, 1.0)
    if profile == "loop_drums":
        beat_pos = (t * 2.0) % 1.0
        beat_idx = int(t * 2.0) % 4
        kick_env = pulse_decay(t, 2.0, 15.0)
        kick = 0.72 * math.sin(2.0 * math.pi * (54.0 + 34.0 * kick_env) * t) * kick_env
        snare_env = math.exp(-beat_pos * 19.0) if beat_idx in {1, 3} else 0.0
        snare = 0.28 * noise * snare_env
        hat = 0.10 * noise * pulse_decay(t, 8.0, 22.0)
        return kick + snare + hat
    if profile == "loop_bass":
        freqs = (55.0, 55.0, 73.42, 82.41, 55.0, 110.0, 98.0, 73.42)
        freq = freqs[int(t * 2.0) % len(freqs)]
        gate = pulse_gate(t, 4.0, 0.62)
        return 0.48 * math.sin(2.0 * math.pi * freq * t) * gate * (0.65 + 0.35 * pulse_decay(t, 4.0, 5.5))
    if profile == "loop_metal":
        env = pulse_decay(t, 8.0, 24.0)
        return env * (0.32 * math.sin(2.0 * math.pi * 1175.0 * t) + 0.22 * math.sin(2.0 * math.pi * 1820.0 * t))
    if profile == "loop_noise_hat":
        return 0.22 * noise * pulse_decay(t, 8.0, 28.0) + 0.05 * noise * pulse_gate(t, 16.0, 0.18)
    if profile == "voice_a":
        return qa_voice_value(t, rng, base_shift=0.0)
    if profile == "voice_b":
        return qa_voice_value(t, rng, base_shift=42.0)
    if profile == "voice_gap":
        return qa_voice_value(t, rng, base_shift=-18.0, gaps=True)
    if profile == "signal_bursts":
        carrier = 0.16 * math.sin(2.0 * math.pi * 920.0 * t)
        bursts = 0.62 * noise * pulse_gate(t, 1.35, 0.12) * pulse_decay(t, 1.35, 4.0)
        return carrier + bursts
    if profile == "signal_dropouts":
        dropout = 0.0 if pulse_gate(t, 2.8, 0.18) else 1.0
        carrier = 0.32 * math.sin(2.0 * math.pi * 720.0 * t) + 0.08 * math.sin(2.0 * math.pi * 1440.0 * t)
        return dropout * (carrier + 0.045 * noise)
    if profile == "signal_scanline":
        hash_gate = pulse_gate(t, 31.0, 0.16)
        sweep = math.sin(2.0 * math.pi * (380.0 + 260.0 * math.sin(2.0 * math.pi * 0.24 * t)) * t)
        return 0.18 * sweep + 0.34 * noise * hash_gate
    return 0.0


def write_pcm16_wav(path: Path, duration_s: float, sample_value: Callable[[float, int, random.Random], float], sample_rate: int = 44100) -> None:
    rng = random.Random(path.name)
    frame_count = int(duration_s * sample_rate)
    frames = bytearray()
    for idx in range(frame_count):
        t = idx / float(sample_rate)
        value = clamp(sample_value(t, idx, rng), -0.98, 0.98)
        frames.extend(struct.pack("<h", int(value * 32767)))
    with wave.open(str(path), "wb") as wav:
        wav.setnchannels(1)
        wav.setsampwidth(2)
        wav.setframerate(sample_rate)
        wav.writeframes(frames)


def format_srt_timecode(ms: int) -> str:
    total_ms = max(0, int(ms))
    hours, rem = divmod(total_ms, 3_600_000)
    minutes, rem = divmod(rem, 60_000)
    seconds, millis = divmod(rem, 1000)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d},{millis:03d}"


def write_qa_srt(path: Path) -> None:
    blocks = []
    for cue_index, start_ms, end_ms, text in QA_SRT_CUES:
        blocks.append(
            "\n".join(
                [
                    str(cue_index),
                    f"{format_srt_timecode(start_ms)} --> {format_srt_timecode(end_ms)}",
                    text,
                ]
            )
        )
    path.write_text("\n\n".join(blocks) + "\n", encoding="utf-8")


def write_qa_cue_csv(path: Path) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["file", "cue_index", "start_tc", "end_tc", "text"])
        writer.writeheader()
        for cue_index, (filename, start_ms, end_ms, text) in enumerate(QA_CSV_CUES, start=1):
            writer.writerow(
                {
                    "file": filename,
                    "cue_index": cue_index,
                    "start_tc": format_srt_timecode(start_ms),
                    "end_tc": format_srt_timecode(end_ms),
                    "text": text,
                }
            )


def write_qa_sources(root: Path, overwrite: bool = False) -> List[Path]:
    root = root.expanduser().resolve()
    if root.exists() and not root.is_dir():
        raise SystemExit(f"--init-qa-sources path exists and is not a directory: {root}")

    existing: List[Path] = []
    targets: List[Tuple[Path, float, str]] = []
    for group, specs in QA_SOURCE_SPECS.items():
        group_dir = root / group
        for filename, duration_s, profile in specs:
            path = group_dir / filename
            targets.append((path, duration_s, profile))
            if path.exists() and not overwrite:
                existing.append(path)
    cue_targets = (root / "voice" / "voice_phrase_a.srt", root / "voice" / "voice_cues.csv")
    for path in cue_targets:
        if path.exists() and not overwrite:
            existing.append(path)
    if existing:
        shown = ", ".join(str(path) for path in existing[:3])
        suffix = "..." if len(existing) > 3 else ""
        raise SystemExit(f"QA source file already exists: {shown}{suffix}. Pass --overwrite to replace generated QA sources.")

    written: List[Path] = []
    for path, duration_s, profile in targets:
        path.parent.mkdir(parents=True, exist_ok=True)
        write_pcm16_wav(path, duration_s, lambda t, idx, rng, profile=profile: qa_source_value(profile, t, idx, rng))
        written.append(path)
    cue_targets[0].parent.mkdir(parents=True, exist_ok=True)
    write_qa_srt(cue_targets[0])
    write_qa_cue_csv(cue_targets[1])
    written.extend(cue_targets)
    return written


# -------------------------------------------------------------------
# SHARED TEXT UTILITIES
# -------------------------------------------------------------------


def clamp(v: float, low: float, high: float) -> float:
    return max(low, min(high, v))


def beat_control_rates(args: argparse.Namespace) -> Dict[str, float]:
    return {
        key: clamp(float(getattr(args, key, 0.0) or 0.0), 0.0, 1.0)
        for key in BEAT_RATE_KEYS
    }


def build_live_control(args: argparse.Namespace) -> Optional[LiveControlState]:
    control_file = Path(args.live_control_file).expanduser().resolve() if args.live_control_file else None
    telemetry_path = Path(args.live_telemetry_jsonl).expanduser().resolve() if args.live_telemetry_jsonl else None
    if telemetry_path:
        telemetry_path.parent.mkdir(parents=True, exist_ok=True)
    enabled = bool(control_file or telemetry_path)
    return LiveControlState(
        enabled=enabled,
        control_file=control_file,
        poll_ms=max(30, int(args.live_control_poll_ms)),
        telemetry_path=telemetry_path,
    )


def runtime_snapshot(args: argparse.Namespace, live: Optional[LiveControlState] = None) -> RuntimeParams:
    if live and live.enabled:
        live.poll()
        values = {key: live.value(args, key) for key in LIVE_CONTROL_LIMITS}
        return RuntimeParams(
            absurd_seriousness=values["absurd_seriousness"],
            text_chaos=values["text_chaos"],
            rupture_prob=values["rupture_prob"],
            stutter_prob=values["stutter_prob"],
            recurrence_prob=values["recurrence_prob"],
            ghost_prob=values["ghost_prob"],
            silence_prob=values["silence_prob"],
            burst_rate=values["burst_rate"],
            dropout_rate=values["dropout_rate"],
            reverse_shard_rate=values["reverse_shard_rate"],
            filter_severity=live.filter_severity_override,
            stutter_rate=values["stutter_rate"],
            mute_rate=values["mute_rate"],
            repeat_rate=values["repeat_rate"],
            beat_dropout_rate=values["beat_dropout_rate"],
            source_diversity=values["source_diversity"],
            section_arc=live.section_arc_override,
            source_score=live.source_score_override,
            baseline_placement=live.baseline_placement_override,
            force_section=live.section_override,
            hold_section=live.hold_section,
            burst_now=live.burst_now,
            panic_silence=live.panic_silence,
        )
    return RuntimeParams(
        absurd_seriousness=float(args.absurd_seriousness),
        text_chaos=float(args.text_chaos),
        rupture_prob=float(args.rupture_prob),
        stutter_prob=float(args.stutter_prob),
        recurrence_prob=float(args.recurrence_prob),
        ghost_prob=float(args.ghost_prob),
        silence_prob=float(args.silence_prob),
        burst_rate=float(getattr(args, "burst_rate", 0.0)),
        dropout_rate=float(getattr(args, "dropout_rate", 0.0)),
        reverse_shard_rate=float(getattr(args, "reverse_shard_rate", 0.0)),
        filter_severity=str(getattr(args, "filter_severity", "")),
        stutter_rate=float(getattr(args, "stutter_rate", 0.0)),
        mute_rate=float(getattr(args, "mute_rate", 0.0)),
        repeat_rate=float(getattr(args, "repeat_rate", 0.0)),
        beat_dropout_rate=float(getattr(args, "beat_dropout_rate", 0.0)),
        source_diversity=float(getattr(args, "source_diversity", 0.0)),
        section_arc="",
        source_score="",
        baseline_placement="",
    )


def apply_runtime_params(args: argparse.Namespace, runtime: RuntimeParams) -> argparse.Namespace:
    local_args = argparse.Namespace(**vars(args))
    local_args.absurd_seriousness = runtime.absurd_seriousness
    local_args.text_chaos = runtime.text_chaos
    local_args.rupture_prob = runtime.rupture_prob
    local_args.stutter_prob = runtime.stutter_prob
    local_args.recurrence_prob = runtime.recurrence_prob
    local_args.ghost_prob = runtime.ghost_prob
    local_args.silence_prob = runtime.silence_prob
    local_args.burst_rate = runtime.burst_rate
    local_args.dropout_rate = runtime.dropout_rate
    local_args.reverse_shard_rate = runtime.reverse_shard_rate
    if runtime.filter_severity:
        local_args.filter_severity = runtime.filter_severity
    local_args.stutter_rate = runtime.stutter_rate
    local_args.mute_rate = runtime.mute_rate
    local_args.repeat_rate = runtime.repeat_rate
    local_args.beat_dropout_rate = runtime.beat_dropout_rate
    local_args.source_diversity = runtime.source_diversity
    if runtime.section_arc:
        local_args.section_arc = runtime.section_arc
    if runtime.source_score:
        local_args.source_score = runtime.source_score
    if runtime.baseline_placement:
        local_args.baseline_placement = runtime.baseline_placement
    return local_args


def clean_text(text: str) -> str:
    t = str(text or "").replace("\ufeff", "").strip()
    t = re.sub(r">>+", " ", t)
    t = re.sub(r"\s+", " ", t)
    t = t.replace(" ,", ",").replace(" .", ".")
    return t.strip(" -")


def is_usable_text(text: str) -> bool:
    return bool(text and len(text) >= 3 and not re.fullmatch(r"[^\w]+", text))


def count_words(text: str) -> int:
    return len(TOKEN_RE.findall(text))


def normalize_text(text: str) -> str:
    t = clean_text(text).lower()
    return re.sub(r"\s+", " ", t)


def token_list(text: str) -> List[str]:
    return [t.lower() for t in TOKEN_RE.findall(text)]


def get_first_present(row: Dict[str, str], keys: Sequence[str], default: str = "") -> str:
    for key in keys:
        if key in row and str(row.get(key, "")).strip() != "":
            return str(row.get(key, "")).strip()
    return default


def safe_float(value: str, default: float = 0.0) -> float:
    try:
        return float(str(value).strip())
    except Exception:
        return default


def safe_int(value: str, default: int = 0) -> int:
    try:
        return int(float(str(value).strip()))
    except Exception:
        return default


def parse_timecode_ms(raw: object) -> Optional[int]:
    text = str(raw or "").strip()
    if not text:
        return None
    if re.fullmatch(r"\d+(?:\.\d+)?", text):
        return int(round(float(text) * 1000))
    match = re.fullmatch(r"(?:(\d+):)?(\d{1,2}):(\d{1,2})(?:[,.](\d{1,3}))?", text)
    if not match:
        return None
    hours = int(match.group(1) or 0)
    minutes = int(match.group(2))
    seconds = int(match.group(3))
    millis_text = (match.group(4) or "0").ljust(3, "0")[:3]
    return ((hours * 3600 + minutes * 60 + seconds) * 1000) + int(millis_text)


def cut_words(text: str) -> List[str]:
    return TOKEN_RE.findall(text)


def parse_silence_insert_ms(raw: str) -> Tuple[int, int]:
    text = str(raw or "").strip().lower()
    if not text or text == "auto":
        return (0, 0)
    match = re.fullmatch(r"(\d+)\s*:\s*(\d+)", text)
    if not match:
        raise SystemExit("--silence-insert-ms must be formatted as min:max milliseconds, e.g. 120:420")
    low, high = int(match.group(1)), int(match.group(2))
    if low > high:
        raise SystemExit("--silence-insert-ms min must be <= max")
    if high > 5000:
        raise SystemExit("--silence-insert-ms max must be <= 5000")
    return (low, high)


def silence_duration_ms(args: argparse.Namespace, fallback: Tuple[int, int]) -> int:
    low, high = getattr(args, "silence_insert_range_ms", (0, 0))
    if high <= 0:
        low, high = fallback
    return random.randint(int(low), int(high))


def planner_profile_name(args: argparse.Namespace) -> str:
    requested = str(getattr(args, "planner_profile", "auto") or "auto").lower()
    if requested in {"classic", "phrase", "beat", "breach"}:
        return requested
    mode = source_score_mode(args)
    if mode == "spoken":
        return "phrase"
    if mode == "beat":
        return "beat"
    if mode == "breach":
        return "breach"
    preset = str(getattr(args, "preset", "") or "").lower()
    if "spoken" in preset or "ghost" in preset:
        return "phrase"
    if "beat" in preset or "stutter" in preset:
        return "beat"
    if "breach" in preset or "radio" in preset:
        return "breach"
    return "classic"


def source_is_spoken(sample: SampleFile) -> bool:
    text = source_text_blob(sample)
    return (
        sample.has_cue()
        or str(sample.manifest_role).lower() == "spoken"
        or keyword_hits(text, SPOKEN_SOURCE_KEYWORDS) > 0
    )


def section_planner_intent(section: str, profile_name: str) -> str:
    if profile_name == "phrase":
        return {
            "ENTRY": "establish intelligible phrase material",
            "BUILD": "reorder phrases with light interruptions",
            "PRESSURE": "tighten recurrence while preserving key words",
            "COLLAPSE": "fracture phrases into memory returns",
            "AFTERIMAGE": "let phrase ghosts decay into silence",
        }.get(section, "phrase cutup")
    if profile_name == "beat":
        return {
            "ENTRY": "lock source choices to the grid",
            "BUILD": "develop rhythmic repeats and mutes",
            "PRESSURE": "increase stutter density",
            "COLLAPSE": "break the groove with dropouts",
            "AFTERIMAGE": "leave sparse rhythmic residues",
        }.get(section, "beat cutup")
    if profile_name == "breach":
        return {
            "ENTRY": "establish unstable carrier",
            "BUILD": "introduce corrupted interruptions",
            "PRESSURE": "stack noise bursts and dropouts",
            "COLLAPSE": "overload the transmission",
            "AFTERIMAGE": "leave damaged residual signal",
        }.get(section, "signal breach")
    return {
        "ENTRY": "introduce source material",
        "BUILD": "develop cutup density",
        "PRESSURE": "increase recurrence and pressure",
        "COLLAPSE": "destabilize arrangement",
        "AFTERIMAGE": "resolve into memory traces",
    }.get(section, "cutup")


def workflow_audio_profile(profile: Dict[str, float], args: argparse.Namespace) -> Dict[str, float]:
    out = dict(profile)
    planner_profile = planner_profile_name(args)
    out["planner_profile"] = planner_profile
    intelligibility = str(getattr(args, "intelligibility", "auto"))
    if intelligibility == "high":
        out["reverse"] = float(out.get("reverse", 0.0)) * 0.25
        out["repeat"] = float(out.get("repeat", 0.0)) * 0.55
        out["filt"] = float(out.get("filt", 0.0)) * 0.55
        out["silence"] = float(out.get("silence", 0.0)) * 0.75
        out["grain_bias"] = 0.22
        out["swarm_bias"] = 0.35
        out["speed_mode"] = "clear"
    elif intelligibility == "medium":
        out["reverse"] = float(out.get("reverse", 0.0)) * 0.75
        out["repeat"] = float(out.get("repeat", 0.0)) * 0.85
        out["filt"] = float(out.get("filt", 0.0)) * 0.85
        out["grain_bias"] = 0.65
        out["swarm_bias"] = 0.75
        out["speed_mode"] = "moderate"
    elif intelligibility == "low":
        out["reverse"] = float(out.get("reverse", 0.0)) * 1.25
        out["repeat"] = float(out.get("repeat", 0.0)) * 1.15
        out["filt"] = float(out.get("filt", 0.0)) * 1.1
        out["silence"] = float(out.get("silence", 0.0)) * 1.1
        out["grain_bias"] = 1.25
        out["swarm_bias"] = 1.2
        out["speed_mode"] = "unstable"

    concrete = bool(getattr(args, "concrete", False))
    hard_cut = 0.22 if concrete else 0.14
    interruption_density = str(getattr(args, "interruption_density", "auto"))
    if interruption_density == "low":
        hard_cut *= 0.35
        out["repeat"] = float(out.get("repeat", 0.0)) * 0.65
        out["silence"] = float(out.get("silence", 0.0)) * 0.7
        out["swarm_bias"] = float(out.get("swarm_bias", 1.0)) * 0.5
    elif interruption_density == "medium":
        hard_cut *= 0.8
    elif interruption_density == "high":
        hard_cut *= 1.65
        out["repeat"] = float(out.get("repeat", 0.0)) * 1.2
        out["silence"] = float(out.get("silence", 0.0)) + 0.08
        out["swarm_bias"] = float(out.get("swarm_bias", 1.0)) * 1.3

    if planner_profile == "phrase":
        out["reverse"] = float(out.get("reverse", 0.0)) * 0.65
        out["filt"] = float(out.get("filt", 0.0)) * 0.75
        out["grain_bias"] = float(out.get("grain_bias", 1.0)) * 0.62
        out["swarm_bias"] = float(out.get("swarm_bias", 1.0)) * 0.65
        hard_cut *= 0.7
    elif planner_profile == "beat":
        out["repeat"] = float(out.get("repeat", 0.0)) * 1.12
        out["silence"] = float(out.get("silence", 0.0)) * 0.82
        out["grid_lock"] = 1.0
        hard_cut *= 0.9
    elif planner_profile == "breach":
        out["reverse"] = float(out.get("reverse", 0.0)) * 1.18
        out["filt"] = float(out.get("filt", 0.0)) * 1.12
        out["silence"] = float(out.get("silence", 0.0)) + 0.05
        out["grain_bias"] = float(out.get("grain_bias", 1.0)) * 1.18
        hard_cut *= 1.22

    out["hard_cut"] = clamp(hard_cut, 0.0, 0.75)
    for key in ("reverse", "repeat", "filt", "silence", "ghost"):
        if key in out:
            out[key] = clamp(float(out[key]), 0.0, 0.99)
    return out


def event_audio_profile(profile: Dict[str, float], args: argparse.Namespace, sample: SampleFile) -> Tuple[Dict[str, float], bool]:
    out = dict(profile)
    phrase_protected = planner_profile_name(args) == "phrase" and source_is_spoken(sample)
    if phrase_protected:
        out["reverse"] = min(float(out.get("reverse", 0.0)), 0.08)
        out["repeat"] = min(float(out.get("repeat", 0.0)), 0.38)
        out["filt"] = min(float(out.get("filt", 0.0)), 0.55)
        out["hard_cut"] = min(float(out.get("hard_cut", 0.0)), 0.08)
        out["silence"] = min(float(out.get("silence", 0.0)), 0.42)
        out["grain_bias"] = min(float(out.get("grain_bias", 1.0)), 0.24)
        out["swarm_bias"] = min(float(out.get("swarm_bias", 1.0)), 0.28)
        out["speed_mode"] = "clear"
    return out, phrase_protected


def beat_grid_ms(args: argparse.Namespace) -> int:
    bpm = float(getattr(args, "bpm", 0.0) or 0.0)
    grid = str(getattr(args, "slice_grid", "off") or "off")
    factor = SLICE_GRID_FACTORS.get(grid, 0.0)
    if bpm <= 0 or factor <= 0:
        return 0
    return max(8, int(round((60000.0 / bpm) * factor)))


def quantize_to_grid(value_ms: int, grid_ms: int) -> int:
    if grid_ms <= 0:
        return int(value_ms)
    return max(0, int(round(value_ms / grid_ms) * grid_ms))


def grid_fragment_length(audio_len: int, min_ms: int, max_ms: int, frag_mul: float, grid_ms: int) -> int:
    local_min = max(8, int(min_ms * frag_mul))
    local_max = max(local_min, int(max_ms * frag_mul))
    upper = min(audio_len, local_max)
    if upper <= 0:
        return max(8, min(audio_len, grid_ms))
    candidates = [grid_ms * mult for mult in (1, 2, 3, 4, 6, 8) if local_min <= grid_ms * mult <= upper]
    if not candidates:
        snapped = quantize_to_grid(local_min, grid_ms)
        candidates = [max(8, min(audio_len, upper, snapped or grid_ms))]
    return max(8, min(audio_len, random.choice(candidates)))


def resolved_excluded_paths(paths: Optional[Iterable[Path]] = None) -> Set[Path]:
    excluded: Set[Path] = set()
    for path in paths or []:
        try:
            excluded.add(Path(path).expanduser().resolve())
        except OSError:
            continue
    return excluded


def path_is_excluded(path: Path, excluded: Set[Path]) -> bool:
    if not excluded:
        return False
    try:
        return path.expanduser().resolve() in excluded
    except OSError:
        return False


def candidate_audio_paths(root: Path, exclude_paths: Optional[Iterable[Path]] = None) -> List[Path]:
    excluded = resolved_excluded_paths(exclude_paths)
    if root.is_file():
        return [root] if root.suffix.lower() in AUDIO_EXTS and not path_is_excluded(root, excluded) else []
    if root.is_dir():
        return sorted(
            path
            for path in root.rglob("*")
            if path.is_file() and path.suffix.lower() in AUDIO_EXTS and not path_is_excluded(path, excluded)
        )
    return []


def resolve_cue_audio_path(
    raw_file: str,
    input_root: Path,
    cue_file: Path,
    default_audio: Optional[Path],
    exclude_paths: Optional[Iterable[Path]] = None,
) -> Optional[Path]:
    excluded = resolved_excluded_paths(exclude_paths)
    text = str(raw_file or "").strip()
    if not text:
        return None if default_audio is not None and path_is_excluded(default_audio, excluded) else default_audio
    raw_path = Path(text).expanduser()
    candidates: List[Path] = []
    if raw_path.is_absolute():
        candidates.append(raw_path)
    else:
        if input_root.is_dir():
            candidates.append(input_root / raw_path)
        else:
            candidates.append(input_root.parent / raw_path)
        candidates.append(cue_file.parent / raw_path)
    for candidate in candidates:
        if candidate.exists() and candidate.suffix.lower() in AUDIO_EXTS and not path_is_excluded(candidate, excluded):
            return candidate.resolve()
    return None


def parse_srt_cues(path: Path) -> List[Dict[str, object]]:
    text = path.read_text(encoding="utf-8-sig")
    blocks = re.split(r"\n\s*\n", text.replace("\r\n", "\n").replace("\r", "\n").strip())
    rows: List[Dict[str, object]] = []
    for block in blocks:
        lines = [line.strip() for line in block.split("\n") if line.strip()]
        if not lines:
            continue
        cue_index = safe_int(lines[0], len(rows) + 1)
        timing_idx = 1 if len(lines) > 1 and "-->" in lines[1] else 0
        timing_line = lines[timing_idx]
        if "-->" not in timing_line:
            continue
        start_raw, end_raw = [part.strip().split()[0] for part in timing_line.split("-->", 1)]
        start_ms = parse_timecode_ms(start_raw)
        end_ms = parse_timecode_ms(end_raw)
        if start_ms is None or end_ms is None or end_ms <= start_ms:
            continue
        text_start = timing_idx + 1
        cue_text = clean_text(" ".join(lines[text_start:]))
        rows.append({"cue_index": cue_index, "start_ms": start_ms, "end_ms": end_ms, "text": cue_text, "file": ""})
    return rows


def parse_csv_cues(path: Path) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise SystemExit(f"Cue CSV '{path}' has no header row.")
        for idx, row in enumerate(reader, start=1):
            start_ms = parse_timecode_ms(get_first_present(row, START_TC_COLUMN_CANDIDATES, ""))
            end_ms = parse_timecode_ms(get_first_present(row, END_TC_COLUMN_CANDIDATES, ""))
            if start_ms is None:
                continue
            if end_ms is None:
                duration_s = safe_float(get_first_present(row, DURATION_COLUMN_CANDIDATES, "0"), 0.0)
                end_ms = start_ms + int(round(duration_s * 1000)) if duration_s > 0 else None
            if end_ms is None or end_ms <= start_ms:
                continue
            rows.append(
                {
                    "cue_index": safe_int(get_first_present(row, CUE_COLUMN_CANDIDATES, str(idx)), idx),
                    "start_ms": start_ms,
                    "end_ms": end_ms,
                    "text": clean_text(get_first_present(row, TEXT_COLUMN_CANDIDATES, "")),
                    "file": get_first_present(row, FILE_COLUMN_CANDIDATES, ""),
                }
            )
    return rows


def load_cue_rows(path: Path) -> List[Dict[str, object]]:
    suffix = path.suffix.lower()
    if suffix == ".srt":
        return parse_srt_cues(path)
    if suffix == ".csv":
        return parse_csv_cues(path)
    raise SystemExit("--cue-file must be an .srt or .csv file")


def cue_loop_hint(duration_ms: int) -> int:
    if duration_ms <= 450:
        return 3
    if duration_ms <= 1350:
        return 2
    if duration_ms <= 3400:
        return 1
    return 0


def split_manifest_tags(raw: object) -> List[str]:
    text = str(raw or "").strip().lower()
    if not text:
        return []
    parts = re.split(r"[,;|]", text)
    tags = [re.sub(r"\s+", "-", part.strip()) for part in parts if part.strip()]
    return sorted({tag for tag in tags if tag})


def manifest_row_text(row: Dict[str, object]) -> Dict[str, str]:
    return {str(key): "" if value is None else str(value) for key, value in row.items()}


def manifest_entry_from_row(row: Dict[str, object]) -> Tuple[str, Dict[str, object]]:
    text_row = manifest_row_text(row)
    raw_file = get_first_present(text_row, MANIFEST_FILE_COLUMN_CANDIDATES, "")
    if not raw_file:
        return "", {}
    role = get_first_present(text_row, MANIFEST_ROLE_COLUMN_CANDIDATES, "").strip().lower()
    tags = split_manifest_tags(get_first_present(text_row, MANIFEST_TAG_COLUMN_CANDIDATES, ""))
    if role:
        tags = sorted({*tags, role})
    return raw_file, {
        "tags": ",".join(tags),
        "role": role,
        "weight": clamp(safe_float(get_first_present(text_row, MANIFEST_WEIGHT_COLUMN_CANDIDATES, "1.0"), 1.0), 0.05, 20.0),
        "intensity_hint": max(0, safe_int(get_first_present(text_row, INTENSITY_COLUMN_CANDIDATES, "0"), 0)),
        "loop_hint": max(0, safe_int(get_first_present(text_row, MANIFEST_LOOP_COLUMN_CANDIDATES, "0"), 0)),
        "words": max(0, safe_int(get_first_present(text_row, MANIFEST_WORD_COLUMN_CANDIDATES, "0"), 0)),
    }


def source_manifest_keys_for_raw(raw_file: str, input_root: Path, manifest_path: Path) -> Set[str]:
    text = str(raw_file or "").strip()
    if not text:
        return set()
    keys = {text, text.replace("\\", "/"), Path(text).name}
    raw_path = Path(text).expanduser()
    candidates: List[Path] = []
    if raw_path.is_absolute():
        candidates.append(raw_path)
    else:
        candidates.append((input_root if input_root.is_dir() else input_root.parent) / raw_path)
        candidates.append(manifest_path.parent / raw_path)
    for candidate in candidates:
        try:
            resolved = candidate.resolve()
        except OSError:
            resolved = candidate.absolute()
        keys.add(str(resolved))
        keys.add(resolved.name)
        try:
            keys.add(str(resolved.relative_to(input_root if input_root.is_dir() else input_root.parent)))
        except ValueError:
            pass
    return {key for key in keys if key}


def sample_manifest_keys(sample: SampleFile, input_root: Path) -> Set[str]:
    keys = {str(sample.path), sample.path.name}
    try:
        keys.add(str(sample.path.resolve()))
    except OSError:
        pass
    roots = [input_root if input_root.is_dir() else input_root.parent, input_root.parent]
    for root in roots:
        try:
            keys.add(str(sample.path.relative_to(root)))
        except ValueError:
            pass
    return {key for key in keys if key}


def load_source_manifest(path: Path, input_root: Path) -> Dict[str, Dict[str, object]]:
    suffix = path.suffix.lower()
    rows: List[Dict[str, object]] = []
    if suffix == ".csv":
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                raise SystemExit(f"Source manifest CSV '{path}' has no header row.")
            rows = [dict(row) for row in reader]
    elif suffix == ".json":
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise SystemExit(f"Source manifest JSON could not be parsed: {path}: {exc}") from exc
        if isinstance(payload, dict):
            raw_rows = payload.get("sources", payload.get("files", payload))
            if isinstance(raw_rows, dict):
                rows = [dict({"file": str(key)}, **{str(k): v for k, v in value.items()}) for key, value in raw_rows.items() if isinstance(value, dict)]
            elif isinstance(raw_rows, list):
                rows = [row for row in raw_rows if isinstance(row, dict)]
        elif isinstance(payload, list):
            rows = [row for row in payload if isinstance(row, dict)]
    else:
        raise SystemExit("--source-manifest must be a .csv or .json file")

    entries: Dict[str, Dict[str, object]] = {}
    for row in rows:
        raw_file, entry = manifest_entry_from_row(row)
        if not raw_file or not entry:
            continue
        for key in source_manifest_keys_for_raw(raw_file, input_root, path):
            entries[key] = dict(entry)
    return entries


def apply_source_manifest(samples: List[SampleFile], entries: Dict[str, Dict[str, object]], input_root: Path) -> int:
    matched = 0
    for sample in samples:
        entry = next((entries[key] for key in sample_manifest_keys(sample, input_root) if key in entries), None)
        if not entry:
            continue
        matched += 1
        tags = sorted({*split_manifest_tags(sample.manifest_tags), *split_manifest_tags(entry.get("tags", ""))})
        sample.manifest_tags = ",".join(tags)
        sample.manifest_role = str(entry.get("role", "") or sample.manifest_role)
        sample.manifest_weight = clamp(safe_float(str(entry.get("weight", sample.manifest_weight)), sample.manifest_weight), 0.05, 20.0)
        sample.intensity_hint = max(sample.intensity_hint, int(entry.get("intensity_hint", 0) or 0))
        sample.loop_hint = max(sample.loop_hint, int(entry.get("loop_hint", 0) or 0))
        words = int(entry.get("words", 0) or 0)
        if words > 0:
            sample.words = words
    return matched


def is_nonempty_dir(path: Path) -> bool:
    try:
        return path.is_dir() and any(path.iterdir())
    except OSError:
        return False


def unique_output_path(path: Path) -> Path:
    if not path.exists() or not is_nonempty_dir(path):
        return path
    for idx in range(2, 1000):
        candidate = path.with_name(f"{path.name}_{idx:02d}")
        if not candidate.exists() or not is_nonempty_dir(candidate):
            return candidate
    raise SystemExit(f"Could not find an unused output folder near {path}")


def resolve_output_root(raw_output: str, overwrite: bool) -> Path:
    output_root = Path(raw_output).expanduser().resolve()
    if output_root.exists() and not output_root.is_dir():
        raise SystemExit(f"--output path exists and is not a directory: {output_root}")
    if overwrite:
        return output_root
    safe_root = unique_output_path(output_root)
    if safe_root != output_root:
        print(f"Output folder exists and is non-empty; writing to: {safe_root}")
    return safe_root


def resolve_analysis_cache_path(raw_cache: str, output_root: Path) -> Optional[Path]:
    text = str(raw_cache or "").strip()
    if not text:
        return None
    if text.lower() == "auto":
        return output_root / "audio_analysis_cache.json"
    return Path(text).expanduser().resolve()


def print_dry_run(args: argparse.Namespace, output_root: Path) -> None:
    print("=== CUTUP DRY RUN ===")
    print(f"mode: {args.mode}")
    print(f"preset: {args.preset or 'none'}")
    print(f"seed: {args.seed}")
    print(f"output: {output_root}")
    print(f"duration: {args.duration}s")
    print(f"preview_duration: {args.preview_duration}s" if args.preview_duration > 0 else "preview_duration: off")
    print(f"semi_live: {'on' if args.semi_live else 'off'}")
    if args.semi_live:
        print(f"semi_live_chunk_sec: {args.semi_live_chunk_sec:g}")
        print(f"semi_live_track: {Path(args.semi_live_track).expanduser().resolve() if args.semi_live_track else 'auto'}")
    analysis_cache = resolve_analysis_cache_path(args.analysis_cache, output_root)
    print(f"analysis_cache: {analysis_cache if analysis_cache else 'off'}")
    if analysis_cache:
        print(f"analysis_cache_mode: {'readonly' if getattr(args, 'analysis_cache_readonly', False) else 'update'}")
    print(f"source_manifest: {Path(args.source_manifest).expanduser().resolve() if args.source_manifest else 'off'}")
    print(f"variants: {args.variants}")
    print(f"density: {args.density}")
    print(f"sectional: {args.sectional}")
    print(f"section_arc: {args.section_arc}")
    print(f"planner_profile: {planner_profile_name(args)}")
    print(f"arrangement_style: {args.arrangement_style}")
    print(f"source_score: {source_score_mode(args)}")
    print(f"effective_source_score: {effective_source_score_mode(args)}")
    print(f"source_diversity: {args.source_diversity:.2f}")
    print(f"concrete: {args.concrete}")
    print(f"bed_noise: {args.bed_noise}")
    if args.baseline_beat:
        print(f"baseline_beat: {Path(args.baseline_beat).expanduser().resolve()}")
        print(f"baseline_beat_gain: {args.baseline_beat_gain:.2f} dB")
        if args.baseline_beat_duck_db > 0:
            print(f"baseline_beat_ducking: {args.baseline_beat_duck_db:.2f} dB over {args.baseline_beat_duck_ms} ms")
        else:
            print("baseline_beat_ducking: off")
        print(f"baseline_placement: {args.baseline_placement}")
        if args.baseline_beat_bars > 0 and args.bpm <= 0:
            print(f"baseline_beat_bpm: infer during render from {args.baseline_beat_bars:g} bar(s)")
        elif args.baseline_beat_bars > 0:
            print(f"baseline_beat_bpm: manual --bpm keeps {args.bpm:g} bpm")
        else:
            print("baseline_beat_bpm: off")
    else:
        print("baseline_beat: off")
        print("baseline_placement: any")
    print(f"fragments: {args.min_frag:.3f}s..{args.max_frag:.3f}s")
    print(f"phrase_length: {args.phrase_length}")
    print(f"intelligibility: {args.intelligibility}")
    print(f"interruption_density: {args.interruption_density}")
    print(f"silence_insert_ms: {args.silence_insert_ms or 'auto'}")
    print(f"burst_rate: {args.burst_rate:.2f}")
    print(f"dropout_rate: {args.dropout_rate:.2f}")
    print(f"reverse_shard_rate: {args.reverse_shard_rate:.2f}")
    print(f"filter_severity: {args.filter_severity}")
    grid_ms = beat_grid_ms(args)
    if grid_ms > 0:
        print(f"beat_grid: {args.bpm:g} bpm {args.slice_grid} ({grid_ms} ms)")
    else:
        print(f"beat_grid: inactive (bpm={args.bpm:g}, slice_grid={args.slice_grid})")
    print(f"beat_jump_mode: {args.beat_jump_mode}")
    print(f"beat_similarity_weight: {args.beat_similarity_weight:.2f}")
    print(f"beat_novelty: {args.beat_novelty:.2f}")
    if args.beat_jump_mode == "similarity" and not analysis_cache:
        print("beat_jump_plan: enable --analysis-cache to write similarity planning metadata")
    beat_rates = beat_control_rates(args)
    if any(beat_rates.values()):
        state = "active" if grid_ms > 0 else "inactive until --bpm and --slice-grid are active"
        print(
            "beat_controls: "
            f"stutter={beat_rates['stutter_rate']:.2f} "
            f"mute={beat_rates['mute_rate']:.2f} "
            f"repeat={beat_rates['repeat_rate']:.2f} "
            f"dropout={beat_rates['beat_dropout_rate']:.2f} "
            f"({state})"
        )
    else:
        print("beat_controls: off")
    print(f"pydub: {'ok' if ('pydub' in sys.modules or importlib.util.find_spec('pydub')) else 'missing'}")
    print(f"ffmpeg: {shutil.which('ffmpeg') or shutil.which('avconv') or 'missing'}")
    if args.mode in {"audio", "both", "all"}:
        if args.input:
            input_path = Path(args.input).expanduser().resolve()
            baseline_path = baseline_beat_path(args)
            candidates = candidate_audio_paths(input_path, exclude_paths=[baseline_path] if baseline_path else None)
            print(f"input: {input_path}")
            print(f"audio candidates: {len(candidates)}")
            print(f"cue_file: {Path(args.cue_file).expanduser().resolve() if args.cue_file else 'none'}")
            print(f"cue_slice_mode: {args.cue_slice_mode}")
        else:
            print("input: missing (--input is required for audio/both/all)")
    if args.mode in {"agitprop", "cuttargets", "both", "all"}:
        print(f"top300_csv: {Path(args.top300_csv).expanduser().resolve()}")
        print(f"full_csv: {Path(args.full_csv).expanduser().resolve()}")


# -------------------------------------------------------------------
# CSV LOADING / SCHEMA NORMALIZATION
# -------------------------------------------------------------------


def tag_text(text: str) -> List[str]:
    tag_rules = {
        "official": ["commission", "federal", "authority", "policy", "department", "official"],
        "threat": ["threat", "license", "revocation", "punish", "warning", "censor"],
        "freedom": ["free speech", "first amendment", "rights", "liberty", "freedom"],
        "command": ["must", "need to", "have to", "stop", "do it", "go ahead"],
        "collapse": ["silence", "ending", "fear", "erasure", "danger", "attack"],
        "bureaucratic": ["obligation", "regulatory", "accountable", "public interest", "license"],
    }
    t = text.lower()
    tags = {tag for tag, terms in tag_rules.items() if any(term in t for term in terms)}
    wc = count_words(text)
    tags.add("micro" if wc <= 3 else "short" if wc <= 8 else "phrase" if wc <= 16 else "long")
    if re.search(r"\bno\b|\bnot\b|\bnever\b", t):
        tags.add("negation")
    if not tags:
        tags.add("loose")
    return sorted(tags)


def load_line_bank(path: Path, bank_name: str) -> Tuple[List[Line], CSVLoadStats]:
    stats = CSVLoadStats()
    rows: List[Line] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise SystemExit(f"CSV '{path}' has no header row; cannot load {bank_name} bank.")
        for row in reader:
            if not row:
                stats.skipped_empty += 1
                continue
            text = clean_text(get_first_present(row, TEXT_COLUMN_CANDIDATES, ""))
            if not text:
                stats.skipped_empty += 1
                continue
            if not is_usable_text(text):
                stats.skipped_unusable += 1
                continue
            line = Line(
                text=text,
                file=get_first_present(row, FILE_COLUMN_CANDIDATES, ""),
                clip_id=get_first_present(row, CLIP_ID_COLUMN_CANDIDATES, ""),
                cue_index=safe_int(get_first_present(row, CUE_COLUMN_CANDIDATES, "0"), 0),
                start_tc=get_first_present(row, START_TC_COLUMN_CANDIDATES, ""),
                end_tc=get_first_present(row, END_TC_COLUMN_CANDIDATES, ""),
                duration_sec=safe_float(get_first_present(row, DURATION_COLUMN_CANDIDATES, "0"), 0.0),
                source_bank=bank_name,
                score=safe_float(get_first_present(row, SCORE_COLUMN_CANDIDATES, "0"), 0.0),
                loop_bin=get_first_present(row, LOOP_BIN_COLUMN_CANDIDATES, ""),
                intensity=get_first_present(row, INTENSITY_COLUMN_CANDIDATES, ""),
            )
            line.word_count = count_words(text)
            line.tags = tag_text(text)
            rows.append(line)
            stats.loaded += 1
    return rows, stats


# -------------------------------------------------------------------
# AGITPROP TEXT ENGINE
# -------------------------------------------------------------------


def agitprop_weighted_choice(pool: List[Line], chaos: float) -> Line:
    weights = []
    for line in pool:
        w = 1.0
        if line.source_bank == "top300":
            w += 2.6 + max(0.0, line.score / 14.0)
        if 2 <= line.word_count <= 10:
            w += 1.1
        if 0.3 <= line.duration_sec <= 4.0:
            w += 1.0
        for tag in ("official", "threat", "freedom", "command", "bureaucratic", "collapse"):
            if tag in line.tags:
                w += 0.55
        weights.append(max(0.1, w * (1.0 + chaos * random.uniform(-0.35, 0.7))))
    return random.choices(pool, weights=weights, k=1)[0]


def choose_line(bank: List[Line], chaos: float, required_tags: Iterable[str] = (), excluded_texts: Optional[set[str]] = None, fallback: bool = True) -> Line:
    required = list(required_tags)
    excluded = excluded_texts or set()
    pool = [x for x in bank if all(t in x.tags for t in required) and x.text not in excluded]
    if not pool and fallback:
        pool = [x for x in bank if x.text not in excluded]
    if not pool and fallback:
        # Final safety fallback for tiny banks where exclusions consumed all entries.
        pool = [x for x in bank if all(t in x.tags for t in required)]
    if not pool and fallback:
        pool = list(bank)
    if not pool:
        raise ValueError("No available lines matched selection criteria.")
    return agitprop_weighted_choice(pool, chaos=chaos)


def parse_agitprop_personalities(raw: str) -> List[str]:
    if not raw or raw.strip().lower() in {"auto", "all"}:
        return list(AGITPROP_MODE_PROFILES.keys())
    requested = [x.strip().upper() for x in raw.split(",") if x.strip()]
    canonical = {k.upper(): k for k in AGITPROP_MODE_PROFILES}
    selected = [canonical[name] for name in requested if name in canonical]
    return selected or list(AGITPROP_MODE_PROFILES.keys())


def resolve_personality(args: argparse.Namespace) -> str:
    return random.choice(getattr(args, "agitprop_personalities", list(AGITPROP_MODE_PROFILES.keys())))


def personality_weight(args: argparse.Namespace, personality: str, key: str, jitter: float = 0.15) -> float:
    profile = AGITPROP_MODE_PROFILES.get(personality, AGITPROP_MODE_PROFILES["POSTER"])
    base = profile.get(key, 0.5)
    return clamp(base * 0.6 + args.absurd_seriousness * 0.8 + random.uniform(-jitter, jitter), 0.0, 1.0)


def compress_phrase(text: str, max_words: int = 6) -> str:
    words = [w.upper() for w in TOKEN_RE.findall(text) if len(w) > 2]
    return " ".join(words[: max(1, max_words)]).strip()


def fragment(text: str, min_words: int = 1, max_words: int = 6) -> str:
    words = text.split()
    if not words:
        return text
    upper = max(1, min(max_words, len(words)))
    lower = max(1, min(min_words, upper))
    return " ".join(words[: random.randint(lower, upper)]).strip()


def splice_halves(a: str, b: str) -> str:
    aw, bw = cut_words(a), cut_words(b)
    if not aw or not bw:
        return clean_text(f"{a} {b}")
    return " ".join(aw[: max(1, len(aw) // 2)] + bw[max(1, len(bw) // 2) :])


def stutter_phrase(text: str) -> str:
    words = cut_words(text)
    if not words:
        return text.upper()
    pivot = random.choice(words[: min(len(words), 4)]).upper()
    return f"{pivot} / {pivot} / {fragment(text, 2, 5).upper()}"


def recursive_burst(text: str) -> str:
    words = cut_words(text)
    if not words:
        return text.upper()
    picks = words[: min(5, len(words))]
    return "\n".join(" ".join(w.upper() for w in picks[:i]) for i in range(1, len(picks) + 1))


def bureaucratic_melt(text: str) -> str:
    swaps = {
        "public interest": random.choice(["managed interest", "mandatory interest", "interest management"]),
        "accountable": random.choice(["countable", "procedurally loyal"]),
        "free speech": random.choice(["metered speech", "licensed speech"]),
        "first amendment": random.choice(["first adjustment", "preliminary amendment"]),
        "license": random.choice(["permission", "compliance credential"]),
        "authority": random.choice(["authorized fear", "managed authority"]),
        "policy": random.choice(["signal policy", "policy protocol", "policy instrument"]),
        "department": random.choice(["office", "committee", "bureau"]),
        "security": random.choice(["stability", "managed alarm"]),
    }
    out = text
    for src, dst in swaps.items():
        out = re.sub(src, dst, out, flags=re.I)
    if random.random() < 0.45:
        out = f"{out} {random.choice(BANAL_CONNECTORS)}"
    return clean_text(out)


def echo_decay(text: str) -> str:
    words = cut_words(text)
    if not words:
        return text
    pieces = [" ".join(words[: max(1, len(words) - i)]).upper() for i in range(min(4, len(words)))]
    return "\n".join(pieces)


def ladder_phrase(text: str) -> str:
    words = cut_words(text)
    if not words:
        return text.upper()
    nounish = [w for w in words if len(w) > 4][:4] or words[:3]
    return "\n".join(" > ".join(nounish[:i]).upper() for i in range(1, len(nounish) + 1))


def interrupt_with(a: str, b: str) -> str:
    return f"{fragment(a,2,5).upper()} // INTERRUPT // {fragment(b,1,4).lower()}"


def braid_fragments(a: str, b: str, c: str) -> str:
    fa, fb, fc = cut_words(fragment(a, 2, 4)), cut_words(fragment(b, 2, 4)), cut_words(fragment(c, 2, 4))
    out = []
    for i in range(max(len(fa), len(fb), len(fc))):
        for src in (fa, fb, fc):
            if i < len(src):
                out.append(src[i])
    return " ".join(out).upper()


def keyword_pressure(text: str) -> str:
    words = [w.upper() for w in cut_words(text)]
    if not words:
        return text.upper()
    pivot = max(words, key=len)
    return "\n".join([pivot] * random.randint(2, 5))


def collide_registers(official: str, conversational: str) -> str:
    return f"{compress_phrase(bureaucratic_melt(official), 8)}\n{fragment(conversational,2,6).lower()}"


def mirrored_contradiction(a: str, b: str) -> str:
    return f"{fragment(a,2,5).upper()}\nNOT {fragment(a,1,4).upper()}\n{fragment(b,2,5).lower()}"


def restart_with_drift(text: str) -> str:
    head = fragment(text, 2, 4).upper()
    return f"{head}\n{head}\n{fragment(bureaucratic_melt(text),2,7).lower()}"


def phrase_decay(text: str) -> str:
    ws = cut_words(text)
    if not ws:
        return text
    while len(ws) > 1 and random.random() < 0.5:
        ws.pop()
    return " ".join(ws).lower()


def glitch_gap(text: str) -> str:
    parts = cut_words(text)
    if not parts:
        return text
    keep = max(1, min(len(parts), random.randint(1, 4)))
    picked = parts[:keep]
    return " ... ".join(w.upper() for w in picked)


def false_restart(text: str) -> str:
    head = fragment(text, 1, 3).upper()
    tail = fragment(text, 2, 6).lower()
    return f"{head}\n{head}\n{head} --\n{tail}"


def collapse_to_term(text: str) -> str:
    words = cut_words(text)
    if not words:
        return "SIGNAL"
    term = max(words, key=len).upper()
    return "\n".join([term] * random.randint(4, 8))


def official_noun_stack(text: str, depth: int = 4) -> str:
    words = [w.upper() for w in cut_words(text) if len(w) > 4]
    seeds = words[: max(1, depth // 2)]
    stack = seeds + random.sample(OFFICIAL_NOUNS, k=min(depth, len(OFFICIAL_NOUNS)))
    return " ".join(stack[: max(2, depth + 1)])


def false_decree(base: str, support: str) -> str:
    clause = random.choice(PROCEDURAL_FILLERS)
    return (
        f"BY ORDER OF THE {official_noun_stack(base, 3)}\n"
        f"{clause} {fragment(support, 2, 6).upper()}\n"
        f"THIS DECLARATION REMAINS EFFECTIVE UNTIL FURTHER CLARIFICATION"
    )


def procedural_escalation(a: str, b: str) -> str:
    core = fragment(a, 2, 5).upper()
    rung2 = official_noun_stack(splice_halves(a, b), 4)
    rung3 = official_noun_stack(bureaucratic_melt(b), 6)
    return f"{core}\n{rung2}\n{rung3}\nCOMPLIANCE ESCALATES AUTOMATICALLY"


def fake_committee_statement(a: str, b: str) -> str:
    lead = official_noun_stack(a, 4)
    tail = fragment(bureaucratic_melt(b), 3, 7).lower()
    return f"THE STANDING COMMITTEE FOR {lead}\nhas reviewed {tail}\nand approves temporary contradiction"


def impossible_administrative_phrase(a: str, b: str) -> str:
    return f"{official_noun_stack(a, 3)} FOR THE MANAGEMENT OF {fragment(b,2,5).upper()} WITHOUT IMPLEMENTATION"


def slogan_inflation(text: str, seriousness: float) -> str:
    head = compress_phrase(text, max_words=3)
    rung_count = 3 + int(seriousness * 3)
    lines = [head]
    for i in range(2, rung_count + 1):
        lines.append(official_noun_stack(text, i + 1))
    lines.append("FOR STABILITY")
    return "\n".join(lines)


def recursive_command_block(command: str, support: str, seriousness: float) -> str:
    c = fragment(command, 1, 3).upper()
    suffix = fragment(support, 2, 6).lower()
    loops = 2 + int(seriousness * 3)
    return "\n".join([f"{c}. {suffix}" for _ in range(loops)] + [f"REPEAT {c} UNTIL CALM"]) 


def deadpan_contradiction_block(a: str, b: str) -> str:
    decree = fragment(a, 2, 6).upper()
    anti = fragment(a, 1, 4).upper()
    bridge = random.choice(BANAL_CONNECTORS)
    return f"{decree}\nTHIS DOES NOT CONSTITUTE {anti}\n{fragment(b,2,6).lower()} {bridge}"


def serious_nonsense_structure(a: str, b: str, seriousness: float) -> str:
    return (
        f"{fake_committee_statement(a, b)}\n"
        f"{recursive_command_block(a, b, seriousness)}\n"
        f"{impossible_administrative_phrase(b, a)}"
    )


def repetition_drift(text: str, seriousness: float) -> str:
    pivot = fragment(text, 2, 5).upper()
    lines = [pivot]
    loops = 2 + int(seriousness * 4)
    for _ in range(loops):
        pivot = bureaucratic_melt(pivot).upper()
        lines.append(pivot)
    lines.append(phrase_decay(pivot).lower())
    return "\n".join(lines)


def noun_pressure(a: str, b: str, seriousness: float) -> str:
    depth = 4 + int(seriousness * 4)
    base = splice_halves(a, b)
    return "\n".join(official_noun_stack(base, min(8, 2 + i)) for i in range(1, depth))


def fake_policy_language(a: str, b: str) -> str:
    clause = random.choice(PROCEDURAL_FILLERS)
    return (
        f"POLICY INSTRUMENT {official_noun_stack(a, 3)}\n"
        f"{clause} {fragment(bureaucratic_melt(b), 3, 8).upper()}\n"
        f"IMPLEMENTATION SHALL PRECEDE EXPLANATION"
    )


def contradictory_mission_statement(a: str, b: str, c: str) -> str:
    return (
        f"MISSION: {fragment(a,2,5).upper()}\n"
        f"COUNTER-MISSION: {fragment(b,2,5).upper()}\n"
        f"BOTH MISSIONS ARE MANDATORY\n"
        f"{fragment(c,2,6).lower()}"
    )


def overdetermined_public_interest(a: str, b: str, seriousness: float) -> str:
    loops = 3 + int(seriousness * 3)
    phrases = []
    seed = splice_halves(a, b)
    for _ in range(loops):
        seed = bureaucratic_melt(seed)
        phrases.append(f"PUBLIC INTEREST / {compress_phrase(seed, 4)}")
    phrases.append("PUBLIC INTEREST REMAINS UNDER REVIEW")
    return "\n".join(phrases)


def command_becomes_bureaucracy_becomes_chant(command: str, support: str, seriousness: float) -> str:
    cmd = fragment(command, 1, 3).upper()
    bureau = official_noun_stack(support, 4 + int(seriousness * 3))
    chant = keyword_pressure(splice_halves(command, support))
    repeats = [f"{cmd} PURSUANT TO {bureau}" for _ in range(1 + int(seriousness * 2))]
    return "\n".join([cmd] + repeats + [f"{cmd} ACCORDINGLY", chant])


def decree_mode(a: str, b: str, seriousness: float) -> str:
    return f"{false_decree(a, b)}\n{repetition_drift(a, seriousness)}"


def policy_meltdown_mode(a: str, b: str, seriousness: float) -> str:
    return f"{fake_policy_language(a, b)}\n{noun_pressure(a, b, seriousness)}"


def administrative_chant_mode(a: str, b: str, seriousness: float) -> str:
    return f"{command_becomes_bureaucracy_becomes_chant(a, b, seriousness)}\n{repetition_drift(b, seriousness)}"


def patriotic_absurdity_mode(a: str, b: str, c: str) -> str:
    return f"{contradictory_mission_statement(a, b, c)}\n{official_noun_stack(splice_halves(a, c), 6)}"


def committee_nightmare_mode(a: str, b: str, seriousness: float) -> str:
    return f"{fake_committee_statement(a, b)}\n{procedural_escalation(a, b)}\n{noun_pressure(a, b, seriousness)}"


def public_interest_recursion_mode(a: str, b: str, seriousness: float) -> str:
    return f"{overdetermined_public_interest(a, b, seriousness)}\n{recursive_command_block(a, b, seriousness)}"


def transmission_break(a: str, b: str, c: str) -> str:
    return (
        f"{interrupt_with(a, b)}\n"
        f"[carrier drop]\n"
        f"{glitch_gap(c)}\n"
        f"{phrase_decay(b)}"
    )


def rhetorical_pattern(official: str, threat: str, freedom: str, command: str, bridge: str, args: argparse.Namespace, personality: str) -> str:
    patterns = [
        lambda: f"{compress_phrase(official)}\n{interrupt_with(bridge, threat)}\n{collapse_to_term(threat)}\n{phrase_decay(command)}",
        lambda: f"{compress_phrase(freedom)}\n{mirrored_contradiction(freedom, command)}\n{false_restart(command)}",
        lambda: f"{collide_registers(official, bridge)}\n{glitch_gap(bridge)}\n{recursive_burst(threat)}\n[open channel]",
        lambda: f"{fragment(bridge,2,5)}?\nREFUSAL\n{keyword_pressure(official)}\n{collapse_to_term(threat)}",
        lambda: f"{fragment(official,2,6).upper()}\n{bureaucratic_melt(splice_halves(official, freedom)).lower()}\n{collapse_to_term(command)}",
        lambda: transmission_break(official, threat, bridge),
        lambda: decree_mode(official, bridge, args.absurd_seriousness),
        lambda: policy_meltdown_mode(official, threat, args.absurd_seriousness),
        lambda: administrative_chant_mode(command, bridge, args.absurd_seriousness),
        lambda: patriotic_absurdity_mode(freedom, command, threat),
        lambda: committee_nightmare_mode(official, freedom, args.absurd_seriousness),
        lambda: public_interest_recursion_mode(official, bridge, args.absurd_seriousness),
    ]
    weights = []
    for idx, _ in enumerate(patterns):
        w = 1.0
        if idx in {6, 10}:
            w += personality_weight(args, personality, "decree")
        if idx in {7, 8, 10, 11}:
            w += personality_weight(args, personality, "escalation")
        if idx in {1, 9}:
            w += personality_weight(args, personality, "contradiction")
        if idx in {7, 10, 11}:
            w += personality_weight(args, personality, "stack")
        if idx in {8, 11}:
            w += personality_weight(args, personality, "chant")
        if idx >= 6:
            w += args.absurd_seriousness * 0.95
        weights.append(max(0.1, w))
    return random.choices(patterns, weights=weights, k=1)[0]()


def build_slogan(top300: List[Line], full: List[Line], args: argparse.Namespace, personality: str) -> str:
    used: set[str] = set()
    a = choose_line(top300, args.text_chaos, excluded_texts=used)
    used.add(a.text)
    b = choose_line(full, args.text_chaos, excluded_texts=used)
    used.add(b.text)
    c = choose_line(top300, args.text_chaos, excluded_texts=used)
    d = choose_line(full, args.text_chaos, excluded_texts=used)

    ops = [
        lambda: braid_fragments(a.text, b.text, c.text),
        lambda: interrupt_with(splice_halves(a.text, b.text), c.text),
        lambda: restart_with_drift(a.text),
        lambda: mirrored_contradiction(a.text, b.text),
        lambda: f"{echo_decay(a.text)}\n{phrase_decay(d.text)}",
        lambda: f"{ladder_phrase(c.text)}\n{keyword_pressure(b.text)}",
        lambda: f"{false_restart(splice_halves(a.text, c.text))}\n{glitch_gap(b.text)}",
        lambda: transmission_break(a.text, c.text, d.text),
        lambda: decree_mode(a.text, b.text, args.absurd_seriousness),
        lambda: policy_meltdown_mode(c.text, d.text, args.absurd_seriousness),
        lambda: administrative_chant_mode(a.text, d.text, args.absurd_seriousness),
        lambda: patriotic_absurdity_mode(a.text, b.text, c.text),
        lambda: committee_nightmare_mode(a.text, c.text, args.absurd_seriousness),
        lambda: public_interest_recursion_mode(b.text, d.text, args.absurd_seriousness),
        lambda: contradictory_mission_statement(a.text, b.text, d.text),
        lambda: noun_pressure(a.text, c.text, args.absurd_seriousness),
    ]
    if random.random() < args.stutter_prob:
        ops.append(lambda: f"{stutter_phrase(a.text)}\n{recursive_burst(c.text)}\n{glitch_gap(d.text)}")
    if random.random() < args.rupture_prob:
        ops.append(lambda: f"{splice_halves(a.text, c.text).upper()}\n/// SIGNAL CUT ///\n{collapse_to_term(b.text)}")

    weights = []
    for idx, _ in enumerate(ops):
        w = 0.85
        if idx in {5, 10, 13}:
            w += personality_weight(args, personality, "chant")
        if idx in {8, 12}:
            w += personality_weight(args, personality, "decree")
        if idx in {9, 10, 12, 15}:
            w += personality_weight(args, personality, "escalation")
        if idx in {3, 11, 14}:
            w += personality_weight(args, personality, "contradiction")
        if idx in {9, 12, 13, 15}:
            w += personality_weight(args, personality, "stack")
        if idx >= 8:
            w += args.absurd_seriousness * 1.05
        weights.append(max(0.1, w))

    out = random.choices(ops, weights=weights, k=1)[0]()
    words = cut_words(out)
    if len(words) > args.max_words_slogan * 2:
        trimmed = words[: args.max_words_slogan * 2]
        pivot = random.randint(max(1, len(trimmed) // 3), len(trimmed))
        out = " ".join(trimmed[:pivot])
    return out.strip()


def build_broadcast(top300: List[Line], full: List[Line], args: argparse.Namespace, personality: str) -> str:
    used: set[str] = set()
    official = choose_line(top300, args.text_chaos, ["official"], used, True)
    used.add(official.text)
    threat = choose_line(top300, args.text_chaos, ["threat"], used, True)
    used.add(threat.text)
    freedom = choose_line(top300, args.text_chaos, ["freedom"], used, True)
    used.add(freedom.text)
    command = choose_line(top300, args.text_chaos, ["command"], used, True)
    bridge = choose_line(full, args.text_chaos, excluded_texts=used)
    return rhetorical_pattern(official.text, threat.text, freedom.text, command.text, bridge.text, args, personality)


def build_chant_cell(top300: List[Line], full: List[Line], args: argparse.Namespace, personality: str) -> Dict[str, str]:
    use_full = random.random() < 0.3
    line = choose_line(full if use_full else top300, args.text_chaos)
    partner = choose_line(top300 if use_full else full, args.text_chaos)
    anchor = choose_line(top300, args.text_chaos)
    mode = random.choice([
        "chant", "loop", "burst", "call", "splice", "stutter", "echo_decay", "ladder", "triplet", "pulse_break", "collapse",
        "decree_mode", "policy_meltdown_mode", "administrative_chant_mode", "patriotic_absurdity_mode", "committee_nightmare_mode", "public_interest_recursion_mode",
    ])

    if mode == "chant":
        text, delivery = compress_phrase(line.text, args.max_words_slogan), "shouted"
    elif mode == "loop":
        text, delivery = keyword_pressure(line.text), "hard repeat"
    elif mode == "burst":
        text, delivery = fragment(line.text, 1, 4).upper(), "short burst"
    elif mode == "call":
        text, delivery = f"{fragment(line.text,2,4).upper()}\n{fragment(partner.text,1,4).lower()}", "call-response"
    elif mode == "splice":
        text, delivery = splice_halves(line.text, partner.text).upper(), "cut splice"
    elif mode == "echo_decay":
        text, delivery = echo_decay(line.text), "decay chant"
    elif mode == "ladder":
        text, delivery = ladder_phrase(line.text), "escalation"
    elif mode == "triplet":
        hit = fragment(line.text, 1, 2).upper()
        text, delivery = f"{hit} / {hit} / {hit}\n{fragment(partner.text,1,3).lower()}", "triplet cell"
    elif mode == "pulse_break":
        text, delivery = f"{keyword_pressure(line.text)}\n--\n{glitch_gap(partner.text)}", "pulse break"
    elif mode == "collapse":
        text, delivery = collapse_to_term(line.text), "collapse loop"
    elif mode == "decree_mode":
        text, delivery = decree_mode(line.text, partner.text, args.absurd_seriousness), "decree recital"
    elif mode == "policy_meltdown_mode":
        text, delivery = policy_meltdown_mode(line.text, partner.text, args.absurd_seriousness), "policy meltdown"
    elif mode == "administrative_chant_mode":
        text, delivery = administrative_chant_mode(line.text, partner.text, args.absurd_seriousness), "administrative chant"
    elif mode == "patriotic_absurdity_mode":
        text, delivery = patriotic_absurdity_mode(line.text, partner.text, anchor.text), "false patriotic"
    elif mode == "committee_nightmare_mode":
        text, delivery = committee_nightmare_mode(line.text, partner.text, args.absurd_seriousness), "committee nightmare"
    else:
        text, delivery = public_interest_recursion_mode(line.text, partner.text, args.absurd_seriousness), "public-interest recursion"

    return {
        "mode": mode,
        "text": text,
        "delivery": delivery,
        "source_bank": line.source_bank,
        "file": line.file,
        "clip_id": line.clip_id,
        "cue_index": str(line.cue_index),
        "start_tc": line.start_tc,
        "end_tc": line.end_tc,
        "personality": personality,
    }


def run_agitprop_mode(
    args: argparse.Namespace,
    output_root: Path,
    summary: RunSummary,
    live: Optional[LiveControlState] = None,
    progress: Optional[ProgressReporter] = None,
    progress_span: Tuple[float, float] = (0.0, 1.0),
) -> Path:
    if progress:
        progress.update_span(progress_span, 0.0, "agitprop", "loading line banks", force=True)
    top300_path, full_path = Path(args.top300_csv).expanduser().resolve(), Path(args.full_csv).expanduser().resolve()
    if not top300_path.exists() or not full_path.exists():
        raise SystemExit("Missing --top300-csv or --full-csv input file.")
    if not top300_path.is_file() or not full_path.is_file():
        raise SystemExit("--top300-csv and --full-csv must be file paths.")

    top300, top_stats = load_line_bank(top300_path, "top300")
    full, full_stats = load_line_bank(full_path, "full")
    summary.top300_loaded, summary.full_loaded = top_stats.loaded, full_stats.loaded
    summary.top300_skipped = top_stats.skipped_empty + top_stats.skipped_unusable
    summary.full_skipped = full_stats.skipped_empty + full_stats.skipped_unusable

    if not top300 or not full:
        raise SystemExit("CSV banks loaded no usable lines.")
    if progress:
        progress.update_span(progress_span, 0.15, "agitprop", "building slogans", force=True)

    agit_out = output_root / "agitprop"
    agit_out.mkdir(parents=True, exist_ok=True)

    personality = resolve_personality(args)
    slogan_count = max(1, args.agitprop_count)
    broadcast_count = max(1, args.broadcast_count)
    chant_count = max(1, args.chant_count)
    slogans = []
    for idx in range(slogan_count):
        slogans.append(build_slogan(top300, full, args, personality))
        if progress and (idx == 0 or idx + 1 == slogan_count or idx % 10 == 0):
            progress.update_span(progress_span, 0.15 + 0.25 * ((idx + 1) / slogan_count), "agitprop", f"slogans {idx + 1}/{slogan_count}")
    broadcasts = []
    for idx in range(broadcast_count):
        broadcasts.append(build_broadcast(top300, full, args, personality))
        if progress and (idx == 0 or idx + 1 == broadcast_count or idx % 5 == 0):
            progress.update_span(progress_span, 0.40 + 0.20 * ((idx + 1) / broadcast_count), "agitprop", f"broadcasts {idx + 1}/{broadcast_count}")
    chant_cells = []
    for idx in range(chant_count):
        chant_cells.append(build_chant_cell(top300, full, args, personality))
        if progress and (idx == 0 or idx + 1 == chant_count or idx % 20 == 0):
            progress.update_span(progress_span, 0.60 + 0.30 * ((idx + 1) / chant_count), "agitprop", f"chant cells {idx + 1}/{chant_count}")
    if progress:
        progress.update_span(progress_span, 0.92, "agitprop", "writing files", force=True)

    (agit_out / "slogans.txt").write_text("\n\n".join(s.strip() for s in slogans) + "\n", encoding="utf-8")
    (agit_out / "broadcasts.txt").write_text("\n\n".join(s.strip() for s in broadcasts) + "\n", encoding="utf-8")

    chant_path = agit_out / "chant_cells.csv"
    with chant_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["mode", "text", "delivery", "source_bank", "file", "clip_id", "cue_index", "start_tc", "end_tc", "personality"])
        writer.writeheader()
        writer.writerows(chant_cells)

    summary.slogans, summary.broadcasts, summary.chants = len(slogans), len(broadcasts), len(chant_cells)
    summary.output_paths.extend([str(agit_out / "slogans.txt"), str(agit_out / "broadcasts.txt"), str(chant_path)])
    if progress:
        progress.update_span(progress_span, 1.0, "agitprop", "complete", force=True)
    return chant_path


# -------------------------------------------------------------------
# CUTTARGETS / SOURCE MATCHING
# -------------------------------------------------------------------


def load_source_rows(path: Path, bank_name: str) -> List[SourceRow]:
    rows, _ = load_line_bank(path, bank_name)
    return [
        SourceRow(
            text=r.text,
            file=r.file,
            clip_id=r.clip_id,
            cue_index=str(r.cue_index),
            start_tc=r.start_tc,
            end_tc=r.end_tc,
            duration_sec=str(r.duration_sec),
            source_bank=bank_name,
        )
        for r in rows
    ]


def load_chant_cells(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise SystemExit(f"Chant cells CSV '{path}' has no header row.")
        return list(reader)


def overlap_score(a: str, b: str) -> float:
    at, bt = set(token_list(a)), set(token_list(b))
    if not at or not bt:
        return 0.0
    return len(at & bt) / max(1, len(at | bt))


def compressed_overlap(a: str, b: str) -> float:
    ac = " ".join(token_list(a)[:8])
    bc = " ".join(token_list(b)[:8])
    if not ac or not bc:
        return 0.0
    return 1.0 if ac in bc or bc in ac else 0.0


def longest_token_match(a: str, b: str) -> float:
    bt = set(token_list(b))
    longest = max(token_list(a), key=len, default="")
    return 1.0 if longest and longest in bt else 0.0


def keyword_weight_boost(query: str) -> float:
    score = 0.0
    q = normalize_text(query)
    for kw, w in KEYWORD_WEIGHTS.items():
        if kw in q:
            score += w
    return score / 8.0


def score_match(query: str, row: SourceRow) -> Tuple[float, str]:
    qn, rn = normalize_text(query), normalize_text(row.text)
    ov = overlap_score(qn, rn)
    comp = compressed_overlap(qn, rn)
    long_m = longest_token_match(qn, rn)
    contain = 0.35 if (qn in rn or rn in qn) else 0.0
    kboost = keyword_weight_boost(query)
    top_bias = 0.15 if row.source_bank == "top300" else 0.0
    total = ov + comp * 0.6 + long_m * 0.35 + contain + kboost + top_bias
    method = f"token:{ov:.2f}|contain:{contain:.2f}|compress:{comp:.2f}|long:{long_m:.2f}"
    return total, method


def best_matches(query: str, source_rows: List[SourceRow], top_n: int) -> List[Tuple[float, str, SourceRow]]:
    scored: List[Tuple[float, str, SourceRow]] = []
    for row in source_rows:
        score, method = score_match(query, row)
        if score > 0:
            scored.append((score, method, row))
    scored.sort(key=lambda x: x[0], reverse=True)
    return scored[: max(1, top_n)]


def run_cuttargets_mode(
    args: argparse.Namespace,
    output_root: Path,
    summary: RunSummary,
    chant_cells_path: Optional[Path] = None,
    progress: Optional[ProgressReporter] = None,
    progress_span: Tuple[float, float] = (0.0, 1.0),
) -> Path:
    if progress:
        progress.update_span(progress_span, 0.0, "cuttargets", "loading CSV inputs", force=True)
    top300_path, full_path = Path(args.top300_csv).expanduser().resolve(), Path(args.full_csv).expanduser().resolve()
    chant_path = chant_cells_path or (Path(args.chant_cells_csv).expanduser().resolve() if args.chant_cells_csv else (output_root / "agitprop" / "chant_cells.csv").resolve())
    if not top300_path.exists() or not full_path.exists() or not chant_path.exists():
        raise SystemExit("Missing CSV inputs for cuttargets mode.")
    if not top300_path.is_file() or not full_path.is_file() or not chant_path.is_file():
        raise SystemExit("CSV input paths for cuttargets must be files.")

    all_rows = load_source_rows(top300_path, "top300") + load_source_rows(full_path, "full")
    if not all_rows:
        raise SystemExit("No usable source rows found in top300/full CSV inputs.")
    chant_cells = load_chant_cells(chant_path)
    if progress:
        progress.update_span(progress_span, 0.25, "cuttargets", f"matching {len(chant_cells)} chant cells", force=True)

    out_path = output_root / "agitprop" / "cut_targets.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    out_rows: List[Dict[str, str]] = []
    for i, cell in enumerate(chant_cells, start=1):
        text = clean_text(str(cell.get("text", "")).replace("\n", " "))
        mode = str(cell.get("mode", ""))
        delivery = str(cell.get("delivery", ""))
        query_norm = normalize_text(text)
        matches = best_matches(text, all_rows, args.cut_match_count)
        if not matches:
            out_rows.append({"cell_index": str(i), "mode": mode, "delivery": delivery, "generated_text": text, "normalized_query": query_norm, "match_rank": "", "match_score": "", "recommended": "", "match_method": "none", "source_bank": "", "file": "", "clip_id": "", "cue_index": "", "start_tc": "", "end_tc": "", "duration_sec": "", "source_text": ""})
            if progress and (i == 1 or i == len(chant_cells) or i % 25 == 0):
                progress.update_span(progress_span, 0.25 + 0.60 * (i / max(1, len(chant_cells))), "cuttargets", f"matched {i}/{len(chant_cells)}")
            continue
        for rank, (score, method, row) in enumerate(matches, start=1):
            out_rows.append({
                "cell_index": str(i),
                "mode": mode,
                "delivery": delivery,
                "generated_text": text,
                "normalized_query": query_norm,
                "match_rank": str(rank),
                "match_score": f"{score:.3f}",
                "recommended": "true" if rank == 1 else "false",
                "match_method": method,
                "source_bank": row.source_bank,
                "file": row.file,
                "clip_id": row.clip_id,
                "cue_index": row.cue_index,
                "start_tc": row.start_tc,
                "end_tc": row.end_tc,
                "duration_sec": row.duration_sec,
                "source_text": row.text,
            })
        if progress and (i == 1 or i == len(chant_cells) or i % 25 == 0):
            progress.update_span(progress_span, 0.25 + 0.60 * (i / max(1, len(chant_cells))), "cuttargets", f"matched {i}/{len(chant_cells)}")

    if progress:
        progress.update_span(progress_span, 0.9, "cuttargets", "writing cut_targets.csv", force=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["cell_index", "mode", "delivery", "generated_text", "normalized_query", "match_rank", "match_score", "recommended", "match_method", "source_bank", "file", "clip_id", "cue_index", "start_tc", "end_tc", "duration_sec", "source_text"])
        writer.writeheader()
        writer.writerows(out_rows)

    summary.cut_matches = len([r for r in out_rows if r["match_rank"]])
    summary.output_paths.append(str(out_path))
    if progress:
        progress.update_span(progress_span, 1.0, "cuttargets", "complete", force=True)
    return out_path


# -------------------------------------------------------------------
# AUDIO DISCOVERY / SELECTION + TRANSFORM + ARRANGEMENT
# -------------------------------------------------------------------


def sample_from_analysis_cache_entry(entry: Dict[str, object], sample_rate: int) -> Optional[SampleFile]:
    path_text = str(entry.get("path", "") or "")
    if not path_text:
        return None
    path = Path(path_text).expanduser().resolve()
    if not path.exists() or not path.is_file() or path.suffix.lower() not in AUDIO_EXTS:
        return None
    if safe_int(str(entry.get("analysis_sample_rate", sample_rate)), sample_rate) != int(sample_rate):
        return None
    if safe_int(str(entry.get("cue_start_ms", 0)), 0) != 0:
        return None
    if safe_int(str(entry.get("cue_end_ms", 0)), 0) != 0:
        return None
    if safe_int(str(entry.get("cue_index", 0)), 0) != 0:
        return None
    size_bytes, mtime = audio_file_stat(path)
    if safe_int(str(entry.get("file_size_bytes", -1)), -1) != size_bytes:
        return None
    if safe_int(str(entry.get("file_mtime", -1)), -1) != mtime:
        return None
    duration_ms = safe_int(str(entry.get("duration_ms", 0)), 0)
    if duration_ms <= 1:
        return None
    return SampleFile(
        path=path,
        duration_ms=duration_ms,
        words=max(0, safe_int(str(entry.get("words", 0)), 0)),
        intensity_hint=max(0, safe_int(str(entry.get("intensity_hint", 0)), 0)),
        loop_hint=max(0, safe_int(str(entry.get("loop_hint", 0)), 0)),
        manifest_tags=str(entry.get("manifest_tags", "") or ""),
        manifest_role=str(entry.get("manifest_role", "") or ""),
        manifest_weight=clamp(safe_float(str(entry.get("manifest_weight", 1.0)), 1.0), 0.05, 20.0),
    )


def cached_samples_by_path(payload: Dict[str, object], sample_rate: int) -> Dict[Path, SampleFile]:
    out: Dict[Path, SampleFile] = {}
    for entry in cached_analysis_entries(payload).values():
        sample = sample_from_analysis_cache_entry(entry, sample_rate)
        if sample is not None:
            out[sample.path.resolve()] = sample
    return out


def discover_samples(
    root: Path,
    exclude_paths: Optional[Iterable[Path]] = None,
    analysis_cache_payload: Optional[Dict[str, object]] = None,
    sample_rate: int = 44100,
) -> Tuple[List[SampleFile], int]:
    ensure_audio_backend()
    samples: List[SampleFile] = []
    unreadable = 0
    cached = cached_samples_by_path(analysis_cache_payload or {}, sample_rate)
    for path in candidate_audio_paths(root, exclude_paths=exclude_paths):
        cached_sample = cached.get(path.resolve())
        if cached_sample is not None:
            samples.append(cached_sample)
            continue
        try:
            audio = AudioSegment.from_file(path)
        except Exception:
            unreadable += 1
            continue
        if len(audio) <= 1:
            continue
        stem = path.stem.lower().replace("_", " ")
        words = len(TOKEN_RE.findall(stem))
        low = str(path).lower()
        intensity = sum(1 for k in ["threat", "warning", "command", "official", "censor", "collapse"] if k in low)
        loop_hint = 3 if "micro" in low else 2 if "short" in low else 1 if "phrase" in low else 0
        samples.append(SampleFile(path=path, duration_ms=len(audio), words=words, intensity_hint=intensity, loop_hint=loop_hint))
    return samples, unreadable


def discover_cue_samples(input_root: Path, cue_file: Path, exclude_paths: Optional[Iterable[Path]] = None) -> Tuple[List[SampleFile], int]:
    ensure_audio_backend()
    if not cue_file.exists():
        raise SystemExit(f"Cue file not found: {cue_file}")
    if not cue_file.is_file():
        raise SystemExit(f"--cue-file must point to an .srt or .csv file: {cue_file}")

    excluded = resolved_excluded_paths(exclude_paths)
    default_audio: Optional[Path] = input_root if input_root.is_file() and input_root.suffix.lower() in AUDIO_EXTS and not path_is_excluded(input_root, excluded) else None
    audio_candidates = candidate_audio_paths(input_root, exclude_paths=exclude_paths)
    if default_audio is None and len(audio_candidates) == 1:
        default_audio = audio_candidates[0]

    duration_cache: Dict[Path, int] = {}
    samples: List[SampleFile] = []
    skipped = 0
    for row in load_cue_rows(cue_file):
        audio_path = resolve_cue_audio_path(str(row.get("file", "")), input_root, cue_file, default_audio, exclude_paths=exclude_paths)
        if audio_path is None:
            skipped += 1
            continue
        if audio_path not in duration_cache:
            try:
                duration_cache[audio_path] = len(AudioSegment.from_file(audio_path))
            except Exception:
                skipped += 1
                continue
        audio_duration = duration_cache[audio_path]
        start_ms = int(row.get("start_ms", 0))
        end_ms = int(row.get("end_ms", 0))
        start_ms = int(clamp(start_ms, 0, max(0, audio_duration - 1)))
        end_ms = int(clamp(end_ms, start_ms + 1, audio_duration))
        if end_ms <= start_ms:
            skipped += 1
            continue
        cue_text = str(row.get("text", "") or "")
        duration_ms = end_ms - start_ms
        words = max(1, count_words(cue_text) or len(TOKEN_RE.findall(audio_path.stem)))
        low = f"{audio_path} {cue_text}".lower()
        intensity = sum(1 for k in ["threat", "warning", "command", "official", "censor", "collapse"] if k in low)
        samples.append(
            SampleFile(
                path=audio_path,
                duration_ms=duration_ms,
                words=words,
                intensity_hint=intensity,
                loop_hint=cue_loop_hint(duration_ms),
                cue_start_ms=start_ms,
                cue_end_ms=end_ms,
                cue_text=cue_text,
                cue_index=int(row.get("cue_index", 0) or 0),
            )
        )
    return samples, skipped


def relative_dataset_path(path: Path, root: Path) -> str:
    base = root if root.is_dir() else root.parent
    try:
        return str(path.resolve().relative_to(base.resolve()))
    except ValueError:
        return path.name


def dataset_loop_hint(path: Path, duration_ms: int, text: str) -> int:
    if keyword_hits(text, BEAT_SOURCE_KEYWORDS) > 0:
        return 3
    duration_s = duration_ms / 1000.0
    if 1.0 <= duration_s <= 16.0:
        common_loop_lengths = (1.0, 2.0, 4.0, 8.0, 16.0)
        if any(abs(duration_s - length) <= 0.08 for length in common_loop_lengths):
            return 2
    if any(token in path.stem.lower() for token in ("loop", "bar", "bpm", "groove")):
        return 2
    return 0


def classify_dataset_source(path: Path, duration_ms: int, words: int, dbfs: Optional[float], zero_crossing: float) -> Dict[str, object]:
    text = f"{path.stem} {path.parent.name}".lower().replace("_", " ")
    spoken_hits = keyword_hits(text, SPOKEN_SOURCE_KEYWORDS)
    beat_hits = keyword_hits(text, BEAT_SOURCE_KEYWORDS)
    breach_hits = keyword_hits(text, BREACH_SOURCE_KEYWORDS)
    loop_hint = dataset_loop_hint(path, duration_ms, text)
    loudness = -90.0 if dbfs is None else float(dbfs)
    duration_s = duration_ms / 1000.0

    if breach_hits >= max(spoken_hits, beat_hits, 1) or (zero_crossing >= 0.22 and duration_s <= 8.0):
        role = "breach"
    elif beat_hits > 0 or loop_hint >= 2:
        role = "beat"
    elif spoken_hits > 0 or (words >= 2 and duration_s >= 0.7):
        role = "spoken"
    else:
        role = "texture"

    tags = {role}
    for keyword, target in (
        ("voice", "voice"),
        ("speech", "voice"),
        ("phrase", "phrase"),
        ("loop", "loop"),
        ("drum", "drum"),
        ("beat", "beat"),
        ("noise", "noise"),
        ("static", "static"),
        ("radio", "radio"),
        ("dropout", "dropout"),
        ("glitch", "glitch"),
    ):
        if keyword in text:
            tags.add(target)
    if duration_s <= 0.5:
        tags.add("micro")
    elif duration_s <= 2.0:
        tags.add("short")
    elif duration_s >= 12.0:
        tags.add("long")
    if loudness > -18:
        tags.add("hot")
    if zero_crossing >= 0.18:
        tags.add("noisy")

    intensity = 0
    intensity += min(3, breach_hits)
    if loudness > -18:
        intensity += 1
    if zero_crossing >= 0.18:
        intensity += 1
    intensity = int(clamp(intensity, 0, 5))

    if role == "spoken":
        preset = "spoken-word-cutup"
        flags = "--preset spoken-word-cutup --source-score spoken"
        notes = "Add --cue-file or local transcription/alignment for phrase-accurate cuts."
        weight = 1.35 if spoken_hits else 1.0
    elif role == "beat":
        preset = "beat-cutup"
        flags = "--preset beat-cutup --source-score beat --bpm <tempo> --slice-grid 1/16"
        notes = "Set --bpm manually unless this file is used as --baseline-beat with --baseline-beat-bars."
        weight = 1.25 + min(0.5, loop_hint * 0.1)
    elif role == "breach":
        preset = "signal-breach"
        flags = "--preset signal-breach --source-score breach"
        notes = "Useful for bursts, dropouts, scanline textures, and radio interruptions."
        weight = 1.35 + min(0.4, intensity * 0.08)
    else:
        preset = "ghost-transmission"
        flags = "--preset ghost-transmission"
        notes = "Unlabeled texture; add role/tags in the manifest if this should drive a specific workflow."
        weight = 0.8

    return {
        "role": role,
        "tags": ",".join(sorted(tags)),
        "intensity": intensity,
        "loop_hint": loop_hint,
        "words": max(0, words),
        "weight": round(float(weight), 3),
        "recommended_preset": preset,
        "recommended_flags": flags,
        "notes": notes,
    }


def scan_dataset(root: Path, sample_rate: int = 44100, max_files: int = 0) -> Tuple[List[Dict[str, object]], int]:
    ensure_audio_backend()
    root = root.expanduser().resolve()
    if not root.exists():
        raise SystemExit(f"--scan-dataset path not found: {root}")
    candidates = candidate_audio_paths(root)
    if max_files > 0:
        candidates = candidates[:max_files]
    rows: List[Dict[str, object]] = []
    unreadable = 0
    for path in candidates:
        try:
            audio = AudioSegment.from_file(path).set_frame_rate(sample_rate).set_channels(2)
        except Exception:
            unreadable += 1
            continue
        duration_ms = len(audio)
        if duration_ms <= 1:
            unreadable += 1
            continue
        words = len(TOKEN_RE.findall(path.stem.replace("_", " ")))
        dbfs = finite_audio_float(audio.dBFS)
        zcr = zero_crossing_rate(audio)
        classification = classify_dataset_source(path, duration_ms, words, dbfs, zcr)
        row = {
            "file": relative_dataset_path(path, root),
            **classification,
            "duration_ms": duration_ms,
            "duration_sec": round(duration_ms / 1000.0, 3),
            "dbfs": dbfs,
            "zero_crossing_rate": zcr,
        }
        rows.append(row)
    return rows, unreadable


def dataset_report(root: Path, rows: List[Dict[str, object]], unreadable: int) -> Dict[str, object]:
    role_counts = Counter(str(row.get("role", "")) for row in rows)
    preset_counts = Counter(str(row.get("recommended_preset", "")) for row in rows)
    tag_counts: Counter = Counter()
    durations = [safe_float(str(row.get("duration_sec", 0.0)), 0.0) for row in rows]
    for row in rows:
        tag_counts.update(split_manifest_tags(row.get("tags", "")))

    recommendations: List[str] = []
    if role_counts.get("spoken", 0):
        recommendations.append("Use --cue-file or local transcription/alignment for phrase-aware spoken-word renders.")
    if role_counts.get("beat", 0):
        recommendations.append("Use --bpm and --slice-grid, or pass a known loop with --baseline-beat and --baseline-beat-bars.")
    if role_counts.get("breach", 0):
        recommendations.append("Use signal-breach or radio-intrusion presets with --source-score breach.")
    if len(rows) >= 8:
        recommendations.append("Use --source-diversity 0.5..0.8 when the render collapses onto one source.")

    return {
        "kind": "cutups.dataset_report",
        "version": 1,
        "input": str(root.expanduser().resolve()),
        "audio_files": len(rows),
        "unreadable": unreadable,
        "duration_sec_total": round(sum(durations), 3),
        "duration_sec_min": round(min(durations), 3) if durations else 0.0,
        "duration_sec_max": round(max(durations), 3) if durations else 0.0,
        "role_counts": sorted_count_map(role_counts),
        "recommended_preset_counts": sorted_count_map(preset_counts),
        "top_tags": top_count_rows(tag_counts, limit=12),
        "recommendations": recommendations,
        "sources": rows,
    }


def write_dataset_manifest(path: Path, rows: List[Dict[str, object]], overwrite: bool = False) -> None:
    if path.exists() and not overwrite:
        raise SystemExit(f"--write-source-manifest already exists: {path}. Pass --overwrite to replace it.")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=DATASET_MANIFEST_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_dataset_report(path: Path, payload: Dict[str, object], overwrite: bool = False) -> None:
    if path.exists() and not overwrite:
        raise SystemExit(f"--write-dataset-report already exists: {path}. Pass --overwrite to replace it.")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def print_dataset_scan_summary(report: Dict[str, object], manifest_path: Optional[Path], report_path: Optional[Path]) -> None:
    print("DATASET SCAN")
    print(f"input: {report['input']}")
    print(f"audio_files: {report['audio_files']}")
    print(f"unreadable: {report['unreadable']}")
    print(f"duration_sec_total: {report['duration_sec_total']}")
    print(f"role_counts: {report['role_counts']}")
    print(f"recommended_preset_counts: {report['recommended_preset_counts']}")
    if manifest_path:
        print(f"source_manifest: {manifest_path}")
    if report_path:
        print(f"dataset_report: {report_path}")
    recommendations = report.get("recommendations", [])
    if isinstance(recommendations, list) and recommendations:
        print("recommendations:")
        for item in recommendations:
            print(f" - {item}")


def run_dataset_scan_cli(args: argparse.Namespace) -> None:
    if args.dataset_max_files < 0:
        raise SystemExit("--dataset-max-files must be >= 0")
    root = Path(args.scan_dataset).expanduser().resolve()
    rows, unreadable = scan_dataset(root, sample_rate=max(8000, int(args.sample_rate)), max_files=int(args.dataset_max_files))
    report = dataset_report(root, rows, unreadable)
    manifest_path = Path(args.write_source_manifest).expanduser().resolve() if args.write_source_manifest else None
    report_path = Path(args.write_dataset_report).expanduser().resolve() if args.write_dataset_report else None
    if manifest_path:
        write_dataset_manifest(manifest_path, rows, overwrite=bool(args.overwrite))
    if report_path:
        write_dataset_report(report_path, report, overwrite=bool(args.overwrite))
    print_dataset_scan_summary(report, manifest_path, report_path)


def finite_audio_float(value: float) -> Optional[float]:
    number = float(value)
    if not math.isfinite(number):
        return None
    return round(number, 3)


def audio_file_stat(path: Path) -> Tuple[int, int]:
    try:
        stat = path.stat()
    except OSError:
        return 0, 0
    return stat.st_size, int(stat.st_mtime)


def analysis_cache_key(path: str, cue_start_ms: int, cue_end_ms: int, cue_index: int, sample_rate: int) -> str:
    resolved = Path(path).expanduser().resolve()
    return f"{resolved}|cue={cue_start_ms}:{cue_end_ms}:{cue_index}|sr={sample_rate}"


def analysis_cache_key_for_sample(sample: SampleFile, args: argparse.Namespace) -> str:
    return analysis_cache_key(str(sample.path), sample.cue_start_ms, sample.cue_end_ms, sample.cue_index, int(args.sample_rate))


def analysis_cache_key_for_entry(entry: Dict[str, object], default_sample_rate: int) -> str:
    sample_rate = safe_int(str(entry.get("analysis_sample_rate", default_sample_rate)), default_sample_rate)
    return analysis_cache_key(
        str(entry.get("path", "")),
        safe_int(str(entry.get("cue_start_ms", 0)), 0),
        safe_int(str(entry.get("cue_end_ms", 0)), 0),
        safe_int(str(entry.get("cue_index", 0)), 0),
        sample_rate,
    )


def load_analysis_cache(path: Path) -> Dict[str, object]:
    if not path.exists() or path.is_dir():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, dict):
        return {}
    if payload.get("kind") != "cutups.audio_analysis_cache":
        return {}
    if safe_int(str(payload.get("version", 0)), 0) > ANALYSIS_CACHE_VERSION:
        return {}
    return payload


def cached_analysis_entries(payload: Dict[str, object]) -> Dict[str, Dict[str, object]]:
    default_sample_rate = safe_int(str(payload.get("sample_rate", 0)), 0)
    rows = payload.get("samples", [])
    if not isinstance(rows, list):
        return {}
    entries: Dict[str, Dict[str, object]] = {}
    for raw in rows:
        if not isinstance(raw, dict) or not raw.get("path"):
            continue
        key = str(raw.get("cache_key") or analysis_cache_key_for_entry(raw, default_sample_rate))
        entries[key] = raw
    return entries


def cached_entry_matches_sample(entry: Dict[str, object], sample: SampleFile, args: argparse.Namespace) -> bool:
    if str(entry.get("cache_key") or analysis_cache_key_for_entry(entry, int(args.sample_rate))) != analysis_cache_key_for_sample(sample, args):
        return False
    if any(key not in entry for key in ANALYSIS_CACHE_REQUIRED_SAMPLE_KEYS):
        return False
    size_bytes, mtime = audio_file_stat(sample.path)
    return (
        safe_int(str(entry.get("file_size_bytes", -1)), -1) == size_bytes
        and safe_int(str(entry.get("file_mtime", -1)), -1) == mtime
        and safe_int(str(entry.get("cue_start_ms", -1)), -1) == sample.cue_start_ms
        and safe_int(str(entry.get("cue_end_ms", -1)), -1) == sample.cue_end_ms
        and safe_int(str(entry.get("cue_index", -1)), -1) == sample.cue_index
        and safe_int(str(entry.get("analysis_sample_rate", args.sample_rate)), int(args.sample_rate)) == int(args.sample_rate)
    )


def zero_crossing_rate(audio: Any, max_frames: int = 200000) -> float:
    channels = max(1, int(getattr(audio, "channels", 1) or 1))
    samples = audio.get_array_of_samples()
    frame_count = len(samples) // channels
    if frame_count < 2:
        return 0.0
    stride = max(1, int(math.ceil(frame_count / max(1, max_frames))))

    last_sign = 0
    crossings = 0
    observed = 0
    for frame_idx in range(0, frame_count, stride):
        offset = frame_idx * channels
        mixed = sum(int(samples[offset + channel]) for channel in range(channels))
        sign = 1 if mixed > 0 else -1 if mixed < 0 else 0
        if sign == 0:
            continue
        if last_sign and sign != last_sign:
            crossings += 1
        last_sign = sign
        observed += 1
    if observed < 2:
        return 0.0
    return round(crossings / float(observed - 1), 6)


def grid_cell_summary(audio: Any, grid_ms: int, max_cells: int = ANALYSIS_GRID_CELL_MAX_CELLS) -> Dict[str, object]:
    if grid_ms <= 0 or len(audio) <= 0:
        return {"grid_ms": 0, "cell_count": 0, "captured": 0, "truncated": False, "cells": []}

    total_cells = int(math.ceil(len(audio) / float(grid_ms)))
    capture_count = min(total_cells, max(0, max_cells))
    cells: List[Dict[str, object]] = []
    for idx in range(capture_count):
        start_ms = idx * grid_ms
        end_ms = min(len(audio), start_ms + grid_ms)
        if end_ms <= start_ms:
            continue
        cell = audio[start_ms:end_ms]
        cells.append(
            {
                "index": idx,
                "start_ms": start_ms,
                "end_ms": end_ms,
                "duration_ms": end_ms - start_ms,
                "rms": int(cell.rms),
                "dbfs": finite_audio_float(cell.dBFS),
                "zero_crossing_rate": zero_crossing_rate(cell),
            }
        )
    return {
        "grid_ms": grid_ms,
        "cell_count": total_cells,
        "captured": len(cells),
        "truncated": total_cells > len(cells),
        "cells": cells,
    }


def normalize_dbfs(value: object) -> float:
    dbfs = safe_float(str(value), -90.0)
    if not math.isfinite(dbfs):
        return 0.0
    return round(max(0.0, min(1.0, (dbfs + 60.0) / 60.0)), 6)


def normalized_duration(duration_ms: object, max_seconds: float = 30.0) -> float:
    seconds = max(0.0, safe_float(str(duration_ms), 0.0) / 1000.0)
    return round(max(0.0, min(1.0, math.log1p(seconds) / math.log1p(max_seconds))), 6)


def mean_and_variation(values: Sequence[float]) -> Tuple[float, float]:
    clean = [float(v) for v in values if math.isfinite(float(v))]
    if not clean:
        return 0.0, 0.0
    mean = sum(clean) / len(clean)
    variation = math.sqrt(sum((value - mean) ** 2 for value in clean) / len(clean))
    return round(max(0.0, min(1.0, mean)), 6), round(max(0.0, min(1.0, variation)), 6)


def similarity_vector_for_entry(entry: Dict[str, object]) -> Dict[str, object]:
    grid_summary = entry.get("grid_cell_summary", {})
    cells = grid_summary.get("cells", []) if isinstance(grid_summary, dict) else []
    if not isinstance(cells, list):
        cells = []
    grid_loudness, grid_loudness_variation = mean_and_variation(
        [normalize_dbfs(cell.get("dbfs")) for cell in cells if isinstance(cell, dict)]
    )
    grid_zcr, grid_zcr_variation = mean_and_variation(
        [max(0.0, min(1.0, safe_float(str(cell.get("zero_crossing_rate", 0.0)), 0.0))) for cell in cells if isinstance(cell, dict)]
    )
    values = [
        normalized_duration(entry.get("duration_ms", 0)),
        normalize_dbfs(entry.get("dbfs")),
        max(0.0, min(1.0, safe_float(str(entry.get("zero_crossing_rate", 0.0)), 0.0))),
        grid_loudness,
        grid_loudness_variation,
        grid_zcr,
        grid_zcr_variation,
    ]
    return {"fields": list(ANALYSIS_SIMILARITY_VECTOR_FIELDS), "values": [round(value, 6) for value in values]}


def similarity_vector_values(entry: Dict[str, object]) -> List[float]:
    raw_vector = entry.get("similarity_vector", {})
    raw_values = raw_vector.get("values", []) if isinstance(raw_vector, dict) else []
    if not isinstance(raw_values, list):
        return []
    values: List[float] = []
    for raw in raw_values:
        value = safe_float(str(raw), 0.0)
        if math.isfinite(value):
            values.append(max(0.0, min(1.0, value)))
    return values


def similarity_distance(left: Dict[str, object], right: Dict[str, object]) -> Optional[float]:
    left_values = similarity_vector_values(left)
    right_values = similarity_vector_values(right)
    width = min(len(left_values), len(right_values))
    if width <= 0:
        return None
    distance = math.sqrt(sum((left_values[idx] - right_values[idx]) ** 2 for idx in range(width)) / width)
    return round(distance, 6)


def build_beat_jump_plan(entries: List[Dict[str, object]], args: argparse.Namespace, top_k: int = 8) -> Dict[str, object]:
    mode = str(getattr(args, "beat_jump_mode", "random") or "random")
    if mode != "similarity":
        return {"mode": mode, "metric": "none", "fields": list(ANALYSIS_SIMILARITY_VECTOR_FIELDS), "sources": []}

    sources: List[Dict[str, object]] = []
    for source in entries:
        neighbors: List[Dict[str, object]] = []
        for target in entries:
            if target is source:
                continue
            distance = similarity_distance(source, target)
            if distance is None:
                continue
            neighbors.append(
                {
                    "target_cache_key": str(target.get("cache_key", "")),
                    "target_index": safe_int(str(target.get("index", 0)), 0),
                    "target_basename": str(target.get("basename", "")),
                    "target_path": str(target.get("path", "")),
                    "distance": distance,
                }
            )
        neighbors.sort(key=lambda item: (float(item["distance"]), str(item["target_basename"])))
        sources.append(
            {
                "source_cache_key": str(source.get("cache_key", "")),
                "source_index": safe_int(str(source.get("index", 0)), 0),
                "source_basename": str(source.get("basename", "")),
                "source_path": str(source.get("path", "")),
                "neighbors": neighbors[: max(0, top_k)],
            }
        )
    return {
        "mode": mode,
        "metric": "normalized_euclidean",
        "fields": list(ANALYSIS_SIMILARITY_VECTOR_FIELDS),
        "neighbor_count": max(0, min(top_k, len(entries) - 1)),
        "sources": sources,
    }


def beat_jump_neighbor_keys(payload: Dict[str, object]) -> Dict[str, List[str]]:
    plan = payload.get("beat_jump_plan", {})
    if not isinstance(plan, dict) or plan.get("mode") != "similarity":
        return {}
    raw_sources = plan.get("sources", [])
    if not isinstance(raw_sources, list):
        return {}

    out: Dict[str, List[str]] = {}
    for raw_source in raw_sources:
        if not isinstance(raw_source, dict):
            continue
        source_key = str(raw_source.get("source_cache_key", ""))
        raw_neighbors = raw_source.get("neighbors", [])
        if not source_key or not isinstance(raw_neighbors, list):
            continue
        keys = [str(neighbor.get("target_cache_key", "")) for neighbor in raw_neighbors if isinstance(neighbor, dict)]
        keys = [key for key in keys if key]
        if keys:
            out[source_key] = keys
    return out


def build_beat_jump_state(samples: List[SampleFile], args: argparse.Namespace, cache_payload: Dict[str, object]) -> BeatJumpState:
    if str(getattr(args, "beat_jump_mode", "random") or "random") != "similarity":
        return BeatJumpState()
    neighbor_keys = beat_jump_neighbor_keys(cache_payload)
    if not neighbor_keys:
        return BeatJumpState()
    samples_by_key = {analysis_cache_key_for_sample(sample, args): sample for sample in samples}
    usable: Dict[str, List[str]] = {}
    for source_key, keys in neighbor_keys.items():
        if source_key not in samples_by_key:
            continue
        filtered = [key for key in keys if key in samples_by_key and key != source_key]
        if filtered:
            usable[source_key] = filtered
    return BeatJumpState(active=bool(usable), neighbor_keys=usable, samples_by_key=samples_by_key)


def analysis_entry_for_sample(sample: SampleFile, args: argparse.Namespace, index: int) -> Dict[str, object]:
    audio = source_audio_for_sample(sample, args)
    size_bytes, mtime = audio_file_stat(sample.path)
    grid_ms = beat_grid_ms(args)
    entry = {
        "index": index,
        "cache_key": analysis_cache_key_for_sample(sample, args),
        "cache_state": "fresh",
        "path": str(sample.path),
        "basename": sample.path.name,
        "suffix": sample.path.suffix.lower(),
        "file_size_bytes": size_bytes,
        "file_mtime": mtime,
        "analysis_sample_rate": args.sample_rate,
        "duration_ms": len(audio),
        "cue_start_ms": sample.cue_start_ms,
        "cue_end_ms": sample.cue_end_ms,
        "cue_index": sample.cue_index,
        "cue_text": sample.cue_text,
        "words": sample.words,
        "intensity_hint": sample.intensity_hint,
        "loop_hint": sample.loop_hint,
        "manifest_tags": sample.manifest_tags,
        "manifest_role": sample.manifest_role,
        "manifest_weight": round(float(sample.manifest_weight), 6),
        "frame_rate": audio.frame_rate,
        "channels": audio.channels,
        "sample_width": audio.sample_width,
        "rms": int(audio.rms),
        "dbfs": finite_audio_float(audio.dBFS),
        "max_dbfs": finite_audio_float(audio.max_dBFS),
        "zero_crossing_rate": zero_crossing_rate(audio),
        "grid_cell_summary": grid_cell_summary(audio, grid_ms),
    }
    entry["similarity_vector"] = similarity_vector_for_entry(entry)
    return entry


def write_analysis_cache(path: Path, samples: List[SampleFile], args: argparse.Namespace, input_root: Path) -> Path:
    if path.exists() and path.is_dir():
        raise SystemExit(f"--analysis-cache path is a directory: {path}")
    if path.exists() and not args.overwrite:
        raise SystemExit(f"--analysis-cache already exists: {path}. Pass --overwrite or choose a new path.")
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise SystemExit(f"Failed to create analysis cache folder '{path.parent}': {exc}") from exc

    existing_entries = cached_analysis_entries(load_analysis_cache(path)) if path.exists() and args.overwrite else {}
    entries: List[Dict[str, object]] = []
    errors: List[Dict[str, str]] = []
    reused = 0
    refreshed = 0
    for index, sample in enumerate(samples, start=1):
        cache_key = analysis_cache_key_for_sample(sample, args)
        cached = existing_entries.get(cache_key)
        if cached and cached_entry_matches_sample(cached, sample, args):
            entry = dict(cached)
            entry["index"] = index
            entry["cache_key"] = cache_key
            entry["cache_state"] = "reused"
            entries.append(entry)
            reused += 1
            continue
        try:
            entries.append(analysis_entry_for_sample(sample, args, index))
            refreshed += 1
        except Exception as exc:
            errors.append({"path": str(sample.path), "error": str(exc)})

    payload = {
        "version": ANALYSIS_CACHE_VERSION,
        "kind": "cutups.audio_analysis_cache",
        "input": str(input_root),
        "sample_rate": args.sample_rate,
        "bpm": args.bpm,
        "slice_grid": args.slice_grid,
        "beat_jump_mode": str(getattr(args, "beat_jump_mode", "random") or "random"),
        "beat_similarity_weight": float(getattr(args, "beat_similarity_weight", 1.0)),
        "beat_novelty": float(getattr(args, "beat_novelty", 0.0)),
        "grid_ms": beat_grid_ms(args),
        "similarity_vector_fields": list(ANALYSIS_SIMILARITY_VECTOR_FIELDS),
        "beat_jump_plan": build_beat_jump_plan(entries, args),
        "cache_stats": {"reused": reused, "refreshed": refreshed, "errors": len(errors)},
        "samples": entries,
        "errors": errors,
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def choose_event_count(duration_s: float, density: str, sectional: bool) -> int:
    base = {"sparse": 24, "medium": 44, "dense": 74}[density]
    if sectional:
        base = int(base * 1.25)
    return max(8, int(base * (duration_s / 90.0)))


def source_balance_key(sample: SampleFile) -> str:
    return str(sample.path)


def base_source_weight(sample: SampleFile, concrete: bool) -> float:
    dur_s = sample.duration_ms / 1000.0
    dur_bonus = 2.1 if 0.05 <= dur_s <= 1.7 else 1.2 if dur_s <= 3.5 else 0.45
    if not concrete and dur_s > 3.2:
        dur_bonus += 0.5
    word_bonus = 1.3 if 2 <= sample.words <= 8 else 0.7
    return max(0.1, dur_bonus + word_bonus + sample.intensity_hint + sample.loop_hint)


def source_score_mode(args: argparse.Namespace) -> str:
    mode = str(getattr(args, "source_score", "off") or "off")
    return mode if mode in SOURCE_SCORE_MODES else "off"


def effective_source_score_mode(args: argparse.Namespace) -> str:
    mode = source_score_mode(args)
    if mode != "off":
        return mode
    planner = planner_profile_name(args)
    if planner == "phrase":
        return "spoken"
    if planner == "beat":
        return "beat"
    if planner == "breach":
        return "breach"
    return "off"


def source_text_blob(sample: SampleFile) -> str:
    return f"{sample.path.stem} {sample.path.parent.name} {sample.manifest_role} {sample.manifest_tags} {sample.cue_text}".lower()


def keyword_hits(text: str, keywords: Sequence[str]) -> int:
    return sum(1 for keyword in keywords if keyword in text)


def source_material_score(
    sample: SampleFile,
    args: argparse.Namespace,
    profile: Optional[Dict[str, float]] = None,
) -> float:
    mode = effective_source_score_mode(args)
    if mode == "off":
        return 1.0

    dur_s = max(0.001, sample.duration_ms / 1000.0)
    words = max(0, int(sample.words))
    text = source_text_blob(sample)
    section = str((profile or {}).get("name", ""))

    if mode == "spoken":
        duration_fit = 1.35 if 0.65 <= dur_s <= 5.8 else 1.0 if 0.35 <= dur_s <= 8.0 else 0.52
        word_fit = 1.3 if 3 <= words <= 18 else 0.95 if 1 <= words <= 28 else 0.55
        cue_bonus = 1.18 if sample.has_cue() else 1.0
        name_bonus = 1.0 + min(3, keyword_hits(text, SPOKEN_SOURCE_KEYWORDS)) * 0.08
        pressure_penalty = 0.9 if section in {"PRESSURE", "COLLAPSE"} and dur_s > 6.0 else 1.0
        return clamp(duration_fit * word_fit * cue_bonus * name_bonus * pressure_penalty, 0.2, 3.2)

    if mode == "beat":
        grid_ms = beat_grid_ms(args)
        duration_fit = 1.22 if dur_s >= 0.45 else 0.85
        grid_fit = 1.16 if grid_ms > 0 and sample.duration_ms >= grid_ms * 2 else 1.0
        loop_bonus = 1.0 + min(4, sample.loop_hint) * 0.12
        name_bonus = 1.0 + min(5, keyword_hits(text, BEAT_SOURCE_KEYWORDS)) * 0.12
        speech_penalty = 0.82 if sample.has_cue() and words > 12 else 1.0
        return clamp(duration_fit * grid_fit * loop_bonus * name_bonus * speech_penalty, 0.25, 3.5)

    if mode == "breach":
        hit_bonus = 1.0 + min(5, keyword_hits(text, BREACH_SOURCE_KEYWORDS)) * 0.16
        intensity_bonus = 1.0 + min(5, sample.intensity_hint) * 0.18
        duration_fit = 1.22 if dur_s <= 2.4 else 1.0 if dur_s <= 6.5 else 0.78
        pressure_bonus = 1.18 if section in {"PRESSURE", "COLLAPSE"} else 1.0
        concrete_bonus = 1.08 if bool(getattr(args, "concrete", False)) else 1.0
        return clamp(hit_bonus * intensity_bonus * duration_fit * pressure_bonus * concrete_bonus, 0.25, 3.8)

    return 1.0


def source_diversity_multiplier(
    sample: SampleFile,
    args: argparse.Namespace,
    source_counts: Optional[Counter] = None,
    recent_source_keys: Optional[Sequence[str]] = None,
    previous_sample: Optional[SampleFile] = None,
) -> float:
    diversity = clamp(float(getattr(args, "source_diversity", 0.0) or 0.0), 0.0, 1.0)
    if diversity <= 0:
        return 1.0
    key = source_balance_key(sample)
    count = int(source_counts.get(key, 0)) if source_counts else 0
    recent_hits = sum(1 for recent_key in (recent_source_keys or []) if recent_key == key)
    use_penalty = 1.0 / (1.0 + count * (0.75 + diversity * 1.75))
    recent_penalty = 1.0 / (1.0 + recent_hits * diversity * 2.5)
    previous_penalty = 1.0
    if previous_sample is not None and source_balance_key(previous_sample) == key:
        previous_penalty = max(0.05, 1.0 - 0.85 * diversity)
    return max(0.01, use_penalty * recent_penalty * previous_penalty)


def source_weight_components(
    sample: SampleFile,
    concrete: bool,
    args: Optional[argparse.Namespace] = None,
    source_counts: Optional[Counter] = None,
    recent_source_keys: Optional[Sequence[str]] = None,
    previous_sample: Optional[SampleFile] = None,
    profile: Optional[Dict[str, float]] = None,
) -> Dict[str, float]:
    base = base_source_weight(sample, concrete)
    manifest_weight = clamp(float(getattr(sample, "manifest_weight", 1.0) or 1.0), 0.05, 20.0)
    material = source_material_score(sample, args, profile=profile) if args is not None else 1.0
    diversity = (
        source_diversity_multiplier(
            sample,
            args,
            source_counts=source_counts,
            recent_source_keys=recent_source_keys,
            previous_sample=previous_sample,
        )
        if args is not None
        else 1.0
    )
    return {
        "base_weight": round(base, 6),
        "manifest_weight": round(manifest_weight, 6),
        "material_score": round(material, 6),
        "diversity_multiplier": round(diversity, 6),
        "final_weight": round(max(0.01, base * manifest_weight * material * diversity), 6),
    }


def source_selection_diagnostics(
    sample: SampleFile,
    args: argparse.Namespace,
    concrete: bool,
    source_counts: Optional[Counter] = None,
    recent_source_keys: Optional[Sequence[str]] = None,
    previous_sample: Optional[SampleFile] = None,
    profile: Optional[Dict[str, float]] = None,
    reason: str = "weighted_source",
) -> Dict[str, object]:
    key = source_balance_key(sample)
    recent = list(recent_source_keys or [])
    components = source_weight_components(
        sample,
        concrete,
        args=args,
        source_counts=source_counts,
        recent_source_keys=recent,
        previous_sample=previous_sample,
        profile=profile,
    )
    return {
        "selection_reason": reason,
        "source_score_mode": effective_source_score_mode(args),
        "source_base_weight": components["base_weight"],
        "source_material_score": components["material_score"],
        "source_diversity_multiplier": components["diversity_multiplier"],
        "source_final_weight": components["final_weight"],
        "source_use_count_before": int(source_counts.get(key, 0)) if source_counts else 0,
        "source_recent_hits_before": sum(1 for recent_key in recent if recent_key == key),
        "source_immediate_repeat": bool(previous_sample is not None and source_balance_key(previous_sample) == key),
        "section_density_target": round(float((profile or {}).get("dens", 0.0)), 6),
        "section_fragment_multiplier": round(float((profile or {}).get("frag_mul", 0.0)), 6),
        "section_repeat_probability": round(float((profile or {}).get("repeat", 0.0)), 6),
        "section_ghost_probability": round(float((profile or {}).get("ghost", 0.0)), 6),
    }


def weighted_choice(
    samples: List[SampleFile],
    concrete: bool,
    args: Optional[argparse.Namespace] = None,
    source_counts: Optional[Counter] = None,
    recent_source_keys: Optional[Sequence[str]] = None,
    previous_sample: Optional[SampleFile] = None,
    profile: Optional[Dict[str, float]] = None,
) -> SampleFile:
    weights = []
    for sample in samples:
        weights.append(
            source_weight_components(
                sample,
                concrete,
                args=args,
                source_counts=source_counts,
                recent_source_keys=recent_source_keys,
                previous_sample=previous_sample,
                profile=profile,
            )["final_weight"]
        )
    return random.choices(samples, weights=weights, k=1)[0]


def choose_similarity_neighbor(
    candidates: List[SampleFile],
    args: argparse.Namespace,
    source_counts: Optional[Counter] = None,
    recent_source_keys: Optional[Sequence[str]] = None,
    previous_sample: Optional[SampleFile] = None,
    profile: Optional[Dict[str, float]] = None,
) -> SampleFile:
    novelty = clamp(float(getattr(args, "beat_novelty", 0.0)), 0.0, 1.0)
    pool_size = max(1, min(len(candidates), 3 + int(round(novelty * max(0, len(candidates) - 3)))))
    pool = candidates[:pool_size]
    width = max(1, len(pool) - 1)
    weights = []
    for idx, sample in enumerate(pool):
        near_weight = len(pool) - idx
        far_weight = 1.0 + (idx / width) * len(pool)
        novelty_weight = max(0.01, (1.0 - novelty) * near_weight + novelty * far_weight)
        components = source_weight_components(
            sample,
            concrete=bool(getattr(args, "concrete", False)),
            args=args,
            source_counts=source_counts,
            recent_source_keys=recent_source_keys,
            previous_sample=previous_sample,
            profile=profile,
        )
        weights.append(
            max(
                0.01,
                novelty_weight
                * float(components["manifest_weight"])
                * float(components["material_score"])
                * float(components["diversity_multiplier"]),
            )
        )
    return random.choices(pool, weights=weights, k=1)[0]


def choose_source_sample(
    samples: List[SampleFile],
    args: argparse.Namespace,
    concrete: bool,
    beat_jump: Optional[BeatJumpState] = None,
    previous_sample: Optional[SampleFile] = None,
    source_counts: Optional[Counter] = None,
    recent_source_keys: Optional[Sequence[str]] = None,
    profile: Optional[Dict[str, float]] = None,
) -> SampleFile:
    if beat_jump and beat_jump.active and previous_sample is not None:
        similarity_weight = clamp(float(getattr(args, "beat_similarity_weight", 1.0)), 0.0, 1.0)
        if random.random() > similarity_weight:
            beat_jump.fallbacks += 1
            return weighted_choice(
                samples,
                concrete,
                args=args,
                source_counts=source_counts,
                recent_source_keys=recent_source_keys,
                previous_sample=previous_sample,
                profile=profile,
            )
        source_key = analysis_cache_key_for_sample(previous_sample, args)
        neighbor_keys = beat_jump.neighbor_keys.get(source_key, [])
        candidates = [beat_jump.samples_by_key[key] for key in neighbor_keys if key in beat_jump.samples_by_key]
        if candidates:
            beat_jump.selections += 1
            return choose_similarity_neighbor(
                candidates,
                args,
                source_counts=source_counts,
                recent_source_keys=recent_source_keys,
                previous_sample=previous_sample,
                profile=profile,
            )
        beat_jump.fallbacks += 1
    elif beat_jump and beat_jump.active:
        beat_jump.fallbacks += 1
    return weighted_choice(
        samples,
        concrete,
        args=args,
        source_counts=source_counts,
        recent_source_keys=recent_source_keys,
        previous_sample=previous_sample,
        profile=profile,
    )


def section_arc_name(args: argparse.Namespace) -> str:
    arc = str(getattr(args, "section_arc", "classic") or "classic")
    return arc if arc in SECTION_ARCS else "classic"


def base_section_profile(progress: float, args: argparse.Namespace) -> Dict[str, float]:
    silence_prob = float(getattr(args, "silence_prob", 0.15))
    ghost_prob = float(getattr(args, "ghost_prob", 0.22))
    if progress < 0.2:
        return {"name": "ENTRY", "dens": 0.44, "frag_mul": 1.28, "repeat": 0.2, "reverse": 0.14, "filt": 0.52, "silence": silence_prob + 0.16, "ghost": ghost_prob * 0.72}
    if progress < 0.45:
        return {"name": "BUILD", "dens": 1.18, "frag_mul": 0.82, "repeat": 0.42, "reverse": 0.22, "filt": 0.72, "silence": silence_prob * 0.88, "ghost": ghost_prob + 0.08}
    if progress < 0.68:
        return {"name": "PRESSURE", "dens": 1.72, "frag_mul": 0.42, "repeat": 0.64, "reverse": 0.36, "filt": 0.92, "silence": silence_prob * 0.5, "ghost": ghost_prob + 0.19}
    if progress < 0.86:
        return {"name": "COLLAPSE", "dens": 0.66, "frag_mul": 0.3, "repeat": 0.72, "reverse": 0.58, "filt": 0.98, "silence": silence_prob + 0.28, "ghost": ghost_prob + 0.3}
    return {"name": "AFTERIMAGE", "dens": 0.3, "frag_mul": 0.24, "repeat": 0.78, "reverse": 0.68, "filt": 0.99, "silence": silence_prob + 0.33, "ghost": ghost_prob + 0.42}


def apply_section_arc(profile: Dict[str, float], args: argparse.Namespace) -> Dict[str, float]:
    out = dict(profile)
    arc = section_arc_name(args)
    modifiers = SECTION_ARC_MODIFIERS.get(arc, {}).get(str(out.get("name", "")), {})
    for key in SECTION_PROFILE_KEYS:
        value = float(out.get(key, 0.0)) * float(modifiers.get(key, 1.0))
        value += float(modifiers.get(f"{key}_add", 0.0))
        if key == "dens":
            value = clamp(value, 0.12, 2.8)
        elif key == "frag_mul":
            value = clamp(value, 0.12, 2.2)
        else:
            value = clamp(value, 0.0, 0.99)
        out[key] = value
    out["arc"] = arc
    return out


def section_profile(progress: float, args: argparse.Namespace) -> Dict[str, float]:
    return apply_section_arc(base_section_profile(progress, args), args)


def section_plan(total_ms: int) -> Dict[str, Tuple[int, int]]:
    marks = [0, int(total_ms * 0.2), int(total_ms * 0.45), int(total_ms * 0.68), int(total_ms * 0.86), total_ms]
    names = list(SECTION_NAMES)
    return {name: (marks[i], marks[i + 1]) for i, name in enumerate(names)}


def clamp_to_section(position_ms: int, span: Tuple[int, int], frag_len: int, grid_ms: int = 0) -> int:
    start, end = span
    room_end = max(start, end - max(10, frag_len + 4))
    if grid_ms > 0:
        lower = ((start + grid_ms - 1) // grid_ms) * grid_ms
        upper = (room_end // grid_ms) * grid_ms
        if lower <= upper:
            return int(clamp(quantize_to_grid(position_ms, grid_ms), lower, upper))
    return int(clamp(position_ms, start, room_end))


def command_cell_swarm(audio: AudioSegment, profile: Dict[str, float]) -> Tuple[AudioSegment, bool]:
    swarm_prob = clamp((0.18 + profile["repeat"] * 0.42) * float(profile.get("swarm_bias", 1.0)), 0.0, 0.9)
    if len(audio) < 45 or random.random() > swarm_prob:
        return audio, False
    cell_len = random.randint(35, min(240, len(audio)))
    start = random.randint(0, max(0, len(audio) - cell_len))
    cell = audio[start : start + cell_len]
    if random.random() < 0.52:
        cell = low_pass_filter(cell, random.choice([1800, 2300, 3200]))
    if random.random() < 0.45:
        cell = high_pass_filter(cell, random.choice([180, 340, 520]))
    swarm = AudioSegment.silent(duration=0, frame_rate=audio.frame_rate)
    repeats = random.randint(3, 8)
    for i in range(repeats):
        beat = cell if i % 3 != 2 else cell.reverse()
        if random.random() < 0.3:
            beat = change_speed(beat, random.choice([0.88, 0.96, 1.08, 1.18]))
        swarm += beat + AudioSegment.silent(duration=random.randint(7, 48), frame_rate=audio.frame_rate)
    if random.random() < 0.35:
        swarm += audio[-min(len(audio), random.randint(40, 160)) :]
    return swarm, True


def safe_slice_fragment(audio: AudioSegment, min_ms: int, max_ms: int, frag_mul: float, grid_ms: int = 0) -> AudioSegment:
    audio_len = len(audio)
    if audio_len <= 1:
        return AudioSegment.silent(duration=30, frame_rate=audio.frame_rate)
    if grid_ms > 0:
        frag_len = grid_fragment_length(audio_len, min_ms, max_ms, frag_mul, grid_ms)
        max_start = max(0, audio_len - frag_len)
        if max_start <= 0:
            start = 0
        else:
            grid_starts = list(range(0, max_start + 1, grid_ms))
            start = random.choice(grid_starts) if grid_starts else quantize_to_grid(random.randint(0, max_start), grid_ms)
            start = min(max_start, start)
        return audio[start : start + frag_len]
    local_min = max(15, int(min_ms * frag_mul))
    local_max = max(local_min, int(max_ms * frag_mul))
    upper = min(audio_len, local_max)
    if upper <= 0:
        upper = min(audio_len, max(20, local_min))
    if upper <= local_min:
        frag_len = max(10, upper)
    else:
        frag_len = random.randint(local_min, upper)
    frag_len = max(8, min(frag_len, audio_len))
    start = 0 if audio_len <= frag_len else random.randint(0, max(0, audio_len - frag_len))
    return audio[start : start + frag_len]


def source_audio_for_sample(sample: SampleFile, args: argparse.Namespace) -> AudioSegment:
    audio = AudioSegment.from_file(sample.path).set_frame_rate(args.sample_rate).set_channels(2)
    if sample.has_cue():
        return audio[sample.cue_start_ms : sample.cue_end_ms]
    return audio


def baseline_beat_path(args: argparse.Namespace) -> Optional[Path]:
    raw = str(getattr(args, "baseline_beat", "") or "").strip()
    if not raw:
        return None
    return Path(raw).expanduser().resolve()


def baseline_beat_bpm_from_duration(duration_ms: int, bars: float) -> float:
    if duration_ms <= 0 or bars <= 0:
        return 0.0
    return round((float(bars) * 4.0 * 60000.0) / float(duration_ms), 3)


def load_baseline_beat(args: argparse.Namespace) -> Optional[BaselineBeat]:
    path = baseline_beat_path(args)
    if path is None:
        setattr(args, "baseline_beat_source_duration_ms", 0)
        setattr(args, "baseline_beat_inferred_bpm", 0.0)
        return None
    try:
        audio = AudioSegment.from_file(path).set_frame_rate(args.sample_rate).set_channels(2)
    except Exception as exc:
        raise SystemExit(f"Could not decode --baseline-beat audio file: {path}") from exc
    source_duration_ms = len(audio)
    if source_duration_ms <= 1:
        raise SystemExit(f"--baseline-beat must contain usable audio: {path}")

    inferred_bpm = 0.0
    bars = float(getattr(args, "baseline_beat_bars", 0.0) or 0.0)
    if bars > 0 and float(getattr(args, "bpm", 0.0) or 0.0) <= 0:
        inferred_bpm = baseline_beat_bpm_from_duration(source_duration_ms, bars)
        if not 20 <= inferred_bpm <= 300:
            raise SystemExit(
                f"--baseline-beat-bars inferred {inferred_bpm:g} bpm from {path.name}; "
                "set --bpm manually or adjust --baseline-beat-bars"
            )
        args.bpm = inferred_bpm
        if str(getattr(args, "slice_grid", "off") or "off") == "off" and "slice_grid" not in getattr(args, "_explicit_args", set()):
            args.slice_grid = "1/16"
        print(
            f"Baseline beat BPM inferred: {inferred_bpm:g} bpm "
            f"from {source_duration_ms} ms over {bars:g} bar(s)"
        )

    setattr(args, "baseline_beat_source_duration_ms", source_duration_ms)
    setattr(args, "baseline_beat_inferred_bpm", inferred_bpm)
    return BaselineBeat(
        path=path,
        audio=audio,
        source_duration_ms=source_duration_ms,
        gain_db=float(getattr(args, "baseline_beat_gain", -9.0) or 0.0),
        inferred_bpm=inferred_bpm,
    )


def render_baseline_beat_bed(baseline: BaselineBeat, total_ms: int) -> AudioSegment:
    loops = max(1, int(math.ceil(total_ms / float(max(1, baseline.source_duration_ms)))))
    return (baseline.audio * loops)[:total_ms].apply_gain(baseline.gain_db)


def baseline_duck_windows(events: List[Event], total_ms: int, duck_ms: int) -> List[Tuple[int, int]]:
    if total_ms <= 0 or duck_ms < 0:
        return []
    intervals: List[Tuple[int, int]] = []
    for event in events:
        start = max(0, int(event.start_ms) - duck_ms)
        end = min(total_ms, int(event.end_ms) + duck_ms)
        if end > start:
            intervals.append((start, end))
    if not intervals:
        return []
    intervals.sort()
    merged: List[Tuple[int, int]] = []
    for start, end in intervals:
        if not merged or start > merged[-1][1]:
            merged.append((start, end))
        else:
            prev_start, prev_end = merged[-1]
            merged[-1] = (prev_start, max(prev_end, end))
    return merged


def duck_baseline_beat_bed(bed: AudioSegment, events: List[Event], duck_db: float, duck_ms: int) -> Tuple[AudioSegment, int]:
    amount = float(duck_db or 0.0)
    if amount <= 0 or len(bed) <= 0:
        return bed, 0
    windows = baseline_duck_windows(events, len(bed), max(0, int(duck_ms)))
    if not windows:
        return bed, 0
    out = AudioSegment.silent(duration=0, frame_rate=bed.frame_rate)
    cursor = 0
    for start, end in windows:
        if start > cursor:
            out += bed[cursor:start]
        out += bed[start:end].apply_gain(-amount)
        cursor = end
    if cursor < len(bed):
        out += bed[cursor:]
    return out[: len(bed)], len(windows)


def baseline_placement_mode(args: argparse.Namespace) -> str:
    mode = str(getattr(args, "baseline_placement", "any") or "any").lower()
    return mode if mode in BASELINE_PLACEMENT_MODES else "any"


def baseline_cell_rank(energy: float, low_threshold: float, high_threshold: float) -> str:
    if energy >= high_threshold:
        return "accent"
    if energy <= low_threshold:
        return "gap"
    return "mid"


def baseline_grid_profile(audio: Optional[AudioSegment], args: argparse.Namespace, total_ms: int) -> Dict[str, object]:
    mode = baseline_placement_mode(args)
    grid_ms = beat_grid_ms(args)
    inactive = {
        "active": False,
        "mode": mode,
        "grid_ms": grid_ms,
        "cell_count": 0,
        "energies": [],
        "summary": {"active": False, "mode": mode, "grid_ms": grid_ms, "cell_count": 0, "captured": 0, "truncated": False, "cells": []},
    }
    if audio is None or grid_ms <= 0 or total_ms <= 0:
        return inactive

    cell_count = int(math.ceil(total_ms / float(grid_ms)))
    if cell_count <= 0:
        return inactive
    rms_values: List[int] = []
    for idx in range(cell_count):
        start_ms = idx * grid_ms
        end_ms = min(len(audio), start_ms + grid_ms)
        if end_ms <= start_ms:
            rms_values.append(0)
        else:
            rms_values.append(int(audio[start_ms:end_ms].rms))
    max_rms = max(rms_values) if rms_values else 0
    energies = [round((rms / max_rms) if max_rms > 0 else 0.0, 6) for rms in rms_values]
    ordered = sorted(energies)
    if ordered:
        low_threshold = ordered[min(len(ordered) - 1, max(0, int(len(ordered) * 0.33)))]
        high_threshold = ordered[min(len(ordered) - 1, max(0, int(len(ordered) * 0.67)))]
    else:
        low_threshold = 0.0
        high_threshold = 0.0
    mean_energy, energy_variation = mean_and_variation(energies)
    capture_count = min(cell_count, BASELINE_GRID_SUMMARY_MAX_CELLS)
    cells = [
        {
            "index": idx,
            "start_ms": idx * grid_ms,
            "end_ms": min(total_ms, (idx + 1) * grid_ms),
            "rms": rms_values[idx],
            "energy": energies[idx],
            "rank": baseline_cell_rank(energies[idx], low_threshold, high_threshold),
        }
        for idx in range(capture_count)
    ]
    summary = {
        "active": True,
        "mode": mode,
        "grid_ms": grid_ms,
        "cell_count": cell_count,
        "captured": len(cells),
        "truncated": cell_count > len(cells),
        "mean_energy": mean_energy,
        "energy_variation": energy_variation,
        "low_threshold": round(float(low_threshold), 6),
        "high_threshold": round(float(high_threshold), 6),
        "accent_cells": sum(1 for energy in energies if energy >= high_threshold),
        "gap_cells": sum(1 for energy in energies if energy <= low_threshold),
        "cells": cells,
    }
    return {
        "active": True,
        "mode": mode,
        "grid_ms": grid_ms,
        "cell_count": cell_count,
        "energies": energies,
        "low_threshold": float(low_threshold),
        "high_threshold": float(high_threshold),
        "summary": summary,
    }


def baseline_placement_score(mode: str, idx: int, energies: Sequence[float], low_threshold: float, high_threshold: float) -> float:
    energy = float(energies[idx])
    if mode == "accent":
        return energy
    if mode == "gap":
        return 1.0 - energy
    if mode == "offbeat":
        prev_energy = float(energies[idx - 1]) if idx > 0 else 0.0
        next_energy = float(energies[idx + 1]) if idx + 1 < len(energies) else 0.0
        neighbor_accent = max(prev_energy, next_energy)
        return (1.0 - energy) * 0.7 + neighbor_accent * 0.3
    return 0.0


def apply_baseline_placement(
    pos_ms: int,
    event_len_ms: int,
    sec_span: Tuple[int, int],
    total_ms: int,
    grid_ms: int,
    profile: Dict[str, object],
) -> Tuple[int, Dict[str, object]]:
    mode = str(profile.get("mode", "any") or "any")
    energies = profile.get("energies", [])
    if mode == "any" or not profile.get("active") or grid_ms <= 0 or not isinstance(energies, list):
        return pos_ms, {"mode": "any", "original_start_ms": pos_ms, "cell_index": -1, "cell_energy": 0.0}
    if not energies:
        return pos_ms, {"mode": mode, "original_start_ms": pos_ms, "cell_index": -1, "cell_energy": 0.0}

    current_idx = int(clamp(round(pos_ms / float(grid_ms)), 0, len(energies) - 1))
    radius = max(2, min(16, int(max(4, round(750 / float(grid_ms))))))
    low_threshold = float(profile.get("low_threshold", 0.0) or 0.0)
    high_threshold = float(profile.get("high_threshold", 0.0) or 0.0)
    best: Optional[Tuple[float, int, int, float]] = None
    for idx in range(max(0, current_idx - radius), min(len(energies), current_idx + radius + 1)):
        candidate = clamp_to_section(idx * grid_ms, sec_span, event_len_ms, grid_ms=grid_ms)
        candidate_idx = int(clamp(round(candidate / float(grid_ms)), 0, len(energies) - 1))
        energy = float(energies[candidate_idx])
        distance = abs(candidate_idx - current_idx)
        score = baseline_placement_score(mode, candidate_idx, energies, low_threshold, high_threshold) - (distance * 0.025)
        item = (score, -distance, candidate_idx, energy)
        if best is None or item > best:
            best = item

    if best is None:
        return pos_ms, {"mode": mode, "original_start_ms": pos_ms, "cell_index": -1, "cell_energy": 0.0}
    _, _, cell_idx, cell_energy = best
    adjusted = clamp_to_section(cell_idx * grid_ms, sec_span, event_len_ms, grid_ms=grid_ms)
    return adjusted, {
        "mode": mode,
        "original_start_ms": int(pos_ms),
        "cell_index": int(cell_idx),
        "cell_energy": round(float(cell_energy), 6),
    }


def change_speed(audio: AudioSegment, speed: float) -> AudioSegment:
    if abs(speed - 1.0) < 1e-6:
        return audio
    speed = clamp(speed, 0.45, 1.8)
    altered = audio._spawn(audio.raw_data, overrides={"frame_rate": int(audio.frame_rate * speed)})
    return altered.set_frame_rate(audio.frame_rate)


def make_hiss(duration_ms: int, frame_rate: int) -> AudioSegment:
    bed = AudioSegment.silent(duration=duration_ms, frame_rate=frame_rate)
    tick = AudioSegment.silent(duration=21, frame_rate=frame_rate) - 60
    pos = 0
    while pos < duration_ms:
        bed = bed.overlay(tick.apply_gain(random.uniform(-20, -9)), position=pos)
        pos += random.randint(11, 39)
    return low_pass_filter(high_pass_filter(bed, 4200), 10800) - 12


def preview_duration_ms(args: argparse.Namespace, audio_len_ms: int) -> int:
    requested_s = float(getattr(args, "preview_duration", 0.0) or 0.0)
    if requested_s <= 0 or audio_len_ms <= 0:
        return 0
    return max(1, min(audio_len_ms, int(round(requested_s * 1000))))


def make_noise_burst(duration_ms: int, frame_rate: int, channels: int) -> AudioSegment:
    burst = WhiteNoise().to_audio_segment(duration=max(4, duration_ms)).set_frame_rate(frame_rate).set_channels(channels)
    burst = high_pass_filter(burst, random.choice([1800, 2600, 4200, 6200]))
    burst = low_pass_filter(burst, random.choice([5200, 7600, 10400]))
    return burst.fade_in(1).fade_out(min(12, max(2, len(burst) // 3)))


def reverse_shards(audio: AudioSegment, rate: float) -> Tuple[AudioSegment, bool]:
    if len(audio) < 50 or rate <= 0 or random.random() >= rate:
        return audio, False
    shard_ms = random.randint(18, min(140, max(18, len(audio) // 2)))
    cursor = 0
    out = AudioSegment.silent(duration=0, frame_rate=audio.frame_rate)
    changed = False
    while cursor < len(audio):
        part = audio[cursor : min(len(audio), cursor + shard_ms)]
        if len(part) > 8 and random.random() < clamp(0.25 + rate * 0.55, 0.0, 0.9):
            part = part.reverse()
            changed = True
        out += part
        cursor += shard_ms
    return out, changed


def apply_dropouts(audio: AudioSegment, args: argparse.Namespace) -> Tuple[AudioSegment, bool]:
    rate = float(getattr(args, "dropout_rate", 0.0))
    if len(audio) < 60 or rate <= 0 or random.random() >= rate:
        return audio, False
    holes = random.randint(1, max(1, 1 + int(rate * 4)))
    changed = False
    for _ in range(holes):
        if len(audio) < 40:
            break
        start = random.randint(0, max(0, len(audio) - 24))
        dur = min(len(audio) - start, silence_duration_ms(args, (12, 110)))
        if dur <= 0:
            continue
        audio = audio[:start] + AudioSegment.silent(duration=dur, frame_rate=audio.frame_rate) + audio[start + dur :]
        changed = True
    return audio, changed


def apply_noise_bursts(audio: AudioSegment, args: argparse.Namespace) -> Tuple[AudioSegment, bool]:
    rate = float(getattr(args, "burst_rate", 0.0))
    if len(audio) < 30 or rate <= 0 or random.random() >= rate:
        return audio, False
    bursts = random.randint(1, max(1, 1 + int(rate * 3)))
    changed = False
    for _ in range(bursts):
        dur = random.randint(8, max(9, min(90, len(audio) // 2)))
        pos = random.randint(0, max(0, len(audio) - dur))
        gain = random.uniform(-18.0, -4.0 + rate * 4.0)
        burst = make_noise_burst(dur, audio.frame_rate, audio.channels).apply_gain(gain)
        audio = audio.overlay(burst, position=pos)
        changed = True
    return audio, changed


def _silent_like(audio: AudioSegment, duration_ms: int) -> AudioSegment:
    return AudioSegment.silent(duration=max(0, duration_ms), frame_rate=audio.frame_rate).set_channels(audio.channels)


def apply_beat_grid_controls(audio: AudioSegment, args: argparse.Namespace, grid_ms: int) -> Tuple[AudioSegment, Set[str], int]:
    rates = beat_control_rates(args)
    if grid_ms <= 0 or len(audio) < 8 or not any(rates.values()):
        return audio, set(), 1

    cell_ms = max(8, min(grid_ms, len(audio)))
    out = AudioSegment.silent(duration=0, frame_rate=audio.frame_rate).set_channels(audio.channels)
    tags: Set[str] = set()
    max_repeat = 1

    cursor = 0
    while cursor < len(audio):
        cell = audio[cursor : min(len(audio), cursor + cell_ms)]
        cursor += cell_ms
        if len(cell) <= 0:
            continue

        current = cell
        if rates["stutter_rate"] > 0 and random.random() < rates["stutter_rate"] and len(cell) >= 16:
            sub_len = max(8, min(len(cell), random.choice([max(8, cell_ms // 8), max(8, cell_ms // 4), max(8, cell_ms // 2)])))
            sub_start = 0 if len(cell) <= sub_len else random.randint(0, max(0, len(cell) - sub_len))
            sub = cell[sub_start : sub_start + sub_len]
            stuttered = AudioSegment.silent(duration=0, frame_rate=audio.frame_rate).set_channels(audio.channels)
            while len(stuttered) < len(cell):
                stuttered += sub
            current = stuttered[: len(cell)]
            tags.add("beatstutter")

        if rates["mute_rate"] > 0 and random.random() < rates["mute_rate"]:
            current = _silent_like(audio, len(cell))
            tags.add("beatmute")

        out += current

        if rates["repeat_rate"] > 0 and random.random() < rates["repeat_rate"]:
            extra = random.randint(1, max(1, 1 + int(rates["repeat_rate"] * 3)))
            for _ in range(extra):
                out += current
            max_repeat = max(max_repeat, extra + 1)
            tags.add("beatrepeat")

    if rates["beat_dropout_rate"] > 0 and len(out) >= cell_ms and random.random() < rates["beat_dropout_rate"]:
        holes = random.randint(1, max(1, 1 + int(rates["beat_dropout_rate"] * 3)))
        for _ in range(holes):
            starts = list(range(0, max(1, len(out) - cell_ms + 1), cell_ms))
            if not starts:
                break
            start = random.choice(starts)
            dur = min(len(out) - start, cell_ms * random.choice([1, 1, 2, 4]))
            if dur <= 0:
                continue
            out = out[:start] + _silent_like(audio, dur) + out[start + dur :]
            tags.add("beatdrop")

    max_len = max(len(audio), len(audio) * 3)
    if len(out) > max_len:
        out = out[:max_len]
    return out, tags, max_repeat


def filter_pair(args: argparse.Namespace) -> Tuple[int, int]:
    severity = str(getattr(args, "filter_severity", "auto"))
    if severity == "light":
        return random.choice([80, 120, 180, 260]), random.choice([4400, 6200, 9000, 12000])
    if severity == "medium":
        return random.choice([180, 260, 420, 700, 1000]), random.choice([2100, 3200, 4400, 6200])
    if severity == "hard":
        return random.choice([420, 700, 1200, 1700, 2400]), random.choice([900, 1200, 1800, 2400, 3200])
    return random.choice([100, 180, 260, 420, 700, 1200, 1700]), random.choice([1200, 2100, 3200, 4400, 6200, 9000])


def grainify(audio: AudioSegment) -> AudioSegment:
    if len(audio) < 25:
        return audio
    grain = max(12, min(80, len(audio) // random.randint(2, 6)))
    pieces = []
    cursor = 0
    while cursor < len(audio):
        end = min(len(audio), cursor + grain)
        part = audio[cursor:end]
        if random.random() < 0.28:
            part = part.reverse()
        if random.random() < 0.35:
            part = change_speed(part, random.choice([0.68, 0.82, 0.95, 1.2, 1.33]))
        pieces.append(part)
        cursor += grain
    out = AudioSegment.silent(duration=0, frame_rate=audio.frame_rate)
    for p in pieces:
        out += p
    return out


def shape_fragment(audio: AudioSegment, profile: Dict[str, float], args: argparse.Namespace) -> Tuple[AudioSegment, Dict[str, object]]:
    concrete = bool(getattr(args, "concrete", False))
    reversed_flag = random.random() < profile["reverse"]
    if reversed_flag:
        audio = audio.reverse()

    speed_mode = str(profile.get("speed_mode", "auto"))
    if speed_mode == "clear":
        speed = random.choice([0.94, 1.0, 1.0, 1.04])
    elif speed_mode == "moderate":
        speed = random.choice([0.86, 0.94, 1.0, 1.0, 1.12])
    elif speed_mode == "unstable":
        speed = random.choice([0.58, 0.72, 0.85, 0.94, 1.0, 1.18, 1.35, 1.45])
    else:
        speed = random.choice([0.58, 0.72, 0.85, 0.94, 1.0, 1.12, 1.28, 1.45]) if concrete else random.choice([0.76, 0.86, 0.94, 1.0, 1.1, 1.22])
    audio = change_speed(audio, speed)

    grain_prob = (0.42 if concrete else 0.22) * float(profile.get("grain_bias", 1.0))
    grain_mode = random.random() < clamp(grain_prob, 0.0, 0.9)
    if grain_mode:
        audio = grainify(audio)

    shard_mode = False
    audio, shard_mode = reverse_shards(audio, float(getattr(args, "reverse_shard_rate", 0.0)))

    repeated = 1
    if random.random() < profile["repeat"]:
        repeated = random.choice([2, 3, 4, 5])
        gap = AudioSegment.silent(duration=random.randint(8, 90), frame_rate=audio.frame_rate)
        built = AudioSegment.silent(duration=0, frame_rate=audio.frame_rate)
        for _ in range(repeated):
            built += audio + gap
        audio = built

    swarm_mode = False
    audio, swarm_mode = command_cell_swarm(audio, profile)
    if swarm_mode:
        repeated = max(repeated, 3)

    beat_tags: Set[str] = set()
    beat_repeat = 1
    audio, beat_tags, beat_repeat = apply_beat_grid_controls(audio, args, beat_grid_ms(args))
    repeated = max(repeated, beat_repeat)

    if random.random() < float(profile.get("hard_cut", 0.22 if concrete else 0.14)):
        # hard interruption: chop center out to create phrase discontinuity.
        mid = len(audio) // 2
        cut = random.randint(8, min(120, max(8, len(audio) // 2)))
        audio = audio[: max(0, mid - cut)] + AudioSegment.silent(duration=silence_duration_ms(args, (12, 80)), frame_rate=audio.frame_rate) + audio[min(len(audio), mid + cut) :]

    dropout_mode = False
    audio, dropout_mode = apply_dropouts(audio, args)

    burst_mode = False
    audio, burst_mode = apply_noise_bursts(audio, args)

    hp, lp = filter_pair(args)
    if random.random() < profile["filt"]:
        audio = high_pass_filter(audio, hp)
    if random.random() < profile["filt"]:
        audio = low_pass_filter(audio, lp)

    if random.random() < profile["silence"]:
        pad = AudioSegment.silent(duration=silence_duration_ms(args, (18, 160)), frame_rate=audio.frame_rate)
        audio = (pad + audio) if random.random() < 0.5 else (audio + pad)

    if random.random() < profile["silence"] * 0.45 and len(audio) > 70:
        hole_start = random.randint(0, max(0, len(audio) - 40))
        hole = AudioSegment.silent(duration=silence_duration_ms(args, (10, 70)), frame_rate=audio.frame_rate)
        audio = audio[:hole_start] + hole + audio[hole_start:]

    audio = audio.fade_in(min(20, max(2, len(audio) // 15))).fade_out(min(46, max(5, len(audio) // 9)))
    transform = "grain" if grain_mode else "slice"
    if reversed_flag:
        transform += "+rev"
    if shard_mode:
        transform += "+shards"
    if swarm_mode:
        transform += "+swarm"
    for tag in ("beatstutter", "beatmute", "beatrepeat", "beatdrop"):
        if tag in beat_tags:
            transform += f"+{tag}"
    if dropout_mode:
        transform += "+dropout"
    if burst_mode:
        transform += "+burst"
    return audio, {"reversed": reversed_flag, "speed": speed, "repeated": repeated, "hp_hz": hp, "lp_hz": lp, "grain_mode": grain_mode, "transformation": transform}


def export_manifest(path: Path, events: List[Event]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=EVENT_CSV_FIELDS)
        writer.writeheader()
        for e in events:
            writer.writerow(e.__dict__)


def sorted_count_map(counter: Counter) -> Dict[str, int]:
    return {str(key): int(counter[key]) for key in sorted(counter)}


def top_count_rows(counter: Counter, limit: int = 8) -> List[Dict[str, object]]:
    rows = sorted(counter.items(), key=lambda item: (-int(item[1]), str(item[0])))
    return [{"value": str(key), "count": int(count)} for key, count in rows[:limit]]


def section_window_rows(total_ms: int) -> List[Dict[str, object]]:
    plan = section_plan(total_ms)
    rows = []
    for name in SECTION_NAMES:
        start_ms, end_ms = plan[name]
        rows.append(
            {
                "name": name,
                "start_ms": start_ms,
                "end_ms": end_ms,
                "duration_ms": max(0, end_ms - start_ms),
                "start_sec": round(start_ms / 1000.0, 3),
                "end_sec": round(end_ms / 1000.0, 3),
            }
        )
    return rows


def section_target_rows(args: argparse.Namespace, total_ms: int) -> List[Dict[str, object]]:
    windows = {row["name"]: row for row in section_window_rows(total_ms)}
    rows = []
    for name in SECTION_NAMES:
        profile = workflow_audio_profile(section_profile(SECTION_PROGRESS[name], args), args)
        window = windows[name]
        rows.append(
            {
                "name": name,
                "section_arc": section_arc_name(args),
                "start_ms": window["start_ms"],
                "end_ms": window["end_ms"],
                "density_target": round(float(profile["dens"]), 3),
                "fragment_length_multiplier": round(float(profile["frag_mul"]), 3),
                "repeat_probability": round(float(profile["repeat"]), 3),
                "reverse_probability": round(float(profile["reverse"]), 3),
                "filter_probability": round(float(profile["filt"]), 3),
                "silence_probability": round(float(profile["silence"]), 3),
                "ghost_probability": round(float(profile["ghost"]), 3),
                "hard_cut_probability": round(float(profile.get("hard_cut", 0.0)), 3),
                "planner_profile": planner_profile_name(args),
                "planner_intent": section_planner_intent(name, planner_profile_name(args)),
            }
        )
    return rows


def event_plan_row(event: Event, index: int) -> Dict[str, object]:
    row = dict(event.__dict__)
    planner = {
        "selection_reason": row.pop("selection_reason", ""),
        "source_weight": {
            "source_score_mode": row.pop("source_score_mode", "off"),
            "base_weight": row.pop("source_base_weight", 0.0),
            "manifest_weight": row.get("source_manifest_weight", 1.0),
            "material_score": row.pop("source_material_score", 1.0),
            "diversity_multiplier": row.pop("source_diversity_multiplier", 1.0),
            "final_weight": row.pop("source_final_weight", 0.0),
            "use_count_before": row.pop("source_use_count_before", 0),
            "recent_hits_before": row.pop("source_recent_hits_before", 0),
            "immediate_repeat": row.pop("source_immediate_repeat", False),
        },
        "section_targets": {
            "density": row.pop("section_density_target", 0.0),
            "fragment_multiplier": row.pop("section_fragment_multiplier", 0.0),
            "repeat_probability": row.pop("section_repeat_probability", 0.0),
            "ghost_probability": row.pop("section_ghost_probability", 0.0),
        },
        "baseline_placement": {
            "mode": row.pop("baseline_placement_mode", "any"),
            "original_start_ms": row.pop("baseline_placement_original_start_ms", 0),
            "cell_index": row.pop("baseline_placement_cell_index", -1),
            "cell_energy": row.pop("baseline_placement_cell_energy", 0.0),
        },
        "construction": {
            "profile": row.pop("planner_profile", "classic"),
            "intent": row.pop("planner_intent", ""),
            "phrase_protected": row.pop("phrase_protected", False),
        },
        "beat_grid": {
            "grid_ms": row.pop("beat_grid_ms", 0),
            "cell_index": row.pop("beat_grid_cell_index", -1),
            "offset_ms": row.pop("beat_grid_offset_ms", 0),
        },
    }
    row["event_index"] = index
    row["start_sec"] = round(event.start_ms / 1000.0, 3)
    row["end_sec"] = round(event.end_ms / 1000.0, 3)
    row["fragment_duration_sec"] = round(event.fragment_duration_ms / 1000.0, 3)
    row["transform_tags"] = [tag for tag in event.transformation.split("+") if tag]
    row["has_cue"] = bool(event.source_cue_end_ms > event.source_cue_start_ms)
    row["planner"] = planner
    return row


def section_summary_rows(events: List[Event], total_ms: int) -> List[Dict[str, object]]:
    by_section: Dict[str, List[Event]] = {name: [] for name in SECTION_NAMES}
    for event in events:
        by_section.setdefault(event.section, []).append(event)
    windows = {row["name"]: row for row in section_window_rows(total_ms)}
    rows = []
    for name in SECTION_NAMES:
        items = sorted(by_section.get(name, []), key=lambda event: (event.start_ms, event.layer, event.source_basename))
        window = windows[name]
        layer_counts = Counter(event.layer for event in items)
        source_counts = Counter(event.source_basename for event in items)
        transformation_counts = Counter(event.transformation for event in items)
        fragment_total = sum(event.fragment_duration_ms for event in items)
        rows.append(
            {
                "name": name,
                "start_ms": window["start_ms"],
                "end_ms": window["end_ms"],
                "event_count": len(items),
                "layer_counts": sorted_count_map(layer_counts),
                "top_sources": top_count_rows(source_counts, limit=5),
                "top_transformations": top_count_rows(transformation_counts, limit=5),
                "memory_event_count": sum(1 for event in items if event.from_memory),
                "average_fragment_ms": round(fragment_total / len(items), 2) if items else 0.0,
            }
        )
    return rows


def build_audio_plan(
    variant_name: str,
    events: List[Event],
    args: argparse.Namespace,
    total_ms: int,
    min_frag_ms: int,
    max_frag_ms: int,
) -> Dict[str, object]:
    layer_counts = Counter(event.layer for event in events)
    section_counts = Counter(event.section for event in events)
    source_counts = Counter(event.source_basename for event in events)
    transformation_counts = Counter(event.transformation for event in events)
    return {
        "kind": "cutups.audio_composition_plan",
        "version": AUDIO_PLAN_VERSION,
        "variant": variant_name,
        "seed": int(args.seed),
        "preset": str(args.preset or ""),
        "mode": str(args.mode),
        "duration_ms": total_ms,
        "config": {
            "density": str(args.density),
            "sectional": bool(args.sectional),
            "section_arc": section_arc_name(args),
            "planner_profile": planner_profile_name(args),
            "arrangement_style": str(args.arrangement_style),
            "source_score": source_score_mode(args),
            "effective_source_score": effective_source_score_mode(args),
            "source_diversity": float(args.source_diversity),
            "source_manifest": str(getattr(args, "source_manifest", "") or ""),
            "source_manifest_matches": int(getattr(args, "source_manifest_matches", 0) or 0),
            "concrete": bool(args.concrete),
            "bed_noise": bool(args.bed_noise),
            "baseline_beat": str(baseline_beat_path(args) or ""),
            "baseline_beat_gain": float(getattr(args, "baseline_beat_gain", -9.0)),
            "baseline_beat_bars": float(getattr(args, "baseline_beat_bars", 0.0)),
            "baseline_beat_duck_db": float(getattr(args, "baseline_beat_duck_db", 0.0)),
            "baseline_beat_duck_ms": int(getattr(args, "baseline_beat_duck_ms", 80)),
            "baseline_beat_duck_windows": int(getattr(args, "baseline_beat_duck_windows", 0) or 0),
            "baseline_placement": baseline_placement_mode(args),
            "baseline_beat_source_duration_ms": int(getattr(args, "baseline_beat_source_duration_ms", 0) or 0),
            "baseline_beat_inferred_bpm": float(getattr(args, "baseline_beat_inferred_bpm", 0.0) or 0.0),
            "sample_rate": int(args.sample_rate),
            "master_gain": float(args.master_gain),
            "semi_live": bool(getattr(args, "semi_live", False)),
            "semi_live_chunk_sec": float(getattr(args, "semi_live_chunk_sec", 0.0) or 0.0),
            "semi_live_track": str(getattr(args, "semi_live_track_resolved", "") or getattr(args, "semi_live_track", "") or ""),
            "min_frag_ms": min_frag_ms,
            "max_frag_ms": max_frag_ms,
            "bpm": float(args.bpm),
            "slice_grid": str(args.slice_grid),
            "beat_grid_ms": beat_grid_ms(args),
            "beat_jump_mode": str(args.beat_jump_mode),
            "beat_similarity_weight": float(args.beat_similarity_weight),
            "beat_novelty": float(args.beat_novelty),
        },
        "summary": {
            "event_count": len(events),
            "source_count": len({event.source for event in events}),
            "memory_event_count": sum(1 for event in events if event.from_memory),
            "cue_event_count": sum(1 for event in events if event.source_cue_end_ms > event.source_cue_start_ms),
            "phrase_protected_event_count": sum(1 for event in events if event.phrase_protected),
            "grid_aligned_event_count": sum(1 for event in events if event.beat_grid_ms > 0 and event.beat_grid_offset_ms == 0),
            "layer_counts": sorted_count_map(layer_counts),
            "section_counts": sorted_count_map(section_counts),
            "top_sources": top_count_rows(source_counts, limit=8),
            "top_transformations": top_count_rows(transformation_counts, limit=8),
        },
        "section_windows": section_window_rows(total_ms),
        "section_targets": section_target_rows(args, total_ms),
        "baseline_grid": getattr(args, "baseline_grid_summary", {}),
        "sections": section_summary_rows(events, total_ms),
        "events": [event_plan_row(event, index) for index, event in enumerate(sorted(events, key=lambda event: (event.start_ms, event.layer, event.source_basename)))],
        "composition_notes": [
            "This plan records the event decisions used to render the audio.",
            "Future planner passes can score, reorder, or mutate this structure before rendering.",
        ],
    }


def export_audio_plan(path: Path, plan: Dict[str, object]) -> None:
    path.write_text(json.dumps(plan, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def build_section_score(events: List[Event]) -> str:
    if not events:
        return "NO EVENTS\n"
    by_section: Dict[str, List[Event]] = {name: [] for name in SECTION_NAMES}
    for e in events:
        by_section.setdefault(e.section, []).append(e)

    lines: List[str] = ["CUTUP AUDIO SCORE", "ARCHIVE OF AESTHETIC POSSIBILITY", ""]
    for sec in SECTION_NAMES:
        items = sorted(by_section.get(sec, []), key=lambda ev: (ev.start_ms, ev.layer))
        if not items:
            continue
        layer_counts = Counter(ev.layer for ev in items)
        transformations = Counter(ev.transformation for ev in items)
        insistence = sum(1 for ev in items if ev.repeated >= 3 or ev.from_memory)
        dominant_sources = Counter(ev.source_basename for ev in items).most_common(3)
        source_phrase = ", ".join(name for name, _ in dominant_sources) if dominant_sources else "none"
        top_transform = transformations.most_common(1)[0][0] if transformations else "slice"

        lines.append(f"[{sec}]  events={len(items)}  insistence={insistence}  dominant_transform={top_transform}")
        lines.append(f"  layers: main={layer_counts.get('voice_main', 0)} cuts={layer_counts.get('voice_cuts', 0)} ghosts={layer_counts.get('ghosts', 0)}")
        lines.append(f"  recurring sources: {source_phrase}")

        highlights = sorted(items, key=lambda ev: (ev.recurrence_index, ev.repeated, ev.fragment_duration_ms), reverse=True)[:3]
        for h in highlights:
            mm = int(h.start_ms // 60000)
            ss = int((h.start_ms % 60000) // 1000)
            ms = int(h.start_ms % 1000)
            stamp = f"{mm:02d}:{ss:02d}.{ms:03d}"
            lines.append(
                f"    - {stamp} {h.layer_role.upper()} {h.source_basename} "
                f"x{h.repeated} rec#{h.recurrence_index} {h.transformation}"
            )
        lines.append("")

    lines.extend([
        "COMPOSITION NOTE:",
        "Each recurrence is an argument with itself; each ghost return is a short-circuit in control speech.",
    ])
    return "\n".join(lines).strip() + "\n"


def place_events(
    samples: List[SampleFile],
    total_ms: int,
    args: argparse.Namespace,
    min_frag_ms: int,
    max_frag_ms: int,
    live: Optional[LiveControlState] = None,
    beat_jump: Optional[BeatJumpState] = None,
    baseline_grid: Optional[Dict[str, object]] = None,
    progress: Optional[ProgressReporter] = None,
    progress_span: Tuple[float, float] = (0.0, 1.0),
    progress_label: str = "",
) -> Tuple[AudioSegment, AudioSegment, AudioSegment, List[Event]]:
    voice_main = AudioSegment.silent(duration=total_ms, frame_rate=args.sample_rate).set_channels(2)
    voice_cuts = AudioSegment.silent(duration=total_ms, frame_rate=args.sample_rate).set_channels(2)
    ghosts = AudioSegment.silent(duration=total_ms, frame_rate=args.sample_rate).set_channels(2)
    events: List[Event] = []
    memory: Deque[SampleFile] = deque(maxlen=max(1, args.memory_depth))
    recurrence_memory: Deque[Tuple[SampleFile, AudioSegment, Dict[str, object], str]] = deque(maxlen=max(3, args.memory_depth * 2))
    recurrence_count: Dict[str, int] = {}
    source_counts: Counter = Counter()
    recent_source_keys: Deque[str] = deque(maxlen=max(3, min(12, args.memory_depth)))
    previous_source_sample: Optional[SampleFile] = None
    plan = section_plan(total_ms)
    grid_ms = beat_grid_ms(args)

    n_events = choose_event_count(total_ms / 1000.0, args.density, args.sectional)
    current_anchor = 0
    dead_air_windows: List[Tuple[int, int]] = []
    if args.sectional:
        n_windows = random.randint(2, 5)
        for _ in range(n_windows):
            start = random.randint(0, max(0, total_ms - 1200))
            dur = random.randint(140, 1800)
            dead_air_windows.append((start, min(total_ms, start + dur)))
        for _, (sec_start, sec_end) in plan.items():
            if random.random() < 0.66:
                width = random.randint(120, 980)
                center = random.randint(sec_start, max(sec_start, sec_end - 1))
                dead_air_windows.append((max(0, center - width // 2), min(total_ms, center + width // 2)))

    def in_dead_air(position_ms: int) -> bool:
        return any(a <= position_ms <= b for a, b in dead_air_windows)

    for i in range(n_events):
        if progress and (i == 0 or i + 1 == n_events or i % 10 == 0):
            progress.update_span(progress_span, i / max(1, n_events), "audio", f"{progress_label} placing events {i + 1}/{n_events}".strip())
        runtime = runtime_snapshot(args, live)
        local_args = apply_runtime_params(args, runtime)
        section_progress = i / max(1, n_events - 1)
        profile = section_profile(section_progress, local_args) if local_args.sectional else {"name": "BUILD", "dens": 1.0, "frag_mul": 1.0, "repeat": 0.2, "reverse": 0.18, "filt": 0.6, "silence": local_args.silence_prob, "ghost": local_args.ghost_prob}
        if runtime.force_section and (runtime.hold_section or runtime.burst_now):
            profile = section_profile(SECTION_PROGRESS[runtime.force_section], local_args)
            profile["name"] = runtime.force_section
        profile = workflow_audio_profile(profile, local_args)

        sec_name = str(profile["name"])
        planner_name = planner_profile_name(local_args)
        planner_intent = section_planner_intent(sec_name, planner_name)
        sec_span = plan.get(sec_name, (0, total_ms))
        memory_bias = local_args.recurrence_prob + (0.14 if sec_name in {"COLLAPSE", "AFTERIMAGE"} else (0.08 if sec_name == "PRESSURE" else 0.0))
        use_recurrence_fragment = bool(recurrence_memory and random.random() < clamp(memory_bias * (1.4 if sec_name in {"PRESSURE", "COLLAPSE"} else 1.0), 0.0, 0.97))
        from_memory = False
        selection_reason = "weighted_source"
        if use_recurrence_fragment:
            sample, shaped, meta, _ = random.choice(list(recurrence_memory))
            from_memory = True
            selection_reason = "recurrence_fragment"
            event_profile = dict(profile)
            phrase_protected = bool(meta.get("phrase_protected", False))
            if random.random() < 0.34:
                shaped = change_speed(shaped, random.choice([0.84, 0.92, 1.05, 1.16]))
            if random.random() < 0.38:
                shaped = low_pass_filter(shaped, random.choice([1500, 2100, 2800]))
            if random.random() < 0.28:
                shaped = shaped.reverse()
            meta = dict(meta)
            meta["transformation"] = f"{meta.get('transformation', 'slice')}+memory"
        else:
            if memory and random.random() < clamp(memory_bias, 0.0, 0.95):
                sample = random.choice(list(memory))
                selection_reason = "source_memory"
            else:
                before_jumps = beat_jump.selections if beat_jump else 0
                before_fallbacks = beat_jump.fallbacks if beat_jump else 0
                sample = choose_source_sample(
                    samples,
                    local_args,
                    local_args.concrete,
                    beat_jump=beat_jump,
                    previous_sample=previous_source_sample,
                    source_counts=source_counts,
                    recent_source_keys=list(recent_source_keys),
                    profile=profile,
                )
                if beat_jump and beat_jump.selections > before_jumps:
                    selection_reason = "similarity_neighbor"
                elif beat_jump and beat_jump.fallbacks > before_fallbacks:
                    selection_reason = "weighted_source_fallback"
            event_profile, phrase_protected = event_audio_profile(profile, local_args, sample)
            src = source_audio_for_sample(sample, local_args)
            if sample.has_cue() and str(getattr(local_args, "cue_slice_mode", "full")) == "full":
                frag = src
            else:
                frag = safe_slice_fragment(src, min_frag_ms, max_frag_ms, float(event_profile["frag_mul"]), grid_ms=grid_ms)
            shaped, meta = shape_fragment(frag, event_profile, local_args)
            meta = dict(meta)
            meta["phrase_protected"] = phrase_protected
            if grid_ms > 0:
                meta["transformation"] = f"{meta.get('transformation', 'slice')}+grid"
        recurrence_count[str(sample.path)] = recurrence_count.get(str(sample.path), 0) + 1

        if planner_name == "phrase" and phrase_protected:
            layer_weights = [7, 2, 1]
        elif planner_name == "beat":
            layer_weights = [2, 6, 2]
        elif planner_name == "breach":
            layer_weights = [2, 5, 4]
        else:
            layer_weights = [3, 4, 4] if profile["name"] in {"COLLAPSE", "AFTERIMAGE"} else ([4, 3, 3] if local_args.arrangement_style == "swarm" else [5, 2, 2])
        layer = random.choices(
            ["voice_main", "voice_cuts", "ghosts"],
            weights=layer_weights,
            k=1,
        )[0]
        if random.random() < profile["ghost"]:
            layer = "ghosts"
        if sec_name == "AFTERIMAGE" and random.random() < 0.58:
            layer = "ghosts"

        if local_args.arrangement_style == "collapse":
            if grid_ms > 0:
                jitter = grid_ms * random.choice([-1, 0, 0, 1])
                step = grid_ms * random.choice([1, 1, 2, 3, 4])
            else:
                jitter = random.randint(-120, 520)
                step = max(40, int((900 if profile["name"] == "PRESSURE" else 1300) * float(profile["dens"])))
        else:
            if grid_ms > 0:
                jitter = grid_ms * random.choice([-2, -1, 0, 0, 1, 2])
                step = grid_ms * random.choice([1, 2, 2, 4, 8])
            else:
                jitter = random.randint(-300, 820)
                step = max(70, int(random.randint(350, 2200) / max(0.4, float(profile["dens"]))))
        if planner_name == "phrase" and phrase_protected:
            jitter = random.randint(80, 640)
            step = max(step, len(shaped) + silence_duration_ms(local_args, (90, 320)))
        elif planner_name == "beat" and grid_ms > 0:
            step = max(grid_ms, quantize_to_grid(step, grid_ms) or grid_ms)

        pos = random.randint(0, 500) if i == 0 else max(0, current_anchor + jitter)
        if grid_ms > 0:
            pos = quantize_to_grid(pos, grid_ms)
        pos = clamp_to_section(pos, sec_span, len(shaped), grid_ms=grid_ms)
        if in_dead_air(pos):
            # honor silence windows as structural punctuation.
            move = random.randint(220, 1400)
            if sec_name in {"COLLAPSE", "AFTERIMAGE"} and random.random() < 0.6:
                pos = clamp_to_section(max(0, pos - move // 2), sec_span, len(shaped), grid_ms=grid_ms)
            else:
                pos = clamp_to_section(min(total_ms - 1, pos + move), sec_span, len(shaped), grid_ms=grid_ms)

        if pos + len(shaped) >= total_ms:
            pos = max(0, total_ms - len(shaped) - random.randint(10, 220))
        if grid_ms > 0:
            pos = quantize_to_grid(pos, grid_ms)
        pos = clamp_to_section(pos, sec_span, len(shaped), grid_ms=grid_ms)
        placement_info = {"mode": "any", "original_start_ms": pos, "cell_index": -1, "cell_energy": 0.0}
        if baseline_grid:
            event_baseline_grid = dict(baseline_grid)
            event_baseline_grid["mode"] = baseline_placement_mode(local_args)
            pos, placement_info = apply_baseline_placement(pos, len(shaped), sec_span, total_ms, grid_ms, event_baseline_grid)
        current_anchor = min(total_ms - 50, pos + step)

        if runtime.panic_silence and random.random() < 0.55:
            current_anchor = min(total_ms - 50, current_anchor + random.randint(180, 900))
            continue

        gain = random.uniform(-10.0, -2.5) if layer == "voice_main" else random.uniform(-13.0, -5.5) if layer == "voice_cuts" else random.uniform(-18.0, -8.0)
        if profile["name"] in {"COLLAPSE", "AFTERIMAGE"}:
            gain -= 1.5
        placed = shaped.apply_gain(gain)
        if layer == "voice_main":
            voice_main = voice_main.overlay(placed, position=pos)
        elif layer == "voice_cuts":
            voice_cuts = voice_cuts.overlay(placed, position=pos)
        else:
            if random.random() < 0.45:
                placed = low_pass_filter(placed, random.choice([1600, 2200, 3000]))
            ghosts = ghosts.overlay(placed, position=pos)

        if local_args.sectional and random.random() < (0.18 if sec_name not in {"COLLAPSE", "AFTERIMAGE"} else 0.3) and recurrence_count[str(sample.path)] > 1:
            # ghost return: delayed, filtered recurrence of same material.
            back_pos = min(total_ms - 1, pos + random.randint(160, 2600))
            ghost_copy = low_pass_filter(shaped.reverse(), random.choice([1200, 1800, 2400])).apply_gain(random.uniform(-15, -9))
            ghosts = ghosts.overlay(ghost_copy, position=back_pos)

        if sec_name in {"PRESSURE", "COLLAPSE"} and random.random() < 0.2:
            # insistence burst: command cell repeats as abrupt authority punctuation.
            cell_len = min(len(shaped), random.randint(40, 180))
            if cell_len > 20:
                cell = shaped[:cell_len]
                echoes = random.randint(2, 5)
                cursor = pos + random.randint(35, 260)
                for _ in range(echoes):
                    if cursor >= total_ms - 20:
                        break
                    variant = cell.reverse() if random.random() < 0.22 else cell
                    variant = low_pass_filter(variant, random.choice([1400, 1800, 2400]))
                    ghosts = ghosts.overlay(variant.apply_gain(random.uniform(-16, -10)), position=cursor)
                    cursor += random.randint(30, 190)

        if sec_name in {"COLLAPSE", "AFTERIMAGE"} and random.random() < 0.16:
            # sudden single-word return: tiny direct restatement in the center channel.
            blip_len = min(len(shaped), random.randint(24, 130))
            if blip_len > 12:
                blip_start = random.randint(0, max(0, len(shaped) - blip_len))
                blip = shaped[blip_start : blip_start + blip_len]
                blip_pos = clamp_to_section(pos + random.randint(120, 1100), sec_span, len(blip))
                voice_main = voice_main.overlay(blip.apply_gain(random.uniform(-9, -3)), position=blip_pos)

        memory.append(sample)
        recurrence_memory.append((sample, shaped, meta, sec_name))
        source_key = source_balance_key(sample)
        selection_debug = source_selection_diagnostics(
            sample,
            local_args,
            local_args.concrete,
            source_counts=source_counts,
            recent_source_keys=list(recent_source_keys),
            previous_sample=previous_source_sample,
            profile=event_profile,
            reason=selection_reason,
        )
        previous_source_sample = sample
        source_counts[source_key] += 1
        recent_source_keys.append(source_key)
        rec_idx = recurrence_count[str(sample.path)]
        beat_cell_index = int(pos // grid_ms) if grid_ms > 0 else -1
        beat_offset_ms = int(pos % grid_ms) if grid_ms > 0 else 0
        events.append(
            Event(
                layer=layer,
                section=str(profile["name"]),
                source=str(sample.path),
                source_basename=sample.path.name,
                source_duration_ms=sample.duration_ms,
                source_cue_start_ms=sample.cue_start_ms,
                source_cue_end_ms=sample.cue_end_ms,
                source_cue_text=sample.cue_text,
                source_manifest_tags=sample.manifest_tags,
                source_manifest_role=sample.manifest_role,
                source_manifest_weight=round(float(sample.manifest_weight), 6),
                start_ms=pos,
                end_ms=pos + len(shaped),
                fragment_duration_ms=len(shaped),
                gain_db=round(gain, 2),
                reversed=bool(meta["reversed"]),
                speed=float(meta["speed"]),
                repeated=int(meta["repeated"]),
                hp_hz=int(meta["hp_hz"]),
                lp_hz=int(meta["lp_hz"]),
                grain_mode=bool(meta["grain_mode"]),
                from_memory=from_memory,
                transformation=str(meta["transformation"]),
                layer_role="foreground" if layer == "voice_main" else "rhythmic" if layer == "voice_cuts" else "ghost",
                recurrence_index=rec_idx,
                baseline_placement_mode=str(placement_info.get("mode", "any")),
                baseline_placement_original_start_ms=int(placement_info.get("original_start_ms", pos)),
                baseline_placement_cell_index=int(placement_info.get("cell_index", -1)),
                baseline_placement_cell_energy=float(placement_info.get("cell_energy", 0.0)),
                planner_profile=planner_name,
                planner_intent=planner_intent,
                phrase_protected=phrase_protected,
                beat_grid_ms=grid_ms,
                beat_grid_cell_index=beat_cell_index,
                beat_grid_offset_ms=beat_offset_ms,
                **selection_debug,
            )
        )

        if live and live.enabled and (i == 0 or i == n_events - 1 or i % 20 == 0):
            live.telemetry(
                "audio_event",
                idx=i,
                n_events=n_events,
                section=sec_name,
                pos_ms=pos,
                layer=layer,
                from_memory=from_memory,
                recurrence_index=rec_idx,
                force_section=runtime.force_section,
                hold_section=runtime.hold_section,
                burst_now=runtime.burst_now,
                panic_silence=runtime.panic_silence,
                burst_rate=local_args.burst_rate,
                dropout_rate=local_args.dropout_rate,
                reverse_shard_rate=local_args.reverse_shard_rate,
                filter_severity=local_args.filter_severity,
                stutter_rate=local_args.stutter_rate,
                mute_rate=local_args.mute_rate,
                repeat_rate=local_args.repeat_rate,
                beat_dropout_rate=local_args.beat_dropout_rate,
                source_diversity=local_args.source_diversity,
                section_arc=section_arc_name(local_args),
                source_score=source_score_mode(local_args),
                planner_profile=planner_name,
                planner_intent=planner_intent,
                phrase_protected=phrase_protected,
                beat_grid_cell_index=beat_cell_index,
                baseline_placement=str(placement_info.get("mode", "any")),
                baseline_cell_index=int(placement_info.get("cell_index", -1)),
                baseline_cell_energy=float(placement_info.get("cell_energy", 0.0)),
            )

    if progress:
        progress.update_span(progress_span, 1.0, "audio", f"{progress_label} events placed".strip(), force=True)
    return voice_main, voice_cuts, ghosts, events


def normalize_master(audio: AudioSegment, master_gain: float) -> AudioSegment:
    return compress_dynamic_range(audio, threshold=-22.0, ratio=2.4, attack=8, release=140).apply_gain(master_gain)


def mix_master_layers(
    main: AudioSegment,
    cuts: AudioSegment,
    ghosts: AudioSegment,
    hiss: AudioSegment,
    baseline_bed: Optional[AudioSegment],
    args: argparse.Namespace,
) -> AudioSegment:
    total_ms = max(len(main), len(cuts), len(ghosts), len(hiss), len(baseline_bed) if baseline_bed is not None else 0)
    master = AudioSegment.silent(duration=total_ms, frame_rate=args.sample_rate).set_channels(2)
    master = master.overlay(hiss[:total_ms], position=0)
    if baseline_bed is not None:
        master = master.overlay(baseline_bed[:total_ms], position=0)
    master = master.overlay(ghosts[:total_ms] - 2, position=0).overlay(cuts[:total_ms] + 1, position=0).overlay(main[:total_ms] + 2, position=0)
    return normalize_master(master, args.master_gain)


def offset_event(event: Event, offset_ms: int) -> Event:
    data = dict(event.__dict__)
    for key in ("start_ms", "end_ms"):
        data[key] = int(data.get(key, 0)) + offset_ms
    data["baseline_placement_original_start_ms"] = int(data.get("baseline_placement_original_start_ms", 0)) + offset_ms
    return Event(**data)


def resolve_semi_live_track_path(args: argparse.Namespace, variant_dir: Path, variant_name: str) -> Path:
    raw = str(getattr(args, "semi_live_track", "") or "").strip()
    if not raw:
        return variant_dir / f"{variant_name}_live_track.wav"
    path = Path(raw).expanduser().resolve()
    if variant_name != "cutup_01":
        path = path.with_name(f"{path.stem}_{variant_name}{path.suffix}")
    return path


def build_variant(
    samples: List[SampleFile],
    output_root: Path,
    variant_idx: int,
    args: argparse.Namespace,
    summary: RunSummary,
    live: Optional[LiveControlState] = None,
    beat_jump: Optional[BeatJumpState] = None,
    baseline_beat: Optional[BaselineBeat] = None,
    progress: Optional[ProgressReporter] = None,
    progress_span: Tuple[float, float] = (0.0, 1.0),
) -> None:
    total_ms = max(2000, int(max(1.0, args.duration) * 1000))
    min_frag_ms = max(10, int(max(0.01, args.min_frag) * 1000))
    max_frag_ms = max(min_frag_ms, int(max(args.min_frag, args.max_frag) * 1000))

    variant_name = f"cutup_{variant_idx:02d}"
    variant_dir = output_root / variant_name
    stems_dir = variant_dir / "stems"
    stems_dir.mkdir(parents=True, exist_ok=True)

    if progress:
        progress.update_span(progress_span, 0.0, "audio", f"{variant_name} preparing", force=True)
    baseline_bed = render_baseline_beat_bed(baseline_beat, total_ms) if baseline_beat else None
    baseline_profile = baseline_grid_profile(baseline_bed, args, total_ms)
    setattr(args, "baseline_grid_summary", baseline_profile.get("summary", {}))
    main, cuts, ghosts, events = place_events(
        samples,
        total_ms,
        args,
        min_frag_ms,
        max_frag_ms,
        live=live,
        beat_jump=beat_jump,
        baseline_grid=baseline_profile,
        progress=progress,
        progress_span=progress_child_span(progress_span, 0.05, 0.72),
        progress_label=variant_name,
    )
    if progress:
        progress.update_span(progress_span, 0.75, "audio", f"{variant_name} mixing layers", force=True)
    hiss = make_hiss(total_ms, args.sample_rate) if args.bed_noise else AudioSegment.silent(duration=total_ms, frame_rate=args.sample_rate)
    baseline_duck_count = 0
    if baseline_bed is not None:
        baseline_bed, baseline_duck_count = duck_baseline_beat_bed(
            baseline_bed,
            events,
            float(getattr(args, "baseline_beat_duck_db", 0.0) or 0.0),
            int(getattr(args, "baseline_beat_duck_ms", 80) or 0),
        )
    setattr(args, "baseline_beat_duck_windows", baseline_duck_count)

    master = mix_master_layers(main, cuts, ghosts, hiss, baseline_bed, args)

    if progress:
        progress.update_span(progress_span, 0.84, "audio", f"{variant_name} exporting stems", force=True)
    main.export(stems_dir / "voice_main.wav", format="wav")
    cuts.export(stems_dir / "voice_cuts.wav", format="wav")
    ghosts.export(stems_dir / "ghosts.wav", format="wav")
    hiss.export(stems_dir / "hiss_bed.wav", format="wav")
    if baseline_bed is not None:
        baseline_bed.export(stems_dir / "baseline_beat.wav", format="wav")
    master_path = variant_dir / f"{variant_name}_master.wav"
    event_path = variant_dir / f"{variant_name}_events.csv"
    plan_path = variant_dir / f"{variant_name}_plan.json"
    score_path = variant_dir / f"{variant_name}_score.txt"
    if progress:
        progress.update_span(progress_span, 0.9, "audio", f"{variant_name} exporting master", force=True)
    master.export(master_path, format="wav")
    preview_path: Optional[Path] = None
    preview_ms = preview_duration_ms(args, len(master))
    if preview_ms > 0:
        preview_path = variant_dir / f"{variant_name}_preview.wav"
        master[:preview_ms].export(preview_path, format="wav")
    if progress:
        progress.update_span(progress_span, 0.96, "audio", f"{variant_name} writing plan", force=True)
    export_manifest(event_path, events)
    export_audio_plan(plan_path, build_audio_plan(variant_name, events, args, total_ms, min_frag_ms, max_frag_ms))
    score_path.write_text(build_section_score(events), encoding="utf-8")

    summary.audio_events += len(events)
    summary.section_distribution.update([e.section for e in events])
    summary.recurring_sources.update([e.source_basename for e in events if e.recurrence_index > 1])
    summary.output_paths.extend([str(master_path), str(event_path), str(plan_path), str(score_path)])
    if preview_path:
        summary.output_paths.append(str(preview_path))
    if progress:
        progress.update_span(progress_span, 1.0, "audio", f"{variant_name} complete", force=True)


def build_semi_live_variant(
    samples: List[SampleFile],
    output_root: Path,
    variant_idx: int,
    args: argparse.Namespace,
    summary: RunSummary,
    live: Optional[LiveControlState] = None,
    beat_jump: Optional[BeatJumpState] = None,
    baseline_beat: Optional[BaselineBeat] = None,
    progress: Optional[ProgressReporter] = None,
    progress_span: Tuple[float, float] = (0.0, 1.0),
) -> None:
    total_ms = max(2000, int(max(1.0, args.duration) * 1000))
    chunk_ms = max(1000, min(total_ms, int(round(float(args.semi_live_chunk_sec) * 1000))))
    chunk_count = max(1, int(math.ceil(total_ms / float(chunk_ms))))
    min_frag_ms = max(10, int(max(0.01, args.min_frag) * 1000))
    max_frag_ms = max(min_frag_ms, int(max(args.min_frag, args.max_frag) * 1000))

    variant_name = f"cutup_{variant_idx:02d}"
    variant_dir = output_root / variant_name
    stems_dir = variant_dir / "stems"
    chunks_dir = variant_dir / "chunks"
    stems_dir.mkdir(parents=True, exist_ok=True)
    chunks_dir.mkdir(parents=True, exist_ok=True)

    live_track_path = resolve_semi_live_track_path(args, variant_dir, variant_name)
    if live_track_path.exists() and not args.overwrite:
        raise SystemExit(f"--semi-live-track already exists: {live_track_path}. Pass --overwrite or choose a new path.")
    live_track_path.parent.mkdir(parents=True, exist_ok=True)
    setattr(args, "semi_live_track_resolved", str(live_track_path))

    if progress:
        progress.update_span(progress_span, 0.0, "audio", f"{variant_name} semi-live preparing", force=True)

    baseline_bed_full = render_baseline_beat_bed(baseline_beat, total_ms) if baseline_beat else None
    full_baseline_profile = baseline_grid_profile(baseline_bed_full, args, total_ms)

    main_stem = AudioSegment.silent(duration=0, frame_rate=args.sample_rate).set_channels(2)
    cuts_stem = AudioSegment.silent(duration=0, frame_rate=args.sample_rate).set_channels(2)
    ghosts_stem = AudioSegment.silent(duration=0, frame_rate=args.sample_rate).set_channels(2)
    hiss_stem = AudioSegment.silent(duration=0, frame_rate=args.sample_rate).set_channels(2)
    baseline_stem = AudioSegment.silent(duration=0, frame_rate=args.sample_rate).set_channels(2) if baseline_bed_full is not None else None
    live_track = AudioSegment.silent(duration=0, frame_rate=args.sample_rate).set_channels(2)
    all_events: List[Event] = []
    chunk_rows: List[Dict[str, object]] = []
    baseline_duck_count_total = 0

    for chunk_idx in range(1, chunk_count + 1):
        chunk_start = (chunk_idx - 1) * chunk_ms
        chunk_len = min(chunk_ms, total_ms - chunk_start)
        if chunk_len <= 0:
            continue
        chunk_name = f"{variant_name}_chunk_{chunk_idx:03d}"
        chunk_args = argparse.Namespace(**vars(args))
        chunk_args.duration = chunk_len / 1000.0
        chunk_progress = progress_child_span(progress_span, 0.05 + 0.70 * ((chunk_idx - 1) / chunk_count), 0.05 + 0.70 * (chunk_idx / chunk_count))
        if progress:
            progress.update_span(progress_span, 0.05 + 0.70 * ((chunk_idx - 1) / chunk_count), "audio", f"{chunk_name} rendering", force=True)

        baseline_chunk = baseline_bed_full[chunk_start : chunk_start + chunk_len] if baseline_bed_full is not None else None
        baseline_profile = baseline_grid_profile(baseline_chunk, chunk_args, chunk_len)
        setattr(chunk_args, "baseline_grid_summary", baseline_profile.get("summary", {}))
        main, cuts, ghosts, events = place_events(
            samples,
            chunk_len,
            chunk_args,
            min_frag_ms,
            max_frag_ms,
            live=live,
            beat_jump=beat_jump,
            baseline_grid=baseline_profile,
            progress=progress,
            progress_span=chunk_progress,
            progress_label=chunk_name,
        )
        hiss = make_hiss(chunk_len, args.sample_rate) if args.bed_noise else AudioSegment.silent(duration=chunk_len, frame_rate=args.sample_rate).set_channels(2)
        baseline_duck_count = 0
        if baseline_chunk is not None:
            baseline_chunk, baseline_duck_count = duck_baseline_beat_bed(
                baseline_chunk,
                events,
                float(getattr(args, "baseline_beat_duck_db", 0.0) or 0.0),
                int(getattr(args, "baseline_beat_duck_ms", 80) or 0),
            )
        baseline_duck_count_total += baseline_duck_count
        chunk_master = mix_master_layers(main, cuts, ghosts, hiss, baseline_chunk, args)[:chunk_len]
        chunk_path = chunks_dir / f"{chunk_name}.wav"
        chunk_master.export(chunk_path, format="wav")

        main_stem += main[:chunk_len]
        cuts_stem += cuts[:chunk_len]
        ghosts_stem += ghosts[:chunk_len]
        hiss_stem += hiss[:chunk_len]
        if baseline_stem is not None and baseline_chunk is not None:
            baseline_stem += baseline_chunk[:chunk_len]
        live_track += chunk_master
        live_track.export(live_track_path, format="wav")

        offset_events = [offset_event(event, chunk_start) for event in events]
        all_events.extend(offset_events)
        chunk_row = {
            "chunk_index": chunk_idx,
            "chunk_count": chunk_count,
            "start_ms": chunk_start,
            "end_ms": chunk_start + chunk_len,
            "duration_ms": chunk_len,
            "chunk_path": str(chunk_path),
            "live_track_path": str(live_track_path),
            "rendered_ms": len(live_track),
            "event_count": len(events),
            "complete": chunk_idx == chunk_count,
        }
        chunk_rows.append(chunk_row)
        if live and live.enabled:
            live.telemetry("semi_live_chunk", variant=variant_name, **chunk_row)
        if progress:
            progress.update_span(progress_span, 0.05 + 0.70 * (chunk_idx / chunk_count), "audio", f"{chunk_name} added to live track", force=True)

    setattr(args, "baseline_grid_summary", full_baseline_profile.get("summary", {}))
    setattr(args, "baseline_beat_duck_windows", baseline_duck_count_total)

    if progress:
        progress.update_span(progress_span, 0.82, "audio", f"{variant_name} exporting stems", force=True)
    main_stem[:total_ms].export(stems_dir / "voice_main.wav", format="wav")
    cuts_stem[:total_ms].export(stems_dir / "voice_cuts.wav", format="wav")
    ghosts_stem[:total_ms].export(stems_dir / "ghosts.wav", format="wav")
    hiss_stem[:total_ms].export(stems_dir / "hiss_bed.wav", format="wav")
    if baseline_stem is not None:
        baseline_stem[:total_ms].export(stems_dir / "baseline_beat.wav", format="wav")

    master_path = variant_dir / f"{variant_name}_master.wav"
    event_path = variant_dir / f"{variant_name}_events.csv"
    plan_path = variant_dir / f"{variant_name}_plan.json"
    score_path = variant_dir / f"{variant_name}_score.txt"
    semi_live_manifest_path = variant_dir / f"{variant_name}_semi_live.json"
    if progress:
        progress.update_span(progress_span, 0.9, "audio", f"{variant_name} finalizing live track", force=True)
    live_track = live_track[:total_ms]
    live_track.export(live_track_path, format="wav")
    live_track.export(master_path, format="wav")
    preview_path: Optional[Path] = None
    preview_ms = preview_duration_ms(args, len(live_track))
    if preview_ms > 0:
        preview_path = variant_dir / f"{variant_name}_preview.wav"
        live_track[:preview_ms].export(preview_path, format="wav")

    if progress:
        progress.update_span(progress_span, 0.96, "audio", f"{variant_name} writing plan", force=True)
    export_manifest(event_path, all_events)
    export_audio_plan(plan_path, build_audio_plan(variant_name, all_events, args, total_ms, min_frag_ms, max_frag_ms))
    score_path.write_text(build_section_score(all_events), encoding="utf-8")
    semi_live_manifest = {
        "kind": "cutups.semi_live_track",
        "version": 1,
        "variant": variant_name,
        "track_path": str(live_track_path),
        "master_path": str(master_path),
        "chunk_sec": float(args.semi_live_chunk_sec),
        "chunk_count": len(chunk_rows),
        "duration_ms": total_ms,
        "chunks": chunk_rows,
    }
    semi_live_manifest_path.write_text(json.dumps(semi_live_manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    summary.audio_events += len(all_events)
    summary.section_distribution.update([event.section for event in all_events])
    summary.recurring_sources.update([event.source_basename for event in all_events if event.recurrence_index > 1])
    summary.output_paths.extend([str(live_track_path), str(master_path), str(event_path), str(plan_path), str(score_path), str(semi_live_manifest_path)])
    if preview_path:
        summary.output_paths.append(str(preview_path))
    if progress:
        progress.update_span(progress_span, 1.0, "audio", f"{variant_name} semi-live complete", force=True)


def run_audio_mode(
    args: argparse.Namespace,
    output_root: Path,
    summary: RunSummary,
    live: Optional[LiveControlState] = None,
    progress: Optional[ProgressReporter] = None,
    progress_span: Tuple[float, float] = (0.0, 1.0),
) -> None:
    if progress:
        progress.update_span(progress_span, 0.0, "audio", "checking backend", force=True)
    ensure_audio_backend()
    if not args.input:
        raise SystemExit("--input is required for --mode audio, --mode both, and --mode all")
    input_root = Path(args.input).expanduser().resolve()
    if not input_root.exists():
        raise SystemExit(f"Input path not found: {input_root}")
    if not input_root.is_dir() and not input_root.is_file():
        raise SystemExit(f"--input must be an audio file or directory: {input_root}")

    baseline_beat = load_baseline_beat(args)
    baseline_exclusions = [baseline_beat.path] if baseline_beat else None
    analysis_cache = resolve_analysis_cache_path(args.analysis_cache, output_root)
    cache_payload: Dict[str, object] = load_analysis_cache(analysis_cache) if analysis_cache else {}
    if progress:
        progress.update_span(progress_span, 0.04, "audio", "discovering source audio", force=True)

    if args.cue_file:
        cue_path = Path(args.cue_file).expanduser().resolve()
        samples, unreadable = discover_cue_samples(input_root, cue_path, exclude_paths=baseline_exclusions)
    else:
        samples, unreadable = discover_samples(
            input_root,
            exclude_paths=baseline_exclusions,
            analysis_cache_payload=cache_payload,
            sample_rate=int(args.sample_rate),
        )

    args.source_manifest_matches = 0
    if args.source_manifest:
        manifest_path = Path(args.source_manifest).expanduser().resolve()
        manifest_entries = load_source_manifest(manifest_path, input_root)
        args.source_manifest_matches = apply_source_manifest(samples, manifest_entries, input_root)
        print(f"Source manifest applied: {manifest_path} (matched={args.source_manifest_matches}/{len(samples)})")

    if not samples:
        hint = f" ({unreadable} files could not be decoded)" if unreadable else ""
        if baseline_beat:
            hint += " after excluding --baseline-beat"
        if args.cue_file:
            raise SystemExit(f"No usable audio cue samples found from {Path(args.cue_file).expanduser().resolve()}{hint}")
        raise SystemExit(f"No usable audio samples found in {input_root}{hint}")
    if unreadable:
        source = Path(args.cue_file).expanduser().resolve() if args.cue_file else input_root
        print(f"Warning: skipped {unreadable} unusable audio/cue row(s) while scanning {source}")
    if progress:
        progress.update_span(progress_span, 0.10, "audio", f"loaded {len(samples)} source item(s)", force=True)

    if analysis_cache and getattr(args, "analysis_cache_readonly", False):
        if not cache_payload:
            print(f"Warning: analysis cache not readable; continuing without cached descriptors: {analysis_cache}")
        elif progress:
            progress.update_span(progress_span, 0.12, "audio", "using readonly analysis cache", force=True)
    elif analysis_cache:
        if progress:
            progress.update_span(progress_span, 0.12, "audio", "writing analysis cache", force=True)
        cache_path = write_analysis_cache(analysis_cache, samples, args, input_root)
        summary.output_paths.append(str(cache_path))
        cache_payload = load_analysis_cache(cache_path)
        cache_stats = cache_payload.get("cache_stats", {}) if cache_payload else {}
        if isinstance(cache_stats, dict):
            print(
                f"Analysis cache written: {cache_path} "
                f"(reused={cache_stats.get('reused', 0)}, refreshed={cache_stats.get('refreshed', 0)})"
            )
        else:
            print(f"Analysis cache written: {cache_path}")

    beat_jump = build_beat_jump_state(samples, args, cache_payload)
    if str(getattr(args, "beat_jump_mode", "random") or "random") == "similarity":
        if beat_jump.active:
            print(f"Beat jump planner active: {len(beat_jump.neighbor_keys)} source(s)")
        else:
            print("Beat jump planner inactive; using weighted random source selection")

    audio_out = output_root / "audio_cutups"
    audio_out.mkdir(parents=True, exist_ok=True)
    variant_count = max(1, args.variants)
    for i in range(1, variant_count + 1):
        runtime = runtime_snapshot(args, live)
        local_args = apply_runtime_params(args, runtime)
        variant_span = progress_child_span(progress_span, 0.16 + 0.84 * ((i - 1) / variant_count), 0.16 + 0.84 * (i / variant_count))
        builder = build_semi_live_variant if bool(getattr(local_args, "semi_live", False)) else build_variant
        builder(
            samples,
            audio_out,
            i,
            local_args,
            summary,
            live=live,
            beat_jump=beat_jump,
            baseline_beat=baseline_beat,
            progress=progress,
            progress_span=variant_span,
        )
    summary.beat_similarity_jumps += beat_jump.selections
    summary.beat_similarity_fallbacks += beat_jump.fallbacks
    if progress:
        progress.update_span(progress_span, 1.0, "audio", "complete", force=True)


# -------------------------------------------------------------------
# EXPORT / DEBUG SUMMARY / MAIN
# -------------------------------------------------------------------


def print_summary(summary: RunSummary) -> None:
    print("\n=== CUTUP RUN SUMMARY ===")
    print(f"Loaded top300/full: {summary.top300_loaded}/{summary.full_loaded}")
    print(f"Skipped top300/full: {summary.top300_skipped}/{summary.full_skipped}")
    print(f"Generated slogans/broadcasts/chants: {summary.slogans}/{summary.broadcasts}/{summary.chants}")
    print(f"Cut-target matches written: {summary.cut_matches}")
    print(f"Audio events placed: {summary.audio_events}")
    if summary.beat_similarity_jumps or summary.beat_similarity_fallbacks:
        print(f"Beat similarity jumps/fallbacks: {summary.beat_similarity_jumps}/{summary.beat_similarity_fallbacks}")
    if summary.section_distribution:
        print("Section distribution:", dict(summary.section_distribution))
    if summary.recurring_sources:
        print("Top recurring sources:", summary.recurring_sources.most_common(5))
    if summary.output_paths:
        print("Outputs:")
        for p in summary.output_paths:
            print(f" - {p}")


def maybe_export_debug_summary(summary: RunSummary, output_root: Path) -> None:
    lines = [
        "CUTUP DEBUG SUMMARY",
        f"top300_loaded={summary.top300_loaded}",
        f"top300_skipped={summary.top300_skipped}",
        f"full_loaded={summary.full_loaded}",
        f"full_skipped={summary.full_skipped}",
        f"slogans={summary.slogans}",
        f"broadcasts={summary.broadcasts}",
        f"chants={summary.chants}",
        f"cut_matches={summary.cut_matches}",
        f"audio_events={summary.audio_events}",
        f"beat_similarity_jumps={summary.beat_similarity_jumps}",
        f"beat_similarity_fallbacks={summary.beat_similarity_fallbacks}",
        f"section_distribution={dict(summary.section_distribution)}",
        f"top_recurring_sources={summary.recurring_sources.most_common(8)}",
        "outputs:",
    ]
    lines.extend(summary.output_paths)
    (output_root / "run_summary.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    args = validate_args(parse_args())
    args.agitprop_personalities = parse_agitprop_personalities(args.agitprop_personality)
    random.seed(args.seed)
    live = build_live_control(args)
    progress = build_progress_reporter(args, live)
    spans = progress_spans(args.mode)

    output_root = resolve_output_root(args.output, args.overwrite)
    if args.dry_run:
        print_dry_run(args, output_root)
        return
    try:
        output_root.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise SystemExit(f"Failed to create output directory '{output_root}': {exc}") from exc
    summary = RunSummary()

    if args.mode == "audio":
        run_audio_mode(args, output_root, summary, live=live, progress=progress, progress_span=spans.get("audio", (0.0, 1.0)))
    elif args.mode == "agitprop":
        run_agitprop_mode(args, output_root, summary, live=live, progress=progress, progress_span=spans.get("agitprop", (0.0, 1.0)))
    elif args.mode == "cuttargets":
        run_cuttargets_mode(args, output_root, summary, progress=progress, progress_span=spans.get("cuttargets", (0.0, 1.0)))
    elif args.mode == "both":
        run_agitprop_mode(args, output_root, summary, live=live, progress=progress, progress_span=spans.get("agitprop", (0.0, 0.18)))
        run_audio_mode(args, output_root, summary, live=live, progress=progress, progress_span=spans.get("audio", (0.18, 1.0)))
    elif args.mode == "all":
        chant_path = run_agitprop_mode(args, output_root, summary, live=live, progress=progress, progress_span=spans.get("agitprop", (0.0, 0.12)))
        run_cuttargets_mode(args, output_root, summary, chant_cells_path=chant_path, progress=progress, progress_span=spans.get("cuttargets", (0.12, 0.22)))
        run_audio_mode(args, output_root, summary, live=live, progress=progress, progress_span=spans.get("audio", (0.22, 1.0)))

    progress.finish()
    print_summary(summary)
    if args.export_debug_summary:
        maybe_export_debug_summary(summary, output_root)


if __name__ == "__main__":
    main()
