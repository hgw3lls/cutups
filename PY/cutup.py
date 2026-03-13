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
import random
import re
import time
from collections import Counter, deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Deque, Dict, Iterable, List, Optional, Sequence, Tuple

AudioSegment: Any = None
compress_dynamic_range: Any = None
high_pass_filter: Any = None
low_pass_filter: Any = None


def ensure_audio_backend() -> None:
    """Load pydub lazily so non-audio flows and --help work without it."""
    global AudioSegment, compress_dynamic_range, high_pass_filter, low_pass_filter
    if AudioSegment is not None:
        return
    if importlib.util.find_spec("pydub") is None:
        raise SystemExit(
            "Audio backend unavailable: install 'pydub' (and ffmpeg) to use --mode audio/both/all."
        )
    pydub = importlib.import_module("pydub")
    effects = importlib.import_module("pydub.effects")
    AudioSegment = pydub.AudioSegment
    compress_dynamic_range = effects.compress_dynamic_range
    high_pass_filter = effects.high_pass_filter
    low_pass_filter = effects.low_pass_filter

# -------------------------------------------------------------------
# CONFIG / CONSTANTS
# -------------------------------------------------------------------

AUDIO_EXTS = {".wav", ".mp3", ".flac", ".aiff", ".ogg", ".m4a"}
TOKEN_RE = re.compile(r"[A-Za-z']+")
SECTION_NAMES = ("ENTRY", "BUILD", "PRESSURE", "COLLAPSE", "AFTERIMAGE")

TEXT_COLUMN_CANDIDATES = ["text", "subtitle", "line", "transcript", "content"]
FILE_COLUMN_CANDIDATES = ["file", "filename", "source_file"]
CLIP_ID_COLUMN_CANDIDATES = ["clip_id", "clip", "id"]
CUE_COLUMN_CANDIDATES = ["cue_index", "cue", "index"]
START_TC_COLUMN_CANDIDATES = ["start_tc", "start", "in", "time_in"]
END_TC_COLUMN_CANDIDATES = ["end_tc", "end", "out", "time_out"]
DURATION_COLUMN_CANDIDATES = ["duration_sec", "duration", "dur"]
SCORE_COLUMN_CANDIDATES = ["score", "rank_score", "weight"]
LOOP_BIN_COLUMN_CANDIDATES = ["loop_bin", "loop", "size_bin"]
INTENSITY_COLUMN_CANDIDATES = ["intensity", "level", "energy"]

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

LIVE_CONTROL_LIMITS: Dict[str, Tuple[float, float]] = {
    "absurd_seriousness": (0.0, 1.0),
    "text_chaos": (0.0, 1.5),
    "rupture_prob": (0.0, 1.0),
    "stutter_prob": (0.0, 1.0),
    "recurrence_prob": (0.0, 0.95),
    "ghost_prob": (0.0, 0.95),
    "silence_prob": (0.0, 0.95),
}

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


@dataclass
class Event:
    layer: str
    section: str
    source: str
    source_basename: str
    source_duration_ms: int
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
        row.update(fields)
        try:
            with self.telemetry_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
        except OSError:
            pass


# -------------------------------------------------------------------
# CLI
# -------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Political cut-up + speech concrete engine.")
    p.add_argument("--mode", choices=["audio", "agitprop", "cuttargets", "both", "all"], default="audio")
    p.add_argument("--output", default="transmissions_cutups", help="Output root folder.")
    p.add_argument("--seed", type=int, default=7, help="Deterministic random seed.")

    p.add_argument("--input", help="Root folder containing audio samples (required for audio/both/all).")
    p.add_argument("--duration", type=float, default=90.0, help="Composition duration in seconds.")
    p.add_argument("--variants", type=int, default=1, help="Number of rendered variants.")
    p.add_argument("--sample-rate", type=int, default=44100, help="Export sample rate.")
    p.add_argument("--master-gain", type=float, default=-3.0, help="Master gain in dB.")
    p.add_argument("--bed-noise", action="store_true", help="Add synthetic hiss bed.")
    p.add_argument("--min-frag", type=float, default=0.05, help="Minimum fragment size in seconds.")
    p.add_argument("--max-frag", type=float, default=4.2, help="Maximum fragment size in seconds.")
    p.add_argument("--density", choices=["sparse", "medium", "dense"], default="medium")
    p.add_argument("--concrete", action="store_true", help="Bias toward harsher concrete transformations.")
    p.add_argument("--sectional", action="store_true", help="Enable section-aware timeline behavior.")
    p.add_argument("--arrangement-style", choices=["sequential", "swarm", "collapse"], default="swarm")
    p.add_argument("--memory-depth", type=int, default=10, help="Rolling memory depth for ghost recurrence.")
    p.add_argument("--silence-prob", type=float, default=0.15, help="Probability of dead-air insertion.")
    p.add_argument("--recurrence-prob", type=float, default=0.28, help="Probability to reuse previous source memory.")
    p.add_argument("--ghost-prob", type=float, default=0.22, help="Probability to force ghost-layer behavior.")

    p.add_argument("--top300-csv", default="transmissions_top300_sample_candidates.csv")
    p.add_argument("--full-csv", default="transmissions_full_subtitles.csv")
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

    return p.parse_args()


def validate_args(args: argparse.Namespace) -> argparse.Namespace:
    """Validate and normalize CLI arguments with clear failure reasons."""
    if args.variants < 1:
        raise SystemExit("--variants must be >= 1")
    if args.sample_rate < 8000:
        raise SystemExit("--sample-rate must be >= 8000")
    if args.duration <= 0:
        raise SystemExit("--duration must be > 0")
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

    args.min_frag = max(0.01, args.min_frag)
    args.max_frag = max(args.min_frag, args.max_frag)
    args.silence_prob = clamp(args.silence_prob, 0.0, 0.95)
    args.recurrence_prob = clamp(args.recurrence_prob, 0.0, 0.95)
    args.rupture_prob = clamp(args.rupture_prob, 0.0, 1.0)
    args.stutter_prob = clamp(args.stutter_prob, 0.0, 1.0)
    args.ghost_prob = clamp(args.ghost_prob, 0.0, 0.95)
    args.text_chaos = clamp(args.text_chaos, 0.0, 1.5)
    args.absurd_seriousness = clamp(args.absurd_seriousness, 0.0, 1.0)
    return args


# -------------------------------------------------------------------
# SHARED TEXT UTILITIES
# -------------------------------------------------------------------


def clamp(v: float, low: float, high: float) -> float:
    return max(low, min(high, v))


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


def cut_words(text: str) -> List[str]:
    return TOKEN_RE.findall(text)


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


def build_live_control(args: argparse.Namespace) -> LiveControlState:
    control_file = Path(args.live_control_file).expanduser().resolve() if args.live_control_file else None
    telemetry_path = Path(args.live_telemetry_jsonl).expanduser().resolve() if args.live_telemetry_jsonl else None
    return LiveControlState(
        enabled=bool(control_file),
        control_file=control_file,
        poll_ms=max(30, int(args.live_control_poll_ms)),
        telemetry_path=telemetry_path,
    )


def runtime_snapshot(args: argparse.Namespace, live: Optional[LiveControlState]) -> RuntimeParams:
    if not live or not live.enabled:
        return RuntimeParams(
            absurd_seriousness=float(args.absurd_seriousness),
            text_chaos=float(args.text_chaos),
            rupture_prob=float(args.rupture_prob),
            stutter_prob=float(args.stutter_prob),
            recurrence_prob=float(args.recurrence_prob),
            ghost_prob=float(args.ghost_prob),
            silence_prob=float(args.silence_prob),
            force_section="",
            hold_section=False,
            burst_now=False,
            panic_silence=False,
        )
    return RuntimeParams(
        absurd_seriousness=live.value(args, "absurd_seriousness"),
        text_chaos=live.value(args, "text_chaos"),
        rupture_prob=live.value(args, "rupture_prob"),
        stutter_prob=live.value(args, "stutter_prob"),
        recurrence_prob=live.value(args, "recurrence_prob"),
        ghost_prob=live.value(args, "ghost_prob"),
        silence_prob=live.value(args, "silence_prob"),
        force_section=live.section_override,
        hold_section=live.hold_section,
        burst_now=live.burst_now,
        panic_silence=live.panic_silence,
    )


def apply_runtime_params(args: argparse.Namespace, runtime: RuntimeParams) -> argparse.Namespace:
    local = argparse.Namespace(**vars(args))
    local.absurd_seriousness = runtime.absurd_seriousness
    local.text_chaos = runtime.text_chaos
    local.rupture_prob = runtime.rupture_prob
    local.stutter_prob = runtime.stutter_prob
    local.recurrence_prob = runtime.recurrence_prob
    local.ghost_prob = runtime.ghost_prob
    local.silence_prob = runtime.silence_prob
    return local


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


def run_agitprop_mode(args: argparse.Namespace, output_root: Path, summary: RunSummary, live: Optional[LiveControlState] = None) -> Path:
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

    agit_out = output_root / "agitprop"
    agit_out.mkdir(parents=True, exist_ok=True)

    slogans: List[str] = []
    for _ in range(max(1, args.agitprop_count)):
        runtime = runtime_snapshot(args, live)
        local_args = apply_runtime_params(args, runtime)
        slogans.append(build_slogan(top300, full, local_args, resolve_personality(local_args)))

    broadcasts: List[str] = []
    for _ in range(max(1, args.broadcast_count)):
        runtime = runtime_snapshot(args, live)
        local_args = apply_runtime_params(args, runtime)
        broadcasts.append(build_broadcast(top300, full, local_args, resolve_personality(local_args)))

    chant_cells: List[Dict[str, str]] = []
    for _ in range(max(1, args.chant_count)):
        runtime = runtime_snapshot(args, live)
        local_args = apply_runtime_params(args, runtime)
        chant_cells.append(build_chant_cell(top300, full, local_args, resolve_personality(local_args)))

    if live and live.enabled:
        live.telemetry(
            "agitprop_mode",
            slogans=len(slogans),
            broadcasts=len(broadcasts),
            chants=len(chant_cells),
            absurd_seriousness=runtime_snapshot(args, live).absurd_seriousness,
            text_chaos=runtime_snapshot(args, live).text_chaos,
        )

    (agit_out / "slogans.txt").write_text("\n\n".join(s.strip() for s in slogans) + "\n", encoding="utf-8")
    (agit_out / "broadcasts.txt").write_text("\n\n".join(s.strip() for s in broadcasts) + "\n", encoding="utf-8")

    chant_path = agit_out / "chant_cells.csv"
    with chant_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["mode", "text", "delivery", "source_bank", "file", "clip_id", "cue_index", "start_tc", "end_tc", "personality"])
        writer.writeheader()
        writer.writerows(chant_cells)

    summary.slogans, summary.broadcasts, summary.chants = len(slogans), len(broadcasts), len(chant_cells)
    summary.output_paths.extend([str(agit_out / "slogans.txt"), str(agit_out / "broadcasts.txt"), str(chant_path)])
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


def run_cuttargets_mode(args: argparse.Namespace, output_root: Path, summary: RunSummary, chant_cells_path: Optional[Path] = None) -> Path:
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

    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["cell_index", "mode", "delivery", "generated_text", "normalized_query", "match_rank", "match_score", "recommended", "match_method", "source_bank", "file", "clip_id", "cue_index", "start_tc", "end_tc", "duration_sec", "source_text"])
        writer.writeheader()
        writer.writerows(out_rows)

    summary.cut_matches = len([r for r in out_rows if r["match_rank"]])
    summary.output_paths.append(str(out_path))
    return out_path


# -------------------------------------------------------------------
# AUDIO DISCOVERY / SELECTION + TRANSFORM + ARRANGEMENT
# -------------------------------------------------------------------


def discover_samples(root: Path) -> Tuple[List[SampleFile], int]:
    ensure_audio_backend()
    samples: List[SampleFile] = []
    unreadable = 0
    for path in root.rglob("*"):
        if path.is_file() and path.suffix.lower() in AUDIO_EXTS:
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


def choose_event_count(duration_s: float, density: str, sectional: bool) -> int:
    base = {"sparse": 24, "medium": 44, "dense": 74}[density]
    if sectional:
        base = int(base * 1.25)
    return max(8, int(base * (duration_s / 90.0)))


def weighted_choice(samples: List[SampleFile], concrete: bool) -> SampleFile:
    weights = []
    for s in samples:
        dur_s = s.duration_ms / 1000.0
        dur_bonus = 2.1 if 0.05 <= dur_s <= 1.7 else 1.2 if dur_s <= 3.5 else 0.45
        if not concrete and dur_s > 3.2:
            dur_bonus += 0.5
        word_bonus = 1.3 if 2 <= s.words <= 8 else 0.7
        weights.append(max(0.1, dur_bonus + word_bonus + s.intensity_hint + s.loop_hint))
    return random.choices(samples, weights=weights, k=1)[0]


def section_profile(progress: float, args: argparse.Namespace) -> Dict[str, float]:
    if progress < 0.2:
        return {"name": "ENTRY", "dens": 0.44, "frag_mul": 1.28, "repeat": 0.2, "reverse": 0.14, "filt": 0.52, "silence": args.silence_prob + 0.16, "ghost": args.ghost_prob * 0.72}
    if progress < 0.45:
        return {"name": "BUILD", "dens": 1.18, "frag_mul": 0.82, "repeat": 0.42, "reverse": 0.22, "filt": 0.72, "silence": args.silence_prob * 0.88, "ghost": args.ghost_prob + 0.08}
    if progress < 0.68:
        return {"name": "PRESSURE", "dens": 1.72, "frag_mul": 0.42, "repeat": 0.64, "reverse": 0.36, "filt": 0.92, "silence": args.silence_prob * 0.5, "ghost": args.ghost_prob + 0.19}
    if progress < 0.86:
        return {"name": "COLLAPSE", "dens": 0.66, "frag_mul": 0.3, "repeat": 0.72, "reverse": 0.58, "filt": 0.98, "silence": args.silence_prob + 0.28, "ghost": args.ghost_prob + 0.3}
    return {"name": "AFTERIMAGE", "dens": 0.3, "frag_mul": 0.24, "repeat": 0.78, "reverse": 0.68, "filt": 0.99, "silence": args.silence_prob + 0.33, "ghost": args.ghost_prob + 0.42}


def section_profile_from_name(name: str, args: argparse.Namespace) -> Dict[str, float]:
    probes = {
        "ENTRY": 0.1,
        "BUILD": 0.3,
        "PRESSURE": 0.56,
        "COLLAPSE": 0.78,
        "AFTERIMAGE": 0.93,
    }
    return section_profile(probes.get(name, 0.3), args)


def section_plan(total_ms: int) -> Dict[str, Tuple[int, int]]:
    marks = [0, int(total_ms * 0.2), int(total_ms * 0.45), int(total_ms * 0.68), int(total_ms * 0.86), total_ms]
    names = list(SECTION_NAMES)
    return {name: (marks[i], marks[i + 1]) for i, name in enumerate(names)}


def clamp_to_section(position_ms: int, span: Tuple[int, int], frag_len: int) -> int:
    start, end = span
    room_end = max(start, end - max(10, frag_len + 4))
    return int(clamp(position_ms, start, room_end))


def command_cell_swarm(audio: AudioSegment, profile: Dict[str, float]) -> Tuple[AudioSegment, bool]:
    if len(audio) < 45 or random.random() > (0.18 + profile["repeat"] * 0.42):
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


def safe_slice_fragment(audio: AudioSegment, min_ms: int, max_ms: int, frag_mul: float) -> AudioSegment:
    audio_len = len(audio)
    if audio_len <= 1:
        return AudioSegment.silent(duration=30, frame_rate=audio.frame_rate)
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


def shape_fragment(audio: AudioSegment, profile: Dict[str, float], concrete: bool) -> Tuple[AudioSegment, Dict[str, object]]:
    reversed_flag = random.random() < profile["reverse"]
    if reversed_flag:
        audio = audio.reverse()

    speed = random.choice([0.58, 0.72, 0.85, 0.94, 1.0, 1.12, 1.28, 1.45]) if concrete else random.choice([0.76, 0.86, 0.94, 1.0, 1.1, 1.22])
    audio = change_speed(audio, speed)

    grain_mode = random.random() < (0.42 if concrete else 0.22)
    if grain_mode:
        audio = grainify(audio)

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

    if random.random() < (0.22 if concrete else 0.14):
        # hard interruption: chop center out to create phrase discontinuity.
        mid = len(audio) // 2
        cut = random.randint(8, min(120, max(8, len(audio) // 2)))
        audio = audio[: max(0, mid - cut)] + AudioSegment.silent(duration=random.randint(12, 80), frame_rate=audio.frame_rate) + audio[min(len(audio), mid + cut) :]

    hp = random.choice([100, 180, 260, 420, 700, 1200, 1700])
    lp = random.choice([1200, 2100, 3200, 4400, 6200, 9000])
    if random.random() < profile["filt"]:
        audio = high_pass_filter(audio, hp)
    if random.random() < profile["filt"]:
        audio = low_pass_filter(audio, lp)

    if random.random() < profile["silence"]:
        pad = AudioSegment.silent(duration=random.randint(18, 160), frame_rate=audio.frame_rate)
        audio = (pad + audio) if random.random() < 0.5 else (audio + pad)

    if random.random() < profile["silence"] * 0.45 and len(audio) > 70:
        hole_start = random.randint(0, max(0, len(audio) - 40))
        hole = AudioSegment.silent(duration=random.randint(10, 70), frame_rate=audio.frame_rate)
        audio = audio[:hole_start] + hole + audio[hole_start:]

    audio = audio.fade_in(min(20, max(2, len(audio) // 15))).fade_out(min(46, max(5, len(audio) // 9)))
    transform = "grain" if grain_mode else "slice"
    if reversed_flag:
        transform += "+rev"
    if swarm_mode:
        transform += "+swarm"
    return audio, {"reversed": reversed_flag, "speed": speed, "repeated": repeated, "hp_hz": hp, "lp_hz": lp, "grain_mode": grain_mode, "transformation": transform}


def export_manifest(path: Path, events: List[Event]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["layer", "section", "source", "source_basename", "source_duration_ms", "start_ms", "end_ms", "fragment_duration_ms", "gain_db", "reversed", "speed", "repeated", "hp_hz", "lp_hz", "grain_mode", "from_memory", "transformation", "layer_role", "recurrence_index"])
        writer.writeheader()
        for e in events:
            writer.writerow(e.__dict__)


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


def place_events(samples: List[SampleFile], total_ms: int, args: argparse.Namespace, min_frag_ms: int, max_frag_ms: int, live: Optional[LiveControlState] = None) -> Tuple[AudioSegment, AudioSegment, AudioSegment, List[Event]]:
    voice_main = AudioSegment.silent(duration=total_ms, frame_rate=args.sample_rate).set_channels(2)
    voice_cuts = AudioSegment.silent(duration=total_ms, frame_rate=args.sample_rate).set_channels(2)
    ghosts = AudioSegment.silent(duration=total_ms, frame_rate=args.sample_rate).set_channels(2)
    events: List[Event] = []
    memory: Deque[SampleFile] = deque(maxlen=max(1, args.memory_depth))
    recurrence_memory: Deque[Tuple[SampleFile, AudioSegment, Dict[str, object], str]] = deque(maxlen=max(3, args.memory_depth * 2))
    recurrence_count: Dict[str, int] = {}
    plan = section_plan(total_ms)
    held_section = ""

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
        runtime = runtime_snapshot(args, live)
        local_args = apply_runtime_params(args, runtime)
        progress = i / max(1, n_events - 1)
        if runtime.hold_section and runtime.force_section:
            held_section = runtime.force_section
        elif not runtime.hold_section:
            held_section = ""
        forced_section = runtime.force_section or held_section

        if local_args.sectional:
            profile = section_profile_from_name(forced_section, local_args) if forced_section else section_profile(progress, local_args)
        else:
            profile = {"name": "BUILD", "dens": 1.0, "frag_mul": 1.0, "repeat": 0.2, "reverse": 0.18, "filt": 0.6, "silence": local_args.silence_prob, "ghost": local_args.ghost_prob}

        sec_name = str(profile["name"])
        sec_span = plan.get(sec_name, (0, total_ms))
        memory_bias = local_args.recurrence_prob + (0.14 if sec_name in {"COLLAPSE", "AFTERIMAGE"} else (0.08 if sec_name == "PRESSURE" else 0.0))
        use_recurrence_fragment = bool(recurrence_memory and random.random() < clamp(memory_bias * (1.4 if sec_name in {"PRESSURE", "COLLAPSE"} else 1.0), 0.0, 0.97))
        from_memory = False
        if use_recurrence_fragment:
            sample, shaped, meta, _ = random.choice(list(recurrence_memory))
            from_memory = True
            if random.random() < 0.34:
                shaped = change_speed(shaped, random.choice([0.84, 0.92, 1.05, 1.16]))
            if random.random() < 0.38:
                shaped = low_pass_filter(shaped, random.choice([1500, 2100, 2800]))
            if random.random() < 0.28:
                shaped = shaped.reverse()
            meta = dict(meta)
            meta["transformation"] = f"{meta.get('transformation', 'slice')}+memory"
        else:
            sample = random.choice(list(memory)) if (memory and random.random() < clamp(memory_bias, 0.0, 0.95)) else weighted_choice(samples, local_args.concrete)
            src = AudioSegment.from_file(sample.path).set_frame_rate(local_args.sample_rate).set_channels(2)
            frag = safe_slice_fragment(src, min_frag_ms, max_frag_ms, float(profile["frag_mul"]))
            shaped, meta = shape_fragment(frag, profile, local_args.concrete)
        recurrence_count[str(sample.path)] = recurrence_count.get(str(sample.path), 0) + 1

        layer = random.choices(
            ["voice_main", "voice_cuts", "ghosts"],
            weights=[3, 4, 4] if profile["name"] in {"COLLAPSE", "AFTERIMAGE"} else ([4, 3, 3] if args.arrangement_style == "swarm" else [5, 2, 2]),
            k=1,
        )[0]
        if random.random() < profile["ghost"]:
            layer = "ghosts"
        if sec_name == "AFTERIMAGE" and random.random() < 0.58:
            layer = "ghosts"

        if local_args.arrangement_style == "collapse":
            jitter = random.randint(-120, 520)
            step = max(40, int((900 if profile["name"] == "PRESSURE" else 1300) * float(profile["dens"])))
        else:
            jitter = random.randint(-300, 820)
            step = max(70, int(random.randint(350, 2200) / max(0.4, float(profile["dens"]))))

        pos = random.randint(0, 500) if i == 0 else max(0, current_anchor + jitter)
        pos = clamp_to_section(pos, sec_span, len(shaped))
        if in_dead_air(pos):
            # honor silence windows as structural punctuation.
            move = random.randint(220, 1400)
            if sec_name in {"COLLAPSE", "AFTERIMAGE"} and random.random() < 0.6:
                pos = clamp_to_section(max(0, pos - move // 2), sec_span, len(shaped))
            else:
                pos = clamp_to_section(min(total_ms - 1, pos + move), sec_span, len(shaped))

        if pos + len(shaped) >= total_ms:
            pos = max(0, total_ms - len(shaped) - random.randint(10, 220))
        pos = clamp_to_section(pos, sec_span, len(shaped))
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

        burst_gate = 1.0 if runtime.burst_now else 0.2
        if sec_name in {"PRESSURE", "COLLAPSE"} and random.random() < burst_gate:
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
        rec_idx = recurrence_count[str(sample.path)]
        events.append(
            Event(
                layer=layer,
                section=str(profile["name"]),
                source=str(sample.path),
                source_basename=sample.path.name,
                source_duration_ms=sample.duration_ms,
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
            )

    return voice_main, voice_cuts, ghosts, events


def normalize_master(audio: AudioSegment, master_gain: float) -> AudioSegment:
    return compress_dynamic_range(audio, threshold=-22.0, ratio=2.4, attack=8, release=140).apply_gain(master_gain)


def build_variant(samples: List[SampleFile], output_root: Path, variant_idx: int, args: argparse.Namespace, summary: RunSummary, live: Optional[LiveControlState] = None) -> None:
    total_ms = max(2000, int(max(1.0, args.duration) * 1000))
    min_frag_ms = max(10, int(max(0.01, args.min_frag) * 1000))
    max_frag_ms = max(min_frag_ms, int(max(args.min_frag, args.max_frag) * 1000))

    variant_name = f"cutup_{variant_idx:02d}"
    variant_dir = output_root / variant_name
    stems_dir = variant_dir / "stems"
    stems_dir.mkdir(parents=True, exist_ok=True)

    main, cuts, ghosts, events = place_events(samples, total_ms, args, min_frag_ms, max_frag_ms, live=live)
    hiss = make_hiss(total_ms, args.sample_rate) if args.bed_noise else AudioSegment.silent(duration=total_ms, frame_rate=args.sample_rate)

    master = AudioSegment.silent(duration=total_ms, frame_rate=args.sample_rate).set_channels(2)
    master = master.overlay(hiss, position=0).overlay(ghosts - 2, position=0).overlay(cuts + 1, position=0).overlay(main + 2, position=0)
    master = normalize_master(master, args.master_gain)

    main.export(stems_dir / "voice_main.wav", format="wav")
    cuts.export(stems_dir / "voice_cuts.wav", format="wav")
    ghosts.export(stems_dir / "ghosts.wav", format="wav")
    hiss.export(stems_dir / "hiss_bed.wav", format="wav")
    master_path = variant_dir / f"{variant_name}_master.wav"
    event_path = variant_dir / f"{variant_name}_events.csv"
    score_path = variant_dir / f"{variant_name}_score.txt"
    master.export(master_path, format="wav")
    export_manifest(event_path, events)
    score_path.write_text(build_section_score(events), encoding="utf-8")

    summary.audio_events += len(events)
    summary.section_distribution.update([e.section for e in events])
    summary.recurring_sources.update([e.source_basename for e in events if e.recurrence_index > 1])
    summary.output_paths.extend([str(master_path), str(event_path), str(score_path)])


def run_audio_mode(args: argparse.Namespace, output_root: Path, summary: RunSummary, live: Optional[LiveControlState] = None) -> None:
    ensure_audio_backend()
    if not args.input:
        raise SystemExit("--input is required for --mode audio, --mode both, and --mode all")
    input_root = Path(args.input).expanduser().resolve()
    if not input_root.exists():
        raise SystemExit(f"Input folder not found: {input_root}")
    if not input_root.is_dir():
        raise SystemExit(f"--input must be a directory: {input_root}")

    samples, unreadable = discover_samples(input_root)
    if not samples:
        hint = f" ({unreadable} files could not be decoded)" if unreadable else ""
        raise SystemExit(f"No usable audio samples found in {input_root}{hint}")
    if unreadable:
        print(f"Warning: skipped {unreadable} unreadable audio file(s) while scanning {input_root}")

    audio_out = output_root / "audio_cutups"
    audio_out.mkdir(parents=True, exist_ok=True)
    for i in range(1, max(1, args.variants) + 1):
        runtime = runtime_snapshot(args, live)
        local_args = apply_runtime_params(args, runtime)
        build_variant(samples, audio_out, i, local_args, summary, live=live)


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

    output_root = Path(args.output).expanduser().resolve()
    if output_root.exists() and not output_root.is_dir():
        raise SystemExit(f"--output path exists and is not a directory: {output_root}")
    try:
        output_root.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise SystemExit(f"Failed to create output directory '{output_root}': {exc}") from exc
    summary = RunSummary()

    if args.mode == "audio":
        run_audio_mode(args, output_root, summary, live=live)
    elif args.mode == "agitprop":
        run_agitprop_mode(args, output_root, summary, live=live)
    elif args.mode == "cuttargets":
        run_cuttargets_mode(args, output_root, summary)
    elif args.mode == "both":
        run_agitprop_mode(args, output_root, summary, live=live)
        run_audio_mode(args, output_root, summary, live=live)
    elif args.mode == "all":
        chant_path = run_agitprop_mode(args, output_root, summary, live=live)
        run_cuttargets_mode(args, output_root, summary, chant_cells_path=chant_path)
        run_audio_mode(args, output_root, summary, live=live)

    print_summary(summary)
    if args.export_debug_summary:
        maybe_export_debug_summary(summary, output_root)


if __name__ == "__main__":
    main()
