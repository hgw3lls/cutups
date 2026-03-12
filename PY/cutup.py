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
import random
import re
from collections import Counter, deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Deque, Dict, Iterable, List, Optional, Sequence, Tuple

from pydub import AudioSegment
from pydub.effects import compress_dynamic_range, high_pass_filter, low_pass_filter

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
    p.add_argument("--max-words-slogan", type=int, default=11)
    p.add_argument("--export-debug-summary", action="store_true", help="Write run_summary.txt.")

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

    args.min_frag = max(0.01, args.min_frag)
    args.max_frag = max(args.min_frag, args.max_frag)
    args.silence_prob = clamp(args.silence_prob, 0.0, 0.95)
    args.recurrence_prob = clamp(args.recurrence_prob, 0.0, 0.95)
    args.rupture_prob = clamp(args.rupture_prob, 0.0, 1.0)
    args.stutter_prob = clamp(args.stutter_prob, 0.0, 1.0)
    args.ghost_prob = clamp(args.ghost_prob, 0.0, 0.95)
    args.text_chaos = clamp(args.text_chaos, 0.0, 1.5)
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
        "public interest": "managed interest",
        "accountable": "countable",
        "free speech": "metered speech",
        "first amendment": "first adjustment",
        "license": "permission",
        "authority": "authorized fear",
        "policy": "signal policy",
    }
    out = text
    for src, dst in swaps.items():
        out = re.sub(src, dst, out, flags=re.I)
    return out


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


def transmission_break(a: str, b: str, c: str) -> str:
    return (
        f"{interrupt_with(a, b)}\n"
        f"[carrier drop]\n"
        f"{glitch_gap(c)}\n"
        f"{phrase_decay(b)}"
    )


def rhetorical_pattern(official: str, threat: str, freedom: str, command: str, bridge: str) -> str:
    patterns = [
        lambda: f"{compress_phrase(official)}\n{interrupt_with(bridge, threat)}\n{collapse_to_term(threat)}\n{phrase_decay(command)}",
        lambda: f"{compress_phrase(freedom)}\n{mirrored_contradiction(freedom, command)}\n{false_restart(command)}",
        lambda: f"{collide_registers(official, bridge)}\n{glitch_gap(bridge)}\n{recursive_burst(threat)}\n[open channel]",
        lambda: f"{fragment(bridge,2,5)}?\nREFUSAL\n{keyword_pressure(official)}\n{collapse_to_term(threat)}",
        lambda: f"{fragment(official,2,6).upper()}\n{bureaucratic_melt(splice_halves(official, freedom)).lower()}\n{collapse_to_term(command)}",
        lambda: transmission_break(official, threat, bridge),
    ]
    return random.choice(patterns)()


def build_slogan(top300: List[Line], full: List[Line], args: argparse.Namespace) -> str:
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
    ]
    if random.random() < args.stutter_prob:
        ops.append(lambda: f"{stutter_phrase(a.text)}\n{recursive_burst(c.text)}\n{glitch_gap(d.text)}")
    if random.random() < args.rupture_prob:
        ops.append(lambda: f"{splice_halves(a.text, c.text).upper()}\n/// SIGNAL CUT ///\n{collapse_to_term(b.text)}")

    out = random.choice(ops)()
    words = cut_words(out)
    if len(words) > args.max_words_slogan * 2:
        trimmed = words[: args.max_words_slogan * 2]
        pivot = random.randint(max(1, len(trimmed) // 3), len(trimmed))
        out = " ".join(trimmed[:pivot])
    return out.strip()


def build_broadcast(top300: List[Line], full: List[Line], args: argparse.Namespace) -> str:
    used: set[str] = set()
    official = choose_line(top300, args.text_chaos, ["official"], used, True)
    used.add(official.text)
    threat = choose_line(top300, args.text_chaos, ["threat"], used, True)
    used.add(threat.text)
    freedom = choose_line(top300, args.text_chaos, ["freedom"], used, True)
    used.add(freedom.text)
    command = choose_line(top300, args.text_chaos, ["command"], used, True)
    bridge = choose_line(full, args.text_chaos, excluded_texts=used)
    return rhetorical_pattern(official.text, threat.text, freedom.text, command.text, bridge.text)


def build_chant_cell(top300: List[Line], full: List[Line], args: argparse.Namespace) -> Dict[str, str]:
    use_full = random.random() < 0.3
    line = choose_line(full if use_full else top300, args.text_chaos)
    partner = choose_line(top300 if use_full else full, args.text_chaos)
    mode = random.choice(["chant", "loop", "burst", "call", "splice", "stutter", "echo_decay", "ladder", "triplet", "pulse_break", "collapse"])

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
    else:
        text, delivery = stutter_phrase(line.text), "stutter chant"

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
    }


def run_agitprop_mode(args: argparse.Namespace, output_root: Path, summary: RunSummary) -> Path:
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

    slogans = [build_slogan(top300, full, args) for _ in range(max(1, args.agitprop_count))]
    broadcasts = [build_broadcast(top300, full, args) for _ in range(max(1, args.broadcast_count))]
    chant_cells = [build_chant_cell(top300, full, args) for _ in range(max(1, args.chant_count))]

    (agit_out / "slogans.txt").write_text("\n\n".join(s.strip() for s in slogans) + "\n", encoding="utf-8")
    (agit_out / "broadcasts.txt").write_text("\n\n".join(s.strip() for s in broadcasts) + "\n", encoding="utf-8")

    chant_path = agit_out / "chant_cells.csv"
    with chant_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["mode", "text", "delivery", "source_bank", "file", "clip_id", "cue_index", "start_tc", "end_tc"])
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
        return {"name": "ENTRY", "dens": 0.48, "frag_mul": 1.35, "repeat": 0.18, "reverse": 0.13, "filt": 0.48, "silence": args.silence_prob + 0.12, "ghost": args.ghost_prob * 0.6}
    if progress < 0.45:
        return {"name": "BUILD", "dens": 1.05, "frag_mul": 0.86, "repeat": 0.36, "reverse": 0.2, "filt": 0.68, "silence": args.silence_prob * 0.92, "ghost": args.ghost_prob + 0.05}
    if progress < 0.68:
        return {"name": "PRESSURE", "dens": 1.55, "frag_mul": 0.48, "repeat": 0.56, "reverse": 0.34, "filt": 0.9, "silence": args.silence_prob * 0.55, "ghost": args.ghost_prob + 0.16}
    if progress < 0.86:
        return {"name": "COLLAPSE", "dens": 0.72, "frag_mul": 0.35, "repeat": 0.66, "reverse": 0.52, "filt": 0.96, "silence": args.silence_prob + 0.2, "ghost": args.ghost_prob + 0.24}
    return {"name": "AFTERIMAGE", "dens": 0.32, "frag_mul": 0.28, "repeat": 0.74, "reverse": 0.62, "filt": 0.98, "silence": args.silence_prob + 0.28, "ghost": args.ghost_prob + 0.35}


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
    return audio, {"reversed": reversed_flag, "speed": speed, "repeated": repeated, "hp_hz": hp, "lp_hz": lp, "grain_mode": grain_mode, "transformation": transform}


def export_manifest(path: Path, events: List[Event]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["layer", "section", "source", "source_basename", "source_duration_ms", "start_ms", "end_ms", "fragment_duration_ms", "gain_db", "reversed", "speed", "repeated", "hp_hz", "lp_hz", "grain_mode", "from_memory", "transformation", "layer_role", "recurrence_index"])
        writer.writeheader()
        for e in events:
            writer.writerow(e.__dict__)


def place_events(samples: List[SampleFile], total_ms: int, args: argparse.Namespace, min_frag_ms: int, max_frag_ms: int) -> Tuple[AudioSegment, AudioSegment, AudioSegment, List[Event]]:
    voice_main = AudioSegment.silent(duration=total_ms, frame_rate=args.sample_rate).set_channels(2)
    voice_cuts = AudioSegment.silent(duration=total_ms, frame_rate=args.sample_rate).set_channels(2)
    ghosts = AudioSegment.silent(duration=total_ms, frame_rate=args.sample_rate).set_channels(2)
    events: List[Event] = []
    memory: Deque[SampleFile] = deque(maxlen=max(1, args.memory_depth))
    recurrence_count: Dict[str, int] = {}

    n_events = choose_event_count(total_ms / 1000.0, args.density, args.sectional)
    current_anchor = 0
    dead_air_windows: List[Tuple[int, int]] = []
    if args.sectional:
        n_windows = random.randint(2, 5)
        for _ in range(n_windows):
            start = random.randint(0, max(0, total_ms - 1200))
            dur = random.randint(140, 1800)
            dead_air_windows.append((start, min(total_ms, start + dur)))

    def in_dead_air(position_ms: int) -> bool:
        return any(a <= position_ms <= b for a, b in dead_air_windows)

    for i in range(n_events):
        progress = i / max(1, n_events - 1)
        profile = section_profile(progress, args) if args.sectional else {"name": "BUILD", "dens": 1.0, "frag_mul": 1.0, "repeat": 0.2, "reverse": 0.18, "filt": 0.6, "silence": args.silence_prob, "ghost": args.ghost_prob}

        memory_bias = args.recurrence_prob + (0.1 if profile["name"] in {"COLLAPSE", "AFTERIMAGE"} else 0.0)
        from_memory = bool(memory and random.random() < clamp(memory_bias, 0.0, 0.97))
        sample = random.choice(list(memory)) if from_memory else weighted_choice(samples, args.concrete)
        recurrence_count[str(sample.path)] = recurrence_count.get(str(sample.path), 0) + 1

        src = AudioSegment.from_file(sample.path).set_frame_rate(args.sample_rate).set_channels(2)
        frag = safe_slice_fragment(src, min_frag_ms, max_frag_ms, float(profile["frag_mul"]))
        shaped, meta = shape_fragment(frag, profile, args.concrete)

        layer = random.choices(
            ["voice_main", "voice_cuts", "ghosts"],
            weights=[3, 4, 4] if profile["name"] in {"COLLAPSE", "AFTERIMAGE"} else ([4, 3, 3] if args.arrangement_style == "swarm" else [5, 2, 2]),
            k=1,
        )[0]
        if random.random() < profile["ghost"]:
            layer = "ghosts"

        if args.arrangement_style == "collapse":
            jitter = random.randint(-120, 520)
            step = max(40, int((900 if profile["name"] == "PRESSURE" else 1300) * float(profile["dens"])))
        else:
            jitter = random.randint(-300, 820)
            step = max(70, int(random.randint(350, 2200) / max(0.4, float(profile["dens"]))))

        pos = random.randint(0, 500) if i == 0 else max(0, current_anchor + jitter)
        if in_dead_air(pos):
            # honor silence windows as structural punctuation.
            pos = min(total_ms - 1, pos + random.randint(220, 1400))

        if pos + len(shaped) >= total_ms:
            pos = max(0, total_ms - len(shaped) - random.randint(10, 220))
        current_anchor = min(total_ms - 50, pos + step)

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

        if args.sectional and random.random() < 0.18 and recurrence_count[str(sample.path)] > 1:
            # ghost return: delayed, filtered recurrence of same material.
            back_pos = min(total_ms - 1, pos + random.randint(160, 2600))
            ghost_copy = low_pass_filter(shaped.reverse(), random.choice([1200, 1800, 2400])).apply_gain(random.uniform(-15, -9))
            ghosts = ghosts.overlay(ghost_copy, position=back_pos)

        memory.append(sample)
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

    return voice_main, voice_cuts, ghosts, events


def normalize_master(audio: AudioSegment, master_gain: float) -> AudioSegment:
    return compress_dynamic_range(audio, threshold=-22.0, ratio=2.4, attack=8, release=140).apply_gain(master_gain)


def build_variant(samples: List[SampleFile], output_root: Path, variant_idx: int, args: argparse.Namespace, summary: RunSummary) -> None:
    total_ms = max(2000, int(max(1.0, args.duration) * 1000))
    min_frag_ms = max(10, int(max(0.01, args.min_frag) * 1000))
    max_frag_ms = max(min_frag_ms, int(max(args.min_frag, args.max_frag) * 1000))

    variant_name = f"cutup_{variant_idx:02d}"
    variant_dir = output_root / variant_name
    stems_dir = variant_dir / "stems"
    stems_dir.mkdir(parents=True, exist_ok=True)

    main, cuts, ghosts, events = place_events(samples, total_ms, args, min_frag_ms, max_frag_ms)
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
    master.export(master_path, format="wav")
    export_manifest(event_path, events)

    summary.audio_events += len(events)
    summary.section_distribution.update([e.section for e in events])
    summary.recurring_sources.update([e.source_basename for e in events if e.recurrence_index > 1])
    summary.output_paths.extend([str(master_path), str(event_path)])


def run_audio_mode(args: argparse.Namespace, output_root: Path, summary: RunSummary) -> None:
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
        build_variant(samples, audio_out, i, args, summary)


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
    random.seed(args.seed)

    output_root = Path(args.output).expanduser().resolve()
    if output_root.exists() and not output_root.is_dir():
        raise SystemExit(f"--output path exists and is not a directory: {output_root}")
    try:
        output_root.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise SystemExit(f"Failed to create output directory '{output_root}': {exc}") from exc
    summary = RunSummary()

    if args.mode == "audio":
        run_audio_mode(args, output_root, summary)
    elif args.mode == "agitprop":
        run_agitprop_mode(args, output_root, summary)
    elif args.mode == "cuttargets":
        run_cuttargets_mode(args, output_root, summary)
    elif args.mode == "both":
        run_agitprop_mode(args, output_root, summary)
        run_audio_mode(args, output_root, summary)
    elif args.mode == "all":
        chant_path = run_agitprop_mode(args, output_root, summary)
        run_cuttargets_mode(args, output_root, summary, chant_cells_path=chant_path)
        run_audio_mode(args, output_root, summary)

    print_summary(summary)
    if args.export_debug_summary:
        maybe_export_debug_summary(summary, output_root)


if __name__ == "__main__":
    main()
