#!/usr/bin/env python3
"""
cutup.py

Integrated TRANSMISSIONS workflow:

Modes
-----
- audio
    Build cut-up audio from a folder of real audio clips.

- agitprop
    Generate slogans / broadcasts / chant cells from:
    - transmissions_top300_sample_candidates.csv
    - transmissions_full_subtitles.csv

- cuttargets
    Read chant_cells.csv and map generated text back to likely source rows
    in the two CSV banks, producing cut_targets.csv.

- both
    Run:
    1) agitprop
    2) audio

- all
    Run:
    1) agitprop
    2) cuttargets
    3) audio

This version pushes harder toward:
- Burroughs-like political cut-up
- recursive propaganda fragments
- bureaucratic language mutation
- musique concrete speech treatment
- harsher micro-loop / stutter / rupture behavior

Outputs
-------
Audio mode:
- audio_cutups/cutup_XX/cutup_XX_master.wav
- audio_cutups/cutup_XX/stems/*.wav
- audio_cutups/cutup_XX/cutup_XX_events.csv

Agitprop mode:
- agitprop/slogans.txt
- agitprop/broadcasts.txt
- agitprop/chant_cells.csv

Cuttargets mode:
- agitprop/cut_targets.csv

Dependencies
------------
    pip install pydub

Also requires ffmpeg installed and available on PATH.
"""

from __future__ import annotations

import argparse
import csv
import random
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from pydub import AudioSegment
from pydub.effects import compress_dynamic_range, high_pass_filter, low_pass_filter

# -------------------------------------------------------------------
# GLOBALS
# -------------------------------------------------------------------

AUDIO_EXTS = {".wav", ".mp3", ".flac", ".aiff", ".ogg", ".m4a"}
TOKEN_RE = re.compile(r"[A-Za-z']+")

# -------------------------------------------------------------------
# AUDIO DATA MODELS
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
    source: str
    start_ms: int
    end_ms: int
    gain_db: float
    reversed: bool
    speed: float
    repeated: int
    hp_hz: int
    lp_hz: int


# -------------------------------------------------------------------
# AGITPROP / SOURCE DATA MODELS
# -------------------------------------------------------------------


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


# -------------------------------------------------------------------
# CLI
# -------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Integrated cut-up audio + agitprop + cut-target workflow."
    )

    p.add_argument(
        "--mode",
        choices=["audio", "agitprop", "cuttargets", "both", "all"],
        default="audio",
        help="Which workflow stage(s) to run.",
    )

    p.add_argument(
        "--output",
        default="transmissions_cutups",
        help="Output root folder.",
    )
    p.add_argument(
        "--seed",
        type=int,
        default=7,
        help="Random seed.",
    )

    # audio options
    p.add_argument(
        "--input",
        help="Root folder containing extracted audio samples. Required for audio/both/all.",
    )
    p.add_argument(
        "--duration",
        type=float,
        default=90.0,
        help="Length of each audio composition in seconds.",
    )
    p.add_argument(
        "--variants",
        type=int,
        default=1,
        help="How many cut-up versions to render.",
    )
    p.add_argument(
        "--sample-rate",
        type=int,
        default=44100,
        help="Export sample rate.",
    )
    p.add_argument(
        "--master-gain",
        type=float,
        default=-3.0,
        help="Master gain in dB before export.",
    )
    p.add_argument(
        "--bed-noise",
        action="store_true",
        help="Add generated low-level hiss bed.",
    )
    p.add_argument(
        "--min-frag",
        type=float,
        default=0.35,
        help="Minimum fragment length in seconds.",
    )
    p.add_argument(
        "--max-frag",
        type=float,
        default=4.2,
        help="Maximum fragment length in seconds.",
    )
    p.add_argument(
        "--density",
        choices=["sparse", "medium", "dense"],
        default="medium",
        help="Event density.",
    )
    p.add_argument(
        "--concrete",
        action="store_true",
        help="Push audio rendering toward harsher musique concrete behavior.",
    )

    # CSV / agitprop options
    p.add_argument(
        "--top300-csv",
        default="transmissions_top300_sample_candidates.csv",
        help="CSV of top candidate phrases.",
    )
    p.add_argument(
        "--full-csv",
        default="transmissions_full_subtitles.csv",
        help="CSV of full subtitle/connective lines.",
    )
    p.add_argument(
        "--agitprop-count",
        type=int,
        default=40,
        help="How many slogans to generate.",
    )
    p.add_argument(
        "--broadcast-count",
        type=int,
        default=16,
        help="How many broadcast blocks to generate.",
    )
    p.add_argument(
        "--chant-count",
        type=int,
        default=120,
        help="How many chant cells to generate.",
    )
    p.add_argument(
        "--chant-cells-csv",
        default="",
        help="Optional explicit path to chant_cells.csv for cuttargets mode.",
    )
    p.add_argument(
        "--cut-match-count",
        type=int,
        default=3,
        help="How many candidate source matches to keep per chant cell.",
    )

    return p.parse_args()


# -------------------------------------------------------------------
# SHARED TEXT HELPERS
# -------------------------------------------------------------------


def clean_text(text: str) -> str:
    text = str(text).strip()
    text = re.sub(r">>+", " ", text)
    text = re.sub(r"\s+", " ", text)
    text = text.replace(" ,", ",").replace(" .", ".")
    text = text.strip(" -")
    return text.strip()


def is_usable_text(text: str) -> bool:
    if not text:
        return False
    if len(text) < 4:
        return False
    if re.fullmatch(r"[^\w]+", text):
        return False
    return True


def count_words(text: str) -> int:
    return len(TOKEN_RE.findall(text))


def normalize_text(text: str) -> str:
    text = str(text).strip().lower()
    text = re.sub(r"\s+", " ", text)
    return text


def token_list(text: str) -> List[str]:
    return [t.lower() for t in TOKEN_RE.findall(text)]


# -------------------------------------------------------------------
# AUDIO MODE
# -------------------------------------------------------------------


def discover_samples(root: Path) -> List[SampleFile]:
    samples: List[SampleFile] = []

    for path in root.rglob("*"):
        if path.is_file() and path.suffix.lower() in AUDIO_EXTS:
            try:
                audio = AudioSegment.from_file(path)
            except Exception:
                continue

            stem = path.stem.lower()
            words = len(TOKEN_RE.findall(stem.replace("_", " ")))

            intensity_hint = 0
            low_path = str(path).lower()

            if any(
                k in low_path
                for k in [
                    "max",
                    "high",
                    "threat",
                    "speech",
                    "fcc",
                    "license",
                    "censor",
                    "warning",
                    "command",
                ]
            ):
                intensity_hint += 2
            if any(k in low_path for k in ["med", "phrase", "official", "collapse"]):
                intensity_hint += 1

            loop_hint = 0
            if "micro" in low_path:
                loop_hint += 3
            elif "short" in low_path:
                loop_hint += 2
            elif "phrase" in low_path:
                loop_hint += 1

            samples.append(
                SampleFile(
                    path=path,
                    duration_ms=len(audio),
                    words=words,
                    intensity_hint=intensity_hint,
                    loop_hint=loop_hint,
                )
            )

    return samples


def choose_event_count(duration_s: float, density: str, concrete: bool = False) -> int:
    base = {"sparse": 22, "medium": 40, "dense": 68}[density]
    if concrete:
        base = int(base * 1.35)
    return max(8, int(base * (duration_s / 90.0)))


def weighted_choice(samples: List[SampleFile], concrete: bool = False) -> SampleFile:
    weights = []

    for s in samples:
        dur_s = s.duration_ms / 1000.0

        dur_bonus = 1.0
        if concrete:
            if 0.08 <= dur_s <= 1.8:
                dur_bonus += 2.2
            elif dur_s <= 3.2:
                dur_bonus += 1.2
            else:
                dur_bonus += 0.2
        else:
            if 0.5 <= dur_s <= 3.5:
                dur_bonus += 1.5
            elif dur_s <= 6:
                dur_bonus += 0.8
            else:
                dur_bonus += 0.2

        word_bonus = 1.0
        if 2 <= s.words <= 8:
            word_bonus += 1.2
        elif s.words <= 12:
            word_bonus += 0.5

        weights.append(
            max(0.1, dur_bonus + word_bonus + s.intensity_hint + s.loop_hint)
        )

    return random.choices(samples, weights=weights, k=1)[0]


def slice_fragment(
    audio: AudioSegment,
    min_ms: int,
    max_ms: int,
    concrete: bool = False,
) -> AudioSegment:
    audio_len = len(audio)

    if audio_len <= 0:
        return audio

    upper = min(max_ms, audio_len)

    if concrete and upper > 90 and random.random() < 0.35:
        upper = min(upper, random.randint(90, min(550, audio_len)))

    if upper <= min_ms:
        frag_len = upper
    else:
        frag_len = random.randint(min_ms, upper)

    if frag_len <= 0:
        return audio

    if audio_len <= frag_len:
        start = 0
    else:
        start = random.randint(0, audio_len - frag_len)

    return audio[start : start + frag_len]


def change_speed(audio: AudioSegment, speed: float) -> AudioSegment:
    if speed == 1.0:
        return audio

    altered = audio._spawn(
        audio.raw_data,
        overrides={"frame_rate": int(audio.frame_rate * speed)},
    )
    return altered.set_frame_rate(audio.frame_rate)


def make_hiss(duration_ms: int, frame_rate: int = 44100) -> AudioSegment:
    seg = AudioSegment.silent(duration=duration_ms, frame_rate=frame_rate)
    tick = AudioSegment.silent(duration=23, frame_rate=frame_rate) - 60
    bed = seg

    pos = 0
    while pos < duration_ms:
        burst = tick.apply_gain(random.uniform(-18, -8))
        bed = bed.overlay(burst, position=pos)
        pos += random.randint(12, 37)

    bed = high_pass_filter(bed, 4500)
    bed = low_pass_filter(bed, 11000)
    return bed - 12


def shape_fragment(
    audio: AudioSegment,
    concrete: bool = False,
) -> Tuple[AudioSegment, dict]:
    reversed_flag = random.random() < (0.32 if concrete else 0.18)
    speed_choices = (
        [0.62, 0.74, 0.85, 0.92, 1.0, 1.13, 1.28, 1.45]
        if concrete
        else [0.78, 0.85, 0.92, 1.0, 1.08, 1.18, 1.28]
    )
    speed = random.choice(speed_choices)
    repeated = 1

    if reversed_flag:
        audio = audio.reverse()

    audio = change_speed(audio, speed)

    if concrete and len(audio) > 120 and random.random() < 0.35:
        chop = random.randint(60, min(220, len(audio)))
        start = random.randint(0, max(0, len(audio) - chop))
        audio = audio[start : start + chop]

    if random.random() < (0.42 if concrete else 0.22) and len(audio) < (
        2400 if concrete else 1800
    ):
        repeated = random.choice([2, 3, 4, 5] if concrete else [2, 3, 4])
        gap = AudioSegment.silent(
            duration=random.randint(10, 90) if concrete else random.randint(35, 140),
            frame_rate=audio.frame_rate,
        )
        built = AudioSegment.silent(duration=0, frame_rate=audio.frame_rate)
        for _ in range(repeated):
            built += audio + gap
        audio = built

    hp = random.choice(
        [120, 180, 240, 350, 500, 800, 1200]
        if concrete
        else [90, 120, 180, 240, 350, 500, 800]
    )
    lp = random.choice(
        [1400, 2200, 3200, 4200, 6000, 9000]
        if concrete
        else [2200, 3200, 4200, 6000, 9000, 12000]
    )

    if random.random() < 0.9:
        audio = high_pass_filter(audio, hp)
    if random.random() < 0.9:
        audio = low_pass_filter(audio, lp)

    if concrete and random.random() < 0.3:
        silence = AudioSegment.silent(
            duration=random.randint(20, 120),
            frame_rate=audio.frame_rate,
        )
        audio = silence + audio

    fade_in = min(20 if concrete else 25, max(3, len(audio) // 16))
    fade_out = min(40 if concrete else 60, max(6, len(audio) // 10))
    audio = audio.fade_in(fade_in).fade_out(fade_out)

    return audio, {
        "reversed": reversed_flag,
        "speed": speed,
        "repeated": repeated,
        "hp_hz": hp,
        "lp_hz": lp,
    }


def place_events(
    samples: List[SampleFile],
    total_ms: int,
    density: str,
    min_frag_ms: int,
    max_frag_ms: int,
    concrete: bool = False,
) -> Tuple[AudioSegment, AudioSegment, AudioSegment, List[Event]]:
    voice_main = AudioSegment.silent(duration=total_ms, frame_rate=44100)
    voice_cuts = AudioSegment.silent(duration=total_ms, frame_rate=44100)
    ghosts = AudioSegment.silent(duration=total_ms, frame_rate=44100)
    events: List[Event] = []

    n_events = choose_event_count(total_ms / 1000.0, density, concrete=concrete)
    current_anchor = 0

    for i in range(n_events):
        s = weighted_choice(samples, concrete=concrete)
        src = AudioSegment.from_file(s.path).set_frame_rate(44100).set_channels(2)

        frag = slice_fragment(src, min_frag_ms, max_frag_ms, concrete=concrete)
        shaped, meta = shape_fragment(frag, concrete=concrete)

        layer_name = random.choices(
            ["voice_main", "voice_cuts", "ghosts"],
            weights=[4, 3, 2] if not concrete else [3, 4, 3],
            k=1,
        )[0]

        if density == "sparse":
            jitter = random.randint(-150, 900)
            step = random.randint(1200, 4200)
        elif density == "dense":
            jitter = random.randint(-400, 650)
            step = random.randint(300, 1700)
        else:
            jitter = random.randint(-250, 800)
            step = random.randint(700, 2600)

        if concrete:
            jitter = int(jitter * 0.65)
            step = max(90, int(step * 0.58))

        if i == 0:
            pos = random.randint(0, 900 if not concrete else 400)
        else:
            pos = max(0, current_anchor + jitter)

        if pos + len(shaped) >= total_ms:
            pos = max(0, total_ms - len(shaped) - random.randint(50, 600))

        current_anchor = min(total_ms - 100, pos + step)

        if layer_name == "voice_main":
            gain = (
                random.uniform(-8.0, -2.0)
                if not concrete
                else random.uniform(-10.0, -3.5)
            )
            target = voice_main
        elif layer_name == "voice_cuts":
            gain = (
                random.uniform(-14.0, -6.0)
                if not concrete
                else random.uniform(-12.5, -5.0)
            )
            target = voice_cuts
        else:
            gain = (
                random.uniform(-19.0, -10.0)
                if not concrete
                else random.uniform(-17.0, -8.5)
            )
            if random.random() < (0.35 if not concrete else 0.55):
                shaped = shaped.reverse()
            target = ghosts

        target = target.overlay(shaped.apply_gain(gain), position=pos)

        if layer_name == "voice_main":
            voice_main = target
        elif layer_name == "voice_cuts":
            voice_cuts = target
        else:
            ghosts = target

        events.append(
            Event(
                layer=layer_name,
                source=str(s.path),
                start_ms=pos,
                end_ms=pos + len(shaped),
                gain_db=round(gain, 2),
                reversed=meta["reversed"],
                speed=meta["speed"],
                repeated=meta["repeated"],
                hp_hz=meta["hp_hz"],
                lp_hz=meta["lp_hz"],
            )
        )

    return voice_main, voice_cuts, ghosts, events


def export_manifest(path: Path, events: List[Event]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "layer",
                "source",
                "start_ms",
                "end_ms",
                "gain_db",
                "reversed",
                "speed",
                "repeated",
                "hp_hz",
                "lp_hz",
            ],
        )
        writer.writeheader()

        for e in events:
            writer.writerow(
                {
                    "layer": e.layer,
                    "source": e.source,
                    "start_ms": e.start_ms,
                    "end_ms": e.end_ms,
                    "gain_db": e.gain_db,
                    "reversed": e.reversed,
                    "speed": e.speed,
                    "repeated": e.repeated,
                    "hp_hz": e.hp_hz,
                    "lp_hz": e.lp_hz,
                }
            )


def normalize_master(audio: AudioSegment, master_gain: float) -> AudioSegment:
    audio = compress_dynamic_range(
        audio,
        threshold=-22.0,
        ratio=2.2,
        attack=8,
        release=140,
    )
    return audio.apply_gain(master_gain)


def build_variant(
    samples: List[SampleFile],
    output_root: Path,
    variant_idx: int,
    duration_s: float,
    bed_noise: bool,
    sample_rate: int,
    master_gain: float,
    min_frag_s: float,
    max_frag_s: float,
    density: str,
    concrete: bool = False,
) -> None:
    total_ms = int(duration_s * 1000)
    min_frag_ms = max(20, int(min_frag_s * 1000))
    max_frag_ms = max(min_frag_ms, int(max_frag_s * 1000))

    variant_name = f"cutup_{variant_idx:02d}"
    variant_dir = output_root / variant_name
    stems_dir = variant_dir / "stems"

    variant_dir.mkdir(parents=True, exist_ok=True)
    stems_dir.mkdir(parents=True, exist_ok=True)

    main, cuts, ghosts, events = place_events(
        samples=samples,
        total_ms=total_ms,
        density=density,
        min_frag_ms=min_frag_ms,
        max_frag_ms=max_frag_ms,
        concrete=concrete,
    )

    if bed_noise:
        hiss = make_hiss(total_ms, frame_rate=sample_rate)
    else:
        hiss = AudioSegment.silent(duration=total_ms, frame_rate=sample_rate)

    ghosts = ghosts - 2
    cuts = cuts + 1
    main = main + 2

    master = AudioSegment.silent(duration=total_ms, frame_rate=sample_rate).set_channels(2)
    master = master.overlay(hiss, position=0)
    master = master.overlay(ghosts, position=0)
    master = master.overlay(cuts, position=0)
    master = master.overlay(main, position=0)

    master = normalize_master(master, master_gain)

    main.export(stems_dir / "voice_main.wav", format="wav")
    cuts.export(stems_dir / "voice_cuts.wav", format="wav")
    ghosts.export(stems_dir / "ghosts.wav", format="wav")
    hiss.export(stems_dir / "hiss_bed.wav", format="wav")
    master.export(variant_dir / f"{variant_name}_master.wav", format="wav")

    export_manifest(variant_dir / f"{variant_name}_events.csv", events)


def run_audio_mode(args: argparse.Namespace, output_root: Path) -> None:
    if not args.input:
        raise SystemExit(
            "--input is required for --mode audio, --mode both, and --mode all"
        )

    input_root = Path(args.input).expanduser().resolve()
    samples = discover_samples(input_root)

    if not samples:
        raise SystemExit(f"No audio samples found in {input_root}")

    audio_out = output_root / "audio_cutups"
    audio_out.mkdir(parents=True, exist_ok=True)

    for i in range(1, args.variants + 1):
        build_variant(
            samples=samples,
            output_root=audio_out,
            variant_idx=i,
            duration_s=args.duration,
            bed_noise=args.bed_noise,
            sample_rate=args.sample_rate,
            master_gain=args.master_gain,
            min_frag_s=args.min_frag,
            max_frag_s=args.max_frag,
            density=args.density,
            concrete=args.concrete,
        )

    print(f"Built {args.variants} audio cut-up composition(s) in: {audio_out}")


# -------------------------------------------------------------------
# AGITPROP MODE
# -------------------------------------------------------------------

TAG_RULES: Dict[str, List[str]] = {
    "official": [
        "public interest",
        "obligations",
        "accountable",
        "administration",
        "commission",
        "federal",
        "regulatory",
        "fcc",
        "official",
        "authority",
        "policy",
        "department",
        "president",
    ],
    "threat": [
        "threat",
        "threatening",
        "license",
        "licenses",
        "revocation",
        "revocations",
        "fines",
        "punish",
        "pressure",
        "take down",
        "censor",
        "censorship",
    ],
    "freedom": [
        "free speech",
        "free expression",
        "first amendment",
        "speech",
        "rights",
        "liberty",
    ],
    "broadcast": [
        "broadcast",
        "broadcasters",
        "station",
        "programming",
        "hosts",
        "media",
        "air",
    ],
    "command": [
        "hold",
        "must",
        "need to",
        "have to",
        "do it",
        "stop",
        "ask",
        "go ahead",
        "look",
        "see",
    ],
    "collapse": [
        "fear",
        "ending",
        "threats",
        "silence",
        "erasure",
        "casualty",
        "stifling",
        "danger",
        "attack",
        "attacks",
    ],
    "bureaucratic": [
        "obligations",
        "public interest",
        "accountable",
        "regulatory",
        "licenses",
        "revocations",
        "federal",
        "commission",
    ],
}


def tag_text(text: str) -> List[str]:
    t = text.lower()
    tags = set()

    for tag, terms in TAG_RULES.items():
        if any(term in t for term in terms):
            tags.add(tag)

    wc = count_words(text)

    if 1 <= wc <= 3:
        tags.add("micro")
    elif 4 <= wc <= 8:
        tags.add("short")
    elif 9 <= wc <= 16:
        tags.add("phrase")
    else:
        tags.add("long")

    if re.search(r"\bwe\b", t):
        tags.add("collective")
    if re.search(r"\byou\b", t):
        tags.add("address")
    if re.search(r"\bthey\b", t):
        tags.add("accusation")
    if re.search(r"\bno\b|\bnot\b|\bnever\b", t):
        tags.add("negation")

    if not tags:
        tags.add("loose")

    return sorted(tags)


def load_top300(path: Path) -> List[Line]:
    rows: List[Line] = []

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)

        for row in reader:
            text = clean_text(row.get("text", ""))
            if not is_usable_text(text):
                continue

            rows.append(
                Line(
                    text=text,
                    file=row.get("file", ""),
                    clip_id=row.get("clip_id", ""),
                    cue_index=int(float(row.get("cue_index", 0) or 0)),
                    start_tc=row.get("start_tc", ""),
                    end_tc=row.get("end_tc", ""),
                    duration_sec=float(row.get("duration_sec", 0) or 0),
                    source_bank="top300",
                    score=float(row.get("score", 0) or 0),
                    loop_bin=row.get("loop_bin", ""),
                    intensity=row.get("intensity", ""),
                    word_count=count_words(text),
                    tags=tag_text(text),
                )
            )

    return rows


def load_full(path: Path) -> List[Line]:
    rows: List[Line] = []

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)

        for row in reader:
            text = clean_text(row.get("text", ""))
            if not is_usable_text(text):
                continue

            rows.append(
                Line(
                    text=text,
                    file=row.get("file", ""),
                    clip_id=row.get("clip_id", ""),
                    cue_index=int(float(row.get("cue_index", 0) or 0)),
                    start_tc=row.get("start_tc", ""),
                    end_tc=row.get("end_tc", ""),
                    duration_sec=float(row.get("duration_sec", 0) or 0),
                    source_bank="full",
                    score=0.0,
                    loop_bin="",
                    intensity="",
                    word_count=count_words(text),
                    tags=tag_text(text),
                )
            )

    return rows


def agitprop_weighted_choice(pool: List[Line]) -> Line:
    weights = []

    for line in pool:
        w = 1.0
        wc = line.word_count
        dur = line.duration_sec

        if line.source_bank == "top300":
            w += 3.0
            w += max(0.0, line.score / 12.0)

            if line.intensity == "max":
                w += 1.5
            elif line.intensity == "high":
                w += 1.0

            if line.loop_bin == "micro":
                w += 1.7
            elif line.loop_bin == "short":
                w += 1.2
            elif line.loop_bin == "phrase":
                w += 0.7
        else:
            if 2 <= wc <= 10:
                w += 1.3
            if 0.4 <= dur <= 3.2:
                w += 1.0

        if 2 <= wc <= 8:
            w += 1.2
        elif wc <= 14:
            w += 0.5

        if 0.45 <= dur <= 3.5:
            w += 1.0
        elif dur <= 6:
            w += 0.4

        if "official" in line.tags:
            w += 0.9
        if "threat" in line.tags:
            w += 1.0
        if "freedom" in line.tags:
            w += 0.8
        if "bureaucratic" in line.tags:
            w += 0.7
        if "micro" in line.tags:
            w += 0.7
        if "short" in line.tags:
            w += 0.6

        weights.append(max(0.1, w))

    return random.choices(pool, weights=weights, k=1)[0]


def choose_line(
    bank: List[Line],
    required_tags: Iterable[str] = (),
    excluded_texts: Optional[set[str]] = None,
    fallback: bool = True,
) -> Line:
    required_tags = list(required_tags)
    excluded_texts = excluded_texts or set()

    pool = [
        x
        for x in bank
        if all(tag in x.tags for tag in required_tags) and x.text not in excluded_texts
    ]

    if not pool and fallback:
        pool = [x for x in bank if x.text not in excluded_texts]

    if not pool:
        raise ValueError("No available lines matched selection criteria.")

    return agitprop_weighted_choice(pool)


def compress_phrase(text: str, max_words: int = 6) -> str:
    words = [w.upper() for w in TOKEN_RE.findall(text) if len(w) > 2]
    return " ".join(words[:max_words]).strip()


def fragment(text: str, min_words: int = 2, max_words: int = 6) -> str:
    words = text.split()

    if not words:
        return text

    upper_bound = min(max_words, len(words))
    lower_bound = min(min_words, upper_bound)
    take = random.randint(lower_bound, upper_bound)
    return " ".join(words[:take]).strip()


def repeat_word(text: str, n: Optional[int] = None) -> str:
    words = TOKEN_RE.findall(text)

    if not words:
        return text.upper()

    target = max(words, key=len).upper()
    count = n if n is not None else random.randint(2, 4)

    return "\n".join([target] * count)


def contradiction_line(a: str, b: str) -> str:
    return f"{a.upper()}\n{b.lower()}"


def weaponize(text: str) -> str:
    swaps = {
        "public interest": "managed obedience",
        "accountable": "disciplined",
        "free speech": "licensed speech",
        "first amendment": "first warning",
        "patriotic programming": "mandatory patriotism",
        "regulatory action": "permission to punish",
    }

    out = text
    for src, dst in swaps.items():
        out = re.sub(src, dst, out, flags=re.I)

    return out


def cut_words(text: str) -> List[str]:
    return TOKEN_RE.findall(text)


def splice_halves(a: str, b: str) -> str:
    aw = cut_words(a)
    bw = cut_words(b)

    if not aw or not bw:
        return f"{a} {b}".strip()

    a_mid = max(1, len(aw) // 2)
    b_mid = max(1, len(bw) // 2)

    left = " ".join(aw[:a_mid])
    right = " ".join(bw[b_mid:])

    return f"{left} {right}".strip()


def stutter_phrase(text: str) -> str:
    words = cut_words(text)
    if not words:
        return text.upper()

    if len(words) == 1:
        w = words[0].upper()
        return f"{w} / {w} / {w}"

    pivot = random.choice(words[: min(4, len(words))]).upper()
    tail = " ".join(words[: min(len(words), random.randint(2, 5))]).upper()
    return f"{pivot} / {pivot} / {tail}"


def fragment_stack(a: str, b: str, c: str) -> str:
    return "\n".join(
        [
            fragment(a).upper(),
            fragment(b).lower(),
            fragment(c).upper(),
        ]
    )


def bureaucratic_melt(text: str) -> str:
    swaps = {
        "public interest": "managed interest",
        "accountable": "countable",
        "federal communications commission": "federal transmission commission",
        "free speech": "metered speech",
        "first amendment": "first adjustment",
        "broadcasters": "bodies casting",
        "license": "permission",
        "licenses": "permissions",
        "authority": "authorized fear",
        "policy": "signal policy",
    }

    out = text
    for src, dst in swaps.items():
        out = re.sub(src, dst, out, flags=re.I)

    return out


def recursive_burst(text: str) -> str:
    words = cut_words(text)
    if not words:
        return text.upper()

    picks = words[: min(len(words), 4)]
    lines = []

    for i in range(1, len(picks) + 1):
        lines.append(" ".join(w.upper() for w in picks[:i]))

    return "\n".join(lines)


def cutup_block(a: str, b: str, c: str) -> str:
    mode = random.randint(0, 5)

    if mode == 0:
        return splice_halves(a, b).upper()
    if mode == 1:
        return stutter_phrase(a)
    if mode == 2:
        return fragment_stack(a, b, c)
    if mode == 3:
        return contradiction_line(fragment(a).upper(), bureaucratic_melt(fragment(b)))
    if mode == 4:
        return recursive_burst(c)
    return f"{splice_halves(a, b).upper()}\n{fragment(c).lower()}"


def build_slogan(top300: List[Line], full: List[Line]) -> str:
    used: set[str] = set()

    a = choose_line(top300, excluded_texts=used)
    used.add(a.text)

    b = choose_line(full, excluded_texts=used)
    used.add(b.text)

    c = choose_line(top300, excluded_texts=used)
    used.add(c.text)

    d = choose_line(full, excluded_texts=used)
    used.add(d.text)

    mode = random.randint(0, 7)

    if mode == 0:
        return cutup_block(a.text, b.text, c.text)

    if mode == 1:
        return f"{stutter_phrase(a.text)}\n{fragment(d.text).lower()}"

    if mode == 2:
        return (
            f"{splice_halves(a.text, c.text).upper()}\n"
            f"{splice_halves(b.text, d.text).lower()}"
        )

    if mode == 3:
        return f"{recursive_burst(a.text)}\n{fragment(b.text).lower()}"

    if mode == 4:
        return f"{bureaucratic_melt(compress_phrase(a.text))}\n{repeat_word(c.text, n=3)}"

    if mode == 5:
        return f"{fragment_stack(a.text, b.text, c.text)}\n{fragment(d.text).lower()}"

    if mode == 6:
        return contradiction_line(
            splice_halves(a.text, b.text).upper(),
            bureaucratic_melt(splice_halves(c.text, d.text)).lower(),
        )

    return f"{stutter_phrase(a.text)}\n{recursive_burst(c.text)}"


def build_broadcast(top300: List[Line], full: List[Line]) -> str:
    used: set[str] = set()

    official = choose_line(
        top300,
        required_tags=["official"],
        excluded_texts=used,
        fallback=True,
    )
    used.add(official.text)

    threat = choose_line(
        top300,
        required_tags=["threat"],
        excluded_texts=used,
        fallback=True,
    )
    used.add(threat.text)

    freedom = choose_line(
        top300,
        required_tags=["freedom"],
        excluded_texts=used,
        fallback=True,
    )
    used.add(freedom.text)

    command = choose_line(
        top300,
        required_tags=["command"],
        excluded_texts=used,
        fallback=True,
    )
    used.add(command.text)

    bridge1 = choose_line(full, excluded_texts=used, fallback=True)
    used.add(bridge1.text)

    bridge2 = choose_line(full, excluded_texts=used, fallback=True)
    used.add(bridge2.text)

    mode = random.randint(0, 4)

    if mode == 0:
        return (
            f"{compress_phrase(official.text)}\n\n"
            f"{fragment(bridge1.text).lower()}\n\n"
            f"{repeat_word(threat.text, n=2)}\n\n"
            f"{bureaucratic_melt(compress_phrase(freedom.text))}\n\n"
            f"{stutter_phrase(command.text)}"
        )

    if mode == 1:
        return (
            f"{splice_halves(official.text, threat.text).upper()}\n\n"
            f"{fragment_stack(bridge1.text, freedom.text, bridge2.text)}\n\n"
            f"{repeat_word(command.text, n=3)}"
        )

    if mode == 2:
        return (
            f"{recursive_burst(official.text)}\n\n"
            f"{fragment(bridge1.text).lower()}\n\n"
            f"{contradiction_line(compress_phrase(freedom.text), bureaucratic_melt(threat.text))}\n\n"
            f"{fragment(bridge2.text).lower()}"
        )

    if mode == 3:
        return (
            f"{stutter_phrase(official.text)}\n\n"
            f"{repeat_word(threat.text, n=2)}\n\n"
            f"{fragment(bridge1.text).lower()}\n\n"
            f"{fragment(command.text).upper()}\n"
            f"{fragment(command.text).lower()}"
        )

    return (
        f"{cutup_block(official.text, bridge1.text, threat.text)}\n\n"
        f"{fragment(bridge2.text).lower()}\n\n"
        f"{bureaucratic_melt(compress_phrase(freedom.text))}\n\n"
        f"{recursive_burst(command.text)}"
    )


def build_chant_cell(top300: List[Line], full: List[Line]) -> Dict[str, str]:
    use_full = random.random() < 0.3
    bank = full if use_full else top300

    line = choose_line(bank)
    partner = choose_line(top300 if use_full else full, fallback=True)

    mode = random.choice(["chant", "loop", "burst", "call", "splice", "stutter"])

    if mode == "chant":
        text = compress_phrase(line.text)
        delivery = "shouted"
    elif mode == "loop":
        text = repeat_word(line.text, n=3)
        delivery = "hard repeat"
    elif mode == "burst":
        text = fragment(line.text).upper()
        delivery = "short burst"
    elif mode == "call":
        text = f"{fragment(line.text).upper()}\n{fragment(partner.text).lower()}"
        delivery = "call-response"
    elif mode == "splice":
        text = splice_halves(line.text, partner.text).upper()
        delivery = "cut splice"
    else:
        text = stutter_phrase(line.text)
        delivery = "stutter chant"

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


def run_agitprop_mode(args: argparse.Namespace, output_root: Path) -> Path:
    top300_path = Path(args.top300_csv).expanduser().resolve()
    full_path = Path(args.full_csv).expanduser().resolve()

    if not top300_path.exists():
        raise SystemExit(f"Missing --top300-csv file: {top300_path}")
    if not full_path.exists():
        raise SystemExit(f"Missing --full-csv file: {full_path}")

    top300 = load_top300(top300_path)
    full = load_full(full_path)

    if not top300:
        raise SystemExit("Top300 CSV loaded no usable lines.")
    if not full:
        raise SystemExit("Full subtitles CSV loaded no usable lines.")

    agit_out = output_root / "agitprop"
    agit_out.mkdir(parents=True, exist_ok=True)

    slogans = [build_slogan(top300, full) for _ in range(args.agitprop_count)]
    broadcasts = [build_broadcast(top300, full) for _ in range(args.broadcast_count)]
    chant_cells = [build_chant_cell(top300, full) for _ in range(args.chant_count)]

    with (agit_out / "slogans.txt").open("w", encoding="utf-8") as f:
        for s in slogans:
            f.write(s.strip())
            f.write("\n\n")

    with (agit_out / "broadcasts.txt").open("w", encoding="utf-8") as f:
        for s in broadcasts:
            f.write(s.strip())
            f.write("\n\n")

    chant_path = agit_out / "chant_cells.csv"
    with chant_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "mode",
                "text",
                "delivery",
                "source_bank",
                "file",
                "clip_id",
                "cue_index",
                "start_tc",
                "end_tc",
            ],
        )
        writer.writeheader()
        writer.writerows(chant_cells)

    print(f"Built agit-prop outputs in: {agit_out}")
    return chant_path


# -------------------------------------------------------------------
# CUT TARGETS MODE
# -------------------------------------------------------------------


def load_source_csv(path: Path, bank_name: str) -> List[SourceRow]:
    rows: List[SourceRow] = []

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)

        for row in reader:
            text = str(row.get("text", "")).strip()
            if not text:
                continue

            rows.append(
                SourceRow(
                    text=text,
                    file=str(row.get("file", "")),
                    clip_id=str(row.get("clip_id", "")),
                    cue_index=str(row.get("cue_index", "")),
                    start_tc=str(row.get("start_tc", "")),
                    end_tc=str(row.get("end_tc", "")),
                    duration_sec=str(row.get("duration_sec", "")),
                    source_bank=bank_name,
                )
            )

    return rows


def load_chant_cells(path: Path) -> List[dict]:
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def overlap_score(a: str, b: str) -> float:
    a_tokens = set(token_list(a))
    b_tokens = set(token_list(b))

    if not a_tokens or not b_tokens:
        return 0.0

    inter = len(a_tokens & b_tokens)
    union = len(a_tokens | b_tokens)
    return inter / union if union else 0.0


def best_matches(
    query: str,
    source_rows: List[SourceRow],
    top_n: int = 3,
) -> List[Tuple[float, SourceRow]]:
    scored: List[Tuple[float, SourceRow]] = []
    q_norm = normalize_text(query)

    for row in source_rows:
        score = overlap_score(q_norm, row.text)
        row_norm = normalize_text(row.text)

        if q_norm in row_norm or row_norm in q_norm:
            score += 0.35

        if score > 0:
            scored.append((score, row))

    scored.sort(key=lambda x: x[0], reverse=True)
    return scored[:top_n]


def run_cuttargets_mode(
    args: argparse.Namespace,
    output_root: Path,
    chant_cells_path: Optional[Path] = None,
) -> Path:
    top300_path = Path(args.top300_csv).expanduser().resolve()
    full_path = Path(args.full_csv).expanduser().resolve()

    if chant_cells_path is None:
        if args.chant_cells_csv:
            chant_cells_path = Path(args.chant_cells_csv).expanduser().resolve()
        else:
            chant_cells_path = (output_root / "agitprop" / "chant_cells.csv").resolve()

    if not top300_path.exists():
        raise SystemExit(f"Missing --top300-csv file: {top300_path}")
    if not full_path.exists():
        raise SystemExit(f"Missing --full-csv file: {full_path}")
    if not chant_cells_path.exists():
        raise SystemExit(f"Missing chant_cells.csv: {chant_cells_path}")

    top300_rows = load_source_csv(top300_path, "top300")
    full_rows = load_source_csv(full_path, "full")
    all_rows = top300_rows + full_rows

    chant_cells = load_chant_cells(chant_cells_path)

    out_path = output_root / "agitprop" / "cut_targets.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    out_rows: List[Dict[str, str]] = []

    for i, cell in enumerate(chant_cells, start=1):
        text = str(cell.get("text", "")).replace("\n", " ").strip()
        mode = str(cell.get("mode", ""))
        delivery = str(cell.get("delivery", ""))

        matches = best_matches(text, all_rows, top_n=args.cut_match_count)

        if not matches:
            out_rows.append(
                {
                    "cell_index": str(i),
                    "mode": mode,
                    "delivery": delivery,
                    "generated_text": text,
                    "match_rank": "",
                    "match_score": "",
                    "source_bank": "",
                    "file": "",
                    "clip_id": "",
                    "cue_index": "",
                    "start_tc": "",
                    "end_tc": "",
                    "duration_sec": "",
                    "source_text": "",
                }
            )
            continue

        for rank, (score, row) in enumerate(matches, start=1):
            out_rows.append(
                {
                    "cell_index": str(i),
                    "mode": mode,
                    "delivery": delivery,
                    "generated_text": text,
                    "match_rank": str(rank),
                    "match_score": f"{score:.3f}",
                    "source_bank": row.source_bank,
                    "file": row.file,
                    "clip_id": row.clip_id,
                    "cue_index": row.cue_index,
                    "start_tc": row.start_tc,
                    "end_tc": row.end_tc,
                    "duration_sec": row.duration_sec,
                    "source_text": row.text,
                }
            )

    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "cell_index",
                "mode",
                "delivery",
                "generated_text",
                "match_rank",
                "match_score",
                "source_bank",
                "file",
                "clip_id",
                "cue_index",
                "start_tc",
                "end_tc",
                "duration_sec",
                "source_text",
            ],
        )
        writer.writeheader()
        writer.writerows(out_rows)

    print(f"Built cut target map: {out_path}")
    return out_path


# -------------------------------------------------------------------
# MAIN
# -------------------------------------------------------------------


def main() -> None:
    args = parse_args()
    random.seed(args.seed)

    output_root = Path(args.output).expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    if args.mode == "audio":
        run_audio_mode(args, output_root)
        return

    if args.mode == "agitprop":
        run_agitprop_mode(args, output_root)
        return

    if args.mode == "cuttargets":
        run_cuttargets_mode(args, output_root)
        return

    if args.mode == "both":
        run_agitprop_mode(args, output_root)
        run_audio_mode(args, output_root)
        return

    if args.mode == "all":
        chant_path = run_agitprop_mode(args, output_root)
        run_cuttargets_mode(args, output_root, chant_cells_path=chant_path)
        run_audio_mode(args, output_root)
        return


if __name__ == "__main__":
    main()
