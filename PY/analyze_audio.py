"""Standalone audio analysis cache builder for TRANSMISSIONS cutups."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional, Sequence

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from PY import cutup


def default_cache_path(input_path: Path) -> Path:
    resolved = input_path.expanduser().resolve()
    if resolved.is_dir():
        return resolved / "audio_analysis_cache.json"
    return resolved.with_name(f"{resolved.stem}_audio_analysis_cache.json")


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Precompute a cutups audio_analysis_cache.json for faster later renders.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--input", required=True, help="Audio file or folder to analyze.")
    parser.add_argument("--output", default="", help="Cache JSON path. Defaults beside the input.")
    parser.add_argument("--overwrite", action="store_true", help="Allow updating/replacing an existing cache file.")
    parser.add_argument("--sample-rate", type=int, default=44100, help="Analysis sample rate.")
    parser.add_argument("--cue-file", default="", help="Optional SRT or CSV cue file to analyze phrase spans.")
    parser.add_argument("--source-manifest", default="", help="Optional CSV/JSON source manifest to apply before caching.")
    parser.add_argument("--max-files", type=int, default=0, help="Limit analyzed source entries after discovery; 0 analyzes all.")
    parser.add_argument("--bpm", type=float, default=0.0, help="Optional beat-grid BPM for grid cell summaries.")
    parser.add_argument("--slice-grid", choices=sorted(cutup.SLICE_GRID_FACTORS), default="off", help="Beat-grid unit when BPM is set.")
    parser.add_argument("--beat-jump-mode", choices=["random", "similarity"], default="similarity", help="Write similarity neighbor metadata when set to similarity.")
    parser.add_argument("--beat-similarity-weight", type=float, default=1.0, help="Stored planner weight for later beat similarity renders.")
    parser.add_argument("--beat-novelty", type=float, default=0.0, help="Stored novelty bias for later beat similarity renders.")
    parser.add_argument("--progress-jsonl", default="", help="Optional JSONL file for analysis progress telemetry.")
    parser.add_argument("--no-progress", action="store_true", help="Disable terminal progress bar output.")
    return parser.parse_args(argv)


def validate_args(args: argparse.Namespace) -> argparse.Namespace:
    input_path = Path(args.input).expanduser().resolve()
    if not input_path.exists():
        raise SystemExit(f"--input path not found: {input_path}")
    if not input_path.is_file() and not input_path.is_dir():
        raise SystemExit(f"--input must be an audio file or directory: {input_path}")
    if input_path.is_file() and input_path.suffix.lower() not in cutup.AUDIO_EXTS:
        raise SystemExit(f"--input must be an audio file or directory: {input_path}")
    if args.sample_rate < 8000:
        raise SystemExit("--sample-rate must be >= 8000")
    if args.max_files < 0:
        raise SystemExit("--max-files must be >= 0")
    if args.bpm < 0:
        raise SystemExit("--bpm must be >= 0")
    if args.bpm and not 20 <= args.bpm <= 300:
        raise SystemExit("--bpm must be 20..300, or 0 to disable beat-grid behavior")
    if args.bpm <= 0 and args.slice_grid != "off":
        raise SystemExit("--slice-grid requires --bpm")
    if not 0.0 <= args.beat_similarity_weight <= 1.0:
        raise SystemExit("--beat-similarity-weight must be 0..1")
    if not 0.0 <= args.beat_novelty <= 1.0:
        raise SystemExit("--beat-novelty must be 0..1")
    if args.progress_jsonl:
        progress_path = Path(args.progress_jsonl).expanduser().resolve()
        if progress_path.exists() and progress_path.is_dir():
            raise SystemExit(f"--progress-jsonl is a directory: {progress_path}")
    if args.source_manifest:
        manifest_path = Path(args.source_manifest).expanduser().resolve()
        if not manifest_path.exists() or not manifest_path.is_file():
            raise SystemExit(f"--source-manifest not found: {manifest_path}")
        if manifest_path.suffix.lower() not in {".csv", ".json"}:
            raise SystemExit("--source-manifest must be a .csv or .json file")
    if args.cue_file:
        cue_path = Path(args.cue_file).expanduser().resolve()
        if not cue_path.exists() or not cue_path.is_file():
            raise SystemExit(f"--cue-file not found: {cue_path}")
        if cue_path.suffix.lower() not in {".srt", ".csv"}:
            raise SystemExit("--cue-file must be a .srt or .csv file")
    return args


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = validate_args(parse_args(argv))
    input_root = Path(args.input).expanduser().resolve()
    cache_path = Path(args.output).expanduser().resolve() if args.output else default_cache_path(input_root)
    progress_path = Path(args.progress_jsonl).expanduser().resolve() if args.progress_jsonl else None
    if progress_path:
        progress_path.parent.mkdir(parents=True, exist_ok=True)
        progress_path.write_text("", encoding="utf-8")
    live = cutup.LiveControlState(enabled=bool(progress_path), telemetry_path=progress_path)
    progress = cutup.ProgressReporter(enabled=bool(not args.no_progress and sys.stderr.isatty()), live=live)

    progress.update(0.0, "analysis", "checking backend", force=True)
    cutup.ensure_audio_backend()
    existing_payload = cutup.load_analysis_cache(cache_path) if cache_path.exists() else {}
    progress.update(0.08, "analysis", "discovering source audio", force=True)
    if args.cue_file:
        samples, unreadable = cutup.discover_cue_samples(input_root, Path(args.cue_file).expanduser().resolve())
    else:
        samples, unreadable = cutup.discover_samples(
            input_root,
            analysis_cache_payload=existing_payload,
            sample_rate=int(args.sample_rate),
        )

    if args.max_files:
        samples = samples[: args.max_files]
    progress.update(0.24, "analysis", f"loaded {len(samples)} source item(s)", force=True)
    if args.source_manifest:
        manifest_path = Path(args.source_manifest).expanduser().resolve()
        progress.update(0.28, "analysis", "applying source manifest", force=True)
        entries = cutup.load_source_manifest(manifest_path, input_root)
        matched = cutup.apply_source_manifest(samples, entries, input_root)
    else:
        manifest_path = None
        matched = 0

    if not samples:
        hint = f" ({unreadable} files/cue rows could not be decoded)" if unreadable else ""
        raise SystemExit(f"No usable audio entries found in {input_root}{hint}")

    cache_args = argparse.Namespace(
        overwrite=bool(args.overwrite),
        sample_rate=int(args.sample_rate),
        bpm=float(args.bpm),
        slice_grid=str(args.slice_grid),
        beat_jump_mode=str(args.beat_jump_mode),
        beat_similarity_weight=float(args.beat_similarity_weight),
        beat_novelty=float(args.beat_novelty),
    )

    def report_cache_progress(index: int, total: int, state: str) -> None:
        progress.update(
            0.35 + 0.55 * (index / max(1, total)),
            "analysis",
            f"cache {index}/{total} {state}",
            force=index == total,
        )

    progress.update(0.35, "analysis", "writing analysis cache", force=True)
    cutup.write_analysis_cache(cache_path, samples, cache_args, input_root, progress_callback=report_cache_progress)
    progress.update(0.94, "analysis", "reading cache summary", force=True)
    payload = cutup.load_analysis_cache(cache_path)
    stats = payload.get("cache_stats", {}) if isinstance(payload, dict) else {}

    print("CUTUPS AUDIO ANALYSIS")
    print(f"input: {input_root}")
    print(f"cache: {cache_path}")
    print(f"samples: {len(samples)}")
    print(f"unreadable: {unreadable}")
    if manifest_path:
        print(f"source_manifest: {manifest_path} (matched={matched}/{len(samples)})")
    print(f"grid_ms: {payload.get('grid_ms', 0) if isinstance(payload, dict) else 0}")
    print(f"beat_jump_mode: {args.beat_jump_mode}")
    if isinstance(stats, dict):
        print(f"cache_stats: reused={stats.get('reused', 0)} refreshed={stats.get('refreshed', 0)} errors={stats.get('errors', 0)}")
    print(f"next_render_flag: --analysis-cache {cache_path} --analysis-cache-readonly")
    progress.update(1.0, "analysis", "complete", force=True)
    progress.finish()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
