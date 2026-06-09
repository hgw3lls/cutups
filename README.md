# cutups

`cutups` is a generative cut-up instrument with three integrated engines:

- **Audio composition** (`--mode audio`) for speech-concrète collage.
- **Agitprop text generation** (`--mode agitprop`) for slogans, broadcasts, and chant cells.
- **Cut-target matching** (`--mode cuttargets`) for phrase targeting workflows.

The main entry points are:

- `cutups` after installation
- `PY/cutup.py`

---

## Requirements

- Python 3.10+
- `pydub`
- `ffmpeg` available on PATH (required by `pydub` for many file formats)

Install locally as an editable package:

```bash
python3 -m pip install -e .
cutups --doctor
```

The direct script entry point still works:

```bash
python3 PY/cutup.py --doctor
```

For tests:

```bash
python3 -m pip install -e ".[dev]"
python3 -m pytest
```

The test suite includes an audio smoke test that generates a temporary WAV source, renders through the real CLI, checks master/preview/event outputs, and verifies same-seed determinism. It skips automatically if `pydub` or `ffmpeg` is unavailable:

```bash
python3 -m pytest tests/test_audio_smoke.py -q
```

Optional future analysis dependencies are kept separate:

```bash
python3 -m pip install -e ".[analysis]"
```

`cutups --doctor` reports those packages under `optional analysis` without making them required for normal `status: ready`.

Installed console commands:

- `cutups`
- `cutups-live-gui`
- `cutups-live-monitor`
- `cutups-td-bridge`

See [docs/RELATED_TOOLS.md](docs/RELATED_TOOLS.md) for notes on Remixatron, Infinite Remixer, AudioGuide, and why they are references rather than vendored dependencies.

---

## Quick start

From repo root:

```bash
cutups --mode agitprop --output out/demo_text
```

```bash
cutups --mode audio --preset signal-breach --input ./samples --output out/demo_audio
```

List TRANSMISSIONS presets:

```bash
cutups --list-presets
```

Print a copy-ready command recipe:

```bash
cutups --show-recipe beat-similarity
```

Scan an audio folder and generate a starter manifest:

```bash
cutups \
  --scan-dataset ./samples \
  --write-source-manifest ./samples/source_manifest.csv \
  --write-dataset-report ./samples/dataset_report.json
```

Check local dependencies:

```bash
cutups --doctor
```

Create starter local QA WAV/cue sources outside the repo:

```bash
cutups --init-qa-sources ../cutups_qa_sources
```

Check a render setup without writing audio:

```bash
python3 PY/cutup.py --mode audio --preset spoken-word-cutup --input ./voice.wav --output out/preview --dry-run
```

Render a short audition file beside the full master:

```bash
python3 PY/cutup.py --mode audio --preset signal-breach --input ./samples --output out/breach_preview --preview-duration 12
```

See [TRANSMISSIONS_QUICKSTART.md](TRANSMISSIONS_QUICKSTART.md) for signal-breach, spoken-word, beat-cutup, radio-intrusion, hard-stutter, and ghost-transmission recipes. See [docs/MANUAL_QA.md](docs/MANUAL_QA.md) for repeatable listening checks after changes.

Recipe names for `--show-recipe` include `qa-sources`, `scan-dataset`, `signal-breach`, `spoken-word-cutup`, `spoken-word-cues`, `beat-cutup`, `beat-baseline`, `beat-similarity`, `radio-intrusion`, `hard-stutter`, `ghost-transmission`, and `all`.

Available preset names:

- `signal-breach`
- `spoken-word-cutup`
- `beat-cutup`
- `radio-intrusion`
- `hard-stutter`
- `ghost-transmission`

Beat-oriented cutups can use manual grid slicing:

```bash
python3 PY/cutup.py --mode audio --preset beat-cutup --input ./loops --bpm 120 --slice-grid 1/16 --stutter-rate 0.55 --repeat-rate 0.45 --mute-rate 0.2 --beat-dropout-rate 0.15 --output out/beat_demo
```

Use a dedicated beat loop as a timing bed when the cutup source material should ride on a baseline groove instead of becoming the groove:

```bash
python3 PY/cutup.py --mode audio --preset beat-cutup --input ./voice_or_noise --baseline-beat ./beats/drum_loop.wav --baseline-beat-bars 4 --baseline-beat-gain -10 --baseline-beat-duck-db 4 --baseline-placement gap --slice-grid 1/16 --stutter-rate 0.45 --repeat-rate 0.3 --output out/beat_baseline
```

`--baseline-beat` is looped under the master and exported as `stems/baseline_beat.wav`; it is excluded from the random source pool even when the file also lives inside `--input`. If `--baseline-beat-bars` is set and `--bpm` is omitted, cutups infers the grid BPM from the beat-loop duration assuming 4/4 bars. Use `--baseline-beat-duck-db` and `--baseline-beat-duck-ms` to attenuate the beat around cutup events so spoken/noise fragments cut through the groove. Use `--baseline-placement any|accent|gap|offbeat` to bias event starts toward loud beat cells, quieter cells, or quiet cells next to accents. Set `--bpm` manually when the loop has pickup silence, odd meter, or a non-looping arrangement.

Write a lightweight JSON source analysis cache for later inspection or future beat-similarity work:

```bash
python3 PY/cutup.py --mode audio --preset beat-cutup --input ./loops --bpm 120 --slice-grid 1/16 --beat-jump-mode similarity --beat-similarity-weight 0.7 --beat-novelty 0.35 --analysis-cache auto --output out/beat_demo
```

Spoken-word cutups can bias toward intelligibility or rupture:

```bash
python3 PY/cutup.py --mode audio --preset spoken-word-cutup --input ./voice.wav --phrase-length medium --intelligibility high --interruption-density low --silence-insert-ms 120:420 --output out/spoken_demo
```

Use `--source-diversity 0.0..1.0` to penalize immediate and repeated source reuse. TRANSMISSIONS presets set conservative defaults, and higher values are useful when a larger dataset is collapsing onto one dominant file.

Use `--source-score off|spoken|beat|breach` to bias source choice toward the material a workflow needs before placement. Presets enable this automatically: spoken-word favors cue/phrase-like sources, beat cutups favor loop/grid material, and breach presets favor noise, radio, static, dropout, or high-intensity sources.

Use `--source-manifest ./sources.csv` or `--source-manifest ./sources.json` to label mixed datasets explicitly. A CSV can include `file`, `role`, `tags`, `intensity`, `loop_hint`, `words`, and `weight`; those labels feed source scoring, plan diagnostics, and the analysis cache.

Use `--scan-dataset ./samples --write-source-manifest ./samples/source_manifest.csv` to create a starter manifest from local audio. The scan uses `pydub` plus filename and lightweight signal descriptors to infer rough `spoken`, `beat`, `breach`, or `texture` roles. Treat the output as editable prep, not ground truth.

Use `--section-arc classic|spoken|breach|pulse|ghost` with `--sectional` to choose the render's energy curve. Presets set workflow-specific arcs, and each `cutup_XX_plan.json` records the selected arc plus per-section target probabilities.

Use `--planner-profile auto|classic|phrase|beat|breach` to bias construction above raw parameter values. `auto` follows the preset/source score. `phrase` protects cued or spoken sources from heavy reversal/granular damage, `beat` favors rhythmic layers and grid diagnostics, and `breach` pushes cuts, filtering, noise, and ghost layers.

Use subtitle or cue files to cut on phrase boundaries:

```bash
python3 PY/cutup.py --mode audio --preset spoken-word-cutup --input ./voice.wav --cue-file ./voice.srt --cue-slice-mode full --output out/spoken_cued
```

Signal-breach renders can push explicit transmission damage:

```bash
python3 PY/cutup.py --mode audio --preset signal-breach --input ./samples --burst-rate 0.7 --dropout-rate 0.6 --reverse-shard-rate 0.45 --filter-severity hard --output out/breach_demo
```

---

## Command-line examples (broad coverage)

### 1) Minimal text generation

```bash
python PY/cutup.py \
  --mode agitprop \
  --output out/agit_min
```

### 2) High-absurd institutional text profile

```bash
python PY/cutup.py \
  --mode agitprop \
  --output out/agit_absurd \
  --seed 31 \
  --absurd-seriousness 0.95 \
  --agitprop-personality "DECREE,PUBLIC INTEREST FEVER" \
  --agitprop-count 80 \
  --broadcast-count 30 \
  --chant-count 220
```

### 3) Personality sweep (all available personalities)

```bash
python PY/cutup.py \
  --mode agitprop \
  --output out/agit_all_personas \
  --agitprop-personality all \
  --seed 77
```

### 4) Text with custom CSV inputs

```bash
python PY/cutup.py \
  --mode agitprop \
  --top300-csv PY/transmissions_top300_sample_candidates.csv \
  --full-csv PY/transmissions_full_subtitles.csv \
  --output out/agit_custom_csv
```

### 5) Generate cut-target matches from existing chant cells

```bash
python PY/cutup.py \
  --mode cuttargets \
  --top300-csv PY/transmissions_top300_sample_candidates.csv \
  --full-csv PY/transmissions_full_subtitles.csv \
  --chant-cells-csv out/agit_absurd/agitprop/chant_cells.csv \
  --cut-match-count 6 \
  --output out/cuttargets_only
```

### 6) Audio: dense swarm, sectional behavior, concrete transformations

```bash
python PY/cutup.py \
  --mode audio \
  --input ./samples \
  --output out/audio_dense \
  --duration 150 \
  --variants 2 \
  --density dense \
  --sectional \
  --arrangement-style swarm \
  --concrete \
  --bed-noise
```

### 7) Audio: collapse-forward arrangement with stronger recurrence

```bash
python PY/cutup.py \
  --mode audio \
  --input ./samples \
  --output out/audio_collapse \
  --duration 120 \
  --arrangement-style collapse \
  --sectional \
  --memory-depth 24 \
  --recurrence-prob 0.55 \
  --ghost-prob 0.48 \
  --silence-prob 0.28 \
  --min-frag 0.03 \
  --max-frag 2.8
```

### 8) Audio: sparse long-form with explicit gain/rate controls

```bash
python PY/cutup.py \
  --mode audio \
  --input ./samples \
  --output out/audio_sparse \
  --duration 240 \
  --density sparse \
  --sample-rate 48000 \
  --master-gain -4.5 \
  --variants 1
```

### 9) Combined text + audio in one run

```bash
python PY/cutup.py \
  --mode both \
  --input ./samples \
  --top300-csv PY/transmissions_top300_sample_candidates.csv \
  --full-csv PY/transmissions_full_subtitles.csv \
  --output out/both_pipeline \
  --sectional \
  --density medium \
  --agitprop-personality "PRESS BRIEFING FROM HELL,GHOST BUREAU" \
  --absurd-seriousness 0.84
```

### 10) Full pipeline (agitprop + cuttargets + audio)

```bash
python PY/cutup.py \
  --mode all \
  --input ./samples \
  --top300-csv PY/transmissions_top300_sample_candidates.csv \
  --full-csv PY/transmissions_full_subtitles.csv \
  --output out/full_run \
  --variants 3 \
  --duration 90 \
  --sectional \
  --export-debug-summary
```

### 11) Live-control MVP via JSON file polling (text + audio)

Create a control file:

```json
{
  "version": 2,
  "controls": {
    "absurd_seriousness": 0.92,
    "text_chaos": 1.1,
    "rupture_prob": 0.7,
    "stutter_prob": 0.55,
    "recurrence_prob": 0.6,
    "ghost_prob": 0.5,
    "silence_prob": 0.3,
    "source_diversity": 0.55,
    "section_arc": "breach",
    "source_score": "breach",
    "baseline_placement": "gap",
    "force_section": "PRESSURE",
    "hold_section": true,
    "burst_now": false,
    "panic_silence": false
  }
}
```

Run with live polling:

```bash
python PY/cutup.py \
  --mode both \
  --input ./samples \
  --output out/live_mvp \
  --sectional \
  --live-control-file ./live_control.json \
  --live-control-poll-ms 120 \
  --live-telemetry-jsonl out/live_mvp/live_telemetry.jsonl
```

Edit `live_control.json` while the run is active. The engine re-reads values at runtime and applies safe clamping.

### 12) Real-time GUI controller (slider interface)

Start GUI (writes a live control JSON file):

```bash
python PY/live_control_gui.py --control-file ./live_control.json
```

Then run the engine using the same file:

```bash
python PY/cutup.py \
  --mode both \
  --input ./samples \
  --output out/live_gui \
  --sectional \
  --live-control-file ./live_control.json \
  --live-control-poll-ms 120
```

The GUI provides:

- continuous slider control for all current live-override keys
- conductor controls: force section, hold section, burst-now, panic-silence
- one-click presets (`Default`, `Bureaucratic Pressure`, `Ghost Broadcast`, `Collapse Ritual`)
- file/folder input, output, and baseline selectors with Start/Stop render buttons for local audio renders
- semi-live renders that write short chunks into a cumulative playable WAV track while the job is still running
- launch validation, command copying, output/log open buttons, and progress/ETA from the matching telemetry JSONL file
- immediate JSON writes compatible with `cutup.py` live polling

---

## Output structure (typical)

Inside your `--output` directory you will usually see:

- `agitprop/slogans.txt`
- `agitprop/broadcasts.txt`
- `agitprop/chant_cells.csv`
- `cuttargets/cut_targets.csv` (or equivalent cuttarget output)
- `audio_cutups/cutup_XX/cutup_XX_master.wav`
- `audio_cutups/cutup_XX/cutup_XX_preview.wav` (when `--preview-duration` is set)
- `audio_cutups/cutup_XX/cutup_XX_events.csv`
- `audio_cutups/cutup_XX/cutup_XX_plan.json`
- `audio_cutups/cutup_XX/cutup_XX_score.txt`
- `audio_cutups/cutup_XX/cutup_XX_live_track.wav` and `audio_cutups/cutup_XX/chunks/` (when `--semi-live` is set)
- `audio_cutups/cutup_XX/cutup_XX_semi_live.json` (when `--semi-live` is set)
- `audio_analysis_cache.json` (when `--analysis-cache auto` is set)
- `run_summary.txt` (when `--export-debug-summary` is enabled)

When `--cue-file` is used, `cutup_XX_events.csv` includes `source_cue_start_ms`, `source_cue_end_ms`, and `source_cue_text`. The event CSV also includes flat planner diagnostics such as `selection_reason`, `source_final_weight`, `planner_profile`, `phrase_protected`, `beat_grid_cell_index`, and section target values for quick spreadsheet review.

`cutup_XX_plan.json` records the rendered composition plan: section windows, event ordering, source/layer/transform summaries, cue counts, phrase-protected counts, grid diagnostics, and the render settings used for the variant. Each event includes a nested `planner` block explaining the selection reason, source weight components, diversity penalties, construction profile/intent, beat-grid placement, baseline placement, and section targets. This is the inspection surface for tuning smarter cutup construction.

If the requested output folder already exists and is non-empty, `cutups` writes to a numbered sibling such as `out/demo_audio_02`. Use `--overwrite` only when you intentionally want to render into an existing folder.

For explicit cache paths, `--analysis-cache ./some/path.json` refuses to overwrite an existing file unless `--overwrite` is set. When overwrite is allowed, matching source/cue entries are reused and only stale or new entries are refreshed. The cache currently stores native lightweight descriptors from `pydub`, including RMS/loudness, zero-crossing rate, capped grid-cell summaries when beat-grid mode is active, and a compact similarity vector for beat planning. `--beat-jump-mode similarity` uses nearest-neighbor jump suggestions from the cache when available; `--beat-similarity-weight` blends those jumps with weighted random source selection, while `--beat-novelty` biases similarity jumps toward farther neighbors. Optional `librosa`/`scikit-learn` analysis is still separate.

---

## Reproducibility tips

- Use `--seed` for deterministic runs.
- Keep `--top300-csv` and `--full-csv` fixed if comparing text changes.
- For controlled audio experiments, fix: `--duration`, `--density`, `--sectional`, and recurrence settings (`--memory-depth`, `--recurrence-prob`, `--ghost-prob`).

---

## Live control MVP (runtime overrides)

Live control is file-based in this MVP and is intended as a simple bridge toward OSC/MIDI/WebSocket control.

CLI flags:

- `--live-control-file <path>`: JSON file to poll for overrides.
- `--live-control-poll-ms <ms>`: poll interval (minimum `30`).
- `--live-telemetry-jsonl <path>`: append runtime state snapshots/events as JSONL.
- `--no-progress`: disable the terminal progress bar. By default, interactive terminal runs show stage, percent, and ETA on stderr.
- `--semi-live`: render audio variants as short chunks and rewrite a cumulative playable WAV track after each chunk.
- `--semi-live-chunk-sec <seconds>`: chunk length for `--semi-live` renders. The minimum is `1`.
- `--semi-live-track <path.wav>`: optional path for the updating WAV track. By default it is written inside each variant folder.

Supported live keys in the JSON file:

- `absurd_seriousness` (`0.0..1.0`)
- `text_chaos` (`0.0..1.5`)
- `rupture_prob` (`0.0..1.0`)
- `stutter_prob` (`0.0..1.0`)
- `recurrence_prob` (`0.0..0.95`)
- `ghost_prob` (`0.0..0.95`)
- `silence_prob` (`0.0..0.95`)
- `burst_rate` (`0.0..1.0`)
- `dropout_rate` (`0.0..1.0`)
- `reverse_shard_rate` (`0.0..1.0`)
- `stutter_rate` (`0.0..1.0`)
- `mute_rate` (`0.0..1.0`)
- `repeat_rate` (`0.0..1.0`)
- `beat_dropout_rate` (`0.0..1.0`)
- `source_diversity` (`0.0..1.0`)
- `filter_severity` (`auto | light | medium | hard`)
- `section_arc` (`classic | spoken | breach | pulse | ghost`)
- `source_score` (`off | spoken | beat | breach`)
- `baseline_placement` (`any | accent | gap | offbeat`)
- `force_section` (`"" | ENTRY | BUILD | PRESSURE | COLLAPSE | AFTERIMAGE`)
- `hold_section` (`true|false`)
- `burst_now` (`true|false`)
- `panic_silence` (`true|false`)

Notes:

- Invalid JSON or missing files are ignored (engine continues with current values).
- Overrides are clamped to safe ranges.
- Telemetry can be tailed live with: `tail -f out/live_mvp/live_telemetry.jsonl`.

### GUI helper

- Script: `PY/live_control_gui.py`
- Uses Python stdlib `tkinter` (no extra package installs needed on most desktop Python setups).
- Writes the same JSON schema accepted by `--live-control-file`.
- Also reads progress telemetry when `cutup.py` is run with the matching `--live-telemetry-jsonl` path.
- Enables semi-live track rendering by default for GUI-launched renders. The growing track path appears in the progress panel as chunks finish; use `Open track` to audition the latest written WAV.
- Checks common launch mistakes before starting a render, including missing input paths, grid slicing without BPM, invalid duration, invalid chunk size, and missing baseline beat files.

```bash
python PY/live_control_gui.py \
  --control-file ./live_control.json \
  --telemetry-file ./live_control_telemetry.jsonl
```

Run `cutup.py` with both paths to see GUI progress and ETA:

```bash
python PY/cutup.py \
  --mode audio \
  --input ./samples \
  --output out/live_progress \
  --live-control-file ./live_control.json \
  --live-telemetry-jsonl ./live_control_telemetry.jsonl
```

### Live telemetry monitor

Use the monitor to watch realtime section/event/control activity from `--live-telemetry-jsonl`:

```bash
python PY/live_control_monitor.py \
  --telemetry out/live_mvp/live_telemetry.jsonl \
  --refresh-ms 750 \
  --tail 30
```

It prints rolling counters (`where`, `section`), latest override values, and recent events.

### TouchDesigner GUI bridge

You can also drive the live-control system from TouchDesigner using UDP JSON.

1) Start the bridge:

```bash
python PY/live_control_td_bridge.py \
  --host 127.0.0.1 \
  --port 9988 \
  --control-file ./live_control.json \
  --verbose
```

2) Run `cutup.py` using the same control file:

```bash
python PY/cutup.py \
  --mode both \
  --input ./samples \
  --output out/live_td \
  --sectional \
  --live-control-file ./live_control.json \
  --live-control-poll-ms 120
```

3) In TouchDesigner, send UDP packets containing JSON objects (from UDP Out DAT/CHOP), for example:

```json
{"version":2,"controls":{"absurd_seriousness":0.9,"recurrence_prob":0.62,"ghost_prob":0.5,"burst_rate":0.8,"dropout_rate":0.6,"stutter_rate":0.7,"repeat_rate":0.5,"source_diversity":0.5,"filter_severity":"hard","section_arc":"breach","source_score":"breach","baseline_placement":"gap","force_section":"COLLAPSE","burst_now":true}}
```

Notes:

- The bridge clamps values to the same ranges as `cutup.py` live control.
- Partial updates are merged, so you can send only changed keys each frame.
- Supported keys are identical to the file-based live-control schema.
- Versioned payload format is recommended: `{"version":2,"controls":{...}}` (legacy flat payloads are still accepted).

---

## Troubleshooting

Run the environment check first:

```bash
python3 PY/cutup.py --doctor
```

### `ModuleNotFoundError: No module named 'pydub'`

Install:

```bash
python3 -m pip install -r requirements.txt
```

### Audio decoding/export issues

Ensure `ffmpeg` is installed and discoverable from your shell PATH.
