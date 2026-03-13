# cutups

`cutups` is a generative cut-up instrument with three integrated engines:

- **Audio composition** (`--mode audio`) for speech-concrète collage.
- **Agitprop text generation** (`--mode agitprop`) for slogans, broadcasts, and chant cells.
- **Cut-target matching** (`--mode cuttargets`) for phrase targeting workflows.

The main entry point is:

- `PY/cutup.py`

---

## Requirements

- Python 3.10+
- `pydub`
- `ffmpeg` available on PATH (required by `pydub` for many file formats)

Install Python dependency:

```bash
python -m pip install pydub
```

---

## Quick start

From repo root:

```bash
python PY/cutup.py --mode agitprop --output out/demo_text
```

```bash
python PY/cutup.py --mode audio --input ./samples --output out/demo_audio
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
- immediate JSON writes compatible with `cutup.py` live polling

---

## Output structure (typical)

Inside your `--output` directory you will usually see:

- `agitprop/slogans.txt`
- `agitprop/broadcasts.txt`
- `agitprop/chant_cells.csv`
- `cuttargets/cut_targets.csv` (or equivalent cuttarget output)
- `audio_cutups/cutup_XX/cutup_XX_master.wav`
- `audio_cutups/cutup_XX/cutup_XX_events.csv`
- `audio_cutups/cutup_XX/cutup_XX_score.txt`
- `run_summary.txt` (when `--export-debug-summary` is enabled)

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

Supported live keys in the JSON file:

- `absurd_seriousness` (`0.0..1.0`)
- `text_chaos` (`0.0..1.5`)
- `rupture_prob` (`0.0..1.0`)
- `stutter_prob` (`0.0..1.0`)
- `recurrence_prob` (`0.0..0.95`)
- `ghost_prob` (`0.0..0.95`)
- `silence_prob` (`0.0..0.95`)
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
{"version":2,"controls":{"absurd_seriousness":0.9,"recurrence_prob":0.62,"ghost_prob":0.5,"force_section":"COLLAPSE","burst_now":true}}
```

Notes:

- The bridge clamps values to the same ranges as `cutup.py` live control.
- Partial updates are merged, so you can send only changed keys each frame.
- Supported keys are identical to the file-based live-control schema.
- Versioned payload format is recommended: `{"version":2,"controls":{...}}` (legacy flat payloads are still accepted).

---

## Troubleshooting

### `ModuleNotFoundError: No module named 'pydub'`

Install:

```bash
python -m pip install pydub
```

### Audio decoding/export issues

Ensure `ffmpeg` is installed and discoverable from your shell PATH.
