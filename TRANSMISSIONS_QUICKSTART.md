# TRANSMISSIONS Quickstart

`cutups` is meant to be run from the repo root with Python 3:

```bash
python3 -m pip install -e .
ffmpeg -version
cutups --doctor
```

If `ffmpeg` is missing, install it with your system package manager before using audio modes. `--doctor` reports Python, `pydub`, `ffmpeg`/`avconv`, bundled CSVs, and preset availability. The direct script form, `python3 PY/cutup.py`, still works if you do not install the package.

Create starter local WAV and cue sources for the listening recipes:

```bash
cutups --init-qa-sources ../cutups_qa_sources
```

This writes `loops`, `voice`, and `signal` folders. The voice folder includes `voice_phrase_a.srt` and `voice_cues.csv` for cue-slicing tests.

## Presets

List the current TRANSMISSIONS presets:

```bash
cutups --list-presets
```

Presets are starting points. Any explicit CLI flag overrides the preset value.

Print a copy-ready recipe from the CLI:

```bash
cutups --show-recipe signal-breach
```

Use `--show-recipe all` to print every built-in recipe.

## Signal Breach

Use speech, room recordings, radio grabs, or mixed source folders:

```bash
python3 PY/cutup.py \
  --mode audio \
  --preset signal-breach \
  --input ./samples \
  --burst-rate 0.7 \
  --dropout-rate 0.6 \
  --reverse-shard-rate 0.45 \
  --filter-severity hard \
  --output out/signal_breach \
  --duration 60 \
  --seed 7
```

Add a short audition file beside the full master:

```bash
python3 PY/cutup.py \
  --mode audio \
  --preset signal-breach \
  --input ./samples \
  --output out/signal_breach_preview \
  --duration 60 \
  --preview-duration 12 \
  --seed 7
```

Signal-breach controls:

- `--burst-rate 0..1`: inserts static/noise bursts into fragments.
- `--dropout-rate 0..1`: cuts hard dead-air holes into fragments.
- `--reverse-shard-rate 0..1`: reverses tiny shards inside fragments.
- `--filter-severity light|medium|hard|auto`: narrows or loosens the transmission-band filtering.

The live-control file, Tk GUI, and TouchDesigner bridge can also drive `burst_rate`, `dropout_rate`, `reverse_shard_rate`, and `filter_severity` during audio renders.

For a short setup check without rendering:

```bash
python3 PY/cutup.py \
  --mode audio \
  --preset signal-breach \
  --input ./samples \
  --output out/signal_breach \
  --dry-run
```

## Spoken-Word Cutup

Use a folder of voice recordings, or pass a single file directly:

```bash
python3 PY/cutup.py \
  --mode audio \
  --preset spoken-word-cutup \
  --input ./voice/interview.wav \
  --phrase-length medium \
  --intelligibility high \
  --interruption-density low \
  --silence-insert-ms 120:420 \
  --output out/spoken_word \
  --duration 90 \
  --seed 11
```

For more fractured speech, lower intelligibility and increase interruptions:

```bash
python3 PY/cutup.py \
  --mode audio \
  --preset spoken-word-cutup \
  --input ./voice \
  --phrase-length short \
  --intelligibility low \
  --interruption-density high \
  --silence-insert-ms 40:180 \
  --output out/spoken_fragments \
  --duration 60 \
  --seed 12
```

To preserve phrase boundaries from subtitles or prepared cue sheets, pass an `.srt` or cue CSV:

```bash
python3 PY/cutup.py \
  --mode audio \
  --preset spoken-word-cutup \
  --input ./voice/interview.wav \
  --cue-file ./voice/interview.srt \
  --cue-slice-mode full \
  --intelligibility high \
  --output out/spoken_cued \
  --duration 90 \
  --seed 13
```

Use `--cue-slice-mode fragment` when you want random sub-fragments inside each cue instead of whole subtitle spans. Cue CSVs can use columns such as `file`, `start_tc`, `end_tc`, `duration_sec`, and `text`; SRT files use the single `--input` audio file.

After creating QA sources, this command exercises the generated SRT cues:

```bash
python3 PY/cutup.py \
  --mode audio \
  --preset spoken-word-cutup \
  --input ../cutups_qa_sources/voice/voice_phrase_a.wav \
  --cue-file ../cutups_qa_sources/voice/voice_phrase_a.srt \
  --cue-slice-mode full \
  --output out/spoken_qa_cued \
  --duration 30 \
  --preview-duration 10 \
  --seed 13
```

Phrase-length values: `micro`, `short`, `medium`, `long`, `auto`.
Intelligibility values: `high`, `medium`, `low`, `auto`.
Interruption-density values: `low`, `medium`, `high`, `auto`.

For generated TRANSMISSIONS text:

```bash
python3 PY/cutup.py \
  --mode agitprop \
  --preset spoken-word-cutup \
  --output out/spoken_text \
  --seed 11
```

## Beat Cutup

Use known-tempo loops or rhythmic material. This first pass does not do BPM detection; pass the tempo manually and choose a slice grid.

```bash
python3 PY/cutup.py \
  --mode audio \
  --preset beat-cutup \
  --input ./loops \
  --bpm 120 \
  --slice-grid 1/16 \
  --stutter-rate 0.55 \
  --repeat-rate 0.45 \
  --mute-rate 0.20 \
  --beat-dropout-rate 0.15 \
  --output out/beat_cutup \
  --duration 45 \
  --seed 23
```

Useful grid values: `1/4`, `1/8`, `1/16`, `1/32`, `1/8t`, `1/16t`. Grid mode quantizes source slice lengths and event starts, while repeats, ghosts, reversals, and gaps can still disrupt the loop.

Add `--analysis-cache auto` to write `audio_analysis_cache.json` under the output folder. It captures source/cue identity, duration, RMS/loudness, zero-crossing rate, channels, sample rate, beat-grid context, capped grid-cell summaries, and normalized similarity vectors for inspection and future similarity planning.

Add `--beat-jump-mode similarity` with `--analysis-cache auto` to write nearest-neighbor beat jump suggestions into the cache and use them for source-to-source jumps when possible. Use `--beat-similarity-weight 0.0..1.0` to blend similarity jumps with weighted random selection; lower values keep more of the older behavior. Use `--beat-novelty 0.0..1.0` to bias similarity jumps toward farther, more disruptive neighbors.

Beat controls only act when the grid is active:

- `--stutter-rate 0..1`: retriggers tiny pieces inside grid cells.
- `--repeat-rate 0..1`: repeats grid cells to create fills and skips.
- `--mute-rate 0..1`: replaces individual grid cells with silence.
- `--beat-dropout-rate 0..1`: cuts longer grid-aligned holes.

## Source Diversity

Use `--source-diversity 0.0..1.0` when a render overuses one file from a larger dataset. TRANSMISSIONS presets set conservative defaults; higher values push non-memory source choices toward less-used and less-recent files while still allowing intentional recurrence.

## Source Scoring

Use `--source-score off|spoken|beat|breach` to bias which source is selected before slicing and placement. This uses local metadata only: duration, cue word counts, cue text, file names, intensity hints, loop hints, and the current section. Presets enable the appropriate mode automatically, and `--source-score off` restores unscored source weighting.

## Section Arcs

Use `--section-arc classic|spoken|breach|pulse|ghost` with `--sectional` to choose the composition curve across ENTRY, BUILD, PRESSURE, COLLAPSE, and AFTERIMAGE. Presets choose an arc automatically; override it when you want a spoken-word render to breach harder, or a signal render to hold back.

## Radio Intrusion

```bash
python3 PY/cutup.py \
  --mode audio \
  --preset radio-intrusion \
  --input ./voice \
  --output out/radio_intrusion \
  --duration 75 \
  --seed 31
```

## Hard Stutter

```bash
python3 PY/cutup.py \
  --mode audio \
  --preset hard-stutter \
  --input ./loops \
  --bpm 120 \
  --slice-grid 1/32 \
  --stutter-rate 0.85 \
  --repeat-rate 0.65 \
  --mute-rate 0.30 \
  --output out/hard_stutter \
  --duration 30 \
  --seed 41
```

## Ghost Transmission

```bash
python3 PY/cutup.py \
  --mode audio \
  --preset ghost-transmission \
  --input ./voice \
  --output out/ghost_transmission \
  --duration 120 \
  --seed 53
```

## Output Safety

By default, if the requested output folder already exists and contains files, `cutups` writes to a numbered sibling such as `out/signal_breach_02`. Use `--overwrite` only when you intentionally want to render into an existing non-empty folder.

Use `--preview-duration <seconds>` to write a short `cutup_XX_preview.wav` beside each full master for fast auditioning.

Each audio variant also writes `cutup_XX_plan.json`, a structured render plan with section windows, event choices, source/layer summaries, transform tags, and per-event `planner` diagnostics. Use it when comparing whether a cutup is being constructed intelligently, not just whether it sounds good. The matching `cutup_XX_events.csv` includes flat diagnostic columns for spreadsheet review.

Use `--analysis-cache auto` to write a versioned JSON source cache beside the render outputs. Explicit cache paths are allowed, and existing files require `--overwrite`; once overwrite is allowed, matching entries are reused instead of decoded again.

For repeatable listening checks after changes, use [docs/MANUAL_QA.md](docs/MANUAL_QA.md).

## Validation

Run the full test suite from the repo root:

```bash
python3 -m pytest
```

For the audio path specifically:

```bash
python3 -m pytest tests/test_audio_smoke.py -q
```

The audio smoke test generates temporary source material, renders through the real CLI, verifies master/preview/event outputs, and checks same-seed determinism. It skips automatically when `pydub` or `ffmpeg` is missing.
