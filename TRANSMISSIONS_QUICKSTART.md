# TRANSMISSIONS Quickstart

`cutups` is meant to be run from the repo root with Python 3:

```bash
python3 -m pip install -r requirements.txt
ffmpeg -version
```

If `ffmpeg` is missing, install it with your system package manager before using audio modes.

## Presets

List the current TRANSMISSIONS presets:

```bash
python3 PY/cutup.py --list-presets
```

Presets are starting points. Any explicit CLI flag overrides the preset value.

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

Beat controls only act when the grid is active:

- `--stutter-rate 0..1`: retriggers tiny pieces inside grid cells.
- `--repeat-rate 0..1`: repeats grid cells to create fills and skips.
- `--mute-rate 0..1`: replaces individual grid cells with silence.
- `--beat-dropout-rate 0..1`: cuts longer grid-aligned holes.

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
