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
  --output out/signal_breach \
  --duration 60 \
  --seed 7
```

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
  --output out/spoken_word \
  --duration 90 \
  --seed 11
```

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
  --output out/beat_cutup \
  --duration 45 \
  --seed 23
```

Useful grid values: `1/4`, `1/8`, `1/16`, `1/32`, `1/8t`, `1/16t`. Grid mode quantizes source slice lengths and event starts, while repeats, ghosts, reversals, and gaps can still disrupt the loop.

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
