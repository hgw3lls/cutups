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

## Troubleshooting

### `ModuleNotFoundError: No module named 'pydub'`

Install:

```bash
python -m pip install pydub
```

### Audio decoding/export issues

Ensure `ffmpeg` is installed and discoverable from your shell PATH.

