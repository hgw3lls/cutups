# Manual QA Recipes

Use these recipes to audition TRANSMISSIONS workflows after CLI or audio-engine changes. They are meant for human listening, not automated tests.

Keep source audio outside the repo, and write renders to `out/`, which is ignored by git. To create starter WAV and cue sources for every recipe:

```bash
python3 PY/cutup.py --init-qa-sources ../cutups_qa_sources
```

The generated files are synthetic placeholders. Replace or supplement them with short loop files in `../cutups_qa_sources/loops`, spoken recordings in `../cutups_qa_sources/voice`, and noisy/radio/mixed sources in `../cutups_qa_sources/signal` for real production checks. The voice folder also includes `voice_phrase_a.srt` and `voice_cues.csv` for phrase-boundary tests.

## Preflight

```bash
python3 PY/cutup.py --doctor
python3 PY/cutup.py --list-presets
python3 PY/cutup.py --show-recipe all
python3 PY/cutup.py --help
python3 PY/cutup.py --init-qa-sources ../cutups_qa_sources --overwrite
```

Expected:

- `--doctor` reports `status: ready`.
- `--list-presets` includes `signal-breach`, `spoken-word-cutup`, `beat-cutup`, `radio-intrusion`, `hard-stutter`, and `ghost-transmission`.
- `--show-recipe all` prints copy-ready QA and production commands.
- `--help` includes beat controls such as `--beat-jump-mode`, `--beat-similarity-weight`, and `--beat-novelty`.
- `--init-qa-sources` writes `loops`, `voice`, and `signal` WAV folders plus voice cue files.

## Beat Similarity And Novelty

Use at least four loop sources with the same BPM. The source folder can include contrasting drums, bass loops, texture loops, or rendered stems. Keep `--seed`, `--bpm`, and `--slice-grid` fixed when comparing novelty values.

Near-neighbor similarity:

```bash
python3 PY/cutup.py \
  --mode audio \
  --preset beat-cutup \
  --input ../cutups_qa_sources/loops \
  --bpm 120 \
  --slice-grid 1/16 \
  --beat-jump-mode similarity \
  --beat-similarity-weight 1.0 \
  --beat-novelty 0.0 \
  --analysis-cache auto \
  --output out/manual_qa/beat_near \
  --duration 32 \
  --preview-duration 12 \
  --seed 230
```

Balanced novelty:

```bash
python3 PY/cutup.py \
  --mode audio \
  --preset beat-cutup \
  --input ../cutups_qa_sources/loops \
  --bpm 120 \
  --slice-grid 1/16 \
  --beat-jump-mode similarity \
  --beat-similarity-weight 1.0 \
  --beat-novelty 0.35 \
  --analysis-cache auto \
  --output out/manual_qa/beat_balanced \
  --duration 32 \
  --preview-duration 12 \
  --seed 230
```

Disruptive novelty:

```bash
python3 PY/cutup.py \
  --mode audio \
  --preset beat-cutup \
  --input ../cutups_qa_sources/loops \
  --bpm 120 \
  --slice-grid 1/16 \
  --beat-jump-mode similarity \
  --beat-similarity-weight 1.0 \
  --beat-novelty 0.85 \
  --analysis-cache auto \
  --output out/manual_qa/beat_disruptive \
  --duration 32 \
  --preview-duration 12 \
  --seed 230
```

Check these files after each render:

- `out/manual_qa/beat_*/audio_analysis_cache.json`
- `out/manual_qa/beat_*/audio_cutups/cutup_01/cutup_01_master.wav`
- `out/manual_qa/beat_*/audio_cutups/cutup_01/cutup_01_preview.wav`
- `out/manual_qa/beat_*/audio_cutups/cutup_01/cutup_01_events.csv`
- `out/manual_qa/beat_*/audio_cutups/cutup_01/cutup_01_plan.json`

Listening notes:

- `beat_near` should jump between sources with similar loudness/texture more often.
- `beat_balanced` should keep musical continuity while allowing more surprising source changes.
- `beat_disruptive` should produce more obvious source contrast and interruption.
- Grid timing should still feel locked to the supplied BPM.
- `cutup_01_events.csv` should show source changes and grid-aligned start positions.
- `cutup_01_plan.json` should summarize section counts, top sources, transform tags, and grid settings.

## Beat Damage

Use this to check stutters, repeats, mutes, and longer beat dropouts:

```bash
python3 PY/cutup.py \
  --mode audio \
  --preset hard-stutter \
  --input ../cutups_qa_sources/loops \
  --bpm 120 \
  --slice-grid 1/32 \
  --stutter-rate 0.85 \
  --repeat-rate 0.65 \
  --mute-rate 0.30 \
  --beat-dropout-rate 0.25 \
  --output out/manual_qa/beat_damage \
  --duration 24 \
  --preview-duration 10 \
  --seed 241
```

Listening notes:

- Stutters should feel like retriggered grid cells, not random clicks.
- Mutes and beat dropouts should create intentional holes without changing the total render length.
- Repeats should read as rhythmic fills or skips.

## Signal Breach

```bash
python3 PY/cutup.py \
  --mode audio \
  --preset signal-breach \
  --input ../cutups_qa_sources/signal \
  --burst-rate 0.75 \
  --dropout-rate 0.65 \
  --reverse-shard-rate 0.50 \
  --filter-severity hard \
  --output out/manual_qa/signal_breach \
  --duration 30 \
  --preview-duration 10 \
  --seed 701
```

Listening notes:

- Noise bursts should interrupt rather than fully mask the whole piece.
- Dropouts should read as broken transmission or dead air.
- Reverse shards should add corrupted edges without destroying every source phrase.
- Hard filtering should create a narrower radio/transmission band.

## Spoken Word

High-intelligibility pass:

```bash
python3 PY/cutup.py \
  --mode audio \
  --preset spoken-word-cutup \
  --input ../cutups_qa_sources/voice \
  --phrase-length medium \
  --intelligibility high \
  --interruption-density low \
  --silence-insert-ms 120:420 \
  --output out/manual_qa/spoken_clear \
  --duration 45 \
  --preview-duration 12 \
  --seed 311
```

Fragmented pass:

```bash
python3 PY/cutup.py \
  --mode audio \
  --preset spoken-word-cutup \
  --input ../cutups_qa_sources/voice \
  --phrase-length short \
  --intelligibility low \
  --interruption-density high \
  --silence-insert-ms 40:180 \
  --output out/manual_qa/spoken_fragmented \
  --duration 45 \
  --preview-duration 12 \
  --seed 311
```

Cue-boundary SRT pass:

```bash
python3 PY/cutup.py \
  --mode audio \
  --preset spoken-word-cutup \
  --input ../cutups_qa_sources/voice/voice_phrase_a.wav \
  --cue-file ../cutups_qa_sources/voice/voice_phrase_a.srt \
  --cue-slice-mode full \
  --intelligibility high \
  --output out/manual_qa/spoken_srt_cues \
  --duration 30 \
  --preview-duration 10 \
  --seed 313
```

Multi-file cue CSV pass:

```bash
python3 PY/cutup.py \
  --mode audio \
  --preset spoken-word-cutup \
  --input ../cutups_qa_sources/voice \
  --cue-file ../cutups_qa_sources/voice/voice_cues.csv \
  --cue-slice-mode fragment \
  --intelligibility medium \
  --output out/manual_qa/spoken_csv_cues \
  --duration 30 \
  --preview-duration 10 \
  --seed 314
```

Listening notes:

- `spoken_clear` should preserve phrase intelligibility and leave usable gaps.
- `spoken_fragmented` should rupture syntax while keeping recognizably vocal material.
- Cue passes should keep event sources inside the cue spans recorded in `cutup_01_events.csv`.
- Silence insertion should feel like editorial pacing or transmission loss, not accidental render failure.

## Failure Triage

- If audio import/export fails, run `python3 PY/cutup.py --doctor` and fix `ffmpeg` first.
- If a source folder is missing or empty, the command should fail clearly before rendering.
- If an output folder already contains files, `cutups` should choose a numbered sibling unless `--overwrite` is passed.
- Confirm each audio variant writes `cutup_01_plan.json`; if `--analysis-cache auto` is used, confirm the cache is under the output folder and includes `version`, `samples`, `grid_ms`, and `beat_jump_plan`.
