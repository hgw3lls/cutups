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

Scan a local source folder before rendering when you are pointing `cutups` at an unfamiliar dataset:

```bash
cutups \
  --scan-dataset ./samples \
  --write-source-manifest ./samples/source_manifest.csv \
  --write-dataset-report ./samples/dataset_report.json
```

The scan writes editable role hints (`spoken`, `beat`, `breach`, or `texture`), tags, weights, and recommended presets. Use the generated CSV with `--source-manifest`; revise it by hand when you know a file's role better than the heuristic.

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

The live-control file, Tk GUI, and TouchDesigner bridge can also drive `burst_rate`, `dropout_rate`, `reverse_shard_rate`, `filter_severity`, `source_diversity`, `section_arc`, `source_score`, and `baseline_placement` during audio renders.

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

Use known-tempo loops or rhythmic material. Pass the tempo manually and choose a slice grid, or use a baseline beat loop with a known bar count to infer the grid BPM.

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

To keep one beat as the baseline groove while cutting other material around it, keep the beat out of `--input` and pass it separately:

```bash
python3 PY/cutup.py \
  --mode audio \
  --preset beat-cutup \
  --input ./voice_or_noise \
  --baseline-beat ./beats/drum_loop.wav \
  --baseline-beat-bars 4 \
  --baseline-beat-gain -10 \
  --baseline-beat-duck-db 4 \
  --baseline-placement gap \
  --slice-grid 1/16 \
  --stutter-rate 0.45 \
  --repeat-rate 0.30 \
  --output out/beat_baseline \
  --duration 45 \
  --seed 29
```

`--baseline-beat` is looped under the master, exported as `stems/baseline_beat.wav`, and excluded from source selection even if it is also inside `--input`. `--baseline-beat-bars` infers BPM only when `--bpm` is omitted; set `--bpm` manually for odd meters, pickups, or non-looping beat files. Add `--baseline-beat-duck-db` and `--baseline-beat-duck-ms` when voice, noise, or stutter fragments need to push through the beat without burying the groove. Use `--baseline-placement gap` for intelligible voice against a beat, `accent` for harder rhythmic hits, or `offbeat` for syncopated interruptions.

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

## Planner Profiles

Use `--planner-profile auto|classic|phrase|beat|breach` to bias construction beyond source scoring:

- `auto`: follows the active preset/source score.
- `phrase`: protects cued or spoken material from heavy reversal, swarm, hard cuts, and unstable speed changes.
- `beat`: favors rhythmic layers, grid-aware steps, and beat-grid diagnostics.
- `breach`: pushes damaged-signal construction with harder cuts, filtering, silence, and ghost/cut layers.

Each `cutup_XX_plan.json` records the selected planner profile, per-section intent, phrase-protected event count, and beat-grid event diagnostics.

## Source Manifest

Use `--source-manifest ./sources.csv` or `--source-manifest ./sources.json` when a mixed dataset needs explicit labels. CSV columns can include `file`, `role`, `tags`, `intensity`, `loop_hint`, `words`, and `weight`.

```csv
file,role,tags,intensity,loop_hint,words,weight
voice/interview_01.wav,spoken,"voice,interview,clear",1,0,12,1.2
loops/drum_loop_120.wav,beat,"drum,loop,pulse",0,3,1,1.4
signal/radio_static.wav,breach,"radio,static,dropout",3,0,1,1.8
```

Manifest labels feed `--source-score`, `cutup_XX_events.csv`, `cutup_XX_plan.json`, and `audio_analysis_cache.json`.

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

Interactive terminal runs show a live progress bar with stage, percent, and ETA on stderr. Add `--no-progress` when writing logs or running in a host that should stay quiet.

For GUI progress, start the live GUI with a telemetry file and run `cutup.py` with the same `--live-telemetry-jsonl` path:

```bash
python PY/live_control_gui.py --control-file ./live_control.json --telemetry-file ./live_control_telemetry.jsonl
python PY/cutup.py --mode audio --input ./samples --output out/live_progress --live-control-file ./live_control.json --live-telemetry-jsonl ./live_control_telemetry.jsonl
```

The GUI can also launch local audio renders directly. Choose input/output paths, preset, duration, optional BPM/grid, and optional baseline beat, then use Start render. It writes the same live-control JSON and telemetry paths shown above, so sliders and progress continue to work during the render.

GUI-launched renders enable semi-live tracks by default: `cutups` renders short chunks, appends them to `cutup_XX_live_track.wav`, and updates the path in the GUI progress panel as the track grows. Use the Chunk sec field to control how often the playable track is refreshed.

CLI equivalent:

```bash
python PY/cutup.py \
  --mode audio \
  --preset signal-breach \
  --input ./samples \
  --output out/semi_live_breach \
  --duration 90 \
  --semi-live \
  --semi-live-chunk-sec 8 \
  --live-control-file ./live_control.json \
  --live-telemetry-jsonl ./live_control_telemetry.jsonl
```

Semi-live renders also write each source chunk to `audio_cutups/cutup_XX/chunks/` and a `cutup_XX_semi_live.json` manifest for later inspection.

## SuperCollider Tape Deck

Open `SC/cutup.scd` in SuperCollider after starting or completing a semi-live render. Point **SOURCE** at the variant folder, for example `out/semi_live_breach/audio_cutups/cutup_01`.

- Use `LOAD LIVE` to load `cutup_XX_live_track.wav`.
- Use `LOAD CHUNKS` to load the semi-live `chunks/` folder.
- Use `LOAD DIR` for any plain folder of `.wav` files.

Once loaded, start individual decks (`CUTS`, `LOOPS`, `GHOST`, `CLOUD`) or use `SCENE AUTO`.

The SC deck keeps the useful parts of the older tape-loop sketches: `STAB`, `PHRS`, and `CNCR` one-shot buttons, tape `AGE` in the master strip, and OSC hooks for external control (`/cutups/stab`, `/cutups/phrase`, `/cutups/concrete`, `/cutups/scene`, `/cutups/stop`, `/cutups/loadLive`, `/cutups/loadChunks`, `/cutups/master/age`).

Each audio variant also writes `cutup_XX_plan.json`, a structured render plan with section windows, event choices, source/layer summaries, transform tags, and per-event `planner` diagnostics. Use it when comparing whether a cutup is being constructed intelligently, not just whether it sounds good. The matching `cutup_XX_events.csv` includes flat diagnostic columns for spreadsheet review.

Use `--analysis-cache auto` to write a versioned JSON source cache beside the render outputs. For large source folders, precompute it first:

```bash
cutups-analyze --input ./samples --output ./samples/audio_analysis_cache.json --overwrite --bpm 120 --slice-grid 1/16
cutups --mode audio --preset beat-cutup --input ./samples --analysis-cache ./samples/audio_analysis_cache.json --overwrite --output out/beat_demo
```

Explicit cache paths are allowed, and existing files require `--overwrite`; once overwrite is allowed, matching entries are reused instead of decoded again. The renderer can also use matching non-cue cache entries during source discovery.

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
