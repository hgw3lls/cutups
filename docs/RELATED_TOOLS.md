# Related Tools Research Notes

These notes track outside projects that can inform future TRANSMISSIONS features without becoming core dependencies. Do not vendor or copy code from these projects unless license compatibility and attribution requirements are reviewed again at implementation time.

## Summary Recommendation

Use these projects as references, not embedded dependencies.

- Build small native features in `cutups` first.
- Keep the current `pydub` + `ffmpeg` runtime lightweight.
- Put heavier analysis work behind explicit optional dependencies.
- Treat license and maintenance status as implementation blockers before copying code.

## Decision Matrix

| Project | Best Use | Fit | License / Risk | Recommendation |
| --- | --- | --- | --- | --- |
| [Remixatron](https://github.com/drensin/Remixatron) | Beat similarity, cluster-based jumps, infinite-jukebox path planning | Strong fit for beat cutups | Apache-2.0; Python CLI is marked legacy and current work is in the Rust app | Study algorithms; do not vendor legacy Python code |
| [Infinite Remixer](https://github.com/musikalkemist/infiniteremixer) | Multi-song remixing with feature extraction and nearest-neighbor matching | Useful for future analysis mode | MIT; depends on `librosa` and `scikit-learn`; small project history | Reference only; borrow concepts, not code |
| [AudioGuide](https://github.com/benhackbarth/audioguide) | Concatenative synthesis, corpus/target matching, descriptor-driven selection | Strong conceptual fit for advanced corpus work | Large ecosystem with Csound/DAW/Max outputs; license needs explicit verification before code reuse | Keep external; use as design inspiration |

## Ideas Worth Borrowing

### Remixatron

- Beat-level segmentation with a reusable analysis cache.
- Similarity clusters for musically coherent non-linear jumps.
- Jump constraints that avoid immediate repetition.
- A future `--beat-jump-mode similarity` path that still falls back to current random grid slicing.

### Infinite Remixer

- Optional `librosa` feature extraction for beat slices.
- Nearest-neighbor selection across a folder of loops.
- Multi-source beat path planning.
- A separate `requirements-analysis.txt` so users opt in to heavy dependencies.

### AudioGuide

- Corpus and target descriptor separation.
- Descriptor cache files that can be inspected or reused.
- Hierarchical matching passes.
- Richer export formats for external tools, especially JSON and DAW-friendly cue/region data.

## Proposed Future Work

1. Keep optional analysis dependency checks visible in `--doctor` without making them required for normal rendering.
2. Expand native lightweight beat descriptors:
   - spectral centroid if `librosa` is installed
   - onset/novelty hints
3. Wire beat jump planning into source selection:
   - `--beat-jump-mode random|similarity`
   - `--beat-similarity-weight`
   - `--beat-novelty`
   - `--analysis-cache`
4. Add corpus/target matching for spoken-word and signal-breach workflows after beat similarity is stable.

## Non-goals

- Do not replace `cutup.py` with a full MIR framework.
- Do not require `librosa`, `scikit-learn`, Csound, Max, or DAW tools for normal operation.
- Do not fork or vendor third-party code without a concrete implementation need.
- Do not copy AudioGuide code unless its license status is confirmed and compatible.
