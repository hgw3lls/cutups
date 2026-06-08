import contextlib
import io
import json
import sys
import tempfile
import types
import unittest
import wave
from unittest import mock
from pathlib import Path


class _DummyAudioSegment:
    @classmethod
    def silent(cls, *args, **kwargs):
        return cls()


def _install_pydub_stubs() -> None:
    pydub = types.ModuleType("pydub")
    pydub.AudioSegment = _DummyAudioSegment
    effects = types.ModuleType("pydub.effects")

    def _noop(x, *args, **kwargs):
        return x

    effects.compress_dynamic_range = _noop
    effects.high_pass_filter = _noop
    effects.low_pass_filter = _noop

    sys.modules.setdefault("pydub", pydub)
    sys.modules.setdefault("pydub.effects", effects)


_install_pydub_stubs()

from PY import cutup  # noqa: E402
from PY import live_control_td_bridge as td_bridge  # noqa: E402


class LiveControlTests(unittest.TestCase):
    def test_doctor_format_check(self) -> None:
        self.assertEqual(cutup.format_check(True, "ready"), "ok - ready")
        self.assertEqual(cutup.format_check(False, "missing thing"), "missing - missing thing")

    def test_optional_analysis_checks_report_expected_labels(self) -> None:
        checks = cutup.optional_analysis_checks()
        self.assertEqual([name for name, _, _ in checks], ["librosa", "scikit-learn"])
        self.assertTrue(all(isinstance(ok, bool) for _, ok, _ in checks))
        self.assertTrue(all(detail for _, _, detail in checks))

    def test_write_qa_sources_creates_source_tree_and_refuses_clobber(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "qa_sources"
            written = cutup.write_qa_sources(root, overwrite=False)
            expected_count = sum(len(specs) for specs in cutup.QA_SOURCE_SPECS.values())
            self.assertEqual(len(written), expected_count)
            self.assertEqual({path.parent.name for path in written}, {"loops", "voice", "signal"})
            self.assertTrue((root / "loops" / "drum_pulse_120.wav").exists())

            with wave.open(str(written[0]), "rb") as wav:
                self.assertEqual(wav.getnchannels(), 1)
                self.assertEqual(wav.getsampwidth(), 2)
                self.assertEqual(wav.getframerate(), 44100)
                self.assertGreater(wav.getnframes(), 0)

            with self.assertRaises(SystemExit):
                cutup.write_qa_sources(root, overwrite=False)
            rewritten = cutup.write_qa_sources(root, overwrite=True)
            self.assertEqual(len(rewritten), expected_count)

    def test_runtime_snapshot_uses_defaults_when_disabled(self) -> None:
        args = types.SimpleNamespace(
            absurd_seriousness=0.2,
            text_chaos=0.3,
            rupture_prob=0.4,
            stutter_prob=0.5,
            recurrence_prob=0.6,
            ghost_prob=0.7,
            silence_prob=0.8,
        )
        runtime = cutup.runtime_snapshot(args, live=None)
        self.assertEqual(runtime.absurd_seriousness, 0.2)
        self.assertEqual(runtime.ghost_prob, 0.7)

    def test_apply_runtime_params_updates_signal_damage_controls(self) -> None:
        args = types.SimpleNamespace(
            absurd_seriousness=0.2,
            text_chaos=0.3,
            rupture_prob=0.4,
            stutter_prob=0.5,
            recurrence_prob=0.6,
            ghost_prob=0.7,
            silence_prob=0.8,
            burst_rate=0.0,
            dropout_rate=0.0,
            reverse_shard_rate=0.0,
            filter_severity="auto",
            stutter_rate=0.0,
            mute_rate=0.0,
            repeat_rate=0.0,
            beat_dropout_rate=0.0,
        )
        runtime = cutup.RuntimeParams(
            absurd_seriousness=0.2,
            text_chaos=0.3,
            rupture_prob=0.4,
            stutter_prob=0.5,
            recurrence_prob=0.6,
            ghost_prob=0.7,
            silence_prob=0.8,
            burst_rate=0.9,
            dropout_rate=0.8,
            reverse_shard_rate=0.7,
            filter_severity="hard",
            stutter_rate=0.6,
            mute_rate=0.5,
            repeat_rate=0.4,
            beat_dropout_rate=0.3,
        )
        out = cutup.apply_runtime_params(args, runtime)
        self.assertEqual(out.burst_rate, 0.9)
        self.assertEqual(out.dropout_rate, 0.8)
        self.assertEqual(out.reverse_shard_rate, 0.7)
        self.assertEqual(out.filter_severity, "hard")
        self.assertEqual(out.stutter_rate, 0.6)
        self.assertEqual(out.mute_rate, 0.5)
        self.assertEqual(out.repeat_rate, 0.4)
        self.assertEqual(out.beat_dropout_rate, 0.3)

    def test_apply_preset_keeps_explicit_cli_values(self) -> None:
        args = types.SimpleNamespace(
            preset="signal-breach",
            _explicit_args={"density", "bed_noise"},
            density="sparse",
            concrete=False,
            bed_noise=False,
            min_frag=0.05,
            max_frag=4.2,
        )
        cutup.apply_preset(args)
        self.assertEqual(args.density, "sparse")
        self.assertTrue(args.concrete)
        self.assertFalse(args.bed_noise)
        self.assertEqual(args.min_frag, 0.025)

    def test_beat_cutup_preset_sets_slice_grid(self) -> None:
        args = types.SimpleNamespace(preset="beat-cutup", _explicit_args=set(), slice_grid="off")
        cutup.apply_preset(args)
        self.assertEqual(args.slice_grid, "1/16")
        self.assertEqual(args.stutter_rate, 0.48)
        self.assertEqual(args.mute_rate, 0.18)
        self.assertEqual(args.repeat_rate, 0.38)
        self.assertEqual(args.beat_dropout_rate, 0.16)

    def test_beat_control_rates_clamps_values(self) -> None:
        args = types.SimpleNamespace(stutter_rate=2.0, mute_rate=-1.0, repeat_rate=0.25, beat_dropout_rate=1.2)
        rates = cutup.beat_control_rates(args)
        self.assertEqual(rates["stutter_rate"], 1.0)
        self.assertEqual(rates["mute_rate"], 0.0)
        self.assertEqual(rates["repeat_rate"], 0.25)
        self.assertEqual(rates["beat_dropout_rate"], 1.0)

    def test_spoken_word_preset_sets_voice_controls(self) -> None:
        args = types.SimpleNamespace(
            preset="spoken-word-cutup",
            _explicit_args=set(),
            phrase_length="auto",
            intelligibility="auto",
            interruption_density="auto",
            silence_insert_ms="",
        )
        cutup.apply_preset(args)
        self.assertEqual(args.phrase_length, "medium")
        self.assertEqual(args.intelligibility, "high")
        self.assertEqual(args.interruption_density, "low")
        self.assertEqual(args.silence_insert_ms, "120:420")

    def test_signal_breach_preset_sets_breach_controls(self) -> None:
        args = types.SimpleNamespace(
            preset="signal-breach",
            _explicit_args=set(),
            burst_rate=0.0,
            dropout_rate=0.0,
            reverse_shard_rate=0.0,
            filter_severity="auto",
        )
        cutup.apply_preset(args)
        self.assertEqual(args.burst_rate, 0.58)
        self.assertEqual(args.dropout_rate, 0.64)
        self.assertEqual(args.reverse_shard_rate, 0.46)
        self.assertEqual(args.filter_severity, "hard")

    def test_filter_pair_hard_is_narrower_than_light(self) -> None:
        hard_args = types.SimpleNamespace(filter_severity="hard")
        light_args = types.SimpleNamespace(filter_severity="light")
        for _ in range(20):
            hard_hp, hard_lp = cutup.filter_pair(hard_args)
            light_hp, light_lp = cutup.filter_pair(light_args)
            self.assertGreaterEqual(hard_hp, 420)
            self.assertLessEqual(hard_lp, 3200)
            self.assertLessEqual(light_hp, 260)
            self.assertGreaterEqual(light_lp, 4400)

    def test_apply_phrase_length_respects_explicit_fragment_bounds(self) -> None:
        args = types.SimpleNamespace(
            phrase_length="long",
            _explicit_args={"min_frag"},
            min_frag=0.9,
            max_frag=1.2,
        )
        cutup.apply_phrase_length(args)
        self.assertEqual(args.min_frag, 0.9)
        self.assertEqual(args.max_frag, 6.8)

    def test_parse_silence_insert_ms(self) -> None:
        self.assertEqual(cutup.parse_silence_insert_ms("120:420"), (120, 420))
        self.assertEqual(cutup.parse_silence_insert_ms(""), (0, 0))

    def test_parse_silence_insert_ms_rejects_bad_range(self) -> None:
        with self.assertRaises(SystemExit):
            cutup.parse_silence_insert_ms("420:120")

    def test_parse_timecode_ms_accepts_srt_and_seconds(self) -> None:
        self.assertEqual(cutup.parse_timecode_ms("00:01:02,345"), 62345)
        self.assertEqual(cutup.parse_timecode_ms("01:02.500"), 62500)
        self.assertEqual(cutup.parse_timecode_ms("3.25"), 3250)
        self.assertIsNone(cutup.parse_timecode_ms("not time"))

    def test_parse_srt_cues(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "voice.srt"
            path.write_text(
                "1\n"
                "00:00:00,500 --> 00:00:01,750\n"
                "first phrase\n\n"
                "2\n"
                "00:00:02,000 --> 00:00:03,250\n"
                "second phrase\n",
                encoding="utf-8",
            )
            rows = cutup.parse_srt_cues(path)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["start_ms"], 500)
        self.assertEqual(rows[0]["end_ms"], 1750)
        self.assertEqual(rows[0]["text"], "first phrase")

    def test_workflow_audio_profile_high_intelligibility_reduces_disruption(self) -> None:
        profile = {"reverse": 0.4, "repeat": 0.6, "filt": 0.8, "silence": 0.3, "ghost": 0.2}
        args = types.SimpleNamespace(intelligibility="high", interruption_density="low", concrete=False)
        out = cutup.workflow_audio_profile(profile, args)
        self.assertLess(out["reverse"], profile["reverse"])
        self.assertLess(out["repeat"], profile["repeat"])
        self.assertLess(out["filt"], profile["filt"])
        self.assertLess(out["hard_cut"], 0.14)
        self.assertLess(out["swarm_bias"], 1.0)

    def test_beat_grid_ms_uses_manual_bpm_and_grid(self) -> None:
        args = types.SimpleNamespace(bpm=120.0, slice_grid="1/16")
        self.assertEqual(cutup.beat_grid_ms(args), 125)

    def test_beat_grid_ms_inactive_without_bpm(self) -> None:
        args = types.SimpleNamespace(bpm=0.0, slice_grid="1/16")
        self.assertEqual(cutup.beat_grid_ms(args), 0)

    def test_quantize_to_grid(self) -> None:
        self.assertEqual(cutup.quantize_to_grid(188, 125), 250)
        self.assertEqual(cutup.quantize_to_grid(187, 125), 125)

    def test_preview_duration_ms_clamps_to_audio_length(self) -> None:
        args = types.SimpleNamespace(preview_duration=12.0)
        self.assertEqual(cutup.preview_duration_ms(args, 5000), 5000)
        self.assertEqual(cutup.preview_duration_ms(args, 15000), 12000)

    def test_preview_duration_ms_disabled_by_default(self) -> None:
        args = types.SimpleNamespace(preview_duration=0.0)
        self.assertEqual(cutup.preview_duration_ms(args, 5000), 0)

    def test_clamp_to_section_preserves_grid_when_possible(self) -> None:
        self.assertEqual(cutup.clamp_to_section(4870, (0, 5000), 700, grid_ms=125), 4250)

    def test_candidate_audio_paths_accepts_single_audio_file(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            audio_path = Path(td) / "voice.wav"
            audio_path.write_bytes(b"not real wav but good enough for suffix discovery")
            self.assertEqual(cutup.candidate_audio_paths(audio_path), [audio_path])

    def test_resolve_output_root_avoids_nonempty_folder(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "render"
            out.mkdir()
            (out / "existing.txt").write_text("keep me", encoding="utf-8")
            with contextlib.redirect_stdout(io.StringIO()):
                resolved = cutup.resolve_output_root(str(out), overwrite=False)
            self.assertEqual(resolved, (Path(td) / "render_02").resolve())
            self.assertEqual((out / "existing.txt").read_text(encoding="utf-8"), "keep me")

    def test_resolve_output_root_honors_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "render"
            out.mkdir()
            (out / "existing.txt").write_text("keep me", encoding="utf-8")
            self.assertEqual(cutup.resolve_output_root(str(out), overwrite=True), out.resolve())

    def test_resolve_analysis_cache_path_supports_auto_and_explicit_paths(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td).resolve()
            self.assertIsNone(cutup.resolve_analysis_cache_path("", root))
            self.assertEqual(cutup.resolve_analysis_cache_path("auto", root), root / "audio_analysis_cache.json")
            explicit = root / "cache" / "sources.json"
            self.assertEqual(cutup.resolve_analysis_cache_path(str(explicit), root), explicit)

    def test_write_analysis_cache_refuses_existing_file_without_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cache = Path(td) / "sources.json"
            cache.write_text("keep me\n", encoding="utf-8")
            args = types.SimpleNamespace(overwrite=False, sample_rate=44100, bpm=0.0, slice_grid="off")
            with self.assertRaises(SystemExit):
                cutup.write_analysis_cache(cache, [], args, Path(td))
            self.assertEqual(cache.read_text(encoding="utf-8"), "keep me\n")

    def test_zero_crossing_rate_counts_sign_changes(self) -> None:
        class FakeAudio:
            channels = 1

            def get_array_of_samples(self):
                return [-1, 1, -1, 1]

        self.assertEqual(cutup.zero_crossing_rate(FakeAudio()), 1.0)

    def test_grid_cell_summary_caps_captured_cells(self) -> None:
        class FakeAudio:
            channels = 1
            rms = 10
            dBFS = -12.0

            def __init__(self, duration_ms: int = 1000) -> None:
                self.duration_ms = duration_ms

            def __len__(self) -> int:
                return self.duration_ms

            def __getitem__(self, key):
                return FakeAudio(max(0, int(key.stop) - int(key.start)))

            def get_array_of_samples(self):
                return [-1, 1] * max(1, self.duration_ms // 2)

        summary = cutup.grid_cell_summary(FakeAudio(), grid_ms=100, max_cells=3)
        self.assertEqual(summary["cell_count"], 10)
        self.assertEqual(summary["captured"], 3)
        self.assertTrue(summary["truncated"])
        self.assertEqual(summary["cells"][0]["duration_ms"], 100)
        self.assertEqual(summary["cells"][0]["zero_crossing_rate"], 1.0)

    def test_similarity_vector_for_entry_normalizes_descriptors(self) -> None:
        entry = {
            "duration_ms": 30000,
            "dbfs": -30.0,
            "zero_crossing_rate": 0.4,
            "grid_cell_summary": {
                "cells": [
                    {"dbfs": -30.0, "zero_crossing_rate": 0.25},
                    {"dbfs": -60.0, "zero_crossing_rate": 0.75},
                ]
            },
        }
        vector = cutup.similarity_vector_for_entry(entry)
        self.assertEqual(vector["fields"], list(cutup.ANALYSIS_SIMILARITY_VECTOR_FIELDS))
        self.assertEqual(len(vector["values"]), len(cutup.ANALYSIS_SIMILARITY_VECTOR_FIELDS))
        self.assertTrue(all(0 <= value <= 1 for value in vector["values"]))
        self.assertEqual(vector["values"][0], 1.0)
        self.assertEqual(vector["values"][1], 0.5)
        self.assertEqual(vector["values"][2], 0.4)

    def test_build_beat_jump_plan_orders_nearest_neighbors(self) -> None:
        entries = [
            {
                "index": 1,
                "basename": "a.wav",
                "path": "a.wav",
                "cache_key": "a",
                "similarity_vector": {"values": [0.10, 0.10]},
            },
            {
                "index": 2,
                "basename": "b.wav",
                "path": "b.wav",
                "cache_key": "b",
                "similarity_vector": {"values": [0.12, 0.10]},
            },
            {
                "index": 3,
                "basename": "c.wav",
                "path": "c.wav",
                "cache_key": "c",
                "similarity_vector": {"values": [0.90, 0.90]},
            },
        ]
        args = types.SimpleNamespace(beat_jump_mode="similarity")
        plan = cutup.build_beat_jump_plan(entries, args, top_k=2)
        self.assertEqual(plan["mode"], "similarity")
        self.assertEqual(plan["neighbor_count"], 2)
        self.assertEqual(plan["sources"][0]["source_cache_key"], "a")
        self.assertEqual(plan["sources"][0]["neighbors"][0]["target_cache_key"], "b")
        self.assertEqual(plan["sources"][0]["neighbors"][0]["target_basename"], "b.wav")
        self.assertLess(plan["sources"][0]["neighbors"][0]["distance"], plan["sources"][0]["neighbors"][1]["distance"])

    def test_choose_source_sample_uses_similarity_neighbor_when_available(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = [root / "a.wav", root / "b.wav"]
            for path in paths:
                path.write_bytes(b"placeholder")
            args = types.SimpleNamespace(sample_rate=44100, beat_jump_mode="similarity", beat_similarity_weight=1.0)
            samples = [
                cutup.SampleFile(path=paths[0], duration_ms=1000, words=1, intensity_hint=0, loop_hint=0),
                cutup.SampleFile(path=paths[1], duration_ms=1000, words=1, intensity_hint=0, loop_hint=0),
            ]
            key_a = cutup.analysis_cache_key_for_sample(samples[0], args)
            key_b = cutup.analysis_cache_key_for_sample(samples[1], args)
            payload = {
                "beat_jump_plan": {
                    "mode": "similarity",
                    "sources": [
                        {
                            "source_cache_key": key_a,
                            "neighbors": [{"target_cache_key": key_b}],
                        }
                    ],
                }
            }
            state = cutup.build_beat_jump_state(samples, args, payload)

            picked = cutup.choose_source_sample(samples, args, concrete=False, beat_jump=state, previous_sample=samples[0])

            self.assertTrue(state.active)
            self.assertEqual(picked, samples[1])
            self.assertEqual(state.selections, 1)
            self.assertEqual(state.fallbacks, 0)

    def test_choose_source_sample_honors_zero_similarity_weight(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = [root / "a.wav", root / "b.wav"]
            for path in paths:
                path.write_bytes(b"placeholder")
            args = types.SimpleNamespace(sample_rate=44100, beat_jump_mode="similarity", beat_similarity_weight=0.0)
            samples = [
                cutup.SampleFile(path=paths[0], duration_ms=1000, words=1, intensity_hint=0, loop_hint=0),
                cutup.SampleFile(path=paths[1], duration_ms=1000, words=1, intensity_hint=0, loop_hint=0),
            ]
            key_a = cutup.analysis_cache_key_for_sample(samples[0], args)
            key_b = cutup.analysis_cache_key_for_sample(samples[1], args)
            payload = {
                "beat_jump_plan": {
                    "mode": "similarity",
                    "sources": [
                        {
                            "source_cache_key": key_a,
                            "neighbors": [{"target_cache_key": key_b}],
                        }
                    ],
                }
            }
            state = cutup.build_beat_jump_state(samples, args, payload)

            picked = cutup.choose_source_sample(samples, args, concrete=False, beat_jump=state, previous_sample=samples[0])

            self.assertIn(picked, samples)
            self.assertEqual(state.selections, 0)
            self.assertEqual(state.fallbacks, 1)

    def test_choose_similarity_neighbor_expands_pool_with_novelty(self) -> None:
        samples = [
            cutup.SampleFile(path=Path(f"{idx}.wav"), duration_ms=1000, words=1, intensity_hint=0, loop_hint=0)
            for idx in range(6)
        ]
        args = types.SimpleNamespace(beat_novelty=1.0)
        with mock.patch.object(cutup.random, "choices", return_value=[samples[-1]]) as choices:
            picked = cutup.choose_similarity_neighbor(samples, args)
        called_pool = choices.call_args.args[0]
        self.assertEqual(len(called_pool), 6)
        self.assertEqual(picked, samples[-1])
        self.assertLess(choices.call_args.kwargs["weights"][0], choices.call_args.kwargs["weights"][-1])

    def test_cached_entry_without_required_descriptor_is_stale(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            audio_path = root / "voice.wav"
            audio_path.write_bytes(b"placeholder")
            args = types.SimpleNamespace(sample_rate=44100)
            sample = cutup.SampleFile(path=audio_path, duration_ms=1000, words=1, intensity_hint=0, loop_hint=0)
            size_bytes, mtime = cutup.audio_file_stat(audio_path)
            entry = {
                "cache_key": cutup.analysis_cache_key_for_sample(sample, args),
                "path": str(audio_path),
                "file_size_bytes": size_bytes,
                "file_mtime": mtime,
                "analysis_sample_rate": 44100,
                "cue_start_ms": 0,
                "cue_end_ms": 0,
                "cue_index": 0,
            }
            self.assertFalse(cutup.cached_entry_matches_sample(entry, sample, args))

    def test_write_analysis_cache_reuses_matching_existing_entry(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            audio_path = root / "voice.wav"
            audio_path.write_bytes(b"placeholder")
            cache = root / "sources.json"
            args = types.SimpleNamespace(overwrite=True, sample_rate=44100, bpm=120.0, slice_grid="1/16")
            sample = cutup.SampleFile(path=audio_path, duration_ms=1000, words=1, intensity_hint=0, loop_hint=0)
            size_bytes, mtime = cutup.audio_file_stat(audio_path)
            entry = {
                "index": 1,
                "cache_key": cutup.analysis_cache_key_for_sample(sample, args),
                "cache_state": "fresh",
                "path": str(audio_path),
                "basename": audio_path.name,
                "file_size_bytes": size_bytes,
                "file_mtime": mtime,
                "analysis_sample_rate": 44100,
                "duration_ms": 1000,
                "cue_start_ms": 0,
                "cue_end_ms": 0,
                "cue_index": 0,
                "rms": 123,
                "zero_crossing_rate": 0.25,
                "grid_cell_summary": {"grid_ms": 125, "cell_count": 8, "captured": 8, "truncated": False, "cells": []},
                "similarity_vector": {
                    "fields": list(cutup.ANALYSIS_SIMILARITY_VECTOR_FIELDS),
                    "values": [0.1, 0.2, 0.25, 0.2, 0.0, 0.25, 0.0],
                },
            }
            cache.write_text(
                json.dumps(
                    {
                        "version": cutup.ANALYSIS_CACHE_VERSION,
                        "kind": "cutups.audio_analysis_cache",
                        "sample_rate": 44100,
                        "samples": [entry],
                    }
                ),
                encoding="utf-8",
            )

            cutup.write_analysis_cache(cache, [sample], args, root)

            payload = json.loads(cache.read_text(encoding="utf-8"))
            self.assertEqual(payload["cache_stats"], {"errors": 0, "refreshed": 0, "reused": 1})
            self.assertEqual(payload["samples"][0]["cache_state"], "reused")
            self.assertEqual(payload["samples"][0]["rms"], 123)
            self.assertEqual(payload["samples"][0]["zero_crossing_rate"], 0.25)
            self.assertEqual(payload["samples"][0]["grid_cell_summary"]["grid_ms"], 125)
            self.assertEqual(payload["samples"][0]["similarity_vector"]["values"][2], 0.25)

    def test_live_poll_accepts_versioned_controls_payload(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            control_path = Path(td) / "live.json"
            control_path.write_text(
                json.dumps(
                    {
                        "version": 2,
                        "controls": {
                            "absurd_seriousness": 1.7,
                            "ghost_prob": -1,
                            "silence_prob": 0.3,
                            "burst_rate": 2.0,
                            "dropout_rate": 0.44,
                            "reverse_shard_rate": -1.0,
                            "filter_severity": "hard",
                            "stutter_rate": 1.5,
                            "mute_rate": 0.25,
                            "repeat_rate": 0.5,
                            "beat_dropout_rate": -0.1,
                            "force_section": "collapse",
                            "hold_section": True,
                            "burst_now": True,
                        },
                    }
                ),
                encoding="utf-8",
            )
            live = cutup.LiveControlState(enabled=True, control_file=control_path, poll_ms=0)
            live.poll()
            self.assertEqual(live.overrides["absurd_seriousness"], 1.0)
            self.assertEqual(live.overrides["ghost_prob"], 0.0)
            self.assertEqual(live.overrides["silence_prob"], 0.3)
            self.assertEqual(live.overrides["burst_rate"], 1.0)
            self.assertEqual(live.overrides["dropout_rate"], 0.44)
            self.assertEqual(live.overrides["reverse_shard_rate"], 0.0)
            self.assertEqual(live.overrides["stutter_rate"], 1.0)
            self.assertEqual(live.overrides["mute_rate"], 0.25)
            self.assertEqual(live.overrides["repeat_rate"], 0.5)
            self.assertEqual(live.overrides["beat_dropout_rate"], 0.0)
            self.assertEqual(live.filter_severity_override, "hard")
            self.assertEqual(live.section_override, "COLLAPSE")
            self.assertTrue(live.hold_section)
            self.assertTrue(live.burst_now)

            control_path.write_text(json.dumps({"version": 2, "controls": {"burst_rate": 0.2}}), encoding="utf-8")
            live.last_mtime_ns = -1
            live.poll()
            self.assertEqual(live.overrides["burst_rate"], 0.2)
            self.assertEqual(live.filter_severity_override, "hard")

    def test_live_poll_accepts_legacy_flat_payload(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            control_path = Path(td) / "legacy_live.json"
            control_path.write_text(
                json.dumps({"absurd_seriousness": 0.44, "recurrence_prob": 0.51}),
                encoding="utf-8",
            )
            live = cutup.LiveControlState(enabled=True, control_file=control_path, poll_ms=0)
            live.poll()
            self.assertEqual(live.overrides["absurd_seriousness"], 0.44)
            self.assertEqual(live.overrides["recurrence_prob"], 0.51)

    def test_live_poll_ignores_unsupported_version(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            control_path = Path(td) / "bad_version.json"
            control_path.write_text(
                json.dumps({"version": 99, "controls": {"absurd_seriousness": 0.11}}),
                encoding="utf-8",
            )
            live = cutup.LiveControlState(enabled=True, control_file=control_path, poll_ms=0)
            live.poll()
            self.assertEqual(live.overrides, {})

    def test_td_bridge_clamp_payload(self) -> None:
        clamped = td_bridge.clamp_payload(
            {
                "absurd_seriousness": 9,
                "ghost_prob": -2,
                "burst_rate": 1.4,
                "dropout_rate": 0.2,
                "stutter_rate": 2,
                "mute_rate": -1,
                "repeat_rate": 0.6,
                "beat_dropout_rate": 0.4,
                "x": 1,
            }
        )
        self.assertEqual(clamped["absurd_seriousness"], 1.0)
        self.assertEqual(clamped["ghost_prob"], 0.0)
        self.assertEqual(clamped["burst_rate"], 1.0)
        self.assertEqual(clamped["dropout_rate"], 0.2)
        self.assertEqual(clamped["stutter_rate"], 1.0)
        self.assertEqual(clamped["mute_rate"], 0.0)
        self.assertEqual(clamped["repeat_rate"], 0.6)
        self.assertEqual(clamped["beat_dropout_rate"], 0.4)
        self.assertNotIn("x", clamped)

    def test_td_bridge_extracts_conductor_controls(self) -> None:
        out = td_bridge.extract_conductor_controls({"force_section": "pressure", "filter_severity": "medium", "hold_section": 1, "burst_now": 0, "panic_silence": True})
        self.assertEqual(out["force_section"], "PRESSURE")
        self.assertEqual(out["filter_severity"], "medium")
        self.assertTrue(out["hold_section"])
        self.assertFalse(out["burst_now"])
        self.assertTrue(out["panic_silence"])
        self.assertNotIn("filter_severity", td_bridge.extract_conductor_controls({"force_section": "entry"}))


if __name__ == "__main__":
    unittest.main()
