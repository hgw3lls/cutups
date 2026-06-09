import contextlib
import io
import json
import sys
import tempfile
import types
import unittest
import wave
from collections import Counter
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
from PY import live_control_gui  # noqa: E402
from PY import live_control_td_bridge as td_bridge  # noqa: E402


REPO_ROOT = Path(__file__).resolve().parents[1]


class LiveControlTests(unittest.TestCase):
    def test_doctor_format_check(self) -> None:
        self.assertEqual(cutup.format_check(True, "ready"), "ok - ready")
        self.assertEqual(cutup.format_check(False, "missing thing"), "missing - missing thing")

    def test_pyproject_exposes_console_scripts_and_package_data(self) -> None:
        pyproject = (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
        self.assertIn('cutups = "PY.cutup:main"', pyproject)
        self.assertIn('cutups-live-gui = "PY.live_control_gui:main"', pyproject)
        self.assertIn('cutups-live-monitor = "PY.live_control_monitor:main"', pyproject)
        self.assertIn('cutups-td-bridge = "PY.live_control_td_bridge:main"', pyproject)
        self.assertIn('PY = ["*.csv"]', pyproject)
        self.assertTrue((REPO_ROOT / "PY" / "__init__.py").exists())

    def test_optional_analysis_checks_report_expected_labels(self) -> None:
        checks = cutup.optional_analysis_checks()
        self.assertEqual([name for name, _, _ in checks], ["librosa", "scikit-learn"])
        self.assertTrue(all(isinstance(ok, bool) for _, ok, _ in checks))
        self.assertTrue(all(detail for _, _, detail in checks))

    def test_print_recipe_outputs_copy_ready_command(self) -> None:
        with contextlib.redirect_stdout(io.StringIO()) as out:
            cutup.print_recipe("beat-similarity")
        text = out.getvalue()
        self.assertIn("## beat-similarity", text)
        self.assertIn("cutups \\", text)
        self.assertIn("--preset beat-cutup", text)
        self.assertIn("--beat-jump-mode similarity", text)
        self.assertIn("--beat-novelty 0.35", text)
        self.assertNotIn("python3 PY/cutup.py", text)

    def test_print_recipe_all_includes_qa_sources(self) -> None:
        with contextlib.redirect_stdout(io.StringIO()) as out:
            cutup.print_recipe("all")
        text = out.getvalue()
        self.assertIn("## qa-sources", text)
        self.assertIn("--init-qa-sources ../cutups_qa_sources", text)
        self.assertIn("## signal-breach", text)

    def test_build_audio_plan_summarizes_events(self) -> None:
        event = cutup.Event(
            layer="voice_main",
            section="BUILD",
            source="/tmp/source.wav",
            source_basename="source.wav",
            source_duration_ms=1000,
            source_cue_start_ms=100,
            source_cue_end_ms=700,
            source_cue_text="testing one phrase",
            source_manifest_tags="spoken,voice",
            source_manifest_role="spoken",
            source_manifest_weight=1.5,
            start_ms=250,
            end_ms=850,
            fragment_duration_ms=600,
            gain_db=-4.0,
            reversed=False,
            speed=1.0,
            repeated=1,
            hp_hz=120,
            lp_hz=3200,
            grain_mode=False,
            from_memory=False,
            transformation="slice+grid",
            layer_role="foreground",
            recurrence_index=1,
            selection_reason="weighted_source",
            source_score_mode="spoken",
            source_base_weight=3.4,
            source_material_score=1.2,
            source_diversity_multiplier=0.8,
            source_final_weight=3.264,
            source_use_count_before=2,
            source_recent_hits_before=1,
            source_immediate_repeat=True,
            section_density_target=0.95,
            section_fragment_multiplier=1.2,
            section_repeat_probability=0.23,
            section_ghost_probability=0.18,
            baseline_placement_mode="gap",
            baseline_placement_original_start_ms=375,
            baseline_placement_cell_index=4,
            baseline_placement_cell_energy=0.12,
            planner_profile="phrase",
            planner_intent="reorder phrases with light interruptions",
            phrase_protected=True,
            beat_grid_ms=125,
            beat_grid_cell_index=2,
            beat_grid_offset_ms=0,
        )
        baseline_path = str(REPO_ROOT / "baseline.wav")
        args = types.SimpleNamespace(
            seed=9,
            preset="spoken-word-cutup",
            mode="audio",
            density="medium",
            sectional=True,
            section_arc="spoken",
            arrangement_style="sequential",
            source_score="spoken",
            planner_profile="phrase",
            source_diversity=0.65,
            concrete=False,
            bed_noise=False,
            baseline_beat=baseline_path,
            baseline_beat_gain=-12.0,
            baseline_beat_bars=2.0,
            baseline_beat_duck_db=4.0,
            baseline_beat_duck_ms=90,
            baseline_beat_duck_windows=3,
            baseline_placement="gap",
            baseline_grid_summary={"active": True, "mode": "gap", "grid_ms": 125, "cell_count": 16, "captured": 2, "truncated": True, "cells": []},
            baseline_beat_source_duration_ms=4000,
            baseline_beat_inferred_bpm=120.0,
            sample_rate=44100,
            master_gain=-3.0,
            bpm=120.0,
            slice_grid="1/16",
            beat_jump_mode="random",
            beat_similarity_weight=1.0,
            beat_novelty=0.0,
        )
        plan = cutup.build_audio_plan("cutup_01", [event], args, 2000, 100, 700)
        self.assertEqual(plan["kind"], "cutups.audio_composition_plan")
        self.assertEqual(plan["version"], 3)
        self.assertEqual(plan["summary"]["event_count"], 1)
        self.assertEqual(plan["summary"]["cue_event_count"], 1)
        self.assertEqual(plan["summary"]["phrase_protected_event_count"], 1)
        self.assertEqual(plan["summary"]["grid_aligned_event_count"], 1)
        self.assertEqual(plan["config"]["beat_grid_ms"], 125)
        self.assertEqual(plan["config"]["section_arc"], "spoken")
        self.assertEqual(plan["config"]["planner_profile"], "phrase")
        self.assertEqual(plan["config"]["source_score"], "spoken")
        self.assertEqual(plan["config"]["effective_source_score"], "spoken")
        self.assertEqual(plan["config"]["source_diversity"], 0.65)
        self.assertEqual(plan["config"]["baseline_beat"], baseline_path)
        self.assertEqual(plan["config"]["baseline_beat_gain"], -12.0)
        self.assertEqual(plan["config"]["baseline_beat_bars"], 2.0)
        self.assertEqual(plan["config"]["baseline_beat_duck_db"], 4.0)
        self.assertEqual(plan["config"]["baseline_beat_duck_ms"], 90)
        self.assertEqual(plan["config"]["baseline_beat_duck_windows"], 3)
        self.assertEqual(plan["config"]["baseline_placement"], "gap")
        self.assertEqual(plan["config"]["baseline_beat_source_duration_ms"], 4000)
        self.assertEqual(plan["config"]["baseline_beat_inferred_bpm"], 120.0)
        self.assertEqual(plan["baseline_grid"]["mode"], "gap")
        self.assertEqual(plan["baseline_grid"]["cell_count"], 16)
        self.assertEqual(plan["config"]["source_manifest"], "")
        self.assertEqual(plan["config"]["source_manifest_matches"], 0)
        self.assertEqual(plan["events"][0]["source_manifest_role"], "spoken")
        self.assertEqual(plan["events"][0]["transform_tags"], ["slice", "grid"])
        self.assertEqual(plan["events"][0]["planner"]["selection_reason"], "weighted_source")
        self.assertEqual(plan["events"][0]["planner"]["source_weight"]["source_score_mode"], "spoken")
        self.assertEqual(plan["events"][0]["planner"]["source_weight"]["manifest_weight"], 1.5)
        self.assertEqual(plan["events"][0]["planner"]["source_weight"]["final_weight"], 3.264)
        self.assertEqual(plan["events"][0]["planner"]["source_weight"]["use_count_before"], 2)
        self.assertTrue(plan["events"][0]["planner"]["source_weight"]["immediate_repeat"])
        self.assertEqual(plan["events"][0]["planner"]["section_targets"]["fragment_multiplier"], 1.2)
        self.assertEqual(plan["events"][0]["planner"]["baseline_placement"]["mode"], "gap")
        self.assertEqual(plan["events"][0]["planner"]["baseline_placement"]["cell_index"], 4)
        self.assertEqual(plan["events"][0]["planner"]["construction"]["profile"], "phrase")
        self.assertTrue(plan["events"][0]["planner"]["construction"]["phrase_protected"])
        self.assertEqual(plan["events"][0]["planner"]["beat_grid"]["cell_index"], 2)
        self.assertNotIn("source_final_weight", plan["events"][0])
        self.assertEqual(len(plan["section_windows"]), 5)
        self.assertEqual(len(plan["section_targets"]), 5)
        self.assertEqual(plan["section_targets"][0]["section_arc"], "spoken")
        self.assertEqual(plan["section_targets"][0]["planner_profile"], "phrase")

    def test_baseline_beat_bpm_from_duration(self) -> None:
        self.assertEqual(cutup.baseline_beat_bpm_from_duration(8000, 4), 120.0)
        self.assertEqual(cutup.baseline_beat_bpm_from_duration(2000, 1), 120.0)
        self.assertEqual(cutup.baseline_beat_bpm_from_duration(0, 4), 0.0)
        self.assertEqual(cutup.baseline_beat_bpm_from_duration(8000, 0), 0.0)

    def test_baseline_duck_windows_merge_event_ranges(self) -> None:
        first = cutup.Event(
            layer="voice_main",
            section="BUILD",
            source="/tmp/a.wav",
            source_basename="a.wav",
            source_duration_ms=1000,
            source_cue_start_ms=0,
            source_cue_end_ms=0,
            source_cue_text="",
            source_manifest_tags="",
            source_manifest_role="",
            source_manifest_weight=1.0,
            start_ms=100,
            end_ms=220,
            fragment_duration_ms=120,
            gain_db=-4.0,
            reversed=False,
            speed=1.0,
            repeated=1,
            hp_hz=0,
            lp_hz=0,
            grain_mode=False,
            from_memory=False,
            transformation="slice",
            layer_role="foreground",
            recurrence_index=1,
        )
        second = cutup.Event(
            layer="voice_cuts",
            section="BUILD",
            source="/tmp/b.wav",
            source_basename="b.wav",
            source_duration_ms=1000,
            source_cue_start_ms=0,
            source_cue_end_ms=0,
            source_cue_text="",
            source_manifest_tags="",
            source_manifest_role="",
            source_manifest_weight=1.0,
            start_ms=260,
            end_ms=340,
            fragment_duration_ms=80,
            gain_db=-5.0,
            reversed=False,
            speed=1.0,
            repeated=1,
            hp_hz=0,
            lp_hz=0,
            grain_mode=False,
            from_memory=False,
            transformation="slice",
            layer_role="rhythmic",
            recurrence_index=1,
        )
        self.assertEqual(cutup.baseline_duck_windows([first, second], total_ms=1000, duck_ms=50), [(50, 390)])

    def test_apply_baseline_placement_biases_to_requested_cell_type(self) -> None:
        profile = {
            "active": True,
            "mode": "accent",
            "grid_ms": 100,
            "cell_count": 5,
            "energies": [0.1, 0.9, 0.2, 0.8, 0.0],
            "low_threshold": 0.2,
            "high_threshold": 0.8,
        }
        pos, info = cutup.apply_baseline_placement(250, 50, (0, 1000), 1000, 100, profile)
        self.assertEqual(pos, 100)
        self.assertEqual(info["cell_index"], 1)
        self.assertEqual(info["cell_energy"], 0.9)

        gap_profile = dict(profile, mode="gap")
        pos, info = cutup.apply_baseline_placement(250, 50, (0, 1000), 1000, 100, gap_profile)
        self.assertEqual(pos, 400)
        self.assertEqual(info["cell_index"], 4)
        self.assertEqual(info["cell_energy"], 0.0)

        offbeat_profile = dict(profile, mode="offbeat", energies=[1.0, 0.1, 1.0], cell_count=3)
        pos, info = cutup.apply_baseline_placement(100, 50, (0, 1000), 1000, 100, offbeat_profile)
        self.assertEqual(pos, 100)
        self.assertEqual(info["cell_index"], 1)

    def test_baseline_grid_profile_stays_available_for_live_override(self) -> None:
        class FakeAudio:
            def __init__(self, duration_ms: int = 500, rms: int = 100) -> None:
                self.duration_ms = duration_ms
                self.rms = rms

            def __len__(self) -> int:
                return self.duration_ms

            def __getitem__(self, key):
                start = int(key.start or 0)
                stop = int(key.stop or self.duration_ms)
                rms = 200 if (start // 100) % 2 == 0 else 20
                return FakeAudio(max(0, stop - start), rms=rms)

        args = types.SimpleNamespace(baseline_placement="any", bpm=150.0, slice_grid="1/16")
        profile = cutup.baseline_grid_profile(FakeAudio(), args, total_ms=500)
        self.assertTrue(profile["active"])
        self.assertEqual(profile["mode"], "any")
        self.assertEqual(profile["grid_ms"], 100)
        self.assertEqual(profile["cell_count"], 5)
        self.assertEqual(len(profile["energies"]), 5)

    def test_write_qa_sources_creates_source_tree_and_refuses_clobber(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "qa_sources"
            written = cutup.write_qa_sources(root, overwrite=False)
            expected_wavs = sum(len(specs) for specs in cutup.QA_SOURCE_SPECS.values())
            expected_count = expected_wavs + 2
            self.assertEqual(len(written), expected_count)
            self.assertEqual({path.parent.name for path in written}, {"loops", "voice", "signal"})
            self.assertTrue((root / "loops" / "drum_pulse_120.wav").exists())
            self.assertTrue((root / "voice" / "voice_phrase_a.srt").exists())
            self.assertTrue((root / "voice" / "voice_cues.csv").exists())

            with wave.open(str(written[0]), "rb") as wav:
                self.assertEqual(wav.getnchannels(), 1)
                self.assertEqual(wav.getsampwidth(), 2)
                self.assertEqual(wav.getframerate(), 44100)
                self.assertGreater(wav.getnframes(), 0)

            srt_rows = cutup.parse_srt_cues(root / "voice" / "voice_phrase_a.srt")
            csv_rows = cutup.parse_csv_cues(root / "voice" / "voice_cues.csv")
            self.assertEqual(len(srt_rows), len(cutup.QA_SRT_CUES))
            self.assertEqual(len(csv_rows), len(cutup.QA_CSV_CUES))
            self.assertEqual(csv_rows[0]["file"], "voice_phrase_a.wav")

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
            source_diversity=0.4,
        )
        runtime = cutup.runtime_snapshot(args, live=None)
        self.assertEqual(runtime.absurd_seriousness, 0.2)
        self.assertEqual(runtime.ghost_prob, 0.7)
        self.assertEqual(runtime.source_diversity, 0.4)
        self.assertEqual(runtime.baseline_placement, "")

    def test_progress_reporter_writes_telemetry_rows(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            telemetry = Path(td) / "progress.jsonl"
            live = cutup.LiveControlState(enabled=True, telemetry_path=telemetry)
            reporter = cutup.ProgressReporter(enabled=False, live=live)
            reporter.update(0.25, "audio", "placing events", force=True)
            row = json.loads(telemetry.read_text(encoding="utf-8").strip())
        self.assertEqual(row["where"], "progress")
        self.assertEqual(row["progress"], 0.25)
        self.assertEqual(row["percent"], 25.0)
        self.assertEqual(row["stage"], "audio")
        self.assertEqual(row["detail"], "placing events")
        self.assertIn("eta_sec", row)

    def test_progress_helpers_format_eta_and_child_spans(self) -> None:
        self.assertEqual(cutup.format_eta(65), "01:05")
        self.assertEqual(cutup.format_eta(3661), "1:01:01")
        self.assertEqual(cutup.format_eta(None), "--:--")
        self.assertEqual(cutup.progress_child_span((0.2, 0.8), 0.25, 0.75), (0.35000000000000003, 0.6500000000000001))
        self.assertEqual(cutup.progress_spans("all")["audio"], (0.22, 1.0))

    def test_planner_profile_auto_follows_source_score(self) -> None:
        self.assertEqual(cutup.planner_profile_name(types.SimpleNamespace(planner_profile="auto", source_score="spoken", preset="")), "phrase")
        self.assertEqual(cutup.planner_profile_name(types.SimpleNamespace(planner_profile="auto", source_score="beat", preset="")), "beat")
        self.assertEqual(cutup.effective_source_score_mode(types.SimpleNamespace(planner_profile="breach", source_score="off", preset="")), "breach")

    def test_classify_dataset_source_recommends_roles(self) -> None:
        beat = cutup.classify_dataset_source(Path("drum_loop_120.wav"), 8000, 2, -12.0, 0.05)
        self.assertEqual(beat["role"], "beat")
        self.assertEqual(beat["recommended_preset"], "beat-cutup")
        self.assertIn("loop", beat["tags"])

        breach = cutup.classify_dataset_source(Path("radio_static_dropout.wav"), 1600, 2, -8.0, 0.3)
        self.assertEqual(breach["role"], "breach")
        self.assertEqual(breach["recommended_preset"], "signal-breach")
        self.assertGreaterEqual(breach["intensity"], 2)

        spoken = cutup.classify_dataset_source(Path("voice_phrase_a.wav"), 3200, 3, -20.0, 0.04)
        self.assertEqual(spoken["role"], "spoken")
        self.assertIn("transcription", spoken["notes"])

    def test_dataset_manifest_and_report_writers(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            rows = [
                {
                    "file": "voice.wav",
                    "role": "spoken",
                    "tags": "spoken,voice",
                    "intensity": 0,
                    "loop_hint": 0,
                    "words": 2,
                    "weight": 1.2,
                    "duration_ms": 1000,
                    "duration_sec": 1.0,
                    "dbfs": -18.0,
                    "zero_crossing_rate": 0.02,
                    "recommended_preset": "spoken-word-cutup",
                    "recommended_flags": "--preset spoken-word-cutup",
                    "notes": "cue recommended",
                }
            ]
            manifest = root / "manifest.csv"
            report_path = root / "report.json"
            cutup.write_dataset_manifest(manifest, rows)
            report = cutup.dataset_report(root, rows, unreadable=1)
            cutup.write_dataset_report(report_path, report)
            self.assertIn("recommended_preset", manifest.read_text(encoding="utf-8"))
            payload = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["kind"], "cutups.dataset_report")
            self.assertEqual(payload["role_counts"]["spoken"], 1)

    def test_live_gui_build_render_command(self) -> None:
        cmd = live_control_gui.build_render_command(
            "python",
            Path("/repo/PY/cutup.py"),
            "./samples",
            "out/gui",
            "beat-cutup",
            "32",
            Path("live.json"),
            Path("telemetry.jsonl"),
            bpm="120",
            slice_grid="1/16",
            baseline_beat="./beat.wav",
            semi_live=True,
            semi_live_chunk_sec="6",
        )
        self.assertEqual(cmd[:5], ["python", "/repo/PY/cutup.py", "--mode", "audio", "--input"])
        self.assertIn("--preset", cmd)
        self.assertIn("beat-cutup", cmd)
        self.assertIn("--baseline-beat", cmd)
        self.assertIn("./beat.wav", cmd)
        self.assertIn("--semi-live", cmd)
        self.assertIn("--semi-live-chunk-sec", cmd)
        self.assertIn("6", cmd)

    def test_live_gui_validate_render_settings_blocks_common_launch_errors(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            source = root / "source.wav"
            source.write_text("not real audio", encoding="utf-8")
            self.assertIn(
                "Grid slicing requires a BPM",
                live_control_gui.validate_render_settings(str(source), "30", "", "1/16", "", True, "8", root),
            )
            self.assertEqual(
                "",
                live_control_gui.validate_render_settings(str(source), "30", "120", "1/16", "", True, "8", root),
            )
            self.assertIn(
                "Chunk sec",
                live_control_gui.validate_render_settings(str(source), "30", "120", "1/16", "", True, "0.5", root),
            )
            self.assertIn(
                "Input does not exist",
                live_control_gui.validate_render_settings(str(root / "missing.wav"), "30", "120", "off", "", False, "8", root),
            )
            self.assertEqual(root / "relative.wav", live_control_gui.resolve_render_path("relative.wav", root))
            self.assertEqual(source, live_control_gui.resolve_render_path(str(source), root))

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
            source_diversity=0.0,
            section_arc="classic",
            source_score="off",
            baseline_placement="any",
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
            source_diversity=0.88,
            section_arc="ghost",
            source_score="breach",
            baseline_placement="gap",
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
        self.assertEqual(out.source_diversity, 0.88)
        self.assertEqual(out.section_arc, "ghost")
        self.assertEqual(out.source_score, "breach")
        self.assertEqual(out.baseline_placement, "gap")

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
        self.assertEqual(args.section_arc, "pulse")
        self.assertEqual(args.source_score, "beat")
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
        self.assertEqual(args.section_arc, "spoken")
        self.assertEqual(args.source_score, "spoken")
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
        self.assertEqual(args.section_arc, "breach")
        self.assertEqual(args.source_score, "breach")
        self.assertEqual(args.dropout_rate, 0.64)
        self.assertEqual(args.reverse_shard_rate, 0.46)
        self.assertEqual(args.filter_severity, "hard")

    def test_section_arc_modifies_classic_profile(self) -> None:
        base_args = types.SimpleNamespace(section_arc="classic", silence_prob=0.2, ghost_prob=0.3)
        breach_args = types.SimpleNamespace(section_arc="breach", silence_prob=0.2, ghost_prob=0.3)
        classic = cutup.section_profile(cutup.SECTION_PROGRESS["COLLAPSE"], base_args)
        breach = cutup.section_profile(cutup.SECTION_PROGRESS["COLLAPSE"], breach_args)
        self.assertEqual(classic["arc"], "classic")
        self.assertEqual(breach["arc"], "breach")
        self.assertGreater(breach["dens"], classic["dens"])
        self.assertLess(breach["frag_mul"], classic["frag_mul"])

    def test_source_material_score_prefers_matching_workflow_material(self) -> None:
        spoken_args = types.SimpleNamespace(source_score="spoken", bpm=0.0, slice_grid="off", concrete=False)
        beat_args = types.SimpleNamespace(source_score="beat", bpm=120.0, slice_grid="1/16", concrete=True)
        breach_args = types.SimpleNamespace(source_score="breach", bpm=0.0, slice_grid="off", concrete=True)
        voice = cutup.SampleFile(
            path=Path("voice_phrase.wav"),
            duration_ms=1800,
            words=6,
            intensity_hint=0,
            loop_hint=1,
            cue_start_ms=100,
            cue_end_ms=1900,
            cue_text="the signal repeats across the room",
        )
        loop = cutup.SampleFile(path=Path("drum_loop.wav"), duration_ms=8000, words=1, intensity_hint=0, loop_hint=2)
        noise = cutup.SampleFile(path=Path("radio_static_dropout.wav"), duration_ms=900, words=1, intensity_hint=3, loop_hint=0)
        pressure = {"name": "PRESSURE"}
        self.assertGreater(cutup.source_material_score(voice, spoken_args), cutup.source_material_score(noise, spoken_args))
        self.assertGreater(cutup.source_material_score(loop, beat_args), cutup.source_material_score(voice, beat_args))
        self.assertGreater(cutup.source_material_score(noise, breach_args, profile=pressure), cutup.source_material_score(voice, breach_args, profile=pressure))

    def test_source_manifest_enriches_sample_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            audio_path = root / "voice_phrase.wav"
            audio_path.write_bytes(b"placeholder")
            manifest = root / "sources.csv"
            manifest.write_text(
                "file,role,tags,intensity,loop_hint,words,weight\n"
                "voice_phrase.wav,spoken,\"voice,interview\",2,3,9,1.75\n",
                encoding="utf-8",
            )
            samples = [cutup.SampleFile(path=audio_path, duration_ms=1800, words=1, intensity_hint=0, loop_hint=0)]
            entries = cutup.load_source_manifest(manifest, root)
            matched = cutup.apply_source_manifest(samples, entries, root)
        self.assertEqual(matched, 1)
        self.assertEqual(samples[0].manifest_role, "spoken")
        self.assertEqual(samples[0].manifest_tags, "interview,spoken,voice")
        self.assertEqual(samples[0].intensity_hint, 2)
        self.assertEqual(samples[0].loop_hint, 3)
        self.assertEqual(samples[0].words, 9)
        self.assertEqual(samples[0].manifest_weight, 1.75)

    def test_source_manifest_json_sources_shape(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            manifest = root / "sources.json"
            manifest.write_text(
                json.dumps({"sources": [{"file": "signal/dropout.wav", "type": "breach", "tags": "radio,dropout", "weight": 2.0}]}),
                encoding="utf-8",
            )
            entries = cutup.load_source_manifest(manifest, root)
        self.assertIn("dropout.wav", entries)
        self.assertEqual(entries["dropout.wav"]["role"], "breach")
        self.assertEqual(entries["dropout.wav"]["tags"], "breach,dropout,radio")
        self.assertEqual(entries["dropout.wav"]["weight"], 2.0)

    def test_source_selection_diagnostics_report_weight_context(self) -> None:
        sample = cutup.SampleFile(path=Path("radio_static_dropout.wav"), duration_ms=900, words=1, intensity_hint=3, loop_hint=0)
        args = types.SimpleNamespace(source_score="breach", source_diversity=1.0, bpm=0.0, slice_grid="off", concrete=True)
        source_counts = Counter({str(sample.path): 3})
        diagnostics = cutup.source_selection_diagnostics(
            sample,
            args,
            concrete=True,
            source_counts=source_counts,
            recent_source_keys=[str(sample.path), str(sample.path), "other.wav"],
            previous_sample=sample,
            profile={"name": "COLLAPSE", "dens": 1.4, "frag_mul": 0.5, "repeat": 0.7, "ghost": 0.6},
            reason="weighted_source_fallback",
        )
        self.assertEqual(diagnostics["selection_reason"], "weighted_source_fallback")
        self.assertEqual(diagnostics["source_score_mode"], "breach")
        self.assertEqual(diagnostics["source_use_count_before"], 3)
        self.assertEqual(diagnostics["source_recent_hits_before"], 2)
        self.assertTrue(diagnostics["source_immediate_repeat"])
        self.assertGreater(diagnostics["source_material_score"], 1.0)
        self.assertLess(diagnostics["source_diversity_multiplier"], 1.0)
        self.assertGreater(diagnostics["source_final_weight"], 0.0)
        self.assertEqual(diagnostics["section_density_target"], 1.4)

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

    def test_candidate_audio_paths_excludes_baseline_file(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            voice = root / "voice.wav"
            baseline = root / "baseline.wav"
            voice.write_bytes(b"suffix only")
            baseline.write_bytes(b"suffix only")
            self.assertEqual(cutup.candidate_audio_paths(root, exclude_paths=[baseline]), [voice])
            self.assertEqual(cutup.candidate_audio_paths(baseline, exclude_paths=[baseline]), [])

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

    def test_weighted_choice_penalizes_overused_recent_source_when_diverse(self) -> None:
        samples = [
            cutup.SampleFile(path=Path("a.wav"), duration_ms=1000, words=1, intensity_hint=0, loop_hint=0),
            cutup.SampleFile(path=Path("b.wav"), duration_ms=1000, words=1, intensity_hint=0, loop_hint=0),
            cutup.SampleFile(path=Path("c.wav"), duration_ms=1000, words=1, intensity_hint=0, loop_hint=0),
        ]
        args = types.SimpleNamespace(source_diversity=1.0)
        source_counts = Counter({str(samples[0].path): 4})
        with mock.patch.object(cutup.random, "choices", return_value=[samples[1]]) as choices:
            picked = cutup.weighted_choice(
                samples,
                concrete=False,
                args=args,
                source_counts=source_counts,
                recent_source_keys=[str(samples[0].path), str(samples[0].path)],
                previous_sample=samples[0],
            )
        weights = choices.call_args.kwargs["weights"]
        self.assertEqual(picked, samples[1])
        self.assertLess(weights[0], weights[1])
        self.assertEqual(weights[1], weights[2])

    def test_weighted_choice_applies_source_material_score(self) -> None:
        samples = [
            cutup.SampleFile(path=Path("voice_phrase.wav"), duration_ms=1800, words=6, intensity_hint=0, loop_hint=1),
            cutup.SampleFile(path=Path("radio_static_dropout.wav"), duration_ms=1800, words=1, intensity_hint=3, loop_hint=0),
        ]
        args = types.SimpleNamespace(source_score="breach", source_diversity=0.0, bpm=0.0, slice_grid="off", concrete=True)
        with mock.patch.object(cutup.random, "choices", return_value=[samples[1]]) as choices:
            picked = cutup.weighted_choice(samples, concrete=True, args=args, profile={"name": "COLLAPSE"})
        weights = choices.call_args.kwargs["weights"]
        self.assertEqual(picked, samples[1])
        self.assertGreater(weights[1], weights[0])

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
                            "source_diversity": 1.7,
                            "section_arc": "ghost",
                            "source_score": "breach",
                            "baseline_placement": "offbeat",
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
            self.assertEqual(live.overrides["source_diversity"], 1.0)
            self.assertEqual(live.section_arc_override, "ghost")
            self.assertEqual(live.source_score_override, "breach")
            self.assertEqual(live.baseline_placement_override, "offbeat")
            self.assertEqual(live.filter_severity_override, "hard")
            self.assertEqual(live.section_override, "COLLAPSE")
            self.assertTrue(live.hold_section)
            self.assertTrue(live.burst_now)

            control_path.write_text(json.dumps({"version": 2, "controls": {"burst_rate": 0.2}}), encoding="utf-8")
            live.last_mtime_ns = -1
            live.poll()
            self.assertEqual(live.overrides["burst_rate"], 0.2)
            self.assertEqual(live.filter_severity_override, "hard")
            self.assertEqual(live.section_arc_override, "ghost")
            self.assertEqual(live.source_score_override, "breach")
            self.assertEqual(live.baseline_placement_override, "offbeat")

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
                "source_diversity": 1.4,
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
        self.assertEqual(clamped["source_diversity"], 1.0)
        self.assertNotIn("x", clamped)

    def test_td_bridge_extracts_conductor_controls(self) -> None:
        out = td_bridge.extract_conductor_controls(
            {
                "force_section": "pressure",
                "filter_severity": "medium",
                "section_arc": "pulse",
                "source_score": "beat",
                "baseline_placement": "gap",
                "hold_section": 1,
                "burst_now": 0,
                "panic_silence": True,
            }
        )
        self.assertEqual(out["force_section"], "PRESSURE")
        self.assertEqual(out["filter_severity"], "medium")
        self.assertEqual(out["section_arc"], "pulse")
        self.assertEqual(out["source_score"], "beat")
        self.assertEqual(out["baseline_placement"], "gap")
        self.assertTrue(out["hold_section"])
        self.assertFalse(out["burst_now"])
        self.assertTrue(out["panic_silence"])
        invalid = td_bridge.extract_conductor_controls({"force_section": "entry", "section_arc": "bad", "source_score": "bad", "baseline_placement": "bad"})
        self.assertNotIn("filter_severity", invalid)
        self.assertNotIn("section_arc", invalid)
        self.assertNotIn("source_score", invalid)
        self.assertNotIn("baseline_placement", invalid)


if __name__ == "__main__":
    unittest.main()
