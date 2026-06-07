import contextlib
import io
import json
import sys
import tempfile
import types
import unittest
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
            self.assertEqual(live.section_override, "COLLAPSE")
            self.assertTrue(live.hold_section)
            self.assertTrue(live.burst_now)

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
        clamped = td_bridge.clamp_payload({"absurd_seriousness": 9, "ghost_prob": -2, "x": 1})
        self.assertEqual(clamped["absurd_seriousness"], 1.0)
        self.assertEqual(clamped["ghost_prob"], 0.0)
        self.assertNotIn("x", clamped)

    def test_td_bridge_extracts_conductor_controls(self) -> None:
        out = td_bridge.extract_conductor_controls({"force_section": "pressure", "hold_section": 1, "burst_now": 0, "panic_silence": True})
        self.assertEqual(out["force_section"], "PRESSURE")
        self.assertTrue(out["hold_section"])
        self.assertFalse(out["burst_now"])
        self.assertTrue(out["panic_silence"])


if __name__ == "__main__":
    unittest.main()
