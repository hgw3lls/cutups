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
