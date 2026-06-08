import csv
import hashlib
import json
import math
import shutil
import struct
import subprocess
import sys
import tempfile
import unittest
import wave
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _write_tone_wav(path: Path, duration_s: float = 3.0, sample_rate: int = 44100) -> None:
    frames = int(duration_s * sample_rate)
    with wave.open(str(path), "wb") as wav:
        wav.setnchannels(1)
        wav.setsampwidth(2)
        wav.setframerate(sample_rate)
        for idx in range(frames):
            phase = idx / sample_rate
            freq = 180.0 if (idx // (sample_rate // 2)) % 2 == 0 else 360.0
            value = 0.36 * math.sin(2.0 * math.pi * freq * phase)
            value += 0.12 * math.sin(2.0 * math.pi * 720.0 * phase)
            wav.writeframes(struct.pack("<h", int(max(-1.0, min(1.0, value)) * 32767)))


def _wav_info(path: Path) -> tuple[int, int, float]:
    with wave.open(str(path), "rb") as wav:
        channels = wav.getnchannels()
        sample_rate = wav.getframerate()
        duration = wav.getnframes() / float(sample_rate)
    return channels, sample_rate, duration


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


class AudioSmokeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        if shutil.which("ffmpeg") is None and shutil.which("avconv") is None:
            raise unittest.SkipTest("ffmpeg/avconv is required for audio smoke tests")
        probe = subprocess.run(
            [
                sys.executable,
                "-c",
                "from pydub import AudioSegment; import pydub.effects; import pydub.generators",
            ],
            text=True,
            capture_output=True,
        )
        if probe.returncode != 0:
            raise unittest.SkipTest(f"pydub is required for audio smoke tests: {probe.stderr.strip()}")

    def run_cutup(self, args: list[str]) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(
            [sys.executable, "PY/cutup.py", *args],
            cwd=REPO_ROOT,
            text=True,
            capture_output=True,
            timeout=60,
        )
        if result.returncode != 0:
            self.fail(
                "cutup.py failed\n"
                f"stdout:\n{result.stdout}\n"
                f"stderr:\n{result.stderr}"
            )
        return result

    def test_doctor_cli_reports_environment(self) -> None:
        result = self.run_cutup(["--doctor"])
        self.assertIn("CUTUP DOCTOR", result.stdout)
        self.assertIn("python>=3.10:", result.stdout)
        self.assertIn("pydub:", result.stdout)
        self.assertIn("ffmpeg/avconv:", result.stdout)
        self.assertIn("optional analysis:", result.stdout)
        self.assertIn("librosa:", result.stdout)
        self.assertIn("scikit-learn:", result.stdout)
        self.assertRegex(result.stdout, r"analysis_status: (ready|optional dependencies not installed)")
        if "analysis_status: optional dependencies not installed" in result.stdout:
            self.assertIn("requirements-analysis.txt", result.stdout)
        self.assertIn("presets:", result.stdout)
        self.assertRegex(result.stdout, r"status: (ready|action needed)")

    def test_audio_cli_smoke_writes_master_preview_and_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            source = root / "voice_phrase.wav"
            output = root / "render"
            _write_tone_wav(source)

            result = self.run_cutup(
                [
                    "--mode",
                    "audio",
                    "--preset",
                    "beat-cutup",
                    "--input",
                    str(source),
                    "--bpm",
                    "120",
                    "--slice-grid",
                    "1/16",
                    "--beat-jump-mode",
                    "similarity",
                    "--beat-similarity-weight",
                    "0.75",
                    "--beat-novelty",
                    "0.4",
                    "--stutter-rate",
                    "1",
                    "--repeat-rate",
                    "1",
                    "--mute-rate",
                    "1",
                    "--beat-dropout-rate",
                    "1",
                    "--output",
                    str(output),
                    "--duration",
                    "3",
                    "--preview-duration",
                    "1",
                    "--analysis-cache",
                    "auto",
                    "--seed",
                    "101",
                    "--overwrite",
                ]
            )
            self.assertIn("Audio events placed:", result.stdout)
            self.assertIn("Analysis cache written:", result.stdout)
            self.assertIn("reused=0", result.stdout)
            self.assertIn("refreshed=1", result.stdout)

            variant = output / "audio_cutups" / "cutup_01"
            master = variant / "cutup_01_master.wav"
            preview = variant / "cutup_01_preview.wav"
            events = variant / "cutup_01_events.csv"
            plan = variant / "cutup_01_plan.json"
            score = variant / "cutup_01_score.txt"
            analysis_cache = output / "audio_analysis_cache.json"
            for path in (master, preview, events, plan, score, analysis_cache):
                self.assertTrue(path.exists(), path)

            self.assertEqual(_wav_info(master)[:2], (2, 44100))
            self.assertAlmostEqual(_wav_info(master)[2], 3.0, places=2)
            self.assertEqual(_wav_info(preview)[:2], (2, 44100))
            self.assertAlmostEqual(_wav_info(preview)[2], 1.0, places=2)

            rows = list(csv.DictReader(events.open(newline="", encoding="utf-8")))
            self.assertGreaterEqual(len(rows), 1)
            transforms = " ".join(row["transformation"] for row in rows)
            for tag in ("+grid", "+beatstutter", "+beatmute", "+beatrepeat", "+beatdrop"):
                self.assertIn(tag, transforms)

            render_plan = json.loads(plan.read_text(encoding="utf-8"))
            self.assertEqual(render_plan["kind"], "cutups.audio_composition_plan")
            self.assertEqual(render_plan["version"], 1)
            self.assertEqual(render_plan["variant"], "cutup_01")
            self.assertEqual(render_plan["seed"], 101)
            self.assertEqual(render_plan["preset"], "beat-cutup")
            self.assertEqual(render_plan["duration_ms"], 3000)
            self.assertEqual(render_plan["config"]["beat_grid_ms"], 125)
            self.assertEqual(render_plan["config"]["beat_novelty"], 0.4)
            self.assertEqual(render_plan["summary"]["event_count"], len(rows))
            self.assertEqual(len(render_plan["events"]), len(rows))
            self.assertEqual(len(render_plan["section_windows"]), 5)
            self.assertIn("transform_tags", render_plan["events"][0])

            cache = json.loads(analysis_cache.read_text(encoding="utf-8"))
            self.assertEqual(cache["kind"], "cutups.audio_analysis_cache")
            self.assertEqual(cache["version"], 7)
            self.assertEqual(cache["grid_ms"], 125)
            self.assertEqual(cache["beat_jump_mode"], "similarity")
            self.assertEqual(cache["beat_similarity_weight"], 0.75)
            self.assertEqual(cache["beat_novelty"], 0.4)
            sample_fields = [
                "duration",
                "loudness",
                "zero_crossing",
                "grid_loudness_mean",
                "grid_loudness_variation",
                "grid_zcr_mean",
                "grid_zcr_variation",
            ]
            self.assertEqual(cache["similarity_vector_fields"], sample_fields)
            self.assertEqual(cache["beat_jump_plan"]["mode"], "similarity")
            self.assertEqual(cache["beat_jump_plan"]["metric"], "normalized_euclidean")
            self.assertEqual(cache["beat_jump_plan"]["neighbor_count"], 0)
            self.assertEqual(cache["cache_stats"], {"errors": 0, "refreshed": 1, "reused": 0})
            self.assertEqual(len(cache["samples"]), 1)
            sample = cache["samples"][0]
            self.assertEqual(sample["basename"], source.name)
            self.assertEqual(sample["cache_state"], "fresh")
            self.assertEqual(sample["channels"], 2)
            self.assertGreater(sample["duration_ms"], 0)
            self.assertGreater(sample["zero_crossing_rate"], 0)
            self.assertLessEqual(sample["zero_crossing_rate"], 1)
            self.assertEqual(sample["grid_cell_summary"]["grid_ms"], 125)
            self.assertEqual(sample["grid_cell_summary"]["cell_count"], 24)
            self.assertEqual(sample["grid_cell_summary"]["captured"], 24)
            self.assertFalse(sample["grid_cell_summary"]["truncated"])
            self.assertIn("rms", sample["grid_cell_summary"]["cells"][0])
            self.assertIn("zero_crossing_rate", sample["grid_cell_summary"]["cells"][0])
            self.assertEqual(sample["similarity_vector"]["fields"], sample_fields)
            self.assertEqual(len(sample["similarity_vector"]["values"]), len(sample_fields))
            self.assertTrue(all(0 <= value <= 1 for value in sample["similarity_vector"]["values"]))

    def test_audio_cli_uses_srt_cues_as_phrase_sources(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            source = root / "voice_phrase.wav"
            cues = root / "voice_phrase.srt"
            output = root / "cue_render"
            _write_tone_wav(source, duration_s=4.0)
            cues.write_text(
                "1\n"
                "00:00:00,500 --> 00:00:01,500\n"
                "first clear phrase\n\n"
                "2\n"
                "00:00:02,000 --> 00:00:03,250\n"
                "second clear phrase\n",
                encoding="utf-8",
            )

            self.run_cutup(
                [
                    "--mode",
                    "audio",
                    "--preset",
                    "spoken-word-cutup",
                    "--input",
                    str(source),
                    "--cue-file",
                    str(cues),
                    "--cue-slice-mode",
                    "full",
                    "--output",
                    str(output),
                    "--duration",
                    "3",
                    "--seed",
                    "303",
                    "--overwrite",
                ]
            )

            events = output / "audio_cutups" / "cutup_01" / "cutup_01_events.csv"
            rows = list(csv.DictReader(events.open(newline="", encoding="utf-8")))
            self.assertGreaterEqual(len(rows), 1)
            cue_texts = {row["source_cue_text"] for row in rows}
            self.assertTrue(cue_texts <= {"first clear phrase", "second clear phrase"})
            self.assertTrue({int(row["source_cue_start_ms"]) for row in rows} <= {500, 2000})
            self.assertTrue(all(int(row["source_cue_start_ms"]) > 0 for row in rows))
            self.assertTrue(all(int(row["source_duration_ms"]) in {1000, 1250} for row in rows))

    def test_audio_cli_same_seed_is_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            source = root / "voice_phrase.wav"
            _write_tone_wav(source)

            common_args = [
                "--mode",
                "audio",
                "--preset",
                "beat-cutup",
                "--input",
                str(source),
                "--bpm",
                "120",
                "--slice-grid",
                "1/16",
                "--stutter-rate",
                "0.7",
                "--repeat-rate",
                "0.5",
                "--mute-rate",
                "0.2",
                "--beat-dropout-rate",
                "0.5",
                "--duration",
                "3",
                "--seed",
                "202",
                "--overwrite",
            ]
            out_a = root / "render_a"
            out_b = root / "render_b"
            self.run_cutup([*common_args, "--output", str(out_a)])
            self.run_cutup([*common_args, "--output", str(out_b)])

            variant_a = out_a / "audio_cutups" / "cutup_01"
            variant_b = out_b / "audio_cutups" / "cutup_01"
            self.assertEqual(
                _hash_file(variant_a / "cutup_01_events.csv"),
                _hash_file(variant_b / "cutup_01_events.csv"),
            )
            self.assertEqual(
                _hash_file(variant_a / "cutup_01_plan.json"),
                _hash_file(variant_b / "cutup_01_plan.json"),
            )
            self.assertEqual(
                _hash_file(variant_a / "cutup_01_master.wav"),
                _hash_file(variant_b / "cutup_01_master.wav"),
            )


if __name__ == "__main__":
    unittest.main()
