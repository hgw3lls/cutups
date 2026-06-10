import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


class SuperColliderStaticTests(unittest.TestCase):
    def test_tape_deck_exposes_python_render_load_modes(self) -> None:
        text = (REPO_ROOT / "SC" / "cutup.scd").read_text(encoding="utf-8")
        for marker in (
            "~scanLiveTrack",
            "~scanChunksFolder",
            "~scanCurrentSource",
            "LOAD DIR",
            "LOAD LIVE",
            "LOAD CHUNKS",
            "*_live_track.wav",
            "chunks",
            "REFRESH CHUNKS",
            "LIVE TRACK",
            "\\liveTrackTape",
            "~startChunkRefresh",
            "~refreshChunksOnce",
            "~startLiveTrack",
            "~hardStopLiveTrack",
            "\\concreteCutTape",
            "~concreteShot",
            "CNCR",
            "\\tapeDeckMaster",
            "\\cutupsTapedeckConcrete",
            "/cutups/concrete",
            "/cutups/loadLive",
            "/cutups/loadChunks",
            "/cutups/chunks/refresh",
            "/cutups/live/start",
            "/cutups/live/stop",
            "/cutups/live/amp",
            "/cutups/master/age",
            "AGE",
        ):
            self.assertIn(marker, text)


if __name__ == "__main__":
    unittest.main()
