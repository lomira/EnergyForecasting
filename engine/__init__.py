"""Make the src-layout package importable during workspace test discovery."""

from pathlib import Path

__path__.append(str(Path(__file__).parent / "src" / "engine"))
