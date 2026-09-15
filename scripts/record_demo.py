"""Record the control plane's guided tour as docs/demo.gif.

Needs the backend and the page running first:

    go run ./cmd/cluster
    python -m http.server 5500 --directory frontend

then:

    pip install playwright
    python scripts/record_demo.py

It drives your installed Chrome (no browser download), records the tour to a
video, and has ffmpeg turn that into a sped-up GIF with a proper palette.
"""

import argparse
import asyncio
import shutil
import subprocess
import tempfile
from pathlib import Path

from playwright.async_api import async_playwright

ROOT = Path(__file__).resolve().parent.parent


async def record(url: str, video_dir: Path, width: int, height: int) -> Path:
    async with async_playwright() as p:
        browser = await p.chromium.launch(channel="chrome")
        ctx = await browser.new_context(
            viewport={"width": width, "height": height},
            record_video_dir=str(video_dir),
            record_video_size={"width": width, "height": height},
        )
        page = await ctx.new_page()
        await page.goto(url)

        # Wait for the cluster to come up: the map renders on the first state frame.
        await page.wait_for_selector("#map:not(.stale)", timeout=120_000)
        # The how-it-works cards are for people reading the page, not the GIF.
        await page.evaluate("document.getElementById('how').open = false")
        await page.wait_for_timeout(1500)

        await page.click("#tour")
        await page.wait_for_function(
            "document.getElementById('tour').textContent === 'stop tour'")
        await page.wait_for_function(
            "document.getElementById('tour').textContent === 'run tour'",
            timeout=600_000, polling=500)
        await page.wait_for_timeout(2500)

        video = page.video
        await ctx.close()  # finalises the video file
        await browser.close()
        return Path(await video.path())


def to_gif(video: Path, out: Path, speed: float, fps: int, width: int) -> None:
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        raise SystemExit("ffmpeg not found on PATH")
    filters = f"setpts=PTS/{speed},fps={fps},scale={width}:-1:flags=lanczos"
    palette = out.with_suffix(".palette.png")
    subprocess.run([ffmpeg, "-y", "-loglevel", "error", "-i", str(video),
                    "-vf", f"{filters},palettegen=max_colors=48:stats_mode=diff",
                    str(palette)], check=True)
    subprocess.run([ffmpeg, "-y", "-loglevel", "error", "-i", str(video), "-i", str(palette),
                    "-lavfi", f"{filters}[x];[x][1:v]paletteuse=dither=none:diff_mode=rectangle",
                    "-loop", "0", str(out)], check=True)
    palette.unlink(missing_ok=True)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", default="http://localhost:5500")
    ap.add_argument("--out", default=str(ROOT / "docs" / "demo.gif"))
    ap.add_argument("--speed", type=float, default=2.0, help="playback speed-up")
    ap.add_argument("--fps", type=int, default=10)
    ap.add_argument("--gif-width", type=int, default=960)
    args = ap.parse_args()

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as tmp:
        video = asyncio.run(record(args.url, Path(tmp), 1280, 1010))
        to_gif(video, out, args.speed, args.fps, args.gif_width)
    print(f"wrote {out} ({out.stat().st_size / 1e6:.1f} MB)")


if __name__ == "__main__":
    main()
