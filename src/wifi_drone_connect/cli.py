"""Inspect bundled profiles and take a JPEG snapshot."""

import argparse
import asyncio
from contextlib import aclosing
from dataclasses import asdict
import json
from pathlib import Path

from . import CameraAdapter, CameraError, camera_for_url, identify, profiles


async def snapshot(camera: CameraAdapter) -> bytes:
    async with aclosing(camera.frames()) as frames:
        return await anext(frames)


def main() -> None:
    parser = argparse.ArgumentParser(prog="wifi-drone-connect")
    commands = parser.add_subparsers(dest="command", required=True)
    listing = commands.add_parser("profiles", help="list profiles or match a supplied SSID")
    listing.add_argument("--ssid")
    capture = commands.add_parser("snapshot", help="save a JPEG from a configured source")
    capture.add_argument("url")
    capture.add_argument("output", type=Path)
    capture.add_argument("--rtsp-transport", choices=("tcp", "udp"), default="tcp")
    capture.add_argument("--copy-mjpeg", action="store_true")
    args = parser.parse_args()

    if args.command == "profiles":
        found = profiles() if args.ssid is None else identify(args.ssid)
        print(json.dumps([asdict(profile) for profile in found], indent=2))
        return

    try:
        camera = camera_for_url(args.url, args.rtsp_transport, args.copy_mjpeg)
        args.output.write_bytes(asyncio.run(snapshot(camera)))
    except (CameraError, OSError, ValueError) as error:
        parser.exit(1, f"{error}\n")
