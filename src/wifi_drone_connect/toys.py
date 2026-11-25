"""Camera-only transport options for toy drones."""

import asyncio
from collections.abc import AsyncGenerator
from contextlib import aclosing, suppress
from dataclasses import dataclass
import socket
from urllib.parse import parse_qs, urlsplit, urlunsplit

from .core import CameraAdapter, CameraError
from .ffmpeg import FFmpegCamera
from .jieli import JieliCamera
from .lewei import LeweiCamera


@dataclass(frozen=True, slots=True)
class KYUFOCamera:
    """Send the documented video heartbeat while reading an RTSP camera."""

    host: str
    video: FFmpegCamera

    async def frames(self) -> AsyncGenerator[bytes, None]:
        loop = asyncio.get_running_loop()
        try:
            addresses = await loop.getaddrinfo(self.host, 7099, type=socket.SOCK_DGRAM)
            family, kind, protocol, _, destination = addresses[0]
            with socket.socket(family, kind, protocol) as sock:
                sock.setblocking(False)
                await loop.sock_connect(sock, destination)
                await loop.sock_sendall(sock, b"\x01\x01")
                heartbeat = asyncio.create_task(self._heartbeat(sock))
                try:
                    async with aclosing(self.video.frames()) as frames:
                        async for jpeg in frames:
                            yield jpeg
                finally:
                    heartbeat.cancel()
                    with suppress(asyncio.CancelledError):
                        await heartbeat
        except OSError as error:
            raise CameraError("Could not send the KY UFO video heartbeat") from error

    async def _heartbeat(self, sock: socket.socket) -> None:
        while True:
            await asyncio.sleep(1)
            try:
                await asyncio.get_running_loop().sock_sendall(sock, b"\x01\x01")
            except OSError:
                # A transient UDP send failure should not interrupt healthy RTSP video.
                continue


def camera_for_url(url: str, rtsp_transport: str = "tcp", copy_mjpeg: bool = False) -> CameraAdapter:
    """Select a camera transport from an explicitly configured URL."""
    parsed = urlsplit(url)
    if parsed.scheme not in {"kyufo", "jieli", "lewei"}:
        return FFmpegCamera(url, rtsp_transport, copy_mjpeg)
    if not parsed.hostname or parsed.username is not None or parsed.fragment or parsed.port == 0:
        raise ValueError("Expected a toy camera hostname and an optional port")
    query = parse_qs(parsed.query, keep_blank_values=True)
    if any(len(values) != 1 for values in query.values()):
        raise ValueError("Toy camera options must occur only once")
    if parsed.scheme == "jieli":
        version = query.get("ver", ["25"])[0]
        if parsed.path not in {"", "/"} or set(query) - {"ver"} or version not in {"2", "25"}:
            raise ValueError("Use jieli://HOST with an optional CTP port and ver=2 or ver=25")
        return JieliCamera(parsed.hostname, parsed.port or 3333, access_version=version)
    if parsed.scheme == "lewei":
        if parsed.path not in {"", "/"} or query:
            raise ValueError(f"Use {parsed.scheme}://HOST with an optional camera port")
        return LeweiCamera(parsed.hostname, parsed.port or 7060)

    if set(query) - {"rotate"}:
        raise ValueError("KY UFO camera URL accepts only the rotate option")
    rotation = query.get("rotate", ["270"])[0]
    if rotation not in {"0", "90", "180", "270"}:
        raise ValueError("KY UFO rotation must be 0, 90, 180 or 270 degrees counter-clockwise")
    authority = parsed.netloc if parsed.port else f"{parsed.netloc}:7070"
    target = urlunsplit(("rtsp", authority, parsed.path or "/webcam", "", ""))
    return KYUFOCamera(
        parsed.hostname, FFmpegCamera(target, rtsp_transport, copy_mjpeg, rotate=int(rotation))
    )
