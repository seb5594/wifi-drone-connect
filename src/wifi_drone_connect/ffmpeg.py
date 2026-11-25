"""Read RTSP or HTTP video via FFmpeg and yield bounded JPEG frames."""

import asyncio
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from urllib.parse import urlsplit

from .core import CameraError

MAX_JPEG_BYTES = 8 * 1024 * 1024
BOUNDARY = b"--wifi_drone_frame"


async def read_jpeg(stream: asyncio.StreamReader) -> bytes:
    """Read a single frame from FFmpeg's multipart JPEG muxer."""
    line = await stream.readline()
    if line == b"\r\n":
        line = await stream.readline()
    if line.rstrip(b"\r\n") != BOUNDARY:
        raise CameraError("Camera stream ended or returned an invalid boundary")

    headers = {}
    for _ in range(16):
        line = await stream.readline()
        if line == b"\r\n":
            break
        key, separator, value = line.partition(b":")
        if not separator:
            raise CameraError("Invalid camera frame headers")
        headers[key.lower()] = value.strip()
    else:
        raise CameraError("Too many camera frame headers")

    try:
        size = int(headers[b"content-length"])
    except (KeyError, ValueError) as error:
        raise CameraError("Invalid camera frame length") from error
    if not 4 <= size <= MAX_JPEG_BYTES:
        raise CameraError("Camera frame exceeds the allowed size")
    jpeg = await stream.readexactly(size)
    if not jpeg.startswith(b"\xff\xd8") or not jpeg.endswith(b"\xff\xd9"):
        raise CameraError("Invalid JPEG frame")
    return jpeg


@dataclass(frozen=True, slots=True)
class FFmpegCamera:
    url: str
    rtsp_transport: str = "tcp"
    copy_mjpeg: bool = False
    frame_timeout: float = 15

    def __post_init__(self) -> None:
        try:
            url = urlsplit(self.url)
            if url.scheme not in {"rtsp", "http", "https"} or not url.hostname:
                raise ValueError
            if url.port is not None and url.port < 1:
                raise ValueError
        except ValueError:
            raise ValueError("Expected a valid rtsp://, http:// or https:// camera URL") from None
        if self.rtsp_transport not in {"tcp", "udp"}:
            raise ValueError("RTSP transport must be tcp or udp")
        if not 0 < self.frame_timeout <= 300:
            raise ValueError("Frame timeout must be greater than zero and at most 300 seconds")

    def command(self) -> list[str]:
        timeout = str(int(self.frame_timeout * 1_000_000))
        if urlsplit(self.url).scheme == "rtsp":
            input_options = ["-rtsp_transport", self.rtsp_transport, "-timeout", timeout]
        else:
            input_options = ["-rw_timeout", timeout]
        codec = ["-c:v", "copy"] if self.copy_mjpeg else ["-c:v", "mjpeg", "-q:v", "3", "-threads", "1"]
        return [
            "ffmpeg", "-hide_banner", "-nostdin", "-loglevel", "error",
            "-protocol_whitelist", "http,https,tls,tcp,udp,rtp",
            *input_options,
            "-i", self.url, "-map", "0:v:0", "-an", *codec,
            "-flush_packets", "1", "-f", "mpjpeg", "-boundary_tag",
            BOUNDARY[2:].decode("ascii"), "pipe:1",
        ]

    async def frames(self) -> AsyncGenerator[bytes, None]:
        try:
            process = await asyncio.create_subprocess_exec(
                *self.command(),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
            )
        except OSError as error:
            raise CameraError("Could not start FFmpeg") from error

        assert process.stdout is not None
        try:
            while True:
                yield await asyncio.wait_for(read_jpeg(process.stdout), self.frame_timeout)
        except TimeoutError as error:
            raise CameraError("Camera did not deliver a frame before the timeout") from error
        except (asyncio.IncompleteReadError, ValueError) as error:
            raise CameraError("Incomplete or invalid camera frame") from error
        finally:
            if process.returncode is None:
                try:
                    process.kill()
                except ProcessLookupError:
                    pass
            await process.communicate()
