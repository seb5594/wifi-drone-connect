"""Read RTSP or HTTP video via FFmpeg and yield bounded JPEG frames."""

import asyncio
from collections.abc import AsyncGenerator
from contextlib import aclosing, suppress
from dataclasses import dataclass
from urllib.parse import urlsplit

from .core import CameraError

MAX_JPEG_BYTES = 8 * 1024 * 1024
BOUNDARY = b"--wifi_drone_frame"
FFMPEG = ("ffmpeg", "-hide_banner", "-nostdin", "-loglevel", "error")
JPEG_OUTPUT = ("-flush_packets", "1", "-f", "mpjpeg", "-boundary_tag",
               BOUNDARY[2:].decode("ascii"), "pipe:1")
JPEG_ENCODER = ("-c:v", "mjpeg", "-q:v", "3", "-threads", "1")
ROTATION_FILTERS = {90: "transpose=cclock", 180: "hflip,vflip", 270: "transpose=clock"}


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
    rotate: int = 0

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

        if self.rotate not in {0, *ROTATION_FILTERS}:
            raise ValueError("Camera rotation must be 0, 90, 180 or 270 degrees")
        if self.copy_mjpeg and self.rotate:
            raise ValueError("Choose rotate=0 when preserving the original MJPEG frames")

    def command(self) -> list[str]:
        timeout = str(int(self.frame_timeout * 1_000_000))
        if urlsplit(self.url).scheme == "rtsp":
            input_options = ["-rtsp_transport", self.rtsp_transport, "-timeout", timeout]
        else:
            input_options = ["-rw_timeout", timeout]
        codec = ["-c:v", "copy"] if self.copy_mjpeg else JPEG_ENCODER
        filters = ["-vf", ROTATION_FILTERS[self.rotate]] if self.rotate else []
        return [
            *FFMPEG,
            "-protocol_whitelist", "http,https,tls,tcp,udp,rtp",
            *input_options,
            "-i", self.url, "-map", "0:v:0", "-an", *filters, *codec,
            *JPEG_OUTPUT,
        ]

    async def frames(self) -> AsyncGenerator[bytes, None]:
        async with aclosing(ffmpeg_frames(self.command(), self.frame_timeout)) as frames:
            async for frame in frames:
                yield frame


async def feed_ffmpeg(writer: asyncio.StreamWriter, source: AsyncGenerator[bytes, None]) -> None:
    try:
        async with aclosing(source) as chunks:
            async for chunk in chunks:
                writer.write(chunk)
                await writer.drain()
    finally:
        writer.close()


async def ffmpeg_frames(
    command: list[str], frame_timeout: float,
    source: AsyncGenerator[bytes, None] | None = None,
) -> AsyncGenerator[bytes, None]:
    """Read FFmpeg's JPEG output, optionally supplying compressed video through stdin."""
    try:
        process = await asyncio.create_subprocess_exec(
            *command,
            stdin=asyncio.subprocess.PIPE if source is not None else None,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
    except OSError as error:
        raise CameraError("Could not start FFmpeg") from error

    assert process.stdout is not None
    feeder = None
    if source is not None:
        assert process.stdin is not None
        feeder = asyncio.create_task(feed_ffmpeg(process.stdin, source))
    try:
        while True:
            yield await asyncio.wait_for(read_jpeg(process.stdout), frame_timeout)
    except (TimeoutError, CameraError, asyncio.IncompleteReadError, ValueError) as error:
        if feeder is not None and feeder.done() and not feeder.cancelled():
            with suppress(OSError):
                feeder.result()
        if isinstance(error, TimeoutError):
            raise CameraError("Camera did not deliver a frame before the timeout") from error
        if isinstance(error, CameraError):
            raise
        raise CameraError("Incomplete or invalid camera frame") from error
    finally:
        if feeder is not None:
            feeder.cancel()
            await asyncio.gather(feeder, return_exceptions=True)
        if process.returncode is None:
            with suppress(ProcessLookupError):
                process.kill()
        await process.communicate()
