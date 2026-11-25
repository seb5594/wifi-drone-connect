"""Receive the documented Lewei TCP camera stream and decode its H.264 video."""

import asyncio
from collections.abc import AsyncGenerator
from contextlib import aclosing, suppress
from dataclasses import dataclass
from struct import Struct

from .core import CameraError
from .ffmpeg import FFMPEG, JPEG_ENCODER, JPEG_OUTPUT, ffmpeg_frames

PREFIX = b"lewei_cmd\x00"
MESSAGE = Struct("<10s9I")
FRAME = Struct("<IIQ16x")
MAX_MESSAGE_BYTES = 4 * 1024 * 1024
HEARTBEAT, START, STOP, VIDEO = 1, 2, 3, 257
COUNTER_MASK = (1 << 63) - 1


def command(code: int, argument: int = 0) -> bytes:
    return MESSAGE.pack(PREFIX, code, argument, 0, 0, 0, 0, 0, 0, 0)


def inverted_byte_index(size: int, counter: int) -> int:
    value = (counter & COUNTER_MASK) ^ size
    value += size + (size % 2 == 0)
    return ((value ^ size) & COUNTER_MASK) % size


async def read_video(reader: asyncio.StreamReader) -> bytes:
    while True:
        prefix, code, _, _, size, stream_type, *_ = MESSAGE.unpack(
            await reader.readexactly(MESSAGE.size)
        )
        if prefix != PREFIX or size > MAX_MESSAGE_BYTES:
            raise CameraError("Invalid Lewei camera message header")
        if code != VIDEO:
            await reader.readexactly(size)
            continue
        if size <= FRAME.size:
            raise CameraError("Invalid Lewei video message length")
        _, length, counter = FRAME.unpack(await reader.readexactly(FRAME.size))
        if length != size - FRAME.size:
            raise CameraError("Inconsistent Lewei video frame length")
        data = await reader.readexactly(length)
        if stream_type == 0:
            return data
        if stream_type == 1:
            index = inverted_byte_index(length, counter)
            return data[:index] + bytes((data[index] ^ 0xFF,)) + data[index + 1:]
        raise CameraError(f"Unsupported Lewei video stream type {stream_type}")


async def send(writer: asyncio.StreamWriter, code: int, argument: int = 0) -> None:
    writer.write(command(code, argument))
    await asyncio.wait_for(writer.drain(), 1)


async def keep_alive(writer: asyncio.StreamWriter) -> None:
    while True:
        await asyncio.sleep(1)
        await send(writer, HEARTBEAT)


@dataclass(frozen=True, slots=True)
class LeweiCamera:
    host: str
    port: int = 7060

    async def frames(self) -> AsyncGenerator[bytes, None]:
        decoder = [
            *FFMPEG, "-f", "h264", "-probesize", "32768", "-analyzeduration", "100000",
            "-threads", "1", "-i", "pipe:0", "-map", "0:v:0", "-an",
            *JPEG_ENCODER, *JPEG_OUTPUT,
        ]
        async with aclosing(ffmpeg_frames(decoder, 15, self.encoded_frames())) as images:
            async for jpeg in images:
                yield jpeg

    async def encoded_frames(self) -> AsyncGenerator[bytes, None]:
        try:
            reader, writer = await asyncio.wait_for(asyncio.open_connection(self.host, self.port), 5)
        except (OSError, TimeoutError) as error:
            raise CameraError("Could not connect to the Lewei camera") from error
        heartbeat = None
        try:
            await send(writer, START, 1)
            heartbeat = asyncio.create_task(keep_alive(writer))
            while True:
                if heartbeat.done():
                    heartbeat.result()
                yield await asyncio.wait_for(read_video(reader), 5)
        except (OSError, TimeoutError, asyncio.IncompleteReadError) as error:
            raise CameraError("The Lewei camera disconnected or stopped sending video") from error
        finally:
            if heartbeat is not None:
                heartbeat.cancel()
                await asyncio.gather(heartbeat, return_exceptions=True)
            try:
                with suppress(OSError, TimeoutError):
                    await send(writer, STOP)
            finally:
                writer.close()
                with suppress(OSError, TimeoutError):
                    await asyncio.wait_for(writer.wait_closed(), 1)
