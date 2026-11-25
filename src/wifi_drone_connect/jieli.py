"""JieLi CTP and JPEG camera transport from the documented HF/YL3 protocol."""

import asyncio
from collections.abc import AsyncGenerator, Iterator
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime
import json
import logging
import socket
from struct import Struct

from .core import CameraError

CTP_LENGTH = Struct("<H")
JSON_LENGTH = Struct("<I")
FRAGMENT = Struct("<BxHIII4x")
MAX_JPEG = 2 * 1024 * 1024
VIDEO_PORT = 2224
JPEG_PORT = 6666
LOGGER = logging.getLogger(__name__)


def ctp_message(topic: str, *, include_empty_param: bool = False, **parameters: str) -> bytes:
    topic_bytes = topic.encode("ascii")
    payload = {"op": "PUT"}
    if parameters or include_empty_param:
        payload["param"] = parameters
    body = json.dumps(payload, separators=(",", ":")).encode()
    return b"CTP:" + CTP_LENGTH.pack(len(topic_bytes)) + topic_bytes + JSON_LENGTH.pack(len(body)) + body


async def read_ctp(reader: asyncio.StreamReader) -> tuple[str, dict]:
    magic, length = await reader.readexactly(4), CTP_LENGTH.unpack(await reader.readexactly(2))[0]
    if magic != b"CTP:" or not 1 <= length <= 256:
        raise CameraError("Invalid JieLi camera control response")
    topic = (await reader.readexactly(length)).decode()
    size = JSON_LENGTH.unpack(await reader.readexactly(4))[0]
    if size > 64 * 1024:
        raise CameraError("JieLi camera control response exceeded the size limit")
    data = await reader.readexactly(size)
    try:
        payload = json.loads(data) if data else {}
    except ValueError:
        payload = {}
    parameters = payload.get("param") if isinstance(payload, dict) else None
    return topic, parameters if isinstance(parameters, dict) else {}


def complete_jpeg(data: bytes) -> bytes | None:
    if not data.startswith(b"\xff\xd8"):
        return None
    end = data.rfind(b"\xff\xd9")
    return data[:end + 2] if end >= 2 else None


def newer(counter: int, previous: int) -> bool:
    return 0 < (counter - previous) % (1 << 32) < 1 << 31


class Fragments:
    """Assemble bounded JPEGs from continuous fragments, including packed datagrams."""

    def __init__(self) -> None:
        self.frame = bytearray()
        self.total = 0
        self.last_counter: int | None = None

    def feed(self, datagram: bytes) -> Iterator[bytes]:
        cursor = 0
        while cursor + FRAGMENT.size <= len(datagram):
            kind, size, counter, total, offset = FRAGMENT.unpack_from(datagram, cursor)
            cursor += FRAGMENT.size
            end = cursor + size
            if end > len(datagram):
                return
            piece = datagram[cursor:end]
            cursor = end
            if kind & 0x7F != 2 or not size or not 4 <= total <= MAX_JPEG or offset + size > total:
                continue
            if self.last_counter is not None and newer(self.last_counter, counter):
                continue
            if offset == 0:
                self.frame.clear()
                self.total = total
            if offset != len(self.frame) or total != self.total:
                continue
            self.last_counter = counter
            self.frame.extend(piece)
            if len(self.frame) == total:
                jpeg = complete_jpeg(bytes(self.frame))
                self.frame.clear()
                self.total = 0
                if jpeg is not None:
                    yield jpeg


class VideoPackets(asyncio.DatagramProtocol):
    def __init__(self, host: str, images: asyncio.Queue, port: int):
        self.host = host
        self.images = images
        self.port = port
        self.fragments = Fragments() if port == VIDEO_PORT else None
        self.datagrams = 0
        self.decoded = 0
        self.rtp_payload_types: set[int] = set()

    def summary(self) -> str:
        payload_types = ",".join(str(value) for value in sorted(self.rtp_payload_types)) or "-"
        return (f"UDP {self.port}: {self.datagrams} packets, {self.decoded} JPEGs,"
                f" possible RTP payload types {payload_types}")

    def datagram_received(self, data: bytes, address) -> None:
        if address[0] != self.host:
            return
        self.datagrams += 1
        if self.port == JPEG_PORT and len(data) >= 12 and data[0] >> 6 == 2:
            self.rtp_payload_types.add(data[1] & 0x7F)
        frames = self.fragments.feed(data) if self.fragments else (complete_jpeg(data),)
        for jpeg in frames:
            if jpeg is not None:
                self.decoded += 1
                if self.decoded == 1:
                    LOGGER.info("JieLi received a complete JPEG on UDP %d", self.port)
                if self.images.full():
                    self.images.get_nowait()
                self.images.put_nowait(jpeg)


def number(parameters: dict, key: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(parameters.get(key, default))
    except (ValueError, TypeError):
        return default
    return value if minimum <= value <= maximum else default


@dataclass(frozen=True, slots=True)
class JieliCamera:
    host: str
    port: int = 3333
    frame_timeout: float = 15
    access_version: str = "25"

    async def frames(self) -> AsyncGenerator[bytes, None]:
        loop = asyncio.get_running_loop()
        images: asyncio.Queue[bytes] = asyncio.Queue(maxsize=1)
        listeners: list[tuple[asyncio.DatagramTransport, VideoPackets]] = []
        writer = None
        control = None
        try:
            async with asyncio.timeout(5):
                addresses = await loop.getaddrinfo(self.host, self.port, family=socket.AF_INET, type=socket.SOCK_STREAM)
                host = addresses[0][4][0]
                reader, writer = await asyncio.open_connection(host, self.port)
                local = writer.get_extra_info("sockname")[0]
                for port in (VIDEO_PORT, JPEG_PORT):
                    receiver = VideoPackets(host, images, port)
                    endpoint, _ = await loop.create_datagram_endpoint(
                        lambda: receiver, local_addr=(local, port),
                    )
                    listeners.append((endpoint, receiver))
            LOGGER.info("JieLi CTP connected on TCP %d, listening for video on UDP %d and %d",
                        self.port, VIDEO_PORT, JPEG_PORT)
            control = asyncio.create_task(self._control(reader, writer))
            while True:
                receive = asyncio.create_task(images.get())
                try:
                    done, _ = await asyncio.wait(
                        {receive, control}, timeout=self.frame_timeout, return_when=asyncio.FIRST_COMPLETED
                    )
                    if control in done:
                        control.result()
                    if receive not in done:
                        LOGGER.warning("JieLi video timeout. %s",
                                       " | ".join(receiver.summary() for _, receiver in listeners))
                        raise CameraError("JieLi camera did not deliver a supported JPEG frame")
                    yield receive.result()
                finally:
                    receive.cancel()
                    with suppress(asyncio.CancelledError):
                        await receive
        except (OSError, TimeoutError, UnicodeError, asyncio.IncompleteReadError) as error:
            LOGGER.warning("JieLi session ended during %s (%s)",
                           "video reception" if control is not None else "TCP or UDP setup",
                           type(error).__name__)
            raise CameraError("JieLi camera session disconnected or timed out") from error
        finally:
            if control is not None:
                control.cancel()
                await asyncio.gather(control, return_exceptions=True)
            if writer is not None:
                if control is not None:
                    with suppress(OSError, TimeoutError):
                        writer.write(ctp_message("CLOSE_RT_STREAM", status="1"))
                        await asyncio.wait_for(writer.drain(), 0.5)
                writer.close()
                with suppress(OSError, TimeoutError):
                    await asyncio.wait_for(writer.wait_closed(), 0.5)
            for listener, _ in listeners:
                listener.close()

    async def _control(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        loop = asyncio.get_running_loop()
        writer.write(ctp_message("APP_ACCESS", ver=self.access_version, type="0"))
        await asyncio.wait_for(writer.drain(), 2)
        keep_alive_request = ctp_message("CTP_KEEP_ALIVE", include_empty_param=self.access_version == "2")
        parameters = {}
        video_wait = loop.time() + 1.0
        keep_alive = 7.5
        next_send = video_wait
        opened = False
        pending = asyncio.create_task(read_ctp(reader))
        try:
            while True:
                now = loop.time()
                if not opened and (parameters or now >= video_wait):
                    writer.write(ctp_message("DATE_TIME", date=datetime.now().strftime("%Y%m%d%H%M%S")))
                    writer.write(keep_alive_request)
                    writer.write(ctp_message(
                        "OPEN_RT_STREAM", format="0",
                        h=str(number(parameters, "h", 480, 16, 4096)),
                        w=str(number(parameters, "w", 640, 16, 4096)),
                        fps=str(number(parameters, "fps", 25, 1, 120)),
                    ))
                    await asyncio.wait_for(writer.drain(), 2)
                    LOGGER.info("JieLi sent OPEN_RT_STREAM for JPEG")
                    opened = True
                    next_send = now + keep_alive
                elif opened and now >= next_send:
                    writer.write(keep_alive_request)
                    await asyncio.wait_for(writer.drain(), 2)
                    next_send = now + keep_alive
                done, _ = await asyncio.wait({pending}, timeout=max(0, next_send - loop.time()))
                if pending in done:
                    topic, values = pending.result()
                    if topic == "APP_ACCESS":
                        LOGGER.info("JieLi replied to APP_ACCESS (requested version %s)", self.access_version)
                    elif topic == "OPEN_RT_STREAM":
                        LOGGER.info("JieLi replied to OPEN_RT_STREAM (%d × %d at %d fps, format %d)",
                                    number(values, "w", 0, 1, 4096),
                                    number(values, "h", 0, 1, 4096),
                                    number(values, "fps", 0, 1, 120),
                                    number(values, "format", -1, 0, 255))
                    elif topic == "VIDEO_PARAM":
                        parameters = values
                    elif topic == "KEEP_ALIVE_INTERVAL":
                        interval_ms = number(values, "timeout", 30000, 1, 600000)
                        keep_alive = min(7.5, max(0.25, interval_ms / 4000))
                        if opened:
                            next_send = min(next_send, loop.time() + keep_alive)
                    pending = asyncio.create_task(read_ctp(reader))
        finally:
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
