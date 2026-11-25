"""A simulated Lewei camera checks framing, lifecycle and FFmpeg decoding."""

import asyncio
from contextlib import aclosing, suppress
from itertools import cycle
import re
import shutil
import subprocess
import unittest

from wifi_drone_connect import CameraError, camera_for_url
from wifi_drone_connect.lewei import FRAME, MESSAGE, PREFIX, VIDEO, read_video


def video_message(data, counter, stream_type):
    payload = FRAME.pack(1, len(data), counter) + data
    header = MESSAGE.pack(PREFIX, VIDEO, 0, 0, len(payload), stream_type, 0, 0, 0, 0)
    return header + payload


class LeweiTests(unittest.IsolatedAsyncioTestCase):
    async def test_type_one_restores_the_documented_byte(self):
        # With a 12-byte frame and a time counter of 1000, byte 1 is inverted.
        frame = b"\0\0\0\1\x65fixture"
        damaged = frame[:1] + bytes([frame[1] ^ 0xFF]) + frame[2:]
        reader = asyncio.StreamReader()
        reader.feed_data(video_message(damaged, 1000, 1))
        self.assertEqual(await read_video(reader), frame)

    async def test_reports_an_unknown_frame_encoding(self):
        reader = asyncio.StreamReader()
        reader.feed_data(video_message(b"\0\0\0\1fixture", 100, 129))
        with self.assertRaisesRegex(CameraError, "Unsupported Lewei video stream type 129"):
            await read_video(reader)

    @unittest.skipUnless(shutil.which("ffmpeg"), "FFmpeg is required for the video fixture")
    async def test_live_stream_decodes_and_sends_camera_control_only(self):
        generated = await asyncio.to_thread(
            subprocess.run,
            ["ffmpeg", "-v", "error", "-f", "lavfi", "-i", "testsrc2=size=96x64:rate=12",
             "-frames:v", "12", "-c:v", "libx264", "-threads", "1", "-g", "12",
             "-preset", "ultrafast", "-tune", "zerolatency", "-x264-params", "aud=1",
             "-f", "h264", "pipe:1"],
            check=True, capture_output=True, timeout=5,
        )
        frames = [packet for packet in re.split(rb"(?=\x00\x00\x00\x01\x09)", generated.stdout) if packet]
        self.assertGreater(len(frames), 1)
        seen = asyncio.Queue()

        async def serve(reader, writer):
            async def produce():
                for number, packet in enumerate(cycle(frames)):
                    writer.write(video_message(packet, number * 83, 0))
                    await writer.drain()
                    await asyncio.sleep(0.05)

            producer = None
            try:
                while True:
                    prefix, code, argument, _, size, *_ = MESSAGE.unpack(
                        await reader.readexactly(MESSAGE.size)
                    )
                    seen.put_nowait((prefix, code, argument, size))
                    if code == 2:
                        producer = asyncio.create_task(produce())
                    if code == 3:
                        return
            finally:
                if producer is not None:
                    producer.cancel()
                    with suppress(asyncio.CancelledError, ConnectionError):
                        await producer
                writer.close()
                await writer.wait_closed()

        async with await asyncio.start_server(serve, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            camera = camera_for_url(f"lewei://127.0.0.1:{port}")
            async with aclosing(camera.frames()) as output:
                jpeg = await asyncio.wait_for(anext(output), 7)
                self.assertTrue(jpeg.startswith(b"\xff\xd8") and jpeg.endswith(b"\xff\xd9"))
                self.assertEqual(await asyncio.wait_for(seen.get(), 2), (PREFIX, 2, 1, 0))
                self.assertEqual(await asyncio.wait_for(seen.get(), 2), (PREFIX, 1, 0, 0))
            packet = await asyncio.wait_for(seen.get(), 2)
            while packet[1] == 1:
                packet = await asyncio.wait_for(seen.get(), 2)
            self.assertEqual(packet, (PREFIX, 3, 0, 0))
