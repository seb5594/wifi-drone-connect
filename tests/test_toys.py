"""Exercise the toy protocol adapters with synthetic camera endpoints."""

import asyncio
from contextlib import aclosing
import socket
import unittest

from wifi_drone_connect import CameraError, camera_for_url
from wifi_drone_connect.jieli import FRAGMENT, Fragments, ctp_message, read_ctp
from wifi_drone_connect.toys import KYUFOCamera

JPEG = b"\xff\xd8synthetic-jpeg\xff\xd9"


def part(offset, size, sequence, kind=2):
    return FRAGMENT.pack(kind, size, sequence, len(JPEG), offset) + JPEG[offset:offset + size]


class ToyTests(unittest.IsolatedAsyncioTestCase):
    def test_dispatch_and_rotation(self):
        camera = camera_for_url("kyufo://192.168.1.1")
        self.assertEqual(camera.video.url, "rtsp://192.168.1.1:7070/webcam")
        self.assertIn("transpose=clock", camera.video.command())
        upright = camera_for_url("kyufo://192.168.1.1?rotate=0", copy_mjpeg=True)
        self.assertTrue(upright.video.copy_mjpeg)
        for bad_url in ("kyufo://host?rotate=45", "jieli://host?format=1", "jieli://host:0"):
            with self.subTest(bad_url=bad_url), self.assertRaises(ValueError):
                camera_for_url(bad_url)

    def test_fragmented_jpeg_handles_multiple_pieces_and_drops_holes(self):
        fragments = Fragments()
        self.assertEqual(list(fragments.feed(part(0, 3, 7))), [])
        self.assertEqual(list(fragments.feed(part(5, len(JPEG) - 5, 8, 130))), [])
        full = part(0, 5, 9) + part(5, len(JPEG) - 5, 10, 130)
        self.assertEqual(list(fragments.feed(full)), [JPEG])

    async def test_ky_heartbeat_is_camera_only_and_closes_with_source(self):
        class Video:
            async def frames(self):
                yield JPEG
                raise CameraError("source ended")

        loop = asyncio.get_running_loop()
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as listener:
            listener.bind(("127.0.0.1", 7099))
            listener.setblocking(False)
            async with aclosing(KYUFOCamera("127.0.0.1", Video()).frames()) as frames:
                self.assertEqual(await anext(frames), JPEG)
                for _ in range(2):
                    data, _ = await asyncio.wait_for(loop.sock_recvfrom(listener, 8), 2)
                    self.assertEqual(data, b"\x01\x01")
                with self.assertRaisesRegex(CameraError, "source ended"):
                    await anext(frames)

    async def test_jieli_session_reads_camera_parameters_and_returns_fragmented_jpeg(self):
        received = asyncio.get_running_loop().create_future()

        async def drone(reader, writer):
            messages = []
            try:
                messages.append(await read_ctp(reader))
                writer.write(ctp_message("VIDEO_PARAM", w="320", h="240", fps="20"))
                await writer.drain()
                for _ in range(3):
                    messages.append(await read_ctp(reader))
                with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as video:
                    video.sendto(part(0, 5, 1) + part(5, len(JPEG) - 5, 2, 130), ("127.0.0.1", 2224))
                messages.append(await read_ctp(reader))
                received.set_result(messages)
            except Exception as error:
                received.set_exception(error)
            finally:
                writer.close()
                await writer.wait_closed()

        async with await asyncio.start_server(drone, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            camera = camera_for_url(f"jieli://127.0.0.1:{port}?ver=2")
            async with aclosing(camera.frames()) as frames:
                self.assertEqual(await asyncio.wait_for(anext(frames), 3), JPEG)
            messages = await asyncio.wait_for(received, 3)
        self.assertEqual([message[0] for message in messages], [
            "APP_ACCESS", "DATE_TIME", "CTP_KEEP_ALIVE", "OPEN_RT_STREAM", "CLOSE_RT_STREAM",
        ])
        self.assertEqual(messages[0][1], {"ver": "2", "type": "0"})
        self.assertTrue(ctp_message("CTP_KEEP_ALIVE", include_empty_param=True).endswith(b'{"op":"PUT","param":{}}'))
        self.assertEqual(camera_for_url("jieli://127.0.0.1").access_version, "25")
        self.assertEqual(messages[3][1], {"format": "0", "w": "320", "h": "240", "fps": "20"})
