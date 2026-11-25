import asyncio
import unittest

from wifi_drone_connect import CameraError, FFmpegCamera, identify, mjpeg_part
from wifi_drone_connect.ffmpeg import MAX_JPEG_BYTES, read_jpeg


class CameraTests(unittest.IsolatedAsyncioTestCase):
    async def test_multipart_round_trip(self):
        jpeg = b"\xff\xd8camera\xff\xd9"
        stream = asyncio.StreamReader()
        stream.feed_data(mjpeg_part(jpeg, "wifi_drone_frame") * 2)
        stream.feed_eof()
        self.assertEqual(await read_jpeg(stream), jpeg)
        self.assertEqual(await read_jpeg(stream), jpeg)

    async def test_frame_size_limit(self):
        stream = asyncio.StreamReader()
        stream.feed_data(
            f"--wifi_drone_frame\r\nContent-Length: {MAX_JPEG_BYTES + 1}\r\n\r\n".encode()
        )
        with self.assertRaises(CameraError):
            await read_jpeg(stream)

    def test_profile_and_validation(self):
        self.assertIsNone(identify("SKY CAM DRONE")[0].verified_protocol)
        self.assertEqual(identify("another network"), ())
        for url in ("file:///tmp/video", "rtsp://", "http://camera:bad"):
            with self.subTest(url=url), self.assertRaises(ValueError):
                FFmpegCamera(url)

    def test_ffmpeg_uses_the_timeout_option_for_each_input_transport(self):
        rtsp = FFmpegCamera("rtsp://camera/live", rtsp_transport="udp").command()
        http = FFmpegCamera("http://camera/live").command()
        self.assertIn("-timeout", rtsp)
        self.assertNotIn("-rw_timeout", rtsp)
        self.assertEqual(rtsp[rtsp.index("-rtsp_transport") + 1], "udp")
        self.assertIn("-rw_timeout", http)
