# WiFi Drone Connect

Asynchronous Python SDK and CLI for reading the camera of a WiFi drone. It accepts explicit RTSP (RTP over TCP or UDP) and HTTP(S) video sources via FFmpeg, with JPEG snapshots and multipart MJPEG output for downstream applications.

## Install and use

Requires Python 3.11 or later and FFmpeg available on PATH.

```sh
python -m pip install .
wifi-drone-connect profiles --ssid 'SKY CAM DRONE'
wifi-drone-connect snapshot 'rtsp://camera.local/live' snapshot.jpg
```

Use `--rtsp-transport udp` when the RTSP server requires UDP. Use `--copy-mjpeg` for a source already encoded as MJPEG to preserve its JPEG bytes. The default converts video frames to JPEG without resizing.

```python
from contextlib import aclosing

from wifi_drone_connect import FFmpegCamera

async def read_snapshot(url: str) -> bytes:
    async with aclosing(FFmpegCamera(url).frames()) as frames:
        return await anext(frames)
```

## Device compatibility

| Source | Support |
| --- | --- |
| RTSP with RTP over TCP or UDP | Generic protocol adapter |
| HTTP(S) video or MJPEG endpoint | Generic protocol adapter |
| Gear2Play Sky Cam (`SKY CAM DRONE`, app `com.rcfpv.rxdrone`) | Profile identified by SSID. The unit's stream endpoint and protocol still need verification. |
| JieLi CTP, Lewei, WFNet, FH / Fullhan | Candidate adapter families. No compatibility claim. |

The Sky Cam profile records the SSID and Android package from the [project research](https://chatgpt.com/share/6b0735f3-9cb4-83ed-872d-bd84fff09078). Static APK findings indicate several OEM families within the same app. To identify the camera transport, record the local gateway and capture the traffic when opening the camera preview on your own unit. Record the transport, endpoints and framing details, keeping device identifiers out of public fixtures.

`CameraAdapter` lets additional protocols deliver complete JPEG frames through the same interface. The bridge is in [Home-Assistant-WiFi-Drone-App](https://github.com/seb5594/Home-Assistant-WiFi-Drone-App) and the Home Assistant integration is in [hass-wifi_drone-integration](https://github.com/seb5594/hass-wifi_drone-integration).

MIT licensed.
