"""Device profiles and the public camera adapter interface."""

from collections.abc import AsyncGenerator
from dataclasses import dataclass
from importlib.resources import files
import json
from typing import Protocol


@dataclass(frozen=True, slots=True)
class DeviceProfile:
    id: str
    name: str
    ssid: str | None
    android_package: str | None
    verified_protocol: str | None
    ssid_prefix: str | None = None


def profiles() -> tuple[DeviceProfile, ...]:
    """Return bundled profiles. A matching SSID alone cannot establish compatibility."""
    directory = files("wifi_drone_connect").joinpath("profiles")
    return tuple(
        DeviceProfile(**json.loads(file.read_text(encoding="utf-8")))
        for file in sorted(directory.iterdir(), key=lambda item: item.name)
        if file.name.endswith(".json")
    )


def identify(ssid: str) -> tuple[DeviceProfile, ...]:
    """Match a supplied SSID locally, without transmitting on the network."""
    return tuple(
        profile for profile in profiles()
        if profile.ssid == ssid or (profile.ssid_prefix and ssid.startswith(profile.ssid_prefix))
    )


class CameraError(Exception):
    """The camera could not provide a frame."""


class CameraAdapter(Protocol):
    """An adapter produces complete JPEG frames and closes its source on exit."""

    def frames(self) -> AsyncGenerator[bytes, None]:
        """Yield JPEG frames until the source disconnects."""


def mjpeg_part(jpeg: bytes, boundary: str = "frame") -> bytes:
    """Wrap one JPEG for an HTTP multipart stream."""
    headers = (
        f"--{boundary}\r\nContent-Type: image/jpeg\r\n"
        f"Content-Length: {len(jpeg)}\r\n\r\n"
    ).encode("ascii")
    return headers + jpeg + b"\r\n"
