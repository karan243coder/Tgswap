"""Inline button layouts for the English-only bot control panel."""

from __future__ import annotations

from telethon import Button

# Keep payloads short, stable and strictly allow-listed in the callback router.
HOME = b"m:home"
AGREE = b"m:agree"
SOURCE = b"m:source"
TARGET = b"m:target"
STATUS = b"m:status"
QUALITY = b"m:quality"
CANCEL = b"m:cancel"
RESET = b"m:reset"
HELP = b"m:help"


def home_keyboard(*, consented: bool) -> list[list[Button]]:
    consent_label = "✅ Consent Confirmed" if consented else "✅ I Agree"
    consent_data = HOME if consented else AGREE
    return [
        [
            Button.inline(consent_label, consent_data),
            Button.inline("📖 How It Works", HELP),
        ],
        [
            Button.inline("📷 Set Source Image", SOURCE),
            Button.inline("🎬 Send Target Video", TARGET),
        ],
        [
            Button.inline("📊 My Status", STATUS),
            Button.inline("⚙️ Quality Details", QUALITY),
        ],
        [
            Button.inline("🛑 Cancel Render", CANCEL),
            Button.inline("🗑 Reset Session", RESET),
        ],
    ]


def back_keyboard() -> list[list[Button]]:
    return [[Button.inline("← Back to Control Panel", HOME)]]


def status_keyboard() -> list[list[Button]]:
    return [
        [
            Button.inline("🔄 Refresh Status", STATUS),
            Button.inline("🛑 Cancel Render", CANCEL),
        ],
        [Button.inline("← Back to Control Panel", HOME)],
    ]
