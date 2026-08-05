# Third-party components and model notice

This repository contains the MTProto/Telegram/FastAPI wrapper code only. It does
**not** bundle FaceFusion model files or claim ownership of third-party models.

| Component | Used for | Source / terms |
| --- | --- | --- |
| FaceFusion 3.8.0 CPU/CUDA container | Headless frame-by-frame face-swap inference | [facefusion/facefusion-docker](https://github.com/facefusion/facefusion-docker), [FaceFusion license](https://github.com/facefusion/facefusion/blob/master/LICENSE.md) |
| FaceFusion project | Inference CLI and model-asset downloader | [facefusion/facefusion](https://github.com/facefusion/facefusion) |
| Telethon 1.44 | MTProto bot updates and large-file transfer callbacks | [Telethon](https://github.com/LonamiWebs/Telethon), [Telethon documentation](https://docs.telethon.dev/) |
| Telegram MTProto API | Bot identity, direct media download/upload | [Telegram API](https://core.telegram.org/api) |
| FFmpeg | Media inspection, optional normalization, output label | [FFmpeg legal information](https://ffmpeg.org/legal.html) |

## Telegram API credentials

`TELEGRAM_API_ID` and `TELEGRAM_API_HASH` are obtained by the operator from
[my.telegram.org/apps](https://my.telegram.org/apps). They must be stored as
Koyeb secrets. This repository never needs, requests, or stores a personal
Telegram phone-number login/session: it authenticates the **bot account** using
the BotFather token plus the operator's developer application credentials.

## Important model/licensing note

FaceFusion downloads processor/common model assets on the first job. Those
assets can have terms separate from this wrapper and separate from FaceFusion's
source license. Before commercial operation or redistribution, review current
FaceFusion documentation and the terms for every model it downloads. The default
image purposely does not bake model files into this repository.

## Responsible operation

Only process media you own or have explicit permission to edit. Do not process
minors, non-consensual intimate media, or material intended to deceive,
harass, impersonate, or defraud people. The wrapper requires an in-bot consent
acknowledgement, defaults to private chats, expires source images, labels output
`AI face swap` by default, and retains a user cancellation path.
