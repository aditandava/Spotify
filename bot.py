import os
import asyncio
import re
import time
from pathlib import Path
from collections import defaultdict
from typing import Optional
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, Message
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, filters, ContextTypes
)
from telegram.error import TelegramError, RetryAfter, TimedOut, NetworkError
from telegram.constants import ParseMode

TOKEN                 = os.environ["BOT_TOKEN"]
SPOTIFY_CLIENT_ID     = os.environ.get("SPOTIFY_CLIENT_ID", "")
SPOTIFY_CLIENT_SECRET = os.environ.get("SPOTIFY_CLIENT_SECRET", "")
ALLOWED_USERS         = set(os.environ.get("ALLOWED_USERS", "").split(",")) - {""}
MAX_FILE_MB           = 49
DOWNLOAD_TIMEOUT      = 600
RETRY_ATTEMPTS        = 3
COOLDOWN_SECONDS      = 10

DOWNLOAD_DIR = Path("downloads")
DOWNLOAD_DIR.mkdir(exist_ok=True)

SPOTIFY_RE = re.compile(
    r"https?://open\.spotify\.com/(track|album|playlist|artist)/[A-Za-z0-9?=&_-]+"
)

FORMATS = {
    "mp3_320": ("mp3",  "320k", "🎵 MP3 320kbps"),
    "mp3_128": ("mp3",  "128k", "🎵 MP3 128kbps"),
    "flac":    ("flac", "flac", "🎼 FLAC (lossless)"),
    "opus":    ("opus", "160k", "🔊 Opus 160kbps"),
}

active_downloads: set[int]          = set()
last_request:     dict[int, float]  = defaultdict(float)

# ── Keyboards ──────────────────────────────────────────────────────────────────

def main_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📖 How to Use", callback_data="menu:help"),
            InlineKeyboardButton("🎵 Formats",    callback_data="menu:formats"),
        ],
        [
            InlineKeyboardButton("❓ FAQ",         callback_data="menu:faq"),
            InlineKeyboardButton("ℹ️ About",       callback_data="menu:about"),
        ],
    ])

def back_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("« Back", callback_data="menu:main")],
    ])

def format_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🎵 MP3 320kbps",  callback_data="fmt:mp3_320"),
            InlineKeyboardButton("🎼 FLAC",         callback_data="fmt:flac"),
        ],
        [
            InlineKeyboardButton("🔊 Opus 160kbps", callback_data="fmt:opus"),
            InlineKeyboardButton("📦 MP3 128kbps",  callback_data="fmt:mp3_128"),
        ],
        [
            InlineKeyboardButton("❌ Cancel",        callback_data="fmt:cancel"),
        ],
    ])

# ── Helpers ────────────────────────────────────────────────────────────────────

def user_dir(uid: int) -> Path:
    d = DOWNLOAD_DIR / str(uid)
    d.mkdir(parents=True, exist_ok=True)
    return d

def cleanup_dir(path: Path):
    for f in path.glob("*"):
        f.unlink(missing_ok=True)

def detect_type(url: str) -> str:
    for t in ("track", "album", "playlist", "artist"):
        if f"/{t}/" in url:
            return t
    return "link"

def clean_url(url: str) -> str:
    return url.split("?")[0]

def is_allowed(uid: int) -> bool:
    return not ALLOWED_USERS or str(uid) in ALLOWED_USERS

def cooldown_left(uid: int) -> float:
    return max(0.0, COOLDOWN_SECONDS - (time.time() - last_request[uid]))

async def safe_edit(msg: Message, text: str, reply_markup=None):
    for attempt in range(3):
        try:
            await msg.edit_text(
                text,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=reply_markup,
            )
            return
        except RetryAfter as e:
            await asyncio.sleep(e.retry_after + 1)
        except (TimedOut, NetworkError):
            await asyncio.sleep(2 ** attempt)
        except TelegramError as e:
            if "not modified" in str(e).lower():
                return
            return

async def safe_reply(msg: Message, text: str, reply_markup=None) -> Optional[Message]:
    for attempt in range(3):
        try:
            return await msg.reply_text(
                text,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=reply_markup,
            )
        except RetryAfter as e:
            await asyncio.sleep(e.retry_after + 1)
        except (TimedOut, NetworkError):
            await asyncio.sleep(2 ** attempt)
        except TelegramError:
            return None

async def send_audio_with_retry(msg: Message, fp: Path) -> bool:
    for attempt in range(RETRY_ATTEMPTS):
        try:
            with open(fp, "rb") as f:
                await msg.reply_audio(
                    audio=f,
                    filename=fp.name,
                    title=fp.stem[:64],
                    read_timeout=120,
                    write_timeout=120,
                    connect_timeout=30,
                )
            return True
        except RetryAfter as e:
            await asyncio.sleep(e.retry_after + 2)
        except (TimedOut, NetworkError):
            await asyncio.sleep(3 * (attempt + 1))
        except TelegramError:
            return False
    return False

# ── spotdl ─────────────────────────────────────────────────────────────────────

async def run_spotdl(url: str, fmt: str, bitrate: str, out_dir: Path, status_msg: Message) -> tuple[bool, str]:
    args = [
        "spotdl", "download", url,
        "--output",    str(out_dir / "{artists} - {title}"),
        "--format",    fmt,
        "--threads",   "2",
        "--log-level", "ERROR",
    ]
    if fmt != "flac":
        args += ["--bitrate", bitrate]
    if SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET:
        args += ["--client-id", SPOTIFY_CLIENT_ID, "--client-secret", SPOTIFY_CLIENT_SECRET]

    link_type = detect_type(url)

    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            proc = await asyncio.create_subprocess_exec(
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )

            lines: list[str] = []
            start = time.time()

            async def reader():
                async for line in proc.stdout:
                    decoded = line.decode(errors="ignore").rstrip()
                    if decoded:
                        lines.append(decoded)

            read_task = asyncio.create_task(reader())

            async def updater():
                dots = 0
                while not read_task.done():
                    await asyncio.sleep(15)
                    dots = (dots % 3) + 1
                    elapsed = int(time.time() - start)
                    await safe_edit(
                        status_msg,
                        f"⏳ Downloading *{link_type}* as *{fmt.upper()}*{'.' * dots}\n"
                        f"⏱ `{elapsed}s elapsed`"
                        + (f"\n_Retry {attempt}/{RETRY_ATTEMPTS}_" if attempt > 1 else ""),
                    )

            upd_task = asyncio.create_task(updater())

            try:
                await asyncio.wait_for(
                    asyncio.gather(read_task, proc.wait()),
                    timeout=DOWNLOAD_TIMEOUT,
                )
            finally:
                upd_task.cancel()
                try:
                    await upd_task
                except asyncio.CancelledError:
                    pass

            log = "\n".join(lines)
            if proc.returncode == 0:
                return True, log

            if attempt < RETRY_ATTEMPTS:
                await safe_edit(status_msg, f"⚠️ Attempt {attempt} failed, retrying…")
                await asyncio.sleep(5)

        except asyncio.TimeoutError:
            try:
                proc.kill()
            except Exception:
                pass
            if attempt < RETRY_ATTEMPTS:
                await safe_edit(status_msg, f"⏰ Timed out, retrying… ({attempt+1}/{RETRY_ATTEMPTS})")
                await asyncio.sleep(5)
            else:
                return False, "Timed out"

        except Exception as e:
            if attempt >= RETRY_ATTEMPTS:
                return False, str(e)
            await asyncio.sleep(5)

    return False, "\n".join(lines) if lines else "Unknown error"

# ── /start ─────────────────────────────────────────────────────────────────────

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update.effective_user.id):
        return
    await update.message.reply_text(
        "🎧 *Spotify Audio Downloader*\n\n"
        "Send me any Spotify link to download it in high quality.\n\n"
        "_Supports tracks, albums & playlists._",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=main_menu_kb(),
    )

# ── Menu callbacks ─────────────────────────────────────────────────────────────

MENU_PAGES = {
    "main": (
        "🎧 *Spotify Audio Downloader*\n\n"
        "Send me any Spotify link to download it in high quality.\n\n"
        "_Supports tracks, albums & playlists._",
        lambda: main_menu_kb()
    ),
    "help": (
        "📖 *How to Use*\n\n"
        "1️⃣ Copy any link from the Spotify app\n"
        "2️⃣ Paste it here in the chat\n"
        "3️⃣ Choose your preferred audio format\n"
        "4️⃣ Wait for the file — done!\n\n"
        "_Large playlists may take a few minutes._",
        lambda: back_kb()
    ),
    "formats": (
        "🎵 *Available Formats*\n\n"
        "• *MP3 320kbps* — Best lossy quality, widely compatible\n"
        "• *FLAC* — Lossless, perfect quality, larger files\n"
        "• *Opus 160kbps* — Great quality, smallest size\n"
        "• *MP3 128kbps* — Smaller files, decent quality\n\n"
        "_All files include embedded metadata & album art._",
        lambda: back_kb()
    ),
    "faq": (
        "❓ *FAQ*\n\n"
        "*Does it need a Spotify account?*\n"
        "No — public tracks work without login.\n\n"
        "*Why did my download fail?*\n"
        "Track may be region-locked or unavailable on YouTube Music.\n\n"
        "*What's the file size limit?*\n"
        "Telegram allows up to 50 MB per file.\n\n"
        "*Can I download private playlists?*\n"
        "No — only public Spotify content is supported.",
        lambda: back_kb()
    ),
    "about": (
        "ℹ️ *About*\n\n"
        "This bot uses *spotdl* to match Spotify tracks on YouTube Music "
        "and download them with full metadata.\n\n"
        "• Audio matched via YouTube Music\n"
        "• Metadata fetched from Spotify\n"
        "• Re-encoded with FFmpeg\n\n"
        "_For personal use only. Respect copyright laws._",
        lambda: back_kb()
    ),
}

async def handle_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    page = query.data.split(":")[1]
    if page not in MENU_PAGES:
        return

    text, kb_fn = MENU_PAGES[page]
    await safe_edit(query.message, text, reply_markup=kb_fn())

# ── URL handler ────────────────────────────────────────────────────────────────

async def handle_url(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id

    if not is_allowed(uid):
        await update.message.reply_text("🔒 Unauthorized.")
        return

    if uid in active_downloads:
        await update.message.reply_text(
            "⏳ You have an active download running.\n"
            "Wait for it to finish or send /cancel."
        )
        return

    left = cooldown_left(uid)
    if left > 0:
        await update.message.reply_text(f"⏱ Wait *{left:.0f}s* before next request.", parse_mode=ParseMode.MARKDOWN)
        return

    match = SPOTIFY_RE.search(update.message.text.strip())
    if not match:
        await update.message.reply_text(
            "❌ *Invalid link*\n\nSend a valid Spotify URL:\n"
            "`https://open.spotify.com/track/...`",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    url = clean_url(match.group(0))
    ctx.user_data["pending_url"] = url
    last_request[uid] = time.time()

    await update.message.reply_text(
        f"🔗 Detected: *{detect_type(url).capitalize()}*\n\nChoose audio format:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=format_kb(),
    )

# ── Format choice ──────────────────────────────────────────────────────────────

async def handle_format(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    uid   = query.from_user.id
    await query.answer()

    choice = query.data.split(":")[1]

    if choice == "cancel":
        ctx.user_data.pop("pending_url", None)
        await safe_edit(query.message, "❌ Cancelled.")
        return

    if choice not in FORMATS:
        await safe_edit(query.message, "❌ Unknown format. Send the link again.")
        return

    fmt, bitrate, label = FORMATS[choice]
    url = ctx.user_data.pop("pending_url", None)

    if not url:
        await safe_edit(query.message, "❌ Session expired. Send the link again.")
        return

    if uid in active_downloads:
        await safe_edit(query.message, "⏳ Already downloading. Wait or send /cancel.")
        return

    out_dir = user_dir(uid)
    cleanup_dir(out_dir)
    active_downloads.add(uid)

    status_msg = await safe_edit(
        query.message,
        f"⏳ Starting *{detect_type(url).capitalize()}* download as *{label}*…",
    ) or query.message

    try:
        success, log = await run_spotdl(url, fmt, bitrate, out_dir, query.message)
    except Exception:
        await safe_edit(query.message, "❌ Unexpected error. Try again.")
        active_downloads.discard(uid)
        cleanup_dir(out_dir)
        return
    finally:
        active_downloads.discard(uid)

    files = sorted(out_dir.glob(f"*.{fmt}"))
    if not files:
        files = sorted(
            f for f in out_dir.glob("*")
            if f.suffix.lstrip(".") in ("mp3", "flac", "opus", "m4a", "ogg")
        )

    if not files:
        hint = (
            "_Rate limited — add SPOTIFY_CLIENT_ID secret._" if "429" in log or "rate" in log.lower()
            else "_Track unavailable or region-locked._" if "unavailable" in log.lower()
            else "_Try a single track link instead._"
        )
        await safe_edit(query.message, f"❌ *Download failed*\n{hint}")
        cleanup_dir(out_dir)
        return

    total = len(files)
    await safe_edit(query.message, f"📤 Uploading *{total}* file(s)…")

    sent = skipped = failed = 0

    for i, fp in enumerate(files, 1):
        if fp.stat().st_size / (1024 * 1024) > MAX_FILE_MB:
            await safe_reply(query.message, f"⚠️ `{fp.name[:50]}` too large, skipping.")
            skipped += 1
            fp.unlink(missing_ok=True)
            continue

        if total > 1:
            await safe_edit(query.message, f"📤 Uploading *{i}/{total}*…")

        ok = await send_audio_with_retry(query.message, fp)
        sent += ok
        failed += not ok
        fp.unlink(missing_ok=True)

        if i < total:
            await asyncio.sleep(1.5)

    summary = f"✅ *Done!* Sent *{sent}/{total}* as *{label}*"
    if skipped:
        summary += f"\n⚠️ {skipped} skipped (too large)"
    if failed:
        summary += f"\n❌ {failed} failed to upload"

    await safe_edit(query.message, summary)
    cleanup_dir(out_dir)

# ── /cancel ────────────────────────────────────────────────────────────────────

async def cmd_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid in active_downloads:
        active_downloads.discard(uid)
        ctx.user_data.pop("pending_url", None)
        await update.message.reply_text("🛑 Download cancelled.")
    else:
        await update.message.reply_text("ℹ️ No active download.")

# ── Error handler ──────────────────────────────────────────────────────────────

async def error_handler(update: object, ctx: ContextTypes.DEFAULT_TYPE):
    if isinstance(update, Update) and update.effective_message:
        try:
            await update.effective_message.reply_text("⚠️ Something went wrong. Try again.")
        except Exception:
            pass

# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    app = (
        Application.builder()
        .token(TOKEN)
        .read_timeout(30)
        .write_timeout(120)
        .connect_timeout(30)
        .pool_timeout(30)
        .build()
    )

    app.add_handler(CommandHandler("start",  cmd_start))
    app.add_handler(CommandHandler("cancel", cmd_cancel))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_url))
    app.add_handler(CallbackQueryHandler(handle_menu,   pattern=r"^menu:"))
    app.add_handler(CallbackQueryHandler(handle_format, pattern=r"^fmt:"))
    app.add_error_handler(error_handler)

    app.run_polling(drop_pending_updates=True, allowed_updates=["message", "callback_query"])

if __name__ == "__main__":
    main()
