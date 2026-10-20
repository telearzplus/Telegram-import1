import asyncio
import csv
import io
import logging
import math
import os
import random
import re
import sqlite3
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from telethon import Button, TelegramClient, errors, events
from telethon.sessions import StringSession
from telethon.tl.functions.channels import InviteToChannelRequest
from telethon.tl.functions.contacts import SearchRequest
from telethon.tl.types import (
    Channel,
    InputPeerChannel,
    InputPeerUser,
    User,
    UserStatusEmpty,
    UserStatusLastMonth,
)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("adder")


# ------------------------------------------------------------------ config


def env(name, required=True):
    """مقدار متغیر را می‌خواند و فاصله، خط جدید و کوتیشن‌های اضافه را پاک می‌کند."""
    v = re.sub(r"\s+", "", os.environ.get(name, "")).strip("'\"")
    if v.upper().startswith(name + "="):  # اگر کل «NAME=value» را به‌جای value چسبانده باشند
        v = v[len(name) + 1:].strip("'\"")
    if required and not v:
        raise SystemExit(f"متغیر {name} در Variables تنظیم نشده یا خالی است.")
    return v


def env_int(name):
    v = env(name)
    try:
        return int(v)
    except ValueError:
        raise SystemExit(f"متغیر {name} باید فقط عدد باشد.")


def make_session(s):
    try:
        return StringSession(s)
    except ValueError:
        raise SystemExit(
            f"SESSION_STRING نامعتبر است (طول فعلی: {len(s)}؛ مقدار درست معمولاً حدود ۳۵۳ کاراکتر است و با 1 شروع می‌شود). "
            "احتمالاً ناقص کپی شده یا چیز دیگری (مثل API_HASH یا کد تلگرام) گذاشته‌ای. "
            "با gen_session.py دوباره بساز و کامل کپی کن."
        )


API_ID = env_int("API_ID")
API_HASH = env("API_HASH")
SESSION_STRING = env("SESSION_STRING", required=False)
BOT_TOKEN = env("BOT_TOKEN")
ADMIN_ID = env_int("ADMIN_ID")
DATA_DIR = os.environ.get("DATA_DIR", "/data")
DAILY_LIMIT = int(os.environ.get("DAILY_LIMIT", "40"))  # پیش‌فرض اد موفق در روز (از منو هم قابل تغییر)
MIN_DELAY = int(os.environ.get("MIN_DELAY", "40"))  # ثانیه بین هر تلاش
MAX_DELAY = int(os.environ.get("MAX_DELAY", "90"))
AUTO_HOURS = float(os.environ.get("AUTO_COLLECT_HOURS", "6"))
MESSAGE_SCAN = int(os.environ.get("MESSAGE_SCAN", "500"))  # چند پیام آخر هر گروه برای پیدا کردن کاربر فعال
SENDERS_ENV = os.environ.get("SENDERS", "")  # مثلاً: mozhdeh,@other
ATTEMPTS_FACTOR = 3  # حداکثر تلاش در روز = سقف اد × این عدد (شامل ناموفق‌ها)
GROUP_PAUSE = 3  # مکث بین گروه‌ها هنگام جمع‌آوری (ثانیه)
TZ_OFFSET = float(os.environ.get("TZ_OFFSET", "3.5"))  # اختلاف ساعت با UTC برای شروع «روز» (ایران = 3.5)

os.makedirs(DATA_DIR, exist_ok=True)
db = sqlite3.connect(os.path.join(DATA_DIR, "data.db"), check_same_thread=False)
db.executescript(
    """
CREATE TABLE IF NOT EXISTS channels(id INTEGER PRIMARY KEY, hash INTEGER, title TEXT, username TEXT);
CREATE TABLE IF NOT EXISTS sources(id INTEGER PRIMARY KEY, hash INTEGER, title TEXT);
CREATE TABLE IF NOT EXISTS senders(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE IF NOT EXISTS keywords(word TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS raw_ids(token TEXT PRIMARY KEY, source TEXT, status TEXT DEFAULT 'pending', ts TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS candidates(user_id INTEGER PRIMARY KEY, hash INTEGER, username TEXT, name TEXT, source TEXT, priority INTEGER DEFAULT 0, ts TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS results(user_id INTEGER, channel_id INTEGER, status TEXT, ts TEXT DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(user_id, channel_id));
CREATE TABLE IF NOT EXISTS daily(day TEXT PRIMARY KEY, n INTEGER);
CREATE TABLE IF NOT EXISTS daily_attempts(day TEXT PRIMARY KEY, n INTEGER);
CREATE TABLE IF NOT EXISTS settings(k TEXT PRIMARY KEY, v TEXT);
CREATE TABLE IF NOT EXISTS admins(id INTEGER PRIMARY KEY, name TEXT);
"""
)
db.commit()

user = TelegramClient(make_session(SESSION_STRING), API_ID, API_HASH)
bot = TelegramClient(StringSession(), API_ID, API_HASH)
ME_ID = None
collect_lock = asyncio.Lock()
PENDING = {}  # admin_id -> (action, expires_at): منتظر پیام متنی ادمین
PICKS = {}  # kind -> [dict]: لیست‌های انتخابی (کانال‌ها، گروه‌ها، پیوی‌ها) برای دکمه‌ها
LAST_COLLECT = {}

STATUS_FA = {
    "added": "اد شد",
    "already": "از قبل عضو بود",
    "privacy": "فقط با لینک قابل اد (رد شد، پیامی فرستاده نشد)",
    "too_many_channels": "عضو کانال‌های زیاد",
    "banned_in_channel": "بن‌شده در کانال",
    "invalid": "نامعتبر",
}

# ---------------------------------------------------------------- helpers


def scalar(q, params=()):
    return db.execute(q, params).fetchone()[0]


def setting(k, default=None):
    r = db.execute("SELECT v FROM settings WHERE k=?", (k,)).fetchone()
    return r[0] if r else default


def set_setting(k, v):
    db.execute("INSERT OR REPLACE INTO settings VALUES(?,?)", (k, str(v)))
    db.commit()


def daily_limit():
    try:
        return max(1, int(setting("daily_limit", DAILY_LIMIT)))
    except (TypeError, ValueError):
        return DAILY_LIMIT


def today_str():
    """تاریخ امروز به وقت محلی (پیش‌فرض ایران)؛ سقف روزانه از نیمه‌شب همان منطقه ریست می‌شود."""
    return (datetime.now(timezone.utc) + timedelta(hours=TZ_OFFSET)).strftime("%Y-%m-%d")


def is_admin(uid):
    return uid == ADMIN_ID or bool(db.execute("SELECT 1 FROM admins WHERE id=?", (uid,)).fetchone())


def admin_ids():
    return [ADMIN_ID] + [a for (a,) in db.execute("SELECT id FROM admins") if a != ADMIN_ID]


def _count(table):  # table همیشه ثابت داخلی است، نه ورودی کاربر
    r = db.execute(f"SELECT n FROM {table} WHERE day=?", (today_str(),)).fetchone()
    return r[0] if r else 0


def _bump(table):
    d = today_str()
    db.execute(f"INSERT OR IGNORE INTO {table} VALUES(?,0)", (d,))
    db.execute(f"UPDATE {table} SET n=n+1 WHERE day=?", (d,))
    db.commit()


def today_count():
    return _count("daily")


def today_attempts():
    return _count("daily_attempts")


def bump_today():
    _bump("daily")


def bump_attempts():
    _bump("daily_attempts")


USERNAME_RE = re.compile(r"(?:(?<![\w.])@|t\.me/|telegram\.me/)([A-Za-z][A-Za-z0-9_]{3,31})")
NUM_RE = re.compile(r"(?<![\w@/+.])(\d{6,12})(?![\w])")
RESERVED = {"joinchat", "addlist", "addstickers", "share", "proxy", "socks", "login", "boost"}


def parse_tokens(text):
    """@username، لینک t.me و آیدی عددی را از متن بیرون می‌کشد (بدون تکرار، به ترتیب)."""
    out = []
    for m in USERNAME_RE.finditer(text or ""):
        u = m.group(1).lower()
        if u not in RESERVED:
            out.append(u)
    out += NUM_RE.findall(text or "")
    return list(dict.fromkeys(out))


def enqueue(tokens, source):
    n = 0
    for t in tokens:
        cur = db.execute("INSERT OR IGNORE INTO raw_ids(token, source) VALUES(?,?)", (t.lower(), source))
        n += cur.rowcount
    db.commit()
    return n


def add_candidate(ent, source, priority=0):
    name = " ".join(x for x in [ent.first_name, ent.last_name] if x)
    cur = db.execute(
        "INSERT OR IGNORE INTO candidates(user_id, hash, username, name, source, priority) VALUES(?,?,?,?,?,?)",
        (ent.id, ent.access_hash, ent.username, name, source, priority),
    )
    if cur.rowcount == 0 and priority > 0:
        db.execute("UPDATE candidates SET priority=MAX(priority, ?) WHERE user_id=?", (priority, ent.id))
    db.commit()
    return cur.rowcount


def usable(u):
    return bool(isinstance(u, User) and not u.bot and not u.deleted and u.access_hash and u.id != ME_ID)


def good(u):
    """فیلتر کیفیت برای کاربرهایی که خودکار جمع می‌شوند (نه آیدی‌هایی که خودت دادی)."""
    if not usable(u):
        return False
    if getattr(u, "scam", False) or getattr(u, "fake", False):
        return False
    if setting("active", "1") == "1" and isinstance(getattr(u, "status", None), (UserStatusEmpty, UserStatusLastMonth)):
        return False  # مدت‌هاست آنلاین نبوده
    return True


def remaining_pairs():
    return scalar(
        """SELECT COUNT(*) FROM candidates c CROSS JOIN channels ch
           LEFT JOIN results r ON r.user_id=c.user_id AND r.channel_id=ch.id WHERE r.user_id IS NULL"""
    )


def next_job():
    # هر کاربر در هر کانال فقط یک‌بار: کلید یکتای (user_id, channel_id) در جدول results
    return db.execute(
        """SELECT c.user_id, c.hash, ch.id, ch.hash
           FROM candidates c CROSS JOIN channels ch
           LEFT JOIN results r ON r.user_id=c.user_id AND r.channel_id=ch.id
           WHERE r.user_id IS NULL
           ORDER BY c.priority DESC, c.rowid, ch.id LIMIT 1"""
    ).fetchone()


def record(uid, cid, status):
    db.execute("INSERT OR IGNORE INTO results(user_id, channel_id, status) VALUES(?,?,?)", (uid, cid, status))
    db.commit()


async def notify(text, buttons=None):
    """به مالک و همه‌ی ادمین‌ها پیام می‌دهد (ادمینی که ربات را /start نکرده نادیده گرفته می‌شود)."""
    for aid in admin_ids():
        try:
            await bot.send_message(aid, text, buttons=buttons)
        except Exception:
            log.warning("notify to %s failed", aid, exc_info=True)


async def resolve(text):
    text = (text or "").strip()
    if not text:
        raise ValueError("ورودی خالیه")
    if text.lstrip("-").isdigit():
        text = int(text)
    return await user.get_entity(text)


def friendly(e):
    s = str(e)
    if isinstance(e, errors.FloodWaitError):
        return f"⏳ تلگرام می‌گه {e.seconds} ثانیه صبر کن و دوباره امتحان کن."
    if isinstance(e, ValueError) and any(x in s for x in ("Cannot find", "No user has", "Could not find", "ورودی")):
        return (
            "❌ پیدا نشد. یوزرنیم یا لینک رو درست بفرست (مثل @name). "
            "برای کانال/گروه خصوصی باید اکانت عضوش باشه."
        )
    return f"❌ خطا: {e}"


# ------------------------------------------------- reading IDs from the PV


@user.on(events.NewMessage(incoming=True))
async def on_private(event):
    try:
        if not event.is_private:
            return
        if not db.execute("SELECT 1 FROM senders WHERE id=?", (event.sender_id,)).fetchone():
            return
        tokens = parse_tokens(event.raw_text)
        contact = getattr(event.message, "contact", None)
        if contact and getattr(contact, "user_id", None):
            tokens.append(str(contact.user_id))
        n = enqueue(tokens, f"pv:{event.sender_id}")
        if n:
            await notify(f"📥 {n} آیدی جدید از فرستنده {event.sender_id} ثبت شد.")
    except Exception:
        log.exception("on_private failed")


async def resolve_token(token):
    try:
        ent = await user.get_entity(int(token) if token.isdigit() else token)
    except errors.FloodWaitError:
        raise
    except (ValueError, errors.UsernameNotOccupiedError, errors.UsernameInvalidError, errors.PeerIdInvalidError):
        return "not_found", None
    except Exception:
        log.exception("resolve error")
        return "error", None
    if not usable(ent):
        return "not_user", None
    return "resolved", ent


async def resolve_one():
    """یک آیدی خام را به کاربر واقعی تبدیل می‌کند. خروجی: ثانیه‌ی انتظار تا کار بعدی."""
    row = db.execute("SELECT token, source FROM raw_ids WHERE status='pending' ORDER BY rowid LIMIT 1").fetchone()
    if not row:
        return 5
    token, source = row
    try:
        status, ent = await resolve_token(token)
    except errors.FloodWaitError as e:
        return e.seconds + 3  # آیدی در حالت pending می‌ماند و بعداً دوباره امتحان می‌شود
    if ent is not None:
        add_candidate(ent, source, 1 if source.startswith(("pv:", "manual")) else 0)
    db.execute("UPDATE raw_ids SET status=? WHERE token=?", (status, token))
    db.commit()
    return random.uniform(2, 5)


async def resolver():
    while True:
        try:
            delay = await resolve_one()
        except Exception:
            log.exception("resolver error")
            delay = 10
        await asyncio.sleep(delay)


# ------------------------------------------------------ auto collecting


async def collect_group(gid, peer, title, report):
    """اعضا + فرستنده‌های پیام‌های اخیر یک گروه (برای گروه‌هایی که لیست اعضا مخفی است مهم‌تر است)."""
    new = 0
    try:
        async for u in user.iter_participants(peer, limit=3000):
            if good(u):
                new += add_candidate(u, f"group:{gid}")
    except errors.FloodWaitError as e:
        await asyncio.sleep(min(e.seconds, 300))
    except Exception:
        log.warning("members of %s not readable", gid, exc_info=True)
    if MESSAGE_SCAN > 0:
        try:
            async for msg in user.iter_messages(peer, limit=MESSAGE_SCAN):
                u = msg.sender
                if good(u):
                    new += add_candidate(u, f"msgs:{gid}")
        except errors.FloodWaitError as e:
            await asyncio.sleep(min(e.seconds, 300))
        except Exception:
            log.warning("messages of %s not readable", gid, exc_info=True)
    report.append(f"📂 {title}: +{new}")
    return new


async def collect_all():
    """جمع‌کردن کاربر از گروه‌های منبع، (اختیاری) همه‌ی گروه‌های عضو‌شده، و جستجوی کلیدواژه."""
    report, total = [], 0
    groups = {}
    for sid, shash, title in db.execute("SELECT id, hash, title FROM sources").fetchall():
        groups[sid] = (InputPeerChannel(sid, shash), title or str(sid))
    if setting("allgroups", "0") == "1":
        targets = {c for (c,) in db.execute("SELECT id FROM channels")}
        async for d in user.iter_dialogs():
            e = d.entity
            if isinstance(e, Channel) and e.megagroup and e.id not in targets:
                groups.setdefault(e.id, (InputPeerChannel(e.id, e.access_hash), e.title))
    for gid, (peer, title) in groups.items():
        total += await collect_group(gid, peer, title, report)
        await asyncio.sleep(GROUP_PAUSE)
    for (word,) in db.execute("SELECT word FROM keywords").fetchall():
        n = 0
        try:
            res = await user(SearchRequest(q=word, limit=50))
            for u in res.users:
                if good(u):
                    n += add_candidate(u, f"search:{word}")
        except errors.FloodWaitError as e:
            await asyncio.sleep(min(e.seconds, 300))
        except Exception:
            log.warning("search %s failed", word, exc_info=True)
        report.append(f"🔤 {word}: +{n}")
        total += n
        await asyncio.sleep(GROUP_PAUSE)
    LAST_COLLECT.update(total=total, lines=report, ts=time.time())
    return total


async def run_collect():
    if collect_lock.locked():
        return None
    async with collect_lock:
        return await collect_all()


def collect_report_text():
    if not LAST_COLLECT:
        return "هنوز اجرا نشده."
    when = time.strftime("%H:%M", time.gmtime(LAST_COLLECT["ts"]))
    lines = LAST_COLLECT["lines"][:8]
    return f"{LAST_COLLECT['total']} کاربر جدید (ساعت {when} UTC)\n" + "\n".join(lines)


def has_collect_targets():
    return bool(
        scalar("SELECT COUNT(*) FROM sources")
        or scalar("SELECT COUNT(*) FROM keywords")
        or setting("allgroups", "0") == "1"
    )


async def collect_and_report():
    n = await run_collect()
    if n is None:
        await notify("⏳ جمع‌آوری از قبل در حال اجراست.")
    else:
        await notify("✅ جمع‌آوری تموم شد.\n" + collect_report_text())


async def auto_loop():
    while True:
        await asyncio.sleep(60)
        try:
            if setting("auto", "1") != "1" or not has_collect_targets():
                continue
            if time.time() - float(setting("last_collect", "0")) < AUTO_HOURS * 3600:
                continue
            set_setting("last_collect", time.time())
            n = await run_collect()
            if n:
                await notify(f"🔎 جمع‌آوری خودکار: {n} کاربر جدید به صف اضافه شد.")
        except Exception:
            log.exception("auto loop error")


# ------------------------------------------------------------- adding


async def invite(uid, uhash, cid, chash):
    try:
        res = await user(InviteToChannelRequest(InputPeerChannel(cid, chash), [InputPeerUser(uid, uhash)]))
        if getattr(res, "missing_invitees", None):
            return "privacy"
        return "added"
    except errors.UserAlreadyParticipantError:
        return "already"
    except (errors.UserPrivacyRestrictedError, errors.UserNotMutualContactError):
        return "privacy"
    except errors.UserChannelsTooMuchError:
        return "too_many_channels"
    except errors.UserBannedInChannelError:
        return "banned_in_channel"
    except (errors.UserIdInvalidError, errors.InputUserDeactivatedError):
        return "invalid"


async def process_one():
    """یک کار از صف را انجام می‌دهد. خروجی: ثانیه‌ی انتظار تا کار بعدی."""
    if setting("running", "0") != "1":
        return 5
    if today_count() >= daily_limit() or today_attempts() >= daily_limit() * ATTEMPTS_FACTOR:
        return 600
    job = next_job()
    if not job:
        return 30
    uid, uhash, cid, chash = job
    try:
        status = await invite(uid, uhash, cid, chash)
    except errors.FloodWaitError as e:
        await notify(f"⏳ FloodWait: {e.seconds} ثانیه صبر می‌کنم.")
        return e.seconds + 5
    except errors.PeerFloodError:
        set_setting("running", "0")
        await notify("🛑 تلگرام اکانت رو محدود کرده (PeerFlood). کار متوقف شد؛ چند روز استراحت بده.")
        return 5
    except (errors.ChatAdminRequiredError, errors.ChatWriteForbiddenError, errors.ChannelPrivateError):
        set_setting("running", "0")
        await notify(f"🛑 روی کانال {cid} دسترسی ادمین (افزودن عضو) نداره. متوقف شد.")
        return 5
    except errors.RPCError as e:
        # خطای ناشناخته‌ی تلگرام برای این جفت: ثبت می‌شود تا صف روی آن گیر نکند
        log.warning("rpc error for user %s channel %s: %r", uid, cid, e)
        status = f"error:{type(e).__name__}"
    except (ConnectionError, OSError, asyncio.TimeoutError):
        return 30  # مشکل شبکه: ثبت نمی‌کنیم، بعداً دوباره
    except Exception as e:
        log.exception("unexpected error for user %s channel %s", uid, cid)
        status = f"error:{type(e).__name__}"
    record(uid, cid, status)
    bump_attempts()
    if status == "added":
        bump_today()
    return random.randint(MIN_DELAY, MAX_DELAY)


async def worker():
    while True:
        try:
            delay = await process_one()
        except Exception:
            log.exception("worker error")
            delay = 30
        await asyncio.sleep(delay)


# ------------------------------------------------------- bot: data actions


def display_name(ent):
    return " ".join(x for x in [ent.first_name, ent.last_name] if x)


async def add_channel_from(text):
    ent = await resolve(text)
    if not isinstance(ent, Channel):
        return "این کانال/سوپرگروه نیست."
    db.execute("INSERT OR REPLACE INTO channels VALUES(?,?,?,?)", (ent.id, ent.access_hash, ent.title, ent.username))
    db.commit()
    return f"✅ کانال اضافه شد: {ent.title} ({ent.id})"


async def add_sender_from(text):
    ent = await resolve(text)
    if not isinstance(ent, User):
        return "این یک کاربر نیست."
    name = display_name(ent)
    db.execute("INSERT OR REPLACE INTO senders VALUES(?,?)", (ent.id, name))
    db.commit()
    return f"✅ فرستنده ثبت شد: {name} ({ent.id}). از این به بعد آیدی‌های پیوی‌اش برداشته می‌شه."


async def add_source_from(text):
    ent = await resolve(text)
    if not isinstance(ent, Channel):
        return "فقط سوپرگروه/کانال پشتیبانی می‌شه."
    db.execute("INSERT OR REPLACE INTO sources VALUES(?,?,?)", (ent.id, ent.access_hash, ent.title))
    db.commit()
    return f"✅ منبع اضافه شد: {ent.title}"


def add_keywords_from(text):
    words = [w.strip().lower() for w in re.split(r"[,\n،]+", text or "") if w.strip()]
    if not words:
        return "کلیدواژه‌ای پیدا نشد."
    for w in words:
        db.execute("INSERT OR IGNORE INTO keywords VALUES(?)", (w,))
    db.commit()
    return f"✅ {len(words)} کلیدواژه اضافه شد."


def add_users_from(text):
    tokens = parse_tokens(text) or [t.strip().lstrip("@").lower() for t in (text or "").split() if t.strip()]
    n = enqueue(tokens, "manual")
    return f"✅ {n} مورد جدید به صف تحلیل رفت (اولویت بالا)."


async def add_admin_from(text):
    t = (text or "").strip().lstrip("@")
    if not t:
        raise ValueError("ورودی خالیه")
    if t.isdigit():
        aid, name = int(t), t
    else:
        ent = await resolve(t)
        if not isinstance(ent, User) or ent.bot:
            return "این یک کاربر عادی نیست."
        aid, name = ent.id, display_name(ent) or t
    if aid == ADMIN_ID:
        return "مالک از قبل همه‌ی دسترسی‌ها رو داره."
    db.execute("INSERT OR REPLACE INTO admins VALUES(?,?)", (aid, name))
    db.commit()
    return f"✅ ادمین اضافه شد: {name} ({aid})\nباید یک بار به ربات /start بزنه تا پیام‌ها بهش برسه."


TEXT_ACTIONS = {
    "ada": (add_admin_from, "adl", "✍️ یوزرنیم یا آیدی عددی ادمین جدید رو بفرست (مثل @name)."),
    "chm": (add_channel_from, "ch", "✍️ یوزرنیم یا لینک کانال رو بفرست (مثل @mychannel).\nاکانت باید توش ادمین با دسترسی افزودن عضو باشه."),
    "snm": (add_sender_from, "sn", "✍️ یوزرنیم فرستنده رو بفرست (مثل @mozhdeh)."),
    "srm": (add_source_from, "src", "✍️ یوزرنیم یا لینک گروه منبع رو بفرست. اکانت باید عضوش باشه."),
    "kwa": (add_keywords_from, "kw", "✍️ کلیدواژه رو بفرست (چندتا رو با کاما یا خط جدید جدا کن)."),
    "adu": (add_users_from, "co", "✍️ آیدی‌ها (@username یا لینک t.me) رو بفرست؛ چندتا رو با فاصله یا خط جدید جدا کن."),
}


def set_pending(action, uid=None):
    PENDING[uid or ADMIN_ID] = (action, time.time() + 600)


def get_pending(uid=None):
    uid = uid or ADMIN_ID
    p = PENDING.get(uid)
    if p and p[1] > time.time():
        return p[0]
    PENDING.pop(uid, None)
    return None


async def apply_text(action, text):
    fn, target, _ = TEXT_ACTIONS[action]
    res = fn(text)
    if asyncio.iscoroutine(res):
        res = await res
    return res, target


# ----------------------------------------------------- bot: pickers (lists)

PICK_CFG = {
    "ch": dict(title="📢 افزودن کانال", add="chp", list="cha", manual="chm", back="ch",
               empty="کانالی پیدا نشد که اکانت توش ادمین (با دسترسی افزودن عضو) باشه."),
    "sr": dict(title="📂 افزودن گروه منبع", add="srp", list="sra", manual="srm", back="src",
               empty="گروهی پیدا نشد."),
    "sn": dict(title="👤 افزودن فرستنده", add="snp", list="sna", manual="snm", back="sn",
               empty="پیوی‌ای پیدا نشد."),
}
PICK_PER_PAGE = 8


async def load_picks(kind):
    """از دیالوگ‌های اکانت لیست قابل‌انتخاب می‌سازد."""
    items = []
    taken = {c for (c,) in db.execute("SELECT id FROM channels")} | {c for (c,) in db.execute("SELECT id FROM sources")}
    async for d in user.iter_dialogs(limit=300):
        e = d.entity
        if kind == "ch" and isinstance(e, Channel):
            ar = getattr(e, "admin_rights", None)
            if e.id not in taken and (getattr(e, "creator", False) or (ar is not None and getattr(ar, "invite_users", False))):
                items.append(dict(id=e.id, hash=e.access_hash, title=e.title, username=e.username))
        elif kind == "sr" and isinstance(e, Channel) and e.megagroup:
            if e.id not in taken:
                items.append(dict(id=e.id, hash=e.access_hash, title=e.title, username=e.username))
        elif kind == "sn" and isinstance(e, User) and not e.bot and not e.deleted and e.id != ME_ID:
            if len(items) < 40:
                items.append(dict(id=e.id, hash=e.access_hash, title=display_name(e) or str(e.id), username=e.username))
    PICKS[kind] = items
    return items


# ------------------------------------------------------------ bot: views


def short(s, n=24):
    s = s or "-"
    return s if len(s) <= n else s[: n - 1] + "…"


def B(text, data):
    return Button.inline(text, data.encode())


def home_row():
    return [B("🏠 منو", "m")]


def checkbox(on, label):
    return f"{'✅' if on else '⬜️'} {label}"


def setup_todo():
    """مراحل ناتمام راه‌اندازی را برمی‌گرداند (برای راهنمایی هوشمند در صفحه‌ی اصلی)."""
    todo = []
    if not scalar("SELECT COUNT(*) FROM channels"):
        todo.append("📢 یک کانال اضافه کن (اکانت باید توش ادمین با دسترسی افزودن عضو باشه)")
    if not (
        scalar("SELECT COUNT(*) FROM senders")
        or scalar("SELECT COUNT(*) FROM sources")
        or scalar("SELECT COUNT(*) FROM keywords")
        or setting("allgroups", "0") == "1"
    ):
        todo.append("👤 یک منبع اضافه کن: فرستنده‌ی پیوی، گروه منبع یا کلیدواژه")
    return todo


def summary_text():
    running = setting("running", "0") == "1"
    cands = scalar("SELECT COUNT(*) FROM candidates")
    pending = scalar("SELECT COUNT(*) FROM raw_ids WHERE status='pending'")
    link_only = scalar("SELECT COUNT(*) FROM results WHERE status='privacy'")
    text = (
        "🤖 پنل مدیریت\n\n"
        f"وضعیت: {'🟢 روشن' if running else '🔴 خاموش'}\n"
        f"امروز: {today_count()}/{daily_limit()} اد موفق\n"
        f"کاربران جمع‌شده: {cands}\n"
        f"در صف اد: {remaining_pairs()}\n"
        f"در انتظار تحلیل: {pending}\n"
        f"فقط با لینک قابل اد (رد شده): {link_only}"
    )
    todo = setup_todo()
    if todo:
        text += "\n\n📌 قدم‌های بعدی:\n" + "\n".join(f"• {t}" for t in todo)
    elif not running:
        text += "\n\n✅ همه‌چیز آماده‌ست؛ دکمه‌ی «شروع» رو بزن."
    return text


def main_view():
    running = setting("running", "0") == "1"
    rows = [
        [B("⏸ توقف" if running else "▶️ شروع", "pause" if running else "run")],
        [B("📢 کانال‌ها", "ch"), B("👤 فرستنده‌ها", "sn")],
        [B("🔎 جمع‌آوری", "co"), B("📊 آمار", "st")],
        [B("👥 کاربران", "us:1"), B("📁 خروجی CSV", "csv")],
        [B("🛡 ادمین‌ها", "adl"), B("⚙️ تنظیمات", "se")],
    ]
    return summary_text(), rows


def admins_view():
    rows = []
    for aid, name in db.execute("SELECT id, name FROM admins").fetchall():
        rows.append([B(f"🗑 {short(name or str(aid))}", f"adr:{aid}")])
    rows.append([B("➕ افزودن ادمین", "ada")])
    rows.append(home_row())
    text = (
        "🛡 ادمین‌ها\n"
        f"مالک: {ADMIN_ID} (قابل حذف نیست)\n\n"
        "ادمین‌ها می‌تونن ربات رو کنترل کنن، ولی افزودن و حذف ادمین فقط کار مالکه.\n"
        "برای حذف روی اسم بزن."
    )
    if len(rows) == 2:
        text += "\n\nهنوز ادمین دیگه‌ای نیست."
    return text, rows


def channels_view():
    rows = []
    for cid, title in db.execute("SELECT id, title FROM channels").fetchall():
        n = scalar("SELECT COUNT(*) FROM results WHERE channel_id=? AND status='added'", (cid,))
        rows.append([B(f"🗑 {short(title)} · {n} اد شده", f"chd:{cid}")])
    rows.append([B("➕ افزودن کانال", "cha:1")])
    rows.append(home_row())
    text = "📢 کانال‌ها\nبرای حذف، روی کانال بزن. (سابقه‌ی افزودن‌ها نگه داشته می‌شه تا تکرار نشه)"
    if len(rows) == 2:
        text += "\n\nهنوز کانالی ثبت نشده."
    return text, rows


def senders_view():
    rows = []
    for sid, name in db.execute("SELECT id, name FROM senders").fetchall():
        rows.append([B(f"🗑 {short(name or str(sid))}", f"snd:{sid}")])
    rows.append([B("➕ افزودن فرستنده", "sna:1")])
    rows.append(home_row())
    text = "👤 فرستنده‌ها\nآیدی‌هایی که این افراد تو پیوی اکانتت می‌فرستن خودکار برداشته و اد می‌شه.\nبرای حذف روی اسم بزن."
    if len(rows) == 2:
        text += "\n\nهنوز فرستنده‌ای ثبت نشده."
    return text, rows


def sources_view():
    rows = []
    for sid, title in db.execute("SELECT id, title FROM sources").fetchall():
        rows.append([B(f"🗑 {short(title or str(sid))}", f"srd:{sid}")])
    rows.append([B("➕ افزودن گروه منبع", "sra:1")])
    rows.append([B("🔙 جمع‌آوری", "co")])
    text = "📂 گروه‌های منبع\nاعضا و فرستنده‌های پیام‌های این گروه‌ها جمع می‌شن. برای حذف روی گروه بزن."
    if len(rows) == 2:
        text += "\n\nهنوز منبعی ثبت نشده."
    return text, rows


def keywords_view():
    rows = []
    for rid, word in db.execute("SELECT rowid, word FROM keywords").fetchall():
        rows.append([B(f"🗑 {short(word)}", f"kwd:{rid}")])
    rows.append([B("➕ افزودن کلیدواژه", "kwa")])
    rows.append([B("🔙 جمع‌آوری", "co")])
    text = "🔤 کلیدواژه‌ها\nاکانت با این کلمات کاربر جستجو می‌کنه. برای حذف روی کلمه بزن."
    if len(rows) == 2:
        text += "\n\nهنوز کلیدواژه‌ای نیست."
    return text, rows


def collect_view():
    ns = scalar("SELECT COUNT(*) FROM sources")
    nk = scalar("SELECT COUNT(*) FROM keywords")
    text = (
        "🔎 جمع‌آوری\n"
        f"منابع: {ns} | کلیدواژه‌ها: {nk}\n\n"
        f"آخرین اجرا: {collect_report_text()}"
    )
    rows = [
        [B("⚡ همین الان جمع کن", "now")],
        [B(f"📂 منابع ({ns})", "src"), B(f"🔤 کلیدواژه‌ها ({nk})", "kw")],
        [B(checkbox(setting("auto", "1") == "1", "جمع‌آوری خودکار"), "t:auto")],
        [B(checkbox(setting("allgroups", "0") == "1", "همه‌ی گروه‌های من"), "t:allgroups")],
        [B(checkbox(setting("active", "1") == "1", "فقط کاربران فعال"), "t:active")],
        [B("✍️ افزودن دستی آیدی", "adu")],
        home_row(),
    ]
    return text, rows


def settings_view():
    lim = daily_limit()
    text = (
        "⚙️ تنظیمات\n\n"
        f"سقف اد موفق در روز: {lim}\n"
        f"سقف تلاش در روز: {lim * ATTEMPTS_FACTOR}\n"
        f"فاصله بین هر اد: {MIN_DELAY} تا {MAX_DELAY} ثانیه\n"
        f"جمع‌آوری خودکار: هر {AUTO_HOURS:g} ساعت\n\n"
        "بالا بردن سقف خطر محدود شدن اکانت رو زیاد می‌کنه."
    )
    rows = [[B("➖ ۵", "lim:-5"), B(str(lim), "noop"), B("➕ ۵", "lim:5")], home_row()]
    return text, rows


def stats_text():
    raw = dict(db.execute("SELECT status, COUNT(*) FROM raw_ids GROUP BY status").fetchall())
    res = dict(db.execute("SELECT status, COUNT(*) FROM results GROUP BY status").fetchall())
    lim = daily_limit()
    lines = [
        f"وضعیت: {'🟢 روشن' if setting('running', '0') == '1' else '🔴 خاموش'}",
        f"جمع‌آوری خودکار: {'روشن' if setting('auto', '1') == '1' else 'خاموش'}",
        f"امروز: {today_count()}/{lim} اد موفق | {today_attempts()}/{lim * ATTEMPTS_FACTOR} تلاش",
        "",
        f"کاربران جمع‌شده: {scalar('SELECT COUNT(*) FROM candidates')}",
        f"آیدی خام در انتظار تحلیل: {raw.get('pending', 0)}",
        f"آیدی پیدا‌نشده/نامعتبر: {raw.get('not_found', 0) + raw.get('not_user', 0) + raw.get('error', 0)}",
        f"تلاش‌های باقی‌مانده (کاربر×کانال): {remaining_pairs()}",
        "",
        "نتایج:",
    ]
    for k, v in sorted(res.items(), key=lambda x: -x[1]):
        lines.append(f"  {STATUS_FA.get(k, k)}: {v}")
    lines.append("")
    lines.append("کانال‌ها:")
    for cid, title in db.execute("SELECT id, title FROM channels").fetchall():
        n = scalar("SELECT COUNT(*) FROM results WHERE channel_id=? AND status='added'", (cid,))
        lines.append(f"  {title}: {n} اد شده")
    week = db.execute("SELECT day, n FROM daily ORDER BY day DESC LIMIT 7").fetchall()
    if week:
        lines.append("")
        lines.append("۷ روز اخیر (اد موفق):")
        lines += [f"  {d}: {n}" for d, n in week]
    return "\n".join(lines)


def stats_view():
    return "📊 آمار\n\n" + stats_text(), [
        [B("🔄 بروزرسانی", "st"), B("📁 نیازمند لینک", "csvl")],
        home_row(),
    ]


def users_page(page):
    total = scalar("SELECT COUNT(*) FROM candidates")
    pages = max(1, math.ceil(total / 30))
    page = min(max(1, page), pages)
    rows = db.execute(
        """SELECT c.user_id, c.username, c.source,
                  (SELECT COUNT(*) FROM results r WHERE r.user_id=c.user_id AND r.status='added')
           FROM candidates c ORDER BY c.rowid DESC LIMIT 30 OFFSET ?""",
        ((page - 1) * 30,),
    ).fetchall()
    lines = [f"👥 کاربران (صفحه {page}/{pages}) — مجموع {total}", ""]
    lines += [f"{('@' + u) if u else '-'} | {i} | {s} | اد در {n} کانال" for i, u, s, n in rows]
    return "\n".join(lines), page, pages


def users_view(page):
    text, page, pages = users_page(page)
    nav = []
    if page > 1:
        nav.append(B("◀️ قبلی", f"us:{page - 1}"))
    nav.append(B(f"{page}/{pages}", "noop"))
    if page < pages:
        nav.append(B("بعدی ▶️", f"us:{page + 1}"))
    return text, [nav, home_row()]


def picker_view(kind, page):
    cfg = PICK_CFG[kind]
    items = PICKS.get(kind, [])
    pages = max(1, math.ceil(len(items) / PICK_PER_PAGE))
    page = min(max(1, page), pages)
    rows = []
    for it in items[(page - 1) * PICK_PER_PAGE: page * PICK_PER_PAGE]:
        label = short(it["title"], 28) + (f" @{it['username']}" if kind == "sn" and it.get("username") else "")
        rows.append([B(label, f"{cfg['add']}:{it['id']}")])
    if pages > 1:
        nav = []
        if page > 1:
            nav.append(B("◀️", f"{cfg['list']}:{page - 1}"))
        nav.append(B(f"{page}/{pages}", "noop"))
        if page < pages:
            nav.append(B("▶️", f"{cfg['list']}:{page + 1}"))
        rows.append(nav)
    rows.append([B("✍️ ورود دستی", cfg["manual"])])
    rows.append([B("🔙 بازگشت", cfg["back"])])
    text = f"{cfg['title']}\nیکی رو انتخاب کن:" if items else f"{cfg['title']}\n\n{cfg['empty']}\nمی‌تونی دستی وارد کنی."
    return text, rows


def prompt_view(action, uid=None):
    _, _, prompt = TEXT_ACTIONS[action]
    set_pending(action, uid)
    return prompt, [[B("❌ لغو", "cancel")]]


def find_pick(kind, pid):
    for it in PICKS.get(kind, []):
        if it["id"] == pid:
            return it
    return None


async def dispatch(data, uid=None):
    """یک دکمه را اجرا می‌کند. خروجی: (متن، دکمه‌ها، پیام کوتاه یا None)."""
    uid = uid or ADMIN_ID
    head, _, arg = data.partition(":")

    if head == "m":
        PENDING.pop(uid, None)
        return (*main_view(), None)
    if head == "cancel":
        PENDING.pop(uid, None)
        return (*main_view(), "لغو شد")

    # ---- ادمین‌ها (فقط مالک)
    if head in ("adl", "adr", "ada"):
        if uid != ADMIN_ID:
            return (*main_view(), "فقط مالک می‌تونه ادمین‌ها رو مدیریت کنه 🔒")
        if head == "adl":
            return (*admins_view(), None)
        if head == "adr":
            db.execute("DELETE FROM admins WHERE id=?", (int(arg),))
            db.commit()
            return (*admins_view(), "🗑 حذف شد")
        return (*prompt_view("ada", uid), None)

    # ---- start / stop
    if head == "run":
        if not scalar("SELECT COUNT(*) FROM channels"):
            return (*main_view(), "اول یه کانال اضافه کن 📢")
        set_setting("running", "1")
        return (*main_view(), "▶️ شروع شد")
    if head == "pause":
        set_setting("running", "0")
        return (*main_view(), "⏸ متوقف شد")

    # ---- menus
    if head == "ch":
        return (*channels_view(), None)
    if head == "sn":
        return (*senders_view(), None)
    if head == "src":
        return (*sources_view(), None)
    if head == "kw":
        return (*keywords_view(), None)
    if head == "co":
        return (*collect_view(), None)
    if head == "st":
        return (*stats_view(), None)
    if head == "se":
        return (*settings_view(), None)
    if head == "us":
        return (*users_view(int(arg) if arg.isdigit() else 1), None)

    # ---- pickers
    if head in ("cha", "sra", "sna"):
        kind = head[:2]
        page = int(arg) if arg.isdigit() else 1
        if page == 1 or kind not in PICKS:
            await load_picks(kind)
        return (*picker_view(kind, page), None)
    if head in ("chp", "srp", "snp"):
        kind = head[:2]
        pid = int(arg)
        if find_pick(kind, pid) is None:
            await load_picks(kind)
        it = find_pick(kind, pid)
        if it is None:
            return (*picker_view(kind, 1), "دوباره انتخاب کن")
        if kind == "ch":
            db.execute("INSERT OR REPLACE INTO channels VALUES(?,?,?,?)", (it["id"], it["hash"], it["title"], it["username"]))
            view = channels_view()
        elif kind == "sr":
            db.execute("INSERT OR REPLACE INTO sources VALUES(?,?,?)", (it["id"], it["hash"], it["title"]))
            view = sources_view()
        else:
            db.execute("INSERT OR REPLACE INTO senders VALUES(?,?)", (it["id"], it["title"]))
            view = senders_view()
        db.commit()
        return (*view, "✅ اضافه شد")

    # ---- deletes
    if head == "chd":
        db.execute("DELETE FROM channels WHERE id=?", (int(arg),))
        db.commit()
        return (*channels_view(), "🗑 حذف شد")
    if head == "snd":
        db.execute("DELETE FROM senders WHERE id=?", (int(arg),))
        db.commit()
        return (*senders_view(), "🗑 حذف شد")
    if head == "srd":
        db.execute("DELETE FROM sources WHERE id=?", (int(arg),))
        db.commit()
        return (*sources_view(), "🗑 حذف شد")
    if head == "kwd":
        db.execute("DELETE FROM keywords WHERE rowid=?", (int(arg),))
        db.commit()
        return (*keywords_view(), "🗑 حذف شد")

    # ---- typed input
    if head in TEXT_ACTIONS:
        return (*prompt_view(head, uid), None)

    # ---- toggles / numbers
    if head == "t" and arg in ("auto", "allgroups", "active"):
        default = "0" if arg == "allgroups" else "1"
        set_setting(arg, "0" if setting(arg, default) == "1" else "1")
        return (*collect_view(), None)
    if head == "lim":
        try:
            new = min(200, max(5, daily_limit() + int(arg)))
        except ValueError:
            new = daily_limit()
        set_setting("daily_limit", new)
        return (*settings_view(), None)

    return (*main_view(), None)


# --------------------------------------------------------- bot: handlers

HELP = """📋 راهنما

بهترین راه: /start و استفاده از دکمه‌ها.
ربات به هیچ کاربری پیام یا لینک نمی‌فرسته؛ کسی که فقط با لینک قابل اده رد می‌شه و تو لیست «نیازمند لینک» ثبت می‌شه.
ادمین‌ها رو از دکمه‌ی «🛡 ادمین‌ها» (فقط مالک) مدیریت کن.

دستورهای متنی (اختیاری):
/addchannel @کانال   /delchannel @کانال   /channels
/addsender @نام   /delsender @نام   /senders
/addsource @گروه   /delsource @گروه   /sources
/addkeyword کلمه   /delkeyword کلمه   /keywords
/allgroups on|off   /auto on|off   /collect
/adduser @a @b ...
/stats   /users [صفحه]   /ids [@کانال]   /allids
/run   /pause"""

NEED_ARG = {
    "/addchannel", "/delchannel", "/addsender", "/delsender", "/addsource",
    "/delsource", "/addkeyword", "/delkeyword", "/adduser",
}


def arg_of(event):
    parts = (event.raw_text or "").split(maxsplit=1)
    return parts[1].strip() if len(parts) > 1 else ""


def onoff(arg):
    return arg.strip().lower() in ("on", "1", "روشن", "yes")


async def reply_long(event, lines, empty="خالیه."):
    buf = ""
    for ln in lines:
        if len(buf) + len(ln) > 3500:
            await event.respond(buf)
            buf = ""
        buf += ln + "\n"
    await event.respond(buf or empty)


def build_csv(only_status=None):
    """CSV همه‌ی آیدی‌ها؛ با only_status فقط کاربرانی که حداقل یک نتیجه با آن وضعیت دارند."""
    ch_title = dict(db.execute("SELECT id, title FROM channels").fetchall())
    keep = None
    if only_status:
        keep = {u for (u,) in db.execute("SELECT DISTINCT user_id FROM results WHERE status=?", (only_status,))}
    per_user = defaultdict(list)
    for uid, cid, st in db.execute("SELECT user_id, channel_id, status FROM results").fetchall():
        per_user[uid].append(f"{ch_title.get(cid, cid)}:{st}")
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["user_id", "username", "name", "source", "collected_at", "results"])
    for uid, un, name, src, ts in db.execute("SELECT user_id, username, name, source, ts FROM candidates ORDER BY rowid"):
        if keep is not None and uid not in keep:
            continue
        w.writerow([uid, un or "", name or "", src, ts, "; ".join(per_user.get(uid, []))])
    if keep is None:
        for token, src, st, ts in db.execute("SELECT token, source, status, ts FROM raw_ids WHERE status!='resolved' ORDER BY rowid"):
            w.writerow(["", token, "", src, ts, f"raw:{st}"])
    data = io.BytesIO(("﻿" + buf.getvalue()).encode("utf-8"))
    data.name = "need_link.csv" if only_status else "all_ids.csv"
    return data


async def send_menu(event, prefix=""):
    text, rows = main_view()
    await event.respond(prefix + text, buttons=rows)


@bot.on(events.CallbackQuery(func=lambda e: is_admin(e.sender_id)))
async def on_callback(event):
    data = (event.data or b"").decode()
    try:
        if data == "noop":
            return await event.answer()
        if data == "csv":
            await event.answer("در حال ساخت فایل...")
            return await bot.send_file(event.chat_id, build_csv(), caption="همه‌ی آیدی‌های جمع‌شده از ابتدا")
        if data == "csvl":
            await event.answer("در حال ساخت فایل...")
            return await bot.send_file(
                event.chat_id,
                build_csv("privacy"),
                caption="کاربرهایی که فقط با لینک قابل اد بودن (بهشون هیچ پیامی فرستاده نشد)",
            )
        if data == "now":
            if not has_collect_targets():
                return await event.answer("اول یه گروه منبع یا کلیدواژه اضافه کن، یا «همه‌ی گروه‌های من» رو روشن کن.", alert=True)
            await event.answer("جمع‌آوری شروع شد؛ نتیجه رو برات می‌فرستم.")
            asyncio.create_task(collect_and_report())
            return
        text, rows, toast = await dispatch(data, event.sender_id)
        await event.answer(toast)
        try:
            await event.edit(text, buttons=rows)
        except errors.MessageNotModifiedError:
            pass
    except Exception as e:
        log.exception("callback error")
        try:
            await event.answer(friendly(e)[:190], alert=True)
        except Exception:
            pass


@bot.on(events.NewMessage(incoming=True, func=lambda e: is_admin(e.sender_id) and not (e.raw_text or "").startswith("/")))
async def on_text(event):
    uid = event.sender_id
    action = get_pending(uid)
    if not action:
        return await send_menu(event)
    PENDING.pop(uid, None)
    try:
        result, target = await apply_text(action, event.raw_text or "")
    except Exception as e:
        log.warning("text action %s failed: %r", action, e)
        result, target = friendly(e), TEXT_ACTIONS[action][1]
    text, rows, _ = await dispatch(target, uid)
    await event.respond(f"{result}\n\n{text}", buttons=rows)


@bot.on(events.NewMessage(incoming=True, pattern=r"^/", func=lambda e: is_admin(e.sender_id)))
async def on_cmd(event):
    PENDING.pop(event.sender_id, None)
    cmd = event.raw_text.split()[0].split("@")[0].lower()
    arg = arg_of(event)
    try:
        if cmd in ("/start", "/menu"):
            await send_menu(event)
        elif cmd == "/help":
            await event.respond(HELP)
        elif cmd in NEED_ARG and not arg:
            await send_menu(event, "این دستور باید همراه ورودی باشه (مثلاً /addchannel @name). بهتره از دکمه‌های منو استفاده کنی 👇\n\n")

        # ---- channels
        elif cmd == "/addchannel":
            await event.respond(await add_channel_from(arg))
        elif cmd == "/delchannel":
            ent = await resolve(arg)
            db.execute("DELETE FROM channels WHERE id=?", (ent.id,))
            db.commit()
            await event.respond("🗑 کانال حذف شد. (سابقه‌ی افزودن‌ها نگه داشته می‌شه تا تکرار نشه)")
        elif cmd == "/channels":
            lines = []
            for cid, t, u in db.execute("SELECT id, title, username FROM channels").fetchall():
                n = scalar("SELECT COUNT(*) FROM results WHERE channel_id=? AND status='added'", (cid,))
                lines.append(f"{t} | @{u or '-'} | {cid} | اد شده: {n}")
            await reply_long(event, lines, "کانالی ثبت نشده.")

        # ---- senders (PV)
        elif cmd == "/addsender":
            await event.respond(await add_sender_from(arg))
        elif cmd == "/delsender":
            ent = await resolve(arg)
            db.execute("DELETE FROM senders WHERE id=?", (ent.id,))
            db.commit()
            await event.respond("🗑 فرستنده حذف شد.")
        elif cmd == "/senders":
            rows = db.execute("SELECT id, name FROM senders").fetchall()
            await reply_long(event, [f"{n} | {i}" for i, n in rows], "فرستنده‌ای ثبت نشده.")

        # ---- sources / keywords
        elif cmd == "/addsource":
            await event.respond(await add_source_from(arg))
        elif cmd == "/delsource":
            ent = await resolve(arg)
            db.execute("DELETE FROM sources WHERE id=?", (ent.id,))
            db.commit()
            await event.respond("🗑 منبع حذف شد.")
        elif cmd == "/sources":
            rows = db.execute("SELECT id, title FROM sources").fetchall()
            await reply_long(event, [f"{t} | {i}" for i, t in rows], "منبعی ثبت نشده.")
        elif cmd == "/addkeyword":
            await event.respond(add_keywords_from(arg))
        elif cmd == "/delkeyword":
            db.execute("DELETE FROM keywords WHERE word=?", (arg.lower(),))
            db.commit()
            await event.respond("🗑 حذف شد.")
        elif cmd == "/keywords":
            await reply_long(event, [w for (w,) in db.execute("SELECT word FROM keywords")], "کلیدواژه‌ای نیست.")
        elif cmd == "/allgroups":
            set_setting("allgroups", "1" if onoff(arg) else "0")
            await event.respond("✅ انجام شد: " + ("همه‌ی گروه‌های عضو‌شده" if onoff(arg) else "فقط منابع ثبت‌شده"))
        elif cmd == "/auto":
            set_setting("auto", "1" if onoff(arg) else "0")
            await event.respond("✅ جمع‌آوری خودکار " + ("روشن شد." if onoff(arg) else "خاموش شد."))
        elif cmd == "/collect":
            await event.respond("در حال جمع‌آوری...")
            n = await run_collect()
            await event.respond("از قبل در حال اجراست." if n is None else f"✅ {n} کاربر جدید به صف اضافه شد.")
        elif cmd == "/adduser":
            await event.respond(add_users_from(arg))

        # ---- stats / review
        elif cmd in ("/stats", "/status"):
            await event.respond(stats_text())
        elif cmd == "/users":
            text, _, _ = users_page(int(arg) if arg.isdigit() else 1)
            await reply_long(event, text.split("\n"))
        elif cmd == "/ids":
            q = "SELECT user_id, channel_id FROM results WHERE status='added'"
            params = ()
            if arg:
                ent = await resolve(arg)
                q += " AND channel_id=?"
                params = (ent.id,)
            rows = db.execute(q, params).fetchall()
            await reply_long(event, [f"{u} → {c}" for u, c in rows], "هنوز کسی اد نشده.")
        elif cmd == "/allids":
            await bot.send_file(event.chat_id, build_csv(), caption="همه‌ی آیدی‌های جمع‌شده از ابتدا")

        # ---- control
        elif cmd == "/run":
            set_setting("running", "1")
            await event.respond("▶️ شروع شد.")
        elif cmd == "/pause":
            set_setting("running", "0")
            await event.respond("⏸ متوقف شد.")
        else:
            await send_menu(event)
    except Exception as e:
        log.exception("cmd error")
        await event.respond(friendly(e))


async def main():
    global ME_ID
    await user.connect()
    if not await user.is_user_authorized():
        raise SystemExit("SESSION_STRING نامعتبر یا منقضی است. دوباره با gen_session.py بساز.")
    ME_ID = (await user.get_me()).id
    for s in [x.strip().lstrip("@") for x in SENDERS_ENV.split(",") if x.strip()]:
        try:
            ent = await resolve(s)
            if isinstance(ent, User):
                db.execute("INSERT OR REPLACE INTO senders VALUES(?,?)", (ent.id, display_name(ent)))
                db.commit()
        except Exception:
            log.exception("sender %s from env failed", s)
    await bot.start(bot_token=BOT_TOKEN)
    text, rows = main_view()
    await notify("✅ سرویس بالا آمد\n\n" + text, buttons=rows)
    await asyncio.gather(
        worker(), resolver(), auto_loop(), bot.run_until_disconnected(), user.run_until_disconnected()
    )


if __name__ == "__main__":
    asyncio.run(main())
