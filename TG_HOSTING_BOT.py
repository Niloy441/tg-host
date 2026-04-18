# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════╗
║      BOT MANAGER PRO  v2.0           ║
║   Multi-User | VPS Linux Ready       ║
║   Supports: Python, Node.js, PHP     ║
╚══════════════════════════════════════╝
"""

import telebot
import subprocess
import os
import zipfile
import shutil
import signal
import threading
import sqlite3
import logging
import logging.handlers
import time
import psutil
import requests
import sys
import atexit
from telebot import types
from datetime import datetime

# ─────────────────────────────────────────
#           CONFIGURATION — এখানে দাও
# ─────────────────────────────────────────
TOKEN    = '8729646041:AAGqMD2wxksI1yXel266pswwVnnSwYe-snY'   # @BotFather থেকে
OWNER_ID = 7038931465                      # @userinfobot থেকে তোমার ID

FREE_LIMIT    = 3
PREMIUM_LIMIT = 10
ADMIN_LIMIT   = 50
OWNER_LIMIT   = float('inf')

# ─────────────────────────────────────────
#               PATHS
# ─────────────────────────────────────────
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
BOTS_DIR = os.path.join(BASE_DIR, 'bots')
DATA_DIR = os.path.join(BASE_DIR, 'data')
LOGS_DIR = os.path.join(BASE_DIR, 'logs')
DB_PATH  = os.path.join(DATA_DIR, 'manager.db')

for _d in [BOTS_DIR, DATA_DIR, LOGS_DIR]:
    os.makedirs(_d, exist_ok=True)

# ─────────────────────────────────────────
#               LOGGING
# ─────────────────────────────────────────
_rot_handler = logging.handlers.RotatingFileHandler(
    os.path.join(LOGS_DIR, 'manager.log'),
    maxBytes=5 * 1024 * 1024,
    backupCount=3,
    encoding='utf-8'
)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[_rot_handler, logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
#               GLOBAL STATE
# ─────────────────────────────────────────
bot            = telebot.TeleBot(TOKEN, parse_mode='HTML')
bot_start_time = datetime.now()
running_bots   = {}
bot_locked     = False

# ─────────────────────────────────────────
#               DATABASE
# ─────────────────────────────────────────
def _conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def init_db():
    with _conn() as c:
        c.execute('''CREATE TABLE IF NOT EXISTS users (
            user_id    INTEGER PRIMARY KEY,
            username   TEXT,
            role       TEXT    DEFAULT 'free',
            join_date  TEXT,
            file_count INTEGER DEFAULT 0
        )''')
        c.execute('''CREATE TABLE IF NOT EXISTS logs (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id   INTEGER,
            action    TEXT,
            detail    TEXT,
            timestamp TEXT
        )''')
        c.execute('''CREATE TABLE IF NOT EXISTS banned (
            user_id INTEGER PRIMARY KEY
        )''')
        c.execute('''CREATE TABLE IF NOT EXISTS user_states (
            user_id INTEGER PRIMARY KEY,
            state   TEXT,
            updated TEXT
        )''')

def db_save_user(user_id, username):
    with _conn() as c:
        c.execute('''
            INSERT INTO users (user_id, username, role, join_date)
            VALUES (?, ?, 'free', ?)
            ON CONFLICT(user_id) DO UPDATE SET username=excluded.username
        ''', (user_id, username, datetime.now().strftime('%Y-%m-%d %H:%M')))

def db_get_role(user_id):
    if user_id == OWNER_ID:
        return 'owner'
    with _conn() as c:
        row = c.execute("SELECT role FROM users WHERE user_id=?", (user_id,)).fetchone()
    return row[0] if row else 'free'

def db_set_role(user_id, role):
    with _conn() as c:
        c.execute("UPDATE users SET role=? WHERE user_id=?", (role, user_id))

def db_is_banned(user_id):
    with _conn() as c:
        return c.execute("SELECT 1 FROM banned WHERE user_id=?", (user_id,)).fetchone() is not None

def db_ban(user_id):
    with _conn() as c:
        c.execute("INSERT OR IGNORE INTO banned (user_id) VALUES (?)", (user_id,))

def db_unban(user_id):
    with _conn() as c:
        c.execute("DELETE FROM banned WHERE user_id=?", (user_id,))

def db_log(user_id, action, detail=""):
    with _conn() as c:
        c.execute(
            "INSERT INTO logs (user_id, action, detail, timestamp) VALUES (?,?,?,?)",
            (user_id, action, detail, datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        )

def db_all_users():
    with _conn() as c:
        return c.execute(
            "SELECT user_id, username, role FROM users ORDER BY join_date DESC"
        ).fetchall()

def db_set_state(user_id, state):
    with _conn() as c:
        c.execute('''
            INSERT INTO user_states (user_id, state, updated)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET state=excluded.state, updated=excluded.updated
        ''', (user_id, state, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

def db_get_state(user_id):
    with _conn() as c:
        row = c.execute("SELECT state FROM user_states WHERE user_id=?", (user_id,)).fetchone()
    return row[0] if row else None

def db_clear_state(user_id):
    with _conn() as c:
        c.execute("DELETE FROM user_states WHERE user_id=?", (user_id,))

# ─────────────────────────────────────────
#               HELPERS
# ─────────────────────────────────────────
def get_uptime():
    u   = datetime.now() - bot_start_time
    tot = u.days * 86400 + u.seconds
    h, r = divmod(tot % 86400, 3600)
    m, s = divmod(r, 60)
    return f"{u.days}d {h}h {m}m {s}s"

def fmt_size(b):
    for unit in ['B', 'KB', 'MB', 'GB']:
        if b < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} TB"

def limit_str(lim):
    return '∞' if lim == float('inf') else str(int(lim))

def get_limit(user_id):
    role = db_get_role(user_id)
    return {'owner': OWNER_LIMIT, 'admin': ADMIN_LIMIT,
            'premium': PREMIUM_LIMIT, 'free': FREE_LIMIT}.get(role, FREE_LIMIT)

def get_user_dir(user_id):
    d = os.path.join(BOTS_DIR, str(user_id))
    os.makedirs(d, exist_ok=True)
    return d

def list_user_files(user_id):
    d = get_user_dir(user_id)
    return sorted(f for f in os.listdir(d)
                  if os.path.isfile(os.path.join(d, f)) or os.path.isdir(os.path.join(d, f)))

def count_user_files(user_id):
    return len(list_user_files(user_id))

def is_running(user_id, fname):
    key  = f"{user_id}_{fname}"
    info = running_bots.get(key)
    if not info:
        return False
    if info.get('process') and info['process'].poll() is None:
        return True
    running_bots.pop(key, None)
    return False

def kill_proc(info):
    proc = info.get('process')
    if not proc:
        return
    try:
        pgid = os.getpgid(proc.pid)
        os.killpg(pgid, signal.SIGKILL)
        return
    except (ProcessLookupError, OSError):
        pass
    try:
        parent = psutil.Process(proc.pid)
        for child in parent.children(recursive=True):
            child.kill()
        parent.kill()
        return
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        pass
    try:
        proc.kill()
    except Exception:
        pass

_PY_ENTRIES  = ['main.py', 'bot.py', 'index.py', 'app.py', 'run.py', 'start.py']
_JS_ENTRIES  = ['index.js', 'main.js', 'bot.js', 'app.js', 'server.js', 'run.js',
                os.path.join('src', 'index.js'), os.path.join('src', 'main.js')]
_PHP_ENTRIES = ['index.php', 'bot.php', 'main.php', 'app.php', 'run.php']

def detect_run_cmd(target):
    if os.path.isdir(target):
        for name in _PY_ENTRIES:
            p = os.path.join(target, name)
            if os.path.exists(p):
                return ['python3', p]
        for name in _JS_ENTRIES:
            p = os.path.join(target, name)
            if os.path.exists(p):
                return ['node', p]
        for name in _PHP_ENTRIES:
            p = os.path.join(target, name)
            if os.path.exists(p):
                return ['php', p]
        return None
    ext = os.path.splitext(target)[1].lower()
    if ext == '.py':  return ['python3', target]
    if ext == '.js':  return ['node',    target]
    if ext == '.php': return ['php',     target]
    return None

def install_deps(folder):
    msgs = []
    req = os.path.join(folder, 'requirements.txt')
    pkg = os.path.join(folder, 'package.json')
    if os.path.exists(req):
        r = subprocess.run(
            ['pip3', 'install', '-r', req, '--quiet'],
            capture_output=True, text=True, timeout=120
        )
        msgs.append(f"📦 pip install: {'✅' if r.returncode == 0 else '❌'}")
    if os.path.exists(pkg):
        r = subprocess.run(
            ['npm', 'install', '--prefix', folder, '--silent'],
            capture_output=True, text=True, timeout=120
        )
        msgs.append(f"📦 npm install: {'✅' if r.returncode == 0 else '❌'}")
    return msgs

def is_admin_or_owner(user_id):
    return db_get_role(user_id) in ('admin', 'owner')

def check_access(message):
    uid = message.from_user.id
    if db_is_banned(uid):
        bot.reply_to(message, "🚫 তুমি ব্যান করা হয়েছ।")
        return False
    if bot_locked and not is_admin_or_owner(uid):
        bot.reply_to(message, "🔒 বট লক আছে। পরে চেষ্টা করো।")
        return False
    return True

TG_MAX = 4096

def _truncate(text, limit=TG_MAX):
    if len(text) <= limit:
        return text
    return text[:limit - 30] + '\n\n<i>...(truncated)</i>'

def safe_send(chat_id, text, **kwargs):
    text = _truncate(text)
    return bot.send_message(chat_id, text, **kwargs)

def safe_edit(chat_id, msg_id, text, **kwargs):
    text = _truncate(text)
    try:
        return bot.edit_message_text(text, chat_id, msg_id, **kwargs)
    except Exception as e:
        if 'message is not modified' in str(e).lower():
            return None
        try:
            return safe_send(chat_id, text, **kwargs)
        except Exception:
            return None

# ─────────────────────────────────────────
#               KEYBOARDS
# ─────────────────────────────────────────
def main_kb(user_id):
    m = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    m.row("📤 ফাইল আপলোড", "📂 আমার ফাইলসমূহ")
    m.row("🟢 চলমান বট",   "📊 স্ট্যাটাস")
    if is_admin_or_owner(user_id):
        m.row("👑 অ্যাডমিন প্যানেল", "📢 ব্রডকাস্ট")
    m.row("ℹ️ সাহায্য")
    return m

def file_kb(user_id, fname):
    running = is_running(user_id, fname)
    m = types.InlineKeyboardMarkup(row_width=2)
    if running:
        m.add(
            types.InlineKeyboardButton("🛑 বন্ধ করো",  callback_data=f"stop|{fname}"),
            types.InlineKeyboardButton("📋 লগ দেখো",   callback_data=f"log|{fname}")
        )
        m.add(types.InlineKeyboardButton("🔄 রিস্টার্ট", callback_data=f"restart|{fname}"))
    else:
        m.add(
            types.InlineKeyboardButton("▶️ চালাও",      callback_data=f"run|{fname}"),
            types.InlineKeyboardButton("🗑️ মুছে ফেলো",  callback_data=f"delete|{fname}")
        )
        m.add(types.InlineKeyboardButton("📥 ডাউনলোড",  callback_data=f"download|{fname}"))
    m.add(types.InlineKeyboardButton("🔙 ফিরে যাও", callback_data="back_files"))
    return m

def admin_kb():
    m = types.InlineKeyboardMarkup(row_width=2)
    m.add(
        types.InlineKeyboardButton("👥 ইউজার লিস্ট",    callback_data="adm_users"),
        types.InlineKeyboardButton("🟢 সব চলমান বট",    callback_data="adm_running")
    )
    m.add(
        types.InlineKeyboardButton("🔒 বট লক",          callback_data="adm_lock"),
        types.InlineKeyboardButton("🔓 বট আনলক",        callback_data="adm_unlock")
    )
    m.add(
        types.InlineKeyboardButton("⭐ Premium দাও",    callback_data="adm_setprem"),
        types.InlineKeyboardButton("🚫 ইউজার ব্যান",    callback_data="adm_ban")
    )
    m.add(types.InlineKeyboardButton("✅ ইউজার আনব্যান", callback_data="adm_unban"))
    return m

# ─────────────────────────────────────────
#               /start
# ─────────────────────────────────────────
@bot.message_handler(commands=['start'])
def cmd_start(message):
    uid   = message.from_user.id
    uname = message.from_user.username or "নাম নেই"
    db_save_user(uid, uname)
    if not check_access(message):
        return
    db_log(uid, "START")
    role       = db_get_role(uid)
    role_badge = {'owner':'👑 Owner', 'admin':'⭐ Admin',
                  'premium':'💎 Premium', 'free':'👤 Free'}.get(role, '👤 Free')
    lim  = get_limit(uid)
    used = count_user_files(uid)
    text = (
        "╔══════════════════════════════════╗\n"
        "║     🤖 <b>BOT MANAGER PRO  v2</b>     ║\n"
        "╠══════════════════════════════════╣\n"
        f"║  স্বাগতম, <b>{message.from_user.first_name}</b>!\n"
        "║\n"
        f"║  🏷️ রোল: {role_badge}\n"
        f"║  📁 ফাইল: {used}/{limit_str(lim)}\n"
        f"║  🕐 আপটাইম: {get_uptime()}\n"
        "╠══════════════════════════════════╣\n"
        "║  ✅ Python / Node.js / PHP সাপোর্ট\n"
        "║  ✅ ZIP ফাইল এক্সট্র্যাক্ট হয়\n"
        "║  ✅ Auto dependency install\n"
        "╚══════════════════════════════════╝"
    )
    safe_send(message.chat.id, text, reply_markup=main_kb(uid))

# ─────────────────────────────────────────
#               /help
# ─────────────────────────────────────────
@bot.message_handler(commands=['help'])
def cmd_help(message):
    text = (
        "<b>📖 সাহায্য গাইড</b>\n\n"
        "<b>ফাইল আপলোড:</b>\n"
        "• .py, .js, .php ফাইল সরাসরি পাঠাও\n"
        "• অথবা ZIP করে পাঠাও (ফোল্ডার সহ)\n\n"
        "<b>বট চালানো:</b>\n"
        "• ফাইল লিস্টে গিয়ে ▶️ চাপো\n"
        "• requirements.txt থাকলে auto pip install হবে\n"
        "• package.json থাকলে npm install হবে\n\n"
        "<b>ZIP entry point:</b>\n"
        "Python: main.py, bot.py, run.py, app.py, start.py\n"
        "Node.js: index.js, server.js, src/index.js\n"
        "PHP: index.php, bot.php, main.php\n\n"
        "<b>কমান্ড:</b>\n"
        "/start — শুরু\n"
        "/help — সাহায্য\n"
        "/status — সার্ভার স্ট্যাটাস\n"
        "/mybots — চলমান বট\n"
        "/addadmin &lt;id&gt; — অ্যাডমিন বানাও\n"
        "/removeadmin &lt;id&gt; — অ্যাডমিন সরাও"
    )
    safe_send(message.chat.id, text)

# ─────────────────────────────────────────
#               /status
# ─────────────────────────────────────────
@bot.message_handler(commands=['status'])
def cmd_status(message):
    if not check_access(message):
        return
    cpu   = psutil.cpu_percent(interval=1)
    mem   = psutil.virtual_memory()
    disk  = psutil.disk_usage('/')
    total = sum(1 for v in running_bots.values()
                if v.get('process') and v['process'].poll() is None)
    text = (
        "╔══════════════════════════════════╗\n"
        "║       📊 <b>সার্ভার স্ট্যাটাস</b>         ║\n"
        "╠══════════════════════════════════╣\n"
        f"║  🖥️ CPU:  {cpu:.1f}%\n"
        f"║  💾 RAM:  {mem.percent:.1f}% ({fmt_size(mem.used)}/{fmt_size(mem.total)})\n"
        f"║  💿 Disk: {disk.percent:.1f}% ({fmt_size(disk.used)}/{fmt_size(disk.total)})\n"
        f"║  🤖 চলমান বট: {total}\n"
        f"║  ⏱️ আপটাইম: {get_uptime()}\n"
        "╚══════════════════════════════════╝"
    )
    safe_send(message.chat.id, text)

# ─────────────────────────────────────────
#               FILE UPLOAD
# ─────────────────────────────────────────
@bot.message_handler(content_types=['document'])
def handle_upload(message):
    if not check_access(message):
        return
    uid  = message.from_user.id
    doc  = message.document
    used = count_user_files(uid)
    lim  = get_limit(uid)

    if lim != float('inf') and used >= lim:
        safe_send(message.chat.id,
            f"❌ ফাইল লিমিট পূর্ণ! ({used}/{limit_str(lim)})\n"
            "💎 Premium নিতে অ্যাডমিনের সাথে যোগাযোগ করো।")
        return

    fname = doc.file_name
    ext   = os.path.splitext(fname)[1].lower()
    if ext not in ('.py', '.js', '.php', '.zip'):
        bot.reply_to(message, "❌ শুধু .py, .js, .php বা .zip ফাইল আপলোড করা যাবে।")
        return

    msg = bot.reply_to(message, "⏳ আপলোড হচ্ছে...")
    try:
        user_dir  = get_user_dir(uid)
        save_path = os.path.join(user_dir, fname)

        file_info = bot.get_file(doc.file_id)
        url       = f"https://api.telegram.org/file/bot{TOKEN}/{file_info.file_path}"
        with requests.get(url, stream=True, timeout=60) as resp:
            resp.raise_for_status()
            with open(save_path, 'wb') as fout:
                for chunk in resp.iter_content(chunk_size=65536):
                    if chunk:
                        fout.write(chunk)

        dep_msgs = []
        if ext == '.zip':
            folder_name = os.path.splitext(fname)[0]
            extract_dir = os.path.join(user_dir, folder_name)
            os.makedirs(extract_dir, exist_ok=True)
            with zipfile.ZipFile(save_path, 'r') as z:
                z.extractall(extract_dir)
            os.remove(save_path)
            dep_msgs     = install_deps(extract_dir)
            display_name = folder_name + "/"
        else:
            display_name = fname

        db_log(uid, "UPLOAD", fname)
        dep_text = ("\n" + "\n".join(dep_msgs)) if dep_msgs else ""
        safe_edit(message.chat.id, msg.message_id,
            f"✅ <b>আপলোড সফল!</b>\n"
            f"📁 ফাইল: <code>{display_name}</code>{dep_text}\n\n"
            "📂 ফাইল লিস্টে গিয়ে চালাও।")
    except Exception as e:
        logger.error(f"Upload error: {e}")
        safe_edit(message.chat.id, msg.message_id, f"❌ আপলোড ব্যর্থ: {str(e)[:300]}")

# ─────────────────────────────────────────
#               FILE LIST
# ─────────────────────────────────────────
def show_file_list(chat_id, user_id, edit_msg_id=None):
    files = list_user_files(user_id)
    lim   = get_limit(user_id)
    if not files:
        text = "📂 কোনো ফাইল নেই। প্রথমে আপলোড করো।"
        if edit_msg_id: safe_edit(chat_id, edit_msg_id, text)
        else:           safe_send(chat_id, text)
        return
    m = types.InlineKeyboardMarkup(row_width=1)
    for f in files:
        icon = "🟢" if is_running(user_id, f) else "⚪"
        m.add(types.InlineKeyboardButton(f"{icon} {f}", callback_data=f"file|{f}"))
    text = f"📂 <b>তোমার ফাইলসমূহ</b> ({len(files)}/{limit_str(lim)})"
    if edit_msg_id: safe_edit(chat_id, edit_msg_id, text, reply_markup=m)
    else:           safe_send(chat_id, text, reply_markup=m)

# ─────────────────────────────────────────
#               RUNNING BOTS LIST
# ─────────────────────────────────────────
def show_running_bots(chat_id, user_id, edit_msg_id=None):
    prefix = f"{user_id}_"
    active = []
    for key, info in list(running_bots.items()):
        if key.startswith(prefix) and info.get('process') and info['process'].poll() is None:
            fname   = key[len(prefix):]
            elapsed = int((datetime.now() - info['start_time']).total_seconds() // 60)
            active.append((fname, elapsed))
    if not active:
        text = "🔴 কোনো বট চলছে না।"
    else:
        lines = "\n".join(f"🟢 <code>{f}</code> — {m}মিনিট" for f, m in active)
        text  = f"<b>🟢 চলমান বট ({len(active)}টি):</b>\n\n{lines}"
    if edit_msg_id: safe_edit(chat_id, edit_msg_id, text)
    else:           safe_send(chat_id, text)

# ─────────────────────────────────────────
#               RUN BOT
# ─────────────────────────────────────────
def run_bot(chat_id, user_id, fname):
    user_dir = get_user_dir(user_id)
    target   = os.path.join(user_dir, fname)
    key      = f"{user_id}_{fname}"

    if is_running(user_id, fname):
        safe_send(chat_id, f"⚠️ <code>{fname}</code> ইতোমধ্যে চলছে।")
        return

    cmd = detect_run_cmd(target)
    if not cmd:
        safe_send(chat_id, "❌ এই ফাইল চালানো যাচ্ছে না। (py/js/php দরকার)")
        return

    log_path = os.path.join(LOGS_DIR, f"{user_id}_{fname}.log")
    try:
        log_file = open(log_path, 'a', encoding='utf-8', errors='ignore')
        process  = subprocess.Popen(
            cmd,
            stdout=log_file, stderr=subprocess.STDOUT,
            text=True, encoding='utf-8', errors='ignore',
            cwd=(os.path.dirname(target) if os.path.isfile(target) else target),
            start_new_session=True
        )
        running_bots[key] = {
            'process':    process,
            'user_id':    user_id,
            'fname':      fname,
            'start_time': datetime.now(),
            'log_path':   log_path,
            'log_file':   log_file
        }
        time.sleep(1.5)
        if process.poll() is None:
            db_log(user_id, "RUN", fname)
            safe_send(chat_id,
                f"✅ <b>বট চালু হয়েছে!</b>\n"
                f"📁 ফাইল: <code>{fname}</code>\n"
                f"🆔 PID: {process.pid}\n"
                f"⏱️ সময়: {datetime.now().strftime('%H:%M:%S')}",
                reply_markup=file_kb(user_id, fname))
        else:
            log_file.close()
            with open(log_path, 'r', errors='ignore') as f:
                err = f.read()[-800:]
            running_bots.pop(key, None)
            safe_send(chat_id, f"❌ <b>বট চালু হয়নি!</b>\n<pre>{_truncate(err, 600)}</pre>")
    except Exception as e:
        safe_send(chat_id, f"❌ Error: {str(e)[:300]}")

# ─────────────────────────────────────────
#               STOP BOT
# ─────────────────────────────────────────
def stop_bot(chat_id, user_id, fname):
    key  = f"{user_id}_{fname}"
    info = running_bots.get(key)
    if not info:
        safe_send(chat_id, "⚠️ বট চলছে না।")
        return
    kill_proc(info)
    try:
        lf = info.get('log_file')
        if lf: lf.close()
    except Exception:
        pass
    running_bots.pop(key, None)
    db_log(user_id, "STOP", fname)
    safe_send(chat_id,
        f"🛑 <b>বট বন্ধ হয়েছে!</b>\n📁 <code>{fname}</code>",
        reply_markup=file_kb(user_id, fname))

def show_logs(chat_id, user_id, fname):
    log_path = os.path.join(LOGS_DIR, f"{user_id}_{fname}.log")
    if not os.path.exists(log_path):
        safe_send(chat_id, "📋 কোনো লগ নেই।")
        return
    with open(log_path, 'r', errors='ignore') as f:
        lines = f.readlines()
    last = "".join(lines[-50:])
    if not last.strip():
        last = "(লগ খালি)"
    safe_send(chat_id, f"📋 <b>লগ — {fname}:</b>\n<pre>{_truncate(last, 3500)}</pre>")

# ─────────────────────────────────────────
#               CALLBACK QUERY
# ─────────────────────────────────────────
@bot.callback_query_handler(func=lambda c: True)
def handle_callback(call):
    uid  = call.from_user.id
    cid  = call.message.chat.id
    mid  = call.message.message_id
    data = call.data

    if db_is_banned(uid):
        bot.answer_callback_query(call.id, "🚫 ব্যান")
        return
    bot.answer_callback_query(call.id)

    if data == "back_files":
        show_file_list(cid, uid, edit_msg_id=mid)
        return

    if data.startswith("file|"):
        fname  = data[5:]
        status = "🟢 চলছে" if is_running(uid, fname) else "🔴 বন্ধ"
        safe_edit(cid, mid, f"📁 <b>{fname}</b>\n🔘 স্ট্যাটাস: {status}",
                  reply_markup=file_kb(uid, fname))
        return

    if data.startswith("run|"):
        fname = data[4:]
        safe_edit(cid, mid, f"⏳ <code>{fname}</code> চালু হচ্ছে...")
        threading.Thread(target=run_bot, args=(cid, uid, fname), daemon=True).start()
        return

    if data.startswith("stop|"):
        stop_bot(cid, uid, data[5:])
        return

    if data.startswith("restart|"):
        fname = data[8:]
        stop_bot(cid, uid, fname)
        time.sleep(0.5)
        threading.Thread(target=run_bot, args=(cid, uid, fname), daemon=True).start()
        return

    if data.startswith("log|"):
        show_logs(cid, uid, data[4:])
        return

    if data.startswith("delete|"):
        fname = data[7:]
        if is_running(uid, fname):
            safe_send(cid, "⚠️ আগে বট বন্ধ করো, তারপর মুছো।")
            return
        target = os.path.join(get_user_dir(uid), fname)
        try:
            shutil.rmtree(target) if os.path.isdir(target) else os.remove(target)
            db_log(uid, "DELETE", fname)
            safe_edit(cid, mid, f"🗑️ <code>{fname}</code> মুছে ফেলা হয়েছে।")
        except Exception as e:
            safe_send(cid, f"❌ মুছতে ব্যর্থ: {str(e)[:200]}")
        return

    if data.startswith("download|"):
        fname  = data[9:]
        target = os.path.join(get_user_dir(uid), fname)
        try:
            if os.path.isdir(target):
                zip_base = target + "_dl"
                shutil.make_archive(zip_base, 'zip', target)
                with open(zip_base + ".zip", 'rb') as f:
                    bot.send_document(cid, f, caption=f"📥 {fname}.zip")
                os.remove(zip_base + ".zip")
            else:
                with open(target, 'rb') as f:
                    bot.send_document(cid, f, caption=f"📥 {fname}")
        except Exception as e:
            safe_send(cid, f"❌ ডাউনলোড ব্যর্থ: {str(e)[:200]}")
        return

    if not is_admin_or_owner(uid):
        safe_send(cid, "❌ অনুমতি নেই।")
        return

    if data == "adm_users":
        users = db_all_users()
        lines = [f"👤 <code>{u[0]}</code> @{u[1] or '-'} [{u[2]}]" for u in users[:30]]
        text  = "<b>👥 ইউজার লিস্ট:</b>\n\n" + "\n".join(lines)
        if len(users) > 30:
            text += f"\n...আরো {len(users)-30} জন"
        safe_send(cid, text)
        return

    if data == "adm_running":
        active = [(k, v) for k, v in running_bots.items()
                  if v.get('process') and v['process'].poll() is None]
        if not active:
            safe_send(cid, "🔴 কোনো বট চলছে না।")
        else:
            lines = [f"🟢 <code>{k}</code>" for k, _ in active]
            safe_send(cid, "<b>🟢 সব চলমান বট:</b>\n\n" + "\n".join(lines))
        return

    if data == "adm_lock":
        global bot_locked
        bot_locked = True
        safe_send(cid, "🔒 বট লক হয়েছে।")
        return

    if data == "adm_unlock":
        bot_locked = False
        safe_send(cid, "🔓 বট আনলক হয়েছে।")
        return

    if data in ("adm_setprem", "adm_ban", "adm_unban"):
        state_map = {"adm_setprem": "premium_uid", "adm_ban": "ban_uid", "adm_unban": "unban_uid"}
        label_map = {"adm_setprem": "Premium দিতে", "adm_ban": "ব্যান করতে", "adm_unban": "আনব্যান করতে"}
        db_set_state(uid, state_map[data])
        safe_send(cid, f"✍️ {label_map[data]} ইউজারের ID পাঠাও:")
        return

# ─────────────────────────────────────────
#               TEXT HANDLER
# ─────────────────────────────────────────
@bot.message_handler(content_types=['text'])
def handle_text(message):
    if not check_access(message):
        return
    uid  = message.from_user.id
    text = message.text.strip()

    state = db_get_state(uid)
    if state:
        db_clear_state(uid)
        if state == "broadcast_msg":
            users    = db_all_users()
            sent = fail = 0
            for u in users:
                try:
                    safe_send(u[0], f"📢 <b>অ্যাডমিন বার্তা:</b>\n\n{text}")
                    sent += 1
                    time.sleep(0.05)
                except Exception:
                    fail += 1
            bot.reply_to(message, f"📢 ব্রডকাস্ট সম্পন্ন!\n✅ পাঠানো: {sent}\n❌ ব্যর্থ: {fail}")
            return
        try:
            target_id = int(text)
        except ValueError:
            bot.reply_to(message, "❌ সঠিক ID দাও।")
            return
        if state == "premium_uid":
            db_save_user(target_id, "unknown")
            db_set_role(target_id, 'premium')
            bot.reply_to(message, f"✅ {target_id} কে Premium করা হয়েছে।")
        elif state == "ban_uid":
            db_ban(target_id)
            bot.reply_to(message, f"🚫 {target_id} ব্যান হয়েছে।")
        elif state == "unban_uid":
            db_unban(target_id)
            bot.reply_to(message, f"✅ {target_id} আনব্যান হয়েছে।")
        return

    if text == "📤 ফাইল আপলোড":
        bot.reply_to(message, "📤 .py, .js, .php বা .zip ফাইল পাঠাও।")
    elif text == "📂 আমার ফাইলসমূহ":
        show_file_list(message.chat.id, uid)
    elif text == "🟢 চলমান বট":
        show_running_bots(message.chat.id, uid)
    elif text == "📊 স্ট্যাটাস":
        cmd_status(message)
    elif text == "👑 অ্যাডমিন প্যানেল":
        if not is_admin_or_owner(uid):
            bot.reply_to(message, "❌ অনুমতি নেই।")
            return
        safe_send(message.chat.id, "👑 <b>অ্যাডমিন প্যানেল</b>", reply_markup=admin_kb())
    elif text == "📢 ব্রডকাস্ট":
        if not is_admin_or_owner(uid):
            bot.reply_to(message, "❌ অনুমতি নেই।")
            return
        db_set_state(uid, "broadcast_msg")
        bot.reply_to(message, "✍️ যে বার্তা পাঠাতে চাও লেখো:")
    elif text == "ℹ️ সাহায্য":
        cmd_help(message)
    else:
        bot.reply_to(message, "⬇️ নিচের বাটন ব্যবহার করো।", reply_markup=main_kb(uid))

# ─────────────────────────────────────────
#               ADMIN COMMANDS
# ─────────────────────────────────────────
@bot.message_handler(commands=['addadmin'])
def cmd_addadmin(message):
    if message.from_user.id != OWNER_ID:
        return
    parts = message.text.split()
    if len(parts) < 2:
        bot.reply_to(message, "Usage: /addadmin <user_id>")
        return
    try:
        t = int(parts[1])
        db_save_user(t, "unknown")
        db_set_role(t, 'admin')
        bot.reply_to(message, f"✅ {t} অ্যাডমিন হয়েছে।")
    except ValueError:
        bot.reply_to(message, "❌ সঠিক ID দাও।")

@bot.message_handler(commands=['removeadmin'])
def cmd_removeadmin(message):
    if message.from_user.id != OWNER_ID:
        return
    parts = message.text.split()
    if len(parts) < 2:
        bot.reply_to(message, "Usage: /removeadmin <user_id>")
        return
    try:
        t = int(parts[1])
        db_set_role(t, 'free')
        bot.reply_to(message, f"✅ {t} অ্যাডমিন থেকে সরানো হয়েছে।")
    except ValueError:
        bot.reply_to(message, "❌ সঠিক ID দাও।")

@bot.message_handler(commands=['mybots'])
def cmd_mybots(message):
    if not check_access(message):
        return
    show_running_bots(message.chat.id, message.from_user.id)

# ─────────────────────────────────────────
#               CLEANUP
# ─────────────────────────────────────────
def cleanup():
    logger.info("Cleaning up...")
    for key, info in list(running_bots.items()):
        try:
            kill_proc(info)
            lf = info.get('log_file')
            if lf: lf.close()
        except Exception:
            pass

atexit.register(cleanup)

def _sig_handler(sig, frame):
    cleanup()
    sys.exit(0)

signal.signal(signal.SIGTERM, _sig_handler)
signal.signal(signal.SIGINT,  _sig_handler)

# ─────────────────────────────────────────
#               MAIN
# ─────────────────────────────────────────
def main():
    init_db()
    logger.info("=" * 50)
    logger.info("🤖 BOT MANAGER PRO v2.0 চালু হচ্ছে...")
    logger.info(f"📁 Base : {BASE_DIR}")
    logger.info(f"💾 DB   : {DB_PATH}")
    logger.info("=" * 50)

    restart_delay = 5
    max_delay     = 120

    while True:
        try:
            logger.info("🚀 Polling শুরু...")
            restart_delay = 5
            bot.infinity_polling(timeout=60, long_polling_timeout=30)

        except requests.exceptions.ConnectionError as e:
            logger.warning(f"🔌 Connection error: {e}")
            time.sleep(restart_delay)
        except requests.exceptions.ReadTimeout:
            time.sleep(3)
        except requests.exceptions.Timeout as e:
            logger.warning(f"⏱️ Timeout: {e}")
            time.sleep(restart_delay)
        except requests.exceptions.HTTPError as e:
            logger.error(f"🌐 HTTP error: {e}")
            time.sleep(restart_delay)
        except requests.exceptions.ChunkedEncodingError:
            time.sleep(5)
        except requests.exceptions.RequestException as e:
            logger.error(f"🌐 Request error: {e}")
            time.sleep(restart_delay)
        except telebot.apihelper.ApiTelegramException as e:
            logger.error(f"📱 Telegram API error: {e}")
            if 'Unauthorized' in str(e):
                logger.critical("❌ TOKEN ভুল! বট বন্ধ হচ্ছে।")
                sys.exit(1)
            time.sleep(restart_delay)
        except Exception as e:
            logger.error(f"💥 Unexpected error: {e}", exc_info=True)
            time.sleep(restart_delay)
            restart_delay = min(restart_delay * 2, max_delay)

if __name__ == "__main__":
    main()
