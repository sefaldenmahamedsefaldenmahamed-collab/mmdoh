"""
═══════════════════════════════════════════════════════════════════════
  🧭 البوت — استراتيجية Triple Confluence Navigator  (V2 — إعادة بناء مدقّقة)
═══════════════════════════════════════════════════════════════════════
  تنفيذ كامل لاستراتيجية Pine Script v6:
    "Triple Confluence Navigator [MarkitTick]" © MarkitTick (CC BY-NC-SA 4.0)

  الاستراتيجية تجمع 3 منظومات (Confluence) خلال نافذة 5 شموع:
    1) Cardwell RSI   — تقاطع MA السريع (Kalman/RMA/LLAMA) فوق البطيء على RSI
    2) Adaptive ARSI  — تقاطع ARSI فوق/تحت Supertrend (دمج ARSI + Supertrend)
    3) Structure BOS  — كسر آخر قمة/قاع سوينغ (Break of Structure)

  ما تمت مراجعته وتصحيحه في V2 (مطابقة حرفية للمؤشر "كود جديد.txt"):
    ✅ الدوال الحسابية: ta.rsi (حالة down==0→100)، f_kalman (تحديث _cov على كل
       شمعة وتلعق na وإعادة البذر)، f_llama (دلالة ternary مع na)، f_arsi
       (_diff=_d في التسخين، وden=na⇒50)، nz(atrValue[1])، عدّادات barsSince.
    ✅ rawBullCross2/BearCross2 = ta.crossover(arsi[1], rawMerged[1]) دون إزاحات.
    ✅ barstate.isconfirmed: الحساب على الشموع المغلقة المؤكدة فقط، والإشارة
       يجب أن تكون ضمن confluenceWindow من آخر شمعة مغلقة.
    ✅ محاذاة request.security(...[1], lookahead_on): آخر شمعة HTF مغلقة فقط
       (open+duration <= فتح شمعة الفريم الأساسي) — دون تسريب الشمعة الجارية.
    ✅ حساب entry=close[1]، riskDist=min(structDist, atr*atrMultSl)، SL/TP1-3.
    ✅ الإغلاق المعاكس: shortSignal يغلق شراء مفتوح وlongSignal يغلق بيعاً مفتوحاً.
    ✅ TP1=40%، TP2=30%، TP3=إغلاق كامل — بنِسَب الكمية الأصلية (orig_quantity)
       وأعلام تُرفع فقط عند نجاح الأمر — لا عدادات عالقة.
    ✅ مزامنة المراكز الحقيقية مع Binance /fapi/v2/positionRisk عند البدء ودوريّاً
       (إزالة الصفقات الشبحية → لا عداد 2/2 وهمي)، وتوحيد MAX_OPEN_TRADES بين
       البيئة والإعدادات واللوحة، وقاعدة البيانات على /data (Volume)، و
       إعادة مزامنة وقت الخادم دورياً + عند -1021، وأسعار قِدَمها مفحوصة.

  ✅ تحديثات هذه المراجعة:
    - WebSocket الأسعار: REST bookTicker احتياطي فقط عند انقطاع WS أو التهيئة.
    - User Data Stream (listenKey) مع تجديد كل 30 دقيقة لمتابعة ORDER_TRADE_UPDATE
      وACCOUNT_UPDATE لحظياً (كشف التصفي/الإغلاق اليدوي فوراً).
    - ws_connected و ws_listenkey يُعرضان في /api/confluence/panel وبطاقة «حالة الاتصال».
    - أولوية السعة: إعداد القاعدة المحفوظ يعلو؛ MAX_OPEN_TRADES من البيئة فقط
      عند عدم وجود إعداد محفوظ.
    - ema() تتحقق من الطول الفارغ وقيم NaN الابتدائية.
    - htf_duration_ms آمن أمام فجوات الوقت؛ lastSwingHigh/Low ضمن components.
    - close_trade عند qty<=0 و realized_pnl==0: يتحقق من مركز Binance الفعلي
      قبل تسجيل خسارة وهمية.
    - OPPOSITE_SIGNAL: إغلاق كامل مؤكد + فحص api_keys_ok و stepSize/tickSize.

  الإعدادات الافتراضية (مطابقة لـ Pine Script):
    rsiLen=14, rmaFastLen=9, rmaSlowLen=45, maType=Kalman
    arsiLen=14, arsiSigLen=14, arsiFiltType=Kalman Filter
    kalmanProcN=0.01, kalmanMeasN=0.10
    llamaAlphaMin=0.05, llamaAlphaMax=0.50
    stAtrLen=14, stFactor=2.0
    swingLen=5
    atrLen=14, atrMultSl=1.5, slStructBuf=0.25
    rr1=1.0, rr2=2.0, rr3=3.0
    confluenceWindow=5
    useHtfFilter=false, htfTimeframe=240
    useChopFilter=false, adxLen=14, adxMinLevel=20.0

  إعدادات التداول (مطلوبة من المستخدم):
    الهامش TRADE_SIZE_USDT = 50
    الرافعة LEVERAGE = 10
    المتزامنة MAX_OPEN_TRADES = 1

  التشغيل: python bot.py  (المتغيرات عبر البيئة:
    BINANCE_API_KEY, BINANCE_API_SECRET, TRADE_ENABLED, TESTNET,
    TRADE_SIZE_USDT, MAX_OPEN_TRADES, LEVERAGE,
    DB_FILE / DATA_DIR, PORT)
═══════════════════════════════════════════════════════════════════════
"""

import asyncio, aiohttp, json, math, os, sys, time, logging
import collections
import pandas as pd
import numpy as np
import hmac
import hashlib
import signal as sys_signal
from urllib.parse import urlencode
from logging.handlers import RotatingFileHandler
try:
    import websockets
except Exception:
    websockets = None
import aiosqlite
from web_panel import start_web_panel, notify as panel_notify

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

logger = logging.getLogger('TripleBot')
logger.setLevel(logging.INFO)
fmt = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
fh = RotatingFileHandler(os.path.join('/tmp', 'bot.log'),
                         maxBytes=2 * 1024 * 1024, backupCount=2, encoding='utf-8')
fh.setFormatter(fmt); logger.addHandler(fh)


class _StdoutOnly(logging.Filter):
    """يسمح للسجلات الأقل من ERROR فقط — ERROR يذهب إلى stderr (أحمر في Railway)."""
    def filter(self, record):
        return record.levelno < logging.ERROR


# ✅ Railway يوسم كل ما يخرج على stderr بـ severity=error — كان كل سجل INFO/WARNING
#    يظهر كخطأ أحمر في واجهة Railway. الآن: INFO/WARNING → stdout، ERROR+ → stderr.
ch = logging.StreamHandler(sys.stdout); ch.setFormatter(fmt)
ch.addFilter(_StdoutOnly()); logger.addHandler(ch)
ce = logging.StreamHandler(sys.stderr); ce.setFormatter(fmt)
ce.setLevel(logging.ERROR); logger.addHandler(ce)
# ✅ منع التكرار المزدوج في سجل Railway: استيراد web_panel يضبط معالجات
#    root (stdout/stderr)، وبدون propagate=False كان كل سجل يُطبع مرتين (ch + root).
logger.propagate = False

try:
    import requests
except Exception:
    requests = None


# ═══════════════════════ مسار قاعدة البيانات ═══════════════════════
RAILWAY_VOLUME_MOUNT = "/data"
RAILWAY_VOLUME_PATH = "/var/lib/containers/railwayapp/bind-mounts/54c0d898-c1f2-44de-a173-2a711d30a4bc/vol_w3cxm5jyou5b58yw"
BASE_DIR = "/tmp"


def resolve_db_path():
    """يختار أول مسار قابل للاستخدام لقاعدة البيانات بترتيب آمن."""
    candidates = []
    env_file = (os.environ.get('DB_FILE') or '').strip()
    if env_file:
        candidates.append(('DB_FILE', env_file))
    data_dir = (os.environ.get('DATA_DIR') or '').strip()
    if data_dir:
        candidates.append(('DATA_DIR', os.path.join(data_dir, "triple_v1.db")))
    if RAILWAY_VOLUME_MOUNT:
        candidates.append(('VOLUME_/data', os.path.join(RAILWAY_VOLUME_MOUNT, "triple_v1.db")))
    if RAILWAY_VOLUME_PATH:
        candidates.append(('RAILWAY_VOLUME', os.path.join(RAILWAY_VOLUME_PATH, "triple_v1.db")))
    candidates.append(('DEFAULT', os.path.join(BASE_DIR, "triple_v1.db")))
    for src, path in candidates:
        folder = os.path.dirname(os.path.abspath(path)) or '.'
        try:
            os.makedirs(folder, exist_ok=True)
            probe = os.path.join(folder, '.db_probe')
            with open(probe, 'w', encoding='utf-8') as f:
                f.write('ok')
            os.remove(probe)
            if src != 'DEFAULT':
                logger.info(f"💾 قاعدة البيانات: {path} (من المتغير {src})")
            return path
        except Exception as e:
            logger.warning(f"⚠️ مسار قاعدة البيانات غير قابل للاستخدام: {path} — السبب: {e}")
    return os.path.join(BASE_DIR, "triple_v1.db")


db_file = resolve_db_path()


# ═══════════════════════ قاعدة البيانات المشتركة ═══════════════════════
class DatabaseManager:
    """قاعدة البيانات: الصفقات الحية + أرباح الأيام + سجل الإشارات + الإعدادات."""

    def __init__(self, db_name=None):
        self.db_name = db_name or db_file

    async def init_db(self):
        async with aiosqlite.connect(self.db_name) as db:
            await db.execute("""CREATE TABLE IF NOT EXISTS active_trades (
                symbol TEXT PRIMARY KEY, side TEXT, entry_price REAL, quantity REAL,
                sl REAL, tp REAL, tp2 REAL, tp3 REAL, trailing_active INTEGER,
                highest_price REAL, lowest_price REAL, entry_time REAL,
                partial_closed INTEGER, partial2_closed INTEGER,
                realized_pnl REAL DEFAULT 0, stop_order_id INTEGER, tp_order_id INTEGER,
                strategy TEXT, margin_usdt REAL DEFAULT 0)""")
            for col_def in ('realized_pnl REAL DEFAULT 0', 'stop_order_id INTEGER',
                            'tp2 REAL', 'tp3 REAL', 'tp_order_id INTEGER', 'strategy TEXT',
                            'margin_usdt REAL DEFAULT 0', 'partial2_closed INTEGER DEFAULT 0',
                            'partial1_closed INTEGER DEFAULT 0',
                            'orig_quantity REAL'):
                try:
                    await db.execute(f"ALTER TABLE active_trades ADD COLUMN {col_def}")
                except Exception:
                    pass
            try:
                await db.execute("""UPDATE active_trades
                    SET partial1_closed = 1
                    WHERE partial_closed = 1 AND (partial1_closed IS NULL OR partial1_closed = 0)""")
            except Exception:
                pass
            try:
                await db.execute("""UPDATE active_trades
                    SET orig_quantity = quantity
                    WHERE orig_quantity IS NULL AND quantity IS NOT NULL""")
            except Exception:
                pass
            await db.execute("""CREATE TABLE IF NOT EXISTS daily_stats (
                date TEXT PRIMARY KEY, realized_pnl REAL, wins INTEGER, losses INTEGER)""")
            await db.execute("""CREATE TABLE IF NOT EXISTS bot_params (
                key TEXT PRIMARY KEY, value TEXT)""")
            await db.execute("""CREATE TABLE IF NOT EXISTS signal_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, direction TEXT,
                entry_ts REAL, score REAL, components TEXT,
                result INTEGER DEFAULT -1, pnl REAL)""")
            for col_def in ('close_ts REAL', 'close_reason TEXT',
                            'duration_s REAL', 'pnl_pct REAL'):
                try:
                    await db.execute(f"ALTER TABLE signal_log ADD COLUMN {col_def}")
                except Exception:
                    pass
            await db.commit()

    async def get_param(self, key):
        try:
            async with aiosqlite.connect(self.db_name) as db:
                async with db.execute("SELECT value FROM bot_params WHERE key=?", (key,)) as cur:
                    row = await cur.fetchone()
                    return row[0] if row else None
        except Exception:
            return None

    async def set_param(self, key, value):
        async with aiosqlite.connect(self.db_name) as db:
            await db.execute("INSERT OR REPLACE INTO bot_params (key, value) VALUES (?,?)",
                             (key, value))
            await db.commit()

    async def log_signal(self, symbol, direction, entry_ts, score, components):
        try:
            async with aiosqlite.connect(self.db_name) as db:
                await db.execute(
                    "INSERT INTO signal_log (symbol, direction, entry_ts, score, components) VALUES (?,?,?,?,?)",
                    (symbol, direction, entry_ts, score, json.dumps(components, ensure_ascii=False)))
                await db.commit()
        except Exception as e:
            logger.warning(f"تعذر تسجيل الإشارة: {e}")

    async def set_signal_result(self, symbol, entry_ts, win, pnl,
                                close_ts=None, close_reason=None,
                                duration_s=None, pnl_pct=None):
        try:
            async with aiosqlite.connect(self.db_name) as db:
                await db.execute(
                    """UPDATE signal_log SET result=?, pnl=?, close_ts=?, close_reason=?,
                       duration_s=?, pnl_pct=? WHERE symbol=? AND entry_ts=? AND result=-1""",
                    (int(win), pnl, close_ts, close_reason, duration_s, pnl_pct,
                     symbol, entry_ts))
                await db.commit()
        except Exception as e:
            logger.warning(f"تعذر تحديث نتيجة الإشارة: {e}")

    async def save_trade(self, t):
        async with aiosqlite.connect(self.db_name) as db:
            await db.execute("""INSERT OR REPLACE INTO active_trades
                (symbol, side, entry_price, quantity, orig_quantity, sl, tp, tp2, tp3,
                 trailing_active, highest_price, lowest_price, entry_time,
                 partial_closed, partial2_closed, partial1_closed,
                 realized_pnl, stop_order_id, tp_order_id, strategy, margin_usdt)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (t['symbol'], t['side'], t['entry_price'], t['quantity'],
                 float(t.get('orig_quantity') or t['quantity']),
                 t['sl'], t['tp'], t.get('tp2'), t.get('tp3'),
                 int(t.get('trailing_active', False)),
                 t.get('highest_price', 0), t.get('lowest_price', 999999),
                 t['entry_time'], int(t.get('partial_closed', 0)),
                 int(t.get('partial2_closed', 0)),
                 int(t.get('partial1_closed', t.get('partial_closed', 0))),
                 float(t.get('realized_pnl', 0.0)),
                 t.get('stop_order_id'), t.get('tp_order_id'),
                 t.get('strategy') or 'TRIPLE', float(t.get('margin_usdt') or 0)))
            await db.commit()

    async def load_active_trades(self):
        trades = {}
        try:
            async with aiosqlite.connect(self.db_name) as db:
                db.row_factory = aiosqlite.Row
                async with db.execute("""SELECT symbol, side, entry_price, quantity, orig_quantity, sl, tp, tp2, tp3,
                    trailing_active, highest_price, lowest_price, entry_time,
                    partial_closed, partial2_closed, partial1_closed,
                    realized_pnl, stop_order_id, tp_order_id,
                    strategy, margin_usdt FROM active_trades""") as cur:
                    async for row in cur:
                        p1 = row['partial1_closed']
                        if p1 is None:
                            p1 = row['partial_closed'] or 0
                        qty = row['quantity']
                        orig = row['orig_quantity'] if row['orig_quantity'] else qty
                        trades[row['symbol']] = {
                            'symbol': row['symbol'], 'side': row['side'],
                            'entry_price': row['entry_price'], 'quantity': qty,
                            'orig_quantity': orig,
                            'sl': row['sl'], 'tp': row['tp'], 'tp2': row['tp2'], 'tp3': row['tp3'],
                            'trailing_active': bool(row['trailing_active']),
                            'highest_price': row['highest_price'],
                            'lowest_price': row['lowest_price'],
                            'entry_time': row['entry_time'],
                            'partial_closed': bool(row['partial_closed']),
                            'partial1_closed': bool(p1),
                            'partial2_closed': bool(row['partial2_closed'] or 0),
                            'realized_pnl': row['realized_pnl'] if row['realized_pnl'] is not None else 0.0,
                            'stop_order_id': row['stop_order_id'],
                            'tp_order_id': row['tp_order_id'],
                            'strategy': row['strategy'] or 'TRIPLE',
                            'margin_usdt': row['margin_usdt'] or 0,
                            'stop_synced_price': None, 'tp_skipped': False,
                        }
        except Exception as e:
            logger.error(f"❌ فشل تحميل الصفقات من القاعدة: {e}")
        return trades

    async def remove_trade(self, symbol):
        async with aiosqlite.connect(self.db_name) as db:
            await db.execute("DELETE FROM active_trades WHERE symbol=?", (symbol,))
            await db.commit()

    async def update_daily_pnl(self, pnl, is_win=None):
        today = time.strftime("%Y-%m-%d")
        w = 1 if is_win is True else 0
        l = 1 if is_win is False else 0
        async with aiosqlite.connect(self.db_name) as db:
            async with db.execute("SELECT * FROM daily_stats WHERE date=?", (today,)) as cur:
                row = await cur.fetchone()
            if row:
                await db.execute("UPDATE daily_stats SET realized_pnl=?, wins=?, losses=? WHERE date=?",
                                (row[1] + pnl, row[2] + w, row[3] + l, today))
            else:
                await db.execute("INSERT INTO daily_stats VALUES (?,?,?,?)",
                                 (today, pnl, w, l))
            await db.commit()

    async def get_day_stats(self, day):
        try:
            async with aiosqlite.connect(self.db_name) as db:
                async with db.execute(
                        "SELECT realized_pnl, wins, losses FROM daily_stats WHERE date=?", (day,)) as cur:
                    return await cur.fetchone()
        except Exception:
            return None


# ══════════════════════════════════════════════════════════════════════
#  🧭 Triple Confluence Navigator — تنفيذ Pine Script حرفياً
# ══════════════════════════════════════════════════════════════════════
TRIPLE_CONFIG = {
    "name": "TRIPLE",
    "label": "Triple Confluence Navigator",
    "enabled": True,
    "settings_version": 1,
    # ── Cardwell RSI ──
    "rsiLen": 14,
    "rmaFastLen": 9,
    "rmaSlowLen": 45,
    "maType": "Kalman",           # Kalman / RMA / LLAMA
    # ── Adaptive ARSI ──
    "arsiLen": 14,
    "arsiSigLen": 14,
    "arsiFiltType": "Kalman Filter",  # RMA / Kalman Filter / LLAMA
    "kalmanProcN": 0.01,
    "kalmanMeasN": 0.10,
    "llamaAlphaMin": 0.05,
    "llamaAlphaMax": 0.50,
    # ── Supertrend ──
    "stAtrLen": 14,
    "stFactor": 2.0,
    # ── Structure ──
    "swingLen": 5,
    # ── Trade Setup ──
    "atrLen": 14,
    "atrMultSl": 1.5,
    "slStructBuf": 0.25,
    "rr1": 1.0, "rr2": 2.0, "rr3": 3.0,
    "confluenceWindow": 5,
    # ── HTF Filter (معطّل افتراضياً مطابق Pine) ──
    "useHtfFilter": False,
    "htfTimeframe": 240,
    # ── ADX/Choppiness Filter (معطّل افتراضياً مطابق Pine) ──
    "useChopFilter": False,
    "adxLen": 14,
    "adxMinLevel": 20.0,
    # ── التشغيل ──
    "max_open_trades": 1,         # ✅ المتغايرة = 1
    "reentry_cooldown": 3600,
    "scan_limit": 60,
    # ── فريم الاستراتيجية ──
    "timeframe": "15m",
    # ── فلتر الذكاء الاصطناعي Gemini ──
    "useGeminiFilter": True,
    "geminiModel": "gemini-2.5-flash",

  
    # ════════════════════════════════════════════════════════════════
    #  I. إعدادات العرض / التنبيهات / الألوان (مطابقة Pine Script — مرجعية فقط)
    # ════════════════════════════════════════════════════════════════
    "showDash": True,
    "dashPos": "Top Right",
    "actionLong": "long",
    "actionShort": "short",
    "actionCloseLong": "closelong",
    "actionCloseShort": "closeshort",
    "colEntry": "#1e3a5c",
    "colSl": "#7a1f1f",
    "colTp1": "#0e5c4a",
    "colTp2": "#0e6e52",
    "colTp3": "#12a382",
    "colBuyLbl": "#26a69a",
    "colSellLbl": "#ef5350",
}


# ─── دوال المساعدة الحسابية (مطابقة لـ ta.* في Pine) ───
def ema(a, n):
    """Pine ta.ema — مع فحص الطول الفارغ وقيم NaN الابتدائية.

    ✅ إصلاح: القديم كان يفترض len(a) >= 1 وينفّذ a[0] مباشرة فينهار على
    مصفوفة فارغة، ويزرع NaN في بقية السلسلة لو a[0]=NaN. الآن:
      • مصفوفة فارغة → نُرجع مصفوفة فارغة.
      • أول قيمة NaN → نبحث عن أول قيمة صالحة كبذرة.
      • لا تسرب NaN لبقية السلسلة.
    """
    a = np.asarray(a, dtype=float)
    if len(a) == 0:
        return np.array([], dtype=float)
    out = np.full_like(a, np.nan)
    k = 2.0 / (n + 1.0)
    # ابحث عن أول قيمة صالحة كبذرة
    seed_idx = -1
    for i in range(len(a)):
        if not np.isnan(a[i]):
            seed_idx = i
            break
    if seed_idx < 0:
        # كل القيم NaN — نُرجع كلها NaN (لا تسرب لأرقام وهمية)
        return out
    out[seed_idx] = a[seed_idx]
    for i in range(seed_idx + 1, len(a)):
        if np.isnan(a[i]):
            # نحافظ على آخر قيمة صالحة بدل تسريب NaN (مطابق سلوك ta.ema في Pine
            # التي تتخطى na وتُكمل الحساب على القيم الصالحة)
            out[i] = out[i - 1]
            continue
        out[i] = out[i - 1] + k * (a[i] - out[i - 1])
    return out


def rma(a, n):
    """Pine ta.rma = Wilder's smoothing = EMA with alpha=1/n."""
    a = np.asarray(a, dtype=float)
    out = np.full_like(a, np.nan)
    valid_idx = np.where(~np.isnan(a))[0]
    if len(valid_idx) < n:
        return out
    first_valid = valid_idx[0]
    seed_window = []
    j = 0
    for i in range(first_valid, len(a)):
        if not np.isnan(a[i]):
            seed_window.append(a[i])
            if len(seed_window) == n:
                seed_end = i
                break
    if len(seed_window) < n:
        return out
    out[seed_end] = np.mean(seed_window)
    alpha = 1.0 / n
    prev = out[seed_end]
    for i in range(seed_end + 1, len(a)):
        if np.isnan(a[i]):
            out[i] = np.nan
            continue
        prev = alpha * a[i] + (1 - alpha) * prev
        out[i] = prev
    return out


def rsi_wilder(close, n):
    """Pine ta.rsi — مطابق حرفياً للتنفيذ الرسمي في Pine v5/v6."""
    close = np.asarray(close, dtype=float)
    if len(close) < n + 1:
        return np.full_like(close, np.nan)
    delta = np.diff(close, prepend=np.nan)  # ta.change: أول قيمة na
    gain = np.where(np.isnan(delta), np.nan, np.where(delta > 0, delta, 0.0))
    loss = np.where(np.isnan(delta), np.nan, np.where(delta < 0, -delta, 0.0))
    avg_gain = rma(gain, n)
    avg_loss = rma(loss, n)
    with np.errstate(invalid='ignore', divide='ignore'):
        rs = avg_gain / avg_loss
        rsi_arr = np.where(avg_loss == 0, 100.0,
                           np.where(avg_gain == 0, 0.0,
                                    100.0 - (100.0 / (1.0 + rs))))
    rsi_arr[np.isnan(avg_loss) | np.isnan(avg_gain)] = np.nan
    return rsi_arr


def atr_func(h, l, c, n=14):
    """Pine ta.atr باستخدام Wilder smoothing."""
    h = np.asarray(h, dtype=float); l = np.asarray(l, dtype=float); c = np.asarray(c, dtype=float)
    if len(c) < n + 1:
        return np.full_like(c, np.nan)
    pc = np.concatenate([[c[0]], c[:-1]])
    tr = np.maximum.reduce([h - l, np.abs(h - pc), np.abs(l - pc)])
    return rma(tr, n)


def dmi_func(h, l, c, n):
    """Pine ta.dmi -> returns (plus_di, minus_di, adx)."""
    h = np.asarray(h, dtype=float); l = np.asarray(l, dtype=float); c = np.asarray(c, dtype=float)
    if len(c) < 2 * n:
        return (np.full_like(c, np.nan), np.full_like(c, np.nan), np.full_like(c, np.nan))
    up = np.diff(h, prepend=h[0])
    down = -np.diff(l, prepend=l[0])
    plus_dm = np.where((up > down) & (up > 0), up, 0.0)
    minus_dm = np.where((down > up) & (down > 0), down, 0.0)
    pc = np.concatenate([[c[0]], c[:-1]])
    tr = np.maximum.reduce([h - l, np.abs(h - pc), np.abs(l - pc)])
    atr_s = rma(tr, n)
    plus_di = np.where(atr_s > 0, 100 * rma(plus_dm, n) / atr_s, np.nan)
    minus_di = np.where(atr_s > 0, 100 * rma(minus_dm, n) / atr_s, np.nan)
    dx = np.where((plus_di + minus_di) > 0,
                  100 * np.abs(plus_di - minus_di) / (plus_di + minus_di), 0.0)
    adx = rma(dx, n)
    return (plus_di, minus_di, adx)


def linreg(src, n, offset=0):
    """Pine ta.linreg."""
    src = np.asarray(src, dtype=float)
    out = np.full_like(src, np.nan)
    if len(src) < n:
        return out
    x = np.arange(1, n + 1, dtype=float)
    x_mean = x.mean()
    x_var = ((x - x_mean) ** 2).sum()
    for i in range(n - 1, len(src)):
        y = src[i - n + 1: i + 1]
        if np.isnan(y).any():
            continue
        y_mean = y.mean()
        cov = ((x - x_mean) * (y - y_mean)).sum()
        slope = cov / x_var if x_var > 0 else 0.0
        intercept = y_mean - slope * x_mean
        out[i] = intercept + slope * (n - offset)
    return out


def kalman_filter(src, proc_n, meas_n):
    """Pine f_kalman — مطابق حرفياً (دلالة var وNaN في Pine)."""
    src = np.asarray(src, dtype=float)
    out = np.full_like(src, np.nan)
    if len(src) == 0:
        return out
    est = np.nan
    cov = 1.0
    for i in range(len(src)):
        s = src[i]
        if np.isnan(est):
            est = s
        cov = cov + proc_n
        gain = cov / (cov + meas_n)
        est = est + gain * (s - est)
        cov = (1.0 - gain) * cov
        out[i] = est
    return out


def llama_filter(src, n, a_min, a_max):
    """Pine f_llama — مطابق حرفياً (دلالة var/na/ternary في Pine)."""
    src = np.asarray(src, dtype=float)
    out = np.full_like(src, np.nan)
    if len(src) == 0:
        return out
    est = np.nan
    for i in range(len(src)):
        s = src[i]
        if np.isnan(est):
            est = s
        if i - n >= 0 and not np.isnan(src[i - n]) and not np.isnan(s):
            slope = (s - src[i - n]) / n
        else:
            slope = np.nan
        if i - n + 1 >= 0:
            window = src[i - n + 1: i + 1]
            rng = (window.max() - window.min()) if not np.isnan(window).any() else np.nan
        else:
            rng = np.nan
        if np.isnan(rng) or rng == 0:
            eff = 0.0
        elif np.isnan(slope):
            eff = np.nan
        else:
            eff = min(abs(slope * n) / rng, 1.0)
        alpha = a_min + eff * (a_max - a_min)
        est = est + alpha * (s - est)
        out[i] = est
    return out


def rolling_max(a, n):
    """Pine ta.highest."""
    a = np.asarray(a, dtype=float)
    out = np.full_like(a, np.nan)
    for i in range(n - 1, len(a)):
        out[i] = np.nanmax(a[i - n + 1: i + 1])
    return out


def rolling_min(a, n):
    """Pine ta.lowest."""
    a = np.asarray(a, dtype=float)
    out = np.full_like(a, np.nan)
    for i in range(n - 1, len(a)):
        out[i] = np.nanmin(a[i - n + 1: i + 1])
    return out


def crossover(a, b):
    """Pine ta.crossover: a cross above b."""
    a = np.asarray(a, dtype=float); b = np.asarray(b, dtype=float)
    out = np.zeros(len(a), dtype=bool)
    for i in range(1, len(a)):
        if i < 2:
            continue
        if np.isnan(a[i - 1]) or np.isnan(b[i - 1]) or np.isnan(a[i - 2]) or np.isnan(b[i - 2]):
            continue
        out[i] = (a[i - 1] > b[i - 1]) and (a[i - 2] <= b[i - 2])
    return out


def crossunder(a, b):
    """Pine ta.crossunder: a cross below b."""
    a = np.asarray(a, dtype=float); b = np.asarray(b, dtype=float)
    out = np.zeros(len(a), dtype=bool)
    for i in range(1, len(a)):
        if i < 2:
            continue
        if np.isnan(a[i - 1]) or np.isnan(b[i - 1]) or np.isnan(a[i - 2]) or np.isnan(b[i - 2]):
            continue
        out[i] = (a[i - 1] < b[i - 1]) and (a[i - 2] >= b[i - 2])
    return out


def f_arsi_series(close, length):
    """Pine f_arsi — مطابق حرفياً بما فيها دلالة na في الـ ternary."""
    close = np.asarray(close, dtype=float)
    n = len(close)
    u = rolling_max(close, length)
    l = rolling_min(close, length)
    r = u - l
    d = close - np.r_[np.nan, close[:-1]]
    u_prev = np.r_[np.nan, u[:-1]]
    l_prev = np.r_[np.nan, l[:-1]]
    diff = np.where(u > u_prev, r, np.where(l < l_prev, -r, d))
    num = rma(diff, length)
    den = rma(np.abs(diff), length)
    with np.errstate(invalid='ignore', divide='ignore'):
        out = np.where(~np.isnan(den) & (den != 0), num / den * 50.0 + 50.0, 50.0)
    return out


def pivot_high(h, n):
    """Pine f_pivotHigh: قمة سوينغ بطول n — مطابق 100%."""
    h = np.asarray(h, dtype=float)
    out = np.full_like(h, np.nan)
    for i in range(n, len(h) - n):
        c = h[i]
        ok = True
        for j in range(i - n, i):
            if h[j] > c:
                ok = False; break
        if ok:
            for j in range(i + 1, i + n + 1):
                if h[j] >= c:
                    ok = False; break
        if ok:
            out[i + n] = c
    return out


def pivot_low(l, n):
    """Pine f_pivotLow: قاع سوينغ بطول n — مطابق 100%."""
    l = np.asarray(l, dtype=float)
    out = np.full_like(l, np.nan)
    for i in range(n, len(l) - n):
        c = l[i]
        ok = True
        for j in range(i - n, i):
            if l[j] < c:
                ok = False; break
        if ok:
            for j in range(i + 1, i + n + 1):
                if l[j] <= c:
                    ok = False; break
        if ok:
            out[i + n] = c
    return out


def tf_to_ms(tf):
    """يحوّل فريم Binance ('15m','1h','1d'...) إلى مدة بالمللي ثانية."""
    tf = str(tf).strip().lower()
    try:
        if tf.endswith('m'):
            return int(tf[:-1]) * 60_000
        if tf.endswith('h'):
            return int(tf[:-1]) * 3_600_000
        if tf.endswith('d'):
            return int(tf[:-1]) * 86_400_000
        if tf.endswith('w'):
            return int(tf[:-1]) * 7 * 86_400_000
        return int(tf) * 60_000  # رقم مجرد = دقائق
    except Exception:
        return 15 * 60_000


# ─── خنق تحذيرات HTF ───
_HTF_WARN_STATE = {'last_ts': 0.0, 'last_sig': None, 'skipped': 0}


def _htf_warn_throttled(signature, message, min_gap=600.0, new_sig_gap=60.0):
    now = time.time()
    st = _HTF_WARN_STATE
    sig_changed = signature != st['last_sig']
    gap = new_sig_gap if sig_changed else min_gap
    if now - st['last_ts'] >= gap:
        suffix = f" (تم كتم {st['skipped']} تحذيراً مشابهاً منذ آخر إظهار)" \
            if st['skipped'] else ""
        logger.warning(message + suffix)
        st['last_ts'] = now
        st['last_sig'] = signature
        st['skipped'] = 0
    else:
        st['skipped'] += 1


def triple_compute_signal(df15, cfg=None, htf_df=None, symbol=None):
    """يحسب إشارات Triple Confluence Navigator — مطابق 100% لكود Pine الأصلي.

    المرتجع: dict يحوي آخر إشارة {direction, entry, sl, tp1, tp2, tp3,
             components, score, atr_ref, lastSwingHigh, lastSwingLow} أو None.
    """
    cfg = cfg or TRIPLE_CONFIG
    if df15 is None or len(df15) < max(cfg['rmaSlowLen'], cfg['arsiLen'],
                                       cfg['stAtrLen'], cfg['atrLen']) + cfg['confluenceWindow'] + 5:
        return None

    close = df15['close'].values.astype(float)
    high = df15['high'].values.astype(float)
    low = df15['low'].values.astype(float)
    n = len(close)

    # ── 1) Cardwell RSI ──
    rsi_value = rsi_wilder(close, cfg['rsiLen'])

    _kfQ = 0.001; _kfR = 0.01
    _kfFastP = 1.0; _kfSlowP = 1.0
    _kfFastX = np.nan; _kfSlowX = np.nan
    _kfFastX_arr = np.full(n, np.nan)
    _kfSlowX_arr = np.full(n, np.nan)
    slow_ratio = cfg['rmaSlowLen'] / cfg['rmaFastLen']
    for i in range(n):
        rv = rsi_value[i]
        k_fast = _kfFastP / (_kfFastP + _kfR)
        k_slow = _kfSlowP / (_kfSlowP + _kfR * slow_ratio)
        if np.isnan(rv):
            _kfFastX = np.nan
            _kfSlowX = np.nan
        else:
            prev_fast = _kfFastX if not np.isnan(_kfFastX) else rv
            prev_slow = _kfSlowX if not np.isnan(_kfSlowX) else rv
            _kfFastX = prev_fast + k_fast * (rv - prev_fast)
            _kfSlowX = prev_slow + k_slow * (rv - prev_slow)
        _kfFastP = (1.0 - k_fast) * _kfFastP + _kfQ
        _kfSlowP = (1.0 - k_slow) * _kfSlowP + _kfQ
        _kfFastX_arr[i] = _kfFastX
        _kfSlowX_arr[i] = _kfSlowX

    ma_type = cfg['maType']
    if ma_type == "Kalman":
        rma_fast = _kfFastX_arr
        rma_slow = _kfSlowX_arr
    elif ma_type == "LLAMA":
        rma_fast = linreg(rsi_value, cfg['rmaFastLen'], 0)
        rma_slow = linreg(rsi_value, cfg['rmaSlowLen'], 0)
    else:
        rma_fast = rma(rsi_value, cfg['rmaFastLen'])
        rma_slow = rma(rsi_value, cfg['rmaSlowLen'])

    raw_bull_cross = np.zeros(n, dtype=bool)
    raw_bear_cross = np.zeros(n, dtype=bool)
    for i in range(2, n):
        if np.isnan(rma_fast[i - 1]) or np.isnan(rma_slow[i - 1]):
            continue
        if np.isnan(rma_fast[i - 2]) or np.isnan(rma_slow[i - 2]):
            continue
        raw_bull_cross[i] = (rma_fast[i - 1] > rma_slow[i - 1]) and (rma_fast[i - 2] <= rma_slow[i - 2])
        raw_bear_cross[i] = (rma_fast[i - 1] < rma_slow[i - 1]) and (rma_fast[i - 2] >= rma_slow[i - 2])

    # ── ADX (Chop filter) ──
    if cfg['useChopFilter']:
        _pdi, _mdi, adx_value = dmi_func(high, low, close, cfg['adxLen'])
        chop_ok = (~np.isnan(adx_value)) & (np.r_[np.nan, adx_value[:-1]] >= cfg['adxMinLevel'])
    else:
        adx_value = np.full(n, np.nan)
        chop_ok = np.ones(n, dtype=bool)

    # ── HTF filter ──
    if not cfg['useHtfFilter']:
        htf_bull_bias = np.ones(n, dtype=bool)
        htf_bear_bias = np.ones(n, dtype=bool)
        htf_arsi_bull = np.ones(n, dtype=bool)
        htf_arsi_bear = np.ones(n, dtype=bool)
        htf_ok_bull = np.ones(n, dtype=bool)
        htf_ok_bear = np.ones(n, dtype=bool)
    else:
        htf_bull_bias = np.zeros(n, dtype=bool)
        htf_bear_bias = np.zeros(n, dtype=bool)
        htf_arsi_bull = np.zeros(n, dtype=bool)
        htf_arsi_bear = np.zeros(n, dtype=bool)
        htf_ok_bull = np.zeros(n, dtype=bool)
        htf_ok_bear = np.zeros(n, dtype=bool)

    if cfg['useHtfFilter'] and htf_df is not None and len(htf_df) >= cfg['rsiLen'] + 5:
        try:
            htf_close = htf_df['close'].values.astype(float)
            htf_times = htf_df['time'].values if 'time' in htf_df.columns else None
            df_times = df15['time'].values if 'time' in df15.columns else None

            htf_rsi = rsi_wilder(htf_close, cfg['rsiLen'])
            htf_rma_fast = rma(htf_rsi, cfg['rmaFastLen'])
            htf_rma_slow = rma(htf_rsi, cfg['rmaSlowLen'])
            htf_arsi_raw = f_arsi_series(htf_close, cfg['arsiLen'])

            htf_fast_aligned = np.full(n, np.nan)
            htf_slow_aligned = np.full(n, np.nan)
            htf_arsi_aligned = np.full(n, np.nan)
            htf_rsi_aligned = np.full(n, np.nan)
            htf_valid_aligned = np.zeros(n, dtype=bool)
            if htf_times is not None and df_times is not None:
                # ✅ حساب htf_duration_ms بأمان أمام فجوات الوقت:
                #    نستخدم الوسيط (median) بدل الـ mode لتجنّب فجوات بيانات نادرة،
                #    ونتحقق من أن الفرق موجب وصالح. لو فشل كل شيء نعود لقيمة افتراضية.
                if len(htf_times) >= 2:
                    diffs = np.diff(htf_times.astype(np.int64))
                    diffs = diffs[diffs > 0]  # تجاهل الفجوات الصفرية/السالبة
                    if len(diffs) > 0:
                        htf_duration_ms = int(np.median(diffs))
                    else:
                        htf_duration_ms = int(cfg.get('htfTimeframe', 240)) * 60000
                else:
                    htf_duration_ms = int(cfg.get('htfTimeframe', 240)) * 60000
                j = 0
                for i in range(n):
                    t_i = df_times[i]
                    while j < len(htf_times) and htf_times[j] + htf_duration_ms <= t_i:
                        j += 1
                    k = j - 1
                    if k >= 0:
                        if not np.isnan(htf_rma_fast[k]):
                            htf_fast_aligned[i] = htf_rma_fast[k]
                        if not np.isnan(htf_rma_slow[k]):
                            htf_slow_aligned[i] = htf_rma_slow[k]
                        if not np.isnan(htf_arsi_raw[k]):
                            htf_arsi_aligned[i] = htf_arsi_raw[k]
                        if not np.isnan(htf_rsi[k]):
                            htf_rsi_aligned[i] = htf_rsi[k]
                        htf_valid_aligned[i] = True
            else:
                if len(htf_rma_fast):
                    last_fast = htf_rma_fast[~np.isnan(htf_rma_fast)][-1] if np.isfinite(htf_rma_fast).any() else np.nan
                    last_slow = htf_rma_slow[~np.isnan(htf_rma_slow)][-1] if np.isfinite(htf_rma_slow).any() else np.nan
                    last_arsi = htf_arsi_raw[~np.isnan(htf_arsi_raw)][-1] if np.isfinite(htf_arsi_raw).any() else np.nan
                    last_rsi = htf_rsi[~np.isnan(htf_rsi)][-1] if np.isfinite(htf_rsi).any() else np.nan
                    htf_fast_aligned[:] = last_fast
                    htf_slow_aligned[:] = last_slow
                    htf_arsi_aligned[:] = last_arsi
                    htf_rsi_aligned[:] = last_rsi

            valid_fast = ~np.isnan(htf_fast_aligned)
            valid_slow = ~np.isnan(htf_slow_aligned)
            htf_bull_bias = np.where(valid_fast & valid_slow,
                                     htf_fast_aligned > htf_slow_aligned, False)
            htf_bear_bias = np.where(valid_fast & valid_slow,
                                     htf_fast_aligned < htf_slow_aligned, False)
            valid_arsi = ~np.isnan(htf_arsi_aligned)
            htf_arsi_bull = np.where(valid_arsi, htf_arsi_aligned >= 50.0, False)
            htf_arsi_bear = np.where(valid_arsi, htf_arsi_aligned < 50.0, False)
            valid_rsi = ~np.isnan(htf_rsi_aligned)
            htf_bull3 = np.where(valid_rsi, htf_rsi_aligned > 50.0, False)
            htf_bear3 = np.where(valid_rsi, htf_rsi_aligned < 50.0, False)
            htf_ok_bull = htf_bull3.copy()
            htf_ok_bear = htf_bear3.copy()
            n_missing = int((~htf_valid_aligned).sum())
            if n_missing > 0:
                recent_ok = bool(n and htf_valid_aligned[-1])
                _htf_warn_throttled(
                    f"HTF:{n_missing}/{n}",
                    f"⚠️ HTF [{symbol or '?'}]: {n_missing}/{n} شمعة df15 (الأقدم في "
                    f"النافذة) بلا بيانات HTF مغلقة — الفلتر False لهذه الشموع فقط."
                    + (" الشموع الحديثة سليمة ✓" if recent_ok
                       else " ⚠️ حتى الشموع الحديثة بلا HTF — تحقق من بيانات الرمز!"))
        except Exception as e:
            _htf_warn_throttled(
                "HTF-FAIL",
                f"⚠️ تعذر حساب HTF [{symbol or '?'}] — الفلتر معطّل (False) لكل الشموع: {e}")
            htf_bull_bias = np.zeros(n, dtype=bool)
            htf_bear_bias = np.zeros(n, dtype=bool)
            htf_arsi_bull = np.zeros(n, dtype=bool)
            htf_arsi_bear = np.zeros(n, dtype=bool)
            htf_ok_bull = np.zeros(n, dtype=bool)
            htf_ok_bear = np.zeros(n, dtype=bool)

    # إشارات 1
    bull_signal1 = raw_bull_cross & (~cfg['useHtfFilter'] | htf_bull_bias) & chop_ok
    bear_signal1 = raw_bear_cross & (~cfg['useHtfFilter'] | htf_bear_bias) & chop_ok

    # ── 2) Adaptive ARSI + Supertrend ──
    arsi = f_arsi_series(close, cfg['arsiLen'])

    filt_type = cfg['arsiFiltType']
    if filt_type == "RMA":
        arsi_signal = rma(arsi, cfg['arsiSigLen'])
    elif filt_type == "Kalman Filter":
        arsi_signal = kalman_filter(arsi, cfg['kalmanProcN'], cfg['kalmanMeasN'])
    else:  # LLAMA
        arsi_signal = llama_filter(arsi, cfg['arsiSigLen'],
                                   cfg['llamaAlphaMin'], cfg['llamaAlphaMax'])

    arsi_atr = rma(np.abs(arsi - np.r_[np.nan, arsi[:-1]]), cfg['stAtrLen'])
    upper_band = arsi + cfg['stFactor'] * arsi_atr
    lower_band = arsi - cfg['stFactor'] * arsi_atr

    supertrend = np.full(n, np.nan)
    trend_up = np.ones(n, dtype=bool)
    var_supertrend = np.nan
    var_trend_up = True
    for i in range(n):
        ub_i = upper_band[i]; lb_i = lower_band[i]; ar_i = arsi[i]
        if np.isnan(ub_i) or np.isnan(lb_i) or np.isnan(ar_i):
            supertrend[i] = var_supertrend if not np.isnan(var_supertrend) else np.nan
            trend_up[i] = var_trend_up
            continue
        prev_st = var_supertrend
        prev_tu = var_trend_up
        if np.isnan(prev_st):
            var_supertrend = ub_i
            var_trend_up = True
        else:
            if ar_i > prev_st:
                var_trend_up = True
            elif ar_i < prev_st:
                var_trend_up = False
            else:
                var_trend_up = prev_tu
            if var_trend_up:
                var_supertrend = max(lb_i, prev_st)
            else:
                var_supertrend = min(ub_i, prev_st)
        supertrend[i] = var_supertrend
        trend_up[i] = var_trend_up

    raw_merged = np.where(np.isnan(supertrend), arsi_signal, (arsi_signal + supertrend) / 2.0)
    raw_bull_cross2 = crossover(arsi, raw_merged)
    raw_bear_cross2 = crossunder(arsi, raw_merged)

    bull_signal2 = raw_bull_cross2 & (~cfg['useHtfFilter'] | htf_arsi_bull)
    bear_signal2 = raw_bear_cross2 & (~cfg['useHtfFilter'] | htf_arsi_bear)

    # ── 3) Structure (BOS) ──
    ph = pivot_high(high, cfg['swingLen'])
    pl = pivot_low(low, cfg['swingLen'])

    last_swing_high = None
    last_swing_low = None
    last_swing_high_bar = -1
    last_swing_low_bar = -1
    last_processed_high_bar = -1
    last_processed_low_bar = -1
    trend3 = 0
    bull_signal3 = np.zeros(n, dtype=bool)
    bear_signal3 = np.zeros(n, dtype=bool)

    lsh = np.full(n, np.nan); lsl = np.full(n, np.nan)
    lshb = np.full(n, -1, dtype=int); lslb = np.full(n, -1, dtype=int)
    trend3_arr = np.zeros(n, dtype=int)

    for i in range(n):
        if not np.isnan(ph[i]):
            last_swing_high = ph[i]
            last_swing_high_bar = i - cfg['swingLen']
        if not np.isnan(pl[i]):
            last_swing_low = pl[i]
            last_swing_low_bar = i - cfg['swingLen']
        lsh[i] = last_swing_high if last_swing_high is not None else np.nan
        lsl[i] = last_swing_low if last_swing_low is not None else np.nan
        lshb[i] = last_swing_high_bar
        lslb[i] = last_swing_low_bar
        break_up = (last_swing_high is not None and close[i] > last_swing_high
                    and htf_ok_bull[i]
                    and (last_processed_high_bar == -1 or last_swing_high_bar != last_processed_high_bar))
        break_dn = (last_swing_low is not None and close[i] < last_swing_low
                    and htf_ok_bear[i]
                    and (last_processed_low_bar == -1 or last_swing_low_bar != last_processed_low_bar))
        if break_up:
            last_processed_high_bar = last_swing_high_bar
            trend3 = 1
        if break_dn:
            last_processed_low_bar = last_swing_low_bar
            trend3 = -1
        bull_signal3[i] = break_up
        bear_signal3[i] = break_dn
        trend3_arr[i] = trend3

    # ── Confluence (نافذة 5 شموع) ──
    bars_since_bull1 = np.full(n, 9999, dtype=int)
    bars_since_bear1 = np.full(n, 9999, dtype=int)
    bars_since_bull2 = np.full(n, 9999, dtype=int)
    bars_since_bear2 = np.full(n, 9999, dtype=int)
    bars_since_bull3 = np.full(n, 9999, dtype=int)
    bars_since_bear3 = np.full(n, 9999, dtype=int)

    prev_b1 = prev_be1 = prev_b2 = prev_be2 = prev_b3 = prev_be3 = 9999
    for i in range(n):
        prev_b1 = 0 if bull_signal1[i] else prev_b1 + 1
        prev_be1 = 0 if bear_signal1[i] else prev_be1 + 1
        prev_b2 = 0 if bull_signal2[i] else prev_b2 + 1
        prev_be2 = 0 if bear_signal2[i] else prev_be2 + 1
        prev_b3 = 0 if bull_signal3[i] else prev_b3 + 1
        prev_be3 = 0 if bear_signal3[i] else prev_be3 + 1
        bars_since_bull1[i] = prev_b1
        bars_since_bear1[i] = prev_be1
        bars_since_bull2[i] = prev_b2
        bars_since_bear2[i] = prev_be2
        bars_since_bull3[i] = prev_b3
        bars_since_bear3[i] = prev_be3

    cw = cfg['confluenceWindow']
    bull_confluence = ((bars_since_bull1 <= cw) & (bars_since_bull2 <= cw) & (bars_since_bull3 <= cw))
    bear_confluence = ((bars_since_bear1 <= cw) & (bars_since_bear2 <= cw) & (bars_since_bear3 <= cw))

    bull_conf_prev = np.r_[False, bull_confluence[:-1]]
    bear_conf_prev = np.r_[False, bear_confluence[:-1]]
    long_signal = bull_confluence & (~bull_conf_prev)
    short_signal = bear_confluence & (~bear_conf_prev)

    # ── حساب المستويات على آخر إشارة ──
    atr_val = atr_func(high, low, close, cfg['atrLen'])
    atr_prev = np.r_[np.nan, atr_val[:-1]]
    atr_val1 = np.where(np.isnan(atr_prev), atr_val, atr_prev)

    long_indices = np.where(long_signal)[0]
    short_indices = np.where(short_signal)[0]
    last_long = long_indices[-1] if len(long_indices) else -1
    last_short = short_indices[-1] if len(short_indices) else -1

    if last_long < 0 and last_short < 0:
        return None

    if last_long >= last_short:
        i = last_long
        direction = 'BUY'
        entry_price = float(close[i - 1]) if i >= 1 else float(close[i])
        atr_cap_dist = float(atr_val1[i]) * cfg['atrMultSl']
        last_swing_low_at = float(lsl[i]) if not np.isnan(lsl[i]) else None
        if last_swing_low_at is not None:
            struct_dist = entry_price - (last_swing_low_at - float(atr_val1[i]) * cfg['slStructBuf'])
        else:
            struct_dist = None
        if struct_dist is None or struct_dist <= 0:
            risk_dist = atr_cap_dist
        else:
            risk_dist = min(struct_dist, atr_cap_dist)
        sl_price = entry_price - risk_dist
        tp1_price = entry_price + risk_dist * cfg['rr1']
        tp2_price = entry_price + risk_dist * cfg['rr2']
        tp3_price = entry_price + risk_dist * cfg['rr3']
        last_swing_high_at = float(lsh[i]) if not np.isnan(lsh[i]) else None
    else:
        i = last_short
        direction = 'SELL'
        entry_price = float(close[i - 1]) if i >= 1 else float(close[i])
        atr_cap_dist = float(atr_val1[i]) * cfg['atrMultSl']
        last_swing_high_at = float(lsh[i]) if not np.isnan(lsh[i]) else None
        if last_swing_high_at is not None:
            struct_dist = (last_swing_high_at + float(atr_val1[i]) * cfg['slStructBuf']) - entry_price
        else:
            struct_dist = None
        if struct_dist is None or struct_dist <= 0:
            risk_dist = atr_cap_dist
        else:
            risk_dist = min(struct_dist, atr_cap_dist)
        sl_price = entry_price + risk_dist
        tp1_price = entry_price - risk_dist * cfg['rr1']
        tp2_price = entry_price - risk_dist * cfg['rr2']
        tp3_price = entry_price - risk_dist * cfg['rr3']
        last_swing_low_at = float(lsl[i]) if not np.isnan(lsl[i]) else None

    return {
        'direction': direction,
        'price': entry_price,
        'entry': entry_price,
        'sl': float(sl_price),
        'tp1': float(tp1_price),
        'tp2': float(tp2_price),
        'tp3': float(tp3_price),
        'risk_dist': float(risk_dist),
        'atr_ref': float(atr_val1[i]),
        'lastSwingHigh': last_swing_high_at,
        'lastSwingLow': last_swing_low_at,
        'signal_bar_idx': int(i),
        # ✅ lastSwingHigh و lastSwingLow ضمن components المحفوظة مع الإشارة
        'components': {
            'trigger': 'Triple Confluence Navigator',
            'bull_signal1': bool(bull_signal1[i]),
            'bear_signal1': bool(bear_signal1[i]),
            'bull_signal2': bool(bull_signal2[i]),
            'bear_signal2': bool(bear_signal2[i]),
            'bull_signal3': bool(bull_signal3[i]),
            'bear_signal3': bool(bear_signal3[i]),
            'trend3': int(trend3_arr[i]),
            'rsi': float(rsi_value[i]) if not np.isnan(rsi_value[i]) else None,
            'arsi': float(arsi[i]) if not np.isnan(arsi[i]) else None,
            'lastSwingHigh': last_swing_high_at,
            'lastSwingLow': last_swing_low_at,
        },
        'score': 8,
    }


# ══════════════════════════════════════════════════════════════════════
#  فئة الاستراتيجية + مدير الخروج + قاعدة بياناتها
# ══════════════════════════════════════════════════════════════════════
class TripleStrategy:
    def __init__(self, cfg=None, log=None):
        self.cfg = dict(TRIPLE_CONFIG, **(cfg or {}))
        self.last_scan = {}
        self._log = log or (lambda x: None)

    async def scan(self, bot, candidates, active_symbols, cooldowns):
        """يفحص المرشّحين ويُرجع إشارات Triple Confluence."""
        sigs = []
        scanned = 0
        tf = self.cfg.get('timeframe', '15m')
        use_htf = bool(self.cfg.get('useHtfFilter', False))
        tf_ms = tf_to_ms(tf)
        now_ms = int(time.time() * 1000)
        htf_tf = None
        if use_htf:
            htf_min = int(self.cfg.get('htfTimeframe', 240))
            if htf_min < 60:
                htf_tf = f"{htf_min}m"
            elif htf_min == 60:
                htf_tf = "1h"
            elif htf_min == 120:
                htf_tf = "2h"
            elif htf_min == 240:
                htf_tf = "4h"
            elif htf_min == 480:
                htf_tf = "8h"
            elif htf_min == 720:
                htf_tf = "12h"
            elif htf_min == 1440:
                htf_tf = "1d"
            else:
                htf_tf = f"{htf_min // 60}h" if htf_min >= 60 else f"{htf_min}m"
        for sym in candidates:
            if bot.is_in_cooldown(sym) and sym not in active_symbols:
                continue
            try:
                df15 = await bot.get_klines(sym, tf, 300)
                if df15 is None or len(df15) < 60:
                    continue
                try:
                    times = df15['time'].values.astype(np.int64)
                    closed_mask = (times + tf_ms) <= now_ms
                    if closed_mask.any():
                        df15 = df15.iloc[: int(closed_mask.sum())].reset_index(drop=True)
                except Exception:
                    df15 = df15.iloc[:-1].reset_index(drop=True)
                if df15 is None or len(df15) < 60:
                    continue
                htf_df = None
                if use_htf and htf_tf:
                    try:
                        htf_df = await bot.get_klines(sym, htf_tf, 300)
                    except Exception:
                        htf_df = None
                scanned += 1
                sig = triple_compute_signal(df15, self.cfg, htf_df=htf_df, symbol=sym)
                if not sig:
                    continue
                last_idx = len(df15) - 1
                if sig['signal_bar_idx'] < last_idx - self.cfg['confluenceWindow']:
                    continue
                sig['symbol'] = sym
                sig['strategy'] = 'TRIPLE'
                sig['tp'] = sig['tp1']
                sig['signals_smc'] = ['Triple Confluence Navigator',
                                      f"RSI={sig['components'].get('rsi')}",
                                      f"ARSI={sig['components'].get('arsi')}"]
                sigs.append(sig)
            except Exception as e:
                logger.warning(f"⚠️ خطأ فحص {sym}: {e}")
        self.last_scan = {'scanned': scanned, 'signals': len(sigs), 'ts': time.time()}
        return sigs


class TripleExitManager:
    """إدارة الخروج: تنفيذ TP1/TP2/TP3 و SL للأهداف الجزئية."""
    def __init__(self, cfg=None, log=None):
        self.cfg = dict(TRIPLE_CONFIG, **(cfg or {}))
        self._log = log or (lambda x: None)

    async def manage(self, bot, trade):
        """فحص صفقة واحدة — يُرجع True إذا أُغلقت بالكامل."""
        sym = trade['symbol']
        if sym not in bot.active_trades:
            return False
        prices = bot.get_price(sym)
        if not prices:
            return False
        is_buy = trade['side'] == 'BUY'
        cur = prices['bid'] if is_buy else prices['ask']
        if is_buy:
            if trade.get('highest_price') is None or cur > trade['highest_price']:
                trade['highest_price'] = cur
        else:
            if trade.get('lowest_price') is None or cur < trade['lowest_price']:
                trade['lowest_price'] = cur
        sl = trade.get('sl')
        if sl is not None:
            if (is_buy and cur <= sl) or (not is_buy and cur >= sl):
                await bot.close_trade(trade, reason='SL', exit_price=sl)
                return True
        orig_qty = float(trade.get('orig_quantity') or trade.get('quantity') or 0)
        tp1 = trade.get('tp') or trade.get('tp1')
        if tp1 is not None and not trade.get('partial1_closed'):
            if (is_buy and cur >= tp1) or (not is_buy and cur <= tp1):
                ok = await bot.close_trade_partial(trade, ratio=0.40, reason='TP1',
                                                   exit_price=tp1, base_qty=orig_qty)
                if ok:
                    trade['partial1_closed'] = True
                    await bot.db.save_trade(trade)
        tp2 = trade.get('tp2')
        if tp2 is not None and not trade.get('partial2_closed') and sym in bot.active_trades:
            if (is_buy and cur >= tp2) or (not is_buy and cur <= tp2):
                ok = await bot.close_trade_partial(trade, ratio=0.30, reason='TP2',
                                                   exit_price=tp2, base_qty=orig_qty)
                if ok:
                    trade['partial2_closed'] = True
                    await bot.db.save_trade(trade)
        tp3 = trade.get('tp3')
        if tp3 is not None and sym in bot.active_trades:
            if (is_buy and cur >= tp3) or (not is_buy and cur <= tp3):
                await bot.close_trade(trade, reason='TP3', exit_price=tp3, full_close=True)
                return True
        if sym in bot.active_trades:
            await bot.db.save_trade(trade)
        return False


class TripleDB:
    """قاعدة بيانات مستقلة لاستراتيجية Triple — صفقات مغلقة + أرباح يومية."""
    def __init__(self, db_name=None):
        self.db_name = db_name or db_file

    async def init(self):
        async with aiosqlite.connect(self.db_name) as db:
            await db.execute("""CREATE TABLE IF NOT EXISTS triple_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT, side TEXT, entry_ts REAL, exit_ts REAL,
                entry REAL, exit REAL, qty REAL, pnl REAL, pnl_pct REAL,
                win INTEGER, reason TEXT, components TEXT, margin REAL)""")
            for col_def in ('tp1 REAL', 'tp2 REAL', 'tp3 REAL', 'sl REAL'):
                try:
                    await db.execute(f"ALTER TABLE triple_trades ADD COLUMN {col_def}")
                except Exception:
                    pass
            await db.execute("""CREATE TABLE IF NOT EXISTS triple_daily (
                date TEXT PRIMARY KEY, pnl REAL, wins INTEGER, losses INTEGER)""")
            await db.commit()

    async def open(self, trade):
        async with aiosqlite.connect(self.db_name) as db:
            await db.execute("""INSERT INTO triple_trades
                (symbol, side, entry_ts, entry, qty, sl, tp1, tp2, tp3, components, margin)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (trade['symbol'], trade['side'], trade['entry_time'],
                 trade['entry_price'], trade['quantity'], trade.get('sl'),
                 trade.get('tp') or trade.get('tp1'), trade.get('tp2'), trade.get('tp3'),
                 json.dumps(trade.get('components', {}), ensure_ascii=False),
                 trade.get('margin_usdt', 0)))
            await db.commit()

    async def close(self, symbol, entry_ts, exit_ts, exit_price, pnl, pnl_pct, win, reason, components):
        try:
            async with aiosqlite.connect(self.db_name) as db:
                await db.execute("""UPDATE triple_trades
                    SET exit_ts=?, exit=?, pnl=?, pnl_pct=?, win=?, reason=?
                    WHERE symbol=? AND entry_ts=? AND exit_ts IS NULL""",
                    (exit_ts, exit_price, pnl, pnl_pct, int(win), reason, symbol, entry_ts))
                await db.commit()
        except Exception as e:
            logger.warning(f"تعذر تحديث صفقة Triple المغلقة: {e}")

    async def today_stats(self):
        today = time.strftime('%Y-%m-%d')
        async with aiosqlite.connect(self.db_name) as db:
            async with db.execute(
                "SELECT COALESCE(SUM(pnl),0), COUNT(*) FROM triple_trades "
                "WHERE date(exit_ts,'unixepoch')=? AND exit_ts IS NOT NULL", (today,)) as cur:
                row = await cur.fetchone()
                return {'pnl': row[0] or 0.0, 'count': row[1] or 0}

    async def history(self, limit=500):
        async with aiosqlite.connect(self.db_name) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM triple_trades WHERE exit_ts IS NOT NULL "
                "ORDER BY exit_ts DESC LIMIT ?", (limit,)) as cur:
                return [dict(r) for r in await cur.fetchall()]

    async def daily_stats_all(self):
        async with aiosqlite.connect(self.db_name) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM triple_daily ORDER BY date DESC LIMIT 60") as cur:
                return [dict(r) for r in await cur.fetchall()]

    async def update_daily(self, pnl, win):
        today = time.strftime('%Y-%m-%d')
        async with aiosqlite.connect(self.db_name) as db:
            async with db.execute("SELECT * FROM triple_daily WHERE date=?", (today,)) as cur:
                row = await cur.fetchone()
            w = 1 if win else 0; l = 0 if win else 1
            if row:
                await db.execute("UPDATE triple_daily SET pnl=?, wins=?, losses=? WHERE date=?",
                                 (row[1] + pnl, row[2] + w, row[3] + l, today))
            else:
                await db.execute("INSERT INTO triple_daily VALUES (?,?,?,?)", (today, pnl, w, l))
            await db.commit()


# ══════════════════════════════════════════════════════════════════════
#  البوت الرئيسي
# ══════════════════════════════════════════════════════════════════════
class TripleConfluenceBot:
    STRATEGY_LABELS = {'TRIPLE': '🧭 Triple Confluence Navigator'}

    def __init__(self):
        self.binance_api_key = os.environ.get('BINANCE_API_KEY', '')
        self.binance_api_secret = os.environ.get('BINANCE_API_SECRET', '')

        self.TRADE_ENABLED = os.environ.get('TRADE_ENABLED', 'false').lower() == 'true'
        self.LEVERAGE = int(os.environ.get('LEVERAGE', '10'))
        self.TRADE_SIZE_USDT = float(os.environ.get('TRADE_SIZE_USDT', '50'))
        self.TESTNET = os.environ.get('TESTNET', 'false').lower() == 'true'
        self.api_keys_ok = None
        self.public_ip = None

        self.max_open_triple = int(os.environ.get('MAX_OPEN_TRADES', '1'))

        problems = []
        if self.TRADE_SIZE_USDT <= 0:
            problems.append("TRADE_SIZE_USDT يجب أن يكون أكبر من صفر")
        if not (1 <= self.max_open_triple <= 20):
            problems.append("MAX_OPEN_TRADES يجب أن يكون بين 1 و 20")
        if not (1 <= self.LEVERAGE <= 125):
            problems.append("LEVERAGE يجب أن يكون بين 1 و 125")
        if self.TESTNET:
            logger.info("🧪 وضع Testnet التجريبي مفعّل")
        if problems:
            raise SystemExit("❌ إعدادات خاطئة:\n- " + "\n- ".join(problems))

        if self.TRADE_ENABLED and (not self.binance_api_key or not self.binance_api_secret):
            self.TRADE_ENABLED = False
            logger.error("❌ التداول مفعّل لكن مفاتيح API ناقصة! التحويل للمراقبة فقط")

        # ثوابت حماية تشغيلية
        self.PRICE_MAX_AGE = 30
        self.REENTRY_COOLDOWN = 900
        self.BALANCE_CACHE_SECONDS = 60

        self.data_url = "https://fapi.binance.com"
        self.trade_url = ("https://testnet.binancefuture.com" if self.TESTNET
                          else "https://fapi.binance.com")
        self.time_offset_ms = 0

        self.db = DatabaseManager(db_file)
        self.triple = TripleStrategy(None, logger.warning)
        self.triple_exit = TripleExitManager(None, logger.warning)
        self.triple_db = TripleDB(db_file)
        self.port = int(os.environ.get('PORT', '10000'))

        self.session = None
        self.ws_task = None
        self._ws_connected = False
        # ✅ User Data Stream (listenKey) — لتتبع ORDER_TRADE_UPDATE و ACCOUNT_UPDATE
        self.user_stream_task = None
        self._listenkey = None
        self._listenkey_active = False
        self._listenkey_renew_ts = 0
        self.shutdown_requested = False
        self.EMERGENCY_MODE = False

        self.active_trades = {}
        self.all_futures_pairs = {}
        self.symbol_info_cache = {}
        self.min_qty_cache = {}
        self.max_qty_cache = {}
        self.min_notional_cache = {}
        self.price_precision_cache = {}
        self.qty_precision_cache = {}
        self._step_size = {}
        self._tick_size = {}
        self._price_cache = {}
        self.reentry_cooldown = {}
        self.trend_pending = {}
        self.stats = {'trades_executed': 0, 'trades_closed': 0}
        self.stats_triple = {'trades': 0, 'wins': 0, 'losses': 0, 'net_pnl': 0.0}
        self.triple_events = collections.deque(maxlen=200)
        self.error_counts = {}
        self.entry_history = collections.deque(maxlen=100)
        self.last_scan_info = {'ts': 0}
        self._balance_cache = None
        self._balance_ts = 0
        self._balance_fail_ts = 0
        self.server_ts = int(time.time())
        self._triple_cursor = 0
        self.last_positions_sync = 0.0
        self.last_sync_removed = []
        self.triple_capacity = int(self.triple.cfg.get('max_open_trades', self.max_open_triple))
        # ✅ علم: هل وُجد إعداد محفوظ في القاعدة لـ max_open_trades؟
        #    يُضبط أثناء تحميل triple_config في main_loop.
        self._has_saved_capacity = False

    # ─── الوصول للاستراتيجية ───
    def strategy_margin(self, key='TRIPLE'):
        return self.TRADE_SIZE_USDT

    def refresh_capacity(self):
        """يجعل السعة مرآة لإعداد الاستراتيجية — مصدر واحد موحّد للعداد."""
        try:
            self.triple_capacity = max(1, int(self.triple.cfg.get('max_open_trades', 1)))
        except Exception:
            self.triple_capacity = 1
        return self.triple_capacity

    def apply_env_capacity(self):
        """متغير البيئة MAX_OPEN_TRADES يعلو على إعدادات القاعدة عند الإقلاع.

        ✅ إصلاح أولوية السعة: MAX_OPEN_TRADES من البيئة يُطبَّق فقط عند عدم
        وجود إعداد محفوظ في القاعدة (triple_config). إذا عدّل المستخدم
        max_open_trades وحفظها من اللوحة، تُعتمد هي ولا يطغى متغير البيئة.
        """
        env_val = (os.environ.get('MAX_OPEN_TRADES') or '').strip()
        if env_val.isdigit():
            v = max(1, min(20, int(env_val)))
            # ✅ فقط إذا لم يوجد إعداد محفوظ في القاعدة
            if not self._has_saved_capacity:
                if v != int(self.triple.cfg.get('max_open_trades', 1)):
                    logger.info(f"⚙️ MAX_OPEN_TRADES من البيئة ({v}) يُطبَّق (لا إعداد محفوظ)")
                self.triple.cfg['max_open_trades'] = v
            else:
                # يوجد إعداد محفوظ — نحترمه ولا نطغيه
                logger.info(f"⚙️ السعة المحفوظة في القاعدة ({self.triple.cfg.get('max_open_trades')}) "
                            f"تعلو على MAX_OPEN_TRADES من البيئة ({v})")
        self.refresh_capacity()
        return self.triple_capacity

    def strategy_capacity(self, key='TRIPLE'):
        return self.triple_capacity

    def strategy_cfg(self, key='TRIPLE'):
        return self.triple.cfg

    def strategy_events(self, key='TRIPLE'):
        return self.triple_events

    def strategy_stats(self, key='TRIPLE'):
        return self.stats_triple

    @staticmethod
    def other_strategy(key):
        return None

    def count_open(self, key='TRIPLE'):
        return sum(1 for t in self.active_trades.values() if t.get('strategy') == 'TRIPLE')

    def _log_event(self, key, kind, msg):
        try:
            self.triple_events.append({"ts": time.time(), "kind": kind, "msg": msg})
        except Exception:
            pass

    # ─── مكافحة الصمت ───
    def _notify_error(self, source, exc=None, context=""):
        count = self.error_counts.get(source, 0) + 1
        self.error_counts[source] = count
        txt = f"⚠️ {source}" + (f" — {context}" if context else "")
        if exc is not None:
            txt += f" | `{str(exc)[:100]}`"
        logger.warning(f"{txt} (تكرار #{count})")
        if count == 1 or count % 10 == 0:
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(self.tg(f"{txt}\n🔢 مرات التكرار: {count}"))
            except RuntimeError:
                pass

    def _reset_error(self, source):
        self.error_counts.pop(source, None)

    def fmt_price(self, price):
        if price is None or price == 0:
            return "$0"
        if price < 0.00001:
            return f"${price:.10f}"
        elif price < 0.001:
            return f"${price:.6f}"
        elif price < 1:
            return f"${price:.4f}"
        elif price < 100:
            return f"${price:.2f}"
        return f"${price:,.2f}"

    def fmt_pct(self, pct):
        if pct is None:
            return "—"
        return f"{pct:+.2f}%"

    # ─── Telegram ───
    async def tg(self, msg):
        token = os.environ.get('TELEGRAM_BOT_TOKEN', '')
        chat_id = os.environ.get('TELEGRAM_CHAT_ID', '')
        if not token or not chat_id or not self.session:
            return
        try:
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            payload = {'chat_id': chat_id, 'text': msg, 'parse_mode': 'Markdown'}
            async with self.session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status != 200:
                    logger.warning(f"Telegram status {r.status}")
        except Exception as e:
            logger.warning(f"تعذر إرسال Telegram: {e}")

    # ─── طلبات Binance ───
    async def _fapi_request(self, method, path, params=None, signed=False, quiet=(), soft=(),
                            _retry_ts=True):
        """طلب موقّع/عام على trade_url مع احترام time_offset_ms وTESTNET."""
        params = dict(params or {})
        if signed:
            ts = int(time.time() * 1000) + self.time_offset_ms
            params['timestamp'] = ts
            params.pop('signature', None)
            query = urlencode(params)
            sig = hmac.new(self.binance_api_secret.encode(),
                           query.encode(), hashlib.sha256).hexdigest()
            params['signature'] = sig
        url = self.trade_url + path
        headers = {'X-MBX-APIKEY': self.binance_api_key}
        try:
            timeout = aiohttp.ClientTimeout(total=15)
            async with self.session.request(method, url, params=params,
                                            headers=headers, timeout=timeout) as r:
                txt = await r.text()
                try:
                    data = json.loads(txt)
                except Exception:
                    data = {'raw': txt}
                if r.status == 200:
                    return data
                code = data.get('code') if isinstance(data, dict) else None
                if code == -1021 and signed and _retry_ts:
                    await self.sync_server_time()
                    return await self._fapi_request(method, path, params, signed=signed,
                                                    quiet=quiet, soft=soft, _retry_ts=False)
                if code in quiet:
                    return None
                if code in soft:
                    return {'_soft_code': code}
                logger.warning(f"Binance API {method} {path} -> {r.status} {txt[:200]}")
                return data if isinstance(data, dict) else None
        except Exception as e:
            logger.warning(f"استثناء Binance API {method} {path}: {e}")
            return None

    async def _public_get(self, path, params=None):
        url = self.data_url + path
        try:
            timeout = aiohttp.ClientTimeout(total=15)
            async with self.session.get(url, params=params or {}, timeout=timeout) as r:
                if r.status != 200:
                    return None
                return await r.json()
        except Exception as e:
            logger.warning(f"استثناء public GET {path}: {e}")
            return None

    # ─── بيانات السوق ───
    async def load_market_data(self):
        try:
            info = await self._public_get('/fapi/v1/exchangeInfo')
            if not info or 'symbols' not in info:
                return False
            self.all_futures_pairs = {}
            for s in info['symbols']:
                sym = s['symbol']
                if s.get('contractType') == 'PERPETUAL' and s.get('status') == 'TRADING':
                    self.all_futures_pairs[sym] = s
                    self.symbol_info_cache[sym] = s
                    pp = s.get('pricePrecision', 2)
                    qp = s.get('quantityPrecision', 3)
                    self.price_precision_cache[sym] = pp
                    self.qty_precision_cache[sym] = qp
                    for f in s.get('filters', []):
                        ft = f.get('filterType')
                        if ft == 'PRICE_FILTER':
                            self._tick_size[sym] = float(f.get('tickSize', 0))
                        elif ft in ('MARKET_LOT_SIZE', 'LOT_SIZE'):
                            self._step_size[sym] = float(f.get('stepSize', 0))
                            self.min_qty_cache[sym] = float(f.get('minQty', 0.001))
                            self.max_qty_cache[sym] = float(f.get('maxQty', 1e9))
                        elif ft == 'MIN_NOTIONAL':
                            self.min_notional_cache[sym] = float(f.get('notional', 5.0))
            logger.info(f"📊 تم تحميل {len(self.all_futures_pairs)} زوجاً دائماً")
            return True
        except Exception as e:
            logger.error(f"فشل تحميل بيانات السوق: {e}")
            return False

    async def get_klines(self, symbol, interval='15m', limit=300):
        try:
            data = await self._public_get('/fapi/v1/klines',
                                          {'symbol': symbol, 'interval': interval, 'limit': limit})
            if not data or not isinstance(data, list):
                return None
            df = pd.DataFrame(data, columns=['time', 'open', 'high', 'low', 'close', 'volume',
                                             'close_time', 'quote_vol', 'trades',
                                             'taker_buy_base', 'taker_buy_quote', 'ignore'])
            for c in ['open', 'high', 'low', 'close', 'volume']:
                df[c] = pd.to_numeric(df[c], errors='coerce')
            df['time'] = pd.to_numeric(df['time'], errors='coerce')
            df = df.dropna(subset=['open', 'high', 'low', 'close'])
            return df
        except Exception as e:
            logger.warning(f"تعذر جلب klines {symbol} {interval}: {e}")
            return None

    def get_price(self, symbol):
        """آخر سعر bid/ask محفوظ — مع فحص القِدَم (PRICE_MAX_AGE ثوانٍ)."""
        p = self._price_cache.get(symbol)
        if not p:
            return None
        if time.time() - p.get('ts', 0) > self.PRICE_MAX_AGE:
            return None
        return {'bid': p['bid'], 'ask': p['ask']}

    async def _update_prices_rest(self):
        """تحديث الأسعار عبر REST (bookTicker لكل الأزواج) — احتياطي فقط.

        ✅ إصلاح: REST bookTicker يُستدعى فقط عند انقطاع WebSocket أو في
        التهيئة الأولية، وليس بشكل دوري دائم. هذا يقلل استهلاك Rate Limits
        ويجعل WebSocket المصدر الأساسي للأسعار.
        """
        if not self.all_futures_pairs:
            return
        try:
            data = await self._public_get('/fapi/v1/ticker/bookTicker')
            if data and isinstance(data, list):
                now = time.time()
                for t in data:
                    sym = t.get('symbol')
                    if sym and sym in self.all_futures_pairs:
                        try:
                            self._price_cache[sym] = {
                                'bid': float(t.get('bidPrice', 0)),
                                'ask': float(t.get('askPrice', 0)),
                                'ts': now,
                            }
                        except Exception:
                            pass
        except Exception as e:
            logger.warning(f"تعذر تحديث الأسعار (REST): {e}")

    async def _ws_price_feed(self):
        """تدفق الأسعار عبر WebSocket من Binance Futures مع إعادة اتصال تلقائية.

        ✅ عند نجاح الاتصال (_ws_connected == True) لا تُطلب تحديثات REST الدورية.
        REST bookTicker يبقى كاحتياطي يُستدعى من main_loop فقط عند الانقطاع.
        """
        if websockets is None:
            logger.info("ℹ️ مكتبة websockets غير متاحة — الأسعار عبر REST فقط")
            return
        ws_url = "wss://fstream.binance.com/ws/!bookTicker@arr"
        backoff = 1
        max_backoff = 15
        while not self.shutdown_requested:
            try:
                async with websockets.connect(
                    ws_url,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5,
                    max_size=2**24,
                ) as ws:
                    logger.info("📡 WebSocket متصل — تدفق الأسعار الفعلي بدأ")
                    self._ws_connected = True
                    backoff = 1
                    while not self.shutdown_requested:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=30)
                        except asyncio.TimeoutError:
                            try:
                                await ws.ping()
                            except Exception:
                                break
                            continue
                        try:
                            data = json.loads(msg)
                            items = data if isinstance(data, list) else [data]
                            now = time.time()
                            for t in items:
                                sym = t.get('s') or t.get('symbol')
                                if sym and sym in self.all_futures_pairs:
                                    bid = float(t.get('b') or t.get('bidPrice') or 0)
                                    ask = float(t.get('a') or t.get('askPrice') or 0)
                                    if bid > 0 and ask > 0:
                                        self._price_cache[sym] = {
                                            'bid': bid,
                                            'ask': ask,
                                            'ts': now,
                                        }
                        except Exception:
                            pass
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._ws_connected = False
                logger.warning(f"⚠️ WebSocket انقطع ({e}) — إعادة الاتصال خلال {backoff}ث")
            self._ws_connected = False
            if self.shutdown_requested:
                break
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)

    # ─── User Data Stream (listenKey) ───
    async def _ensure_listenkey(self):
        """ينشئ/يجدد listenKey لمتابعة تحديثات الأوامر والمراكز لحظياً.

        ✅ User Data Stream يكشف ORDER_TRADE_UPDATE (تنفيذ/تصفية الأوامر) و
        ACCOUNT_UPDATE (تغيرات المراكز/الرصيد) فوراً دون انتظار دورة positionRisk.
        يُجدد المفتاح كل 30 دقيقة (PUT /fapi/v1/listenKey) لمنع انتهاءه.
        """
        if not self.binance_api_key or not self.binance_api_secret:
            return
        now = time.time()
        # تجديد كل 30 دقيقة (1800 ثانية) — مع هامش أمان
        if self._listenkey and now - self._listenkey_renew_ts < 1800:
            return
        try:
            if not self._listenkey:
                # إنشاء مفتاح جديد
                resp = await self._fapi_request('POST', '/fapi/v1/listenKey', signed=True)
                if isinstance(resp, dict) and resp.get('listenKey'):
                    self._listenkey = resp['listenKey']
                    self._listenkey_renew_ts = now
                    logger.info(f"🔑 listenKey أُنشئ — User Data Stream جاهز")
                else:
                    logger.warning("⚠️ تعذر إنشاء listenKey — User Data Stream معطّل")
            else:
                # تجديد المفتاح القائم
                resp = await self._fapi_request('PUT', '/fapi/v1/listenKey', signed=True)
                # PUT يُرجع 200 فارغ عند النجاح — _fapi_request يُرجع {} أو None
                if resp is None:
                    # قد تعني فشل — نحاول إعادة الإنشاء
                    logger.warning("⚠️ تجديد listenKey فشل — محاولة إعادة إنشاء")
                    self._listenkey = None
                else:
                    self._listenkey_renew_ts = now
        except Exception as e:
            logger.warning(f"⚠️ استثناء في listenKey: {e}")
            self._listenkey = None

    async def _user_data_stream(self):
        """تدفق بيانات المستخدم عبر WebSocket — ORDER_TRADE_UPDATE و ACCOUNT_UPDATE.

        ✅ يكشف فوراً: تنفيذ الأوامر، تصفية المراكز، الإغلاق اليدوي، تغيرات الرصيد.
        عند استلام ACCOUNT_UPDATE أو ORDER_TRADE_UPDATE نُزامن المراكز مع Binance
        فوراً دون انتظار دورة positionRisk كل 60 ثانية.
        """
        if websockets is None:
            logger.info("ℹ️ websockets غير متاحة — User Data Stream معطّل")
            return
        base_url = "wss://stream.binancefuture.com" if self.TESTNET else "wss://fstream.binance.com"
        backoff = 2
        max_backoff = 30
        while not self.shutdown_requested:
            # تأكد من وجود listenKey صالح
            await self._ensure_listenkey()
            if not self._listenkey:
                logger.warning(f"⚠️ لا listenKey — إعادة المحاولة خلال {backoff}ث")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)
                continue
            try:
                ws_url = f"{base_url}/ws/{self._listenkey}"
                async with websockets.connect(
                    ws_url,
                    ping_interval=30,
                    ping_timeout=15,
                    close_timeout=5,
                    max_size=2**24,
                ) as ws:
                    logger.info("🔑 User Data Stream متصل — مراقبة الأوامر والمراكز لحظياً")
                    self._listenkey_active = True
                    backoff = 2
                    while not self.shutdown_requested:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=60)
                        except asyncio.TimeoutError:
                            # لا رسالة — نرسل ping
                            try:
                                await ws.ping()
                            except Exception:
                                break
                            continue
                        try:
                            data = json.loads(msg)
                            ev = data.get('e')
                            if ev == 'ACCOUNT_UPDATE':
                                # تغيرات في المراكز/الرصيد — نُزامن فوراً
                                logger.info("🔄 ACCOUNT_UPDATE — مزامنة المراكز فوراً")
                                try:
                                    await self.sync_positions_with_binance(notify_changes=False)
                                except Exception as e:
                                    logger.warning(f"تعذر المزامنة بعد ACCOUNT_UPDATE: {e}")
                            elif ev == 'ORDER_TRADE_UPDATE':
                                # تنفيذ/تصفية أمر — نُزامن المراكز فوراً
                                o = data.get('o', {})
                                exe_type = o.get('X')  #Execution Type (FILLED, CANCELED...)
                                sym = o.get('s')
                                logger.info(f"📋 ORDER_TRADE_UPDATE: {sym} {exe_type} — مزامنة")
                                try:
                                    await self.sync_positions_with_binance(notify_changes=False)
                                except Exception as e:
                                    logger.warning(f"تعذر المزامنة بعد ORDER_TRADE_UPDATE: {e}")
                            elif ev == 'listenKeyExpired':
                                # انتهى المفتاح — نُعيد إنشاءه
                                logger.warning("⚠️ listenKey انتهى — إعادة إنشاء")
                                self._listenkey = None
                                self._listenkey_active = False
                                break
                        except Exception:
                            pass
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"⚠️ User Data Stream انقطع ({e}) — إعادة خلال {backoff}ث")
            self._listenkey_active = False
            if self.shutdown_requested:
                break
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)

    async def sync_server_time(self):
        try:
            data = await self._public_get('/fapi/v1/time')
            if data and 'serverTime' in data:
                local = int(time.time() * 1000)
                self.time_offset_ms = int(data['serverTime'] - local)
                self.server_ts = int(data['serverTime'] / 1000)
                logger.info(f"⏱️ وقت الخادم مُزامن — فرق {self.time_offset_ms}ms")
        except Exception as e:
            logger.warning(f"تعذر مزامنة وقت الخادم: {e}")

    # ─── مزامنة المراكز الحقيقية مع Binance ───
    async def sync_positions_with_binance(self, notify_changes=True):
        """مزامنة active_trades مع المراكز الفعلية عبر /fapi/v2/positionRisk."""
        if not self.binance_api_key or not self.binance_api_secret:
            return
        if not self.session:
            return
        if self.api_keys_ok is False:
            return
        try:
            positions = await self._fapi_request('GET', '/fapi/v2/positionRisk', signed=True)
        except Exception as e:
            self._notify_error("مزامنة المراكز — فشل positionRisk", exc=e)
            return
        if not isinstance(positions, list):
            self._notify_error("مزامنة المراكز — استجابة غير متوقعة من positionRisk")
            return
        self.last_positions_sync = time.time()
        open_positions = {}
        for p in positions:
            try:
                amt = float(p.get('positionAmt') or 0)
                if amt != 0:
                    open_positions[p.get('symbol')] = {
                        'amt': amt,
                        'entryPrice': float(p.get('entryPrice') or 0),
                        'unRealizedProfit': float(p.get('unRealizedProfit') or 0),
                    }
            except Exception:
                continue

        removed = []
        adjusted = []
        for sym in list(self.active_trades.keys()):
            trade = self.active_trades.get(sym)
            if not trade:
                continue
            pos = open_positions.get(sym)
            if pos is None or pos['amt'] == 0:
                reason = ('أُغلق يدوياً أو تم تصفيته في Binance (لا مركز فعلي)')
                self._log_event('TRIPLE', 'SYNC',
                                f"🧹 {sym} أُزيلت — {reason}")
                logger.info(f"🧹 مزامنة: {sym} ليس له مركز مفتوح في Binance — إزالة من البوت")
                del self.active_trades[sym]
                try:
                    await self.db.remove_trade(sym)
                except Exception as e:
                    logger.warning(f"تعذر حذف {sym} من القاعدة: {e}")
                removed.append(sym)
                continue
            pos_side = 'BUY' if pos['amt'] > 0 else 'SELL'
            pos_qty = abs(pos['amt'])
            pos_entry = float(pos.get('entryPrice') or 0)
            if pos_entry > 0 and abs(pos_entry - float(trade.get('entry_price') or 0)) > 1e-8:
                trade['entry_price'] = pos_entry
                await self.db.save_trade(trade)
                if sym not in adjusted:
                    adjusted.append(sym)
            if trade['side'] != pos_side:
                self._log_event('TRIPLE', 'SYNC',
                                f"⚠️ {sym}: اتجاه المركز الفعلي ({pos_side}) معاكس للسجل — تصحيح")
                trade['side'] = pos_side
                trade['quantity'] = pos_qty
                trade['orig_quantity'] = pos_qty
                await self.db.save_trade(trade)
                if sym not in adjusted:
                    adjusted.append(sym)

        orphan = [s for s in open_positions if s not in self.active_trades]
        self.last_sync_removed = removed
        if (removed or adjusted) and notify_changes:
            msg_parts = []
            if removed:
                msg_parts.append(f"🧹 أُزيلت {len(removed)} صفقة شبحية: {', '.join(removed)}")
            if adjusted:
                msg_parts.append(f"🔧 صُحّحت {len(adjusted)} صفقة: {', '.join(adjusted)}")
            await self.tg("🔄 *مزامنة المراكز مع Binance*\n" + "\n".join(msg_parts))
            try:
                await panel_notify('TRIPLE', '🔄 مزامنة المراكز', ' | '.join(msg_parts))
            except Exception:
                pass
        if orphan and notify_changes:
            logger.info(f"ℹ️ مراكز مفتوحة يدوية (خارج إدارة البوت): {', '.join(orphan)}")
        if removed or adjusted:
            logger.info(f"🔄 مزامنة المراكز: أزيلت {len(removed)}، صُحّحت {len(adjusted)}")

    # ─── التحقق من مركز Binance الفعلي لرمز معين ───
    async def _verify_binance_position(self, symbol):
        """يتحقق من مركز Binance الفعلي لرمز معين ويُرجع (qty, side) أو (0, None).

        ✅ يُستخدم عند إغلاق صفقة بكمية <= 0 و realized_pnl == 0 للتأكد من
        عدم تسجيل خسارة وهمية دون التحقق من المركز الفعلي أو سجل التنفيذ.
        """
        if not self.binance_api_key or not self.binance_api_secret:
            return (0.0, None)
        try:
            positions = await self._fapi_request('GET', '/fapi/v2/positionRisk', signed=True,
                                                 quiet=(-2015,))
            if not isinstance(positions, list):
                return (0.0, None)
            for p in positions:
                if p.get('symbol') == symbol:
                    amt = float(p.get('positionAmt') or 0)
                    if amt != 0:
                        return (abs(amt), 'BUY' if amt > 0 else 'SELL')
                    return (0.0, None)
            return (0.0, None)
        except Exception:
            return (0.0, None)

    # ─── صياغة الكمية/السعر ───
    def format_quantity(self, symbol, qty):
        qty = float(qty)
        step = self._step_size.get(symbol, 0)
        if step and step > 0:
            qty = math.floor(qty / step) * step
            qp = self.qty_precision_cache.get(symbol, 3)
            return round(qty, qp)
        qp = self.qty_precision_cache.get(symbol, 3)
        return round(qty, qp)

    def qty_to_str(self, symbol, qty):
        qp = self.qty_precision_cache.get(symbol, 3)
        return f"{float(qty):.{qp}f}"

    def price_to_str(self, symbol, price):
        pp = self.price_precision_cache.get(symbol, 2)
        return f"{float(price):.{pp}f}"

    # ─── الرصيد ───
    async def get_available_usdt(self):
        now = time.time()
        if self._balance_cache and now - self._balance_ts < self.BALANCE_CACHE_SECONDS:
            return self._balance_cache
        if self.api_keys_ok is False or now - self._balance_fail_ts < 10:
            return self._balance_cache or 0.0
        try:
            data = await self._fapi_request('GET', '/fapi/v2/balance', signed=True)
            if isinstance(data, list):
                for b in data:
                    if b.get('asset') == 'USDT':
                        self._balance_cache = float(b.get('availableBalance', 0))
                        self._balance_ts = now
                        return self._balance_cache
            elif isinstance(data, dict) and data.get('code') is not None:
                self._balance_fail_ts = now
        except Exception as e:
            logger.warning(f"تعذر جلب الرصيد: {e}")
            self._balance_fail_ts = now
        return self._balance_cache or 0.0

    # ─── عنوان IP الصادر ───
    async def get_public_ip(self):
        endpoints = [
            ('https://api.ipify.org/?format=json', 'json'),
            ('https://checkip.amazonaws.com/', 'text'),
            ('https://ifconfig.me/ip', 'text'),
        ]
        for url, kind in endpoints:
            try:
                timeout = aiohttp.ClientTimeout(total=10)
                async with self.session.get(url, timeout=timeout) as r:
                    if r.status != 200:
                        continue
                    if kind == 'json':
                        ip = (await r.json(content_type=None) or {}).get('ip')
                    else:
                        ip = (await r.text()).strip()
                    if ip:
                        self.public_ip = ip
                        return ip
            except Exception:
                continue
        return None

    # ─── تبريد إعادة الدخول ───
    def is_in_cooldown(self, symbol):
        exp = self.reentry_cooldown.get(symbol)
        return bool(exp and time.time() < exp)

    # ─── دوران المرشّحين ───
    def _rotation_candidates(self, cursor_attr, limit):
        all_pairs = sorted(self.all_futures_pairs.keys())
        if not all_pairs:
            return []
        cursor = getattr(self, cursor_attr, 0)
        selected = []
        for i in range(limit):
            idx = (cursor + i) % len(all_pairs)
            selected.append(all_pairs[idx])
        setattr(self, cursor_attr, (cursor + limit) % len(all_pairs))
        return selected

    # ─── المسح ───
    async def _scan_strategy(self, key='TRIPLE'):
        strat = self.triple
        if not strat.cfg.get('enabled'):
            return
        try:
            candidates = self._rotation_candidates('_triple_cursor', int(strat.cfg['scan_limit']))
            if not candidates:
                return
            sigs = await strat.scan(self, candidates, set(self.active_trades),
                                    self.reentry_cooldown)
            if sigs:
                self._log_event(key, 'SCAN',
                                f"🔍 {len(sigs)} إشارة من {len(candidates)} زوجاً مفحوصاً")
            for a in sigs:
                await self.execute_trade(a)
                await asyncio.sleep(1)
        except Exception as e:
            self._notify_error("خطأ مسح استراتيجية Triple", exc=e)

    async def scan_volatile_coins(self):
        if self.EMERGENCY_MODE or not self.TRADE_ENABLED:
            return
        await self._scan_strategy('TRIPLE')
        self.last_scan_info = {
            'ts': time.time(),
            'triple': dict(getattr(self.triple, 'last_scan', {}) or {}),
        }

    # ─── التنفيذ ───
    async def ask_gemini_validator(self, analysis):
        """يرسل تفاصيل الصفقة مع آخر الشموع إلى Google Gemini API لتقييمها قبل التنفيذ."""
        if not self.triple.cfg.get('useGeminiFilter', True):
            return True, "فلتر Gemini معطّل من الإعدادات"

        gemini_key = os.environ.get('GEMINI_API_KEY', '').strip()
        if not gemini_key:
            return True, "مفتاح GEMINI_API_KEY غير مضاف — تخطي الفلتر"

        symbol = analysis['symbol']
        tf = self.triple.cfg.get('timeframe', '15m')
        model = self.triple.cfg.get('geminiModel', 'gemini-2.5-flash')

        # جلب آخر 8 شموع مغلقة ليفهم Gemini سياق السوق والزخم
        candles_summary = "غير متوفر"
        try:
            df = await self.get_klines(symbol, tf, 12)
            if df is not None and len(df) >= 5:
                recent = df.iloc[-9:-1]
                rows = [
                    f"O:{r['open']:.4g} H:{r['high']:.4g} L:{r['low']:.4g} C:{r['close']:.4g} V:{int(r['volume'])}"
                    for _, r in recent.iterrows()
                ]
                candles_summary = " | ".join(rows)
        except Exception:
            pass

        prompt = f"""أنت مدير مخاطر تداول عقود آجلة (Crypto Futures) صارم. قيّم هذه الإشارة قبل تنفيذها:
- الزوج: {symbol} | الفريم: {tf}
- الاتجاه المقترح: {analysis['direction']}
- سعر الدخول: {analysis['price']}
- وقف الخسارة (SL): {analysis['sl']}
- الأهداف: TP1={analysis['tp1']} | TP2={analysis.get('tp2')} | TP3={analysis.get('tp3')}
- مؤشرات التقاطع الثلاثي: {json.dumps(analysis.get('components', {}), ensure_ascii=False)}
- حركة آخر 8 شموع: {candles_summary}

قواعد التقييم:
1. ارفض (REJECT) إذا كان الدخول عكس زخم شموع عنيفة جداً أو في ذروة تشبع متطرف (مثل RSI فوق 78 للشراء أو تحت 22 للبيع) أو إذا كانت المسافة للوقف غير منطقية.
2. وافق (APPROVE) إذا كان الهيكل السعري والزخم يدعمان استمرار الحركة نحو TP1 على الأقل.

أجب بصيغة JSON فقط بدون أي كلام خارجها:
{{"decision": "APPROVE" أو "REJECT", "reason": "السبب بالعربية في أقل من 12 كلمة"}}"""

        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={gemini_key}"
        headers = {"Content-Type": "application/json"}
        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": 0.1,
                "responseMimeType": "application/json"
            }
        }

        try:
            timeout = aiohttp.ClientTimeout(total=12)
            async with self.session.post(url, headers=headers, json=payload, timeout=timeout) as resp:
                if resp.status != 200:
                    logger.warning(f"⚠️ فشل اتصال Gemini ({resp.status}) — السماح بالصفقة احتياطياً")
                    return True, f"تخطي (خطأ اتصال {resp.status})"
                data = await resp.json()
                content = data['candidates'][0]['content']['parts'][0]['text'].strip()
                res = json.loads(content)
                approved = str(res.get('decision', '')).upper() == 'APPROVE'
                reason = str(res.get('reason', 'لا يوجد سبب'))[:80]
                return approved, reason
        except Exception as e:
            logger.warning(f"⚠️ استثناء في فلتر Gemini: {e}")
            return True, "تجاوز تلقائي بسبب المهلة"

    async def execute_trade(self, analysis):
        if not self.TRADE_ENABLED:
            return
        if self.EMERGENCY_MODE:
            return
        # ✅ فحص api_keys_ok قبل أي أمر تداول
        if self.api_keys_ok is False:
            return
        strategy = analysis.get('strategy', 'TRIPLE')
        symbol = analysis['symbol']
        want_buy = analysis['direction'] == 'BUY'

        # ── إشارات الإغلاق المعاكسة (مطابق Pine تماماً) ──
        existing = self.active_trades.get(symbol)
        if existing is not None:
            if (existing['side'] == 'BUY') == want_buy:
                return  # نفس الاتجاه — لا إجراء
            self._log_event('TRIPLE', 'OPPOSITE',
                            f"{symbol} إشارة معاكسة ({analysis['direction']}) — إغلاق "
                            f"صفقة {'الشراء' if existing['side'] == 'BUY' else 'البيع'} المفتوحة")
            # ✅ إصلاح: التحقق من نجاح الإغلاق المعاكس قبل فتح الصفقة الجديدة.
            #    يجب إغلاق الصفقة المعاكسة بالكامل قبل فتح الصفقة الجديدة، مع
            #    التحقق الدائم من self.api_keys_ok.
            if self.api_keys_ok is False:
                logger.warning(f"⚠️ {symbol} مفاتيح API مرفوضة — لا تنفيذ إغلاق معاكس")
                return
            await self.close_trade(existing, reason='OPPOSITE_SIGNAL', full_close=True)
            if symbol in self.active_trades:
                # فشل الإغلاق — لا نفتح صفقة جديدة فوق الموجودة
                logger.warning(f"⚠️ {symbol} فشل إغلاق الصفقة المعاكسة — لا فتح صفقة جديدة")
                return
            self.reentry_cooldown.pop(symbol, None)

        margin = self.strategy_margin(strategy)
        if self.count_open(strategy) >= self.strategy_capacity(strategy):
            return
        if self.is_in_cooldown(symbol):
            return

        available = await self.get_available_usdt()
        if available < margin * 1.2:
            self._notify_error("رصيد غير كافٍ للدخول",
                               context=f"المتاح {available:.2f}$ < المطلوب {margin * 1.2:.2f}$")
            return

        # ── فحص الصفقة عبر وكيل Gemini قبل التنفيذ ──
        approved, gemini_reason = await self.ask_gemini_validator(analysis)
        if not approved:
            msg = f"🤖 Gemini رفض صفقة {symbol} ({analysis['direction']}): {gemini_reason}"
            logger.info(msg)
            self._log_event('TRIPLE', 'GEMINI_REJECT', msg)
            try:
                await panel_notify('TRIPLE', f"🤖 Gemini رفض — {symbol}", gemini_reason, level='bad')
            except Exception:
                pass
            return
        else:
            self._log_event('TRIPLE', 'GEMINI_OK', f"🤖 Gemini وافق على {symbol}: {gemini_reason}")

    
        notional = margin * self.LEVERAGE
        # ✅ التحقق من فلاتر stepSize و tickSize الحقيقية من exchangeInfo
        #    لو لم تُحمَل بيانات الفلاتر بعد نرفض الدخول (لا تنفيذ بأحجام غير صحيحة)
        step = self._step_size.get(symbol)
        tick = self._tick_size.get(symbol)
        if step is None or tick is None:
            logger.warning(f"⚠️ {symbol} فلاتر stepSize/tickSize غير محملة — تخطي الدخول")
            return
        quantity = self.format_quantity(symbol, notional / analysis['price'])
        if quantity <= 0:
            return
        if quantity < self.min_qty_cache.get(symbol, 0.0):
            return
        if quantity > self.max_qty_cache.get(symbol, 1e12):
            return
        if quantity * analysis['price'] < self.min_notional_cache.get(symbol, 5.0) * 1.02:
            return

        await self._fapi_request('POST', '/fapi/v1/leverage',
            {'symbol': symbol, 'leverage': self.LEVERAGE}, signed=True)
        await self._fapi_request('POST', '/fapi/v1/marginType',
            {'symbol': symbol, 'marginType': 'ISOLATED'}, signed=True, quiet=(-4046,))

        side = 'BUY' if want_buy else 'SELL'
        result = await self._fapi_request('POST', '/fapi/v1/order', {
            'symbol': symbol, 'side': side, 'type': 'MARKET',
            'quantity': self.qty_to_str(symbol, quantity)
        }, signed=True)

        if not result or result.get('status') not in ['FILLED', 'NEW']:
            await self.tg(f"❌ *فشل تنفيذ {side} (`{symbol}`)*")
            return

        avg = float(result.get('avgPrice') or 0)
        order_id = result.get('orderId')
        if avg <= 0 and order_id:
            await asyncio.sleep(0.4)
            ord_info = await self._fapi_request('GET', '/fapi/v1/order', {
                'symbol': symbol, 'orderId': order_id
            }, signed=True, quiet=(-2013,))
            if isinstance(ord_info, dict):
                avg = float(ord_info.get('avgPrice') or 0)

        if avg <= 0:
            positions = await self._fapi_request('GET', '/fapi/v2/positionRisk',
                                                 {'symbol': symbol}, signed=True)
            if isinstance(positions, list):
                for p in positions:
                    if p.get('symbol') == symbol and float(p.get('positionAmt') or 0) != 0:
                        avg = float(p.get('entryPrice') or 0)
                        break

        entry_price = avg if avg > 0 else analysis['price']
        risk_dist = analysis.get('risk_dist') or abs(analysis['price'] - analysis['sl'])
        if want_buy:
            sl_price = entry_price - risk_dist
            tp1_price = entry_price + risk_dist * float(self.triple.cfg.get('rr1', 1.0))
            tp2_price = entry_price + risk_dist * float(self.triple.cfg.get('rr2', 2.0))
            tp3_price = entry_price + risk_dist * float(self.triple.cfg.get('rr3', 3.0))
        else:
            sl_price = entry_price + risk_dist
            tp1_price = entry_price - risk_dist * float(self.triple.cfg.get('rr1', 1.0))
            tp2_price = entry_price - risk_dist * float(self.triple.cfg.get('rr2', 2.0))
            tp3_price = entry_price - risk_dist * float(self.triple.cfg.get('rr3', 3.0))

        analysis['sl'] = sl_price
        analysis['tp1'] = tp1_price
        analysis['tp2'] = tp2_price
        analysis['tp3'] = tp3_price

        trade_data = {
            'symbol': symbol, 'side': side, 'entry_price': entry_price,
            'quantity': quantity,
            'orig_quantity': quantity,
            'sl': sl_price, 'tp': tp1_price,
            'tp2': tp2_price, 'tp3': tp3_price,
            'score': analysis.get('score', 8),
            'trailing_active': False, 'highest_price': entry_price,
            'lowest_price': entry_price, 'entry_time': time.time(),
            'partial_closed': False, 'partial1_closed': False, 'partial2_closed': False,
            'realized_pnl': 0.0, 'stop_order_id': None, 'tp_order_id': None,
            'stop_is_algo': False, 'tp_is_algo': False,
            'stop_synced_price': None, 'tp_skipped': False,
            'strategy': 'TRIPLE', 'margin_usdt': margin,
            'atr_ref': analysis.get('atr_ref'),
            'components': analysis.get('components', {}),
        }
        self.active_trades[symbol] = trade_data
        await self.db.save_trade(trade_data)
        self.stats['trades_executed'] += 1
        self.stats_triple['trades'] += 1
        self.entry_history.append((time.time(), time.strftime("%Y-%m-%d")))
        await self.db.log_signal(symbol, side, trade_data['entry_time'],
                                 analysis.get('score', 8), analysis.get('components', {}))

        try:
            await self.triple_db.open(trade_data)
        except Exception as e:
            logger.warning(f"تعذر تسجيل صفقة Triple: {e}")

        try:
            cd = int(self.triple.cfg.get('reentry_cooldown', 3600))
        except Exception:
            cd = 3600
        self.reentry_cooldown[symbol] = time.time() + cd

        risk = abs(analysis['price'] - analysis['sl'])
        self._log_event('TRIPLE', 'ENTRY',
                        f"{symbol} {analysis['direction']} @ {self.fmt_price(analysis['price'])} | "
                        f"وقف {self.fmt_price(analysis['sl'])} ({risk / analysis['price'] * 100:.2f}%) | "
                        f"TP1 {self.fmt_price(analysis['tp1'])} | TP2 {self.fmt_price(analysis.get('tp2'))} | "
                        f"TP3 {self.fmt_price(analysis.get('tp3'))} | مخاطرة {risk * (quantity or 0):.3f}$")
        try:
            await panel_notify('TRIPLE', f"🧭 دخول جديد — {symbol}",
                               f"{analysis['direction']} @ {self.fmt_price(entry_price)} | "
                               f"SL {self.fmt_price(analysis['sl'])} | TP1 {self.fmt_price(analysis['tp1'])}",
                               level='good')
        except Exception:
            pass
        await self.tg(f"🧭 *Triple Confluence — صفقة جديدة*\n━━━━━━━━━━━━━━━━━━━━━━━━\n"
                      f"🪙 `{symbol}` | رافعة {self.LEVERAGE}x | هامش {margin:.0f}$\n"
                      f"💵 الدخول: {self.fmt_price(entry_price)}\n"
                      f"🛑 SL: {self.fmt_price(analysis['sl'])}\n"
                      f"🎯 TP1: {self.fmt_price(analysis['tp1'])}\n"
                      f"🎯 TP2: {self.fmt_price(analysis.get('tp2'))}\n"
                      f"🎯 TP3: {self.fmt_price(analysis.get('tp3'))}")

    async def close_trade_partial(self, trade, ratio, reason, exit_price, base_qty=None):
        """إغلاق جزئي بنسبة ratio من الكمية الأصلية. يُرجع True عند النجاح فعلاً."""
        if ratio <= 0 or ratio >= 1:
            return False
        symbol = trade['symbol']
        if symbol not in self.active_trades:
            return False
        side = trade['side']
        close_side = 'SELL' if side == 'BUY' else 'BUY'
        base = float(base_qty or trade.get('orig_quantity') or trade['quantity'])
        qty = base * ratio
        qty = min(qty, float(trade['quantity']))
        qty = self.format_quantity(symbol, qty)
        if qty <= 0:
            return False
        try:
            result = await self._fapi_request('POST', '/fapi/v1/order', {
                'symbol': symbol, 'side': close_side, 'type': 'MARKET',
                'quantity': self.qty_to_str(symbol, qty),
                'reduceOnly': 'true'
            }, signed=True, soft=(-2022, -2011))
            if result and result.get('_soft_code') == -2022:
                trade['quantity'] = 0.0
                return True
            if result and result.get('_soft_code') == -2011:
                logger.warning(f"⚠️ {symbol} خطأ -2011 (Precision) — إعادة محاولة بضبط stepSize")
                step = self._step_size.get(symbol, 0)
                if step and step > 0:
                    qty = math.floor(float(trade['quantity']) / step) * step
                    qp = self.qty_precision_cache.get(symbol, 3)
                    qty = round(qty, qp)
                    if qty > 0:
                        result = await self._fapi_request('POST', '/fapi/v1/order', {
                            'symbol': symbol, 'side': close_side, 'type': 'MARKET',
                            'quantity': self.qty_to_str(symbol, qty),
                            'reduceOnly': 'true'
                        }, signed=True)
            if not result or result.get('status') not in ['FILLED', 'NEW']:
                if result and result.get('_soft_code') == -2022:
                    trade['quantity'] = 0.0
                    return True
                return False
            entry = trade['entry_price']
            pnl = ((exit_price - entry) if side == 'BUY' else (entry - exit_price)) * qty
            trade['realized_pnl'] = float(trade.get('realized_pnl') or 0) + pnl
            trade['quantity'] = float(trade['quantity']) - qty
            try:
                await self.db.save_trade(trade)
            except Exception as e:
                logger.warning(f"تعذر حفظ الصفقة الجزئية {symbol}: {e}")
            try:
                await self.db.update_daily_pnl(pnl, is_win=(pnl > 0))
            except Exception as e:
                logger.warning(f"تعذر تحديث أرباح اليوم (جزئي) {symbol}: {e}")
            try:
                await self.triple_db.update_daily(pnl, win=(pnl > 0))
            except Exception:
                pass
            self._log_event('TRIPLE', 'PARTIAL',
                            f"{symbol} إغلاق {reason} ({ratio*100:.0f}% من الأصل) "
                            f"@ {self.fmt_price(exit_price)} | ربح {pnl:+.2f}$")
            try:
                await panel_notify('TRIPLE', f"🎯 {reason} — {symbol}",
                                   f"إغلاق جزئي @ {self.fmt_price(exit_price)} | ربح {pnl:+.2f}$")
            except Exception:
                pass
            return True
        except Exception as e:
            logger.warning(f"تعذر الإغلاق الجزئي {symbol}: {e}")
            return False

    async def close_trade(self, trade, reason='CLOSE', exit_price=None, full_close=False):
        """إغلاق الصفقة بالكامل.

        ✅ إصلاح qty <= 0 و realized_pnl == 0: القديم كان يسجل الإغلاق فوراً
        حتى لو كان realized_pnl == 0 بسبب عدم تنفيذ أو تقريب صفر — فتُسجل
        كخسارة وهمية. الآن: لو realized_pnl == 0 نتحقق من مركز Binance الفعلي
        أو سجل التنفيذ قبل تسجيل أي نتيجة. لو المركز الفعلي = 0 (أُغلق فعلاً)
        نُسجّل بإجمالي 0 (لا ربح ولا خسارة) بدل خسارة وهمية.
        """
        symbol = trade['symbol']
        if symbol not in self.active_trades:
            return
        side = trade['side']
        close_side = 'SELL' if side == 'BUY' else 'BUY'
        qty = float(trade['quantity'])
        if qty <= 0:
            # ✅ إصلاح حرج: لو realized_pnl == 0 لا نسجل خسارة وهمية دون التحقق
            #    من مركز Binance الفعلي أو سجل التنفيذ. قد يكون الإغلاق الجزئي
            #    (TP1+TP2) نُفذ لكن realized_pnl لم يُحدّث بسبب تقريب صفر أو
            #    استثناء سابق. نتحقق من المركز الفعلي:
            entry = trade['entry_price']
            realized = float(trade.get('realized_pnl') or 0.0)
            actual_exit = exit_price or (self.get_price(symbol) or {}).get(
                'bid' if side == 'BUY' else 'ask') or entry
            # ✅ لو realized == 0 نتحقق من مركز Binance الفعلي
            if realized == 0.0:
                try:
                    pos_qty, pos_side = await self._verify_binance_position(symbol)
                except Exception:
                    pos_qty, pos_side = (0.0, None)
                if pos_qty > 0 and pos_side == side:
                    # المركز ما زال مفتوحاً — لا نُغلق بالكمية 0، نُزامن لاحقاً
                    logger.warning(f"⚠️ {symbol} qty=0 و realized=0 لكن المركز الفعلي "
                                   f"ما زال مفتوحاً ({pos_qty}) — لا تسجيل خسارة وهمية")
                    return
                # المركز الفعلي = 0 (أُغلق فعلاً) → نُسجّل بإجمالي 0 (لا خسارة وهمية)
                pnl = 0.0
                win = False  # لا ربح ولا خسارة — لا نُعده خاسرة
            else:
                pnl = realized
                win = pnl > 0
            margin_used = float(trade.get('margin_usdt') or 50)
            del self.active_trades[symbol]
            try:
                await self.db.remove_trade(symbol)
            except Exception as e:
                logger.error(f"تعذر حذف {symbol} من القاعدة بعد إغلاق الكمية الصفرية: {e}")
            try:
                await self.db.update_daily_pnl(pnl, is_win=(win if pnl != 0 else None))
            except Exception as e:
                logger.warning(f"تعذر تحديث أرباح اليوم {symbol}: {e}")
            try:
                await self.triple_db.update_daily(pnl, win=win)
            except Exception:
                pass
            try:
                await self.triple_db.close(symbol, trade['entry_time'], time.time(),
                                           actual_exit, pnl, pnl / max(margin_used, 1) * 100,
                                           win, reason, trade.get('components', {}))
            except Exception:
                pass
            try:
                await self.db.set_signal_result(symbol, trade['entry_time'], win, pnl,
                                                close_ts=time.time(), close_reason=reason,
                                                duration_s=time.time() - trade['entry_time'],
                                                pnl_pct=pnl / max(margin_used, 1) * 100)
            except Exception as e:
                logger.warning(f"تعذر تحديث نتيجة الإشارة {symbol}: {e}")
            self.stats['trades_closed'] += 1
            if win:
                self.stats_triple['wins'] += 1
            elif pnl < 0:
                self.stats_triple['losses'] += 1
            self.stats_triple['net_pnl'] += pnl
            self._log_event('TRIPLE', 'CLOSE',
                            f"{symbol} أُغلقت ({reason}) — كمية متبقية 0 | صافي {pnl:+.2f}$")
            try:
                await panel_notify('TRIPLE', f"{'✅' if win else '➖' if pnl == 0 else '❌'} إغلاق {symbol}",
                                   f"{reason} | صافي {pnl:+.2f}$",
                                   level='good' if win else 'bad')
            except Exception:
                pass
            await self.tg(f"{'✅' if win else '➖' if pnl == 0 else '❌'} *Triple — صفقة مغلقة*\n"
                          f"🪙 `{symbol}` | السبب: {reason}\n"
                          f"💵 الدخول: {self.fmt_price(entry)} | الخروج: {self.fmt_price(actual_exit)}\n"
                          f"💰 الصافي: {pnl:+.2f}$")
            return
        try:
            result = await self._fapi_request('POST', '/fapi/v1/order', {
                'symbol': symbol, 'side': close_side, 'type': 'MARKET',
                'quantity': self.qty_to_str(symbol, qty),
                'reduceOnly': 'true'
            }, signed=True, soft=(-2022, -2011))
            if result and result.get('_soft_code') == -2011:
                logger.warning(f"⚠️ {symbol} خطأ -2011 في الإغلاق الكامل — إعادة بضبط stepSize")
                step = self._step_size.get(symbol, 0)
                if step and step > 0:
                    qty = math.floor(qty / step) * step
                    qp = self.qty_precision_cache.get(symbol, 3)
                    qty = round(qty, qp)
                    if qty > 0:
                        result = await self._fapi_request('POST', '/fapi/v1/order', {
                            'symbol': symbol, 'side': close_side, 'type': 'MARKET',
                            'quantity': self.qty_to_str(symbol, qty),
                            'reduceOnly': 'true'
                        }, signed=True)
            if not (result and (result.get('_soft_code') == -2022
                                or result.get('status') in ['FILLED', 'NEW'])):
                return
            # ✅ الإزالة الفورية بعد نجاح الأمر
            del self.active_trades[symbol]
            try:
                await self.db.remove_trade(symbol)
            except Exception as e:
                logger.error(f"تعذر حذف {symbol} من القاعدة بعد الإغلاق: {e}")

            entry = trade['entry_price']
            actual_exit = exit_price or (self.get_price(symbol) or {}).get('bid' if side == 'BUY' else 'ask') or entry
            pnl = ((actual_exit - entry) if side == 'BUY' else (entry - actual_exit)) * qty
            pnl += float(trade.get('realized_pnl') or 0.0)
            win = pnl > 0
            margin_used = float(trade.get('margin_usdt') or 50)
            try:
                await self.db.update_daily_pnl(pnl, is_win=win)
            except Exception as e:
                logger.warning(f"تعذر تحديث أرباح اليوم {symbol}: {e}")
            try:
                await self.triple_db.update_daily(pnl, win=win)
            except Exception:
                pass
            try:
                await self.triple_db.close(symbol, trade['entry_time'], time.time(),
                                           actual_exit, pnl, pnl / margin_used * 100,
                                           win, reason, trade.get('components', {}))
            except Exception:
                pass
            try:
                await self.db.set_signal_result(symbol, trade['entry_time'], win, pnl,
                                                close_ts=time.time(), close_reason=reason,
                                                duration_s=time.time() - trade['entry_time'],
                                                pnl_pct=pnl / max(margin_used, 1) * 100)
            except Exception as e:
                logger.warning(f"تعذر تحديث نتيجة الإشارة {symbol}: {e}")
            self.stats['trades_closed'] += 1
            if win:
                self.stats_triple['wins'] += 1
            else:
                self.stats_triple['losses'] += 1
            self.stats_triple['net_pnl'] += pnl
            self._log_event('TRIPLE', 'CLOSE',
                            f"{symbol} أُغلقت ({reason}) @ {self.fmt_price(actual_exit)} | "
                            f"صافي {pnl:+.2f}$")
            try:
                await panel_notify('TRIPLE', f"{'✅' if win else '❌'} إغلاق {symbol}",
                                   f"{reason} @ {self.fmt_price(actual_exit)} | صافي {pnl:+.2f}$",
                                   level='good' if win else 'bad')
            except Exception:
                pass
            await self.tg(f"{'✅' if win else '❌'} *Triple — صفقة مغلقة*\n"
                          f"🪙 `{symbol}` | السبب: {reason}\n"
                          f"💵 الدخول: {self.fmt_price(entry)} | الخروج: {self.fmt_price(actual_exit)}\n"
                          f"💰 الصافي: {pnl:+.2f}$")
        except Exception as e:
            logger.error(f"خطأ إغلاق {symbol}: {e}")

    # ─── إغلاق طارئ ───
    async def close_all_trades(self):
        closed = []
        for sym in list(self.active_trades.keys()):
            try:
                await self.close_trade(self.active_trades[sym], reason='CLOSE_ALL')
                if sym not in self.active_trades:
                    closed.append(sym)
            except Exception as e:
                logger.error(f"تعذر إغلاق {sym}: {e}")
        return closed

    # ─── التحكم الخارجي ───
    async def request_shutdown(self, source='external'):
        logger.info(f"🛑 طلب إيقاف من {source}")
        self.shutdown_requested = True

    async def pause_trading(self):
        self.TRADE_ENABLED = False
        logger.info("⏸️ التداول موقوف")

    async def resume_trading(self):
        if self.api_keys_ok is False:
            logger.error("▶️ رفض استئناف التداول — مفاتيح API مرفوضة من Binance (-2015)")
            return False
        if self.binance_api_key and self.binance_api_secret:
            self.TRADE_ENABLED = True
            logger.info("▶️ التداول مستأنف")
            return True
        return False

    # ─── إدارة الصفقات ───
    async def manage_positions(self):
        if not self.active_trades:
            return
        for sym in list(self.active_trades.keys()):
            trade = self.active_trades.get(sym)
            if not trade:
                continue
            try:
                await self.triple_exit.manage(self, trade)
            except Exception as e:
                logger.warning(f"خطأ إدارة {sym}: {e}")

    # ─── اللوب الرئيسي ───
    async def main_loop(self):
        self.session = aiohttp.ClientSession()
        try:
            await start_web_panel(self)
            loop = asyncio.get_running_loop()
            for sig_name in ('SIGINT', 'SIGTERM'):
                try:
                    loop.add_signal_handler(getattr(sys_signal, sig_name),
                        lambda: asyncio.ensure_future(self.request_shutdown('signal')))
                except (NotImplementedError, ValueError, AttributeError, RuntimeError):
                    pass

            await self.db.init_db()
            try:
                await self.triple_db.init()
            except Exception as e:
                logger.error(f"تعذر تهيئة قاعدة بيانات Triple: {e}")

            # تحميل الإعدادات المحفوظة
            try:
                raw = await self.db.get_param('triple_config')
                if raw:
                    saved = json.loads(raw)
                    if int(saved.get('settings_version', 1) or 1) == \
                            int(self.triple.cfg.get('settings_version', 1) or 1):
                        self.triple.cfg.update({k: v for k, v in saved.items() if k in self.triple.cfg})
                        self.triple_exit.cfg.update(self.triple.cfg)
                        # ✅ نسجل أن إعداد max_open_trades محفوظ في القاعدة
                        #    حتى لا يطغيه MAX_OPEN_TRADES من البيئة
                        if 'max_open_trades' in saved:
                            self._has_saved_capacity = True
                    else:
                        logger.info("⚙️ triple_config: إعدادات قديمة — اعتماد الجديدة")
                        await self.db.set_param('triple_config', json.dumps(dict(self.triple.cfg)))
            except Exception as e:
                logger.warning(f"تعذر تحميل إعدادات محفوظة (triple_config): {e}")
            # ✅ أولوية السعة: إعداد القاعدة المحفوظ يعلو؛ MAX_OPEN_TRADES من البيئة
            #    فقط عند عدم وجود إعداد محفوظ.
            cap = self.apply_env_capacity()
            logger.info(f"⚙️ السعة — Triple: {cap} | "
                        f"هامش: {self.TRADE_SIZE_USDT}$ | رافعة: {self.LEVERAGE}x | "
                        f"الحالة: {'مفعّل' if self.triple.cfg.get('enabled') else 'متوقف'}"
                        f" | مصدر السعة: {'القاعدة' if self._has_saved_capacity else 'البيئة'}")

            market_tries = 0
            while not self.all_futures_pairs:
                market_tries += 1
                await self.load_market_data()
                if self.all_futures_pairs:
                    break
                if market_tries == 1 or market_tries % 5 == 0:
                    await self.tg(f"❌ *فشل تحميل بيانات السوق* (محاولة {market_tries})\n"
                                  f"⏳ إعادة خلال 30 ثانية...")
                logger.error(f"فشل تحميل بيانات السوق (محاولة {market_tries})")
                await asyncio.sleep(30)

            self.active_trades = await self.db.load_active_trades()
            await self.sync_server_time()

            # ✅ تحديث أسعار REST أولي (قبل بدء WebSocket)
            await self._update_prices_rest()

            if self.binance_api_key and self.binance_api_secret:
                await self.sync_positions_with_binance(notify_changes=True)

            try:
                dual = await self._fapi_request('GET', '/fapi/v1/positionSide/dual', signed=True)
                if dual and dual.get('dualSidePosition'):
                    if self.TRADE_ENABLED:
                        self.TRADE_ENABLED = False
                        logger.error("❌ الحساب في وضع Hedge Mode — التحويل للمراقبة فقط")
                        await self.tg("⚠️ *تنبيه:* Hedge Mode مفعّل — البوت لوضع One-way فقط.")
            except Exception as e:
                logger.warning(f"تعذر فحص Hedge Mode: {e}")

            pub_ip = await self.get_public_ip()
            if pub_ip:
                logger.info(
                    f"🌐 IP الصادر (الذي يراه Binance): {pub_ip} — أضفه إلى قائمة "
                    "IP Restriction في إعدادات مفتاح API. تنبيه: على Railway يتغير "
                    "العنوان مع كل نشر إلا إن فعّلت Static Outbound IPs (خطة Pro)")
            else:
                logger.warning("🌐 تعذر اكتشاف IP الصادر (تعذر الوصول لخدمات صدى IP)")

            if self.TRADE_ENABLED and self.binance_api_key:
                acct = await self._fapi_request('GET', '/fapi/v2/account', signed=True)
                if (isinstance(acct, dict) and acct.get('canTrade') is not None
                        and acct.get('totalWalletBalance') is not None):
                    self.api_keys_ok = True
                    logger.info("✅ مفاتيح API صالحة — صلاحية التداول مؤكدة من Binance")
                else:
                    self.api_keys_ok = False
                    self.TRADE_ENABLED = False
                    code = acct.get('code') if isinstance(acct, dict) else 'لا استجابة'
                    logger.error(
                        f"❌ مفاتيح API مرفوضة من Binance (رمز {code}) — التحويل للمراقبة فقط. "
                        "تحقق من: مفاتيح الحساب الحقيقي عند TESTNET=false / صلاحية Futures على "
                        "المفتاح / تقييد IP / سلامة النسخ بدون مسافات")
                    if code == -2015 and pub_ip:
                        logger.error(
                            f"🌐 IP الصادر الحالي: {pub_ip} — إذا كان تقييد IP مفعّلاً "
                            "على المفتاح فأضف هذا العنوان إلى قائمة السماح في Binance")
                    await self.tg("🚫 *مفاتيح API مرفوضة من Binance*\n"
                                  "البوت في وضع المراقبة حتى إصلاح المفاتيح وإعادة التشغيل.\n"
                                  "تحقق من: مفاتيح حقيقية + صلاحية *Enable Futures* + تقييد IP"
                                  + (f"\n🌐 IP الصادر الحالي: `{pub_ip}`" if pub_ip else ""))

            await self.tg("🧭 *Triple Confluence Navigator جاهز*\n"
                          f"⚙️ هامش {self.TRADE_SIZE_USDT}$ | رافعة {self.LEVERAGE}x | "
                          f"متزامنة {self.triple_capacity}\n"
                          f"📊 الأزواج المحملة: {len(self.all_futures_pairs)}\n"
                          f"🧪 البيئة: {'TESTNET' if self.TESTNET else 'حقيقي'}\n"
                          f"📡 التداول: {'مفعّل' if self.TRADE_ENABLED else 'متوقف (مراقبة فقط)'}"
                          + (f"\n🌐 IP الصادر: `{pub_ip}`" if pub_ip else ""))

            # ✅ تشغيل WebSocket الفعلي لتدفق الأسعار + User Data Stream
            self.ws_task = asyncio.ensure_future(self._ws_price_feed())
            if self.binance_api_key and self.binance_api_secret:
                self.user_stream_task = asyncio.ensure_future(self._user_data_stream())

            # اللوب الرئيسي
            last_scan = 0
            last_rest_price = 0
            last_manage = 0
            last_sync = time.time()
            last_tz_sync = time.time()
            last_listenkey_check = 0
            while not self.shutdown_requested:
                now = time.time()
                # ✅ تحديث الأسعار عبر REST فقط عند انقطاع WebSocket (احتياطي)
                #    أو كل 60 ثانية كمساند خفيف للمزامنة الأولية.
                if not self._ws_connected or now - last_rest_price > 60:
                    if not self._ws_connected:
                        logger.info("📡 WebSocket منقطع — تحديث الأسعار عبر REST")
                    await self._update_prices_rest()
                    last_rest_price = now
                # إدارة الصفقات كل 3 ثوانٍ
                if now - last_manage > 3:
                    await self.manage_positions()
                    last_manage = now
                # المسح كل 60 ثانية
                if now - last_scan > 60:
                    await self.scan_volatile_coins()
                    last_scan = now
                # ✅ مزامنة المراكز مع Binance كل 60 ثانية (نسخة احتياطية —
                #    User Data Stream يكشف التغيرات لحظياً لكن نُبقي المزامنة
                #    الدورية كشبكة أمان).
                if self.binance_api_key and self.binance_api_secret and now - last_sync > 60:
                    await self.sync_positions_with_binance(notify_changes=True)
                    last_sync = now
                # ✅ تجديد listenKey كل 30 دقيقة (User Data Stream)
                if (self.binance_api_key and self.binance_api_secret
                        and now - last_listenkey_check > 30):
                    try:
                        await self._ensure_listenkey()
                    except Exception:
                        pass
                    last_listenkey_check = now
                # إعادة مزامنة وقت الخادم كل 15 دقيقة
                if now - last_tz_sync > 900:
                    await self.sync_server_time()
                    last_tz_sync = now
                await asyncio.sleep(1)

            logger.info("🛑 إيقاف البوت...")
            if self.active_trades:
                logger.info(f"⏳ {len(self.active_trades)} صفقة مفتوحة — تبقى محفوظة في القاعدة")
        finally:
            if self.session and not self.session.closed:
                await self.session.close()

    def start(self):
        asyncio.run(self.main_loop())


if __name__ == "__main__":
    bot = TripleConfluenceBot()
    bot.start()
