"""
POLYMARKET WEATHER — OVERSHOOT STRATEGY BOT  v1
================================================
Estrategia: busca mercados donde el threshold supera al
pronóstico máximo de Meteoblue en 1.5-2°C (o °F equivalente).

LÓGICA CENTRAL:
  1. Descargar pronóstico Meteoblue para cada ciudad.
  2. Tomar el MÁXIMO de todos los modelos disponibles.
  3. Buscar mercados T+1 o T+2 SOLAMENTE donde:
       threshold_del_mercado >= max_modelo + 1.5°C  (o 2.7°F para ciudades US)
       Sin techo: cuanto más lejos, mejor (más separado = más seguro)
  4. En esos mercados, comprar token NO si NO_ask_CLOB <= 0.995
  5. Priorizar por ROE ajustado al tiempo (retorno/día hasta cobro).
  6. En modo SIM: registrar la "compra" y hacer tracking en cada run.
  7. Stop: si el valor del token NO cae a ≤ 0.80 del valor de entrada,
     cerrar la posición (sell sim).
  8. Correr cada 3 horas vía cron / GitHub Actions.

ARCHIVOS DE SALIDA:
  pwbot_overshoot.db           — SQLite con posiciones y ticks
  pwbot_overshoot_positions.csv — tabla viva de posiciones ABIERTAS
  pwbot_overshoot_closed.csv   — historial de posiciones cerradas
  pwbot_overshoot.log          — log completo

MODO SIM: todos los trades son simulados. No se envían órdenes reales.

Dependencias: pip install requests
"""

import sys, os, re, json, time, logging, sqlite3, csv, statistics
from datetime import datetime, timedelta
from typing import Optional, List
import threading

import requests as req

# ── Configuración principal ───────────────────────────────────────────────────
CLOB_HOST   = "https://clob.polymarket.com"
GAMMA_HOST  = "https://gamma-api.polymarket.com"
DB_PATH     = "pwbot_overshoot.db"
POSITIONS_CSV = "pwbot_overshoot_positions.csv"
CLOSED_CSV    = "pwbot_overshoot_closed.csv"
DASHBOARD_OPEN_HTML   = "index.html"
DASHBOARD_CLOSED_HTML = "closed.html"
DASHBOARD_SANDBOX_HTML = "sandbox.html"

# ── Parámetros de estrategia ──────────────────────────────────────────────────
OVERSHOOT_MIN_C = 1.5   # °C mínimo de overshoot para ciudades en Celsius
OVERSHOOT_MIN_F = 2.7   # °F equivalente (1.5°C × 9/5 = 2.7°F)
# Sin techo en ninguna escala: más overshoot = más seguro
NO_MIN_PRICE    = 0.600 # precio mínimo del token NO (ask real CLOB) para entrar
                        # NO_ask < 0.600 → mercado ya descuenta alto riesgo, no entrar
NO_MAX_PRICE    = 0.995 # precio máximo del token NO (ask real CLOB) para entrar
                        # NO_ask <= 0.995 significa que el mercado cotiza el NO casi a par
SIM_STAKE       = 5.0   # dólares simulados por posición
STOP_MULTIPLIER = 0.85  # si no_price baja a 85% del entry → stop loss

# ── Resolución automática al vencer ──────────────────────────────────────────
# False = solo loguear que expiró, no determinar WIN/LOSS todavía.
#         Permite revisar manualmente las apuestas antes de confiar en la fuente.
# True  = consultar IEM ASOS + Open-Meteo y cerrar automáticamente.
RESOLVE_EXPIRED = True

MIN_DAYS = 1  # T+1
MAX_DAYS = 2  # T+2, nunca T+0 ni T+3+

FETCH_TIMEOUT = 12

BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

# ── Ciudades ──────────────────────────────────────────────────────────────────
CITIES = {
    "Buenos Aires": {"slug":"buenos-aires","unit":"C","flag":"AR",
        "lat":-34.6037,"lon":-58.3816,"tz":"America/Argentina/Buenos_Aires",
        "mb_url":"https://www.meteoblue.com/en/weather/forecast/multimodel/buenos-aires_argentina_3435910"},
    "Seoul":        {"slug":"seoul","unit":"C","flag":"KR",
        "lat":37.5665,"lon":126.9780,"tz":"Asia/Seoul",
        "mb_url":"https://www.meteoblue.com/en/weather/forecast/multimodel/seoul_south-korea_1835848"},
    "Ankara":       {"slug":"ankara","unit":"C","flag":"TR",
        "lat":39.9334,"lon":32.8597,"tz":"Europe/Istanbul",
        "mb_url":"https://www.meteoblue.com/en/weather/forecast/multimodel/ankara_turkey_323786"},
    "Miami":        {"slug":"miami","unit":"F","flag":"US",
        "lat":25.7617,"lon":-80.1918,"tz":"America/New_York",
        "mb_url":"https://www.meteoblue.com/en/weather/forecast/multimodel/miami_united-states_4164138"},
    "New York":     {"slug":"nyc","unit":"F","flag":"US",
        "lat":40.7128,"lon":-74.0060,"tz":"America/New_York",
        "mb_url":"https://www.meteoblue.com/en/weather/forecast/multimodel/new-york_united-states_5128581"},
    "Londres":      {"slug":"london","unit":"C","flag":"GB",
        "lat":51.5074,"lon":-0.1278,"tz":"Europe/London",
        "mb_url":"https://www.meteoblue.com/en/weather/forecast/multimodel/london_united-kingdom_2643743"},
    "Dallas":       {"slug":"dallas","unit":"F","flag":"US",
        "lat":32.7767,"lon":-96.7970,"tz":"America/Chicago",
        "mb_url":"https://www.meteoblue.com/en/weather/forecast/multimodel/dallas_united-states_4684888"},
    "Chicago":      {"slug":"chicago","unit":"F","flag":"US",
        "lat":41.8781,"lon":-87.6298,"tz":"America/Chicago",
        "mb_url":"https://www.meteoblue.com/en/weather/forecast/multimodel/chicago_united-states_4887398"},
    "Los Angeles":  {"slug":"los-angeles","unit":"F","flag":"US",
        "lat":34.0522,"lon":-118.2437,"tz":"America/Los_Angeles",
        "mb_url":"https://www.meteoblue.com/en/weather/forecast/multimodel/los-angeles_united-states_5368361"},
    "Seattle":      {"slug":"seattle","unit":"F","flag":"US",
        "lat":47.6062,"lon":-122.3321,"tz":"America/Los_Angeles",
        "mb_url":"https://www.meteoblue.com/en/weather/forecast/multimodel/seattle_united-states_5809844"},
    "Atlanta":      {"slug":"atlanta","unit":"F","flag":"US",
        "lat":33.7490,"lon":-84.3880,"tz":"America/New_York",
        "mb_url":"https://www.meteoblue.com/en/weather/forecast/multimodel/atlanta_united-states_4180439"},
    "Toronto":      {"slug":"toronto","unit":"C","flag":"CA",
        "lat":43.6532,"lon":-79.3832,"tz":"America/Toronto",
        "mb_url":"https://www.meteoblue.com/en/weather/forecast/multimodel/toronto_canada_6167865",
        "sandbox": True},  # perdedor habitual — modo prueba
    "Wellington":   {"slug":"wellington","unit":"C","flag":"NZ",
        "lat":-41.2866,"lon":174.7756,"tz":"Pacific/Auckland",
        "mb_url":"https://www.meteoblue.com/en/weather/forecast/multimodel/wellington_new-zealand_2179537"},
    "Paris":        {"slug":"paris","unit":"C","flag":"FR",
        "lat":48.8566,"lon":2.3522,"tz":"Europe/Paris",
        "mb_url":"https://www.meteoblue.com/en/weather/forecast/multimodel/paris_france_2988507"},
    "Lucknow":      {"slug":"lucknow","unit":"C","flag":"IN",
        "lat":26.8467,"lon":80.9462,"tz":"Asia/Kolkata",
        "mb_url":"https://www.meteoblue.com/en/weather/forecast/multimodel/lucknow_india_1264733"},
    "Sao Paulo":    {"slug":"sao-paulo","unit":"C","flag":"BR",
        "lat":-23.5505,"lon":-46.6333,"tz":"America/Sao_Paulo",
        "mb_url":"https://www.meteoblue.com/en/weather/forecast/multimodel/s%C3%A3o-paulo_brazil_3448439",
        "sandbox": True},  # baja liquidez — modo prueba
    "Munich":       {"slug":"munich","unit":"C","flag":"DE",
        "lat":48.1351,"lon":11.5820,"tz":"Europe/Berlin",
        "mb_url":"https://www.meteoblue.com/en/weather/forecast/multimodel/munich_germany_2867714"},
    "Shanghai":     {"slug":"shanghai","unit":"C","flag":"CN",
        "lat":31.2304,"lon":121.4737,"tz":"Asia/Shanghai",
        "mb_url":"https://www.meteoblue.com/en/weather/forecast/multimodel/shanghai_china_1796236"},
    "Tokyo":        {"slug":"tokyo","unit":"C","flag":"JP",
        "lat":35.6762,"lon":139.6503,"tz":"Asia/Tokyo",
        "mb_url":"https://www.meteoblue.com/en/weather/forecast/multimodel/tokyo_japan_1850147"},
    "Singapore":    {"slug":"singapore","unit":"C","flag":"SG",
        "lat":1.3521,"lon":103.8198,"tz":"Asia/Singapore",
        "mb_url":"https://www.meteoblue.com/en/weather/forecast/multimodel/singapore_singapore_1880252"},
    "Tel Aviv":     {"slug":"tel-aviv","unit":"C","flag":"IL",
        "lat":32.0853,"lon":34.7818,"tz":"Asia/Jerusalem",
        "mb_url":"https://www.meteoblue.com/en/weather/forecast/multimodel/tel-aviv_israel_293397",
        "sandbox": True},  # baja liquidez — modo prueba
}

MODEL_DISPLAY = {
    "HRRR":"HRRR","NAM5":"NAM5","NAM3":"NAM3","AIFS025":"AIFS",
    "GEM15":"GEM15","GEM2":"GEM2","GFS05":"GFS","ICON":"ICON",
    "IFS025":"IFS","IFSHRES":"IFS-HR","MFGLOBAL":"ARPEGE",
    "NAM12":"NAM12","NBM":"NBM","NEMSGLOBAL":"NEMS",
    "NEMSGLOBAL_E":"NEMS2","UMGLOBAL10":"UKMO",
}


# ── Estaciones ICAO por ciudad ────────────────────────────────────────────────
# Polymarket usa la temperatura máxima del día del aeropuerto oficial de cada ciudad.
# Fuente de verificación: Iowa Environmental Mesonet (IEM) ASOS/METAR archive.
#
# Estaciones confirmadas:
#   New York     → KLGA  (LaGuardia — estación oficial de Polymarket para NYC)
#   Miami        → KMIA  (Miami International)
#   Chicago      → KORD  (O'Hare International)
#   Dallas       → KDFW  (Dallas/Fort Worth International)
#   Los Angeles  → KLAX  (Los Angeles International)
#   Seattle      → KSEA  (Seattle-Tacoma International)
#   Atlanta      → KATL  (Hartsfield-Jackson)
#   Buenos Aires → SAEZ  (Ministro Pistarini / Ezeiza)
#   Londres      → EGLC  (London City Airport — oficial Polymarket)
#   Paris        → LFPG  (Charles de Gaulle)
#   Seoul        → RKSS  (Gimpo International — más cercano al centro)
#   Ankara       → LTAC  (Esenboğa International)
#   Toronto      → CYYZ  (Pearson International)
#   Wellington   → NZWN  (Wellington International)
#   Lucknow      → VILK  (Amausi / Chaudhary Charan Singh)
#   Sao Paulo    → SBSP  (Congonhas — estación más usada para Sao Paulo ciudad)
#   Munich       → EDDM  (Munich International)
#   Shanghai     → ZSPD  (Pudong International)
#   Tokyo        → RJTT  (Haneda International)
#   Singapore    → WSSS  (Changi International)
#   Tel Aviv     → LLBG  (Ben Gurion International)

CITY_ICAO = {
    "New York":     "KLGA",   # LaGuardia — oficial Polymarket NYC
    "Miami":        "KMIA",
    "Chicago":      "KORD",
    "Dallas":       "KDFW",
    "Los Angeles":  "KLAX",
    "Seattle":      "KSEA",
    "Atlanta":      "KATL",
    "Buenos Aires": "SAEZ",   # Ezeiza
    "Londres":      "EGLC",   # London City Airport — oficial Polymarket
    "Paris":        "LFPG",   # Charles de Gaulle
    "Seoul":        "RKSS",   # Gimpo
    "Ankara":       "LTAC",   # Esenboğa
    "Toronto":      "CYYZ",   # Pearson
    "Wellington":   "NZWN",
    "Lucknow":      "VILK",
    "Sao Paulo":    "SBSP",   # Congonhas
    "Munich":       "EDDM",
    "Shanghai":     "ZSPD",   # Pudong International
    "Tokyo":        "RJTT",   # Haneda International
    "Singapore":    "WSSS",   # Changi International
    "Tel Aviv":     "LLBG",   # Ben Gurion International
}

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("pwbot_overshoot.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger("overshoot")

# ── Rate limiters ─────────────────────────────────────────────────────────────
class RateLimiter:
    def __init__(self, interval):
        self._interval = interval
        self._last     = 0.0
        self._lock     = threading.Lock()

    def wait(self):
        with self._lock:
            delta = self._interval - (time.time() - self._last)
            if delta > 0:
                time.sleep(delta)
            self._last = time.time()

_rate_gamma = RateLimiter(1.2)
_rate_clob  = RateLimiter(0.5)
_rate_mb    = RateLimiter(3.0)

# ── Base de datos ─────────────────────────────────────────────────────────────
class OvershootDB:
    def __init__(self, path=DB_PATH):
        self.path = path
        self._init()

    def _conn(self):
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init(self):
        with self._conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS positions (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts_entry        TEXT NOT NULL,          -- timestamp de entrada
                    city            TEXT NOT NULL,
                    market_id       TEXT NOT NULL,
                    question        TEXT,
                    date_str        TEXT NOT NULL,          -- fecha del evento
                    days_ahead      INTEGER NOT NULL,       -- T+1 o T+2
                    unit            TEXT NOT NULL,          -- C o F

                    -- Pronóstico Meteoblue al momento de entrada
                    mb_max_model    REAL NOT NULL,          -- máximo entre todos los modelos
                    mb_consensus    REAL NOT NULL,          -- media de modelos
                    mb_n_models     INTEGER NOT NULL,
                    mb_models_json  TEXT,                   -- JSON con cada modelo y su valor

                    -- Datos del mercado
                    market_threshold REAL NOT NULL,         -- temperatura del threshold del mercado
                    overshoot        REAL NOT NULL,         -- threshold - mb_max_model
                    market_type      TEXT NOT NULL,         -- "above_equal" o "exact"

                    -- Precios al entrar (token NO)
                    no_ask_entry    REAL NOT NULL,          -- precio ask del token NO al entrar
                    no_bid_entry    REAL,                   -- precio bid del token NO al entrar
                    yes_bid_entry   REAL,                   -- precio bid del token YES al entrar
                    token_yes_id    TEXT,
                    token_no_id     TEXT,

                    -- Métricas de calidad de entrada
                    roe_per_day     REAL,                   -- ROE diario estimado (%)
                    risk_distance   REAL,                   -- threshold - mb_max (cuanto le falta)
                    score           REAL,                   -- score combinado para priorización

                    -- Simulación
                    sim_stake       REAL DEFAULT 5.0,       -- dólares invertidos (sim)
                    sim_tokens      REAL,                   -- tokens NO comprados (sim)
                    stop_price      REAL,                   -- precio de stop loss (0.80 * entry)

                    -- Tracking
                    no_price_last   REAL,                   -- último precio NO observado
                    no_price_max    REAL,                   -- máximo histórico de NO price
                    no_price_min    REAL,                   -- mínimo histórico de NO price
                    ticks_tracked   INTEGER DEFAULT 0,
                    last_update     TEXT,

                    -- Cierre
                    outcome         TEXT DEFAULT 'OPEN',    -- OPEN / WIN / LOSS / EXPIRED
                    close_reason    TEXT,                   -- STOP / EXPIRED / MARKET_RESOLVED
                    ts_close        TEXT,
                    no_price_close  REAL,
                    sim_pnl         REAL,                   -- P&L simulado en dólares
                    sim_pnl_pct     REAL,                   -- P&L en %
                    is_sandbox      INTEGER DEFAULT 0        -- 1 = ciudad en modo prueba (baja liquidez)
                )""")

            conn.execute("""
                CREATE TABLE IF NOT EXISTS price_ticks (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    pos_id      INTEGER NOT NULL,
                    ts          TEXT NOT NULL,
                    no_bid      REAL,
                    no_ask      REAL,
                    yes_bid     REAL,
                    elapsed_h   REAL                        -- horas desde entrada
                )""")

            conn.execute("""
                CREATE TABLE IF NOT EXISTS scan_log (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts          TEXT NOT NULL,
                    cities_ok   INTEGER,
                    candidates  INTEGER,
                    entered     INTEGER,
                    tracked     INTEGER,
                    stopped     INTEGER,
                    notes       TEXT
                )""")

            conn.execute("CREATE INDEX IF NOT EXISTS idx_pos_outcome ON positions(outcome)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tick_pos ON price_ticks(pos_id)")

            # Migración: agregar is_sandbox si la DB fue creada antes de esta versión
            cols_existing = [r[1] for r in conn.execute("PRAGMA table_info(positions)").fetchall()]
            if "is_sandbox" not in cols_existing:
                conn.execute("ALTER TABLE positions ADD COLUMN is_sandbox INTEGER DEFAULT 0")
                log.info("  🔧 Migración DB: columna is_sandbox agregada")

    # ── CRUD ──────────────────────────────────────────────────────────────────
    def save_position(self, p: dict) -> int:
        with self._conn() as conn:
            cols = ", ".join(p.keys())
            ph   = ", ".join(["?"] * len(p))
            cur  = conn.execute(
                f"INSERT INTO positions ({cols}) VALUES ({ph})", list(p.values())
            )
            return cur.lastrowid

    def already_open(self, market_id: str) -> bool:
        """¿Ya tenemos una posición abierta en este mercado?"""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT id FROM positions WHERE market_id=? AND outcome='OPEN'",
                (market_id,)
            ).fetchone()
            return row is not None

    def get_open_positions(self) -> List[dict]:
        with self._conn() as conn:
            cur = conn.execute(
                "SELECT * FROM positions WHERE outcome='OPEN' ORDER BY ts_entry DESC"
            )
            return [dict(r) for r in cur.fetchall()]

    def save_tick(self, pos_id: int, no_bid, no_ask, yes_bid, elapsed_h):
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO price_ticks (pos_id,ts,no_bid,no_ask,yes_bid,elapsed_h) "
                "VALUES (?,?,?,?,?,?)",
                (pos_id, datetime.now().isoformat(), no_bid, no_ask, yes_bid, round(elapsed_h, 2))
            )

    def update_tracking(self, pos_id: int, no_price, no_price_max, no_price_min, ticks):
        with self._conn() as conn:
            conn.execute("""
                UPDATE positions
                SET no_price_last=?, no_price_max=?, no_price_min=?,
                    ticks_tracked=?, last_update=?
                WHERE id=?""",
                (no_price, no_price_max, no_price_min, ticks,
                 datetime.now().isoformat(), pos_id)
            )

    def close_position(self, pos_id: int, outcome: str, reason: str,
                        no_price_close: float, sim_pnl: float, sim_pnl_pct: float):
        with self._conn() as conn:
            conn.execute("""
                UPDATE positions
                SET outcome=?, close_reason=?, ts_close=?,
                    no_price_close=?, sim_pnl=?, sim_pnl_pct=?
                WHERE id=?""",
                (outcome, reason, datetime.now().isoformat(),
                 no_price_close, round(sim_pnl, 4), round(sim_pnl_pct, 2), pos_id)
            )

    def log_scan(self, cities_ok, candidates, entered, tracked, stopped, notes=""):
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO scan_log (ts,cities_ok,candidates,entered,tracked,stopped,notes) "
                "VALUES (?,?,?,?,?,?,?)",
                (datetime.now().isoformat(), cities_ok, candidates, entered, tracked, stopped, notes)
            )

# ══════════════════════════════════════════════════════════════════════════════
#  FUENTES DE DATOS
# ══════════════════════════════════════════════════════════════════════════════
#  PRONÓSTICO:  Meteoblue MultiModel — hasta 17 modelos, se toma el MÁXIMO.
#  TEMP REAL:   IEM ASOS (mesonet.agron.iastate.edu) — archivo METAR histórico.
#               METAR siempre reporta en °F. Si la ciudad usa °C, se convierte.
#               Polymarket usa el máximo METAR del día en hora local → mismo
#               criterio: se pide con tz=local a IEM.
#               Fallback: Open-Meteo si IEM no tiene datos (siempre devuelve °C).
# ══════════════════════════════════════════════════════════════════════════════

def fetch_actual_temp(city: str, date_str: str) -> Optional[float]:
    """
    Temperatura máxima real del día para resolver WIN/LOSS.

    Fuente primaria: IEM ASOS — pedimos tmpc (°C) siempre.
      IEM ofrece tanto tmpf como tmpc. Pidiendo tmpc evitamos cualquier
      problema de conversión F↔C — el dato llega directamente en °C
      para todas las estaciones (EEUU e internacionales).
      Se usa hora local de la ciudad (mismo criterio que Polymarket).

    Fallback: Open-Meteo — también devuelve °C siempre.

    En ambos casos: si cfg["unit"] == "F" se convierte el resultado °C → °F
    antes de devolver, para que sea consistente con los mercados US.

    Solo funciona para fechas pasadas (date_str < hoy).
    """
    cfg = CITIES.get(city)
    if not cfg:
        return None
    target = datetime.strptime(date_str, "%Y-%m-%d").date()
    if target >= datetime.now().date():
        return None

    unit = cfg["unit"]   # "C" o "F" — unidad del mercado para esta ciudad
    icao = CITY_ICAO.get(city)
    tz   = cfg["tz"]

    # ── Fuente 1: IEM ASOS — tmpc (°C) ───────────────────────────────────────
    if icao:
        try:
            url = (
                f"https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py"
                f"?station={icao}&data=tmpc"   # siempre °C, sin ambigüedad
                f"&year1={target.year}&month1={target.month:02d}&day1={target.day:02d}"
                f"&year2={target.year}&month2={target.month:02d}&day2={target.day:02d}"
                f"&tz={tz.replace('/', '%2F')}"   # hora local = mismo criterio Polymarket
                f"&format=onlycomma&latlon=no&missing=M&trace=T&direct=no&report_type=3"
            )
            r = req.get(url, timeout=20)
            if r.status_code == 200:
                temps_c = []
                for line in r.text.strip().splitlines():
                    if line.startswith("#") or line.startswith("station"):
                        continue
                    parts = line.split(",")
                    if len(parts) < 3:
                        continue
                    raw = parts[2].strip()
                    if raw in ("M", "T", "", "tmpc"):
                        continue
                    try:
                        temps_c.append(float(raw))
                    except ValueError:
                        continue
                if temps_c:
                    max_c  = max(temps_c)
                    result = round(max_c * 9 / 5 + 32, 1) if unit == "F" else round(max_c, 1)
                    log.info(f"  🛬 IEM {icao} {date_str}: max={max_c:.1f}°C → {result}°{unit} ({len(temps_c)} obs)")
                    return result
                log.debug(f"  IEM {icao} {date_str}: sin lecturas tmpc válidas")
        except Exception as e:
            log.debug(f"  IEM {icao} {date_str}: {e}")
        log.info(f"  ⚠️  IEM {icao} sin datos → fallback Open-Meteo")

    # ── Fuente 2: Open-Meteo (fallback) — también °C ──────────────────────────
    try:
        days_back = (datetime.now().date() - target).days
        if days_back <= 92:
            url = (
                f"https://api.open-meteo.com/v1/forecast"
                f"?latitude={cfg['lat']}&longitude={cfg['lon']}"
                f"&daily=temperature_2m_max"
                f"&past_days={min(days_back + 1, 92)}"
                f"&forecast_days=1"
                f"&timezone={tz.replace('/', '%2F')}"
            )
            r = req.get(url, timeout=15)
            if r.status_code == 200:
                data  = r.json()
                times = data.get("daily", {}).get("time", [])
                temps = data.get("daily", {}).get("temperature_2m_max", [])
                for t, temp_c in zip(times, temps):
                    if t == date_str and temp_c is not None:
                        result = round(temp_c * 9 / 5 + 32, 1) if unit == "F" else round(temp_c, 1)
                        log.info(f"  🌐 Open-Meteo {city} {date_str}: {temp_c:.1f}°C → {result}°{unit}")
                        return result
    except Exception as e:
        log.debug(f"  Open-Meteo {city} {date_str}: {e}")

    log.warning(f"  ❌ Sin temperatura real para {city} {date_str}")
    return None


def fetch_meteoblue(city: str, target_date) -> Optional[dict]:
    """
    Descarga el multimodel de Meteoblue para city/fecha.
    Retorna: {max_model, consensus, std_dev, n_models, models{}, unit}
    """
    cfg    = CITIES[city]
    mb_url = cfg.get("mb_url")
    unit   = cfg["unit"]
    if not mb_url:
        return None

    _rate_mb.wait()
    session = req.Session()
    session.headers.update({"User-Agent": BROWSER_UA, "Accept-Language": "en-US,en;q=0.9"})

    try:
        r = session.get(mb_url, timeout=FETCH_TIMEOUT)
        r.raise_for_status()
    except Exception as e:
        log.warning(f"MB page {city}: {e}")
        return None

    pattern = r"my\.meteoblue\.com/images/meteogram_multimodel\?([^\"'<>\s]+format=highcharts[^\"'<>\s]+)"
    matches = re.findall(pattern, r.text)
    if not matches:
        log.warning(f"MB {city}: no se encontró URL del API en la página")
        return None

    raw_qs = None
    for m in matches:
        if "dpi=" not in m:
            raw_qs = m.replace("&amp;", "&")
            break
    if raw_qs is None:
        raw_qs = matches[0].replace("&amp;", "&")

    api_url = f"https://my.meteoblue.com/images/meteogram_multimodel?{raw_qs}"
    _rate_mb.wait()
    try:
        r2 = session.get(api_url, headers={"Referer": mb_url}, timeout=FETCH_TIMEOUT)
        r2.raise_for_status()
        data = r2.json()
    except Exception as e:
        log.warning(f"MB API {city}: {e}")
        return None

    if data.get("error"):
        return None

    mu       = re.search(r"temperature_units=([^&]+)", raw_qs)
    api_unit = mu.group(1).upper() if mu else "F"

    def to_unit(v):
        if api_unit == "F" and unit == "C": return (v - 32) * 5 / 9
        if api_unit == "C" and unit == "F": return v * 9 / 5 + 32
        return v

    day_prefix = target_date.strftime("%Y-%m-%d")
    model_max  = {}
    seen       = set()

    for s in data.get("series", []):
        sname = s.get("name")
        if sname not in MODEL_DISPLAY or sname in seen:
            continue
        pts = s.get("data", [])
        if len(pts) < 48:
            continue
        display  = MODEL_DISPLAY[sname]
        day_vals = [to_unit(p["y"]) for p in pts
                    if p.get("y") is not None and str(p.get("name", "")).startswith(day_prefix)]
        if day_vals:
            model_max[display] = round(max(day_vals), 2)
            seen.add(sname)

    if not model_max:
        return None

    vals      = list(model_max.values())
    max_model = round(max(vals), 2)
    consensus = round(statistics.mean(vals), 2)
    std_dev   = round(statistics.pstdev(vals) if len(vals) > 1 else 0.0, 2)

    return {
        "max_model": max_model,
        "consensus": consensus,
        "std_dev":   std_dev,
        "n_models":  len(model_max),
        "models":    model_max,
        "unit":      unit,
    }

# ── Helpers Polymarket ────────────────────────────────────────────────────────
def parse_market_threshold(question: str) -> Optional[dict]:
    """
    Extrae el threshold y tipo del mercado.
    Solo nos interesan mercados "above_equal" (or higher) y "exact" (be X° on date).
    Para la estrategia overshoot necesitamos el LÍMITE INFERIOR del threshold.
    
    Retorna: {threshold, type, unit}
    """
    q    = question.lower()
    unit = "F" if "°f" in q else "C"

    # above_equal: "be 30°C or higher" → threshold = 30
    m = re.search(r'be\s+(-?\d+\.?\d*)\s*(?:°[cf])?\s*or\s+(?:higher|above)', q)
    if m:
        return {"threshold": float(m.group(1)), "type": "above_equal", "unit": unit}

    m = re.search(r'(?:above|over)\s+(-?\d+\.?\d*)', q)
    if m:
        return {"threshold": float(m.group(1)), "type": "above_equal", "unit": unit}

    # exact: "be 30°C on March 7"
    m = re.search(r'be\s+(-?\d+\.?\d*)\s*(?:°[cf])?\s+on\b', q)
    if m:
        return {"threshold": float(m.group(1)), "type": "exact", "unit": unit}

    m = re.search(r'be\s+(-?\d+\.?\d*)\s*°[cf]', q)
    if m:
        return {"threshold": float(m.group(1)), "type": "exact", "unit": unit}

    return None

def get_clob_prices(token_yes: str, token_no: str) -> dict:
    """Retorna {yes_bid, yes_ask, no_bid, no_ask} o {}."""
    result = {}
    for token_id, prefix in [(token_yes, "yes"), (token_no, "no")]:
        try:
            _rate_clob.wait()
            r = req.get(
                f"{CLOB_HOST}/book?token_id={token_id}&_t={int(time.time())}",
                timeout=FETCH_TIMEOUT
            )
            if r.status_code != 200:
                continue
            book = r.json()
            bids = book.get("bids", [])
            asks = book.get("asks", [])
            if bids:
                result[f"{prefix}_bid"] = float(max(bids, key=lambda x: float(x["price"]))["price"])
            if asks:
                result[f"{prefix}_ask"] = float(min(asks, key=lambda x: float(x["price"]))["price"])
        except Exception as e:
            log.debug(f"clob {prefix} {token_id[:12]}: {e}")
    return result

def fetch_markets_for_city(city: str) -> List[dict]:
    """
    Retorna mercados T+1 y T+2 para una ciudad.
    Solo los que tengan token YES y NO.
    """
    cfg     = CITIES[city]
    slug    = cfg["slug"]
    results = []
    today   = datetime.now()
    session = req.Session()
    session.headers.update({"User-Agent": BROWSER_UA})

    for d in range(MIN_DAYS, MAX_DAYS + 1):   # solo 1 y 2
        dt    = today + timedelta(days=d)
        month = dt.strftime("%B").lower()
        day   = dt.day
        year  = dt.year
        base  = f"highest-temperature-in-{slug}-on-{month}-{day}"
        slugs = [
            f"{base}-{year}",
            base,
            f"highest-temperature-in-{slug}-on-{month}-{day:02d}-{year}",
            f"highest-temperature-in-{slug}-on-{month}-{day:02d}",
        ]
        for event_slug in slugs:
            try:
                _rate_gamma.wait()
                r = session.get(
                    f"{GAMMA_HOST}/events/slug/{event_slug}",
                    timeout=FETCH_TIMEOUT
                )
                if r.status_code != 200:
                    continue
                event_data = r.json()
                markets = event_data.get("markets", [])
                if not markets:
                    continue

                for mkt in markets:
                    if mkt.get("closed") and not mkt.get("active"):
                        continue
                    clob_raw = mkt.get("clobTokenIds")
                    if not clob_raw:
                        continue
                    ids = json.loads(clob_raw) if isinstance(clob_raw, str) else clob_raw
                    if len(ids) < 2:
                        continue
                    q = mkt.get("question", "")
                    minfo = parse_market_threshold(q)
                    if not minfo:
                        continue
                    results.append({
                        "city":       city,
                        "market_id":  mkt.get("conditionId") or mkt.get("id", ""),
                        "question":   q,
                        "threshold":  minfo["threshold"],
                        "mtype":      minfo["type"],
                        "unit":       minfo["unit"],
                        "date_str":   dt.strftime("%Y-%m-%d"),
                        "days_ahead": d,
                        "token_yes":  ids[0],
                        "token_no":   ids[1],
                    })
                break  # slug encontrado, no seguir
            except Exception as e:
                log.debug(f"fetch_markets {city} d+{d} {event_slug}: {e}")

    return results

# ── ROE y scoring ─────────────────────────────────────────────────────────────
def calc_roe_per_day(no_ask: float, days_ahead: int) -> float:
    """
    ROE diario estimado si el token NO llega a 1.0 al vencimiento.
    Fórmula: (1.0 - no_ask) / no_ask / days_ahead * 100
    """
    if no_ask <= 0 or days_ahead <= 0:
        return 0.0
    roe_total = (1.0 - no_ask) / no_ask * 100
    return round(roe_total / days_ahead, 2)

def calc_score(no_ask: float, overshoot: float, overshoot_min: float,
               days_ahead: int, mb_std_dev: float) -> float:
    """
    Score compuesto para priorizar entre candidatos.

    PRIORIDAD: SEGURIDAD sobre retorno.
    Cuanto mas lejos este el threshold del maximo modelo, mas improbable
    que se alcance.

      1. overshoot normalizado — componente dominante. Se normaliza sobre
                                 el minimo de la unidad de la ciudad para
                                 que 1 grado extra en C y en F sean comparables.
      2. ROE/dia              — tiebreaker.
      3. std_dev              — penalizacion por dispersion entre modelos.
    """
    # Overshoot normalizado: cuantos "minimos" extra tiene por encima del umbral
    # Ej: overshoot=4.18°C, min=1.5°C → exceso=2.68 → 2.68/1.5=1.79 unidades normalizadas
    # Ej: overshoot=8.83°F, min=2.7°F → exceso=6.13 → 6.13/2.7=2.27 unidades normalizadas
    # Ambos comparables entre ciudades C y F
    exceso_normalizado = max(0, overshoot - overshoot_min) / overshoot_min
    overshoot_bonus = exceso_normalizado * 20

    # ROE: tiebreaker
    roe = calc_roe_per_day(no_ask, days_ahead)

    # Penalizar dispersion alta entre modelos
    std_penalty = max(0, mb_std_dev - 1.0) * 5.0

    return round(overshoot_bonus + roe - std_penalty, 3)

# ── Exportar CSV de posiciones ────────────────────────────────────────────────
def export_open_positions_csv(db: OvershootDB):
    """Exporta la tabla de posiciones ABIERTAS con todos los datos relevantes."""
    positions = db.get_open_positions()
    if not positions:
        # archivo vacío con headers
        headers = [
            "id","ts_entry","city","question","date_str","days_ahead","unit",
            "mb_max_model","mb_consensus","mb_n_models",
            "market_threshold","overshoot","market_type",
            "no_ask_entry","stop_price","roe_per_day","score",
            "sim_stake","sim_tokens","sim_value_now","sim_pnl_now","sim_pnl_pct_now",
            "no_price_last","no_price_max","no_price_min",
            "ticks_tracked","last_update","status"
        ]
        with open(POSITIONS_CSV, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(headers)
        return

    rows = []
    for p in positions:
        no_now    = p.get("no_price_last") or p.get("no_ask_entry", 0)
        tokens    = p.get("sim_tokens") or 0
        val_now   = round(no_now * tokens, 4) if tokens else 0
        pnl_now   = round(val_now - p.get("sim_stake", SIM_STAKE), 4)
        pnl_pct   = round(pnl_now / p.get("sim_stake", SIM_STAKE) * 100, 2) if p.get("sim_stake") else 0
        rows.append([
            p["id"],
            p["ts_entry"],
            p["city"],
            p["question"],
            p["date_str"],
            p["days_ahead"],
            p["unit"],
            p["mb_max_model"],
            p["mb_consensus"],
            p["mb_n_models"],
            p["market_threshold"],
            p["overshoot"],
            p["market_type"],
            p["no_ask_entry"],
            p.get("stop_price", ""),
            p.get("roe_per_day", ""),
            p.get("score", ""),
            p.get("sim_stake", SIM_STAKE),
            round(tokens, 4),
            val_now,
            pnl_now,
            f"{pnl_pct}%",
            no_now,
            p.get("no_price_max", ""),
            p.get("no_price_min", ""),
            p.get("ticks_tracked", 0),
            p.get("last_update", ""),
            "🟢 OPEN",
        ])

    headers = [
        "id","ts_entry","city","question","date_str","days_ahead","unit",
        "mb_max_model","mb_consensus","mb_n_models",
        "market_threshold","overshoot","market_type",
        "no_ask_entry","stop_price","roe_per_day","score",
        "sim_stake","sim_tokens","sim_value_now","sim_pnl_now","sim_pnl_pct_now",
        "no_price_last","no_price_max","no_price_min",
        "ticks_tracked","last_update","status"
    ]
    with open(POSITIONS_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(headers)
        w.writerows(rows)

    log.info(f"  📊 Positions CSV actualizado: {len(rows)} posiciones abiertas → {POSITIONS_CSV}")

def export_closed_csv(db: OvershootDB):
    """Exporta posiciones cerradas a CSV histórico."""
    with db._conn() as conn:
        cur = conn.execute(
            "SELECT * FROM positions WHERE outcome != 'OPEN' ORDER BY ts_close DESC"
        )
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    if not rows:
        return
    with open(CLOSED_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        w.writerows([list(r) for r in rows])
    log.info(f"  📁 Closed CSV actualizado: {len(rows)} posiciones cerradas → {CLOSED_CSV}")

# ── Dashboard HTML ────────────────────────────────────────────────────────────

_HTML_HEAD = """<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&family=Syne:wght@400;700;800&display=swap" rel="stylesheet">
<style>
:root{{--bg:#0a0c0f;--surface:#111318;--border:#1e2229;--accent:#00e5a0;--loss:#ff4d6d;--gain:#00e5a0;--neutral:#6b7280;--text:#e2e8f0;--muted:#4b5563;--warn:#f5a623}}
*{{box-sizing:border-box;margin:0;padding:0}}
body{{background:var(--bg);color:var(--text);font-family:'JetBrains Mono',monospace;min-height:100vh;padding:28px 20px}}
a{{color:var(--accent);text-decoration:none}}a:hover{{text-decoration:underline}}
header{{margin-bottom:28px;border-left:3px solid var(--accent);padding-left:14px}}
header h1{{font-family:'Syne',sans-serif;font-size:1.4rem;font-weight:800;color:#fff}}
header p{{font-size:.7rem;color:var(--neutral);margin-top:4px}}
.nav{{display:flex;gap:12px;margin-bottom:20px;font-size:.75rem}}
.nav a{{padding:5px 14px;border:1px solid var(--border);border-radius:4px;color:var(--muted)}}
.nav a.active{{border-color:var(--accent);color:var(--accent);background:#0d1f19}}
.nav a.active-sandbox{{border-color:var(--warn);color:var(--warn);background:#2a1f0a}}
.summary{{display:flex;gap:12px;flex-wrap:wrap;margin-bottom:24px}}
.stat{{background:var(--surface);border:1px solid var(--border);padding:10px 18px;border-radius:6px;min-width:110px}}
.stat-label{{font-size:.62rem;color:var(--muted);text-transform:uppercase;letter-spacing:.08em}}
.stat-val{{font-size:1.15rem;font-weight:700;margin-top:3px}}
.day-group{{margin-bottom:26px}}
.day-header{{display:flex;align-items:center;gap:10px;margin-bottom:8px}}
.day-label{{font-family:'Syne',sans-serif;font-size:.82rem;font-weight:700;color:var(--accent);text-transform:uppercase;letter-spacing:.05em}}
.day-count{{font-size:.68rem;background:#1a2a22;color:var(--accent);border:1px solid #1f3d2e;padding:2px 8px;border-radius:20px}}
.day-line{{flex:1;height:1px;background:var(--border)}}
table{{width:100%;border-collapse:collapse;font-size:.74rem}}
thead tr{{background:#0d1117}}
th{{padding:7px 10px;text-align:left;color:var(--muted);font-size:.62rem;text-transform:uppercase;letter-spacing:.06em;font-weight:600;border-bottom:1px solid var(--border)}}
th.r,td.r{{text-align:right}}
tbody tr{{border-bottom:1px solid #13171e;transition:background .1s}}
tbody tr:hover{{background:#13181f}}
td{{padding:8px 10px}}
.city{{font-weight:600;color:#fff}}
.hor{{font-size:.62rem;padding:2px 5px;border-radius:3px;background:#1a1d24;color:var(--muted)}}
.pill{{display:inline-block;padding:2px 8px;border-radius:4px;font-weight:700;font-size:.7rem;min-width:58px;text-align:center}}
.pill-pos{{background:#0d2b1e;color:var(--gain);border:1px solid #1a4030}}
.pill-neg{{background:#2b0d14;color:var(--loss);border:1px solid #4a1525}}
.pill-flat{{background:#1a1d24;color:var(--muted);border:1px solid var(--border)}}
.pill-win{{background:#0d2b1e;color:var(--gain);border:1px solid #1a4030}}
.pill-loss{{background:#2b0d14;color:var(--loss);border:1px solid #4a1525}}
.pill-exp{{background:#2a1f0a;color:var(--warn);border:1px solid #4a3010}}
.bar-wrap{{display:flex;align-items:center;gap:6px}}
.bar-track{{width:60px;height:4px;background:var(--border);border-radius:2px;position:relative}}
.bar-fill{{height:100%;border-radius:2px;position:absolute;top:0;left:0}}
.bar-pos{{background:var(--gain)}}.bar-neg{{background:var(--loss)}}
.stop{{color:var(--loss);font-size:.7rem}}.muted{{color:var(--muted)}}
footer{{margin-top:28px;font-size:.63rem;color:var(--muted);border-top:1px solid var(--border);padding-top:10px}}
</style></head><body>
"""

_HTML_NAV = """<div class="nav">
  <a href="index.html" class="{cls_open}">📂 Abiertas ({n_open})</a>
  <a href="closed.html" class="{cls_closed}">✅ Cerradas ({n_closed})</a>
  <a href="sandbox.html" class="{cls_sandbox}" style="border-color:#f5a623;color:#f5a623;opacity:.8">🧪 Sandbox ({n_sandbox})</a>
</div>
"""

_HTML_FOOT = """<footer>
  Overshoot Bot v1 &middot; Modo SIM &middot; stake ${stake}/pos &middot; stop loss 20% &middot;
  Actualizado: {ts}
</footer></body></html>"""


def _short_market(p: dict) -> str:
    """
    Construye una etiqueta corta del mercado a partir del threshold y la unidad.
    Ejemplos: 'highest 25c', 'highest 56f', 'highest -3c'
    Usa market_threshold + unit almacenados en la posición.
    """
    threshold = p.get("market_threshold")
    unit      = (p.get("unit") or "").lower()
    mtype     = (p.get("market_type") or "highest").lower()
    if threshold is None:
        return "—"
    # Formatear: entero si no tiene decimales, si no 1 decimal
    try:
        t = float(threshold)
        t_s = str(int(t)) if t == int(t) else f"{t:.1f}"
    except Exception:
        t_s = str(threshold)
    return f"{mtype} {t_s}{unit}"

def _pct_pill(delta: float) -> str:
    sign = "+" if delta > 0.05 else ""
    cls  = "pill-pos" if delta > 0.05 else ("pill-neg" if delta < -0.05 else "pill-flat")
    return f'<span class="pill {cls}">{sign}{delta:.1f}%</span>'

def _bar(delta: float) -> str:
    pct = min(abs(delta) / 15 * 100, 100)
    cls = "bar-pos" if delta >= 0 else "bar-neg"
    return (f'<div class="bar-wrap"><div class="bar-track">'
            f'<div class="bar-fill {cls}" style="width:{pct:.0f}%"></div>'
            f'</div></div>')

def _outcome_pill(outcome: str) -> str:
    m = {"WIN": ("pill-win","✅ WIN"), "LOSS": ("pill-loss","❌ LOSS"),
         "EXPIRED": ("pill-exp","⏱ EXPIRED")}
    cls, label = m.get(outcome, ("pill-flat", outcome))
    return f'<span class="pill {cls}">{label}</span>'

def _group_by_date(rows: list, date_key: str, sort_key, reverse_sort=True) -> dict:
    groups: dict = {}
    for r in rows:
        groups.setdefault(r.get(date_key, "?"), []).append(r)
    for d in groups:
        groups[d].sort(key=sort_key, reverse=reverse_sort)
    return groups


def _build_open_page(open_positions: list, n_closed: int, n_sandbox: int, ts: str) -> str:
    for p in open_positions:
        entry = p.get("no_ask_entry") or 0
        now   = p.get("no_price_last") or entry
        p["_delta"] = ((now - entry) / entry * 100) if entry else 0.0

    groups       = _group_by_date(open_positions, "date_str", lambda r: r["_delta"])
    dates_sorted = sorted(groups.keys(), reverse=True)
    gains  = [p for p in open_positions if p["_delta"] > 0.05]
    losses = [p for p in open_positions if p["_delta"] < -0.05]
    best   = f"+{max(p['_delta'] for p in gains):.1f}%"   if gains  else "—"
    worst  = f"{min(p['_delta'] for p in losses):.1f}%"   if losses else "—"

    html  = _HTML_HEAD.format(title="Overshoot Bot — Posiciones Abiertas")
    html += '<header><h1>⚡ Overshoot Bot</h1>'
    html += '<p>Posiciones abiertas · Modo SIM · token NO · estrategia overshoot ≥1.5°C</p></header>'
    html += _HTML_NAV.format(cls_open="active", cls_closed="", cls_sandbox="",
                              n_open=len(open_positions), n_closed=n_closed, n_sandbox=n_sandbox)
    html += '<div class="summary">'
    html += f'<div class="stat"><div class="stat-label">Abiertas</div><div class="stat-val" style="color:#fff">{len(open_positions)}</div></div>'
    html += f'<div class="stat"><div class="stat-label">En ganancia</div><div class="stat-val" style="color:var(--gain)">{len(gains)}</div></div>'
    html += f'<div class="stat"><div class="stat-label">En pérdida</div><div class="stat-val" style="color:var(--loss)">{len(losses)}</div></div>'
    html += f'<div class="stat"><div class="stat-label">Mejor pos</div><div class="stat-val" style="color:var(--gain)">{best}</div></div>'
    html += f'<div class="stat"><div class="stat-label">Peor pos</div><div class="stat-val" style="color:var(--loss)">{worst}</div></div>'
    html += '</div>'

    for date in dates_sorted:
        rows = groups[date]
        html += f'<div class="day-group">'
        html += (f'<div class="day-header"><span class="day-label">{date}</span>'
                 f'<span class="day-count">{len(rows)} pos</span>'
                 f'<div class="day-line"></div></div>')
        html += ('<table><thead><tr>'
                 '<th>Ciudad</th><th>Mercado</th><th>Hor.</th>'
                 '<th class="r">Entrada</th><th class="r">Actual</th>'
                 '<th class="r">Stop</th><th class="r">Var %</th><th>Movim.</th>'
                 '<th class="r">Overshoot</th><th class="r">ROE/d</th><th class="r">Horas</th>'
                 '</tr></thead><tbody>')
        for p in rows:
            entry   = p.get("no_ask_entry", 0)
            now     = p.get("no_price_last") or entry
            stop    = p.get("stop_price", 0)
            delta   = p["_delta"]
            elapsed = ""
            if p.get("ts_entry"):
                try:
                    elapsed_h = (datetime.now() - datetime.fromisoformat(p["ts_entry"])).total_seconds() / 3600
                    elapsed = f"{elapsed_h:.1f}h"
                except Exception:
                    pass
            roe    = p.get("roe_per_day", "")
            roe_s  = f"{roe:.1f}%" if isinstance(roe, (int, float)) else "—"
            over   = p.get("overshoot", "")
            over_s = f"{over:.2f}°{p.get('unit','')}" if isinstance(over, (int, float)) else "—"
            mkt_s  = _short_market(p)
            html += (f'<tr>'
                     f'<td class="city">{p["city"]}</td>'
                     f'<td class="muted" style="font-size:.68rem;white-space:nowrap">{mkt_s}</td>'
                     f'<td><span class="hor">T+{p.get("days_ahead","?")}</span></td>'
                     f'<td class="r">{entry:.4f}</td>'
                     f'<td class="r">{now:.4f}</td>'
                     f'<td class="r stop">{stop:.4f}</td>'
                     f'<td class="r">{_pct_pill(delta)}</td>'
                     f'<td>{_bar(delta)}</td>'
                     f'<td class="r muted">{over_s}</td>'
                     f'<td class="r muted">{roe_s}</td>'
                     f'<td class="r muted">{elapsed}</td>'
                     f'</tr>')
        html += '</tbody></table></div>'

    html += _HTML_FOOT.format(stake=SIM_STAKE, ts=ts)
    return html


def _build_closed_page(closed_positions: list, n_open: int, n_sandbox: int, ts: str) -> str:
    for p in closed_positions:
        p["_close_date"] = (p.get("ts_close") or "")[:10] or "?"

    groups       = _group_by_date(closed_positions, "_close_date",
                                   lambda r: r.get("sim_pnl_pct") or 0)
    dates_sorted = sorted(groups.keys(), reverse=True)
    wins      = [p for p in closed_positions if p.get("outcome") == "WIN"]
    losses    = [p for p in closed_positions if p.get("outcome") == "LOSS"]
    exps      = [p for p in closed_positions if p.get("outcome") == "EXPIRED"]
    total_pnl = sum(p.get("sim_pnl") or 0 for p in closed_positions)
    wr        = len(wins) / (len(wins) + len(losses)) * 100 if (wins or losses) else 0
    pnl_col   = "var(--gain)" if total_pnl >= 0 else "var(--loss)"

    html  = _HTML_HEAD.format(title="Overshoot Bot — Posiciones Cerradas")
    html += '<header><h1>⚡ Overshoot Bot</h1>'
    html += '<p>Historial de posiciones cerradas · Modo SIM</p></header>'
    html += _HTML_NAV.format(cls_open="", cls_closed="active", cls_sandbox="",
                              n_open=n_open, n_closed=len(closed_positions), n_sandbox=n_sandbox)
    html += '<div class="summary">'
    html += f'<div class="stat"><div class="stat-label">Cerradas</div><div class="stat-val" style="color:#fff">{len(closed_positions)}</div></div>'
    html += f'<div class="stat"><div class="stat-label">WIN</div><div class="stat-val" style="color:var(--gain)">{len(wins)}</div></div>'
    html += f'<div class="stat"><div class="stat-label">LOSS</div><div class="stat-val" style="color:var(--loss)">{len(losses)}</div></div>'
    html += f'<div class="stat"><div class="stat-label">Win rate</div><div class="stat-val" style="color:#fff">{wr:.0f}%</div></div>'
    html += f'<div class="stat"><div class="stat-label">P&amp;L total</div><div class="stat-val" style="color:{pnl_col}">${total_pnl:+.2f}</div></div>'
    if exps:
        html += f'<div class="stat"><div class="stat-label">Expired</div><div class="stat-val" style="color:var(--warn)">{len(exps)}</div></div>'
    html += '</div>'

    if not closed_positions:
        html += '<p style="color:var(--muted);font-size:.8rem;margin-top:20px">Todavía no hay posiciones cerradas.</p>'
    else:
        for date in dates_sorted:
            rows = groups[date]
            html += f'<div class="day-group">'
            html += (f'<div class="day-header"><span class="day-label">Cierre: {date}</span>'
                     f'<span class="day-count">{len(rows)} pos</span>'
                     f'<div class="day-line"></div></div>')
            html += ('<table><thead><tr>'
                     '<th>Ciudad</th><th>Mercado</th><th>Hor.</th><th>Resultado</th><th>Razón</th>'
                     '<th class="r">Entrada</th><th class="r">Cierre</th>'
                     '<th class="r">P&amp;L $</th><th class="r">P&amp;L %</th>'
                     '<th class="r">Overshoot</th><th class="r">Evento</th>'
                     '</tr></thead><tbody>')
            for p in rows:
                pnl     = p.get("sim_pnl") or 0
                pnl_pct = p.get("sim_pnl_pct") or 0
                gc      = "var(--gain)" if pnl >= 0 else "var(--loss)"
                gpc     = "var(--gain)" if pnl_pct >= 0 else "var(--loss)"
                over    = p.get("overshoot", "")
                over_s  = f"{over:.2f}°{p.get('unit','')}" if isinstance(over, (int, float)) else "—"
                mkt_s   = _short_market(p)
                html += (f'<tr>'
                         f'<td class="city">{p["city"]}</td>'
                         f'<td class="muted" style="font-size:.68rem;white-space:nowrap">{mkt_s}</td>'
                         f'<td><span class="hor">T+{p.get("days_ahead","?")}</span></td>'
                         f'<td>{_outcome_pill(p.get("outcome","?"))}</td>'
                         f'<td class="muted" style="font-size:.68rem">{p.get("close_reason","—")}</td>'
                         f'<td class="r">{(p.get("no_ask_entry") or 0):.4f}</td>'
                         f'<td class="r">{(p.get("no_price_close") or 0):.4f}</td>'
                         f'<td class="r"><span style="color:{gc}">${pnl:+.3f}</span></td>'
                         f'<td class="r"><span style="color:{gpc}">{pnl_pct:+.1f}%</span></td>'
                         f'<td class="r muted">{over_s}</td>'
                         f'<td class="r muted">{p.get("date_str","—")}</td>'
                         f'</tr>')
            html += '</tbody></table></div>'

    html += _HTML_FOOT.format(stake=SIM_STAKE, ts=ts)
    return html


def _build_sandbox_page(sandbox_open: list, sandbox_closed: list,
                         n_open: int, n_closed: int, ts: str) -> str:
    """
    Página de sandbox: Tel Aviv, Sao Paulo y cualquier ciudad con sandbox=True.
    Incluye posiciones abiertas Y cerradas en la misma página, separadas por sección.
    No contamina los stats de las páginas principales.
    """
    for p in sandbox_open:
        entry = p.get("no_ask_entry") or 0
        now   = p.get("no_price_last") or entry
        p["_delta"] = ((now - entry) / entry * 100) if entry else 0.0

    sandbox_cities = sorted({p["city"] for p in sandbox_open + sandbox_closed})
    gains  = [p for p in sandbox_open if p["_delta"] > 0.05]
    losses_open = [p for p in sandbox_open if p["_delta"] < -0.05]
    wins_closed  = [p for p in sandbox_closed if p.get("outcome") == "WIN"]
    losses_closed = [p for p in sandbox_closed if p.get("outcome") == "LOSS"]
    total_pnl = sum(p.get("sim_pnl") or 0 for p in sandbox_closed)
    pnl_col   = "var(--gain)" if total_pnl >= 0 else "var(--loss)"
    n_sandbox = len(sandbox_open) + len(sandbox_closed)

    html  = _HTML_HEAD.format(title="Overshoot Bot — Sandbox")
    html += '<header><h1>⚡ Overshoot Bot</h1>'
    html += ('<p>Modo sandbox · ciudades con baja liquidez · datos excluidos de estadísticas principales</p>'
             '<p style="font-size:.65rem;color:var(--warn);margin-top:4px">'
             '⚠ ' + ", ".join(sandbox_cities) + '</p></header>')
    html += _HTML_NAV.format(cls_open="", cls_closed="", cls_sandbox="active-sandbox",
                              n_open=n_open, n_closed=n_closed, n_sandbox=n_sandbox)
    # Stats
    html += '<div class="summary">'
    html += f'<div class="stat"><div class="stat-label">Abiertas</div><div class="stat-val" style="color:#fff">{len(sandbox_open)}</div></div>'
    html += f'<div class="stat"><div class="stat-label">En ganancia</div><div class="stat-val" style="color:var(--gain)">{len(gains)}</div></div>'
    html += f'<div class="stat"><div class="stat-label">En pérdida</div><div class="stat-val" style="color:var(--loss)">{len(losses_open)}</div></div>'
    html += f'<div class="stat"><div class="stat-label">WIN</div><div class="stat-val" style="color:var(--gain)">{len(wins_closed)}</div></div>'
    html += f'<div class="stat"><div class="stat-label">LOSS</div><div class="stat-val" style="color:var(--loss)">{len(losses_closed)}</div></div>'
    html += f'<div class="stat"><div class="stat-label">P&amp;L cerradas</div><div class="stat-val" style="color:{pnl_col}">${total_pnl:+.2f}</div></div>'
    html += '</div>'

    # ── Sección: Abiertas ──
    html += ('<div class="day-header" style="margin-bottom:12px">'
             '<span class="day-label" style="color:var(--warn)">Posiciones abiertas</span>'
             f'<span class="day-count" style="background:#2a1f0a;color:var(--warn);border-color:#4a3010">{len(sandbox_open)}</span>'
             '<div class="day-line"></div></div>')

    if not sandbox_open:
        html += '<p style="color:var(--muted);font-size:.8rem;margin-bottom:24px">Sin posiciones abiertas en sandbox.</p>'
    else:
        groups_open  = _group_by_date(sandbox_open, "date_str", lambda r: r["_delta"])
        for date in sorted(groups_open.keys(), reverse=True):
            rows = groups_open[date]
            html += (f'<div class="day-group"><div class="day-header">'
                     f'<span class="day-label">{date}</span>'
                     f'<span class="day-count">{len(rows)} pos</span>'
                     f'<div class="day-line"></div></div>')
            html += ('<table><thead><tr>'
                     '<th>Ciudad</th><th>Mercado</th><th>Hor.</th>'
                     '<th class="r">Entrada</th><th class="r">Actual</th>'
                     '<th class="r">Stop</th><th class="r">Var %</th><th>Movim.</th>'
                     '<th class="r">Overshoot</th><th class="r">ROE/d</th><th class="r">Horas</th>'
                     '</tr></thead><tbody>')
            for p in rows:
                entry   = p.get("no_ask_entry", 0)
                now     = p.get("no_price_last") or entry
                stop    = p.get("stop_price", 0)
                delta   = p["_delta"]
                elapsed = ""
                if p.get("ts_entry"):
                    try:
                        elapsed_h = (datetime.now() - datetime.fromisoformat(p["ts_entry"])).total_seconds() / 3600
                        elapsed = f"{elapsed_h:.1f}h"
                    except Exception:
                        pass
                roe    = p.get("roe_per_day", "")
                roe_s  = f"{roe:.1f}%" if isinstance(roe, (int, float)) else "—"
                over   = p.get("overshoot", "")
                over_s = f"{over:.2f}°{p.get('unit','')}" if isinstance(over, (int, float)) else "—"
                mkt_s  = _short_market(p)
                html += (f'<tr>'
                         f'<td class="city">{p["city"]}</td>'
                         f'<td class="muted" style="font-size:.68rem;white-space:nowrap">{mkt_s}</td>'
                         f'<td><span class="hor">T+{p.get("days_ahead","?")}</span></td>'
                         f'<td class="r">{entry:.4f}</td>'
                         f'<td class="r">{now:.4f}</td>'
                         f'<td class="r stop">{stop:.4f}</td>'
                         f'<td class="r">{_pct_pill(delta)}</td>'
                         f'<td>{_bar(delta)}</td>'
                         f'<td class="r muted">{over_s}</td>'
                         f'<td class="r muted">{roe_s}</td>'
                         f'<td class="r muted">{elapsed}</td>'
                         f'</tr>')
            html += '</tbody></table></div>'

    # ── Sección: Cerradas ──
    html += ('<div class="day-header" style="margin:24px 0 12px">'
             '<span class="day-label" style="color:var(--warn)">Historial cerradas</span>'
             f'<span class="day-count" style="background:#2a1f0a;color:var(--warn);border-color:#4a3010">{len(sandbox_closed)}</span>'
             '<div class="day-line"></div></div>')

    if not sandbox_closed:
        html += '<p style="color:var(--muted);font-size:.8rem">Sin posiciones cerradas en sandbox.</p>'
    else:
        for p in sandbox_closed:
            p["_close_date"] = (p.get("ts_close") or "")[:10] or "?"
        groups_closed = _group_by_date(sandbox_closed, "_close_date",
                                        lambda r: r.get("sim_pnl_pct") or 0)
        for date in sorted(groups_closed.keys(), reverse=True):
            rows = groups_closed[date]
            html += (f'<div class="day-group"><div class="day-header">'
                     f'<span class="day-label">Cierre: {date}</span>'
                     f'<span class="day-count">{len(rows)} pos</span>'
                     f'<div class="day-line"></div></div>')
            html += ('<table><thead><tr>'
                     '<th>Ciudad</th><th>Mercado</th><th>Hor.</th><th>Resultado</th><th>Razón</th>'
                     '<th class="r">Entrada</th><th class="r">Cierre</th>'
                     '<th class="r">P&amp;L $</th><th class="r">P&amp;L %</th>'
                     '<th class="r">Overshoot</th><th class="r">Evento</th>'
                     '</tr></thead><tbody>')
            for p in rows:
                pnl     = p.get("sim_pnl") or 0
                pnl_pct = p.get("sim_pnl_pct") or 0
                gc      = "var(--gain)" if pnl >= 0 else "var(--loss)"
                gpc     = "var(--gain)" if pnl_pct >= 0 else "var(--loss)"
                over    = p.get("overshoot", "")
                over_s  = f"{over:.2f}°{p.get('unit','')}" if isinstance(over, (int, float)) else "—"
                mkt_s   = _short_market(p)
                html += (f'<tr>'
                         f'<td class="city">{p["city"]}</td>'
                         f'<td class="muted" style="font-size:.68rem;white-space:nowrap">{mkt_s}</td>'
                         f'<td><span class="hor">T+{p.get("days_ahead","?")}</span></td>'
                         f'<td>{_outcome_pill(p.get("outcome","?"))}</td>'
                         f'<td class="muted" style="font-size:.68rem">{p.get("close_reason","—")}</td>'
                         f'<td class="r">{(p.get("no_ask_entry") or 0):.4f}</td>'
                         f'<td class="r">{(p.get("no_price_close") or 0):.4f}</td>'
                         f'<td class="r"><span style="color:{gc}">${pnl:+.3f}</span></td>'
                         f'<td class="r"><span style="color:{gpc}">{pnl_pct:+.1f}%</span></td>'
                         f'<td class="r muted">{over_s}</td>'
                         f'<td class="r muted">{p.get("date_str","—")}</td>'
                         f'</tr>')
            html += '</tbody></table></div>'

    html += _HTML_FOOT.format(stake=SIM_STAKE, ts=ts)
    return html


def generate_html_dashboard(db: "OvershootDB"):
    """
    Genera index.html (abiertas), closed.html (cerradas) y sandbox.html (prueba).
    Las ciudades con sandbox=True quedan separadas y NO contaminan los stats principales.
    """
    ts = datetime.now().strftime("%Y-%m-%d %H:%M UTC")

    all_open     = db.get_open_positions()
    open_pos     = [p for p in all_open if not p.get("is_sandbox")]
    sandbox_open = [p for p in all_open if p.get("is_sandbox")]

    with db._conn() as conn:
        cols = None
        cur = conn.execute(
            "SELECT * FROM positions WHERE outcome != 'OPEN' AND (is_sandbox IS NULL OR is_sandbox=0) "
            "ORDER BY ts_close DESC"
        )
        cols        = [d[0] for d in cur.description]
        closed_rows = [dict(zip(cols, r)) for r in cur.fetchall()]

        cur2 = conn.execute(
            "SELECT * FROM positions WHERE outcome != 'OPEN' AND is_sandbox=1 "
            "ORDER BY ts_close DESC"
        )
        sandbox_closed = [dict(zip([d[0] for d in cur2.description], r)) for r in cur2.fetchall()]

    n_sandbox = len(sandbox_open) + len(sandbox_closed)

    with open(DASHBOARD_OPEN_HTML, "w", encoding="utf-8") as f:
        f.write(_build_open_page(open_pos, len(closed_rows), n_sandbox, ts))
    with open(DASHBOARD_CLOSED_HTML, "w", encoding="utf-8") as f:
        f.write(_build_closed_page(closed_rows, len(open_pos), n_sandbox, ts))
    with open(DASHBOARD_SANDBOX_HTML, "w", encoding="utf-8") as f:
        f.write(_build_sandbox_page(sandbox_open, sandbox_closed, len(open_pos), len(closed_rows), ts))

    log.info(
        f"  🌐 Dashboard HTML generado: {DASHBOARD_OPEN_HTML} / "
        f"{DASHBOARD_CLOSED_HTML} / {DASHBOARD_SANDBOX_HTML} "
        f"(sandbox: {n_sandbox} pos)"
    )

# ── Motor principal ───────────────────────────────────────────────────────────
class OvershootBot:
    def __init__(self):
        self.db = OvershootDB()

    def run(self):
        """
        Ciclo único: escanear + trackear.
        Diseñado para correr cada 3 horas vía cron o GitHub Actions.
        """
        log.info("=" * 65)
        log.info("  PWBOT OVERSHOOT — iniciando ciclo")
        log.info("=" * 65)

        stats = {"cities_ok": 0, "candidates": 0, "entered": 0, "tracked": 0, "stopped": 0}

        # ── 1. Trackear posiciones abiertas primero ──
        log.info("── [1/3] Trackeando posiciones abiertas ──")
        open_positions = self.db.get_open_positions()
        log.info(f"  {len(open_positions)} posiciones abiertas")
        for pos in open_positions:
            self._track_position(pos, stats)

        # ── 2. Escanear nuevas oportunidades ──
        log.info("── [2/3] Escaneando ciudades ──")
        for city in CITIES:
            self._scan_city(city, stats)

        # ── 3. Exportar CSVs + Dashboard HTML ──
        log.info("── [3/3] Exportando CSVs y dashboard HTML ──")
        export_open_positions_csv(self.db)
        export_closed_csv(self.db)
        generate_html_dashboard(self.db)

        self.db.log_scan(
            stats["cities_ok"], stats["candidates"],
            stats["entered"],   stats["tracked"],
            stats["stopped"]
        )

        log.info("=" * 65)
        log.info(
            f"  Ciclo completo — ciudades_ok={stats['cities_ok']}  "
            f"candidatos={stats['candidates']}  entradas={stats['entered']}  "
            f"tracked={stats['tracked']}  stops={stats['stopped']}"
        )
        log.info("=" * 65)

        self._print_summary()

    # ── Track posición existente ──────────────────────────────────────────────
    def _track_position(self, pos: dict, stats: dict):
        pos_id       = pos["id"]
        city         = pos["city"]
        days_ahead   = pos["days_ahead"]
        date_str     = pos["date_str"]
        no_ask_entry = pos["no_ask_entry"]
        stop_price   = pos.get("stop_price") or (no_ask_entry * STOP_MULTIPLIER)
        sim_stake    = pos.get("sim_stake", SIM_STAKE)
        ts_entry     = datetime.fromisoformat(pos["ts_entry"])
        elapsed_h    = (datetime.now() - ts_entry).total_seconds() / 3600

        # ── ¿El mercado ya venció? Chequeamos PRIMERO, antes de pedir precios ──
        # Los mercados vencidos no tienen libro de órdenes activo en el CLOB,
        # por lo que no_price_now sería None y el bot nunca resolvería la posición.
        event_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        if datetime.now().date() > event_date:
            if RESOLVE_EXPIRED:
                no_price_ref = pos.get("no_price_last") or no_ask_entry
                self._resolve_expired(pos, no_price_ref, sim_stake)
            else:
                log.info(
                    f"  ⏸  #{pos_id} {city} {date_str} — mercado expirado, "
                    f"resolución desactivada (RESOLVE_EXPIRED=False)"
                )
            return

        prices = get_clob_prices(pos.get("token_yes_id", ""), pos.get("token_no_id", ""))
        no_bid = prices.get("no_bid")
        no_ask = prices.get("no_ask")
        yes_bid = prices.get("yes_bid")

        # Usar bid como precio actual (precio de salida real)
        no_price_now = no_bid if no_bid else no_ask

        if no_price_now is None:
            log.debug(f"  Track #{pos_id} {city}: sin precios, saltando")
            return

        no_max = max(filter(None, [pos.get("no_price_max"), no_price_now]))
        no_min = min(filter(None, [pos.get("no_price_min", 1.0), no_price_now]))

        self.db.save_tick(pos_id, no_bid, no_ask, yes_bid, elapsed_h)
        self.db.update_tracking(pos_id, no_price_now, no_max, no_min, (pos.get("ticks_tracked") or 0) + 1)
        stats["tracked"] += 1

        log.info(
            f"  📍 #{pos_id} {city} T+{days_ahead} {date_str}  "
            f"no_entry={no_ask_entry:.4f}  no_now={no_price_now:.4f}  "
            f"stop={stop_price:.4f}  elapsed={elapsed_h:.1f}h"
        )

        # ── Stop loss ──
        if no_price_now <= stop_price:
            tokens    = sim_stake / no_ask_entry if no_ask_entry else 0
            val_close = no_price_now * tokens
            pnl       = val_close - sim_stake
            pnl_pct   = pnl / sim_stake * 100
            self.db.close_position(pos_id, "LOSS", "STOP", no_price_now, pnl, pnl_pct)
            stats["stopped"] += 1
            log.info(
                f"  🛑 STOP #{pos_id} {city}  "
                f"no_price={no_price_now:.4f} ≤ stop={stop_price:.4f}  "
                f"PnL={pnl:+.3f}$ ({pnl_pct:+.1f}%)"
            )
            return


    # ── Resolver posición expirada ──────────────────────────────────────────────
    def _resolve_expired(self, pos: dict, no_price_now, sim_stake: float):
        """
        Consulta la temperatura máxima real del aeropuerto ICAO oficial (IEM ASOS) y determina WIN o LOSS.
        
        Compramos token NO apostando a que el threshold NO se alcanzará.
        - WIN: temperatura real < threshold  → el NO vale 1.0 → cobro completo
        - LOSS: temperatura real >= threshold → el NO vale 0.0 → perdemos todo
        - UNKNOWN: sin datos aún → dejar abierta, intentar en el próximo run
        """
        pos_id    = pos["id"]
        city      = pos["city"]
        date_str  = pos["date_str"]
        threshold = pos["market_threshold"]
        unit      = pos["unit"]
        no_ask_entry = pos["no_ask_entry"]

        actual_temp = fetch_actual_temp(city, date_str)

        if actual_temp is None:
            log.info(
                f"  ⏳ #{pos_id} {city} {date_str} — temperatura real no disponible aún, "
                f"manteniendo abierta"
            )
            return

        tokens = sim_stake / no_ask_entry if no_ask_entry else 0

        if actual_temp < threshold:
            # El threshold NO se alcanzó → token NO vale 1.0 → WIN
            val_close = 1.0 * tokens
            pnl       = val_close - sim_stake
            pnl_pct   = pnl / sim_stake * 100
            self.db.close_position(pos_id, "WIN", "EXPIRED_WIN", 1.0, pnl, pnl_pct)
            log.info(
                f"  ✅ WIN #{pos_id} {city} {date_str}  "
                f"real={actual_temp}°{unit} < threshold={threshold}°{unit}  "
                f"PnL={pnl:+.3f}$ ({pnl_pct:+.1f}%)"
            )
        else:
            # El threshold SÍ se alcanzó → token NO vale 0.0 → LOSS
            val_close = 0.0
            pnl       = val_close - sim_stake
            pnl_pct   = -100.0
            close_price = no_price_now if no_price_now is not None else 0.0
            self.db.close_position(pos_id, "LOSS", "EXPIRED_LOSS", close_price, pnl, pnl_pct)
            log.info(
                f"  ❌ LOSS #{pos_id} {city} {date_str}  "
                f"real={actual_temp}°{unit} >= threshold={threshold}°{unit}  "
                f"PnL={pnl:+.3f}$ ({pnl_pct:+.1f}%)"
            )

    # ── Escanear ciudad ───────────────────────────────────────────────────────
    def _scan_city(self, city: str, stats: dict):
        is_sandbox = bool(CITIES[city].get("sandbox"))
        sandbox_tag = " [SANDBOX]" if is_sandbox else ""
        log.info(f"  🌐 {city}{sandbox_tag} — escaneando...")

        # Obtener pronóstico Meteoblue para T+1 y T+2
        forecasts = {}
        for d in range(MIN_DAYS, MAX_DAYS + 1):
            target = datetime.now().date() + timedelta(days=d)
            fc = fetch_meteoblue(city, target)
            if fc:
                forecasts[d] = fc
                log.info(
                    f"    MB {city} T+{d}: max_model={fc['max_model']}°{fc['unit']}  "
                    f"consensus={fc['consensus']}°  n={fc['n_models']}"
                )
            else:
                log.warning(f"    MB {city} T+{d}: sin datos")

        if not forecasts:
            log.warning(f"  {city}: sin pronóstico Meteoblue, saltando")
            return

        stats["cities_ok"] += 1

        # Obtener mercados de Polymarket
        markets = fetch_markets_for_city(city)
        if not markets:
            log.info(f"  {city}: sin mercados T+1/T+2 en Polymarket")
            return

        log.info(f"  {city}: {len(markets)} mercados encontrados")

        # Filtrar candidatos
        candidates = []
        for mkt in markets:
            d  = mkt["days_ahead"]
            fc = forecasts.get(d)
            if not fc:
                continue

            threshold = mkt["threshold"]
            max_model = fc["max_model"]
            overshoot = round(threshold - max_model, 3)

            # ¿El threshold supera al max_modelo en al menos OVERSHOOT_MIN?
            overshoot_min = OVERSHOOT_MIN_F if fc["unit"] == "F" else OVERSHOOT_MIN_C
            if overshoot < overshoot_min:
                log.info(
                    f"    ↳ T+{d} threshold={threshold}°{fc['unit']}  "
                    f"mb_max={max_model}°  overshoot={overshoot:+.2f}°  "
                    f"(min={overshoot_min}°{fc['unit']})  — muy cerca, skip"
                )
                continue

            # Obtener precios CLOB
            prices = get_clob_prices(mkt["token_yes"], mkt["token_no"])
            no_ask = prices.get("no_ask")
            no_bid = prices.get("no_bid")
            yes_bid = prices.get("yes_bid")

            if no_ask is None:
                log.info(f"    ↳ T+{d} threshold={threshold}°{fc['unit']}  mb_max={max_model}°  overshoot={overshoot:+.2f}°  — sin precio CLOB")
                continue

            # ¿El ask real del token NO está en rango [NO_MIN_PRICE, NO_MAX_PRICE]?
            # Precio muy bajo → mercado descuenta riesgo real, no entrar.
            if no_ask < NO_MIN_PRICE:
                log.info(
                    f"    ↳ T+{d} threshold={threshold}°{fc['unit']}  mb_max={max_model}°  "
                    f"overshoot={overshoot:+.2f}°  no_ask={no_ask:.4f} < {NO_MIN_PRICE}  — precio muy bajo (riesgo), skip"
                )
                continue

            # Precio muy alto → spread/liquidez mala
            if no_ask > NO_MAX_PRICE:
                log.info(
                    f"    ↳ T+{d} threshold={threshold}°{fc['unit']}  mb_max={max_model}°  "
                    f"overshoot={overshoot:+.2f}°  no_ask={no_ask:.4f} > {NO_MAX_PRICE}  — precio muy alto, skip"
                )
                continue

            roe_day = calc_roe_per_day(no_ask, d)
            score   = calc_score(no_ask, overshoot, overshoot_min, d, fc["std_dev"])

            candidates.append({
                "city":       city,
                "market_id":  mkt["market_id"],
                "question":   mkt["question"],
                "date_str":   mkt["date_str"],
                "days_ahead": d,
                "unit":       fc["unit"],
                "is_sandbox": 1 if CITIES[city].get("sandbox") else 0,
                "mb_max_model":  max_model,
                "mb_consensus":  fc["consensus"],
                "mb_std_dev":    fc["std_dev"],
                "mb_n_models":   fc["n_models"],
                "mb_models_json": json.dumps(fc["models"]),
                "market_threshold": threshold,
                "overshoot":  overshoot,
                "market_type": mkt["mtype"],
                "no_ask_entry": no_ask,
                "no_bid_entry": no_bid,
                "yes_bid_entry": yes_bid,
                "token_yes_id": mkt["token_yes"],
                "token_no_id":  mkt["token_no"],
                "roe_per_day":  roe_day,
                "score":        score,
            })

            stats["candidates"] += 1
            log.info(
                f"    ✨ CANDIDATO: {city} T+{d} threshold={threshold}°  "
                f"overshoot={overshoot:.2f}°  no_ask={no_ask:.4f}  "
                f"ROE/day={roe_day:.1f}%  score={score:.2f}"
            )

        if not candidates:
            return

        # Ordenar por score descendente y entrar en el mejor
        candidates.sort(key=lambda x: x["score"], reverse=True)
        top = candidates[0]

        log.info(
            f"  🏆 MEJOR CANDIDATO: {top['city']} T+{top['days_ahead']} "
            f"threshold={top['market_threshold']}°  "
            f"overshoot={top['overshoot']:.2f}°  "
            f"score={top['score']:.2f}"
        )

        # Si ya tenemos una posición abierta en este mercado, no duplicar
        if self.db.already_open(top["market_id"]):
            log.info(f"  ⏭  Ya hay posición abierta en {top['market_id'][:12]}... — skip")
            return

        self._enter_position(top, stats)

    # ── Entrar posición (sim) ─────────────────────────────────────────────────
    def _enter_position(self, candidate: dict, stats: dict):
        no_ask     = candidate["no_ask_entry"]
        sim_tokens = SIM_STAKE / no_ask if no_ask > 0 else 0
        stop_price = round(no_ask * STOP_MULTIPLIER, 4)

        pos = {
            "ts_entry":       datetime.now().isoformat(),
            "city":           candidate["city"],
            "market_id":      candidate["market_id"],
            "question":       candidate["question"],
            "date_str":       candidate["date_str"],
            "days_ahead":     candidate["days_ahead"],
            "unit":           candidate["unit"],
            "mb_max_model":   candidate["mb_max_model"],
            "mb_consensus":   candidate["mb_consensus"],
            "mb_n_models":    candidate["mb_n_models"],
            "mb_models_json": candidate["mb_models_json"],
            "market_threshold": candidate["market_threshold"],
            "overshoot":      candidate["overshoot"],
            "market_type":    candidate["market_type"],
            "no_ask_entry":   no_ask,
            "no_bid_entry":   candidate.get("no_bid_entry"),
            "yes_bid_entry":  candidate.get("yes_bid_entry"),
            "token_yes_id":   candidate["token_yes_id"],
            "token_no_id":    candidate["token_no_id"],
            "roe_per_day":    candidate["roe_per_day"],
            "risk_distance":  candidate["overshoot"],
            "score":          candidate["score"],
            "sim_stake":      SIM_STAKE,
            "sim_tokens":     round(sim_tokens, 4),
            "stop_price":     stop_price,
            "no_price_last":  no_ask,
            "no_price_max":   no_ask,
            "no_price_min":   no_ask,
            "ticks_tracked":  1,
            "last_update":    datetime.now().isoformat(),
            "outcome":        "OPEN",
            "is_sandbox":     candidate.get("is_sandbox", 0),
        }

        pos_id = self.db.save_position(pos)
        stats["entered"] += 1

        log.info(
            f"  💰 [SIM] ENTRADA #{pos_id}: {candidate['city']} T+{candidate['days_ahead']}  "
            f"threshold={candidate['market_threshold']}°  "
            f"mb_max={candidate['mb_max_model']}°  "
            f"overshoot={candidate['overshoot']:.2f}°  "
            f"no_ask={no_ask:.4f}  stop={stop_price:.4f}  "
            f"tokens={sim_tokens:.2f}  stake=${SIM_STAKE}  "
            f"ROE/day={candidate['roe_per_day']:.1f}%"
        )

    # ── Resumen ───────────────────────────────────────────────────────────────
    def _print_summary(self):
        try:
            with self.db._conn() as conn:
                total   = conn.execute("SELECT COUNT(*) FROM positions").fetchone()[0]
                open_n  = conn.execute("SELECT COUNT(*) FROM positions WHERE outcome='OPEN'").fetchone()[0]
                wins    = conn.execute("SELECT COUNT(*) FROM positions WHERE outcome='WIN'").fetchone()[0]
                losses  = conn.execute("SELECT COUNT(*) FROM positions WHERE outcome='LOSS'").fetchone()[0]
                expired = conn.execute("SELECT COUNT(*) FROM positions WHERE outcome='EXPIRED'").fetchone()[0]
                avg_roe = conn.execute(
                    "SELECT AVG(roe_per_day) FROM positions WHERE roe_per_day IS NOT NULL"
                ).fetchone()[0]
                avg_over = conn.execute(
                    "SELECT AVG(overshoot) FROM positions WHERE overshoot IS NOT NULL"
                ).fetchone()[0]
                pnl_total = conn.execute(
                    "SELECT SUM(sim_pnl) FROM positions WHERE outcome!='OPEN'"
                ).fetchone()[0]

            closed = wins + losses
            wr     = wins / closed * 100 if closed else 0.0

            print("\n" + "═" * 60)
            print("  RESUMEN OVERSHOOT BOT")
            print("═" * 60)
            print(f"  Total posiciones:   {total}")
            print(f"  Abiertas:           {open_n}")
            print(f"  Cerradas:           {closed}  ({wins}W / {losses}L / {expired} exp)")
            print(f"  Win rate:           {wr:.0f}%")
            print(f"  P&L total (sim):    ${(pnl_total or 0):.3f}")
            print(f"  ROE/día promedio:   {avg_roe:.1f}%" if avg_roe else "  ROE/día promedio:   n/a")
            print(f"  Overshoot prom:     {avg_over:.2f}°" if avg_over else "  Overshoot prom:     n/a")
            print(f"  Positions CSV:      {os.path.abspath(POSITIONS_CSV)}")
            print(f"  Closed CSV:         {os.path.abspath(CLOSED_CSV)}")
            print("═" * 60 + "\n")
        except Exception as e:
            log.error(f"_print_summary: {e}")


# ── GitHub Actions / cron entry point ────────────────────────────────────────
def main():
    print("\n" + "═" * 65)
    print("  POLYMARKET WEATHER — OVERSHOOT STRATEGY BOT  v1")
    print("  Estrategia: token NO donde threshold > max_modelo + 1.5°")
    print("═" * 65)
    print(f"  DB:            {os.path.abspath(DB_PATH)}")
    print(f"  Positions CSV: {os.path.abspath(POSITIONS_CSV)}")
    print(f"  Closed CSV:    {os.path.abspath(CLOSED_CSV)}")
    print(f"  Modo:          SIM (sin órdenes reales)")
    print(f"  Overshoot:     ≥ {OVERSHOOT_MIN_C}°C  /  ≥ {OVERSHOOT_MIN_F}°F  (sin techo, más es mejor)")
    print(f"  NO_PRICE:      ask CLOB ∈ [{NO_MIN_PRICE}, {NO_MAX_PRICE}]")
    print(f"  Stop loss:     {(1-STOP_MULTIPLIER)*100:.0f}% del precio de entrada")
    print(f"  Stake sim:     ${SIM_STAKE} por posición")
    print("═" * 65 + "\n")

    bot = OvershootBot()
    bot.run()

if __name__ == "__main__":
    main()
