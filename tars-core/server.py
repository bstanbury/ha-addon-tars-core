#!/usr/bin/env python3
"""TARS Core v5.0.0 — Consolidated Intelligence Engine (v5 upgrade)

v5.0 changes:
  - SQLite-backed event store (storage.py) for events/decisions/anomalies/modes
  - Typed configuration (typed_config.py) with validation + secret redaction
  - HTTP helpers (helpers.py) consolidating HA + Services REST clients
  - Embedded HTML dashboard (dashboard.py) at GET /
  - Arrival-based focus mode trigger
  - Energy anomaly detection (week-over-week power use)
  - Daily storage prune cron

Unchanged from v4:
  - 8093 port, 39 routes, single Flask app
  - WebSocket to HA
  - Calls SERVICES_URL for DJ/Hue/SwitchBot/Vacuum

Route map (unchanged):
  Event Bus:   GET /events/stream, /events/recent, /events/stats,
                   /bedroom-motion-age, /patterns, /anomalies
  Intelligence: GET /health, /, /context, /decide, /mode, /learned,
                    /cooper, /insights, /log, /proactive, /presence,
                    /predictions, /weather/reactive, /dashboard,
                    /calendar/today, /sonos/following
               POST /arrive, /depart, /mood/<mood>, /cooper/here,
                    /cooper/gone, /suggestion/<id>/dismiss,
                    /suggestion/<id>/accept, /sonos/follow/<room>/<action>
  Analytics:   GET /analytics/daily, /analytics/sleep, /analytics/energy,
                   /analytics/trends, /analytics/health,
                   /analytics/energy/cost, /analytics/sleep/last,
                   /analytics/sleep/trend

v5 new routes:
  GET /                            — HTML dashboard (was JSON)
  GET /dashboard.json              — original JSON dashboard (preserved)
  GET /config                      — typed config (redacted)
  GET /storage/stats               — SQLite row counts + sizes
  POST /storage/prune              — manual prune trigger
  GET /history/events              — query SQLite events
  GET /history/decisions           — query SQLite decisions
  GET /history/anomalies           — query SQLite anomalies
  GET /history/modes               — query SQLite mode transitions
"""
import os, json, time, logging, threading, re, uuid
from datetime import datetime, timedelta, timezone
from collections import deque, Counter, defaultdict
from flask import Flask, jsonify, Response, request, render_template_string
import requests as http
import websocket
import sseclient

# v5 modules (support both package import and direct script execution)
try:
    from . import storage
    from . import dashboard as dashboard_module
    from .typed_config import CoreConfig
    from .helpers import HAClient, ServicesClient
except ImportError:
    # Running as __main__ script (docker container sets CWD to /app)
    import sys
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import storage
    import dashboard as dashboard_module
    from typed_config import CoreConfig
    from helpers import HAClient, ServicesClient

# ─── Config ──────────────────────────────────────────────────────────────────
# v5: typed config with validation
CONFIG        = CoreConfig.load()
_cfg_errors   = CONFIG.validate()
HA_URL        = CONFIG.ha_url
HA_TOKEN      = CONFIG.ha_token
API_PORT      = CONFIG.api_port
SERVICES_URL  = CONFIG.services_url
COOPER_SCHED  = CONFIG.cooper_schedule
WS_URL        = HA_URL.replace('http', 'ws') + '/api/websocket'
WATCHED_DOMAINS = {'binary_sensor','media_player','lock','weather','vacuum',
                   'cover','sensor','sun','climate','person','device_tracker',
                   'alarm_control_panel','input_boolean','siren','switch'}

# v5: consolidated HTTP clients
HA  = HAClient(HA_URL, HA_TOKEN, mobile_notify_service=CONFIG.mobile_notify_service)
SVC = ServicesClient(SERVICES_URL)

# ─── Flask ────────────────────────────────────────────────────────────────────
app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('tars-core')

# ══════════════════════════════════════════════════════════════════════════════
# SHARED STATE
# ══════════════════════════════════════════════════════════════════════════════

# ─── Safety constants ─────────────────────────────────────────────────────────
BEDROOM_ENTITIES = ['media_player.bedroom', 'media_player.bedroom_sonos',
                    'media_player.bedroom_echo_show_chatsworth']
ECHO_ENTITIES    = ['media_player.chatsworth_living_room_echo_show',
                    'media_player.chatsworth_kitchen_echo_show',
                    'media_player.bedroom_echo_show_chatsworth',
                    'media_player.chatsworth_echo_show_5_bathroom']
SILENT_HOURS     = (22, 8)  # 10pm–8am

# ─── Room presence map ────────────────────────────────────────────────────────
# Corrected 2026-04-29: HA area registry-verified entity names
ROOM_SENSORS = {
    'bedroom':     ['binary_sensor.bedroom_motion', 'binary_sensor.mb_motion_sensor_motion',
                    'binary_sensor.mb_motion_sensor_motion_2'],
    'bathroom':    ['binary_sensor.bathroom_motion_motion', 'binary_sensor.bathroom_motion_motion_2'],
    'living_room': ['binary_sensor.switchbot_hub_3_motion', 'binary_sensor.motion_sensor_motion',
                    'binary_sensor.motion_sensor_motion_2', 'binary_sensor.hue_motion_sensor_3_motion_2'],
    'playroom':    ['binary_sensor.playroom_motion', 'binary_sensor.playroom_motion_motion',
                    'binary_sensor.playroom_motion_motion_2'],
    # Note: no kitchen motion sensor currently deployed.
    # Removed until one is added; Core will not report false 'never occupied' for kitchen.
}
# room_name -> {occupied: bool, last_motion: timestamp|None, last_motion_iso: str|None}
room_presence = {r: {'occupied': False, 'last_motion': None, 'last_motion_iso': None}
                 for r in ROOM_SENSORS}

# ─── Home mode state machine ──────────────────────────────────────────────────
home_mode    = 'unknown'
mode_history = deque(maxlen=50)
cooper_override = None
last_event_time = None
decision_log     = deque(maxlen=500)
event_driven_actions = deque(maxlen=200)

# ─── Adaptive learning ────────────────────────────────────────────────────────
adaptive_rules = {
    'nudge_ignored_count': 0,
    'auto_actions': {},
    'suppressed_actions': set(),
}
# suggestion id -> {suggestion, status, time}
suggestion_feedback = {}

# ─── Event Bus shared state ───────────────────────────────────────────────────
events     = deque(maxlen=500)
counts     = Counter()
states_map = {}   # entity_id -> current state value
ws_ok      = False
ws_id      = 1

# Pattern detection
PATTERN_FILE   = '/data/patterns.json'
patterns       = {}
recent_sequence = deque(maxlen=20)
entity_hour_histogram = defaultdict(lambda: Counter())
anomalies_bus  = deque(maxlen=200)
rate_window    = defaultdict(list)
rate_alerts    = deque(maxlen=50)
RATE_LIMIT     = 20

# v4.0: Anomaly detection v2 — cross-entity anomalies
anomalies_v2   = deque(maxlen=200)
last_bedroom_motion_time = None

# ─── Analytics state ──────────────────────────────────────────────────────────
STATS_DB_FILE = '/data/stats_db.json'
stats_db      = {}
departure_count = 0
arrival_count   = 0
motion_counts   = defaultdict(int)
temp_readings   = defaultdict(list)
dj_plays  = defaultdict(int)
dj_skips  = defaultdict(int)
sleep_disruptions = deque(maxlen=200)

# ─── Sleep scoring ────────────────────────────────────────────────────────────
SLEEP_SCORES_FILE = '/data/sleep_scores.json'
sleep_scores      = {}   # date -> {score, grade, co2_avg, temp_avg, humidity_avg, note}

# Overnight sensor buffers (22:00–08:00)
overnight_co2     = []
overnight_temp    = []
overnight_humidity = []

# ─── Energy tracking ──────────────────────────────────────────────────────────
POWERCALC_SENSORS = [
    'sensor.bathroom_heater_power', 'sensor.refrigerator_power',
    'sensor.living_room_tv_power',  'sensor.washing_machine_power',
    'sensor.dishwasher_power',      'sensor.dryer_power',
]
KWH_RATE = 0.35

# ─── Predictive scheduling ────────────────────────────────────────────────────
PREDICTIONS_FILE = '/data/predictions.json'
predictions_data = {
    'wake_times':   [],   # list of hour floats (e.g. 7.25 = 7:15am)
    'sleep_times':  [],
    'arrive_times': [],
    'depart_times': [],
}
preheat_armed = False   # bathroom preheat fired today?

# ─── Weather reactive state ───────────────────────────────────────────────────
weather_reactive_state = {
    'current_weather': None,
    'last_reaction': None,
    'blinds_closed': False,
    'dj_mood': None,
    'hue_scene': None,
}

# ══════════════════════════════════════════════════════════════════════════════
# PERSISTENCE
# ══════════════════════════════════════════════════════════════════════════════

def load_all_data():
    global adaptive_rules, home_mode, patterns, entity_hour_histogram
    global stats_db, sleep_scores, predictions_data

    # Intelligence state
    try:
        f = '/data/intelligence_v2.json'
        if os.path.exists(f):
            d = json.load(open(f))
            adaptive_rules = d.get('adaptive_rules', adaptive_rules)
            adaptive_rules['suppressed_actions'] = set(adaptive_rules.get('suppressed_actions', []))
            home_mode = d.get('home_mode', 'unknown')
            logger.info(f'Loaded intelligence state: mode={home_mode}')
    except Exception as e:
        logger.error(f'Load intelligence: {e}')

    # Event Bus patterns
    try:
        if os.path.exists(PATTERN_FILE):
            d = json.load(open(PATTERN_FILE))
            patterns = d.get('patterns', {})
            entity_hour_histogram = defaultdict(lambda: Counter(),
                {k: Counter(v) for k, v in d.get('histograms', {}).items()})
            logger.info(f'Loaded {len(patterns)} patterns')
    except Exception as e:
        logger.error(f'Load patterns: {e}')

    # Analytics stats DB
    try:
        if os.path.exists(STATS_DB_FILE):
            stats_db = json.load(open(STATS_DB_FILE))
            logger.info(f'Loaded stats DB: {len(stats_db)} days')
    except Exception as e:
        logger.error(f'Load stats DB: {e}')

    # Sleep scores
    try:
        if os.path.exists(SLEEP_SCORES_FILE):
            sleep_scores = json.load(open(SLEEP_SCORES_FILE))
            logger.info(f'Loaded sleep scores: {len(sleep_scores)} nights')
    except Exception as e:
        logger.error(f'Load sleep scores: {e}')

    # Predictions
    try:
        if os.path.exists(PREDICTIONS_FILE):
            predictions_data.update(json.load(open(PREDICTIONS_FILE)))
            logger.info('Loaded predictions')
    except Exception as e:
        logger.error(f'Load predictions: {e}')

def save_intelligence():
    try:
        d = {'adaptive_rules': {**adaptive_rules, 'suppressed_actions': list(adaptive_rules['suppressed_actions'])},
             'home_mode': home_mode, 'saved_at': datetime.now().isoformat()}
        json.dump(d, open('/data/intelligence_v2.json', 'w'), indent=2)
    except: pass

def save_patterns():
    try:
        d = {'patterns': patterns,
             'histograms': {k: dict(v) for k, v in entity_hour_histogram.items()}}
        json.dump(d, open(PATTERN_FILE, 'w'), indent=2)
    except: pass

def save_stats_db():
    try:
        cutoff = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        pruned = {k: v for k, v in stats_db.items() if k >= cutoff}
        json.dump(pruned, open(STATS_DB_FILE, 'w'), indent=2)
    except: pass

def save_sleep_scores():
    try:
        json.dump(sleep_scores, open(SLEEP_SCORES_FILE, 'w'), indent=2)
    except: pass

def save_predictions():
    try:
        json.dump(predictions_data, open(PREDICTIONS_FILE, 'w'), indent=2)
    except: pass

# ══════════════════════════════════════════════════════════════════════════════
# SHARED HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def svc_get(path, timeout=5):
    """GET from SERVICES_URL."""
    try:
        r = http.get(f'{SERVICES_URL}{path}', timeout=timeout)
        return r.json() if r.status_code == 200 else None
    except: return None

def svc_post(path, data=None, timeout=5):
    """POST to SERVICES_URL."""
    try:
        r = http.post(f'{SERVICES_URL}{path}', json=data, timeout=timeout)
        return r.json() if r.status_code == 200 else None
    except: return None

def ha_get(path):
    try:
        r = http.get(f'{HA_URL}/api{path}',
                     headers={'Authorization': f'Bearer {HA_TOKEN}'}, timeout=10)
        return r.json() if r.status_code == 200 else None
    except: return None

def ha_post(path, payload):
    try:
        r = http.post(f'{HA_URL}/api{path}',
                      headers={'Authorization': f'Bearer {HA_TOKEN}', 'Content-Type': 'application/json'},
                      json=payload, timeout=10)
        return r.json() if r.status_code in (200, 201) else None
    except: return None

def ha_call(domain, service, data):
    return ha_post(f'/services/{domain}/{service}', data)

def ha_notify(title, msg):
    ha_post('/services/notify/mobile_app_bks_home_assistant_chatsworth',
            {'data': {'title': title, 'message': msg}})

def ha_history(entity_id, hours=24):
    start = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    data = ha_get(f'/history/period/{start}?filter_entity_id={entity_id}&minimal_response&no_attributes')
    return data[0] if data and data[0] else []

def is_bedroom_safe():
    """REQUIRED guard before any bedroom audio."""
    if last_bedroom_motion_time is not None:
        if time.time() - last_bedroom_motion_time < 1800:  # 30 min
            return True
    try:
        r = http.get(f'{HA_URL}/api/states/binary_sensor.bedroom_motion',
                     headers={'Authorization': f'Bearer {HA_TOKEN}'}, timeout=5)
        if r.status_code == 200: return r.json()['state'] == 'on'
    except: pass
    return False

def is_silent_hours():
    h = datetime.now().hour
    return h >= SILENT_HOURS[0] or h < SILENT_HOURS[1]

def safe_notify(title, msg):
    """Always push; could add TTS branch here later."""
    ha_notify(title, msg)

def log_decision(decision_type, decisions, source='unknown', why=None):
    """Append to decision log with a human-readable 'why' string.
    v5: also persists to SQLite for post-mortem."""
    why_text = why or _build_why(decision_type, decisions)
    entry = {
        'id': str(uuid.uuid4())[:8],
        'type': decision_type,
        'time': datetime.now().isoformat(),
        'decisions': decisions,
        'source': source,
        'why': why_text,
    }
    decision_log.append(entry)
    # v5: persist to SQLite
    try:
        storage.record_decision(kind=decision_type, source=source,
                                actions=decisions, why=why_text)
    except Exception as e:
        logger.warning(f'storage.record_decision failed: {e}')
    return entry

def _build_why(decision_type, decisions):
    """Generate a natural language explanation for a set of decisions."""
    if not decisions: return f'{decision_type} — no actions taken'
    ctx_bits = []
    now = datetime.now()
    ctx_bits.append(f'{now.strftime("%I:%M%p").lower()}')
    ctx_bits.append(f'mode={home_mode}')
    if is_cooper_here(): ctx_bits.append('Cooper present')
    w = ha_get('/states/weather.forecast_home')
    if w: ctx_bits.append(f'weather={w["state"]}')
    actions = [d.get('action', '?') for d in (decisions if isinstance(decisions, list) else [])]
    reason_parts = [d.get('reason', '') for d in (decisions if isinstance(decisions, list) else []) if d.get('reason')]
    context_str = ', '.join(ctx_bits)
    action_str  = ' + '.join(actions[:3])
    reason_str  = '; '.join(set(reason_parts[:2]))
    return f'{action_str} because {reason_str} [{context_str}]' if reason_str else f'{action_str} [{context_str}]'

# ══════════════════════════════════════════════════════════════════════════════
# ROOM PRESENCE ENGINE
# ══════════════════════════════════════════════════════════════════════════════

# ─── Sonos room-follow (P2 added 2026-04-29) ──────────────────────────────────────────────
# Map rooms (from ROOM_SENSORS) to Sonos entities. Living room is the anchor/master.
# When motion fires in a follow-eligible room AND the anchor is actively playing music,
# join that room's Sonos to the anchor group. After 3+ min of no motion, unjoin.
SONOS_ANCHOR = 'media_player.living_room'
SONOS_ROOM_MAP = {
    'bathroom':  ['media_player.bathroom', 'media_player.sonos_roam_sl'],
    'bedroom':   ['media_player.bedroom'],
    'playroom':  ['media_player.sonos_roam'],
}
# Track which rooms are currently following + last motion per room
sonos_following = {}  # room_name -> {'joined_at': ts, 'entities': [...], 'last_motion': ts}
SONOS_FOLLOW_IDLE_SEC = 180  # 3 minutes of no motion → unjoin

def _is_anchor_playing():
    s = ha_get(f'/states/{SONOS_ANCHOR}')
    return bool(s and s.get('state') == 'playing')

def _sonos_join(room):
    """Join all Sonos entities for `room` to the anchor group."""
    entities = SONOS_ROOM_MAP.get(room, [])
    if not entities: return False
    # Safety: bedroom respects motion (motion already happened to get here, so OK)
    # Cooper mode: cap volume to 30% before joining
    try:
        # Use media_player.join service; pass group_members list including anchor + target
        http.post(f'{HA_URL}/api/services/media_player/join',
                  headers={'Authorization': f'Bearer {HA_TOKEN}', 'Content-Type': 'application/json'},
                  json={'entity_id': SONOS_ANCHOR, 'group_members': entities}, timeout=5)
        # Volume match: pull anchor volume, apply to joined entities (capped by Cooper)
        anchor_state = ha_get(f'/states/{SONOS_ANCHOR}')
        vol = float((anchor_state or {}).get('attributes', {}).get('volume_level', 0.25))
        if is_cooper_here():
            vol = min(vol, 0.30)
        for ent in entities:
            http.post(f'{HA_URL}/api/services/media_player/volume_set',
                      headers={'Authorization': f'Bearer {HA_TOKEN}', 'Content-Type': 'application/json'},
                      json={'entity_id': ent, 'volume_level': vol}, timeout=5)
        logger.info(f'SONOS_FOLLOW: Joined {entities} to {SONOS_ANCHOR} @ {vol:.2f}')
        return True
    except Exception as e:
        logger.error(f'SONOS_FOLLOW join failed: {e}')
        return False

def _sonos_unjoin(room):
    """Remove room's Sonos entities from the group."""
    entities = SONOS_ROOM_MAP.get(room, [])
    if not entities: return False
    try:
        for ent in entities:
            http.post(f'{HA_URL}/api/services/media_player/unjoin',
                      headers={'Authorization': f'Bearer {HA_TOKEN}', 'Content-Type': 'application/json'},
                      json={'entity_id': ent}, timeout=5)
        logger.info(f'SONOS_FOLLOW: Unjoined {entities} from {SONOS_ANCHOR}')
        return True
    except Exception as e:
        logger.error(f'SONOS_FOLLOW unjoin failed: {e}')
        return False

def on_motion_for_follow(room):
    """Called when motion fires in a room that has Sonos follow-eligibility."""
    if room not in SONOS_ROOM_MAP: return
    # Bedroom safety: silent hours block bedroom follow unless user overrides
    if room == 'bedroom':
        h = datetime.now().hour
        if h >= 22 or h < 7:
            logger.info('SONOS_FOLLOW: bedroom motion in silent hours — skipping follow')
            return
    if not _is_anchor_playing(): return
    now = time.time()
    if room in sonos_following:
        # Already following — just refresh last_motion
        sonos_following[room]['last_motion'] = now
        return
    # Join
    if _sonos_join(room):
        sonos_following[room] = {
            'joined_at': now, 'last_motion': now,
            'entities': list(SONOS_ROOM_MAP[room]),
        }
        log_decision('sonos_follow', [{'action': 'join', 'reason': f'Motion in {room}'}],
                     source='motion_event',
                     why=f'Sonos follow: joined {room} to living room anchor (motion detected)')

def sonos_follow_sweep():
    """Background sweep: unjoin rooms idle >3 min. Called from mode_updater_thread."""
    now = time.time()
    to_unjoin = [room for room, info in sonos_following.items()
                 if (now - info['last_motion']) > SONOS_FOLLOW_IDLE_SEC]
    for room in to_unjoin:
        if _sonos_unjoin(room):
            idle_sec = now - sonos_following[room]['last_motion']
            del sonos_following[room]
            log_decision('sonos_follow', [{'action': 'unjoin', 'reason': f'{room} idle {int(idle_sec)}s'}],
                         source='motion_timeout',
                         why=f'Sonos follow: unjoined {room} — no motion for {int(idle_sec/60)}min')
    # Also: if anchor stopped playing, unjoin everyone
    if sonos_following and not _is_anchor_playing():
        for room in list(sonos_following.keys()):
            _sonos_unjoin(room)
            del sonos_following[room]
        if sonos_following:
            log_decision('sonos_follow', [{'action': 'unjoin_all', 'reason': 'anchor stopped'}],
                         source='anchor_state',
                         why='Sonos follow: unjoined all — living room stopped playing')

def update_room_presence(entity_id, new_state):
    """Update room occupancy from motion sensor event."""
    for room, sensors in ROOM_SENSORS.items():
        if entity_id in sensors:
            if new_state == 'on':
                room_presence[room]['occupied'] = True
                room_presence[room]['last_motion'] = time.time()
                room_presence[room]['last_motion_iso'] = datetime.now().isoformat()
                # Sonos room-follow hook
                try: on_motion_for_follow(room)
                except Exception as e: logger.error(f'sonos_follow hook: {e}')
            break

def refresh_room_occupancy():
    """Decay room occupancy: clear rooms with no motion for >10 minutes."""
    now = time.time()
    for room, data in room_presence.items():
        lm = data.get('last_motion')
        if lm and (now - lm) > 600:  # 10 min
            room_presence[room]['occupied'] = False

# ══════════════════════════════════════════════════════════════════════════════
# PREDICTIVE SCHEDULING
# ══════════════════════════════════════════════════════════════════════════════

def record_wake_event():
    """Call when bedroom motion first triggers after 4am."""
    h = datetime.now().hour + datetime.now().minute / 60.0
    predictions_data['wake_times'].append(h)
    predictions_data['wake_times'] = predictions_data['wake_times'][-60:]  # 60-day window
    save_predictions()
    logger.info(f'PREDICT: recorded wake at {h:.2f}')

def record_sleep_event():
    h = datetime.now().hour + datetime.now().minute / 60.0
    predictions_data['sleep_times'].append(h)
    predictions_data['sleep_times'] = predictions_data['sleep_times'][-60:]
    save_predictions()

def record_arrive_event():
    h = datetime.now().hour + datetime.now().minute / 60.0
    predictions_data['arrive_times'].append(h)
    predictions_data['arrive_times'] = predictions_data['arrive_times'][-60:]
    save_predictions()

def record_depart_event():
    h = datetime.now().hour + datetime.now().minute / 60.0
    predictions_data['depart_times'].append(h)
    predictions_data['depart_times'] = predictions_data['depart_times'][-60:]
    save_predictions()

def predict_next(times_list):
    """Return predicted next occurrence as hour float (mean of recent data)."""
    if len(times_list) < 3: return None
    recent = times_list[-14:]
    mean = sum(recent) / len(recent)
    std  = (sum((x - mean) ** 2 for x in recent) / len(recent)) ** 0.5
    return {'mean_hour': round(mean, 2), 'std_dev': round(std, 2),
            'samples': len(recent),
            'human': _hour_float_to_str(mean)}

def _hour_float_to_str(h):
    hour = int(h)
    minute = int((h - hour) * 60)
    ampm = 'am' if hour < 12 else 'pm'
    return f'{hour % 12 or 12}:{minute:02d}{ampm}'

def maybe_preheat_bathroom():
    """Fire bathroom preheat 15 min before predicted wake. Run every minute."""
    global preheat_armed
    if preheat_armed: return
    pred = predict_next(predictions_data['wake_times'])
    if not pred: return
    now_h = datetime.now().hour + datetime.now().minute / 60.0
    target_h = pred['mean_hour'] - 0.25  # 15 min before
    if abs(now_h - target_h) < 1/60:  # within 1 minute
        logger.info(f'PREDICT: preheat bathroom for wake at {pred["human"]}')
        ha_call('switch', 'turn_on', {'entity_id': 'switch.bathroom_heater'})
        preheat_armed = True
        ha_notify('🛁 Bathroom Preheat', f'Heating now — predicted wake at {pred["human"]}')

def reset_preheat_daily():
    global preheat_armed
    preheat_armed = False

# ══════════════════════════════════════════════════════════════════════════════
# SLEEP QUALITY SCORING
# ══════════════════════════════════════════════════════════════════════════════

def collect_overnight_sample(entity_id, value):
    """Accumulate overnight readings for morning scoring."""
    if not is_silent_hours(): return
    try:
        val = float(value)
        if 'carbon_dioxide' in entity_id and 'bedroom' in entity_id:
            overnight_co2.append(val)
        elif 'temperature' in entity_id and 'bedroom' in entity_id:
            overnight_temp.append(val)
        elif 'humidity' in entity_id and 'bedroom' in entity_id:
            overnight_humidity.append(val)
    except (ValueError, TypeError): pass

def calculate_sleep_score():
    """Compute A-F sleep score from overnight data. Call at 8am."""
    today = datetime.now().strftime('%Y-%m-%d')
    if today in sleep_scores: return sleep_scores[today]  # already scored
    if not overnight_co2 and not overnight_temp: return None  # no data

    score = 100
    notes = []

    co2_avg  = sum(overnight_co2) / len(overnight_co2) if overnight_co2 else 450
    temp_avg = sum(overnight_temp) / len(overnight_temp) if overnight_temp else 68
    hum_avg  = sum(overnight_humidity) / len(overnight_humidity) if overnight_humidity else 45

    # CO2 penalties (optimal: <600)
    if co2_avg > 1200:  score -= 30; notes.append(f'Very high CO2 ({int(co2_avg)}ppm)')
    elif co2_avg > 900: score -= 15; notes.append(f'Elevated CO2 ({int(co2_avg)}ppm)')
    elif co2_avg > 700: score -= 5;  notes.append(f'Slightly high CO2 ({int(co2_avg)}ppm)')

    # Temperature penalties (optimal: 65-68°F)
    if temp_avg > 74:   score -= 20; notes.append(f'Too warm ({temp_avg:.1f}°F)')
    elif temp_avg > 71: score -= 10; notes.append(f'Warm ({temp_avg:.1f}°F)')
    elif temp_avg < 62: score -= 10; notes.append(f'Too cold ({temp_avg:.1f}°F)')

    # Humidity penalties (optimal: 40-55%)
    if hum_avg > 65:    score -= 10; notes.append(f'High humidity ({hum_avg:.0f}%)')
    elif hum_avg < 30:  score -= 10; notes.append(f'Dry air ({hum_avg:.0f}%)')

    # Sleep disruptions
    today_disruptions = len([d for d in sleep_disruptions
                              if d.get('time', '')[:10] == today])
    if today_disruptions > 3: score -= 10; notes.append(f'{today_disruptions} sleep disruptions')

    score = max(0, min(100, score))
    grade = 'A' if score >= 90 else 'B' if score >= 80 else 'C' if score >= 70 else 'D' if score >= 60 else 'F'

    result = {
        'date': today, 'score': score, 'grade': grade,
        'co2_avg': round(co2_avg, 0) if overnight_co2 else None,
        'temp_avg': round(temp_avg, 1) if overnight_temp else None,
        'humidity_avg': round(hum_avg, 0) if overnight_humidity else None,
        'disruptions': today_disruptions,
        'notes': notes,
        'calculated_at': datetime.now().isoformat(),
    }
    sleep_scores[today] = result
    save_sleep_scores()
    overnight_co2.clear(); overnight_temp.clear(); overnight_humidity.clear()
    logger.info(f'SLEEP: {today} score={score} grade={grade} notes={notes}')
    return result

# ══════════════════════════════════════════════════════════════════════════════
# ENERGY DASHBOARD
# ══════════════════════════════════════════════════════════════════════════════

def get_energy_snapshot():
    """Fetch current Powercalc sensor states, compute cost."""
    readings = {}
    total_w = 0.0
    for sensor in POWERCALC_SENSORS:
        s = ha_get(f'/states/{sensor}')
        if s:
            try:
                w = float(s['state'])
                name = sensor.replace('sensor.', '').replace('_power', '')
                readings[name] = {'watts': round(w, 1)}
                total_w += w
            except (ValueError, TypeError): pass

    daily_kwh = {}
    for sensor in POWERCALC_SENSORS:
        kwh_sensor = sensor.replace('_power', '_energy')
        s = ha_get(f'/states/{kwh_sensor}')
        if s:
            try:
                kwh = float(s['state'])
                name = sensor.replace('sensor.', '').replace('_power', '')
                daily_kwh[name] = round(kwh, 3)
            except (ValueError, TypeError): pass

    total_kwh_today = sum(daily_kwh.values())
    cost_today = round(total_kwh_today * KWH_RATE, 2)

    # Anomaly: fridge at 0W = possible outage
    # Smart detection: require N consecutive zeros + skip if sensor name is miscategorized
    # or cross-validated by fridge door/temp sensor. Fixes 2026-04-29 false-positive spam.
    fridge = readings.get('refrigerator', {}).get('watts', -1)
    fridge_anomaly = None
    if fridge == 0.0:
        # Cross-check: is the fridge ACTUALLY dead?
        fridge_state = ha_get('/states/sensor.refrigerator_power')
        friendly_name = (fridge_state or {}).get('attributes', {}).get('friendly_name', '')
        # Known-bad sensor: friendly_name doesn't match what it claims to be (mislabeled)
        if 'heater' in friendly_name.lower() or 'refrigerator' not in friendly_name.lower():
            # Sensor is stale/mislabeled — suppress alert, just note once/hour
            global _fridge_noise_logged
            if not globals().get('_fridge_noise_logged') or (time.time() - globals().get('_fridge_noise_logged', 0)) > 3600:
                logger.warning(f'Fridge sensor appears mislabeled (friendly_name="{friendly_name}") — suppressing power_zero anomaly')
                _fridge_noise_logged = time.time()
        else:
            # Require 3 consecutive 0W readings spanning >= 10 min before flagging
            global _fridge_zero_run
            _fridge_zero_run = globals().get('_fridge_zero_run', {'count': 0, 'first_seen': None})
            if _fridge_zero_run['first_seen'] is None:
                _fridge_zero_run = {'count': 1, 'first_seen': time.time()}
            else:
                _fridge_zero_run['count'] += 1
            elapsed = time.time() - _fridge_zero_run['first_seen']
            if _fridge_zero_run['count'] >= 3 and elapsed >= 600:
                fridge_anomaly = {'type': 'power_zero', 'entity': 'sensor.refrigerator_power',
                                  'message': f'Refrigerator showing 0W for {int(elapsed/60)}min — possible power outage',
                                  'time': datetime.now().isoformat(), 'severity': 'high',
                                  'consecutive_zeros': _fridge_zero_run['count']}
                _anom_append(fridge_anomaly)
    else:
        # Reset zero-run counter when fridge reports normal power
        _fridge_zero_run = {'count': 0, 'first_seen': None}

    # v5: Energy anomaly detection — compare each device to 7-day rolling average.
    # When a device uses >= 40% more energy than its rolling baseline, flag for review.
    try:
        energy_anomalies = []
        recent_days = sorted(stats_db.items())[-7:]  # [(date_str, day_stats), ...]
        if len(recent_days) >= 3:
            # Build per-device baseline from prior days (exclude today)
            for dev_name, today_kwh in daily_kwh.items():
                prev_kwhs = []
                for _date, day_stats in recent_days[:-1]:  # prior days only
                    prev_kwh = (day_stats.get('daily_kwh_by_device') or {}).get(dev_name)
                    if prev_kwh is not None and prev_kwh > 0:
                        prev_kwhs.append(prev_kwh)
                if len(prev_kwhs) >= 3:
                    baseline = sum(prev_kwhs) / len(prev_kwhs)
                    if baseline > 0.1:  # ignore noise-level devices
                        ratio = today_kwh / baseline if baseline else 0
                        if ratio >= 1.4:  # 40%+ over baseline
                            msg = f'{dev_name} using {today_kwh:.2f} kWh today vs {baseline:.2f} baseline ({int((ratio-1)*100)}% over)'
                            ea = {'type': 'energy_spike', 'entity': f'sensor.{dev_name}_power',
                                  'message': msg, 'severity': 'medium' if ratio < 2.0 else 'high',
                                  'today_kwh': today_kwh, 'baseline_kwh': round(baseline, 2),
                                  'ratio': round(ratio, 2),
                                  'time': datetime.now().isoformat()}
                            energy_anomalies.append(ea)
                            _anom_append(ea)
    except Exception as e:
        logger.warning(f'energy anomaly detection failed: {e}')

    return {
        'current_watts': round(total_w, 1),
        'devices': readings,
        'daily_kwh': daily_kwh,
        'total_kwh_today': round(total_kwh_today, 3),
        'cost_today_usd': cost_today,
        'rate_per_kwh': KWH_RATE,
        'fridge_anomaly': fridge_anomaly,
        'energy_anomalies': energy_anomalies if 'energy_anomalies' in locals() else [],
        'timestamp': datetime.now().isoformat(),
    }

def get_weekly_energy_cost():
    recent = sorted(stats_db.items())[-7:]
    total = sum(v.get('total_kwh', 0) for _, v in recent)
    return {'estimated_weekly_kwh': round(total, 2),
            'estimated_weekly_cost_usd': round(total * KWH_RATE, 2),
            'days_in_estimate': len(recent)}

# ══════════════════════════════════════════════════════════════════════════════
# WEATHER-REACTIVE AUTOMATION
# ══════════════════════════════════════════════════════════════════════════════

def react_to_weather(new_weather, old_weather):
    """Execute weather-reactive automations when weather changes."""
    if new_weather == old_weather: return

    why_parts = [f'weather changed {old_weather}→{new_weather}', f'time={datetime.now().strftime("%H:%M")}']
    actions_taken = []

    if new_weather in ('rainy', 'pouring', 'lightning', 'lightning-rainy'):
        ha_call('cover', 'close_cover', {'entity_id': 'cover.dining_blinds'})
        weather_reactive_state['blinds_closed'] = True
        actions_taken.append({'action': 'close_blinds', 'reason': 'Rain/storm detected'})

        svc_post('/dj/play', {'mood': 'rainy'})
        weather_reactive_state['dj_mood'] = 'rainy'
        actions_taken.append({'action': 'dj_rainy_mood', 'reason': 'Cozy rain atmosphere'})

        svc_post('/hue/ambient/candlelight')
        weather_reactive_state['hue_scene'] = 'candlelight'
        actions_taken.append({'action': 'hue_candlelight', 'reason': 'Warm cozy lighting for rain'})

        why = f'Rainy weather: closed dining blinds + DJ rainy mood + Hue candlelight [{"→".join(why_parts)}]'

    elif new_weather in ('sunny', 'clear-night') and old_weather in ('rainy', 'cloudy'):
        if weather_reactive_state.get('blinds_closed'):
            ha_call('cover', 'open_cover', {'entity_id': 'cover.dining_blinds'})
            weather_reactive_state['blinds_closed'] = False
            actions_taken.append({'action': 'open_blinds', 'reason': 'Sun returned'})
        svc_post('/hue/ambient/sunset')
        weather_reactive_state['hue_scene'] = 'sunset'
        actions_taken.append({'action': 'hue_sunset', 'reason': 'Sunny day lighting'})
        why = f'Sun returned: opened blinds + Hue sunset [{"→".join(why_parts)}]'

    elif new_weather in ('windy', 'exceptional'):
        ha_call('cover', 'close_cover', {'entity_id': 'cover.dining_blinds'})
        weather_reactive_state['blinds_closed'] = True
        actions_taken.append({'action': 'close_blinds', 'reason': 'Windy conditions'})
        why = f'Windy: closed dining blinds [{"→".join(why_parts)}]'
    else:
        why = f'Weather changed to {new_weather} — no specific reaction [{"→".join(why_parts)}]'

    weather_reactive_state['current_weather'] = new_weather
    weather_reactive_state['last_reaction'] = datetime.now().isoformat()
    if actions_taken:
        log_decision('weather_reactive', actions_taken, source='event_bus', why=why)
    logger.info(f'WEATHER: {why}')

# ══════════════════════════════════════════════════════════════════════════════
# ANOMALY DETECTION v2
# ══════════════════════════════════════════════════════════════════════════════

def _anom_append(alert: dict):
    """v5: Wrap anomaly append to also persist to SQLite with dedup."""
    anomalies_v2.append(alert)
    try:
        storage.record_anomaly(
            entity_id=alert.get('entity', '?'),
            atype=alert.get('type', 'unknown'),
            severity=alert.get('severity', 'medium'),
            message=alert.get('message', '')[:500],
            dedup_window_sec=CONFIG.anomaly_dedup_sec,
        )
    except Exception as e:
        logger.warning(f'storage.record_anomaly failed: {e}')

def check_anomalies_v2(ev):
    """Cross-entity anomaly checks beyond per-entity rate limiting."""
    eid   = ev.get('entity_id', '')
    new   = ev.get('new_state', '')
    now   = datetime.now().isoformat()

    # Door open + no presence = security alert
    if 'door' in eid and new in ('open', 'unlocked'):
        presence = states_map.get('binary_sensor.iphone_presence', 'unknown')
        if presence == 'off':
            alert = {'type': 'security', 'entity': eid, 'state': new,
                     'message': f'{eid} is {new} but nobody home',
                     'time': now, 'severity': 'high'}
            _anom_append(alert)
            ha_notify('🚨 Security Alert', alert['message'])
            logger.warning(f'ANOMALY v2: {alert["message"]}')
            return alert

    # Temperature delta >10°F between rooms = HVAC issue
    if 'temperature' in eid:
        try:
            room_temps = {}
            for r_sensor, r_label in [
                ('sensor.bedroom_co2_monitor_temperature', 'bedroom'),
                ('sensor.living_room_co2_monitor_temperature', 'living_room'),
            ]:
                s = states_map.get(r_sensor)
                if s:
                    try: room_temps[r_label] = float(s)
                    except: pass
            if len(room_temps) >= 2:
                temps = list(room_temps.values())
                delta = max(temps) - min(temps)
                if delta > 10:
                    alert = {'type': 'hvac', 'delta_f': round(delta, 1),
                             'rooms': room_temps,
                             'message': f'Temperature delta {delta:.1f}°F between rooms — HVAC issue?',
                             'time': now, 'severity': 'medium'}
                    # Deduplicate: only alert once per hour
                    recent = [a for a in anomalies_v2
                              if a.get('type') == 'hvac' and
                              (datetime.now() - datetime.fromisoformat(a['time'])).seconds < 3600]
                    if not recent:
                        _anom_append(alert)
                        logger.warning(f'ANOMALY v2: {alert["message"]}')
        except (ValueError, TypeError): pass

    return None

# ══════════════════════════════════════════════════════════════════════════════
# CALENDAR INTEGRATION
# ══════════════════════════════════════════════════════════════════════════════

def get_today_calendar_events():
    """Fetch today's events from all HA calendar entities."""
    now_utc = datetime.now(timezone.utc)
    today    = now_utc.strftime('%Y-%m-%d')
    tomorrow = (now_utc + timedelta(days=1)).strftime('%Y-%m-%d')

    all_states  = ha_get('/states') or []
    cal_entities = [s['entity_id'] for s in all_states
                    if s['entity_id'].startswith('calendar.')]

    events_out = []
    for cal in cal_entities[:5]:
        data = ha_get(f'/calendars/{cal}?start={today}T00:00:00Z&end={tomorrow}T23:59:59Z')
        if isinstance(data, list):
            for e in data:
                events_out.append({
                    'calendar': cal,
                    'summary': e.get('summary', ''),
                    'start': e.get('start', {}).get('dateTime') or e.get('start', {}).get('date', ''),
                    'end':   e.get('end', {}).get('dateTime') or e.get('end', {}).get('date', ''),
                    'all_day': 'dateTime' not in e.get('start', {}),
                })

    events_out.sort(key=lambda x: x['start'])
    return events_out

def check_upcoming_meeting():
    """If a meeting starts in 20-30 min, trigger focus mode ONCE per meeting.
    Also sends push notification so Ben knows focus mode was activated."""
    global _focus_mode_fired
    _focus_mode_fired = globals().get('_focus_mode_fired', set())
    events_list = get_today_calendar_events()
    now = datetime.now(timezone.utc)
    # Garbage-collect fired set every day so we don't accumulate stale ids
    today_key = now.strftime('%Y-%m-%d')
    if getattr(check_upcoming_meeting, '_gc_day', None) != today_key:
        _focus_mode_fired.clear()
        check_upcoming_meeting._gc_day = today_key
    for ev in events_list:
        start_str = ev.get('start', '')
        if not start_str or ev.get('all_day'): continue
        ev_key = f"{ev.get('summary','?')}|{start_str}"
        if ev_key in _focus_mode_fired: continue  # already handled this meeting
        try:
            start_dt  = datetime.fromisoformat(start_str.replace('Z', '+00:00'))
            delta_min = (start_dt - now).total_seconds() / 60
            if 20 <= delta_min <= 30:
                logger.info(f'CALENDAR: Meeting in {delta_min:.0f}min: {ev["summary"]} — activating focus mode')
                svc_post('/dj/play', {'mood': 'focus'})
                svc_post('/hue/ambient/ocean')
                svc_post('/dj/volume', {'level': 8})
                log_decision('focus_mode', [
                    {'action': 'dj_focus',  'reason': f'Meeting in {delta_min:.0f}min'},
                    {'action': 'hue_ocean', 'reason': 'Focus lighting'},
                ], source='calendar',
                why=f'Focus mode: "{ev["summary"]}" starts in {delta_min:.0f}min — lower volume + ocean lighting')
                notify_mobile(
                    title='🎯 Focus mode activated',
                    msg=f'{ev["summary"]} in {delta_min:.0f}min. Lights dimmed, volume down.',
                    priority='active',
                )
                _focus_mode_fired.add(ev_key)
                return ev
        except (ValueError, TypeError): pass
    return None

# ══════════════════════════════════════════════════════════════════════════════
# EVENT BUS MODULE
# ══════════════════════════════════════════════════════════════════════════════

def classify_event(ev):
    eid  = ev.get('entity_id', '')
    hour = datetime.now().hour
    dom  = eid.split('.')[0] if eid else ''
    if is_silent_hours() and 'bedroom' in eid:
        if dom in ('media_player', 'tts', 'notify') or 'speaker' in eid or 'echo' in eid:
            return 'sleep_disrupting'
    if 'presence' in eid or 'lock' in eid: return 'critical'
    hist  = entity_hour_histogram[eid]
    total = sum(hist.values())
    if total > 50:
        hour_pct = hist.get(str(hour), 0) / total
        if hour_pct < 0.02: return 'unusual'
    return 'routine'

def detect_event_anomaly(ev):
    eid  = ev.get('entity_id', '')
    hour = datetime.now().hour
    hist  = entity_hour_histogram[eid]
    total = sum(hist.values())
    if total > 100:
        hour_pct = hist.get(str(hour), 0) / total
        if hour_pct < 0.01:
            anomaly = {'entity_id': eid, 'hour': hour,
                       'expected_pct': round(hour_pct * 100, 2),
                       'time': datetime.now().isoformat()}
            anomalies_bus.append(anomaly)
            return anomaly
    return None

def check_rate_limit(ev):
    eid = ev.get('entity_id', '')
    now = time.time()
    rate_window[eid] = [t for t in rate_window[eid] if now - t < 60]
    rate_window[eid].append(now)
    if len(rate_window[eid]) > RATE_LIMIT:
        alert = {'entity_id': eid, 'events_per_min': len(rate_window[eid]),
                 'time': datetime.now().isoformat(),
                 'message': f'{eid} firing {len(rate_window[eid])} events/min — possible malfunction',
                 'type': 'rate_limit', 'severity': 'medium'}
        recent = [a for a in rate_alerts
                  if a['entity_id'] == eid and
                  (now - datetime.fromisoformat(a['time']).timestamp()) < 300]
        if not recent:
            rate_alerts.append(alert)
            _anom_append(alert)
            logger.warning(f'RATE LIMIT: {alert["message"]}')
        return alert
    return None

def detect_pattern(ev):
    eid = ev.get('entity_id', '')
    now = time.time()
    recent_sequence.append({'entity_id': eid, 'time': now})
    if len(recent_sequence) >= 2:
        prev = recent_sequence[-2]
        gap  = now - prev['time']
        if gap < 60 and prev['entity_id'] != eid:
            key = f"{prev['entity_id']}->{eid}"
            if key not in patterns:
                patterns[key] = {'count': 0, 'last_seen': '', 'avg_gap': 0, 'learned': False}
            p = patterns[key]
            p['count'] += 1
            p['last_seen'] = datetime.now().isoformat()
            p['avg_gap'] = round((p['avg_gap'] * (p['count'] - 1) + gap) / p['count'], 1)
            if p['count'] >= 7 and not p['learned']:
                p['learned'] = True
                logger.info(f'PATTERN: {key} ({p["count"]} times, avg {p["avg_gap"]}s)')
    if counts.get('total', 0) % 100 == 0:
        save_patterns()

def route_event(ev):
    """Core event routing: classify, detect anomalies, update shared state.
    v5: also persists event to SQLite."""
    global last_bedroom_motion_time
    eid = ev.get('entity_id', '')
    new = ev.get('new_state', '')
    old = ev.get('old_state', '')

    # Track bedroom motion
    if 'bedroom' in eid and 'motion' in eid and new == 'on':
        last_bedroom_motion_time = time.time()

    # Room presence engine
    update_room_presence(eid, new)

    # Keep states_map current
    states_map[eid] = new

    ev['classification'] = classify_event(ev)
    entity_hour_histogram[eid][str(datetime.now().hour)] += 1
    detect_event_anomaly(ev)
    check_rate_limit(ev)
    check_anomalies_v2(ev)
    detect_pattern(ev)

    # v5: persist event to SQLite store
    try:
        storage.record_event(entity_id=eid, old_state=old, new_state=new,
                             classification=ev.get('classification', 'unknown'))
    except Exception as e:
        logger.warning(f'storage.record_event failed: {e}')

    # Overnight sleep data collection
    collect_overnight_sample(eid, new)

    # Significance routing
    sig = False; reason = ''
    if eid == 'binary_sensor.iphone_presence' and old != new:
        sig = True; reason = f'Presence: {old}→{new}'
        if new == 'on':
            record_arrive_event()
            threading.Thread(target=arrive_sequence, daemon=True).start()
        else:
            record_depart_event()
            threading.Thread(target=depart_sequence, daemon=True).start()
    elif eid == 'lock.front_door_lock' and old != new:
        sig = True; reason = f'Lock: {old}→{new}'
    elif 'motion' in eid and new == 'on' and old != 'on':
        sig = True; reason = f'Motion: {eid}'
        # First bedroom motion after 4am = wake event
        if 'bedroom' in eid and 4 <= datetime.now().hour < 12:
            record_wake_event()
    elif eid == 'media_player.living_room' and old != new:
        sig = True; reason = f'Music: {old}→{new}'
    elif ('75_the_frame' in eid or 'frame' in eid) and 'media_player' in eid and old != new:
        sig = True; reason = f'TV: {old}→{new}'
    elif eid == 'weather.forecast_home' and old != new:
        sig = True; reason = f'Weather: {old}→{new}'
        react_to_weather(new, old)
        weather_reactive_state['current_weather'] = new
    elif 'vacuum' in eid and old != new:
        sig = True; reason = f'Vacuum: {old}→{new}'
    elif eid == 'sun.sun' and old != new:
        sig = True; reason = f'Sun: {old}→{new}'
    elif 'temperature' in eid:
        try:
            if abs(float(new) - float(old)) > 3:
                sig = True; reason = f'Temp: {eid} {old}→{new}'
        except: pass

    if sig:
        ev['significant'] = True; ev['reason'] = reason
        counts['significant'] += 1
        logger.info(f'SIG [{ev["classification"]}]: {reason}')
        intel_handle_event(ev)
        analytics_handle_event(ev)

# ─── WebSocket thread ──────────────────────────────────────────────────────────

def ws_thread():
    global ws_ok, ws_id
    def on_msg(ws_app, msg):
        global ws_id
        d = json.loads(msg)
        t = d.get('type', '')
        if t == 'auth_required':
            ws_app.send(json.dumps({'type': 'auth', 'access_token': HA_TOKEN}))
        elif t == 'auth_ok':
            logger.info('WS authenticated')
            ws_app.send(json.dumps({'id': ws_id, 'type': 'subscribe_events', 'event_type': 'state_changed'}))
            ws_id += 1
        elif t == 'event':
            ed  = d.get('event', {}).get('data', {})
            eid = ed.get('entity_id', '')
            ns  = ed.get('new_state') or {}
            os_ = ed.get('old_state') or {}
            dom = eid.split('.')[0] if eid else ''
            if dom not in WATCHED_DOMAINS: return
            nv = ns.get('state', '')
            ov = os_.get('state', '')
            if nv == ov: return
            ev = {'entity_id': eid, 'domain': dom, 'old_state': ov, 'new_state': nv,
                  'time': datetime.now().isoformat(), 'significant': False, 'classification': 'routine'}
            events.append(ev); counts[dom] += 1; counts['total'] += 1
            route_event(ev)
    def on_err(ws_app, e):    global ws_ok; ws_ok = False; logger.error(f'WS: {e}')
    def on_close(ws_app, c, m): global ws_ok; ws_ok = False
    def on_open(ws_app):      global ws_ok; ws_ok = True; logger.info('WS connected')
    while True:
        try:
            ws_app = websocket.WebSocketApp(WS_URL, on_message=on_msg, on_error=on_err,
                                             on_close=on_close, on_open=on_open)
            ws_app.run_forever(ping_interval=30, ping_timeout=10)
        except: pass
        logger.info('WS reconnecting in 10s...')
        time.sleep(10)

# ══════════════════════════════════════════════════════════════════════════════
# INTELLIGENCE MODULE
# ══════════════════════════════════════════════════════════════════════════════

def is_cooper_here():
    if cooper_override is not None: return cooper_override
    now = datetime.now()
    hour = now.hour * 100 + now.minute
    days_map = {'mon': 0, 'tue': 1, 'wed': 2, 'thu': 3, 'fri': 4, 'sat': 5, 'sun': 6}
    dow = now.weekday()
    day = now.strftime('%a').lower()
    for block in COOPER_SCHED.split(','):
        if '-' not in block: continue
        parts = block.strip().split('-')
        if len(parts) != 2: continue
        sp = parts[0].split('_')
        ep = parts[1].split('_') if '_' in parts[1] else [day, parts[1]]
        if len(sp) == 2 and len(ep) == 2:
            sd = days_map.get(sp[0], 99); st = int(sp[1])
            ed = days_map.get(ep[0], 99); et = int(ep[1])
            if sd <= dow <= ed:
                if sd == ed:
                    if st <= hour <= et: return True
                elif dow == sd and hour >= st: return True
                elif dow == ed and hour <= et: return True
                elif sd < dow < ed: return True
    return False

def notify_mobile(title, msg, priority='active'):
    """Send iPhone push via HA notify. Lightweight, no Telegram dependency."""
    try:
        http.post(f'{HA_URL}/api/services/notify/mobile_app_bks_home_assistant_chatsworth',
                  headers={'Authorization': f'Bearer {HA_TOKEN}', 'Content-Type': 'application/json'},
                  json={'title': title, 'message': msg,
                        'data': {'push': {'interruption-level': priority}}}, timeout=5)
    except Exception as e:
        logger.warning(f'notify_mobile failed: {e}')

def on_mode_change(old, new):
    if new == 'cooper_day' and old != 'cooper_day':
        svc_post('/dj/play', {'playlist': 'kids'})
        log_decision('mode_change', [{'action': 'kids_music', 'reason': 'Cooper arrived/morning'}],
                     source='mode_machine',
                     why=f'Activated kids music — Cooper day mode started from {old}')
    elif new == 'cooper_night':
        svc_post('/dj/volume', {'level': 12})
        log_decision('mode_change', [{'action': 'volume_down', 'reason': 'Cooper bedtime'}],
                     source='mode_machine',
                     why='Lowered volume to 12% — Cooper night mode')
    elif old in ('cooper_day', 'cooper_night') and 'cooper' not in new:
        svc_post('/dj/play', {'playlist': 'kids_off'})
        log_decision('mode_change', [{'action': 'kids_off', 'reason': 'Cooper gone'}],
                     source='mode_machine',
                     why=f'Kids music off — Cooper left, transitioning to {new}')
    elif new == 'morning' and old == 'night':
        if is_bedroom_safe():
            svc_post('/dj/play', {'mood': 'morning'})
            log_decision('mode_change', [{'action': 'morning_music', 'reason': 'Bedroom motion confirmed'}],
                         source='mode_machine',
                         why=f'Morning music started — bedroom motion confirmed at {datetime.now().strftime("%H:%M")}')
        else:
            logger.info('MODE: morning but no bedroom motion — staying quiet')

    # P1 feature (2026-04-29): Mode transitions → iPhone push for observability.
    # Quiet transitions (cooper_day/night, morning normal, working) use passive priority.
    # Interesting ones (arrive/depart/cooper arrive) use active.
    transition_priority = {
        ('unknown', 'away'): None,        # startup noise — skip
        ('night', 'morning'): 'passive',
        ('morning', 'working'): 'passive',
        ('working', 'evening'): 'passive',
        ('evening', 'night'): 'passive',
    }
    pri = transition_priority.get((old, new), 'active')
    if pri and old != 'unknown':
        icon = {
            'away': '🚪', 'morning': '☀️', 'working': '💻', 'evening': '🌆',
            'night': '🌙', 'cooper_day': '👦', 'cooper_night': '💤',
        }.get(new, '🏠')
        notify_mobile(
            title=f'{icon} TARS: {new.replace("_"," ")}',
            msg=f'Mode transition {old} → {new} at {datetime.now().strftime("%H:%M")}',
            priority=pri,
        )

def update_mode():
    global home_mode
    now = datetime.now(); h = now.hour
    p = ha_get('/states/binary_sensor.iphone_presence')
    is_home = p['state'] == 'on' if p else True
    cooper  = is_cooper_here()
    old_mode = home_mode

    if not is_home:            home_mode = 'away'
    elif cooper and h < 8:     home_mode = 'cooper_night'
    elif cooper:               home_mode = 'cooper_day'
    elif h < 6:                home_mode = 'night'
    elif h < 9:                home_mode = 'morning'
    elif h < 17:               home_mode = 'working'
    elif h < 21:               home_mode = 'evening'
    else:                      home_mode = 'night'

    if old_mode != home_mode:
        mode_history.append({'from': old_mode, 'to': home_mode, 'time': datetime.now().isoformat()})
        logger.info(f'MODE: {old_mode} → {home_mode}')
        # v5: persist mode transition to SQLite
        try:
            # Calculate duration in previous mode
            prev_dur = None
            if len(mode_history) >= 2:
                try:
                    t_from = datetime.fromisoformat(mode_history[-2]['time'])
                    t_to   = datetime.fromisoformat(mode_history[-1]['time'])
                    prev_dur = int((t_to - t_from).total_seconds())
                except Exception:
                    pass
            storage.record_mode(old_mode=old_mode, new_mode=home_mode,
                                reason='time_based_update',
                                prev_duration_sec=prev_dur)
        except Exception as e:
            logger.warning(f'storage.record_mode failed: {e}')
        on_mode_change(old_mode, home_mode)
        save_intelligence()
    return home_mode

def intel_handle_event(ev):
    """Intelligence reactions to significant events."""
    global last_event_time
    last_event_time = datetime.now().isoformat()
    eid = ev.get('entity_id', ''); new = ev.get('new_state', ''); old = ev.get('old_state', '')

    # TV on → movie mode
    if ('75_the_frame' in eid or 'frame' in eid) and 'media_player' in eid:
        if new in ('on', 'playing') and not is_silent_hours():
            svc_post('/hue/ambient/movie')
            svc_post('/dj/play', {'mood': 'chill'})
            log_decision('event', [{'action': 'movie_mode', 'reason': 'TV turned on'}],
                         source='event_bus',
                         why=f'Movie mode: TV on at {datetime.now().strftime("%H:%M")}, mode={home_mode}')
    # Sun below horizon
    elif eid == 'sun.sun' and new == 'below_horizon':
        svc_post('/hue/ambient/sunset')
        log_decision('event', [{'action': 'sunset_lighting', 'reason': 'Sun set'}],
                     source='event_bus', why='Sunset — transitioning to evening lighting')
    # Vacuum done
    elif 'vacuum' in eid and new in ('docked', 'standby') and old in ('cleaning', 'returning'):
        ha_notify('🧹 Clean Complete', 'Vacuum finished and docked.')
    # Night motion
    elif 'motion' in eid and new == 'on' and home_mode == 'night':
        event_driven_actions.append({'time': datetime.now().isoformat(),
                                     'event': eid, 'action': 'night_motion_noted'})

def build_context():
    ctx = {}
    now = datetime.now()
    ctx['time'] = {
        'hour': now.hour, 'minute': now.minute,
        'day': now.strftime('%A'), 'weekend': now.weekday() >= 5,
        'period': ('night' if now.hour < 6 else 'morning_early' if now.hour < 9 else
                   'morning_late' if now.hour < 12 else 'afternoon' if now.hour < 17 else
                   'evening' if now.hour < 21 else 'night'),
        'mode': home_mode,
    }
    p = ha_get('/states/binary_sensor.iphone_presence')
    ctx['presence'] = {'home': p['state'] == 'on' if p else None}
    ctx['cooper']   = {'here': is_cooper_here(), 'schedule_based': cooper_override is None}
    w = ha_get('/states/weather.forecast_home')
    ctx['weather']  = {'state': w['state'] if w else 'unknown',
                       'temp': w['attributes'].get('temperature') if w else None}
    ctx['climate']  = {}
    for room, sensor_prefix in [
        ('Bedroom', 'bedroom_co2_monitor'), ('Living Room', 'living_room_co2_monitor')
    ]:
        t = ha_get(f'/states/sensor.{sensor_prefix}_temperature')
        h = ha_get(f'/states/sensor.{sensor_prefix}_humidity')
        c = ha_get(f'/states/sensor.{sensor_prefix}_carbon_dioxide')
        ctx['climate'][room] = {
            'temp':     float(t['state']) if t else None,
            'humidity': float(h['state']) if h else None,
            'co2':      float(c['state']) if c else None,
        }
    sleep_today = sleep_scores.get(now.strftime('%Y-%m-%d'))
    yesterday   = (now - timedelta(days=1)).strftime('%Y-%m-%d')
    sleep_last  = sleep_today or sleep_scores.get(yesterday)
    ctx['sleep'] = {'grade': sleep_last['grade'] if sleep_last else '?',
                    'score': sleep_last['score'] if sleep_last else 0}
    vac  = ha_get('/states/vacuum.robovac')
    ctx['vacuum'] = {'state': vac['state'] if vac else '?'}
    lock = ha_get('/states/lock.front_door_lock')
    ctx['lock']   = {'state': lock['state'] if lock else '?'}
    music = svc_get('/dj/now-playing')
    ctx['music']  = music or {}
    return ctx

def decide(ctx):
    decisions = []
    period    = ctx.get('time', {}).get('period', 'unknown')
    home      = ctx.get('presence', {}).get('home', False)
    cooper    = ctx.get('cooper', {}).get('here', False)
    weather   = ctx.get('weather', {}).get('state', 'unknown')
    sleep_score = ctx.get('sleep', {}).get('score', 100)

    if home and period == 'evening' and sleep_score < 70:
        decisions.append({'action': 'music_mood', 'value': 'chill',
                          'reason': f'Low sleep score ({sleep_score}) + evening'})
        decisions.append({'action': 'hue_ambient', 'value': 'candlelight',
                          'reason': 'Warm lighting for recovery'})
    if home and weather in ('rainy', 'pouring'):
        decisions.append({'action': 'music_mood', 'value': 'rainy', 'reason': 'Rain detected'})
        decisions.append({'action': 'hue_ambient', 'value': 'candlelight', 'reason': 'Cozy rain'})
    if cooper:
        decisions.append({'action': 'spotify_kids', 'value': True, 'reason': 'Cooper visiting'})
        decisions.append({'action': 'skip_vacuum', 'value': True, 'reason': 'No vacuum with Cooper'})
    bedroom_co2  = ctx.get('climate', {}).get('Bedroom', {}).get('co2', 400)
    outdoor_temp = ctx.get('weather', {}).get('temp', 70)
    if bedroom_co2 and bedroom_co2 > 800 and outdoor_temp and 60 <= outdoor_temp <= 80:
        decisions.append({'action': 'open_windows', 'value': True,
                          'reason': f'CO2 {bedroom_co2}ppm + outdoor {outdoor_temp}°F'})
    if period == 'night' and home and not cooper:
        decisions.append({'action': 'music_mood', 'value': 'sleep', 'reason': 'Night wind-down'})
    if period == 'morning_early' and ctx.get('time', {}).get('weekend'):
        decisions.append({'action': 'music_mood', 'value': 'morning_coffee', 'reason': 'Weekend morning'})

    decisions = [d for d in decisions
                 if d['action'] not in adaptive_rules.get('suppressed_actions', set())]
    return decisions

def execute_decisions(decisions):
    results = []
    for d in decisions:
        a = d['action']; v = d.get('value')
        try:
            if a == 'music_mood':    r = svc_post('/dj/play', {'mood': v})
            elif a == 'hue_ambient': r = svc_post(f'/hue/ambient/{v}')
            elif a == 'spotify_kids': r = svc_post('/dj/play', {'playlist': 'kids'})
            elif a == 'open_windows': ha_notify('🌬 Fresh Air', d['reason']); r = True
            else: r = None
            results.append({**d, 'executed': bool(r)})
        except:
            results.append({**d, 'executed': False})
    return results

def arrive_sequence():
    ctx = build_context()
    # v5.1: auto-disarm ALWAYS happens on arrival (even in silent hours) for safety
    try:
        r = svc_post('/alarm/disarm/chatsworth')
        if r and r.get('success'):
            logger.info('🔓 Auto-disarmed Chatsworth alarm on arrival')
    except Exception as e:
        logger.error(f'auto_disarm on arrival: {e}')
    if is_silent_hours() and not is_bedroom_safe():
        safe_notify('🏠 Welcome Home', 'Arrived (quiet mode, alarm off)')
        log_decision('arrival', [
            {'action': 'silent_welcome', 'reason': 'silent hours'},
            {'action': 'alarm_disarm', 'reason': 'Always disarm on arrival'},
        ], source='event_bus',
        why=f'Silent arrival at {datetime.now().strftime("%H:%M")} — silent hours active, no bedroom motion, alarm disarmed')
        return
    decisions = decide(ctx)
    results   = execute_decisions(decisions)
    results.append({'action': 'alarm_disarm', 'reason': 'Arrival — auto-disarm'})
    update_mode()
    log_decision('arrival', results, source='event_bus', why=_build_why('arrival', results))
    # v5: arrival-based focus mode — if a meeting is starting in ≤30 min, auto-activate focus mode on arrival
    try: check_arrival_focus_mode()
    except Exception as e: logger.error(f'arrival_focus_mode: {e}')

def check_arrival_focus_mode():
    """v5: When you arrive home and have a meeting in ≤30 min, auto-activate focus mode.
    Avoids the 20-30 min window used by time-based focus mode; arrival allows a wider 0-30 min window."""
    events_list = get_today_calendar_events()
    now = datetime.now(timezone.utc)
    for ev in events_list:
        start_str = ev.get('start', '')
        if not start_str or ev.get('all_day'): continue
        try:
            start_dt = datetime.fromisoformat(start_str.replace('Z', '+00:00'))
            delta_min = (start_dt - now).total_seconds() / 60
            if 0 < delta_min <= 30:
                logger.info(f'ARRIVAL_FOCUS: Meeting in {delta_min:.0f}min: {ev["summary"]} — activating focus mode immediately')
                SVC.post('/dj/play', {'mood': 'focus'})
                SVC.post('/hue/ambient/ocean')
                SVC.post('/dj/volume', {'level': 8})
                log_decision('arrival_focus_mode', [
                    {'action': 'dj_focus',  'reason': f'Arrived with meeting in {delta_min:.0f}min'},
                    {'action': 'hue_ocean', 'reason': 'Focus lighting on arrival'},
                ], source='arrival',
                why=f'Arrival focus: "{ev["summary"]}" starts in {delta_min:.0f}min — welcome back, focus mode engaged')
                HA.notify_mobile(
                    title='🎯 Welcome back — focus mode on',
                    message=f'{ev["summary"]} in {delta_min:.0f}min. Lights dimmed, volume down.',
                    priority='active',
                )
                return ev
        except (ValueError, TypeError): pass
    return None

def depart_sequence():
    decisions = []
    if not is_cooper_here():
        if not is_bedroom_safe() or datetime.now().hour >= 9:
            svc_post('/vacuum/start')
            decisions.append({'action': 'vacuum_start', 'reason': 'Departed, no one home, after 9am'})
        else:
            decisions.append({'action': 'vacuum_deferred', 'reason': 'Before 9am — deferring vacuum'})
    else:
        decisions.append({'action': 'vacuum_skip', 'reason': 'Cooper here'})
    svc_post('/dj/play', {'mood': 'off'})
    decisions.append({'action': 'music_stop', 'reason': 'Departure'})
    # v5.1: auto-arm alarm on departure (Chatsworth only; skip if Cooper is here)
    if not is_cooper_here():
        try:
            r = svc_post('/alarm/arm/away/chatsworth')
            if r and r.get('success'):
                decisions.append({'action': 'alarm_arm_away', 'reason': 'Departure — no one home'})
            else:
                decisions.append({'action': 'alarm_arm_failed', 'reason': f'Ring API: {r}'})
        except Exception as e:
            decisions.append({'action': 'alarm_arm_error', 'reason': str(e)})
    else:
        decisions.append({'action': 'alarm_skip_arm', 'reason': 'Cooper is here — not arming'})
    update_mode()
    log_decision('departure', decisions, source='event_bus', why=_build_why('departure', decisions))

def mode_updater_thread():
    while True:
        update_mode()
        maybe_preheat_bathroom()
        check_upcoming_meeting()
        refresh_room_occupancy()
        sonos_follow_sweep()
        time.sleep(60)

def pattern_save_thread():
    while True:
        time.sleep(300)
        save_patterns()

# ══════════════════════════════════════════════════════════════════════════════
# ANALYTICS MODULE
# ══════════════════════════════════════════════════════════════════════════════

def analytics_handle_event(ev):
    global departure_count, arrival_count
    eid = ev.get('entity_id', ''); new = ev.get('new_state', ''); old = ev.get('old_state', '')
    classification = ev.get('classification', 'routine')

    if classification == 'sleep_disrupting':
        sleep_disruptions.append({'time': datetime.now().isoformat(),
                                   'entity_id': eid, 'old': old, 'new': new})

    if 'iphone_presence' in eid:
        if new == 'off' and old == 'on': departure_count += 1
        elif new == 'on' and old == 'off': arrival_count += 1
    elif 'motion' in eid and new == 'on':
        motion_counts[eid] += 1
    elif 'temperature' in eid:
        try:
            val = float(new)
            temp_readings[eid].append(val)
            temp_readings[eid] = temp_readings[eid][-288:]
        except (ValueError, TypeError): pass
    elif 'vacuum' in eid and new in ('docked', 'standby') and old == 'cleaning':
        today = datetime.now().strftime('%Y-%m-%d')
        stats_db.setdefault(today, {}).setdefault('vacuum_sessions', 0)
        stats_db[today]['vacuum_sessions'] += 1
        save_stats_db()
    elif 'weather' in eid and ev.get('significant'):
        today = datetime.now().strftime('%Y-%m-%d')
        stats_db.setdefault(today, {}).setdefault('weather_states', [])
        stats_db[today]['weather_states'].append({'time': datetime.now().isoformat(), 'state': new})
        save_stats_db()

def flush_today_stats():
    today  = datetime.now().strftime('%Y-%m-%d')
    energy = get_energy_snapshot()
    stats_db[today] = {
        'date': today,
        'presence': {'arrivals': arrival_count, 'departures': departure_count},
        'motion': dict(motion_counts),
        'temperature_avg': {k: round(sum(v) / len(v), 1) for k, v in temp_readings.items() if v},
        'total_kwh': energy['total_kwh_today'],
        'energy_cost_usd': energy['cost_today_usd'],
        'daily_kwh_by_device': dict(energy.get('daily_kwh', {})),  # v5: per-device for baseline
        'dj_plays': dict(dj_plays), 'dj_skips': dict(dj_skips),
        'sleep_disruptions': len([d for d in sleep_disruptions if d.get('time', '')[:10] == today]),
        'flushed_at': datetime.now().isoformat(),
    }
    save_stats_db()

def daily_flush_thread():
    last_day = datetime.now().strftime('%Y-%m-%d')
    while True:
        time.sleep(900)
        flush_today_stats()
        today = datetime.now().strftime('%Y-%m-%d')
        if today != last_day:
            if datetime.now().hour == 8:
                calculate_sleep_score()
            reset_preheat_daily()
            last_day = today

# ══════════════════════════════════════════════════════════════════════════════
# FLASK ROUTES — EVENT BUS
# ══════════════════════════════════════════════════════════════════════════════

@app.route('/events/stream')
def events_stream():
    def gen():
        idx = len(events)
        last_keepalive = time.time()
        while True:
            cur = len(events)
            if cur > idx:
                for e in list(events)[idx:]: yield f'data: {json.dumps(e)}\n\n'
                idx = cur
                last_keepalive = time.time()
            # v5: send keepalive every 15s to prevent proxy timeouts
            if time.time() - last_keepalive > 15:
                yield ': keepalive\n\n'
                last_keepalive = time.time()
            time.sleep(0.5)
    return Response(gen(), mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache',
                             'X-Accel-Buffering': 'no'})

@app.route('/events/recent')
def events_recent():
    lim      = request.args.get('limit', 50, type=int)
    sig_only = request.args.get('significant', 'false').lower() == 'true'
    result   = [e for e in events if e.get('significant')] if sig_only else list(events)
    return jsonify(result[-lim:])

@app.route('/events/stats')
def events_stats():
    return jsonify({
        'total': counts.get('total', 0), 'significant': counts.get('significant', 0),
        'by_domain': dict(counts), 'entities': len(states_map), 'connected': ws_ok,
        'patterns_total': len(patterns),
        'patterns_learned': len([p for p in patterns.values() if p.get('learned')]),
        'anomalies_bus': len(anomalies_bus), 'anomalies_v2': len(anomalies_v2),
    })

@app.route('/bedroom-motion-age')
def bedroom_motion_age():
    if last_bedroom_motion_time is None:
        return jsonify({'age_seconds': None, 'last_motion': None, 'message': 'No bedroom motion tracked yet'})
    age = round(time.time() - last_bedroom_motion_time)
    return jsonify({'age_seconds': age,
                    'last_motion': datetime.fromtimestamp(last_bedroom_motion_time).isoformat(),
                    'recent': age < 1800})

@app.route('/patterns')
def get_patterns():
    learned = [{'sequence': k, **v} for k, v in patterns.items() if v.get('learned')]
    all_p   = sorted([{'sequence': k, **v} for k, v in patterns.items()],
                     key=lambda x: x['count'], reverse=True)
    return jsonify({'learned': learned, 'top_50': all_p[:50], 'total': len(patterns)})

@app.route('/anomalies')
def get_anomalies():
    all_anomalies = (
        [{'source': 'bus', **a} for a in list(anomalies_bus)[-20:]] +
        [{'source': 'v2',  **a} for a in list(anomalies_v2)[-20:]]
    )
    all_anomalies.sort(key=lambda x: x.get('time', ''), reverse=True)
    return jsonify({'anomalies': all_anomalies[:30], 'rate_alerts': list(rate_alerts)[-10:]})

# ══════════════════════════════════════════════════════════════════════════════
# FLASK ROUTES — INTELLIGENCE
# ══════════════════════════════════════════════════════════════════════════════

@app.route('/api')
def index():
    return jsonify({
        'name': 'TARS Core', 'version': '5.0.0', 'mode': home_mode,
        'cooper_here': is_cooper_here(), 'ws_connected': ws_ok,
        'last_event': last_event_time,
        'decisions_today': len([d for d in decision_log
                                 if d.get('time', '')[:10] == datetime.now().strftime('%Y-%m-%d')]),
    })

@app.route('/health')
def health():
    svcs = {}
    for name, path in [('dj', '/dj/health'), ('hue', '/hue/health'),
                        ('switchbot', '/switchbot/health'), ('vacuum', '/vacuum/health'),
                        ('alarm', '/alarm/health')]:
        r = svc_get(path)
        svcs[name] = 'ok' if r and r.get('status') == 'ok' else 'unreachable'
    return jsonify({
        'status': 'ok', 'version': '5.1.0', 'mode': home_mode,
        'ws_connected': ws_ok, 'cooper_here': is_cooper_here(),
        'services': svcs,
    })

@app.route('/context')
def context():
    return jsonify(build_context())

@app.route('/decide')
def decide_endpoint():
    ctx = build_context(); decisions = decide(ctx)
    return jsonify({'context': {'period': ctx['time']['period'], 'mode': home_mode,
                                'home': ctx['presence']['home'],
                                'cooper': ctx['cooper']['here'],
                                'weather': ctx['weather']['state'],
                                'sleep': ctx['sleep']},
                    'decisions': decisions})

@app.route('/arrive', methods=['POST', 'GET'])
def arrive():
    ctx = build_context(); decisions = decide(ctx); results = execute_decisions(decisions)
    entry = log_decision('arrival', results, source='manual', why=_build_why('arrival', results))
    return jsonify(entry)

@app.route('/depart', methods=['POST', 'GET'])
def depart():
    decisions = []
    if not is_cooper_here():
        svc_post('/vacuum/start')
        decisions.append({'action': 'vacuum_start', 'reason': 'Departed', 'executed': True})
    svc_post('/dj/play', {'mood': 'off'})
    decisions.append({'action': 'music_stop', 'reason': 'Departure', 'executed': True})
    update_mode()
    entry = log_decision('departure', decisions, source='manual', why=_build_why('departure', decisions))
    return jsonify(entry)

@app.route('/mood/<mood>', methods=['POST', 'GET'])
def set_mood(mood):
    mood_map = {
        'chill':     {'music': 'chill',         'hue': 'candlelight', 'vol': 12},
        'energetic': {'music': 'energetic',      'hue': 'neon',        'vol': 20},
        'focus':     {'music': 'focus',          'hue': 'ocean',       'vol': 8},
        'party':     {'music': 'party',          'hue': 'neon',        'vol': 25},
        'sleep':     {'music': 'sleep',          'hue': 'candlelight', 'vol': 8},
        'romantic':  {'music': 'romantic',       'hue': 'sunset',      'vol': 10},
        'movie':     {'music': None,             'hue': 'movie',       'vol': None},
        'rainy':     {'music': 'rainy',          'hue': 'candlelight', 'vol': 10},
        'morning':   {'music': 'morning_coffee', 'hue': None,          'vol': 12},
    }
    if mood not in mood_map:
        return jsonify({'error': f'Available: {list(mood_map.keys())}'}), 400
    m = mood_map[mood]; results = []
    if m['music']:         r = svc_post('/dj/play', {'mood': m['music']}); results.append({'action': 'music', 'mood': m['music'], 'ok': bool(r)})
    if m['hue'] == 'movie': r = svc_post('/hue/ambient/movie'); results.append({'action': 'hue', 'mode': 'movie', 'ok': bool(r)})
    elif m['hue']:         r = svc_post(f'/hue/ambient/{m["hue"]}'); results.append({'action': 'hue', 'preset': m['hue'], 'ok': bool(r)})
    if m['vol']:           r = svc_post('/dj/volume', {'level': m['vol']}); results.append({'action': 'volume', 'level': m['vol'], 'ok': bool(r)})
    entry = log_decision('mood', results, source='manual',
                         why=f'Mood "{mood}" set at {datetime.now().strftime("%H:%M")} — {" + ".join(k["action"] for k in results)}')
    return jsonify(entry)

@app.route('/mode')
def get_mode():
    return jsonify({'current': home_mode, 'history': list(mode_history)[-10:],
                    'cooper_here': is_cooper_here(), 'updated': last_event_time})

@app.route('/learned')
def get_learned():
    return jsonify({
        'adaptive_rules': {k: v for k, v in adaptive_rules.items() if k != 'suppressed_actions'},
        'suppressed': list(adaptive_rules.get('suppressed_actions', [])),
        'event_driven_actions': list(event_driven_actions)[-20:],
        'suggestion_feedback': {
            'accepted': len([s for s in suggestion_feedback.values() if s['status'] == 'accepted']),
            'dismissed': len([s for s in suggestion_feedback.values() if s['status'] == 'dismissed']),
        },
    })

@app.route('/cooper')
def cooper_status():
    return jsonify({'here': is_cooper_here(), 'override': cooper_override, 'schedule': COOPER_SCHED})

@app.route('/cooper/here', methods=['POST', 'GET'])
def cooper_here():
    global cooper_override
    cooper_override = True
    svc_post('/dj/play', {'playlist': 'kids'})
    ha_notify('👦 Cooper Mode', 'Kids music on, vacuum disabled')
    log_decision('cooper', [{'action': 'kids_mode_on', 'reason': 'Manual trigger'}],
                 source='manual', why='Cooper here manually confirmed — kids music + vacuum disabled')
    return jsonify({'cooper': 'here', 'kids_mode': True})

@app.route('/cooper/gone', methods=['POST', 'GET'])
def cooper_gone():
    global cooper_override
    cooper_override = False
    svc_post('/dj/play', {'playlist': 'kids_off'})
    ha_notify('👦 Cooper Left', 'Normal mode restored')
    log_decision('cooper', [{'action': 'kids_mode_off', 'reason': 'Manual trigger'}],
                 source='manual', why='Cooper gone — kids music off, normal mode restored')
    return jsonify({'cooper': 'gone', 'kids_mode': False})

@app.route('/insights')
def insights():
    ctx = build_context(); tips = []
    if ctx.get('sleep', {}).get('score', 100) < 70:
        tips.append({'type': 'sleep', 'tip': 'Sleep score was low. Consider earlier bedtime or air purifier.'})
    co2 = ctx.get('climate', {}).get('Bedroom', {}).get('co2', 400)
    if co2 and co2 > 600:
        tips.append({'type': 'air', 'tip': f'Bedroom CO2 is {int(co2)}ppm. Open windows before bed.'})
    if not tips: tips.append({'type': 'all_good', 'tip': 'Everything looks great.'})
    return jsonify({'insights': tips, 'mode': home_mode, 'period': ctx['time']['period']})

@app.route('/log')
def get_log():
    limit = request.args.get('limit', 20, type=int)
    fmt = request.args.get('format', 'json')
    hours = request.args.get('hours', type=int)
    items = list(decision_log)
    # Optional: filter to last N hours
    if hours:
        cutoff = datetime.now() - timedelta(hours=hours)
        filtered = []
        for it in items:
            try:
                dt = datetime.fromisoformat(it.get('time', ''))
                if dt >= cutoff: filtered.append(it)
            except (ValueError, TypeError):
                pass
        items = filtered
    items = items[-limit:]

    if fmt == 'digest':
        # Human-readable daily summary (P1 added 2026-04-29)
        counts = Counter(it.get('kind', '?') for it in items)
        sources = Counter(it.get('source', '?') for it in items)
        # Condense per-kind into readable lines
        lines = []
        window_desc = f'last {hours}h' if hours else f'last {limit} decisions'
        lines.append(f"TARS Decision Digest ({window_desc}):")
        lines.append(f'  Total: {len(items)} decisions')
        if counts:
            lines.append('  By kind:')
            for kind, n in counts.most_common(8):
                lines.append(f'    • {kind}: {n}x')
        if sources:
            lines.append('  Triggered by:')
            for src, n in sources.most_common(5):
                lines.append(f'    • {src}: {n}x')
        # Top 5 most recent 'why' explanations
        recent_whys = [it.get('why', '') for it in items[-5:] if it.get('why')]
        if recent_whys:
            lines.append('  Recent explanations:')
            for w in recent_whys:
                lines.append(f'    • {w}')
        return jsonify({'digest': '\n'.join(lines), 'raw_count': len(items),
                        'by_kind': dict(counts), 'by_source': dict(sources)})
    return jsonify(items)

@app.route('/proactive')
def proactive_check():
    suggestions = []; now = datetime.now()
    # CO2
    s = ha_get('/states/sensor.bedroom_co2_monitor_carbon_dioxide')
    if s:
        try:
            co2 = float(s['state'])
            if co2 > 1000:
                w = ha_get('/states/weather.forecast_home')
                temp = w['attributes'].get('temperature', 0) if w else 0
                if 60 <= temp <= 80:
                    suggestions.append({'id': str(uuid.uuid4())[:8], 'type': 'air_quality',
                                        'message': f'CO2 is {int(co2)}ppm. Outdoor {temp}°F — open windows!',
                                        'priority': 'medium'})
        except (ValueError, TypeError): pass
    # Golden hour
    sun = ha_get('/states/sun.sun')
    if sun:
        elev = sun.get('attributes', {}).get('elevation', 90)
        if 0 < elev < 10 and home_mode not in ('away', 'night'):
            w = ha_get('/states/weather.forecast_home')
            if w and w.get('state') in ('sunny', 'partlycloudy'):
                suggestions.append({'id': str(uuid.uuid4())[:8], 'type': 'golden_hour',
                                    'message': 'Golden hour! Perfect for a walk.', 'priority': 'low'})
    # Sleep follow-up
    yesterday = (now - timedelta(days=1)).strftime('%Y-%m-%d')
    score_data = sleep_scores.get(yesterday)
    if score_data and score_data.get('grade') in ('D', 'F'):
        suggestions.append({'id': str(uuid.uuid4())[:8], 'type': 'sleep_recovery',
                             'message': f'Last night scored {score_data["grade"]} — take it easy today.',
                             'priority': 'medium'})
    # Filter suppressed
    filtered = [s for s in suggestions
                if adaptive_rules.get('auto_actions', {}).get(f'ignored_{s["type"]}', 0) < 5]
    # Store for feedback tracking
    for s in filtered:
        suggestion_feedback[s['id']] = {'suggestion': s, 'status': 'pending', 'time': now.isoformat()}
    return jsonify({'suggestions': filtered, 'mode': home_mode,
                    'bedroom_safe': is_bedroom_safe(), 'silent_hours': is_silent_hours()})

# ─── Room Presence ─────────────────────────────────────────────────────────────

@app.route('/presence')
def get_presence():
    refresh_room_occupancy()
    out = {}
    for room, data in room_presence.items():
        age = round(time.time() - data['last_motion']) if data['last_motion'] else None
        out[room] = {'occupied': data['occupied'],
                     'last_motion': data['last_motion_iso'],
                     'seconds_since_motion': age}
    return jsonify({'rooms': out, 'mode': home_mode,
                    'iphone_home': states_map.get('binary_sensor.iphone_presence') == 'on'})


# Sonos room-follow endpoints (P2 2026-04-29)
@app.route('/sonos/following')
def sonos_following_status():
    """Current room-follow state: which rooms are joined to anchor group."""
    now = time.time()
    out = {}
    for room, info in sonos_following.items():
        out[room] = {
            'entities': info['entities'],
            'joined_sec_ago': int(now - info['joined_at']),
            'motion_sec_ago': int(now - info['last_motion']),
        }
    return jsonify({'following': out, 'anchor': SONOS_ANCHOR,
                    'anchor_playing': _is_anchor_playing(),
                    'idle_timeout_sec': SONOS_FOLLOW_IDLE_SEC})

@app.route('/sonos/follow/<room>/<action>', methods=['POST', 'GET'])
def sonos_follow_manual(room, action):
    """Manual trigger: POST /sonos/follow/bathroom/join or /unjoin."""
    if room not in SONOS_ROOM_MAP:
        return jsonify({'error': f'Unknown room. Available: {list(SONOS_ROOM_MAP.keys())}'}), 400
    if action == 'join':
        ok = _sonos_join(room)
        if ok: sonos_following[room] = {'joined_at': time.time(), 'last_motion': time.time(), 'entities': list(SONOS_ROOM_MAP[room])}
        return jsonify({'success': ok, 'room': room, 'action': 'join'})
    elif action == 'unjoin':
        ok = _sonos_unjoin(room)
        if ok and room in sonos_following: del sonos_following[room]
        return jsonify({'success': ok, 'room': room, 'action': 'unjoin'})
    return jsonify({'error': 'action must be join or unjoin'}), 400

@app.route('/sonos/follow/config', methods=['GET', 'POST'])
def sonos_follow_config():
    """Inspect or tweak follow config (idle timeout, anchor)."""
    global SONOS_FOLLOW_IDLE_SEC
    if request.method == 'POST':
        body = request.json or {}
        if 'idle_sec' in body:
            SONOS_FOLLOW_IDLE_SEC = int(body['idle_sec'])
    return jsonify({
        'anchor': SONOS_ANCHOR,
        'room_map': SONOS_ROOM_MAP,
        'idle_timeout_sec': SONOS_FOLLOW_IDLE_SEC,
    })

# ─── Predictive Scheduling ──────────────────────────────────────────────────────

@app.route('/predictions')
def get_predictions():
    return jsonify({
        'wake':   predict_next(predictions_data['wake_times']),
        'sleep':  predict_next(predictions_data['sleep_times']),
        'arrive': predict_next(predictions_data['arrive_times']),
        'depart': predict_next(predictions_data['depart_times']),
        'preheat_armed': preheat_armed,
        'samples': {k: len(v) for k, v in predictions_data.items()},
    })

# ─── Weather Reactive ────────────────────────────────────────────────────────────

@app.route('/weather/reactive')
def weather_reactive():
    w = ha_get('/states/weather.forecast_home')
    return jsonify({**weather_reactive_state,
                    'ha_weather': w['state'] if w else 'unknown',
                    'blinds_entity': 'cover.dining_blinds'})

# ─── Dashboard ──────────────────────────────────────────────────────────────────

@app.route('/dashboard')
def dashboard():
    """Single endpoint: full home status in one payload."""
    now     = datetime.now()
    today   = now.strftime('%Y-%m-%d')
    yesterday = (now - timedelta(days=1)).strftime('%Y-%m-%d')

    w     = ha_get('/states/weather.forecast_home')
    pres  = ha_get('/states/binary_sensor.iphone_presence')
    lock  = ha_get('/states/lock.front_door_lock')
    music = svc_get('/dj/now-playing')
    energy = get_energy_snapshot()
    refresh_room_occupancy()

    sleep_last = sleep_scores.get(today) or sleep_scores.get(yesterday)

    battery_alerts = []
    for eid, name in [
        ('sensor.front_door_lock_battery', 'Front Door Lock'),
        ('sensor.bedroom_co2_monitor_battery', 'Bedroom CO2 Monitor'),
    ]:
        s = ha_get(f'/states/{eid}')
        if s:
            try:
                b = float(s['state'])
                if b < 20: battery_alerts.append({'entity': eid, 'name': name, 'battery': b})
            except (ValueError, TypeError): pass

    recent_anomalies = sorted(list(anomalies_v2)[-10:],
                               key=lambda x: x.get('time', ''), reverse=True)[:3]

    return jsonify({
        'timestamp': now.isoformat(),
        'mode': home_mode,
        'cooper': {'here': is_cooper_here(), 'schedule': COOPER_SCHED},
        'presence': {
            'iphone_home': pres['state'] == 'on' if pres else None,
            'rooms': {r: {'occupied': d['occupied'], 'last_motion': d['last_motion_iso']}
                      for r, d in room_presence.items()},
        },
        'weather': {'state': w['state'] if w else 'unknown',
                    'temp': w['attributes'].get('temperature') if w else None,
                    'reactive': weather_reactive_state},
        'music': music or {},
        'sleep': sleep_last or {'grade': '?', 'score': 0},
        'energy': {'cost_today_usd': energy['cost_today_usd'],
                   'current_watts': energy['current_watts'],
                   'total_kwh_today': energy['total_kwh_today']},
        'security': {'lock': lock['state'] if lock else 'unknown'},
        # v5.1: alarm status
        'alarm': (lambda: {
            'chatsworth': (ha_get('/states/alarm_control_panel.chatsworth_alarm') or {}).get('state', 'unknown'),
            'vlp': (ha_get('/states/alarm_control_panel.villa_las_palmas_alarm') or {}).get('state', 'unknown'),
        })(),
        'battery_alerts': battery_alerts,
        'anomalies': recent_anomalies,
        'ws_connected': ws_ok,
        'decisions_today': len([d for d in decision_log
                                 if d.get('time', '')[:10] == today]),
    })

# ─── Calendar ───────────────────────────────────────────────────────────────────

@app.route('/calendar/today')
def calendar_today():
    cal_events = get_today_calendar_events()
    now = datetime.now(timezone.utc)
    upcoming = []
    for ev in cal_events:
        start_str = ev.get('start', '')
        if start_str and not ev.get('all_day'):
            try:
                start_dt  = datetime.fromisoformat(start_str.replace('Z', '+00:00'))
                delta_min = (start_dt - now).total_seconds() / 60
                ev['minutes_until'] = round(delta_min)
                if delta_min > -60: upcoming.append(ev)
            except (ValueError, TypeError):
                upcoming.append(ev)
        else:
            upcoming.append(ev)
    return jsonify({'events': upcoming, 'count': len(upcoming),
                    'focus_mode_ready': any(25 <= e.get('minutes_until', 999) <= 35 for e in upcoming)})

# ─── Suggestion Feedback ────────────────────────────────────────────────────────

@app.route('/suggestion/<sid>/dismiss', methods=['POST', 'GET'])
def suggestion_dismiss(sid):
    if sid not in suggestion_feedback:
        return jsonify({'error': 'Unknown suggestion id'}), 404
    s = suggestion_feedback[sid]; s['status'] = 'dismissed'
    s_type = s['suggestion'].get('type', '')
    adaptive_rules['auto_actions'][f'ignored_{s_type}'] = \
        adaptive_rules['auto_actions'].get(f'ignored_{s_type}', 0) + 1
    save_intelligence()
    return jsonify({'id': sid, 'status': 'dismissed', 'type': s_type,
                    'total_ignores': adaptive_rules['auto_actions'][f'ignored_{s_type}']})

@app.route('/suggestion/<sid>/accept', methods=['POST', 'GET'])
def suggestion_accept(sid):
    if sid not in suggestion_feedback:
        return jsonify({'error': 'Unknown suggestion id'}), 404
    s = suggestion_feedback[sid]; s['status'] = 'accepted'
    s_type = s['suggestion'].get('type', '')
    adaptive_rules['auto_actions'][f'ignored_{s_type}'] = 0
    adaptive_rules['auto_actions'][f'accepted_{s_type}'] = \
        adaptive_rules['auto_actions'].get(f'accepted_{s_type}', 0) + 1
    save_intelligence()
    return jsonify({'id': sid, 'status': 'accepted', 'type': s_type,
                    'total_accepted': adaptive_rules['auto_actions'][f'accepted_{s_type}']})

# ══════════════════════════════════════════════════════════════════════════════
# FLASK ROUTES — ANALYTICS
# ══════════════════════════════════════════════════════════════════════════════

@app.route('/analytics/daily')
def analytics_daily():
    days = request.args.get('days', 30, type=int)
    recent = sorted(stats_db.items())[-days:]
    return jsonify({'days': [v for _, v in recent], 'total': len(recent)})

@app.route('/analytics/sleep')
def analytics_sleep():
    now = datetime.now()
    yesterday = (now - timedelta(days=1)).strftime('%Y-%m-%d')
    score = sleep_scores.get(yesterday) or sleep_scores.get(now.strftime('%Y-%m-%d'))
    if not score: return jsonify({'status': 'no_data', 'message': 'No sleep score yet'})
    return jsonify(score)

@app.route('/analytics/sleep/last')
def analytics_sleep_last():
    now = datetime.now()
    for delta in range(3):
        d = (now - timedelta(days=delta)).strftime('%Y-%m-%d')
        if d in sleep_scores: return jsonify(sleep_scores[d])
    return jsonify({'status': 'no_data'}), 404

@app.route('/analytics/sleep/trend')
def analytics_sleep_trend():
    days = request.args.get('days', 14, type=int)
    recent = sorted(sleep_scores.items())[-days:]
    if not recent: return jsonify({'status': 'no_data'})
    grades = [v['grade'] for _, v in recent]
    scores = [v['score'] for _, v in recent]
    return jsonify({
        'days': [{'date': k, 'grade': v['grade'], 'score': v['score'],
                  'co2_avg': v.get('co2_avg'), 'temp_avg': v.get('temp_avg')} for k, v in recent],
        'avg_score': round(sum(scores) / len(scores), 1),
        'grade_distribution': {g: grades.count(g) for g in set(grades)},
    })

@app.route('/analytics/energy')
def analytics_energy():
    return jsonify(get_energy_snapshot())

@app.route('/analytics/energy/cost')
def analytics_energy_cost():
    snapshot = get_energy_snapshot()
    weekly   = get_weekly_energy_cost()
    monthly_kwh = sum(v.get('total_kwh', 0) for v in list(stats_db.values())[-30:])
    return jsonify({
        'today':   {'kwh': snapshot['total_kwh_today'], 'usd': snapshot['cost_today_usd']},
        'weekly':  weekly,
        'monthly_estimate': {'kwh': round(monthly_kwh, 2),
                              'usd': round(monthly_kwh * KWH_RATE, 2)},
        'rate_per_kwh': KWH_RATE,
        'devices': snapshot['devices'],
        'timestamp': snapshot['timestamp'],
    })

@app.route('/analytics/trends')
def analytics_trends():
    monthly = defaultdict(lambda: {'arrivals': 0, 'departures': 0, 'vacuum_sessions': 0,
                                    'days': 0, 'total_kwh': 0.0})
    for day, data in stats_db.items():
        m = monthly[day[:7]]
        m['days'] += 1
        p = data.get('presence', {})
        m['arrivals']        += p.get('arrivals', 0)
        m['departures']      += p.get('departures', 0)
        m['vacuum_sessions'] += data.get('vacuum_sessions', 0)
        m['total_kwh']       += data.get('total_kwh', 0)
    return jsonify({'months': [{'month': k, **v} for k, v in sorted(monthly.items())]})

@app.route('/analytics/health')
def analytics_health():
    all_states = ha_get('/states') or []
    unavailable = [
        s['attributes'].get('friendly_name', s['entity_id'])
        for s in all_states
        if s['state'] == 'unavailable' and not any(
            skip in s['entity_id']
            for skip in ['bks_macbook', 'clawdbot', 'dryer', 'unnamed', 'playroom_sonos']
        )
    ]
    automations = [s for s in all_states
                   if s['entity_id'].startswith('automation.') and s['state'] == 'on']
    stale = [s['attributes'].get('friendly_name') for s in automations
             if s['attributes'].get('last_triggered') is None]
    return jsonify({'unavailable_count': len(unavailable), 'unavailable': unavailable[:20],
                    'stale_automations': stale, 'total_automations': len(automations)})

# ══════════════════════════════════════════════════════════════════════════════
# STARTUP
# ══════════════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════════
# v5 MODULE: Storage + History + Config endpoints
# ═══════════════════════════════════════════════════════════════════════════

@app.route('/config')
def get_config():
    """Return typed config (secrets redacted)."""
    return jsonify({
        'config': CONFIG.redact(),
        'validation_errors': _cfg_errors,
        'valid': len(_cfg_errors) == 0,
    })

@app.route('/storage/stats')
def storage_stats_endpoint():
    """SQLite row counts + file sizes."""
    try:
        return jsonify(storage.storage_stats())
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/storage/prune', methods=['POST', 'GET'])
def storage_prune_endpoint():
    """Manual prune trigger. Uses retention from config."""
    try:
        stats = storage.prune_old(
            events_days=CONFIG.events_retention_days,
            decisions_days=CONFIG.decisions_retention_days,
            anomalies_days=CONFIG.anomalies_retention_days,
            modes_days=CONFIG.modes_retention_days,
        )
        return jsonify({'success': True, 'pruned': stats})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/history/events')
def history_events():
    """Query SQLite event store.
    Params: ?hours=<n>&entity=<eid>&domain=<d>&limit=<n>"""
    try:
        hours = request.args.get('hours', 24, type=int)
        since_ms = int((time.time() - hours * 3600) * 1000)
        rows = storage.query_events(
            since_ms=since_ms,
            entity=request.args.get('entity'),
            domain=request.args.get('domain'),
            limit=request.args.get('limit', 100, type=int),
        )
        return jsonify({'count': len(rows), 'events': rows, 'hours': hours})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/history/decisions')
def history_decisions():
    """Query SQLite decision log.
    Params: ?hours=<n>&kind=<k>&source=<s>&limit=<n>"""
    try:
        rows = storage.query_decisions(
            hours=request.args.get('hours', 24, type=int),
            kind=request.args.get('kind'),
            source=request.args.get('source'),
            limit=request.args.get('limit', 100, type=int),
        )
        return jsonify({'count': len(rows), 'decisions': rows})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/history/anomalies')
def history_anomalies():
    """Query SQLite anomaly history.
    Params: ?days=<n>&entity=<eid>&unresolved=1&limit=<n>"""
    try:
        rows = storage.query_anomalies(
            entity=request.args.get('entity'),
            days=request.args.get('days', 7, type=int),
            unresolved_only=request.args.get('unresolved') in ('1', 'true'),
            limit=request.args.get('limit', 100, type=int),
        )
        return jsonify({'count': len(rows), 'anomalies': rows})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/history/anomalies/rate/<path:entity>')
def history_anomaly_rate(entity):
    """Rate of anomalies for a specific entity over the last N hours."""
    try:
        hours = request.args.get('hours', 24, type=int)
        return jsonify({'entity': entity, **storage.anomaly_rate(entity, hours=hours)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/history/modes')
def history_modes():
    """Query SQLite mode transition history."""
    try:
        rows = storage.query_modes(
            days=request.args.get('days', 7, type=int),
            limit=request.args.get('limit', 100, type=int),
        )
        return jsonify({'count': len(rows), 'modes': rows})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/dashboard.html')
@app.route('/')  # NEW: HTML dashboard at root
def html_dashboard():
    """Render HTML dashboard. Reuses dashboard() JSON builder."""
    try:
        # Build context via the existing JSON dashboard function
        ctx_response = dashboard()
        # Unwrap Flask jsonify response
        import json as _json
        ctx = _json.loads(ctx_response.get_data(as_text=True))
        ctx['version'] = '5.0.0'
        return dashboard_module.render_dashboard(ctx), 200, {'Content-Type': 'text/html; charset=utf-8'}
    except Exception as e:
        logger.error(f'html_dashboard failed: {e}')
        return f'<pre>TARS dashboard error: {e}</pre>', 500

@app.route('/dashboard.json')
def dashboard_json_alias():
    """Preserved JSON dashboard at explicit path (root is now HTML)."""
    return dashboard()

# ==============================================================================
# STARTUP
# ==============================================================================

def _storage_prune_thread():
    """Run storage.prune_old() once per 24h."""
    while True:
        time.sleep(24 * 3600)
        try:
            stats = storage.prune_old(
                events_days=CONFIG.events_retention_days,
                decisions_days=CONFIG.decisions_retention_days,
                anomalies_days=CONFIG.anomalies_retention_days,
                modes_days=CONFIG.modes_retention_days,
            )
            logger.info(f'Daily storage prune: {stats}')
        except Exception as e:
            logger.error(f'Daily storage prune failed: {e}')

if __name__ == '__main__':
    logger.info(f'TARS Core v5.1.0 starting on :{API_PORT}')
    if _cfg_errors:
        logger.error(f'Config validation errors: {_cfg_errors}')
        # Don't fail startup — log and continue so /config endpoint is reachable for debugging
    logger.info(f'Config (redacted): {CONFIG.redact()}')
    os.makedirs('/data', exist_ok=True)
    # v5: init SQLite stores
    try:
        storage.init_storage()
        logger.info(f'Storage initialized: {storage.storage_stats()}')
    except Exception as e:
        logger.error(f'Storage init failed: {e}')
    load_all_data()
    threading.Thread(target=ws_thread,           daemon=True).start()
    threading.Thread(target=mode_updater_thread, daemon=True).start()
    threading.Thread(target=daily_flush_thread,  daemon=True).start()
    threading.Thread(target=pattern_save_thread, daemon=True).start()
    # v5: daily storage prune (runs once at startup + every 24h)
    threading.Thread(target=_storage_prune_thread, daemon=True).start()
    logger.info('Threads: WS listener, mode machine, daily flush, pattern saver, storage prune')
    app.run(host='0.0.0.0', port=API_PORT, debug=False)
