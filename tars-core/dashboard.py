"""TARS Core v5.0.0 — Embedded HTML dashboard.

Server-side rendered HTML with auto-refresh. No JS frameworks.
Pulls live state from Core's own endpoints and storage module.
"""
from datetime import datetime

# Import storage, supporting both package and direct-script execution
try:
    from . import storage
except ImportError:
    import storage


DASHBOARD_HTML = '''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>🜂 TARS Dashboard</title>
<meta http-equiv="refresh" content="15">
<style>
:root {
  --bg: #0a0e14;
  --bg-card: #1a1f28;
  --bg-card-hover: #222836;
  --fg: #e6e6e6;
  --fg-dim: #8a8a8a;
  --accent: #7dd3fc;
  --green: #84cc16;
  --amber: #f59e0b;
  --red: #ef4444;
  --purple: #a78bfa;
  --border: #2a2f3a;
}
* { box-sizing: border-box; }
body {
  font-family: -apple-system, BlinkMacSystemFont, "SF Pro Text", sans-serif;
  background: var(--bg); color: var(--fg); margin: 0; padding: 16px;
  font-size: 14px; line-height: 1.5;
}
.header {
  display: flex; justify-content: space-between; align-items: baseline;
  margin-bottom: 20px; padding-bottom: 12px; border-bottom: 1px solid var(--border);
}
.header h1 { margin: 0; font-size: 22px; font-weight: 600; }
.header .ts { color: var(--fg-dim); font-size: 12px; font-variant-numeric: tabular-nums; }
.grid {
  display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
  gap: 14px;
}
.card {
  background: var(--bg-card); border-radius: 10px; padding: 14px;
  border: 1px solid var(--border);
}
.card h2 {
  margin: 0 0 10px 0; font-size: 13px; font-weight: 600;
  color: var(--fg-dim); text-transform: uppercase; letter-spacing: 0.05em;
}
.kv { display: flex; justify-content: space-between; padding: 4px 0; }
.kv .k { color: var(--fg-dim); }
.kv .v { font-variant-numeric: tabular-nums; }
.badge {
  display: inline-block; padding: 2px 8px; border-radius: 12px;
  font-size: 11px; font-weight: 600; text-transform: uppercase;
}
.badge.ok { background: rgba(132,204,22,0.15); color: var(--green); }
.badge.warn { background: rgba(245,158,11,0.15); color: var(--amber); }
.badge.err { background: rgba(239,68,68,0.15); color: var(--red); }
.badge.info { background: rgba(125,211,252,0.15); color: var(--accent); }
.row {
  padding: 6px 0; border-top: 1px solid var(--border);
  display: grid; grid-template-columns: 80px 1fr auto; gap: 10px; align-items: center;
}
.row:first-child { border-top: 0; }
.row .label { color: var(--fg-dim); font-size: 12px; font-variant-numeric: tabular-nums; }
.room-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
.room {
  padding: 10px; background: rgba(255,255,255,0.03); border-radius: 8px;
  display: flex; flex-direction: column; gap: 4px;
}
.room .name { font-weight: 500; }
.room.occupied { background: rgba(132,204,22,0.08); border: 1px solid rgba(132,204,22,0.2); }
.room .age { color: var(--fg-dim); font-size: 11px; }
.music {
  display: flex; gap: 12px; align-items: center;
}
.music .art {
  width: 56px; height: 56px; border-radius: 6px; background: #333;
  flex-shrink: 0; background-size: cover; background-position: center;
}
.music .meta { flex: 1; min-width: 0; }
.music .title { font-weight: 500; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.music .artist { color: var(--fg-dim); font-size: 13px; }
.decision {
  padding: 10px 0; border-top: 1px solid var(--border);
}
.decision:first-child { border-top: 0; padding-top: 0; }
.decision .kind { font-weight: 500; color: var(--accent); font-size: 13px; }
.decision .why { color: var(--fg-dim); font-size: 12px; margin-top: 3px; }
.decision .meta { color: var(--fg-dim); font-size: 11px; font-variant-numeric: tabular-nums; margin-top: 3px; }
a { color: var(--accent); text-decoration: none; }
a:hover { text-decoration: underline; }
.empty { color: var(--fg-dim); font-style: italic; padding: 8px 0; }
.spark { display: flex; gap: 2px; align-items: flex-end; height: 30px; }
.spark .bar { flex: 1; background: var(--accent); border-radius: 2px 2px 0 0; min-height: 2px; }
.footer { margin-top: 20px; text-align: center; color: var(--fg-dim); font-size: 11px; }
</style>
</head>
<body>
<div class="header">
  <h1>🜂 TARS</h1>
  <div class="ts">v__VERSION__ · rendered __TIMESTAMP__</div>
</div>
<div class="grid">

<div class="card">
  <h2>System</h2>
  <div class="kv"><span class="k">Mode</span><span class="v"><span class="badge info">__MODE__</span></span></div>
  <div class="kv"><span class="k">Cooper here</span><span class="v">__COOPER__</span></div>
  <div class="kv"><span class="k">iPhone home</span><span class="v">__IPHONE__</span></div>
  <div class="kv"><span class="k">WebSocket</span><span class="v">__WS__</span></div>
</div>

<div class="card">
  <h2>🛡️ Alarm</h2>
  __ALARM__
</div>

<div class="card">
  <h2>Services</h2>
  __SERVICES__
</div>

<div class="card">
  <h2>🎵 Music</h2>
  __MUSIC__
</div>

<div class="card">
  <h2>🏠 Presence</h2>
  <div class="room-grid">
    __ROOMS__
  </div>
</div>

<div class="card">
  <h2>⚡ Energy (today)</h2>
  <div class="kv"><span class="k">Current</span><span class="v">__WATTS__ W</span></div>
  <div class="kv"><span class="k">kWh today</span><span class="v">__KWH__</span></div>
  <div class="kv"><span class="k">Cost today</span><span class="v">$__COST__</span></div>
</div>

<div class="card">
  <h2>😴 Sleep (last night)</h2>
  <div class="kv"><span class="k">Grade</span><span class="v"><span class="badge info">__SLEEP_GRADE__</span></span></div>
  <div class="kv"><span class="k">Score</span><span class="v">__SLEEP_SCORE__ / 100</span></div>
</div>

<div class="card">
  <h2>⚠️ Anomalies</h2>
  __ANOMALIES__
</div>

<div class="card" style="grid-column: span 2;">
  <h2>🧠 Recent Decisions</h2>
  __DECISIONS__
</div>

<div class="card" style="grid-column: span 2;">
  <h2>📊 Storage</h2>
  __STORAGE__
</div>

</div>

<div class="footer">
  TARS Core v5.1.0 · Auto-refresh 15s · Endpoints: <a href="/health">health</a> ·
  <a href="/dashboard">json</a> · <a href="/log?format=digest&hours=24">digest</a>
</div>
</body>
</html>'''


def _fmt_age(seconds: float) -> str:
    if seconds is None: return '—'
    if seconds < 60: return f'{int(seconds)}s ago'
    if seconds < 3600: return f'{int(seconds/60)}m ago'
    if seconds < 86400: return f'{int(seconds/3600)}h ago'
    return f'{int(seconds/86400)}d ago'


def render_dashboard(context: dict) -> str:
    """Render dashboard from structured context. Returns HTML string."""
    h = DASHBOARD_HTML

    # Header
    h = h.replace('__VERSION__', context.get('version', '5.1.0'))
    h = h.replace('__TIMESTAMP__', datetime.now().strftime('%H:%M:%S %Z'))

    # System
    h = h.replace('__MODE__', context.get('mode', '?'))
    h = h.replace('__COOPER__', '✓ yes' if context.get('cooper_here') else '— no')
    h = h.replace('__IPHONE__', '🏠 home' if context.get('iphone_home') else '🚪 away')
    h = h.replace('__WS__', '<span class="badge ok">connected</span>' if context.get('ws_connected') else '<span class="badge err">disconnected</span>')

    # Services
    svcs = context.get('services', {})
    svc_html = ''.join(
        f'<div class="kv"><span class="k">{n}</span><span class="v"><span class="badge {"ok" if s == "ok" else "err"}">{s}</span></span></div>'
        for n, s in svcs.items()
    )
    h = h.replace('__SERVICES__', svc_html or '<div class="empty">No services reporting.</div>')

    # Alarm (v5.1)
    alarm = context.get('alarm', {}) or {}
    alarm_html = ''
    for loc, state in alarm.items():
        if state == 'disarmed':
            badge = 'info'; icon = '🔓'
        elif state in ('armed_home', 'armed_away', 'armed_night'):
            badge = 'ok'; icon = '🔒'
        elif state == 'triggered':
            badge = 'err'; icon = '🚨'
        elif state in ('arming', 'pending'):
            badge = 'warn'; icon = '⏳'
        else:
            badge = 'warn'; icon = '❓'
        pretty_state = state.replace('_', ' ') if state else 'unknown'
        alarm_html += (
            f'<div class="kv"><span class="k">{icon} {loc.capitalize()}</span>'
            f'<span class="v"><span class="badge {badge}">{pretty_state}</span></span></div>'
        )
    if not alarm_html:
        alarm_html = '<div class="empty">No alarm data.</div>'
    h = h.replace('__ALARM__', alarm_html)

    # Music
    music = context.get('music', {}) or {}
    if music.get('state') in ('playing', 'paused', 'idle') and music.get('title'):
        art = music.get('entity_picture', '')
        art_style = f'background-image: url(\'{art}\');' if art else ''
        state_badge = 'ok' if music.get('state') == 'playing' else 'info'
        music_html = f'''
<div class="music">
  <div class="art" style="{art_style}"></div>
  <div class="meta">
    <div class="title">{music.get('title', '?')}</div>
    <div class="artist">{music.get('artist', '—')}</div>
    <div class="artist"><span class="badge {state_badge}">{music.get('state', '?')}</span>
      Vol: {int(float(music.get('volume', 0)) * 100)}%</div>
  </div>
</div>'''
    else:
        music_html = '<div class="empty">Nothing playing</div>'
    h = h.replace('__MUSIC__', music_html)

    # Rooms
    rooms = context.get('presence', {}).get('rooms', {}) or {}
    if rooms:
        rooms_html = ''
        for name, info in sorted(rooms.items()):
            occ = 'occupied' if info.get('occupied') else ''
            lm = info.get('last_motion') or '—'
            age = _fmt_age(info.get('seconds_since_motion'))
            rooms_html += f'''
<div class="room {occ}">
  <div class="name">{name.replace('_', ' ').title()}</div>
  <div class="age">{'🟢 occupied' if occ else '⚪'} · {age}</div>
</div>'''
    else:
        rooms_html = '<div class="empty">No rooms tracked.</div>'
    h = h.replace('__ROOMS__', rooms_html)

    # Energy
    energy = context.get('energy', {}) or {}
    h = h.replace('__WATTS__', f"{energy.get('current_watts', 0):.0f}")
    h = h.replace('__KWH__', f"{energy.get('total_kwh_today', 0):.2f}")
    h = h.replace('__COST__', f"{energy.get('cost_today_usd', 0):.2f}")

    # Sleep
    sleep = context.get('sleep', {}) or {}
    grade = sleep.get('grade', '?')
    score = sleep.get('score', 0)
    h = h.replace('__SLEEP_GRADE__', str(grade))
    h = h.replace('__SLEEP_SCORE__', str(score))

    # Anomalies (from SQLite now)
    try:
        anomalies = storage.query_anomalies(days=2, unresolved_only=True, limit=10)
    except Exception:
        anomalies = []
    if anomalies:
        anom_html = ''
        for a in anomalies:
            sev = a.get('severity', 'info')
            sev_cls = {'high': 'err', 'medium': 'warn', 'low': 'info'}.get(sev, 'info')
            age = _fmt_age((datetime.now().timestamp() * 1000 - a.get('last_seen', 0)) / 1000)
            anom_html += f'''
<div class="row">
  <span class="label">{age}</span>
  <span><span class="badge {sev_cls}">{sev}</span> {a.get('message', a.get('type', '?'))[:80]}</span>
  <span class="label">×{a.get('count', 1)}</span>
</div>'''
    else:
        anom_html = '<div class="empty">✅ No active anomalies.</div>'
    h = h.replace('__ANOMALIES__', anom_html)

    # Decisions
    try:
        decisions = storage.query_decisions(hours=24, limit=10)
    except Exception:
        decisions = []
    if decisions:
        dec_html = ''
        for d in decisions[:10]:
            ts = datetime.fromtimestamp(d.get('ts', 0) / 1000).strftime('%H:%M')
            dec_html += f'''
<div class="decision">
  <div class="kind">{d.get('kind', '?')} <span class="label">via {d.get('source', '?')}</span></div>
  <div class="why">{(d.get('why') or '(no explanation)')[:200]}</div>
  <div class="meta">{ts} · actions: {len(d.get('actions') or [])}</div>
</div>'''
    else:
        dec_html = '<div class="empty">No decisions in last 24h.</div>'
    h = h.replace('__DECISIONS__', dec_html)

    # Storage
    try:
        stats = storage.storage_stats()
    except Exception:
        stats = {}
    if stats:
        storage_html = ''
        for db, s in stats.items():
            storage_html += f'''
<div class="kv"><span class="k">{db}</span>
  <span class="v">{s.get('rows', 0):,} rows · {s.get('size_mb', 0)} MB</span>
</div>'''
    else:
        storage_html = '<div class="empty">Storage not initialized.</div>'
    h = h.replace('__STORAGE__', storage_html)

    return h
