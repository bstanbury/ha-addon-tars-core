# TARS Core v4.0.0

Consolidated Home Assistant add-on. Replaces three separate add-ons (Event Bus v3, Home Intelligence v3, Home Analytics v3) with a single Flask app on port **8093**.

## What's inside

| Module | Origin | Role |
|--------|--------|------|
| Event Bus | ha-addon-event-bus | HA WebSocket → SSE stream, pattern detection |
| Intelligence | ha-addon-home-intelligence | Context engine, decisions, mode state machine |
| Analytics | ha-addon-home-analytics | Daily stats, sleep scoring, energy tracking |

## v4.0 new features

1. **Room Presence Engine** — per-room occupancy from motion sensors. `GET /presence`
2. **Predictive Scheduling** — learns wake/sleep/arrive/depart patterns. `GET /predictions`
3. **Energy Dashboard** — Powercalc aggregation + cost at $0.35/kWh. `GET /analytics/energy/cost`
4. **Sleep Quality Scoring** — A–F grade from overnight CO2/temp/humidity. `GET /analytics/sleep/last`, `/analytics/sleep/trend`
5. **Weather-Reactive Automation** — blinds + DJ + Hue on weather change. `GET /weather/reactive`
6. **Anomaly Detection v2** — rate limits, security alerts, temp delta, power anomalies. `GET /anomalies`
7. **Natural Language Decision Log** — every decision has a human-readable `why`. `GET /log`
8. **Consolidated Dashboard** — full home status in one call. `GET /dashboard`
9. **Calendar Integration** — today's events + pre-meeting focus mode. `GET /calendar/today`
10. **Suggestion Feedback** — adaptive learning from accept/dismiss. `POST /suggestion/<id>/accept|dismiss`

## Route reference

```
# Event Bus
GET /events/stream          SSE stream (for DJ, Hue, external subscribers)
GET /events/recent          Recent events (?significant=true to filter)
GET /events/stats           Counts by domain, pattern stats
GET /bedroom-motion-age     Seconds since last bedroom motion
GET /patterns               Learned event sequences
GET /anomalies              All anomalies (rate + v2 cross-entity)

# Intelligence
GET  /                      Version + current mode
GET  /health                Service health check
GET  /context               Full home context snapshot
GET  /decide                Context + decision recommendations (dry run)
GET  /mode                  Current mode + history
GET  /learned               Adaptive rules + suppressed actions
GET  /cooper                Cooper status + schedule
GET  /insights              Actionable tips
GET  /log                   Decision log with 'why' strings
GET  /proactive             Proactive suggestions (CO2, golden hour, sleep recovery)
GET  /presence              Room-level occupancy
GET  /predictions           Predicted wake/sleep/arrive/depart times
GET  /weather/reactive      Current weather-reactive state
GET  /dashboard             FULL home status — one call
GET  /calendar/today        Today's calendar events + focus mode flag
POST /arrive                Run arrival sequence
POST /depart                Run departure sequence
POST /mood/<mood>           Set mood (chill/focus/party/sleep/rainy/movie/morning/energetic/romantic)
POST /cooper/here           Force Cooper mode on
POST /cooper/gone           Force Cooper mode off
POST /suggestion/<id>/dismiss   Dismiss proactive suggestion
POST /suggestion/<id>/accept    Accept proactive suggestion

# Analytics
GET /analytics/daily        Per-day stats (default 30 days)
GET /analytics/sleep        Last night's sleep data
GET /analytics/sleep/last   Most recent sleep score
GET /analytics/sleep/trend  Sleep trend (default 14 days)
GET /analytics/energy       Current Powercalc snapshot
GET /analytics/energy/cost  Today/weekly/monthly cost breakdown
GET /analytics/trends       Monthly aggregates
GET /analytics/health       HA entity health check
```

## Architecture

```
┌────────────────────────────────────────┐
│           TARS Core :8093              │
│                                        │
│  ┌──────────┐  ┌───────────┐  ┌──────┐│
│  │Event Bus │  │Intelligence│  │Analyt││
│  │  module  ├──►  module   ├──► module││
│  └────┬─────┘  └────┬──────┘  └──────┘│
│       │              │                 │
└───────┼──────────────┼─────────────────┘
        │              │
    WS conn       SERVICES_URL
   (single)      :8097 (DJ/Hue/
   to HA         SwitchBot/Vacuum)
```

## Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `ha_url` | `http://localhost:8123` | Home Assistant URL |
| `ha_token` | *(required)* | Long-lived access token |
| `api_port` | `8093` | Port to listen on |
| `services_url` | `http://localhost:8097` | TARS Services URL (DJ/Hue/etc) |
| `cooper_schedule` | `fri_1600-mon_1100,...` | Cooper presence schedule |

## Data files

| File | Contents |
|------|----------|
| `/data/intelligence_v2.json` | Mode, adaptive rules |
| `/data/patterns.json` | Learned event sequences |
| `/data/stats_db.json` | Daily stats (365-day window) |
| `/data/sleep_scores.json` | Sleep scores by date |
| `/data/predictions.json` | Wake/sleep/arrive/depart history |

## Migrating from v3

Stop these add-ons (data is preserved in `/data/`):
- `ha-addon-event-bus` (was :8092)
- `ha-addon-home-intelligence` (was :8093 — same port, drop-in)
- `ha-addon-home-analytics` (was :8095)

Update any `rest_command` or `sensor` configs pointing to :8092 or :8095 to use :8093 with new route paths.
