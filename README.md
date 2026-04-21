# TARS Core v4.0.0

Consolidated Home Assistant add-on — replaces v3 Event Bus, Intelligence, and Analytics add-ons with a single service on port **8093**.

## What's New in v4

- **Room Presence** (`GET /presence`) — dict of rooms with occupied/last_motion from HA motion sensors
- **Sleep Scoring** (`GET /analytics/sleep/last`) — A–F grade from overnight CO₂, temperature, and humidity
- **Dashboard** (`GET /dashboard`) — master JSON: mode, presence, weather, anomalies, battery alerts, sleep score, Cooper status
- **Anomaly Detection** (`GET /anomalies`) — entities firing >20 events/min, doors open with no one home
- **Predictive Wake** (`GET /predictions`) — tracks bedroom motion times, calculates average wake window
- **Weather-Reactive** — rainy conditions trigger cozy mood (lights + music) automatically
- **Decision Audit** — every `decision_log` entry includes a `why` field

## Architecture

Single Flask app, three logical modules in `server.py`:

| Module | Responsibility |
|---|---|
| **Event Bus** | HA WebSocket subscriber, SSE stream, pattern learning |
| **Intelligence** | Context engine, mode state machine, Cooper-aware decisions |
| **Analytics** | Climate history, sleep scoring, presence stats, device health |

## Routes

### Event Bus
`GET /events/stream` (SSE) · `/events/recent` · `/patterns`

### Intelligence
`GET /` · `/health` · `/context` · `/mode` · `/learned` · `/cooper` · `/insights` · `/log` · `/proactive` · `/presence` · `/dashboard` · `/predictions` · `/anomalies`  
`POST /arrive` · `/depart` · `/mood/<mood>` · `/cooper/here` · `/cooper/gone`

### Analytics
`GET /analytics/daily` · `/analytics/sleep/last` · `/analytics/energy` · `/analytics/health`

## Configuration

| Option | Default | Description |
|---|---|---|
| `ha_url` | `http://localhost:8123` | Home Assistant URL |
| `ha_token` | _(required)_ | Long-lived access token |
| `api_port` | `8093` | Port to listen on |
| `services_url` | `http://localhost:8097` | TARS Services (Spotify DJ) URL |
| `cooper_schedule` | `fri_1600-mon_1100,...` | Cooper visit schedule |

## Safety

- Audio is **never** sent to bedroom entities without confirming `binary_sensor.bedroom_motion`
- Silent hours (10pm–8am) suppress audio, fall back to push notifications
- All Echo device access is gated through the Echo entity list
