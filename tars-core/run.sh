#!/bin/sh
set -e

CONFIG=/data/options.json

if [ ! -f "$CONFIG" ]; then
    echo "[ERROR] No config at $CONFIG"
    exit 1
fi

export HA_URL=$(python3 -c "import json; print(json.load(open('$CONFIG'))['ha_url'])")
export HA_TOKEN=$(python3 -c "import json; print(json.load(open('$CONFIG'))['ha_token'])")
export API_PORT=$(python3 -c "import json; print(json.load(open('$CONFIG'))['api_port'])")
export SERVICES_URL=$(python3 -c "import json; print(json.load(open('$CONFIG'))['services_url'])")
export COOPER_SCHEDULE=$(python3 -c "import json; print(json.load(open('$CONFIG'))['cooper_schedule'])")

echo "[INFO] TARS Core v4.0.0"
echo "[INFO] HA: ${HA_URL} | Services: ${SERVICES_URL} | Port: ${API_PORT}"

exec python3 /app/server.py
