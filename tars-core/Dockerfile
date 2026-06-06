ARG BUILD_FROM
FROM ${BUILD_FROM}

# Timezone: TARS Core uses naive datetime.now() for mode windows + logging,
# so container TZ must match user's local time (America/Los_Angeles).
ENV TZ=America/Los_Angeles

RUN apk add --no-cache python3 py3-pip tzdata \
    && cp /usr/share/zoneinfo/${TZ} /etc/localtime \
    && echo "${TZ}" > /etc/timezone

COPY requirements.txt /tmp/
RUN pip install --break-system-packages --no-cache-dir -r /tmp/requirements.txt

# Copy package and flatten server.py to /app for run.sh compatibility
COPY tars_core /app/tars_core
RUN cp /app/tars_core/server.py /app/server.py \
    && cp /app/tars_core/helpers.py /app/helpers.py 2>/dev/null || true \
    && cp /app/tars_core/storage.py /app/storage.py 2>/dev/null || true \
    && cp /app/tars_core/typed_config.py /app/typed_config.py 2>/dev/null || true \
    && cp /app/tars_core/dashboard.py /app/dashboard.py 2>/dev/null || true \
    && cp /app/tars_core/__init__.py /app/__init__.py 2>/dev/null || true

COPY run.sh /app/run.sh
RUN chmod +x /app/run.sh

WORKDIR /app
CMD ["/app/run.sh"]
