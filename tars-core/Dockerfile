ARG BUILD_FROM
FROM ${BUILD_FROM}
RUN pip3 install flask requests sseclient-py websocket-client Pillow
COPY server.py /app/server.py
COPY storage.py /app/storage.py
COPY dashboard.py /app/dashboard.py
COPY helpers.py /app/helpers.py
COPY typed_config.py /app/typed_config.py
COPY run.sh /app/run.sh
RUN chmod +x /app/run.sh
WORKDIR /app
ENTRYPOINT []
CMD ["/app/run.sh"]
