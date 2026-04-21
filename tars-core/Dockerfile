ARG BUILD_FROM
FROM ${BUILD_FROM}
RUN pip3 install flask requests sseclient-py websocket-client
COPY server.py /app/server.py
COPY run.sh /app/run.sh
RUN chmod +x /app/run.sh
WORKDIR /app
ENTRYPOINT []
CMD ["/app/run.sh"]
