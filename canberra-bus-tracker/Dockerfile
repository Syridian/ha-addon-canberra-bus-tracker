ARG BUILD_FROM=ghcr.io/home-assistant/amd64-base-python:3.12-alpine3.19
FROM $BUILD_FROM

RUN pip3 install --break-system-packages --no-cache-dir \
      gtfs-realtime-bindings \
      requests \
      paho-mqtt

COPY rootfs /
RUN chmod +x /usr/bin/bus_wled.py

CMD ["python3", "/usr/bin/bus_wled.py"]
