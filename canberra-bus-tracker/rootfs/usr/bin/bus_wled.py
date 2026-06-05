#!/usr/bin/env python3
"""
Canberra Bus Tracker v0.3.16
Tracks buses approaching a stop from both directions using GTFS static +
realtime feeds, with GPS dead-reckoning, route colours, continuous
position-based LED fading across physical LED groups, departed timeout,
mode switching (running/coasting/off), multiple WLED instances with
per-instance HA-adjustable brightness, and full MQTT entity publishing
for Home Assistant.
"""

import os
import io
import sys
import csv
import json
import math
import time
import signal
import logging
import zipfile
import getpass
import threading
from datetime import datetime

import requests
from google.transit import gtfs_realtime_pb2

try:
    import paho.mqtt.client as mqtt_client
    MQTT_AVAILABLE = True
except ImportError:
    MQTT_AVAILABLE = False

# ── Logging ───────────────────────────────────────────────────────────────────
log = logging.getLogger("bus_wled")
logging.basicConfig(
    level=logging.DEBUG if os.environ.get("DEBUG") else logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

def set_log_level(debug: bool):
    """Adjust log level after config is loaded."""
    level = logging.DEBUG if debug else logging.INFO
    logging.getLogger().setLevel(level)
    log.setLevel(level)

# ── Constants ─────────────────────────────────────────────────────────────────
CONFIG_PATH            = "/data/options.json"
GTFS_CACHE             = os.environ.get("GTFS_CACHE", "/data/gtfs_static.zip")
GTFS_STATIC_URL        = "https://www.transport.act.gov.au/googletransit/google_transit.zip"
EARTH_RADIUS_M         = 6_371_000
COASTING_STALE_TIMEOUT = 120

# Average inter-stop travel time used to convert arrival_offset_seconds → stop count.
# Adjust if Canberra buses are consistently faster or slower between stops.
SECS_PER_STOP = 60

MODE_RUNNING  = "running"
MODE_COASTING = "coasting"
MODE_OFF      = "off"

NUM_VIRTUAL    = 5   # always 5 virtual states
STATE_PRIORITY = ["at_stop", "1_stop", "2_stops", "3_stops", "departed", "gone"]

# Virtual state indices (Direction A order: 3-stops → departed)
V_3STOPS   = 0
V_2STOPS   = 1
V_1STOP    = 2
V_ATSTOP   = 3
V_DEPARTED = 4

# Maximum HTTP sends per second to WLED (prevents saturation during pulse)
WLED_MAX_HZ = 5
WLED_MIN_INTERVAL = 1.0 / WLED_MAX_HZ


# ── Colour helpers ─────────────────────────────────────────────────────────────
def parse_color(value) -> list:
    """
    Accept colour in any of these forms and return [R, G, B] int list:
      - [R, G, B]           already a list (pass-through)
      - "#RRGGBB"           hex string with hash
      - "RRGGBB"            hex string without hash
      - "#RGB"              short hex
      - 0xRRGGBB            integer
    All channel values are clamped to 0-255.
    """
    if isinstance(value, (list, tuple)):
        if len(value) != 3:
            raise ValueError(f"Color list must have 3 elements, got {len(value)}: {value}")
        return [max(0, min(255, int(v))) for v in value]
    if isinstance(value, int):
        r = (value >> 16) & 0xFF
        g = (value >>  8) & 0xFF
        b =  value        & 0xFF
        return [r, g, b]
    if isinstance(value, str):
        s = value.strip().lstrip("#")
        if len(s) == 3:
            s = s[0]*2 + s[1]*2 + s[2]*2
        if len(s) != 6:
            raise ValueError(f"Cannot parse colour string: {value!r}")
        r = int(s[0:2], 16)
        g = int(s[2:4], 16)
        b = int(s[4:6], 16)
        return [r, g, b]
    raise ValueError(f"Unsupported colour type {type(value)}: {value!r}")


# ── Group layout ──────────────────────────────────────────────────────────────
def calculate_groups(num_leds: int) -> list[tuple[int, int]]:
    """
    Divide num_leds physical LEDs into NUM_VIRTUAL groups.
    Returns list of (start, end) tuples, end exclusive.
    Extra LEDs (remainder) are distributed centre-outward:
    priority order for extras is V_ATSTOP, V_1STOP, V_2STOPS, V_3STOPS, V_DEPARTED.
    For a 12-LED strip: base=2, extras=2 → groups [2,2,3,3,2]
    """
    base   = num_leds // NUM_VIRTUAL
    extras = num_leds % NUM_VIRTUAL
    sizes  = [base] * NUM_VIRTUAL

    # Centre-weighted extra distribution
    extra_order = [V_ATSTOP, V_1STOP, V_2STOPS, V_3STOPS, V_DEPARTED]
    for i in range(extras):
        sizes[extra_order[i]] += 1

    # Convert sizes to (start, end) ranges
    groups = []
    pos = 0
    for s in sizes:
        groups.append((pos, pos + s))
        pos += s
    return groups


def groups_for_direction(groups: list, direction: str) -> list[tuple[int, int]]:
    """
    Returns a 5-element list of (start, end) physical LED ranges indexed by
    virtual state constant (V_3STOPS=0 .. V_DEPARTED=4).

    Direction A: natural order — index 0 = leftmost group (3-stops)
    Direction B: reversed     — index 0 maps to rightmost group (also 3-stops
                                for Dir B, which physically sits at the right end)
    """
    if direction == "A":
        return groups
    else:
        return list(reversed(groups))


# ── Bus position → strip fraction ─────────────────────────────────────────────
def bus_strip_fraction(stops_away: int, seg_fraction: float) -> float:
    """
    Map a bus's current position to a 0.0–1.0 fraction along the strip.
    0.0 = start of 3-stops group (furthest away)
    1.0 = end of departed group
    """
    centres = [i / (NUM_VIRTUAL - 1) for i in range(NUM_VIRTUAL)]

    if stops_away is None:
        return 0.0
    if stops_away <= -1:  # departed
        return centres[V_DEPARTED]
    if stops_away == 0:   # at_stop
        return centres[V_ATSTOP]
    if stops_away == 1:   # 1_stop — interpolate using GPS fraction
        c_1stop  = centres[V_1STOP]
        c_atstop = centres[V_ATSTOP]
        return c_1stop + (c_atstop - c_1stop) * seg_fraction
    if stops_away == 2:
        return centres[V_2STOPS]
    return centres[V_3STOPS]  # 3+ stops


# ── WLED instance ─────────────────────────────────────────────────────────────
class WledInstance:
    def __init__(self, ip, led_offset, num_leds, brightness_pct, label, segment_id):
        self.ip             = ip
        self.led_offset     = led_offset
        self.num_leds       = max(NUM_VIRTUAL, num_leds)  # minimum 5
        self.label          = label
        self.label_slug     = "".join(
            c if c.isalnum() else "_"
            for c in label.lower()
        ).strip("_")
        self.segment_id     = segment_id
        self._brightness    = max(0, min(100, brightness_pct))
        self._lock          = threading.Lock()
        self.groups         = calculate_groups(self.num_leds)
        self._last_send     = 0.0   # timestamp of last successful HTTP send
        log.info("WLED [%s]: %d LEDs, segment %d, groups: %s",
                 label, self.num_leds, segment_id,
                 [(g[1] - g[0]) for g in self.groups])

    @property
    def brightness_pct(self):
        with self._lock:
            return self._brightness

    @brightness_pct.setter
    def brightness_pct(self, value):
        with self._lock:
            self._brightness = max(0, min(100, int(value)))
            log.info("WLED [%s] brightness → %d%%", self.label, self._brightness)

    @property
    def brightness_255(self):
        return int(self.brightness_pct * 255 / 100)

    def ensure_segment(self):
        """
        Check if our segment_id already exists in WLED.
        If not, create it covering led_offset → led_offset + num_leds.
        If it exists, leave it completely alone (preserving blend mode etc).
        Requires WLED v16+.
        """
        if not self.ip:
            return
        try:
            resp = requests.get(f"http://{self.ip}/json/state", timeout=5)
            resp.raise_for_status()
            state    = resp.json()
            segments = state.get("seg", [])

            existing_ids = {s.get("id") for s in segments if isinstance(s, dict)}
            if self.segment_id in existing_ids:
                log.info("WLED [%s]: segment %d already exists — leaving unchanged",
                         self.label, self.segment_id)
                # Only clear on startup if segment already existed — avoids flash
                # on first-ever run when segment has never been configured
                self.clear()
                return

            log.info("WLED [%s]: creating segment %d (LEDs %d–%d)",
                     self.label, self.segment_id,
                     self.led_offset, self.led_offset + self.num_leds - 1)
            requests.post(
                f"http://{self.ip}/json/state",
                json={"seg": [{
                    "id":    self.segment_id,
                    "start": self.led_offset,
                    "stop":  self.led_offset + self.num_leds,
                    "on":    True,
                    "bri":   self.brightness_255,
                }]},
                timeout=5,
            )
            log.info("WLED [%s]: segment %d created", self.label, self.segment_id)
            self.clear()

        except requests.RequestException as e:
            log.warning("WLED [%s] segment setup failed: %s — will retry on next startup",
                        self.label, e)

    def _seg_payload(self, i_data: list) -> dict:
        return {
            "seg": [{
                "id":  self.segment_id,
                "bri": self.brightness_255,
                "i":   i_data,
            }]
        }

    def send(self, leds: list, force: bool = False):
        """
        Send LED colour data to our segment.
        Throttled to WLED_MAX_HZ to avoid saturating WLED with HTTP.
        Pass force=True to bypass throttle (e.g. clear on shutdown).
        """
        if not self.ip:
            return
        now = time.time()
        if not force and (now - self._last_send) < WLED_MIN_INTERVAL:
            return
        i_data = [0]   # start at position 0 within the segment
        for led in leds:
            i_data.extend([max(0, min(255, v)) for v in led])
        try:
            requests.post(
                f"http://{self.ip}/json/state",
                json=self._seg_payload(i_data),
                timeout=3,
            )
            self._last_send = now
        except requests.RequestException as e:
            log.warning("WLED [%s]: %s", self.label, e)

    def clear(self):
        """Turn off all LEDs in our segment (bypasses rate throttle)."""
        if not self.ip:
            return
        i_data = [0] + [0] * (self.num_leds * 3)
        try:
            requests.post(
                f"http://{self.ip}/json/state",
                json=self._seg_payload(i_data),
                timeout=3,
            )
            self._last_send = time.time()
        except requests.RequestException as e:
            log.warning("WLED [%s] clear: %s", self.label, e)


# ── Config ────────────────────────────────────────────────────────────────────
def load_config() -> dict:
    cfg = {}
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH) as f:
            cfg = json.load(f)
        log.info("Loaded config from %s", CONFIG_PATH)

    def get(key, prompt, secret=False):
        if cfg.get(key):
            return cfg[key]
        env = os.environ.get(key.upper())
        if env:
            return env
        return getpass.getpass(f"{prompt}: ") if secret else input(f"{prompt}: ")

    wled_instances = []
    for i, inst in enumerate(cfg.get("wled_instances", [])):
        wled_instances.append(WledInstance(
            ip             = inst.get("ip", ""),
            led_offset     = int(inst.get("led_offset", 0)),
            num_leds       = int(inst.get("num_leds", 5)),
            brightness_pct = int(inst.get("brightness", 100)),
            label          = inst.get("label", f"wled_{i+1}"),
            segment_id     = int(inst.get("segment_id", 9)),
        ))

    # route_colors can be either:
    # - list format (from HA config UI): [{"route": "5", "color": [255,20,147]}, ...]
    # - dict format (from options.json or standalone): {"5": [255,20,147], ...}
    raw_colors = cfg.get("route_colors", [])
    if isinstance(raw_colors, list):
        route_colors = {}
        for item in raw_colors:
            if "route" in item:
                try:
                    route_colors[item["route"]] = parse_color(item["color"])
                except (ValueError, TypeError) as e:
                    log.warning("Bad route color for route %s: %s", item.get("route"), e)
    else:
        route_colors = {}
        for route, color in raw_colors.items():
            try:
                route_colors[route] = parse_color(color)
            except (ValueError, TypeError) as e:
                log.warning("Bad route color for route %s: %s", route, e)

    def safe_color(key, default):
        raw = cfg.get(key, default)
        try:
            return parse_color(raw)
        except (ValueError, TypeError) as e:
            log.warning("Bad color for %s (%r): %s — using default %s", key, raw, e, default)
            return list(default)

    return {
        "api_key":                  get("api_key", "Transport Canberra API key", secret=True),
        "api_base_url":             cfg.get("api_base_url", "https://transport.api.act.gov.au/gtfs/data/gtfs/v2"),
        "gtfs_static_url":          cfg.get("gtfs_static_url", GTFS_STATIC_URL),
        "stop_id_a":                get("stop_id_a", "Stop ID for Direction A (e.g. 1084)"),
        "stop_id_b":                get("stop_id_b", "Stop ID for Direction B (e.g. 1085)"),
        "route_filter":             cfg.get("route_filter", ""),
        "poll_interval":            int(cfg.get("poll_interval", 15)),
        "gps_poll_interval":        int(cfg.get("gps_poll_interval", 15)),
        "gtfs_refresh_hours":       int(cfg.get("gtfs_refresh_hours", 24)),
        "arrival_offset_seconds":   int(cfg.get("arrival_offset_seconds", 90)),
        "departed_timeout_seconds": int(cfg.get("departed_timeout_seconds", 60)),
        "wled_instances":           wled_instances,
        "flash_interval_ms":        int(cfg.get("flash_interval_ms", 500)),
        "color_at_stop":            safe_color("color_at_stop",  [255, 255, 255]),
        "color_departed":           safe_color("color_departed", [255,   0,   0]),
        "color_off":                safe_color("color_off",      [  0,   0,   0]),
        "color_default":            safe_color("color_default",  [  0, 120, 255]),
        "route_colors":             route_colors,
        "pulse_speed":              float(cfg.get("pulse_speed", 1.5)),
        "mqtt_enabled":             bool(cfg.get("mqtt_enabled", False)),
        "mqtt_host":                cfg.get("mqtt_host", "core-mosquitto"),
        "mqtt_port":                int(cfg.get("mqtt_port", 1883)),
        "mqtt_user":                cfg.get("mqtt_user", ""),
        "mqtt_password":            cfg.get("mqtt_password", ""),
        "mqtt_topic_prefix":        cfg.get("mqtt_topic_prefix", "bus_stop"),
        "debug":                    bool(cfg.get("debug", False)),
    }


# ── Geometry helpers ──────────────────────────────────────────────────────────
def haversine(lat1, lon1, lat2, lon2) -> float:
    r = EARTH_RADIUS_M
    p = math.pi / 180
    a = (math.sin((lat2 - lat1) * p / 2) ** 2 +
         math.cos(lat1 * p) * math.cos(lat2 * p) *
         math.sin((lon2 - lon1) * p / 2) ** 2)
    return 2 * r * math.asin(math.sqrt(a))


def segment_fraction(bus_lat, bus_lon, from_lat, from_lon, to_lat, to_lon) -> float:
    p    = math.pi / 180
    mlat = math.cos((from_lat + to_lat) / 2 * p) * EARTH_RADIUS_M * p
    mlon = EARTH_RADIUS_M * p
    dx   = (to_lon  - from_lon) * mlat
    dy   = (to_lat  - from_lat) * mlon
    bx   = (bus_lon - from_lon) * mlat
    by   = (bus_lat - from_lat) * mlon
    seg_len_sq = dx * dx + dy * dy
    if seg_len_sq == 0:
        return 0.0
    return max(0.0, min(1.0, (bx * dx + by * dy) / seg_len_sq))


# ── GTFS Static ───────────────────────────────────────────────────────────────
class GtfsStatic:
    def __init__(self, url, cache_path, refresh_hours):
        self.url           = url
        self.cache_path    = cache_path
        self.refresh_hours = refresh_hours
        self.loaded_at     = None
        self.trips         = {}
        self.trip_stops    = {}
        self.stop_coords   = {}
        self.stop_trips    = {}
        self._lock         = threading.Lock()

    def ensure_loaded(self):
        if self.loaded_at:
            age_h = (datetime.now() - self.loaded_at).total_seconds() / 3600
            if age_h < self.refresh_hours:
                return
        with self._lock:
            if self.loaded_at:
                age_h = (datetime.now() - self.loaded_at).total_seconds() / 3600
                if age_h < self.refresh_hours:
                    return
            log.info("Loading GTFS static data...")
            self._download()
            self._parse()
            self.loaded_at = datetime.now()
            log.info("GTFS static: %d trips, %d stops", len(self.trips), len(self.stop_coords))

    def _download(self):
        if os.path.exists(self.cache_path):
            age_h = (time.time() - os.path.getmtime(self.cache_path)) / 3600
            if age_h < self.refresh_hours:
                log.info("Using cached GTFS static (%.1fh old)", age_h)
                return
        log.info("Downloading GTFS static...")
        r = requests.get(self.url, timeout=30)
        r.raise_for_status()
        os.makedirs(os.path.dirname(self.cache_path) or ".", exist_ok=True)
        with open(self.cache_path, "wb") as f:
            f.write(r.content)
        log.info("GTFS static saved (%d KB)", len(r.content) // 1024)

    def _parse(self):
        trips = {}; trip_stops = {}; stop_coords = {}; stop_trips = {}
        with zipfile.ZipFile(self.cache_path) as zf:
            with zf.open("trips.txt") as f:
                for row in csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig")):
                    trips[row["trip_id"]] = {
                        "route_id":     row.get("route_id", ""),
                        "direction_id": row.get("direction_id", "0"),
                        "headsign":     row.get("trip_headsign", ""),
                    }
            with zf.open("stops.txt") as f:
                for row in csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig")):
                    try:
                        stop_coords[row["stop_id"]] = (
                            float(row["stop_lat"]), float(row["stop_lon"])
                        )
                    except (KeyError, ValueError):
                        pass
            raw = {}
            with zf.open("stop_times.txt") as f:
                for row in csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig")):
                    tid = row["trip_id"]
                    try:
                        seq = int(row.get("stop_sequence", 0))
                    except ValueError:
                        seq = 0
                    raw.setdefault(tid, []).append((seq, row["stop_id"]))
            for tid, stops in raw.items():
                stops.sort(key=lambda x: x[0])
                ordered = [s[1] for s in stops]
                trip_stops[tid] = ordered
                for sid in ordered:
                    stop_trips.setdefault(sid, set()).add(tid)
        self.trips = trips; self.trip_stops = trip_stops
        self.stop_coords = stop_coords; self.stop_trips = stop_trips

    def stops_away(self, trip_id, stop_id, remaining_stop_ids) -> int | None:
        """
        Returns how many stops the bus is away from stop_id:
          0  = at the stop (stop_id is the very next stop)
         >0  = N stops away
         -1  = has passed the stop (departed)
         None = this trip doesn't serve stop_id

        Uses the GTFS static sequence as ground truth for stop ordering,
        then finds where the first remaining stop sits relative to the target.
        Falls back to scanning remaining_stop_ids directly if the next
        remaining stop isn't found in the static sequence (data gap).
        """
        seq = self.trip_stops.get(trip_id)
        if not seq or stop_id not in seq:
            return None

        target_idx = seq.index(stop_id)

        if not remaining_stop_ids:
            # No remaining stops in the feed at all — bus has passed everything
            return -1

        # Walk remaining stops to find the first one that appears in our static sequence
        for i, sid in enumerate(remaining_stop_ids):
            if sid not in seq:
                continue  # skip stops not in static (data inconsistency)
            next_idx = seq.index(sid)
            if next_idx > target_idx:
                # Bus has already passed the target stop
                return -1
            # Bus is i stops before the first known remaining stop,
            # which is (target_idx - next_idx) stops from the target
            return target_idx - next_idx + i

        # All remaining stops either not in static or after target — treat as departed
        return -1

    def segment_coords(self, trip_id, stop_id) -> tuple | None:
        seq = self.trip_stops.get(trip_id)
        if not seq or stop_id not in seq:
            return None
        idx = seq.index(stop_id)
        if idx == 0:
            return None
        fc = self.stop_coords.get(seq[idx - 1])
        tc = self.stop_coords.get(stop_id)
        if not fc or not tc:
            return None
        return fc, tc

    def get_trip_info(self, trip_id) -> dict:
        return self.trips.get(trip_id, {})


# ── Bus state ─────────────────────────────────────────────────────────────────
class BusState:
    def __init__(self, trip_id, route_id, direction, direction_id, headsign, color, stop_id):
        self.trip_id        = trip_id
        self.route_id       = route_id
        self.direction      = direction
        self.direction_id   = direction_id   # GTFS direction_id from trips.txt
        self.headsign       = headsign
        self.color          = color
        self.stop_id        = stop_id
        self.stops_away     = None
        self.raw_stops_away = None
        self.gps_lat        = None
        self.gps_lon        = None
        self.gps_time       = None
        self.prev_lat       = None
        self.prev_lon       = None
        self.prev_time      = None
        self.speed_ms       = 0.0
        self.seg_fraction   = 0.0
        self.updated_at     = time.time()
        self.departed_at    = None

    def update_gps(self, lat, lon, ts):
        if self.gps_lat is not None:
            self.prev_lat  = self.gps_lat
            self.prev_lon  = self.gps_lon
            self.prev_time = self.gps_time
        self.gps_lat  = lat
        self.gps_lon  = lon
        self.gps_time = ts
        if self.prev_lat is not None and self.prev_time is not None:
            dt = self.gps_time - self.prev_time
            if dt > 0:
                self.speed_ms = haversine(
                    self.prev_lat, self.prev_lon, lat, lon
                ) / dt

    def extrapolated_seg_fraction(self, seg_from, seg_to) -> float:
        if self.gps_lat is None:
            return self.seg_fraction
        # Use a fresh timestamp so dead-reckoning doesn't underestimate
        # due to the stale `now` captured at the top of the main loop
        now  = time.time()
        dt   = now - (self.gps_time or now)
        frac = segment_fraction(
            self.gps_lat, self.gps_lon,
            seg_from[0], seg_from[1],
            seg_to[0],   seg_to[1],
        )
        if self.speed_ms > 0 and dt > 0:
            seg_len = haversine(seg_from[0], seg_from[1], seg_to[0], seg_to[1])
            if seg_len > 0:
                frac += (self.speed_ms * dt) / seg_len
        return max(0.0, min(1.0, frac))

    @property
    def state(self) -> str:
        sa = self.stops_away
        if sa is None:  return "gone"
        if sa < 0:      return "departed"
        if sa == 0:     return "at_stop"
        if sa == 1:     return "1_stop"
        if sa == 2:     return "2_stops"
        return "3_stops"

    def priority(self) -> int:
        return STATE_PRIORITY.index(self.state) if self.state in STATE_PRIORITY else 99

    def to_mqtt_payload(self) -> dict:
        return {
            "state":        self.state,
            "stops_away":   self.stops_away,
            "route_id":     self.route_id,
            "trip_id":      self.trip_id,
            "headsign":     self.headsign,
            "stop_id":      self.stop_id,
            "seg_fraction": round(self.seg_fraction, 3),
            "speed_ms":     round(self.speed_ms, 2),
            "direction":    self.direction,
        }


# ── LED renderer ──────────────────────────────────────────────────────────────
class LedRenderer:
    """
    Renders active buses onto a physical LED strip of arbitrary length.
    """

    def __init__(self, cfg, gtfs):
        self.cfg          = cfg
        self.gtfs         = gtfs
        self.flash_state  = False
        self.last_flash   = time.time()
        self.flash_period = cfg["flash_interval_ms"] / 1000.0

    def tick(self):
        now = time.time()
        if now - self.last_flash >= self.flash_period:
            self.flash_state = not self.flash_state
            self.last_flash  = now

    def render_for_instance(self, inst: WledInstance, active_buses: list,
                            pulse_brightness: float) -> list:
        num_leds = inst.num_leds
        groups   = inst.groups
        leds     = [list(self.cfg["color_off"]) for _ in range(num_leds)]

        led_contributions = {}

        for bus in active_buses:
            if bus.state in ("gone", None):
                continue

            dir_groups = groups_for_direction(groups, bus.direction)

            if bus.state == "departed":
                start, end = dir_groups[V_DEPARTED]
                for i in range(start, end):
                    led_contributions.setdefault(i, []).append(
                        (list(self.cfg["color_departed"]), 1.0, bus)
                    )
                continue

            if bus.state == "at_stop":
                start, end = dir_groups[V_ATSTOP]
                color = [int(c * pulse_brightness) for c in bus.color]
                for i in range(start, end):
                    led_contributions.setdefault(i, []).append(
                        (color, 1.0, bus)
                    )
                continue

            seg_frac = 0.0
            if bus.state == "1_stop":
                seg = self.gtfs.segment_coords(bus.trip_id, bus.stop_id)
                if seg and bus.gps_lat is not None:
                    seg_frac = bus.extrapolated_seg_fraction(seg[0], seg[1])
                    bus.seg_fraction = seg_frac

            strip_frac = bus_strip_fraction(bus.stops_away, seg_frac)
            phys_pos   = strip_frac * (num_leds - 1)

            if bus.state == "1_stop":
                route = bus.color
                white = self.cfg["color_at_stop"]
                color = [int(route[i] + (white[i] - route[i]) * seg_frac)
                         for i in range(3)]
            else:
                color = list(bus.color)

            lo  = int(phys_pos)
            hi  = min(lo + 1, num_leds - 1)
            t   = phys_pos - lo
            w_lo = 1.0 - t
            w_hi = t

            if w_lo > 0.01:
                led_contributions.setdefault(lo, []).append((color, w_lo, bus))
            if w_hi > 0.01 and hi != lo:
                led_contributions.setdefault(hi, []).append((color, w_hi, bus))

        for idx, contributions in led_contributions.items():
            if not contributions:
                continue

            if len(contributions) == 1:
                color, weight, _ = contributions[0]
                leds[idx] = [int(c * weight) for c in color]
            else:
                by_bus = {}
                for color, weight, bus in contributions:
                    tid = bus.trip_id
                    if tid not in by_bus:
                        by_bus[tid] = (color, weight, bus)
                    else:
                        ec, ew, eb = by_bus[tid]
                        combined_w = ew + weight
                        blended = [
                            int((ec[i] * ew + color[i] * weight) / combined_w)
                            for i in range(3)
                        ]
                        by_bus[tid] = (blended, combined_w, eb)

                unique_buses = list(by_bus.values())
                if len(unique_buses) == 1:
                    color, weight, _ = unique_buses[0]
                    leds[idx] = [int(c * weight) for c in color]
                else:
                    unique_buses.sort(key=lambda x: x[2].priority())
                    chosen = unique_buses[0] if self.flash_state else unique_buses[1]
                    color, weight, _ = chosen
                    leds[idx] = [int(c * weight) for c in color]

        return leds


# ── MQTT ──────────────────────────────────────────────────────────────────────
class MqttPublisher:
    def __init__(self, cfg, on_command, on_brightness):
        self.cfg           = cfg
        self.on_command    = on_command
        self.on_brightness = on_brightness
        self.client        = None
        self.prefix        = cfg["mqtt_topic_prefix"]
        self.enabled       = cfg["mqtt_enabled"] and MQTT_AVAILABLE
        self.instances     = cfg["wled_instances"]
        if self.enabled:
            self._connect()
            self._publish_discovery()

    def _connect(self):
        c = mqtt_client.Client(
            client_id="canberra_bus_tracker",
            clean_session=True,
            callback_api_version=mqtt_client.CallbackAPIVersion.VERSION2,
        )
        if self.cfg["mqtt_user"]:
            c.username_pw_set(self.cfg["mqtt_user"], self.cfg["mqtt_password"])
        c.will_set(f"{self.prefix}/status", "offline", retain=True)
        c.on_message = self._on_message
        try:
            c.connect(self.cfg["mqtt_host"], self.cfg["mqtt_port"], keepalive=60)
            c.subscribe(f"{self.prefix}/command")
            for inst in self.instances:
                c.subscribe(f"{self.prefix}/wled/{inst.label_slug}/brightness/set")
            c.loop_start()
            c.publish(f"{self.prefix}/status", "online", retain=True)
            self.client = c
            log.info("MQTT connected to %s:%s", self.cfg["mqtt_host"], self.cfg["mqtt_port"])
        except Exception as e:
            log.warning("MQTT connect failed: %s", e)
            self.enabled = False

    def _on_message(self, client, userdata, msg, properties=None):
        try:
            topic   = msg.topic
            payload = msg.payload.decode().strip()
            if topic == f"{self.prefix}/command":
                self.on_command(payload.upper())
                return
            for inst in self.instances:
                if topic == f"{self.prefix}/wled/{inst.label_slug}/brightness/set":
                    try:
                        pct = int(float(payload))
                        self.on_brightness(inst.label_slug, pct)
                        if self.client:
                            self.client.publish(
                                f"{self.prefix}/wled/{inst.label_slug}/brightness",
                                str(pct), retain=True,
                            )
                    except ValueError:
                        log.warning("Invalid brightness value: %s", payload)
                    return
        except Exception as e:
            log.warning("MQTT message error: %s", e)

    def _publish_discovery(self):
        if not self.client:
            return
        device = {
            "identifiers":  ["canberra_bus_tracker"],
            "name":         "Canberra Bus Tracker",
            "manufacturer": "Custom",
            "model":        "canberra_bus_tracker v0.3.16",
        }
        for uid, name in (
            ("direction_a", "Direction A Bus"),
            ("direction_b", "Direction B Bus"),
        ):
            self.client.publish(
                f"homeassistant/sensor/{uid}/config",
                json.dumps({
                    "name":                  name,
                    "unique_id":             f"bus_wled_{uid}",
                    "state_topic":           f"{self.prefix}/{uid}/state",
                    "json_attributes_topic": f"{self.prefix}/{uid}/attributes",
                    "icon":                  "mdi:bus",
                    "device":                device,
                }),
                retain=True,
            )
        self.client.publish(
            "homeassistant/binary_sensor/bus_due/config",
            json.dumps({
                "name": "Bus Due", "unique_id": "bus_wled_bus_due",
                "state_topic": f"{self.prefix}/bus_due",
                "payload_on": "ON", "payload_off": "OFF",
                "icon": "mdi:bus-clock", "device": device,
            }), retain=True,
        )
        self.client.publish(
            "homeassistant/sensor/bus_stop_mode/config",
            json.dumps({
                "name": "Bus Stop Mode", "unique_id": "bus_wled_mode",
                "state_topic": f"{self.prefix}/mode",
                "icon": "mdi:traffic-light", "device": device,
            }), retain=True,
        )
        self.client.publish(
            "homeassistant/switch/bus_stop/config",
            json.dumps({
                "name": "Bus Stop", "unique_id": "bus_wled_switch",
                "state_topic":   f"{self.prefix}/mode",
                "command_topic": f"{self.prefix}/command",
                "payload_on": "ON", "payload_off": "OFF",
                "state_on": MODE_RUNNING, "state_off": MODE_OFF,
                "icon": "mdi:bus-stop", "device": device,
            }), retain=True,
        )
        for inst in self.instances:
            self.client.publish(
                f"homeassistant/number/bus_wled_{inst.label_slug}_brightness/config",
                json.dumps({
                    "name":          f"{inst.label} Brightness",
                    "unique_id":     f"bus_wled_brightness_{inst.label_slug}",
                    "state_topic":   f"{self.prefix}/wled/{inst.label_slug}/brightness",
                    "command_topic": f"{self.prefix}/wled/{inst.label_slug}/brightness/set",
                    "min": 0, "max": 100, "step": 1,
                    "unit_of_measurement": "%",
                    "icon": "mdi:brightness-6", "device": device,
                }), retain=True,
            )
            self.client.publish(
                f"{self.prefix}/wled/{inst.label_slug}/brightness",
                str(inst.brightness_pct), retain=True,
            )
        log.info("MQTT discovery published (%d WLED instance(s))", len(self.instances))

    def publish_state(self, buses_by_direction: dict, mode: str):
        if not self.enabled or not self.client:
            return
        self.client.publish(f"{self.prefix}/mode", mode, retain=True)
        bus_due = False
        for direction in ("A", "B"):
            bus = buses_by_direction.get(direction)
            uid = f"direction_{direction.lower()}"
            if bus:
                self.client.publish(f"{self.prefix}/{uid}/state", bus.state)
                self.client.publish(f"{self.prefix}/{uid}/attributes",
                                    json.dumps(bus.to_mqtt_payload()))
                if bus.state in ("at_stop", "1_stop"):
                    bus_due = True
            else:
                self.client.publish(f"{self.prefix}/{uid}/state", "none")
                self.client.publish(f"{self.prefix}/{uid}/attributes", json.dumps({}))
        self.client.publish(f"{self.prefix}/bus_due", "ON" if bus_due else "OFF")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    cfg       = load_config()
    set_log_level(cfg["debug"])
    instances = cfg["wled_instances"]
    log.info("Canberra Bus Tracker v0.3.16 starting")
    log.info("  Stop A: %s | Stop B: %s | WLED instances: %d",
             cfg["stop_id_a"], cfg["stop_id_b"], len(instances))

    route_filter = [r.strip() for r in cfg["route_filter"].split(",") if r.strip()]

    # Convert arrival_offset_seconds to a stop count using a realistic per-stop
    # travel time (SECS_PER_STOP). A 90s offset at 60s/stop = 1 stop offset,
    # meaning a bus 2 raw stops away is displayed as 1 stop away.
    offset_stops = max(0, round(cfg["arrival_offset_seconds"] / SECS_PER_STOP))
    log.info("  Arrival offset: %ds → %d stop(s)", cfg["arrival_offset_seconds"], offset_stops)

    gtfs = GtfsStatic(cfg["gtfs_static_url"], GTFS_CACHE, cfg["gtfs_refresh_hours"])
    gtfs.ensure_loaded()

    for inst in instances:
        inst.ensure_segment()

    mode      = MODE_RUNNING
    mode_lock = threading.Lock()

    def set_mode(new_mode):
        nonlocal mode
        with mode_lock:
            if mode == new_mode:
                return
            log.info("Mode: %s → %s", mode, new_mode)
            mode = new_mode

    def handle_command(cmd):
        if cmd == "ON":    set_mode(MODE_RUNNING)
        elif cmd == "OFF": set_mode(MODE_COASTING)

    def handle_brightness(label_slug, pct):
        for inst in instances:
            if inst.label_slug == label_slug:
                inst.brightness_pct = pct
                return
        log.warning("Brightness update for unknown instance: %s", label_slug)

    def on_sigterm(signum, frame):
        log.info("SIGTERM — entering coast mode")
        set_mode(MODE_COASTING)

    signal.signal(signal.SIGTERM, on_sigterm)

    mqtt     = MqttPublisher(cfg, handle_command, handle_brightness)
    renderer = LedRenderer(cfg, gtfs)

    api_url     = cfg["api_base_url"].rstrip("/")
    trip_url    = f"{api_url}/trip-updates.pb"
    vehicle_url = f"{api_url}/vehicle-positions.pb"

    # Mount adapter on both http and https so dev/test http URLs also benefit
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter()
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    active_buses: dict[str, BusState] = {}
    coasting_ids: set[str]            = set()

    prev_leds   = {inst.label_slug: None for inst in instances}
    pulse_phase = 0.0
    last_pulse_time = time.time()
    last_trip   = 0.0
    last_gps    = 0.0
    last_mqtt   = 0.0
    prev_mode   = None

    def route_color(route_id) -> list:
        return list(cfg["route_colors"].get(route_id, cfg["color_default"]))

    def fetch(url):
        client_id, _, client_secret = cfg["api_key"].partition(":")
        r = session.get(
            url,
            auth=(client_id, client_secret) if client_secret else (cfg["api_key"], ""),
            timeout=10,
        )
        r.raise_for_status()
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(r.content)
        return feed

    def best_per_direction() -> dict:
        """
        Return the highest-priority (most actionable) bus per direction.
        Priority order: at_stop > 1_stop > 2_stops > 3_stops > departed > gone.
        A departed bus does not beat an approaching bus.
        """
        best = {}
        for bus in active_buses.values():
            d = bus.direction
            if d not in best or bus.priority() < best[d].priority():
                best[d] = bus
        return best

    def remove_buses(trip_ids, reason):
        for tid in list(trip_ids):
            if tid in active_buses:
                log.info("Trip %s removed (%s)", tid, reason)
                del active_buses[tid]
            coasting_ids.discard(tid)

    log.info("Entering main loop — mode: %s", mode)

    while True:
        loop_start   = time.time()
        now          = loop_start
        current_mode = mode
        gtfs.ensure_loaded()

        # ── Mode transition housekeeping ──────────────────────────────────────
        if current_mode != prev_mode:
            if current_mode == MODE_COASTING:
                coasting_ids = set(active_buses.keys())
                log.info("Coasting — locked %d bus(es): %s",
                         len(coasting_ids), coasting_ids)
            elif current_mode == MODE_RUNNING:
                coasting_ids.clear()
            elif current_mode == MODE_OFF:
                active_buses.clear()
                coasting_ids.clear()
                for inst in instances:
                    inst.clear()
            prev_mode = current_mode

        # ── Trip Updates ──────────────────────────────────────────────────────
        if current_mode != MODE_OFF and now - last_trip >= cfg["poll_interval"]:
            last_trip  = now
            try:
                feed       = fetch(trip_url)
                seen_trips = set()

                for entity in feed.entity:
                    if not entity.HasField("trip_update"):
                        continue
                    tu       = entity.trip_update
                    trip_id  = tu.trip.trip_id
                    route_id = tu.trip.route_id

                    if route_filter and route_id not in route_filter:
                        continue
                    if current_mode == MODE_COASTING and trip_id not in coasting_ids:
                        continue

                    trip_info = gtfs.get_trip_info(trip_id)
                    if not trip_info:
                        continue

                    remaining = [stu.stop_id for stu in tu.stop_time_update]

                    raw_sa_a = gtfs.stops_away(trip_id, cfg["stop_id_a"], remaining)
                    raw_sa_b = gtfs.stops_away(trip_id, cfg["stop_id_b"], remaining)

                    if raw_sa_a is None and raw_sa_b is None:
                        continue

                    # For trips serving both stops (loop routes), prefer the stop
                    # the bus is currently heading toward. Use GTFS direction_id as
                    # a tiebreaker when stops_away values are equal or both departed.
                    gtfs_direction_id = trip_info.get("direction_id", "0")
                    if raw_sa_a is not None and raw_sa_b is not None:
                        # Lower stops_away = closer / more relevant
                        # For equal values (e.g. both -1 departed), use GTFS direction_id:
                        # direction_id "0" → Direction A, "1" → Direction B
                        if raw_sa_a == raw_sa_b:
                            if gtfs_direction_id == "0":
                                direction, raw_sa = "A", raw_sa_a
                            else:
                                direction, raw_sa = "B", raw_sa_b
                        elif raw_sa_a <= raw_sa_b:
                            direction, raw_sa = "A", raw_sa_a
                        else:
                            direction, raw_sa = "B", raw_sa_b
                    elif raw_sa_a is not None:
                        direction, raw_sa = "A", raw_sa_a
                    else:
                        direction, raw_sa = "B", raw_sa_b

                    stop_id = cfg["stop_id_a"] if direction == "A" else cfg["stop_id_b"]

                    adj_sa = raw_sa if raw_sa <= 0 else max(0, min(3, raw_sa - offset_stops))

                    if trip_id not in active_buses:
                        if current_mode == MODE_COASTING:
                            continue
                        active_buses[trip_id] = BusState(
                            trip_id      = trip_id,
                            route_id     = route_id,
                            direction    = direction,
                            direction_id = gtfs_direction_id,
                            headsign     = trip_info.get("headsign", ""),
                            color        = route_color(route_id),
                            stop_id      = stop_id,
                        )

                    bus          = active_buses[trip_id]
                    was_departed = bus.state == "departed"
                    bus.raw_stops_away = raw_sa
                    bus.stops_away     = adj_sa
                    bus.updated_at     = now

                    if bus.state == "departed" and not was_departed:
                        bus.departed_at = now
                        log.info("Trip %s departed stop %s", trip_id, stop_id)

                    seen_trips.add(trip_id)
                    log.debug(
                        "Trip %-12s | Route %-6s | Dir %s | Raw: %s | Adj: %s | %s",
                        trip_id, route_id, direction, raw_sa, adj_sa, bus.state,
                    )

                if current_mode == MODE_RUNNING:
                    remove_buses(
                        [t for t in active_buses if t not in seen_trips],
                        "left feed"
                    )
                if current_mode == MODE_COASTING:
                    remove_buses(
                        [
                            tid for tid in coasting_ids
                            if tid not in seen_trips and tid in active_buses
                            and (
                                active_buses[tid].state == "departed"
                                or (now - active_buses[tid].updated_at) > COASTING_STALE_TIMEOUT
                            )
                        ],
                        "coasting — left feed"
                    )
            except Exception as e:
                log.error("Trip update error: %s", e)

        # ── Vehicle Positions (GPS) ───────────────────────────────────────────
        if current_mode != MODE_OFF and now - last_gps >= cfg["gps_poll_interval"]:
            last_gps = now
            try:
                feed = fetch(vehicle_url)
                for entity in feed.entity:
                    if not entity.HasField("vehicle"):
                        continue
                    vp      = entity.vehicle
                    trip_id = vp.trip.trip_id
                    if trip_id not in active_buses:
                        continue
                    pos = vp.position
                    if pos.latitude and pos.longitude:
                        active_buses[trip_id].update_gps(
                            pos.latitude, pos.longitude,
                            # Use a fresh timestamp — not the stale loop-start `now`
                            vp.timestamp if vp.timestamp else time.time(),
                        )
            except Exception as e:
                log.error("Vehicle position error: %s", e)

        # ── Departed timeout ──────────────────────────────────────────────────
        remove_buses(
            [
                tid for tid, bus in active_buses.items()
                if bus.state == "departed"
                and bus.departed_at is not None
                and (now - bus.departed_at) >= cfg["departed_timeout_seconds"]
            ],
            "departed timeout"
        )

        # ── Coast → Off ───────────────────────────────────────────────────────
        if current_mode == MODE_COASTING and not active_buses:
            log.info("All coasting buses cleared — going off")
            set_mode(MODE_OFF)
            current_mode = MODE_OFF

        # ── MQTT publish ──────────────────────────────────────────────────────
        if now - last_mqtt >= cfg["poll_interval"]:
            last_mqtt = now
            mqtt.publish_state(best_per_direction(), current_mode)

        # ── Render ────────────────────────────────────────────────────────────
        if current_mode == MODE_OFF:
            time.sleep(0.5)
            continue

        # Advance pulse phase using actual elapsed time so the animation
        # runs at a consistent speed regardless of how long API calls took
        now_render   = time.time()
        dt_pulse     = now_render - last_pulse_time
        last_pulse_time = now_render
        pulse_phase += cfg["pulse_speed"] * dt_pulse
        pulse_brightness = 0.4 + 0.6 * (0.5 + 0.5 * math.sin(2 * math.pi * pulse_phase))

        renderer.tick()
        buses      = list(active_buses.values())
        is_pulsing = any(b.state == "at_stop" for b in buses)

        for inst in instances:
            target_leds = renderer.render_for_instance(inst, buses, pulse_brightness)
            if target_leds != prev_leds[inst.label_slug] or is_pulsing:
                inst.send(target_leds)
                prev_leds[inst.label_slug] = [list(led) for led in target_leds]

        time.sleep(0.05)  # 20Hz render loop


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Stopped.")
        sys.exit(0)
