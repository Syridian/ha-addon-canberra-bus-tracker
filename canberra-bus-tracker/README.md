# Canberra Bus Tracker — Home Assistant Addon v0.3.15

Tracks buses approaching a Transport Canberra stop from both directions and
displays their proximity on one or more WLED LED strips, with per-route colours,
smooth position-based fading, GPS dead-reckoning, and full Home Assistant
integration via MQTT.

---

## How it works

The addon pulls two real-time data feeds from Transport Canberra's GTFS-R API:

**Trip updates** (every 15s) tell it which buses are heading toward your stops
and how many stops away they are. Direction of travel is determined by which
stop the bus is heading toward — no GTFS direction_id or headsign guessing needed.

**Vehicle positions** (every 15s) provide GPS coordinates for each tracked bus.
Between GPS updates, the addon dead-reckons the bus forward using its last known
speed, giving smooth continuous LED animation at 20Hz.

Bus position is mapped to a 0.0–1.0 fraction along the LED strip. The strip is
divided into 5 virtual groups (3-stops, 2-stops, 1-stop, at-stop, departed) and
the lit point moves smoothly through them as the bus approaches.

Two stop IDs are configured — one per direction. Direction A buses are those
heading toward `stop_id_a`, Direction B buses toward `stop_id_b`. Swap the two
IDs to reverse which direction appears on which end of the strip.

---

## LED layout

```
Direction A:  [3-stops]──[2-stops]──[1-stop──fade──►]──[AT STOP✦]──[DEPARTED]
              LED 0        LED 1       LED 2               LED 3       LED 4

Direction B:  [DEPARTED]──[AT STOP✦]──[◄──fade──1-stop]──[2-stops]──[3-stops]
              LED 0        LED 1            LED 2           LED 3       LED 4
```

For strips with more than 5 LEDs each virtual state expands to a group of
physical LEDs. Extra LEDs (non-multiples of 5) are distributed centre-outward
so the at-stop and 1-stop groups get the extras first.

- **Approaching buses** move as a smooth point of light in their route colour
- **1-stop → at-stop transition** fades from route colour toward white as the
  bus closes in, driven by GPS position
- **At stop** the entire at-stop group pulses in route colour
- **Departed** the entire departed group glows solid red
- **Two buses on the same LED** alternate between their route colours at
  `flash_interval_ms`
- Both directions are shown simultaneously if buses are active in both

---

## Requirements

- Home Assistant OS with the Supervisor (not Container or Core installs)
- The **Terminal & SSH** addon or **Samba** addon for file access
- The **Mosquitto broker** addon (for MQTT integration)
- One or more WLED devices running **WLED v16 or later**
- A Transport Canberra GTFS-R API key (free, see setup below)

---

## Installation

### Step 1 — Copy the zip to HA

Using Samba, copy `bus_stop_wled.zip` to the `tmp` folder on your HA share.
It will appear at `/config/tmp/bus_stop_wled.zip` on the HA filesystem.

### Step 2 — Extract and install

Open the **Terminal & SSH** addon in HA and run:

```bash
cd /config/tmp
unzip -o bus_stop_wled.zip
bash install.sh
```

This creates `/addons/bus_stop_wled/` with all required files.

### Step 3 — Refresh the addon store

In HA go to:
**Settings → Add-ons → Add-on Store → ⋮ (top right) → Check for updates**

**Canberra Bus Tracker** will appear under **Local add-ons**. Click it then
click **Install** — this builds the Docker container, which takes 1–2 minutes.

### Step 4 — Get a Transport Canberra API key

1. Go to https://anypoint.mulesoft.com/exchange/portals/act-government-9/
2. Create a MuleSoft Anypoint account if you don't have one
3. Find **Transport Canberra GTFS** and click **Request access**
4. Click **Create a new application** (name it anything, e.g. "Canberra Bus Tracker")
5. Select the free SLA tier and submit — approval is usually instant
6. Go to **API Manager → Client Applications**, find your app, and note both
   the **Client ID** and **Client Secret**

Your `api_key` in config is `clientid:clientsecret` (with a colon between them).

> Keep these credentials somewhere safe — they can be hard to retrieve from
> the portal later.

### Step 5 — Find your stop IDs and route IDs

Download the GTFS static zip:
```
https://www.transport.act.gov.au/googletransit/google_transit.zip
```

Open it and check these files:

**`stops.txt`** — search the `stop_name` column for your stop. Each physical
stop (each side of the road) has its own `stop_id`. Note the ID for each
direction — these become `stop_id_a` and `stop_id_b`.

**`routes.txt`** — find your routes by `route_short_name` (the number on the
front of the bus). Note the `route_id` for each. There may be multiple entries
per route number (weekday/weekend variants) — note all of them.

> `stop_times.txt` is very large — don't open it in Excel. The addon parses
> it automatically on startup.

### Step 6 — Configure the addon

In HA go to **Settings → Add-ons → Canberra Bus Tracker → Configuration**.

At minimum you need:

```yaml
api_key: "your_client_id:your_client_secret"
stop_id_a: "1084"   # stop for Direction A (left side of strip)
stop_id_b: "1085"   # stop for Direction B (right side of strip)
```

See the full configuration reference below for all options.

### Step 7 — Set up WLED

The addon automatically creates its own segment (default ID 9) on each WLED
device the first time it starts. You don't need to configure segments manually.

After the segment is created you can set a **blend mode** on it in the WLED UI
to allow a background effect to show through. The addon never modifies the
segment after creation, so your blend mode is preserved across restarts.

> If you later change `led_offset` or `num_leds` in config, delete the segment
> in the WLED UI first so the addon recreates it with the correct boundaries.

### Step 8 — Enable MQTT

1. Install and start the **Mosquitto broker** addon if not already running
2. Set `mqtt_enabled: true` in the addon config
3. Set `mqtt_user` and `mqtt_password` to match your Mosquitto credentials
4. Start the addon — entities auto-discover in HA within a few seconds

### Step 9 — Start and verify

Go to the addon **Info** tab and click **Start**, then click the **Log** tab.
You should see:

```
Canberra Bus Tracker v3.5 starting
Stop A: 1084 | Stop B: 1085 | WLED instances: 1
Loading GTFS static data...
GTFS static: 18158 trips, 2439 stops
Entering main loop — mode: running
GET /gtfs/data/gtfs/v2/trip-updates.pb HTTP/1.1" 200
GET /gtfs/data/gtfs/v2/vehicle-positions.pb HTTP/1.1" 200
```

Enable **Start on boot** on the Info tab once you're happy it's working.

---

## Configuration reference

### Core settings

| Option | Description | Default |
|--------|-------------|---------|
| `api_key` | TC API credentials as `clientid:clientsecret` | *(required)* |
| `api_base_url` | GTFS-R realtime base URL | `https://transport.api.act.gov.au/gtfs/data/gtfs/v2` |
| `gtfs_static_url` | URL for GTFS static zip | TC default |
| `stop_id_a` | Stop ID for Direction A (left side of strip) | *(required)* |
| `stop_id_b` | Stop ID for Direction B (right side of strip) | *(required)* |
| `route_filter` | Comma-separated route IDs to track (blank = all routes) | *(all)* |

### Timing

| Option | Description | Default |
|--------|-------------|---------|
| `poll_interval` | Trip update poll interval in seconds | `15` |
| `gps_poll_interval` | Vehicle position poll interval in seconds | `15` |
| `gtfs_refresh_hours` | How often to re-download static data | `24` |
| `arrival_offset_seconds` | Lead time / walk time offset in seconds | `90` |
| `departed_timeout_seconds` | How long to show a departed bus before clearing | `60` |

`arrival_offset_seconds` advances the displayed position by approximately
`seconds ÷ 30` stops. Use it to compensate for feed lag (typically 30–90s)
or as a walk-time offset — set it to your walking time to the stop so that
"at stop" means "leave now." Always clamped to [0, 3]; can never show a bus
as artificially departed.

### WLED instances

Add one entry per physical WLED device. Leave `ip` blank to disable WLED
output entirely (MQTT still works).

```yaml
wled_instances:
  - ip: "192.168.1.42"
    led_offset: 0
    num_leds: 10
    brightness: 80
    segment_id: 9
    label: "Living Room"
  - ip: "192.168.1.55"
    led_offset: 0
    num_leds: 5
    brightness: 100
    segment_id: 9
    label: "Outside"
```

| Field | Description | Default |
|-------|-------------|---------|
| `ip` | WLED device IP address (blank = disabled) | *(blank)* |
| `led_offset` | Physical strip index of first bus LED | `0` |
| `num_leds` | Number of LEDs in the bus segment (minimum 5) | `5` |
| `brightness` | Segment brightness 0–100% | `100` |
| `segment_id` | WLED segment ID to create/use | `9` |
| `label` | Name shown in HA — becomes the brightness entity name | `"WLED 1"` |

**Segment ID limits:** ESP8266 supports IDs 0–9, ESP32 supports 0–31. The
default of 9 sits at the top of the ESP8266 range, leaving 0–8 free for
background effects.

### Colours

| Option | Description | Default |
|--------|-------------|---------|
| `color_at_stop` | Fade target / pulse colour when bus is at stop | `[255, 255, 255]` |
| `color_departed` | Colour when bus has departed | `[255, 0, 0]` |
| `color_default` | Fallback colour for routes not in `route_colors` | `[0, 120, 255]` |
| `route_colors` | Per-route RGB colours `{"route_id": [R, G, B]}` | `{}` |
| `pulse_speed` | At-stop pulse frequency in Hz | `1.5` |
| `flash_interval_ms` | Flash interval when two buses share an LED (ms) | `500` |

Example route colours — use the exact `route_id` values from `routes.txt`:
```yaml
route_colors:
  "route-5-1": [0, 200, 100]
  "route-5-2": [0, 200, 100]
  "route-76-1": [180, 0, 255]
  "route-76-2": [180, 0, 255]
```

### MQTT

| Option | Description | Default |
|--------|-------------|---------|
| `mqtt_enabled` | Enable MQTT publishing and control | `false` |
| `mqtt_host` | MQTT broker hostname | `core-mosquitto` |
| `mqtt_port` | MQTT broker port | `1883` |
| `mqtt_user` | MQTT username | *(blank)* |
| `mqtt_password` | MQTT password | *(blank)* |
| `mqtt_topic_prefix` | Root topic for all addon messages | `bus_stop` |

---

## Home Assistant entities

With MQTT enabled, these entities auto-discover in HA under the
**Canberra Bus Tracker** device:

| Entity | Type | Values |
|--------|------|--------|
| `switch.bus_stop` | Switch | ON = running, OFF = triggers coasting |
| `sensor.bus_stop_mode` | Sensor | `running` / `coasting` / `off` |
| `sensor.direction_a_bus` | Sensor | `none` `3_stops` `2_stops` `1_stop` `at_stop` `departed` |
| `sensor.direction_b_bus` | Sensor | same as above |
| `binary_sensor.bus_due` | Binary sensor | ON when either direction is at `1_stop` or `at_stop` |
| `number.<label>_brightness` | Number (0–100%) | Per-instance brightness control |

Each direction sensor carries these attributes: `route_id`, `trip_id`,
`headsign`, `stop_id`, `stops_away`, `seg_fraction` (0.0–1.0 GPS position
on final segment), `speed_ms`, `direction`.

### Mode behaviour

- **Running** — normal operation, new buses tracked as they appear in the feed
- **Coasting** — triggered by switch OFF or addon stop (SIGTERM); no new buses
  added, existing buses track through to completion, then transitions to off
- **Off** — all LEDs dark, no API polling

The switch shows ON when running, OFF when off. During coasting the switch
shows OFF but `sensor.bus_stop_mode` shows `coasting` so automations can
tell the difference.

### Example automations

Turn on at 7am, off at 9am:
```yaml
automation:
  - alias: "Bus stop on"
    trigger:
      platform: time
      at: "07:00:00"
    action:
      service: switch.turn_on
      target:
        entity_id: switch.bus_stop

  - alias: "Bus stop off"
    trigger:
      platform: time
      at: "09:00:00"
    action:
      service: switch.turn_off
      target:
        entity_id: switch.bus_stop
```

Announce when a bus is due:
```yaml
automation:
  - alias: "Announce bus due"
    trigger:
      platform: state
      entity_id: binary_sensor.bus_due
      to: "on"
    action:
      service: tts.speak
      data:
        message: >
          Bus {{ state_attr('sensor.direction_a_bus', 'route_id') }}
          is approaching. Leave now.
```

Dim a WLED instance at night:
```yaml
automation:
  - alias: "Dim outside strip at night"
    trigger:
      platform: time
      at: "21:00:00"
    action:
      service: number.set_value
      target:
        entity_id: number.outside_brightness
      data:
        value: 30
```

---

## Running standalone (without Home Assistant)

Useful for testing before installing as an addon.

```bash
pip install gtfs-realtime-bindings requests paho-mqtt
python3 bus_wled.py
```

The script prompts for API key (`clientid:clientsecret`), Stop ID A, Stop ID B,
and WLED IP. Press Enter to skip WLED IP if you don't have hardware yet —
the script will still poll the API and log bus positions.

Enable verbose per-trip logging:
```bash
DEBUG=1 GTFS_CACHE=./gtfs_static.zip python3 bus_wled.py
```

On Windows (PowerShell):
```powershell
$env:DEBUG = "1"
$env:GTFS_CACHE = "./gtfs_static.zip"
python3 bus_wled.py
```

---

## Troubleshooting

**No buses appearing in the log**
Run with `DEBUG=1`. Check that `stop_id_a` and `stop_id_b` are correct — confirm
them against `stops.txt` in the GTFS static zip. If `route_filter` is set,
verify the route IDs match exactly what's in `routes.txt`.

**Both buses showing as Direction A (or B)**
The two stop IDs may be the same stop or one may not be served by your routes.
Verify both stop IDs in `stops.txt` and confirm buses actually stop at both.

**Direction A and B are the wrong way around**
Swap `stop_id_a` and `stop_id_b` in config.

**WLED segment not created**
Check the addon log for `segment setup failed`. Ensure the WLED device is
reachable from HA and running WLED v16+. Delete any existing segment with the
same ID in WLED UI and restart the addon.

**Brightness changes from HA not applying**
Check MQTT is enabled and the broker is running. Verify the topic
`bus_stop/wled/<label_slug>/brightness/set` in MQTT Explorer.

**401 Unauthorized errors**
Check your `api_key` is in `clientid:clientsecret` format with a colon
between them. Regenerate credentials at the MuleSoft portal if needed.

**Feed outages**
Transport Canberra's GTFS-R feed has occasional outages. The addon logs
errors and retries on the next poll cycle without crashing. The GTFS static
zip is cached locally so static data remains available during outages.

**Changing led_offset or num_leds**
Delete the bus segment in the WLED UI first, then restart the addon.
The addon only creates the segment if it doesn't already exist.
