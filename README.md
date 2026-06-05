# Canberra Bus Tracker — Home Assistant Addon Repository

This repository contains the **Canberra Bus Tracker** Home Assistant addon.

## Installation

1. In Home Assistant go to **Settings → Add-ons → Add-on Store**
2. Click **⋮ (three dots)** in the top right → **Repositories**
3. Add this repository URL:
   ```
   https://github.com/YOUR_USERNAME/ha-addon-canberra-bus-tracker
   ```
4. Click **Add**, then close the dialog
5. **Canberra Bus Tracker** will appear in the addon store — click it and install

## Addons

### Canberra Bus Tracker

Tracks buses approaching a Transport Canberra stop from both directions
and displays proximity on one or more WLED LED strips, with per-route
colours, smooth position-based fading, GPS dead-reckoning, and full
Home Assistant integration via MQTT.

**Features:**
- Dual stop tracking (one stop ID per direction)
- GTFS static data for reliable trip identification
- GPS dead-reckoning for smooth 20Hz LED animation
- Per-route colours with final-segment colour fade
- Multiple WLED instances with per-instance HA brightness control
- WLED v16+ segment support for background effect blending
- MQTT auto-discovery entities (switch, sensors, binary sensor)
- Running / coasting / off mode switching
- Configurable walk-time offset

See the [addon documentation](canberra_bus_tracker/README.md) for full setup instructions.

## Requirements

- Home Assistant OS with Supervisor
- WLED v16 or later
- Transport Canberra GTFS-R API key
  (register at https://anypoint.mulesoft.com/exchange/portals/act-government-9/)

## Support

Please open an issue on GitHub for bug reports or feature requests.
