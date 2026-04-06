# Ping Monitor

Local script that continuously pings both your gateway and an internet target, stores samples in a local SQLite database, and serves a live dashboard in your browser.

## Requirements

- Python 3

No separate database server is required. The app writes to a local SQLite file, which is a better fit for a local-only dashboard.

## Run

Recommended:

```powershell
.\run_ping_dashboard.ps1 -InternetTarget 8.8.8.8
```

Then open `http://127.0.0.1:8765` in a browser.

If you want to run the Python script directly:

```powershell
py -3.12 .\ping_dashboard.py 8.8.8.8 --database-file .\ping-monitor.db
```

If your execution policy blocks local scripts:

```powershell
powershell -ExecutionPolicy Bypass -File .\run_ping_dashboard.ps1 -InternetTarget 8.8.8.8
```

## Dashboard

- Gateway monitor pinned to `10.0.0.1`
- Internet monitor defaults to `8.8.8.8` and can be retargeted live
- Separate worker processes for gateway and internet collection
- Local SQLite-backed session and sample storage
- Restart replay by resuming the latest matching session from the local database
- Fixed time-window charts for `1 minute`, `5 minutes`, `10 minutes`, `30 minutes`, `1 hour`, `2 hours`, `4 hours`, `12 hours`, and `24 hours`
- Live view by default, with `Older` and `Newer` buttons to step through earlier time slots
- Windowed queries so the browser only loads the selected time slot instead of the whole history

## Options

```powershell
.\run_ping_dashboard.ps1 -InternetTarget 1.1.1.1 -Interval 0.5 -TimeoutMs 1500 -HighPingThresholdMs 200 -Port 9000 -DatabaseFile .\custom-ping-monitor.db
```

- `target`: Internet hostname or IP to ping. Defaults to `8.8.8.8`.
- `--interval`: Seconds between samples.
- `--timeout-ms`: Timeout for each ping.
- `--high-ping-threshold-ms`: Threshold for high-ping status and the latency threshold line.
- `--port`: Local HTTP port for the dashboard.
- `--database-file`: SQLite database file path.
