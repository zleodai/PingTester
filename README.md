# Ping Monitor

Local script that continuously pings a target, logs every sample to CSV, and serves a live dashboard you can open in a browser.

## Run

```powershell
python .\ping_dashboard.py 8.8.8.8
```

Then open `http://127.0.0.1:8765` in a browser.

The dashboard now includes:

- A latency timeline with labeled `ms` axis values.
- A packet-loss event timeline.
- A high-ping event timeline for responses above the configured threshold.
- A browser control to change the polling rate live in milliseconds.
- A browser control to change the high-ping threshold live in milliseconds.
- A shared timeline window selector with `1 minute`, `5 minutes`, `10 minutes`, `30 minutes`, `1 hour`, and `All data`.

If you want a PowerShell entrypoint:

```powershell
.\run_ping_dashboard.ps1 8.8.8.8
```

If your execution policy blocks local scripts, use:

```powershell
powershell -ExecutionPolicy Bypass -File .\run_ping_dashboard.ps1 8.8.8.8
```

## Options

```powershell
python .\ping_dashboard.py 192.168.1.1 --interval 0.5 --timeout-ms 1500 --high-ping-threshold-ms 200 --port 9000 --log-file .\router-ping.csv
```

- `target`: Hostname or IP to ping. Defaults to `8.8.8.8`.
- `--interval`: Seconds between samples.
- `--timeout-ms`: Timeout for each ping.
- `--high-ping-threshold-ms`: Threshold for high-ping events.
- `--port`: Local HTTP port for the dashboard.
- `--history-size`: Maximum in-memory samples to retain. Use `0` for the full session.
- `--log-file`: CSV file that stores all samples for later review.
