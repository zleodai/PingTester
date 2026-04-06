# Ping Monitor

Local script that continuously pings both your gateway and an internet target, logs samples to CSV, and serves a live dashboard you can open in a browser.

## Run

```powershell
python .\ping_dashboard.py 8.8.8.8
```

Then open `http://127.0.0.1:8765` in a browser.

The dashboard now includes:

- A condensed side-by-side dashboard with the gateway view on the left and the internet view on the right.
- A dedicated gateway view that pings `10.0.0.1`.
- A dedicated internet view that defaults to `8.8.8.8` and can be retargeted live from the browser.
- Separate worker processes for gateway and internet ping collection so the two streams are isolated.
- Startup replay from the CSV logs so the latest compatible session is restored after restarting the app.
- A latency timeline with labeled `ms` axis values.
- A packet-loss event timeline.
- A browser control to change the internet ping target live. Changing target starts a fresh internet session.
- A browser control to change the polling rate live in milliseconds.
- A browser control to change the high-ping threshold live in milliseconds.
- A shared timeline window selector with `1 minute`, `5 minutes`, `10 minutes`, `30 minutes`, `1 hour`, and `All data`.
- Horizontally scrollable timelines where the timed windows control the time scale per screen, while `All data` fits the full session into the chart.

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
python .\ping_dashboard.py 1.1.1.1 --interval 0.5 --timeout-ms 1500 --high-ping-threshold-ms 200 --port 9000 --log-file .\router-ping.csv
```

- `target`: Internet hostname or IP to ping. Defaults to `8.8.8.8`.
- `--interval`: Seconds between samples.
- `--timeout-ms`: Timeout for each ping.
- `--high-ping-threshold-ms`: Threshold for high-ping events.
- `--port`: Local HTTP port for the dashboard.
- `--history-size`: Maximum in-memory samples to retain. Use `0` for the full session.
- `--log-file`: Base CSV path. The script writes separate `-gateway` and `-internet` files.
