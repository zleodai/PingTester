#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
import subprocess
import threading
from dataclasses import dataclass, field
from datetime import datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any


TIME_REGEX = re.compile(r"time[=<]\s*(\d+)\s*ms", re.IGNORECASE)
DEFAULT_HIGH_PING_THRESHOLD_MS = 150


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


@dataclass
class Sample:
    sequence: int
    timestamp: str
    success: bool
    latency_ms: int | None
    message: str
    return_code: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "sequence": self.sequence,
            "timestamp": self.timestamp,
            "success": self.success,
            "latency_ms": self.latency_ms,
            "message": self.message,
            "return_code": self.return_code,
        }


@dataclass
class PingState:
    target: str
    interval_seconds: float
    timeout_ms: int
    high_ping_threshold_ms: int
    max_history_samples: int
    log_file: Path
    lock: threading.Lock = field(default_factory=threading.Lock)
    history: list[Sample] = field(default_factory=list)
    success_latencies: list[int] = field(default_factory=list)
    sequence: int = 0
    sent: int = 0
    received: int = 0
    lost: int = 0
    consecutive_failures: int = 0
    started_at: str = field(default_factory=now_iso)

    def add_sample(self, sample: Sample) -> None:
        with self.lock:
            self.sent += 1
            if sample.success:
                self.received += 1
                self.consecutive_failures = 0
                if sample.latency_ms is not None:
                    self.success_latencies.append(sample.latency_ms)
            else:
                self.lost += 1
                self.consecutive_failures += 1
            self.history.append(sample)
            if self.max_history_samples > 0 and len(self.history) > self.max_history_samples:
                overflow = len(self.history) - self.max_history_samples
                del self.history[:overflow]

    def next_sequence(self) -> int:
        with self.lock:
            self.sequence += 1
            return self.sequence

    def get_interval_seconds(self) -> float:
        with self.lock:
            return self.interval_seconds

    def set_interval_ms(self, interval_ms: int) -> None:
        if interval_ms <= 0:
            raise ValueError("interval_ms must be greater than 0")
        with self.lock:
            self.interval_seconds = interval_ms / 1000.0

    def get_high_ping_threshold_ms(self) -> int:
        with self.lock:
            return self.high_ping_threshold_ms

    def set_high_ping_threshold_ms(self, threshold_ms: int) -> None:
        if threshold_ms <= 0:
            raise ValueError("threshold_ms must be greater than 0")
        with self.lock:
            self.high_ping_threshold_ms = threshold_ms

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            samples = list(self.history)
            latencies = [sample.latency_ms for sample in samples if sample.latency_ms is not None]
            high_ping_events_total = sum(
                1 for latency in self.success_latencies if latency > self.high_ping_threshold_ms
            )
            last = samples[-1] if samples else None
            packet_loss = (self.lost / self.sent * 100.0) if self.sent else 0.0
            avg = round(sum(latencies) / len(latencies), 2) if latencies else None
            minimum = min(latencies) if latencies else None
            maximum = max(latencies) if latencies else None
            return {
                "target": self.target,
                "interval_seconds": self.interval_seconds,
                "timeout_ms": self.timeout_ms,
                "high_ping_threshold_ms": self.high_ping_threshold_ms,
                "started_at": self.started_at,
                "sent": self.sent,
                "received": self.received,
                "lost": self.lost,
                "packet_loss_events_total": self.lost,
                "high_ping_events_total": high_ping_events_total,
                "packet_loss_percent": round(packet_loss, 2),
                "consecutive_failures": self.consecutive_failures,
                "current_latency_ms": last.latency_ms if last else None,
                "last_status": last.message if last else "Waiting for first sample",
                "last_sample_at": last.timestamp if last else None,
                "min_latency_ms": minimum,
                "avg_latency_ms": avg,
                "max_latency_ms": maximum,
                "history": [sample.to_dict() for sample in samples],
            }


def ensure_log_file(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["timestamp", "sequence", "target", "success", "latency_ms", "message", "return_code"])


def append_log(path: Path, target: str, sample: Sample) -> None:
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                sample.timestamp,
                sample.sequence,
                target,
                sample.success,
                sample.latency_ms if sample.latency_ms is not None else "",
                sample.message,
                sample.return_code,
            ]
        )


def parse_ping_output(output: str, return_code: int) -> tuple[bool, int | None, str]:
    latency_match = TIME_REGEX.search(output)
    if latency_match:
        latency = int(latency_match.group(1))
        return True, latency, f"Reply in {latency} ms"

    cleaned_lines = [line.strip() for line in output.splitlines() if line.strip()]
    if cleaned_lines:
        for line in reversed(cleaned_lines):
            if "timed out" in line.lower():
                return False, None, "Request timed out"
        return False, None, cleaned_lines[-1]

    return False, None, f"Ping exited with code {return_code}"


def run_ping(target: str, timeout_ms: int) -> tuple[bool, int | None, str, int]:
    completed = subprocess.run(
        ["ping", "-n", "1", "-w", str(timeout_ms), target],
        capture_output=True,
        text=True,
        check=False,
    )
    output = completed.stdout or completed.stderr or ""
    success, latency, message = parse_ping_output(output, completed.returncode)
    return success, latency, message, completed.returncode


def ping_loop(state: PingState, stop_event: threading.Event) -> None:
    ensure_log_file(state.log_file)
    while not stop_event.is_set():
        sequence = state.next_sequence()
        timestamp = now_iso()
        success, latency, message, return_code = run_ping(state.target, state.timeout_ms)
        sample = Sample(
            sequence=sequence,
            timestamp=timestamp,
            success=success,
            latency_ms=latency,
            message=message,
            return_code=return_code,
        )
        state.add_sample(sample)
        append_log(state.log_file, state.target, sample)
        stop_event.wait(state.get_interval_seconds())


HTML_PAGE = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Ping Monitor</title>
  <style>
    :root {
      color-scheme: light;
      --panel: rgba(255, 252, 245, 0.94);
      --panel-border: rgba(53, 42, 26, 0.12);
      --ink: #1f1a14;
      --muted: #756754;
      --accent: #005f73;
      --accent-soft: rgba(0, 95, 115, 0.12);
      --good: #2a9d8f;
      --warn: #e76f51;
      --bad: #c44536;
      --grid: rgba(53, 42, 26, 0.10);
      --shadow: 0 18px 40px rgba(53, 42, 26, 0.10);
    }

    * {
      box-sizing: border-box;
    }

    body {
      margin: 0;
      min-height: 100vh;
      font-family: Georgia, "Times New Roman", serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(0, 95, 115, 0.10), transparent 34%),
        radial-gradient(circle at top right, rgba(233, 196, 106, 0.18), transparent 28%),
        linear-gradient(160deg, #f9f6ef 0%, #efe7d5 100%);
    }

    .shell {
      max-width: 1280px;
      margin: 0 auto;
      padding: 32px 20px 48px;
    }

    .hero {
      display: grid;
      gap: 8px;
      margin-bottom: 24px;
    }

    .eyebrow {
      font-family: "Trebuchet MS", sans-serif;
      letter-spacing: 0.18em;
      text-transform: uppercase;
      font-size: 12px;
      color: var(--muted);
    }

    h1 {
      margin: 0;
      font-size: clamp(2.2rem, 5vw, 4.3rem);
      line-height: 0.95;
      font-weight: 600;
    }

    .subtitle {
      margin: 0;
      max-width: 780px;
      color: var(--muted);
      font-size: 1rem;
    }

    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 14px;
      margin-bottom: 18px;
    }

    .card {
      background: var(--panel);
      border: 1px solid var(--panel-border);
      border-radius: 22px;
      padding: 18px;
      box-shadow: var(--shadow);
      backdrop-filter: blur(10px);
    }

    .metric-label {
      display: block;
      font-size: 0.8rem;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      color: var(--muted);
      margin-bottom: 10px;
      font-family: "Trebuchet MS", sans-serif;
    }

    .metric-value {
      font-size: clamp(1.7rem, 4vw, 2.8rem);
      line-height: 1;
    }

    .metric-subtext {
      margin-top: 8px;
      color: var(--muted);
      font-size: 0.92rem;
      font-family: "Trebuchet MS", sans-serif;
    }

    .status-ok {
      color: var(--good);
    }

    .status-warn {
      color: var(--warn);
    }

    .status-bad {
      color: var(--bad);
    }

    .section-grid {
      display: grid;
      grid-template-columns: 1fr;
      gap: 18px;
      margin-bottom: 18px;
    }

    .dual-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 18px;
      margin-bottom: 18px;
    }

    .detail-grid {
      display: grid;
      grid-template-columns: minmax(0, 2fr) minmax(300px, 1fr);
      gap: 18px;
    }

    .chart-head,
    .table-head {
      display: flex;
      justify-content: space-between;
      align-items: baseline;
      gap: 12px;
      margin-bottom: 16px;
    }

    .chart-title,
    .table-title {
      margin: 0;
      font-size: 1.25rem;
    }

    .chart-note,
    .table-note {
      color: var(--muted);
      font-size: 0.92rem;
    }

    .chart-shell {
      display: grid;
      grid-template-columns: 74px minmax(0, 1fr);
      gap: 12px;
      align-items: stretch;
    }

    .axis-y {
      display: flex;
      flex-direction: column;
      justify-content: space-between;
      align-items: flex-end;
      padding: 8px 0 30px;
      color: var(--muted);
      font-size: 0.82rem;
      font-family: "Trebuchet MS", sans-serif;
    }

    .axis-y span,
    .axis-x span {
      white-space: nowrap;
    }

    .plot-stack {
      display: grid;
      gap: 10px;
    }

    .plot-area {
      height: 320px;
      border-radius: 18px;
      border: 1px solid var(--panel-border);
      overflow: hidden;
      position: relative;
      background:
        linear-gradient(to bottom, rgba(0, 95, 115, 0.07), transparent 48%),
        repeating-linear-gradient(to bottom, transparent 0 63px, var(--grid) 63px 64px);
    }

    .plot-area.compact {
      height: 220px;
      background:
        linear-gradient(to bottom, rgba(0, 95, 115, 0.05), transparent 38%),
        repeating-linear-gradient(to bottom, transparent 0 53px, var(--grid) 53px 54px);
    }

    .axis-x {
      display: flex;
      justify-content: space-between;
      color: var(--muted);
      font-size: 0.82rem;
      font-family: "Trebuchet MS", sans-serif;
      padding: 0 6px;
    }

    svg {
      width: 100%;
      height: 100%;
      display: block;
    }

    .chart-fallback {
      position: absolute;
      inset: 0;
      display: grid;
      place-items: center;
      color: var(--muted);
      font-style: italic;
    }

    table {
      width: 100%;
      border-collapse: collapse;
      font-family: "Trebuchet MS", sans-serif;
      font-size: 0.92rem;
    }

    th,
    td {
      text-align: left;
      padding: 10px 0;
      border-bottom: 1px solid var(--grid);
    }

    th {
      font-size: 0.76rem;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      color: var(--muted);
    }

    .pill {
      display: inline-block;
      padding: 3px 10px;
      border-radius: 999px;
      font-size: 0.8rem;
      background: var(--accent-soft);
      color: var(--accent);
      font-family: "Trebuchet MS", sans-serif;
    }

    .pill.bad {
      background: rgba(196, 69, 54, 0.12);
      color: var(--bad);
    }

    .aside-copy {
      display: grid;
      gap: 10px;
      color: var(--muted);
      font-size: 0.96rem;
    }

    code {
      font-family: Consolas, monospace;
      font-size: 0.92em;
      background: rgba(53, 42, 26, 0.08);
      padding: 0.18em 0.42em;
      border-radius: 6px;
    }

    input,
    select,
    button {
      font: inherit;
    }

    input,
    select {
      width: 100%;
      padding: 10px 12px;
      border-radius: 10px;
      border: 1px solid var(--panel-border);
      background: rgba(255, 255, 255, 0.7);
      color: var(--ink);
    }

    button {
      padding: 10px 12px;
      border: 0;
      border-radius: 10px;
      background: var(--accent);
      color: #fff;
      cursor: pointer;
    }

    @media (max-width: 980px) {
      .dual-grid,
      .detail-grid {
        grid-template-columns: 1fr;
      }

      .chart-shell {
        grid-template-columns: 58px minmax(0, 1fr);
      }
    }

    @media (max-width: 640px) {
      .plot-area {
        height: 260px;
      }

      .plot-area.compact {
        height: 200px;
      }
    }
  </style>
</head>
<body>
  <div class="shell">
    <section class="hero">
      <span class="eyebrow">Live Network Health</span>
      <h1>Ping Monitor</h1>
      <p class="subtitle">Continuous latency sampling with dedicated event timelines for packet loss and high ping. Both the high-ping threshold and the polling rate can be changed directly from this page.</p>
    </section>

    <section class="grid">
      <article class="card">
        <span class="metric-label">Target</span>
        <div class="metric-value" id="target">-</div>
      </article>
      <article class="card">
        <span class="metric-label">Current</span>
        <div class="metric-value" id="current-latency">-</div>
      </article>
      <article class="card">
        <span class="metric-label">Average</span>
        <div class="metric-value" id="avg-latency">-</div>
      </article>
      <article class="card">
        <span class="metric-label">Packet Loss</span>
        <div class="metric-value" id="packet-loss">-</div>
        <div class="metric-subtext" id="packet-loss-events">0 total loss events</div>
      </article>
      <article class="card">
        <span class="metric-label">High Ping Events</span>
        <div class="metric-value" id="high-ping-events-total">0</div>
        <div class="metric-subtext" id="high-ping-threshold">Threshold: > 150 ms</div>
      </article>
      <article class="card">
        <span class="metric-label">Status</span>
        <div class="metric-value" id="status-text">Waiting</div>
        <div class="metric-subtext" id="status-detail">No samples yet</div>
      </article>
    </section>

    <section class="section-grid">
      <article class="card">
        <div class="chart-head">
          <div>
            <h2 class="chart-title">Latency Timeline</h2>
            <div class="chart-note" id="latency-note">Collecting samples...</div>
          </div>
          <div class="pill" id="sample-count">0 samples</div>
        </div>
        <div class="chart-shell">
          <div class="axis-y" id="latency-y-axis"></div>
          <div class="plot-stack">
            <div class="plot-area">
              <svg id="latency-chart" viewBox="0 0 900 320" preserveAspectRatio="none" aria-label="Latency timeline"></svg>
              <div class="chart-fallback" id="latency-fallback">Waiting for enough data to draw the timeline.</div>
            </div>
            <div class="axis-x" id="latency-x-axis"></div>
          </div>
        </div>
      </article>
    </section>

    <section class="dual-grid">
      <article class="card">
        <div class="chart-head">
          <div>
            <h2 class="chart-title">Packet Loss Timeline</h2>
            <div class="chart-note" id="loss-note">Loss events across the visible sample window.</div>
          </div>
          <div class="pill" id="loss-window-count">0 visible events</div>
        </div>
        <div class="chart-shell">
          <div class="axis-y" id="loss-y-axis"></div>
          <div class="plot-stack">
            <div class="plot-area compact">
              <svg id="loss-chart" viewBox="0 0 900 220" preserveAspectRatio="none" aria-label="Packet loss timeline"></svg>
              <div class="chart-fallback" id="loss-fallback">Waiting for packet-loss history.</div>
            </div>
            <div class="axis-x" id="loss-x-axis"></div>
          </div>
        </div>
      </article>

      <article class="card">
        <div class="chart-head">
          <div>
            <h2 class="chart-title">High Ping Timeline</h2>
            <div class="chart-note" id="high-ping-note">Event timeline for responses above the configured threshold.</div>
          </div>
          <div class="pill" id="high-ping-window-count">0 visible events</div>
        </div>
        <div class="chart-shell">
          <div class="axis-y" id="high-ping-y-axis"></div>
          <div class="plot-stack">
            <div class="plot-area compact">
              <svg id="high-ping-chart" viewBox="0 0 900 220" preserveAspectRatio="none" aria-label="High ping timeline"></svg>
              <div class="chart-fallback" id="high-ping-fallback">Waiting for high-ping history.</div>
            </div>
            <div class="axis-x" id="high-ping-x-axis"></div>
          </div>
        </div>
      </article>
    </section>

    <section class="detail-grid">
      <article class="card">
        <div class="table-head">
          <div>
            <h2 class="table-title">Recent Samples</h2>
            <div class="table-note">Latest 12 checks</div>
          </div>
          <div class="pill">Time axis in hh:mm:ss</div>
        </div>
        <table>
          <thead>
            <tr>
              <th>Time</th>
              <th>Latency</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody id="recent-samples"></tbody>
        </table>
      </article>

      <aside class="card">
        <div class="table-head">
          <div>
            <h2 class="table-title">Session</h2>
            <div class="table-note">Live summary and controls</div>
          </div>
        </div>
        <div class="aside-copy">
          <div>Started: <strong id="started-at">-</strong></div>
          <div>Last sample: <strong id="last-sample-at">-</strong></div>
          <div>Min / Max: <strong id="min-max">-</strong></div>
          <div>Sent / Received / Lost: <strong id="counts">-</strong></div>
          <div>Consecutive failures: <strong id="failures">0</strong></div>
          <div>Timeline window: <strong id="current-timeframe-label">All data</strong></div>
          <label for="timeframe-select">Set timeline window</label>
          <select id="timeframe-select">
            <option value="1m">1 minute</option>
            <option value="5m">5 minutes</option>
            <option value="10m">10 minutes</option>
            <option value="30m">30 minutes</option>
            <option value="1h">1 hour</option>
            <option value="all" selected>All data</option>
          </select>
          <div>Polling rate: <strong id="current-poll-rate">-</strong></div>
          <label for="poll-rate-input">Set polling rate (ms)</label>
          <input id="poll-rate-input" type="number" min="1" step="50" value="1000">
          <button id="poll-rate-save" type="button">Apply polling rate</button>
          <div id="poll-rate-feedback">Changes apply to the next ping cycle.</div>
          <div>High-ping threshold: <strong id="current-high-ping-threshold">-</strong></div>
          <label for="high-ping-threshold-input">Set high-ping threshold (ms)</label>
          <input id="high-ping-threshold-input" type="number" min="1" step="10" value="150">
          <button id="high-ping-threshold-save" type="button">Apply threshold</button>
          <div id="high-ping-threshold-feedback">Timeline and counters update immediately.</div>
          <div>Log file: <code id="log-file">-</code></div>
        </div>
      </aside>
    </section>
  </div>

  <script>
    const SVG_NS = "http://www.w3.org/2000/svg";
    const TIMEFRAME_OPTIONS = {
      "1m": { label: "1 minute", ms: 60 * 1000 },
      "5m": { label: "5 minutes", ms: 5 * 60 * 1000 },
      "10m": { label: "10 minutes", ms: 10 * 60 * 1000 },
      "30m": { label: "30 minutes", ms: 30 * 60 * 1000 },
      "1h": { label: "1 hour", ms: 60 * 60 * 1000 },
      "all": { label: "All data", ms: null },
    };
    let configSaveInFlight = false;

    function formatLatency(value) {
      return value === null || value === undefined ? "timeout" : `${value} ms`;
    }

    function formatTimeLabel(timestamp) {
      if (!timestamp) {
        return "-";
      }

      const date = new Date(timestamp);
      if (Number.isNaN(date.getTime())) {
        return timestamp;
      }

      return date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
    }

    function setMetric(id, value) {
      document.getElementById(id).textContent = value;
    }

    function setAxisLabels(id, labels) {
      const root = document.getElementById(id);
      root.innerHTML = "";
      labels.forEach((label) => {
        const span = document.createElement("span");
        span.textContent = label;
        root.appendChild(span);
      });
    }

    function getTimeAxisLabels(history) {
      if (!history.length) {
        return ["-", "-", "-"];
      }
      if (history.length === 1) {
        const label = formatTimeLabel(history[0].timestamp);
        return [label, label, label];
      }

      const middle = history[Math.floor((history.length - 1) / 2)];
      return [
        formatTimeLabel(history[0].timestamp),
        formatTimeLabel(middle.timestamp),
        formatTimeLabel(history[history.length - 1].timestamp),
      ];
    }

    function getSelectedTimeframeKey() {
      const key = document.getElementById("timeframe-select").value;
      return TIMEFRAME_OPTIONS[key] ? key : "all";
    }

    function getSelectedTimeframeLabel() {
      return TIMEFRAME_OPTIONS[getSelectedTimeframeKey()].label;
    }

    function filterHistoryByTimeframe(history) {
      const selected = TIMEFRAME_OPTIONS[getSelectedTimeframeKey()];
      if (!selected || selected.ms === null || history.length === 0) {
        return history;
      }

      const lastTimestamp = new Date(history[history.length - 1].timestamp).getTime();
      if (Number.isNaN(lastTimestamp)) {
        return history;
      }

      const cutoff = lastTimestamp - selected.ms;
      return history.filter((sample) => {
        const sampleTime = new Date(sample.timestamp).getTime();
        return !Number.isNaN(sampleTime) && sampleTime >= cutoff;
      });
    }

    function setStatus(currentLatencyMs, thresholdMs, lastStatus) {
      const el = document.getElementById("status-text");
      const detail = document.getElementById("status-detail");

      if (currentLatencyMs === null || currentLatencyMs === undefined) {
        el.textContent = "Timeout";
        el.className = "metric-value status-bad";
        detail.textContent = lastStatus;
        return;
      }

      if (currentLatencyMs > thresholdMs) {
        el.textContent = "High Ping";
        el.className = "metric-value status-warn";
        detail.textContent = `${currentLatencyMs} ms exceeds ${thresholdMs} ms`;
        return;
      }

      el.textContent = "Healthy";
      el.className = "metric-value status-ok";
      detail.textContent = lastStatus;
    }

    function renderTable(history, thresholdMs) {
      const body = document.getElementById("recent-samples");
      body.innerHTML = "";
      history.slice(-12).reverse().forEach((sample) => {
        const row = document.createElement("tr");
        let statusText = sample.message;
        let statusClass = "pill";

        if (!sample.success) {
          statusText = "Packet loss";
          statusClass = "pill bad";
        } else if ((sample.latency_ms ?? 0) > thresholdMs) {
          statusText = `High ping (${sample.latency_ms} ms)`;
        }

        row.innerHTML = `
          <td>${sample.timestamp}</td>
          <td>${formatLatency(sample.latency_ms)}</td>
          <td><span class="${statusClass}">${statusText}</span></td>
        `;
        body.appendChild(row);
      });
    }

    function makeSvgElement(name, attrs) {
      const el = document.createElementNS(SVG_NS, name);
      Object.entries(attrs).forEach(([key, value]) => el.setAttribute(key, String(value)));
      return el;
    }

    function drawGrid(svg, width, height, padding, lineCount) {
      for (let index = 0; index < lineCount; index += 1) {
        const ratio = lineCount === 1 ? 0 : index / (lineCount - 1);
        const y = padding + ratio * (height - padding * 2);
        svg.appendChild(
          makeSvgElement("line", {
            x1: padding,
            y1: y,
            x2: width - padding,
            y2: y,
            stroke: "rgba(53, 42, 26, 0.10)",
            "stroke-width": 1,
          })
        );
      }
    }

    function renderLatencyChart(history, timeoutMs, thresholdMs) {
      const chart = document.getElementById("latency-chart");
      const fallback = document.getElementById("latency-fallback");
      chart.innerHTML = "";
      setAxisLabels("latency-x-axis", getTimeAxisLabels(history));

      if (history.length < 2) {
        setAxisLabels("latency-y-axis", ["-", "-", "-"]);
        fallback.style.display = "grid";
        return;
      }

      fallback.style.display = "none";
      const width = 900;
      const height = 320;
      const padding = 28;
      const values = history.map((sample) => sample.latency_ms ?? timeoutMs);
      const maxValue = Math.max(...values, timeoutMs, thresholdMs, 10);
      setAxisLabels("latency-y-axis", [`${maxValue} ms`, `${Math.round(maxValue / 2)} ms`, "0 ms"]);
      drawGrid(chart, width, height, padding, 5);

      const usableWidth = width - padding * 2;
      const usableHeight = height - padding * 2;
      const points = history.map((sample, index) => {
        const x = padding + (index / Math.max(history.length - 1, 1)) * usableWidth;
        const value = sample.latency_ms ?? timeoutMs;
        const y = height - padding - (value / maxValue) * usableHeight;
        return { x, y, sample };
      });

      const thresholdY = height - padding - (thresholdMs / maxValue) * usableHeight;
      chart.appendChild(
        makeSvgElement("line", {
          x1: padding,
          y1: thresholdY,
          x2: width - padding,
          y2: thresholdY,
          stroke: "#e76f51",
          "stroke-width": 2,
          "stroke-dasharray": "8 6",
        })
      );
      const thresholdLabel = makeSvgElement("text", {
        x: padding + 8,
        y: thresholdY - 8,
        fill: "#e76f51",
        "font-size": 14,
        "font-family": "Trebuchet MS, sans-serif",
      });
      thresholdLabel.textContent = `${thresholdMs} ms threshold`;
      chart.appendChild(thresholdLabel);

      const areaPoints = [`${padding},${height - padding}`]
        .concat(points.map((point) => `${point.x},${point.y}`))
        .concat(`${width - padding},${height - padding}`)
        .join(" ");
      chart.appendChild(makeSvgElement("polygon", { points: areaPoints, fill: "rgba(0, 95, 115, 0.16)" }));
      chart.appendChild(
        makeSvgElement("polyline", {
          points: points.map((point) => `${point.x},${point.y}`).join(" "),
          fill: "none",
          stroke: "#005f73",
          "stroke-width": 3,
          "stroke-linejoin": "round",
          "stroke-linecap": "round",
        })
      );

      points.forEach((point) => {
        const fill = !point.sample.success ? "#c44536" : (point.sample.latency_ms ?? 0) > thresholdMs ? "#e76f51" : "#2a9d8f";
        chart.appendChild(
          makeSvgElement("circle", {
            cx: point.x,
            cy: point.y,
            r: !point.sample.success ? 5.5 : 3.8,
            fill,
          })
        );
      });
    }

    function renderEventChart(config) {
      const { history, fallbackId, svgId, xAxisId, yAxisId, predicate, color } = config;
      const chart = document.getElementById(svgId);
      const fallback = document.getElementById(fallbackId);
      chart.innerHTML = "";
      setAxisLabels(xAxisId, getTimeAxisLabels(history));
      setAxisLabels(yAxisId, ["1 event", "0 events"]);

      if (!history.length) {
        fallback.style.display = "grid";
        return 0;
      }

      fallback.style.display = "none";
      const width = 900;
      const height = 220;
      const padding = 24;
      const baselineY = height - padding;
      const topY = padding;
      const usableWidth = width - padding * 2;
      const barWidth = Math.max(4, usableWidth / Math.max(history.length, 1) * 0.72);
      drawGrid(chart, width, height, padding, 3);
      chart.appendChild(
        makeSvgElement("line", {
          x1: padding,
          y1: baselineY,
          x2: width - padding,
          y2: baselineY,
          stroke: "rgba(53, 42, 26, 0.25)",
          "stroke-width": 2,
        })
      );

      let count = 0;
      history.forEach((sample, index) => {
        const event = predicate(sample);
        if (event) {
          count += 1;
        }

        const centerX = padding + ((index + 0.5) / history.length) * usableWidth;
        if (event) {
          chart.appendChild(
            makeSvgElement("rect", {
              x: centerX - barWidth / 2,
              y: topY,
              width: barWidth,
              height: baselineY - topY,
              rx: 4,
              fill: color,
              opacity: 0.85,
            })
          );
        } else {
          chart.appendChild(
            makeSvgElement("circle", {
              cx: centerX,
              cy: baselineY,
              r: 2,
              fill: "rgba(53, 42, 26, 0.28)",
            })
          );
        }
      });

      return count;
    }

    async function updatePollRate() {
      if (configSaveInFlight) {
        return;
      }

      const input = document.getElementById("poll-rate-input");
      const feedback = document.getElementById("poll-rate-feedback");
      const intervalMs = Number.parseInt(input.value, 10);
      if (!Number.isFinite(intervalMs) || intervalMs <= 0) {
        feedback.textContent = "Enter a polling rate greater than 0 ms.";
        return;
      }

      configSaveInFlight = true;
      feedback.textContent = "Saving...";
      try {
        const response = await fetch("/api/config", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ interval_ms: intervalMs }),
        });
        const payload = await response.json();
        if (!response.ok) {
          throw new Error(payload.error || "Unable to save polling rate");
        }
        feedback.textContent = `Polling rate updated to ${payload.interval_ms} ms.`;
        await refresh();
      } catch (error) {
        feedback.textContent = error.message;
      } finally {
        configSaveInFlight = false;
      }
    }

    async function updateHighPingThreshold() {
      if (configSaveInFlight) {
        return;
      }

      const input = document.getElementById("high-ping-threshold-input");
      const feedback = document.getElementById("high-ping-threshold-feedback");
      const thresholdMs = Number.parseInt(input.value, 10);
      if (!Number.isFinite(thresholdMs) || thresholdMs <= 0) {
        feedback.textContent = "Enter a threshold greater than 0 ms.";
        return;
      }

      configSaveInFlight = true;
      feedback.textContent = "Saving...";
      try {
        const response = await fetch("/api/config", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ high_ping_threshold_ms: thresholdMs }),
        });
        const payload = await response.json();
        if (!response.ok) {
          throw new Error(payload.error || "Unable to save threshold");
        }
        feedback.textContent = `High-ping threshold updated to ${payload.high_ping_threshold_ms} ms.`;
        await refresh();
      } catch (error) {
        feedback.textContent = error.message;
      } finally {
        configSaveInFlight = false;
      }
    }

    async function refresh() {
      try {
        const response = await fetch("/api/status", { cache: "no-store" });
        const data = await response.json();
        const visibleHistory = filterHistoryByTimeframe(data.history);
        const timeframeLabel = getSelectedTimeframeLabel();
        setMetric("target", data.target);
        setMetric("current-latency", data.current_latency_ms === null ? "timeout" : `${data.current_latency_ms} ms`);
        setMetric("avg-latency", data.avg_latency_ms === null ? "-" : `${data.avg_latency_ms} ms`);
        setMetric("packet-loss", `${data.packet_loss_percent}%`);
        setMetric("packet-loss-events", `${data.packet_loss_events_total} total loss events`);
        setMetric("high-ping-events-total", `${data.high_ping_events_total}`);
        setMetric("high-ping-threshold", `Threshold: > ${data.high_ping_threshold_ms} ms`);
        setMetric("started-at", data.started_at ?? "-");
        setMetric("last-sample-at", data.last_sample_at ?? "-");
        setMetric(
          "min-max",
          data.min_latency_ms === null ? "-" : `${data.min_latency_ms} ms / ${data.max_latency_ms} ms`
        );
        setMetric("counts", `${data.sent} / ${data.received} / ${data.lost}`);
        setMetric("failures", String(data.consecutive_failures));
        setMetric("current-timeframe-label", timeframeLabel);
        setMetric("current-poll-rate", `${Math.round(data.interval_seconds * 1000)} ms`);
        setMetric("log-file", data.log_file);
        setMetric("sample-count", `${visibleHistory.length} visible samples`);
        setMetric(
          "latency-note",
          `${timeframeLabel} window. Polling every ${Math.round(data.interval_seconds * 1000)} ms. Timeouts are plotted at the ${data.timeout_ms} ms ceiling.`
        );
        setMetric("current-high-ping-threshold", `${data.high_ping_threshold_ms} ms`);
        if (!configSaveInFlight) {
          document.getElementById("poll-rate-input").value = String(Math.round(data.interval_seconds * 1000));
          document.getElementById("high-ping-threshold-input").value = String(data.high_ping_threshold_ms);
        }

        setStatus(data.current_latency_ms, data.high_ping_threshold_ms, data.last_status);
        renderLatencyChart(visibleHistory, data.timeout_ms, data.high_ping_threshold_ms);
        const lossWindowCount = renderEventChart({
          history: visibleHistory,
          fallbackId: "loss-fallback",
          svgId: "loss-chart",
          xAxisId: "loss-x-axis",
          yAxisId: "loss-y-axis",
          predicate: (sample) => !sample.success,
          color: "#c44536",
        });
        const highPingWindowCount = renderEventChart({
          history: visibleHistory,
          fallbackId: "high-ping-fallback",
          svgId: "high-ping-chart",
          xAxisId: "high-ping-x-axis",
          yAxisId: "high-ping-y-axis",
          predicate: (sample) => sample.success && (sample.latency_ms ?? 0) > data.high_ping_threshold_ms,
          color: "#e76f51",
        });
        setMetric("loss-window-count", `${lossWindowCount} visible events`);
        setMetric("high-ping-window-count", `${highPingWindowCount} visible events`);
        setMetric("loss-note", `${lossWindowCount} packet-loss event(s) in the ${timeframeLabel.toLowerCase()} window.`);
        setMetric(
          "high-ping-note",
          `${highPingWindowCount} response(s) above ${data.high_ping_threshold_ms} ms in the ${timeframeLabel.toLowerCase()} window.`
        );
        renderTable(visibleHistory, data.high_ping_threshold_ms);
      } catch (error) {
        document.getElementById("status-text").textContent = "Disconnected";
        document.getElementById("status-text").className = "metric-value status-bad";
        document.getElementById("status-detail").textContent = "The browser could not reach the local monitor.";
      }
    }

    document.getElementById("poll-rate-save").addEventListener("click", updatePollRate);
    document.getElementById("poll-rate-input").addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        updatePollRate();
      }
    });
    document.getElementById("high-ping-threshold-save").addEventListener("click", updateHighPingThreshold);
    document.getElementById("high-ping-threshold-input").addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        updateHighPingThreshold();
      }
    });
    document.getElementById("timeframe-select").addEventListener("change", () => {
      refresh();
    });

    refresh();
    setInterval(refresh, 1000);
  </script>
</body>
</html>
"""


def make_handler(state: PingState) -> type[BaseHTTPRequestHandler]:
    class PingHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path in {"/", "/index.html"}:
                body = HTML_PAGE.encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if self.path == "/api/status":
                payload = state.snapshot()
                payload["log_file"] = str(state.log_file)
                body = json.dumps(payload).encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            self.send_error(HTTPStatus.NOT_FOUND, "Not found")

        def do_POST(self) -> None:
            if self.path != "/api/config":
                self.send_error(HTTPStatus.NOT_FOUND, "Not found")
                return

            try:
                content_length = int(self.headers.get("Content-Length", "0"))
                raw_body = self.rfile.read(content_length)
                payload = json.loads(raw_body.decode("utf-8") or "{}")
                updates_applied = False

                if "interval_ms" in payload:
                    state.set_interval_ms(int(payload["interval_ms"]))
                    updates_applied = True

                if "high_ping_threshold_ms" in payload:
                    state.set_high_ping_threshold_ms(int(payload["high_ping_threshold_ms"]))
                    updates_applied = True

                if not updates_applied:
                    raise ValueError("No supported config values provided")
            except (TypeError, ValueError, json.JSONDecodeError):
                body = json.dumps(
                    {
                        "error": (
                            "Provide interval_ms and/or high_ping_threshold_ms as positive integers"
                        )
                    }
                ).encode("utf-8")
                self.send_response(HTTPStatus.BAD_REQUEST)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            body = json.dumps(
                {
                    "ok": True,
                    "interval_ms": round(state.get_interval_seconds() * 1000),
                    "interval_seconds": state.get_interval_seconds(),
                    "high_ping_threshold_ms": state.get_high_ping_threshold_ms(),
                }
            ).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: Any) -> None:
            return

    return PingHandler


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Continuously ping a host and show the results in a browser dashboard."
    )
    parser.add_argument(
        "target",
        nargs="?",
        default="8.8.8.8",
        help="Hostname or IP to ping. Default: 8.8.8.8",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Seconds between ping attempts. Default: 1.0",
    )
    parser.add_argument(
        "--timeout-ms",
        type=int,
        default=1000,
        help="Ping timeout in milliseconds. Default: 1000",
    )
    parser.add_argument(
        "--high-ping-threshold-ms",
        type=int,
        default=DEFAULT_HIGH_PING_THRESHOLD_MS,
        help="Threshold for high-ping events in milliseconds. Default: 150",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8765,
        help="Local port for the dashboard. Default: 8765",
    )
    parser.add_argument(
        "--history-size",
        type=int,
        default=0,
        help="Maximum samples to keep in memory. Use 0 to keep the full session. Default: 0",
    )
    parser.add_argument(
        "--log-file",
        default="ping-log.csv",
        help="CSV file for all collected samples. Default: ping-log.csv",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.interval <= 0:
        raise SystemExit("--interval must be greater than 0")
    if args.timeout_ms <= 0:
        raise SystemExit("--timeout-ms must be greater than 0")
    if args.high_ping_threshold_ms <= 0:
        raise SystemExit("--high-ping-threshold-ms must be greater than 0")
    if args.history_size < 0:
        raise SystemExit("--history-size must be 0 or greater")
    if not (1 <= args.port <= 65535):
        raise SystemExit("--port must be between 1 and 65535")

    state = PingState(
        target=args.target,
        interval_seconds=args.interval,
        timeout_ms=args.timeout_ms,
        high_ping_threshold_ms=args.high_ping_threshold_ms,
        max_history_samples=args.history_size,
        log_file=Path(args.log_file).resolve(),
    )
    stop_event = threading.Event()
    worker = threading.Thread(target=ping_loop, args=(state, stop_event), daemon=True)
    worker.start()

    server = ThreadingHTTPServer(("127.0.0.1", args.port), make_handler(state))
    print(f"Ping monitor running for {args.target}")
    print(f"Dashboard: http://127.0.0.1:{args.port}")
    print(f"Logging to: {state.log_file}")
    print("Press Ctrl+C to stop.")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        stop_event.set()
        server.server_close()
        worker.join(timeout=2.0)


if __name__ == "__main__":
    main()
