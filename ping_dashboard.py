#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import multiprocessing
import queue
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
DEFAULT_GATEWAY_TARGET = "10.0.0.1"
DEFAULT_INTERNET_TARGET = "8.8.8.8"
DEFAULT_HIGH_PING_THRESHOLD_MS = 150
LOG_HEADER = ["timestamp", "sequence", "session_id", "target", "success", "latency_ms", "message", "return_code"]
LEGACY_LOG_HEADER = ["timestamp", "sequence", "target", "success", "latency_ms", "message", "return_code"]


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def normalize_target(target: str) -> str:
    cleaned = target.strip()
    if not cleaned:
        raise ValueError("target must not be empty")
    return cleaned


def derive_log_paths(base_path: Path) -> tuple[Path, Path]:
    if base_path.suffix:
        gateway_name = f"{base_path.stem}-gateway{base_path.suffix}"
        internet_name = f"{base_path.stem}-internet{base_path.suffix}"
    else:
        gateway_name = f"{base_path.name}-gateway"
        internet_name = f"{base_path.name}-internet"
    return base_path.with_name(gateway_name), base_path.with_name(internet_name)


@dataclass
class Sample:
    sequence: int
    session_id: int
    timestamp: str
    success: bool
    latency_ms: int | None
    message: str
    return_code: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "sequence": self.sequence,
            "session_id": self.session_id,
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
    session_id: int = 0
    worker_pid: int | None = None

    def add_sample(self, sample: Sample) -> bool:
        with self.lock:
            if sample.session_id != self.session_id:
                return False
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
            return True

    def next_sequence(self) -> int:
        with self.lock:
            self.sequence += 1
            return self.sequence

    def get_interval_seconds(self) -> float:
        with self.lock:
            return self.interval_seconds

    def get_target(self) -> str:
        with self.lock:
            return self.target

    def get_session_id(self) -> int:
        with self.lock:
            return self.session_id

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

    def set_target(self, target: str) -> None:
        normalized = normalize_target(target)
        with self.lock:
            self.target = normalized
            self.session_id += 1
            self.history.clear()
            self.success_latencies.clear()
            self.sequence = 0
            self.sent = 0
            self.received = 0
            self.lost = 0
            self.consecutive_failures = 0
            self.started_at = now_iso()

    def set_worker_pid(self, worker_pid: int | None) -> None:
        with self.lock:
            self.worker_pid = worker_pid

    def restore_samples(self, samples: list[Sample], session_id: int) -> None:
        with self.lock:
            self.history = []
            self.success_latencies = []
            self.sequence = 0
            self.sent = 0
            self.received = 0
            self.lost = 0
            self.consecutive_failures = 0
            self.session_id = session_id
            self.started_at = samples[0].timestamp if samples else now_iso()

            for sample in samples:
                self.sequence = max(self.sequence, sample.sequence)
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
                "worker_pid": self.worker_pid,
                "history": [sample.to_dict() for sample in samples],
            }


@dataclass
class WorkerControl:
    process: multiprocessing.context.Process
    control_queue: multiprocessing.queues.Queue
    sample_queue: multiprocessing.queues.Queue
    listener_thread: threading.Thread

    def set_interval_ms(self, interval_ms: int) -> None:
        self.control_queue.put({"type": "set_interval_ms", "interval_ms": interval_ms})

    def set_target(self, target: str, session_id: int) -> None:
        self.control_queue.put({"type": "set_target", "target": target, "session_id": session_id})

    def stop(self) -> None:
        self.control_queue.put({"type": "stop"})


def ensure_log_file(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(LOG_HEADER)
        return

    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        header = next(reader, [])
        if header == LOG_HEADER:
            return
        existing_rows = list(reader)

    if header == LEGACY_LOG_HEADER:
        normalized_rows = []
        for row in existing_rows:
            if not row:
                continue
            if len(row) < len(LEGACY_LOG_HEADER):
                row = row + [""] * (len(LEGACY_LOG_HEADER) - len(row))
            normalized_rows.append([row[0], row[1], "", row[2], row[3], row[4], row[5], row[6]])
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(LOG_HEADER)
            writer.writerows(normalized_rows)
        return

    if not header:
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(LOG_HEADER)
        return

    raise ValueError(f"Unsupported log header in {path}")


def append_log(path: Path, target: str, sample: Sample) -> None:
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                sample.timestamp,
                sample.sequence,
                sample.session_id,
                target,
                sample.success,
                sample.latency_ms if sample.latency_ms is not None else "",
                sample.message,
                sample.return_code,
            ]
        )


def parse_bool(value: str) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes"}


def load_samples_from_log(path: Path, target: str, max_history_samples: int) -> tuple[list[Sample], int]:
    ensure_log_file(path)
    normalized_target = normalize_target(target)

    def row_target(row: dict[str, str]) -> str:
        return str(row.get("target") or "").strip()

    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            return [], 0

        rows = [row for row in reader if row]

    if not rows:
        return [], 0

    last_target_row: dict[str, str] | None = None
    for row in reversed(rows):
        if row_target(row) == normalized_target:
            last_target_row = row
            break

    if last_target_row is None:
        return [], 0

    session_id_text = (last_target_row.get("session_id") or "").strip()
    samples: list[Sample] = []

    if session_id_text:
        session_id = int(session_id_text)
        for row in rows:
            if row_target(row) != normalized_target:
                continue
            if (row.get("session_id") or "").strip() != session_id_text:
                continue
            samples.append(
                Sample(
                    sequence=int(row.get("sequence") or 0),
                    session_id=session_id,
                    timestamp=str(row.get("timestamp") or ""),
                    success=parse_bool(row.get("success", "")),
                    latency_ms=int(row["latency_ms"]) if (row.get("latency_ms") or "").strip() else None,
                    message=str(row.get("message") or ""),
                    return_code=int(row.get("return_code") or 0),
                )
            )
    else:
        session_id = 0
        trailing_rows: list[dict[str, str]] = []
        for row in reversed(rows):
            if row_target(row) != normalized_target:
                if trailing_rows:
                    break
                continue
            trailing_rows.append(row)
        for row in reversed(trailing_rows):
            samples.append(
                Sample(
                    sequence=int(row.get("sequence") or 0),
                    session_id=session_id,
                    timestamp=str(row.get("timestamp") or ""),
                    success=parse_bool(row.get("success", "")),
                    latency_ms=int(row["latency_ms"]) if (row.get("latency_ms") or "").strip() else None,
                    message=str(row.get("message") or ""),
                    return_code=int(row.get("return_code") or 0),
                )
            )

    return samples, session_id


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
        target = state.get_target()
        sequence = state.next_sequence()
        session_id = state.get_session_id()
        timestamp = now_iso()
        success, latency, message, return_code = run_ping(target, state.timeout_ms)
        sample = Sample(
            sequence=sequence,
            session_id=session_id,
            timestamp=timestamp,
            success=success,
            latency_ms=latency,
            message=message,
            return_code=return_code,
        )
        state.add_sample(sample)
        append_log(state.log_file, target, sample)
        stop_event.wait(state.get_interval_seconds())


def ping_worker(
    target: str,
    interval_seconds: float,
    timeout_ms: int,
    log_file: str,
    sample_queue: multiprocessing.queues.Queue,
    control_queue: multiprocessing.queues.Queue,
    stop_event: multiprocessing.synchronize.Event,
    session_id: int = 0,
    starting_sequence: int = 0,
) -> None:
    log_path = Path(log_file)
    parent_process = multiprocessing.parent_process()
    ensure_log_file(log_path)
    current_target = normalize_target(target)
    current_interval_seconds = interval_seconds
    current_session_id = session_id
    sequence = starting_sequence
    sample_queue.put({"type": "worker_started", "pid": multiprocessing.current_process().pid})

    while not stop_event.is_set():
        if parent_process is not None and not parent_process.is_alive():
            break

        while True:
            try:
                command = control_queue.get_nowait()
            except queue.Empty:
                break

            command_type = command.get("type")
            if command_type == "set_interval_ms":
                interval_ms = int(command["interval_ms"])
                if interval_ms <= 0:
                    continue
                current_interval_seconds = interval_ms / 1000.0
            elif command_type == "set_target":
                current_target = normalize_target(str(command["target"]))
                current_session_id = int(command["session_id"])
                sequence = 0
            elif command_type == "stop":
                stop_event.set()
                break

        if stop_event.is_set():
            break

        sequence += 1
        timestamp = now_iso()
        success, latency, message, return_code = run_ping(current_target, timeout_ms)
        sample = Sample(
            sequence=sequence,
            session_id=current_session_id,
            timestamp=timestamp,
            success=success,
            latency_ms=latency,
            message=message,
            return_code=return_code,
        )
        append_log(log_path, current_target, sample)
        sample_queue.put({"type": "sample", "sample": sample.to_dict()})

        if stop_event.wait(current_interval_seconds):
            break


def sample_listener(
    state: PingState,
    sample_queue: multiprocessing.queues.Queue,
    stop_event: threading.Event,
) -> None:
    while not stop_event.is_set():
        try:
            message = sample_queue.get(timeout=0.5)
        except queue.Empty:
            continue

        if not isinstance(message, dict):
            continue

        message_type = message.get("type")
        if message_type == "worker_started":
            pid = message.get("pid")
            state.set_worker_pid(int(pid) if pid is not None else None)
            continue

        if message_type != "sample":
            continue

        raw_sample = message.get("sample")
        if not isinstance(raw_sample, dict):
            continue

        sample = Sample(
            sequence=int(raw_sample["sequence"]),
            session_id=int(raw_sample.get("session_id", 0)),
            timestamp=str(raw_sample["timestamp"]),
            success=bool(raw_sample["success"]),
            latency_ms=raw_sample["latency_ms"],
            message=str(raw_sample["message"]),
            return_code=int(raw_sample["return_code"]),
        )
        state.add_sample(sample)


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
      max-width: 1500px;
      min-height: 100vh;
      margin: 0 auto;
      padding: 20px 18px;
      display: grid;
      grid-template-rows: auto auto 1fr;
      gap: 14px;
    }

    .hero {
      display: grid;
      gap: 6px;
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
      font-size: clamp(1.8rem, 4vw, 3.1rem);
      line-height: 0.95;
      font-weight: 600;
    }

    .subtitle {
      margin: 0;
      max-width: 980px;
      color: var(--muted);
      font-size: 0.96rem;
    }

    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 14px;
      margin-bottom: 18px;
    }

    .monitor-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 14px;
      min-height: 0;
    }

    .monitor-column {
      min-width: 0;
      display: grid;
      align-content: start;
      gap: 12px;
    }

    .monitor-title-row,
    .controls-head {
      display: flex;
      justify-content: space-between;
      align-items: baseline;
      gap: 12px;
    }

    .monitor-name {
      margin: 0;
      font-size: 1.45rem;
    }

    .monitor-copy,
    .controls-copy {
      margin: 4px 0 0;
      color: var(--muted);
      font-size: 0.92rem;
      font-family: "Trebuchet MS", sans-serif;
    }

    .controls-card {
      padding: 16px 18px;
    }

    .controls-grid {
      display: grid;
      grid-template-columns: minmax(250px, 1.5fr) repeat(3, minmax(170px, 1fr));
      gap: 12px;
      align-items: start;
      margin-top: 12px;
    }

    .control-group {
      display: grid;
      gap: 8px;
    }

    .control-note {
      color: var(--muted);
      font-size: 0.86rem;
      font-family: "Trebuchet MS", sans-serif;
    }

    .metrics-grid {
      grid-template-columns: repeat(5, minmax(0, 1fr));
      gap: 10px;
      margin-bottom: 0;
    }

    .metrics-grid .card {
      padding: 14px;
      border-radius: 18px;
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
      font-size: clamp(1.2rem, 2.6vw, 2rem);
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

    .chart-card,
    .session-card {
      padding: 14px 16px;
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

    .plot-scroll {
      overflow-x: auto;
      padding-bottom: 4px;
    }

    .plot-area {
      height: 190px;
      border-radius: 18px;
      border: 1px solid var(--panel-border);
      overflow: hidden;
      position: relative;
      background:
        linear-gradient(to bottom, rgba(0, 95, 115, 0.07), transparent 48%),
        repeating-linear-gradient(to bottom, transparent 0 63px, var(--grid) 63px 64px);
    }

    .plot-area.compact {
      height: 120px;
      background:
        linear-gradient(to bottom, rgba(0, 95, 115, 0.05), transparent 38%),
        repeating-linear-gradient(to bottom, transparent 0 53px, var(--grid) 53px 54px);
    }

    .axis-x {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      align-items: start;
      color: var(--muted);
      font-size: 0.82rem;
      font-family: "Trebuchet MS", sans-serif;
      column-gap: 8px;
      padding: 4px 0 0;
    }

    .axis-x span:first-child {
      text-align: left;
    }

    .axis-x span:nth-child(2) {
      text-align: center;
    }

    .axis-x span:last-child {
      text-align: right;
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

    .summary-grid {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 12px;
    }

    .summary-item {
      display: grid;
      gap: 4px;
      min-width: 0;
    }

    .summary-item strong {
      font-family: "Trebuchet MS", sans-serif;
      font-size: 0.96rem;
      overflow-wrap: anywhere;
    }

    .summary-item.summary-wide {
      grid-column: 1 / -1;
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
      .shell {
        min-height: auto;
      }

      .monitor-grid,
      .controls-grid,
      .metrics-grid,
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
        height: 180px;
      }

      .plot-area.compact {
        height: 115px;
      }

      .summary-grid {
        grid-template-columns: 1fr;
      }
    }
  </style>
</head>
<body>
  <div class="shell">
    <section class="card controls-card">
      <div class="controls-grid">
        <div class="control-group">
          <div>Internet target: <strong id="current-target-control">-</strong></div>
          <label for="target-input">Internet hostname or IP</label>
          <input id="target-input" type="text" value="8.8.8.8" placeholder="Hostname or IP">
          <button id="target-save" type="button">Apply internet target</button>
          <div class="control-note" id="target-feedback">Changing the internet target starts a fresh internet session.</div>
        </div>
        <div class="control-group">
          <div>Timeline window: <strong id="current-timeframe-control">All data</strong></div>
          <label for="timeframe-select">Visible timeline window</label>
          <select id="timeframe-select">
            <option value="1m">1 minute</option>
            <option value="5m">5 minutes</option>
            <option value="10m">10 minutes</option>
            <option value="30m">30 minutes</option>
            <option value="1h">1 hour</option>
            <option value="all" selected>All data</option>
          </select>
          <div class="control-note">Applies to both gateway and internet timelines.</div>
        </div>
        <div class="control-group">
          <div>Polling rate: <strong id="current-poll-rate">-</strong></div>
          <label for="poll-rate-input">Polling rate (ms)</label>
          <input id="poll-rate-input" type="number" min="1" step="50" value="1000">
          <button id="poll-rate-save" type="button">Apply polling rate</button>
          <div class="control-note" id="poll-rate-feedback">Changes apply to the next ping cycle.</div>
        </div>
        <div class="control-group">
          <div>High-ping threshold: <strong id="current-high-ping-threshold">-</strong></div>
          <label for="high-ping-threshold-input">Threshold (ms)</label>
          <input id="high-ping-threshold-input" type="number" min="1" step="10" value="150">
          <button id="high-ping-threshold-save" type="button">Apply threshold</button>
          <div class="control-note" id="high-ping-threshold-feedback">Used for status and the latency threshold line.</div>
        </div>
      </div>
    </section>

    <section class="monitor-grid">
      <section id="gateway-monitor-root" class="monitor-column"></section>

      <section class="monitor-column">
        <div class="monitor-title-row">
          <div>
            <span class="eyebrow">Internet</span>
            <h2 class="monitor-name">Internet Target</h2>
            <p class="monitor-copy">Targetable live endpoint monitor.</p>
          </div>
          <div class="pill" id="sample-count">0 samples</div>
        </div>

        <section class="grid metrics-grid">
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
            <span class="metric-label">Status</span>
            <div class="metric-value" id="status-text">Waiting</div>
            <div class="metric-subtext" id="status-detail">No samples yet</div>
          </article>
        </section>

        <article class="card chart-card">
          <div class="chart-head">
            <div>
              <h2 class="chart-title">Latency Timeline</h2>
              <div class="chart-note" id="latency-note">Collecting samples...</div>
            </div>
          </div>
          <div class="chart-shell">
            <div class="axis-y" id="latency-y-axis"></div>
            <div class="plot-stack">
              <div class="plot-scroll" id="latency-scroll">
                <div class="plot-area" id="latency-plot">
                  <svg id="latency-chart" viewBox="0 0 900 190" preserveAspectRatio="none" aria-label="Latency timeline"></svg>
                  <div class="chart-fallback" id="latency-fallback">Waiting for enough data to draw the timeline.</div>
                </div>
                <div class="axis-x" id="latency-x-axis"></div>
              </div>
            </div>
          </div>
        </article>

        <article class="card chart-card">
          <div class="chart-head">
            <div>
              <h2 class="chart-title">Packet Loss Timeline</h2>
              <div class="chart-note" id="loss-note">Loss events across the visible sample window.</div>
            </div>
            <div class="pill" id="loss-window-count">0 total events</div>
          </div>
          <div class="chart-shell">
            <div class="axis-y" id="loss-y-axis"></div>
            <div class="plot-stack">
              <div class="plot-scroll" id="loss-scroll">
                <div class="plot-area compact" id="loss-plot">
                  <svg id="loss-chart" viewBox="0 0 900 120" preserveAspectRatio="none" aria-label="Packet loss timeline"></svg>
                  <div class="chart-fallback" id="loss-fallback">Waiting for packet-loss history.</div>
                </div>
                <div class="axis-x" id="loss-x-axis"></div>
              </div>
            </div>
          </div>
        </article>

        <article class="card session-card">
          <div class="table-head">
            <div>
              <h2 class="table-title">Session Summary</h2>
              <div class="table-note">Internet monitor details</div>
            </div>
          </div>
          <div class="summary-grid">
            <div class="summary-item">
              <span class="metric-label">Started</span>
              <strong id="started-at">-</strong>
            </div>
            <div class="summary-item">
              <span class="metric-label">Last Sample</span>
              <strong id="last-sample-at">-</strong>
            </div>
            <div class="summary-item">
              <span class="metric-label">Min / Max</span>
              <strong id="min-max">-</strong>
            </div>
            <div class="summary-item">
              <span class="metric-label">Sent / Received / Lost</span>
              <strong id="counts">-</strong>
            </div>
            <div class="summary-item">
              <span class="metric-label">Consecutive Failures</span>
              <strong id="failures">0</strong>
            </div>
            <div class="summary-item">
              <span class="metric-label">Worker PID</span>
              <strong id="worker-pid">-</strong>
            </div>
            <div class="summary-item summary-wide">
              <span class="metric-label">Log File</span>
              <code id="log-file">-</code>
            </div>
          </div>
        </article>
      </section>
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

    function gatewayId(name) {
      return `gateway-${name}`;
    }

    function buildGatewayMonitorMarkup() {
      return `
        <div class="monitor-title-row">
          <div>
            <span class="eyebrow">Gateway</span>
            <h2 class="monitor-name">Local Gateway</h2>
            <p class="monitor-copy">Pinned gateway monitor for 10.0.0.1.</p>
          </div>
          <div class="pill" id="gateway-sample-count">0 samples</div>
        </div>
        <section class="grid metrics-grid">
          <article class="card">
            <span class="metric-label">Target</span>
            <div class="metric-value" id="gateway-target">-</div>
          </article>
          <article class="card">
            <span class="metric-label">Current</span>
            <div class="metric-value" id="gateway-current-latency">-</div>
          </article>
          <article class="card">
            <span class="metric-label">Average</span>
            <div class="metric-value" id="gateway-avg-latency">-</div>
          </article>
          <article class="card">
            <span class="metric-label">Packet Loss</span>
            <div class="metric-value" id="gateway-packet-loss">-</div>
            <div class="metric-subtext" id="gateway-packet-loss-events">0 total loss events</div>
          </article>
          <article class="card">
            <span class="metric-label">Status</span>
            <div class="metric-value" id="gateway-status-text">Waiting</div>
            <div class="metric-subtext" id="gateway-status-detail">No samples yet</div>
          </article>
        </section>
        <article class="card chart-card">
          <div class="chart-head">
            <div>
              <h2 class="chart-title">Latency Timeline</h2>
              <div class="chart-note" id="gateway-latency-note">Collecting samples...</div>
            </div>
          </div>
          <div class="chart-shell">
            <div class="axis-y" id="gateway-latency-y-axis"></div>
            <div class="plot-stack">
              <div class="plot-scroll" id="gateway-latency-scroll">
                <div class="plot-area" id="gateway-latency-plot">
                  <svg id="gateway-latency-chart" viewBox="0 0 900 190" preserveAspectRatio="none" aria-label="Gateway latency timeline"></svg>
                  <div class="chart-fallback" id="gateway-latency-fallback">Waiting for enough data to draw the timeline.</div>
                </div>
                <div class="axis-x" id="gateway-latency-x-axis"></div>
              </div>
            </div>
          </div>
        </article>
        <article class="card chart-card">
          <div class="chart-head">
            <div>
              <h2 class="chart-title">Packet Loss Timeline</h2>
              <div class="chart-note" id="gateway-loss-note">Loss events across the visible sample window.</div>
            </div>
            <div class="pill" id="gateway-loss-window-count">0 total events</div>
          </div>
          <div class="chart-shell">
            <div class="axis-y" id="gateway-loss-y-axis"></div>
            <div class="plot-stack">
              <div class="plot-scroll" id="gateway-loss-scroll">
                <div class="plot-area compact" id="gateway-loss-plot">
                  <svg id="gateway-loss-chart" viewBox="0 0 900 120" preserveAspectRatio="none" aria-label="Gateway packet loss timeline"></svg>
                  <div class="chart-fallback" id="gateway-loss-fallback">Waiting for packet-loss history.</div>
                </div>
                <div class="axis-x" id="gateway-loss-x-axis"></div>
              </div>
            </div>
          </div>
        </article>
        <article class="card session-card">
          <div class="table-head">
            <div>
              <h2 class="table-title">Session Summary</h2>
              <div class="table-note">Gateway monitor details</div>
            </div>
          </div>
          <div class="summary-grid">
            <div class="summary-item">
              <span class="metric-label">Started</span>
              <strong id="gateway-started-at">-</strong>
            </div>
            <div class="summary-item">
              <span class="metric-label">Last Sample</span>
              <strong id="gateway-last-sample-at">-</strong>
            </div>
            <div class="summary-item">
              <span class="metric-label">Min / Max</span>
              <strong id="gateway-min-max">-</strong>
            </div>
            <div class="summary-item">
              <span class="metric-label">Sent / Received / Lost</span>
              <strong id="gateway-counts">-</strong>
            </div>
            <div class="summary-item">
              <span class="metric-label">Consecutive Failures</span>
              <strong id="gateway-failures">0</strong>
            </div>
            <div class="summary-item">
              <span class="metric-label">Worker PID</span>
              <strong id="gateway-worker-pid">-</strong>
            </div>
            <div class="summary-item summary-wide">
              <span class="metric-label">Log File</span>
              <code id="gateway-log-file">-</code>
            </div>
          </div>
        </article>
      `;
    }

    function ensureGatewayMonitor() {
      const root = document.getElementById("gateway-monitor-root");
      if (!root.dataset.initialized) {
        root.innerHTML = buildGatewayMonitorMarkup();
        root.dataset.initialized = "true";
      }
    }

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

    function getSelectedTimeframeKey() {
      const key = document.getElementById("timeframe-select").value;
      return TIMEFRAME_OPTIONS[key] ? key : "all";
    }

    function getSelectedTimeframeLabel() {
      return TIMEFRAME_OPTIONS[getSelectedTimeframeKey()].label;
    }

    function getSelectedTimeframeMs() {
      const selected = TIMEFRAME_OPTIONS[getSelectedTimeframeKey()];
      return selected ? selected.ms : null;
    }

    function getSampleTimeMs(sample) {
      const value = new Date(sample.timestamp).getTime();
      return Number.isNaN(value) ? null : value;
    }

    function getHistoryBounds(history) {
      if (history.length < 2) {
        return null;
      }

      const startMs = getSampleTimeMs(history[0]);
      const endMs = getSampleTimeMs(history[history.length - 1]);
      if (startMs === null || endMs === null || endMs <= startMs) {
        return null;
      }

      return {
        startMs,
        endMs,
        durationMs: endMs - startMs,
      };
    }

    function getTimelineDomain(history) {
      if (!history.length) {
        return null;
      }

      const endMs = getSampleTimeMs(history[history.length - 1]);
      if (endMs === null) {
        return null;
      }

      const timeframeMs = getSelectedTimeframeMs();
      if (timeframeMs === null) {
        const bounds = getHistoryBounds(history);
        if (bounds) {
          return {
            startMs: bounds.startMs,
            endMs: bounds.endMs,
            spanMs: Math.max(bounds.durationMs, 1),
          };
        }

        return {
          startMs: endMs,
          endMs,
          spanMs: 1,
        };
      }

      const bounds = getHistoryBounds(history);
      const dataDurationMs = bounds ? bounds.durationMs : 0;
      const spanMs = Math.max(timeframeMs, dataDurationMs, 1);
      return {
        startMs: endMs - spanMs,
        endMs,
        spanMs,
      };
    }

    function syncInputValue(id, value) {
      const input = document.getElementById(id);
      if (document.activeElement !== input && !configSaveInFlight) {
        input.value = String(value);
      }
    }

    function getTimelineWidth(scrollEl, history, minimumPixelsPerSample) {
      const visibleWidth = Number(scrollEl.clientWidth || 0);
      const fallbackWidth = 720;
      const baseWidth = Math.max(visibleWidth, fallbackWidth);
      const timeframeMs = getSelectedTimeframeMs();
      const bounds = getHistoryBounds(history);

      if (timeframeMs === null) {
        return Math.max(visibleWidth, 1);
      }

      if (timeframeMs !== null && bounds) {
        const ratio = Math.max(bounds.durationMs / timeframeMs, 1);
        return Math.max(baseWidth, Math.ceil(baseWidth * ratio));
      }

      return Math.max(baseWidth, history.length * minimumPixelsPerSample);
    }

    function getTimelineScaleDescription(timeframeLabel) {
      return getSelectedTimeframeMs() === null
        ? "Scale: full session fitted into the chart."
        : `Scale: ${timeframeLabel} per screen. Scroll horizontally to move through the full session.`;
    }

    function setScrollableWidth(scrollId, plotId, axisId, history, minimumPixelsPerSample) {
      const scrollEl = document.getElementById(scrollId);
      const plotEl = document.getElementById(plotId);
      const axisEl = document.getElementById(axisId);
      const allDataMode = getSelectedTimeframeMs() === null;
      const pinnedRight = scrollEl.scrollLeft + scrollEl.clientWidth >= scrollEl.scrollWidth - 24;
      const width = getTimelineWidth(scrollEl, history, minimumPixelsPerSample);
      plotEl.style.width = `${width}px`;
      axisEl.style.width = `${width}px`;
      scrollEl.style.overflowX = allDataMode ? "hidden" : "auto";
      if (allDataMode) {
        scrollEl.scrollLeft = 0;
      } else if (pinnedRight) {
        scrollEl.scrollLeft = scrollEl.scrollWidth;
      }
    }

    function getRenderedPlotWidth(plotId, fallbackWidth) {
      const plotEl = document.getElementById(plotId);
      const styledWidth = Number.parseFloat(plotEl.style.width || "");
      if (Number.isFinite(styledWidth) && styledWidth > 0) {
        return styledWidth;
      }

      const clientWidth = Number(plotEl.clientWidth || 0);
      if (Number.isFinite(clientWidth) && clientWidth > 0) {
        return clientWidth;
      }

      return fallbackWidth;
    }

    function configureChartSvg(svg, plotId, height, fallbackWidth) {
      const width = Math.max(1, Math.round(getRenderedPlotWidth(plotId, fallbackWidth)));
      svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
      svg.setAttribute("width", String(width));
      svg.setAttribute("height", String(height));
      return width;
    }

    function getHistoryAxisLabels(history) {
      if (!history.length) {
        return ["-", "-", "-"];
      }

      const domain = getTimelineDomain(history);
      if (!domain) {
        const label = formatTimeLabel(history[history.length - 1].timestamp);
        return [label, label, label];
      }

      return [0, 0.5, 1].map((ratio) => {
        const timestamp = new Date(domain.startMs + domain.spanMs * ratio).toISOString();
        return formatTimeLabel(timestamp);
      });
    }

    function getSampleX(sample, index, history, padding, usableWidth) {
      const domain = getTimelineDomain(history);
      const sampleTime = getSampleTimeMs(sample);
      if (!domain || sampleTime === null) {
        return padding + (index / Math.max(history.length - 1, 1)) * usableWidth;
      }

      const ratio = (sampleTime - domain.startMs) / domain.spanMs;
      return padding + Math.min(Math.max(ratio, 0), 1) * usableWidth;
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

    function getLatencyVisualStyle(history, width, padding) {
      const usableWidth = Math.max(width - padding * 2, 1);
      const pixelsPerSample = usableWidth / Math.max(history.length - 1, 1);
      const lineWidth = Math.max(1, Math.min(3, pixelsPerSample * 0.85));
      const thresholdWidth = Math.max(1, Math.min(2, lineWidth * 0.75 + 0.35));
      const pointRadius = Math.max(1.2, Math.min(3.8, pixelsPerSample * 0.6 + 0.8));
      const timeoutRadius = Math.max(pointRadius + 0.5, Math.min(5.5, pointRadius + 1.2));
      const thresholdFontSize = Math.max(9, Math.min(12, 7 + lineWidth * 1.5));
      const thresholdDash = `${Math.max(4, Math.round(4 + lineWidth * 2))} ${Math.max(3, Math.round(3 + lineWidth))}`;

      return {
        lineWidth,
        thresholdWidth,
        pointRadius,
        timeoutRadius,
        thresholdFontSize,
        thresholdDash,
      };
    }

    function getEventVisualStyle(history, width, padding) {
      const usableWidth = Math.max(width - padding * 2, 1);
      const pixelsPerSample = usableWidth / Math.max(history.length, 1);
      return {
        barWidth: Math.max(1.25, Math.min(4, pixelsPerSample * 0.72)),
        cornerRadius: Math.max(1, Math.min(4, pixelsPerSample * 0.28 + 0.6)),
        idleDotRadius: Math.max(0.7, Math.min(2, pixelsPerSample * 0.24 + 0.45)),
      };
    }

    function renderLatencyChart(history, timeoutMs, thresholdMs) {
      const chart = document.getElementById("latency-chart");
      const fallback = document.getElementById("latency-fallback");
      chart.innerHTML = "";
      setScrollableWidth("latency-scroll", "latency-plot", "latency-x-axis", history, 16);
      setAxisLabels("latency-x-axis", getHistoryAxisLabels(history));

      if (history.length < 2) {
        setAxisLabels("latency-y-axis", ["-", "-", "-"]);
        fallback.style.display = "grid";
        return;
      }

      fallback.style.display = "none";
      const height = 190;
      const width = configureChartSvg(chart, "latency-plot", height, 900);
      const padding = 28;
      const visual = getLatencyVisualStyle(history, width, padding);
      const values = history.map((sample) => sample.latency_ms ?? timeoutMs);
      const maxValue = Math.max(...values, timeoutMs, thresholdMs, 10);
      setAxisLabels("latency-y-axis", [`${maxValue} ms`, `${Math.round(maxValue / 2)} ms`, "0 ms"]);
      drawGrid(chart, width, height, padding, 5);

      const usableWidth = width - padding * 2;
      const usableHeight = height - padding * 2;
      const points = history.map((sample, index) => {
        const x = getSampleX(sample, index, history, padding, usableWidth);
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
          "stroke-width": visual.thresholdWidth,
          "stroke-dasharray": visual.thresholdDash,
        })
      );
      const thresholdLabel = makeSvgElement("text", {
        x: padding + 8,
        y: thresholdY - 8,
        fill: "#e76f51",
        "font-size": visual.thresholdFontSize,
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
          "stroke-width": visual.lineWidth,
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
            r: !point.sample.success ? visual.timeoutRadius : visual.pointRadius,
            fill,
          })
        );
      });
    }

    function renderEventChart(config) {
      const { history, fallbackId, svgId, xAxisId, yAxisId, predicate, color, scrollId, plotId } = config;
      const chart = document.getElementById(svgId);
      const fallback = document.getElementById(fallbackId);
      chart.innerHTML = "";
      setScrollableWidth(scrollId, plotId, xAxisId, history, 12);
      setAxisLabels(xAxisId, getHistoryAxisLabels(history));
      setAxisLabels(yAxisId, ["1 event", "0 events"]);

      if (!history.length) {
        fallback.style.display = "grid";
        return 0;
      }

      fallback.style.display = "none";
      const height = 120;
      const width = configureChartSvg(chart, plotId, height, 900);
      const padding = 24;
      const visual = getEventVisualStyle(history, width, padding);
      const baselineY = height - padding;
      const topY = padding;
      const usableWidth = width - padding * 2;
      const barWidth = visual.barWidth;
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

        const centerX = getSampleX(sample, index, history, padding, usableWidth);
        if (event) {
          chart.appendChild(
            makeSvgElement("rect", {
              x: centerX - barWidth / 2,
              y: topY,
              width: barWidth,
              height: baselineY - topY,
              rx: visual.cornerRadius,
              fill: color,
              opacity: 0.85,
            })
          );
        } else {
          chart.appendChild(
            makeSvgElement("circle", {
              cx: centerX,
              cy: baselineY,
              r: visual.idleDotRadius,
              fill: "rgba(53, 42, 26, 0.28)",
            })
          );
        }
      });

      return count;
    }

    function setGatewayStatus(currentLatencyMs, thresholdMs, lastStatus) {
      const el = document.getElementById(gatewayId("status-text"));
      const detail = document.getElementById(gatewayId("status-detail"));

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

    function renderGatewayLatencyChart(history, timeoutMs, thresholdMs) {
      const chart = document.getElementById(gatewayId("latency-chart"));
      const fallback = document.getElementById(gatewayId("latency-fallback"));
      chart.innerHTML = "";
      setScrollableWidth(gatewayId("latency-scroll"), gatewayId("latency-plot"), gatewayId("latency-x-axis"), history, 16);
      setAxisLabels(gatewayId("latency-x-axis"), getHistoryAxisLabels(history));

      if (history.length < 2) {
        setAxisLabels(gatewayId("latency-y-axis"), ["-", "-", "-"]);
        fallback.style.display = "grid";
        return;
      }

      fallback.style.display = "none";
      const height = 190;
      const width = configureChartSvg(chart, gatewayId("latency-plot"), height, 900);
      const padding = 28;
      const visual = getLatencyVisualStyle(history, width, padding);
      const values = history.map((sample) => sample.latency_ms ?? timeoutMs);
      const maxValue = Math.max(...values, timeoutMs, thresholdMs, 10);
      setAxisLabels(gatewayId("latency-y-axis"), [`${maxValue} ms`, `${Math.round(maxValue / 2)} ms`, "0 ms"]);
      drawGrid(chart, width, height, padding, 5);

      const usableWidth = width - padding * 2;
      const usableHeight = height - padding * 2;
      const points = history.map((sample, index) => {
        const x = getSampleX(sample, index, history, padding, usableWidth);
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
          "stroke-width": visual.thresholdWidth,
          "stroke-dasharray": visual.thresholdDash,
        })
      );
      const thresholdLabel = makeSvgElement("text", {
        x: padding + 8,
        y: thresholdY - 8,
        fill: "#e76f51",
        "font-size": visual.thresholdFontSize,
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
          "stroke-width": visual.lineWidth,
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
            r: !point.sample.success ? visual.timeoutRadius : visual.pointRadius,
            fill,
          })
        );
      });
    }

    function renderGatewayEventChart(kind, history, predicate, color) {
      return renderEventChart({
        history,
        fallbackId: gatewayId(`${kind}-fallback`),
        svgId: gatewayId(`${kind}-chart`),
        xAxisId: gatewayId(`${kind}-x-axis`),
        yAxisId: gatewayId(`${kind}-y-axis`),
        predicate,
        color,
        scrollId: gatewayId(`${kind}-scroll`),
        plotId: gatewayId(`${kind}-plot`),
      });
    }

    function renderGatewayMonitor(data, timeframeLabel) {
      const history = data.history;
      setMetric(gatewayId("target"), data.target);
      setMetric(gatewayId("current-latency"), data.current_latency_ms === null ? "timeout" : `${data.current_latency_ms} ms`);
      setMetric(gatewayId("avg-latency"), data.avg_latency_ms === null ? "-" : `${data.avg_latency_ms} ms`);
      setMetric(gatewayId("packet-loss"), `${data.packet_loss_percent}%`);
      setMetric(gatewayId("packet-loss-events"), `${data.packet_loss_events_total} total loss events`);
      setMetric(gatewayId("started-at"), data.started_at ?? "-");
      setMetric(gatewayId("last-sample-at"), data.last_sample_at ?? "-");
      setMetric(
        gatewayId("min-max"),
        data.min_latency_ms === null ? "-" : `${data.min_latency_ms} ms / ${data.max_latency_ms} ms`
      );
      setMetric(gatewayId("counts"), `${data.sent} / ${data.received} / ${data.lost}`);
      setMetric(gatewayId("failures"), String(data.consecutive_failures));
      setMetric(gatewayId("worker-pid"), data.worker_pid === null ? "-" : String(data.worker_pid));
      setMetric(gatewayId("log-file"), data.log_file);
      setMetric(gatewayId("sample-count"), `${history.length} total samples`);
      setMetric(
        gatewayId("latency-note"),
        `${getTimelineScaleDescription(timeframeLabel)} Polling every ${Math.round(data.interval_seconds * 1000)} ms. Threshold line: ${data.high_ping_threshold_ms} ms.`
      );

      setGatewayStatus(data.current_latency_ms, data.high_ping_threshold_ms, data.last_status);
      renderGatewayLatencyChart(history, data.timeout_ms, data.high_ping_threshold_ms);
      const lossEventCount = renderGatewayEventChart("loss", history, (sample) => !sample.success, "#c44536");
      setMetric(gatewayId("loss-window-count"), `${lossEventCount} total events`);
      setMetric(gatewayId("loss-note"), `${lossEventCount} packet-loss event(s) across the full session. ${getTimelineScaleDescription(timeframeLabel)}`);
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

    async function updateTarget() {
      if (configSaveInFlight) {
        return;
      }

      const input = document.getElementById("target-input");
      const feedback = document.getElementById("target-feedback");
      const target = input.value.trim();
      if (!target) {
        feedback.textContent = "Enter a hostname or IP address.";
        return;
      }

      configSaveInFlight = true;
      feedback.textContent = "Saving...";
      try {
        const response = await fetch("/api/config", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ target }),
        });
        const payload = await response.json();
        if (!response.ok) {
          throw new Error(payload.error || "Unable to save target");
        }
        feedback.textContent = `Internet target updated to ${payload.target}. A new internet session has started.`;
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
        const timeframeLabel = getSelectedTimeframeLabel();
        const internet = data.internet;
        setMetric("target", internet.target);
        setMetric("current-latency", internet.current_latency_ms === null ? "timeout" : `${internet.current_latency_ms} ms`);
        setMetric("avg-latency", internet.avg_latency_ms === null ? "-" : `${internet.avg_latency_ms} ms`);
        setMetric("packet-loss", `${internet.packet_loss_percent}%`);
        setMetric("packet-loss-events", `${internet.packet_loss_events_total} total loss events`);
        setMetric("current-target-control", internet.target);
        setMetric("started-at", internet.started_at ?? "-");
        setMetric("last-sample-at", internet.last_sample_at ?? "-");
        setMetric(
          "min-max",
          internet.min_latency_ms === null ? "-" : `${internet.min_latency_ms} ms / ${internet.max_latency_ms} ms`
        );
        setMetric("counts", `${internet.sent} / ${internet.received} / ${internet.lost}`);
        setMetric("failures", String(internet.consecutive_failures));
        setMetric("worker-pid", internet.worker_pid === null ? "-" : String(internet.worker_pid));
        setMetric("current-timeframe-control", timeframeLabel);
        setMetric("current-poll-rate", `${Math.round(internet.interval_seconds * 1000)} ms`);
        setMetric("log-file", internet.log_file);
        const internetHistory = internet.history;
        setMetric("sample-count", `${internetHistory.length} total samples`);
        setMetric(
          "latency-note",
          `${getTimelineScaleDescription(timeframeLabel)} Polling every ${Math.round(internet.interval_seconds * 1000)} ms. Threshold line: ${internet.high_ping_threshold_ms} ms.`
        );
        setMetric("current-high-ping-threshold", `${internet.high_ping_threshold_ms} ms`);
        syncInputValue("poll-rate-input", Math.round(internet.interval_seconds * 1000));
        syncInputValue("high-ping-threshold-input", internet.high_ping_threshold_ms);
        syncInputValue("target-input", internet.target);

        setStatus(internet.current_latency_ms, internet.high_ping_threshold_ms, internet.last_status);
        renderLatencyChart(internetHistory, internet.timeout_ms, internet.high_ping_threshold_ms);
        const lossEventCount = renderEventChart({
          history: internetHistory,
          fallbackId: "loss-fallback",
          svgId: "loss-chart",
          xAxisId: "loss-x-axis",
          yAxisId: "loss-y-axis",
          predicate: (sample) => !sample.success,
          color: "#c44536",
          scrollId: "loss-scroll",
          plotId: "loss-plot",
        });
        setMetric("loss-window-count", `${lossEventCount} total events`);
        setMetric("loss-note", `${lossEventCount} packet-loss event(s) across the full session. ${getTimelineScaleDescription(timeframeLabel)}`);
        renderGatewayMonitor(data.gateway, timeframeLabel);
      } catch (error) {
        document.getElementById("status-text").textContent = "Disconnected";
        document.getElementById("status-text").className = "metric-value status-bad";
        document.getElementById("status-detail").textContent = "The browser could not reach the local monitor.";
        const gatewayStatus = document.getElementById(gatewayId("status-text"));
        const gatewayDetail = document.getElementById(gatewayId("status-detail"));
        if (gatewayStatus) {
          gatewayStatus.textContent = "Disconnected";
          gatewayStatus.className = "metric-value status-bad";
        }
        if (gatewayDetail) {
          gatewayDetail.textContent = "The browser could not reach the local monitor.";
        }
      }
    }

    ensureGatewayMonitor();
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
    document.getElementById("target-save").addEventListener("click", updateTarget);
    document.getElementById("target-input").addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        updateTarget();
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


def make_handler(
    gateway_state: PingState,
    internet_state: PingState,
    gateway_worker: WorkerControl,
    internet_worker: WorkerControl,
) -> type[BaseHTTPRequestHandler]:
    class PingHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path in {"/", "/index.html"}:
                body = HTML_PAGE.encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Cache-Control", "no-store, max-age=0")
                self.send_header("Pragma", "no-cache")
                self.send_header("Expires", "0")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if self.path == "/api/status":
                payload = {
                    "gateway": gateway_state.snapshot(),
                    "internet": internet_state.snapshot(),
                }
                payload["gateway"]["log_file"] = str(gateway_state.log_file)
                payload["internet"]["log_file"] = str(internet_state.log_file)
                body = json.dumps(payload).encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store, max-age=0")
                self.send_header("Pragma", "no-cache")
                self.send_header("Expires", "0")
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
                    interval_ms = int(payload["interval_ms"])
                    gateway_state.set_interval_ms(interval_ms)
                    internet_state.set_interval_ms(interval_ms)
                    gateway_worker.set_interval_ms(interval_ms)
                    internet_worker.set_interval_ms(interval_ms)
                    updates_applied = True

                if "high_ping_threshold_ms" in payload:
                    threshold_ms = int(payload["high_ping_threshold_ms"])
                    gateway_state.set_high_ping_threshold_ms(threshold_ms)
                    internet_state.set_high_ping_threshold_ms(threshold_ms)
                    updates_applied = True

                if "target" in payload:
                    target = str(payload["target"])
                    internet_state.set_target(target)
                    internet_worker.set_target(target, internet_state.get_session_id())
                    updates_applied = True

                if not updates_applied:
                    raise ValueError("No supported config values provided")
            except (TypeError, ValueError, json.JSONDecodeError):
                body = json.dumps(
                    {
                        "error": (
                            "Provide interval_ms, high_ping_threshold_ms, and/or target with valid values"
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
                    "target": internet_state.get_target(),
                    "interval_ms": round(internet_state.get_interval_seconds() * 1000),
                    "interval_seconds": internet_state.get_interval_seconds(),
                    "high_ping_threshold_ms": internet_state.get_high_ping_threshold_ms(),
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
        default=DEFAULT_INTERNET_TARGET,
        help="Internet hostname or IP to ping. Default: 8.8.8.8",
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

    gateway_log_file, internet_log_file = derive_log_paths(Path(args.log_file).resolve())
    gateway_state = PingState(
        target=DEFAULT_GATEWAY_TARGET,
        interval_seconds=args.interval,
        timeout_ms=args.timeout_ms,
        high_ping_threshold_ms=args.high_ping_threshold_ms,
        max_history_samples=args.history_size,
        log_file=gateway_log_file,
    )
    internet_state = PingState(
        target=normalize_target(args.target),
        interval_seconds=args.interval,
        timeout_ms=args.timeout_ms,
        high_ping_threshold_ms=args.high_ping_threshold_ms,
        max_history_samples=args.history_size,
        log_file=internet_log_file,
    )

    gateway_samples, gateway_session_id = load_samples_from_log(
        gateway_log_file,
        gateway_state.get_target(),
        args.history_size,
    )
    gateway_state.restore_samples(gateway_samples, gateway_session_id)

    internet_samples, internet_session_id = load_samples_from_log(
        internet_log_file,
        internet_state.get_target(),
        args.history_size,
    )
    internet_state.restore_samples(internet_samples, internet_session_id)

    ctx = multiprocessing.get_context("spawn")
    worker_stop_event = ctx.Event()
    listener_stop_event = threading.Event()

    gateway_sample_queue = ctx.Queue()
    gateway_control_queue = ctx.Queue()
    internet_sample_queue = ctx.Queue()
    internet_control_queue = ctx.Queue()

    gateway_listener = threading.Thread(
        target=sample_listener,
        args=(gateway_state, gateway_sample_queue, listener_stop_event),
        daemon=True,
    )
    internet_listener = threading.Thread(
        target=sample_listener,
        args=(internet_state, internet_sample_queue, listener_stop_event),
        daemon=True,
    )

    gateway_process = ctx.Process(
        target=ping_worker,
        args=(
            gateway_state.get_target(),
            gateway_state.get_interval_seconds(),
            gateway_state.timeout_ms,
            str(gateway_state.log_file),
            gateway_sample_queue,
            gateway_control_queue,
            worker_stop_event,
            gateway_state.get_session_id(),
            gateway_state.sequence,
        ),
        daemon=True,
    )
    internet_process = ctx.Process(
        target=ping_worker,
        args=(
            internet_state.get_target(),
            internet_state.get_interval_seconds(),
            internet_state.timeout_ms,
            str(internet_state.log_file),
            internet_sample_queue,
            internet_control_queue,
            worker_stop_event,
            internet_state.get_session_id(),
            internet_state.sequence,
        ),
        daemon=True,
    )

    gateway_worker = WorkerControl(gateway_process, gateway_control_queue, gateway_sample_queue, gateway_listener)
    internet_worker = WorkerControl(internet_process, internet_control_queue, internet_sample_queue, internet_listener)

    gateway_listener.start()
    internet_listener.start()
    gateway_process.start()
    internet_process.start()

    server = ThreadingHTTPServer(
        ("127.0.0.1", args.port),
        make_handler(gateway_state, internet_state, gateway_worker, internet_worker),
    )
    print(f"Gateway ping target: {gateway_state.get_target()}")
    print(f"Internet ping target: {internet_state.get_target()}")
    print(f"Gateway worker PID: {gateway_process.pid}")
    print(f"Internet worker PID: {internet_process.pid}")
    print(f"Gateway log: {gateway_state.log_file}")
    print(f"Internet log: {internet_state.log_file}")
    print("Press Ctrl+C to stop.")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        listener_stop_event.set()
        worker_stop_event.set()
        gateway_worker.stop()
        internet_worker.stop()
        server.server_close()
        gateway_process.join(timeout=3.0)
        internet_process.join(timeout=3.0)
        gateway_listener.join(timeout=1.0)
        internet_listener.join(timeout=1.0)
        if gateway_process.is_alive():
            gateway_process.terminate()
            gateway_process.join(timeout=1.0)
        if internet_process.is_alive():
            internet_process.terminate()
            internet_process.join(timeout=1.0)


if __name__ == "__main__":
    main()
