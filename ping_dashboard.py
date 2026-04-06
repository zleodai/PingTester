#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import multiprocessing
import queue
import re
import sqlite3
import subprocess
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse


TIME_REGEX = re.compile(r"time[=<]\s*(\d+)\s*ms", re.IGNORECASE)
DEFAULT_GATEWAY_TARGET = "10.0.0.1"
DEFAULT_INTERNET_TARGET = "8.8.8.8"
DEFAULT_HIGH_PING_THRESHOLD_MS = 150
DEFAULT_DATABASE_FILE = "ping-monitor.db"
MAX_LATENCY_POINTS = 720
MAX_LOSS_BUCKETS = 180
TIMEFRAME_OPTIONS: dict[str, tuple[str, int]] = {
    "1m": ("1 minute", 60 * 1000),
    "5m": ("5 minutes", 5 * 60 * 1000),
    "10m": ("10 minutes", 10 * 60 * 1000),
    "30m": ("30 minutes", 30 * 60 * 1000),
    "1h": ("1 hour", 60 * 60 * 1000),
    "2h": ("2 hours", 2 * 60 * 60 * 1000),
    "4h": ("4 hours", 4 * 60 * 60 * 1000),
    "12h": ("12 hours", 12 * 60 * 60 * 1000),
    "24h": ("24 hours", 24 * 60 * 60 * 1000),
}
HTML_PATH = Path(__file__).with_name("dashboard.html")


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def now_iso() -> str:
    return now_utc().isoformat(timespec="seconds")


def normalize_target(target: str) -> str:
    cleaned = target.strip()
    if not cleaned:
        raise ValueError("target must not be empty")
    return cleaned


def normalize_timeframe_key(value: str | None) -> str:
    if value and value in TIMEFRAME_OPTIONS:
        return value
    return "1h"


def parse_offset(value: str | None) -> int:
    if value is None:
        return 0
    offset = int(value)
    if offset < 0:
        raise ValueError("offset must be 0 or greater")
    return offset


def iso_or_dash(value: datetime | None) -> str:
    return value.isoformat(timespec="seconds") if value else "-"


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
    monitor_name: str
    target: str
    interval_seconds: float
    timeout_ms: int
    high_ping_threshold_ms: int
    lock: threading.Lock = field(default_factory=threading.Lock)
    session_id: int | None = None
    started_at: str = field(default_factory=now_iso)
    sequence: int = 0
    sent: int = 0
    received: int = 0
    lost: int = 0
    consecutive_failures: int = 0
    current_latency_ms: int | None = None
    last_status: str = "Waiting for first sample"
    last_sample_at: str | None = None
    latency_total: int = 0
    latency_count: int = 0
    min_latency_ms: int | None = None
    max_latency_ms: int | None = None
    worker_pid: int | None = None

    def apply_session(self, session_id: int, target: str, started_at: str, summary: dict[str, Any]) -> None:
        with self.lock:
            self.session_id = session_id
            self.target = target
            self.started_at = started_at
            self.sequence = int(summary.get("sequence") or 0)
            self.sent = int(summary.get("sent") or 0)
            self.received = int(summary.get("received") or 0)
            self.lost = int(summary.get("lost") or 0)
            self.consecutive_failures = int(summary.get("consecutive_failures") or 0)
            self.current_latency_ms = summary.get("current_latency_ms")
            self.last_status = str(summary.get("last_status") or "Waiting for first sample")
            self.last_sample_at = summary.get("last_sample_at")
            self.latency_total = int(summary.get("latency_total") or 0)
            self.latency_count = int(summary.get("latency_count") or 0)
            self.min_latency_ms = summary.get("min_latency_ms")
            self.max_latency_ms = summary.get("max_latency_ms")

    def start_new_session(self, session_id: int, target: str, started_at: str) -> None:
        with self.lock:
            self.session_id = session_id
            self.target = normalize_target(target)
            self.started_at = started_at
            self.sequence = 0
            self.sent = 0
            self.received = 0
            self.lost = 0
            self.consecutive_failures = 0
            self.current_latency_ms = None
            self.last_status = "Waiting for first sample"
            self.last_sample_at = None
            self.latency_total = 0
            self.latency_count = 0
            self.min_latency_ms = None
            self.max_latency_ms = None

    def add_sample(self, sample: Sample) -> bool:
        with self.lock:
            if self.session_id is None or sample.session_id != self.session_id:
                return False
            self.sequence = max(self.sequence, sample.sequence)
            self.sent += 1
            self.last_sample_at = sample.timestamp
            self.last_status = sample.message
            self.current_latency_ms = sample.latency_ms
            if sample.success:
                self.received += 1
                self.consecutive_failures = 0
                if sample.latency_ms is not None:
                    self.latency_total += sample.latency_ms
                    self.latency_count += 1
                    if self.min_latency_ms is None or sample.latency_ms < self.min_latency_ms:
                        self.min_latency_ms = sample.latency_ms
                    if self.max_latency_ms is None or sample.latency_ms > self.max_latency_ms:
                        self.max_latency_ms = sample.latency_ms
            else:
                self.lost += 1
                self.consecutive_failures += 1
            return True

    def set_runtime_error(self, message: str) -> None:
        with self.lock:
            self.current_latency_ms = None
            self.last_status = message
            self.last_sample_at = now_iso()

    def set_interval_ms(self, interval_ms: int) -> None:
        if interval_ms <= 0:
            raise ValueError("interval_ms must be greater than 0")
        with self.lock:
            self.interval_seconds = interval_ms / 1000.0

    def get_interval_seconds(self) -> float:
        with self.lock:
            return self.interval_seconds

    def get_target(self) -> str:
        with self.lock:
            return self.target

    def get_session_id(self) -> int:
        with self.lock:
            if self.session_id is None:
                raise RuntimeError(f"{self.monitor_name} session not initialized")
            return self.session_id

    def set_high_ping_threshold_ms(self, threshold_ms: int) -> None:
        if threshold_ms <= 0:
            raise ValueError("threshold_ms must be greater than 0")
        with self.lock:
            self.high_ping_threshold_ms = threshold_ms

    def get_high_ping_threshold_ms(self) -> int:
        with self.lock:
            return self.high_ping_threshold_ms

    def set_worker_pid(self, worker_pid: int | None) -> None:
        with self.lock:
            self.worker_pid = worker_pid

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            packet_loss = round((self.lost / self.sent * 100.0), 2) if self.sent else 0.0
            avg = round(self.latency_total / self.latency_count, 2) if self.latency_count else None
            return {
                "monitor_name": self.monitor_name,
                "session_id": self.session_id,
                "target": self.target,
                "interval_seconds": self.interval_seconds,
                "timeout_ms": self.timeout_ms,
                "high_ping_threshold_ms": self.high_ping_threshold_ms,
                "started_at": self.started_at,
                "sent": self.sent,
                "received": self.received,
                "lost": self.lost,
                "packet_loss_events_total": self.lost,
                "packet_loss_percent": packet_loss,
                "consecutive_failures": self.consecutive_failures,
                "current_latency_ms": self.current_latency_ms,
                "last_status": self.last_status,
                "last_sample_at": self.last_sample_at,
                "min_latency_ms": self.min_latency_ms,
                "avg_latency_ms": avg,
                "max_latency_ms": self.max_latency_ms,
                "worker_pid": self.worker_pid,
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


class SampleRepository:
    def __init__(self, database_file: Path) -> None:
        self.database_file = database_file

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_file, timeout=30.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=NORMAL")
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=30000")
        return connection

    def ensure_schema(self) -> None:
        self.database_file.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS ping_sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    monitor_name TEXT NOT NULL,
                    target TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    ended_at TEXT NULL
                );
                CREATE INDEX IF NOT EXISTS ping_sessions_monitor_target_started_idx
                    ON ping_sessions (monitor_name, target, started_at DESC);
                CREATE TABLE IF NOT EXISTS ping_samples (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id INTEGER NOT NULL REFERENCES ping_sessions(id) ON DELETE CASCADE,
                    monitor_name TEXT NOT NULL,
                    target TEXT NOT NULL,
                    sequence INTEGER NOT NULL,
                    sample_time TEXT NOT NULL,
                    success INTEGER NOT NULL,
                    latency_ms INTEGER NULL,
                    message TEXT NOT NULL,
                    return_code INTEGER NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS ping_samples_session_sequence_uidx
                    ON ping_samples (session_id, sequence);
                CREATE INDEX IF NOT EXISTS ping_samples_session_time_idx
                    ON ping_samples (session_id, sample_time);
                """
            )

    def create_session(self, monitor_name: str, target: str) -> tuple[int, str]:
        started_at = now_iso()
        with self.connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO ping_sessions (monitor_name, target, started_at)
                VALUES (?, ?, ?)
                """,
                (monitor_name, target, started_at),
            )
            session_id = int(cursor.lastrowid)
        return session_id, started_at

    def close_session(self, session_id: int) -> None:
        with self.connect() as connection:
            connection.execute(
                "UPDATE ping_sessions SET ended_at = COALESCE(ended_at, ?) WHERE id = ?",
                (now_iso(), session_id),
            )

    def resume_or_create_session(self, monitor_name: str, target: str) -> tuple[int, str, dict[str, Any]]:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT id, started_at
                FROM ping_sessions
                WHERE monitor_name = ? AND target = ?
                ORDER BY started_at DESC
                LIMIT 1
                """,
                (monitor_name, target),
            ).fetchone()
        if row is None:
            session_id, started_at = self.create_session(monitor_name, target)
            return session_id, started_at, self.empty_summary()
        session_id = int(row["id"])
        started_at = str(row["started_at"])
        return session_id, started_at, self.get_session_summary(session_id)

    def empty_summary(self) -> dict[str, Any]:
        return {
            "sequence": 0,
            "sent": 0,
            "received": 0,
            "lost": 0,
            "consecutive_failures": 0,
            "current_latency_ms": None,
            "last_status": "Waiting for first sample",
            "last_sample_at": None,
            "latency_total": 0,
            "latency_count": 0,
            "min_latency_ms": None,
            "max_latency_ms": None,
        }

    def get_session_summary(self, session_id: int) -> dict[str, Any]:
        summary = self.empty_summary()
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT
                    COALESCE(MAX(sequence), 0) AS sequence,
                    COUNT(*) AS sent,
                    SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) AS received,
                    SUM(CASE WHEN success = 0 THEN 1 ELSE 0 END) AS lost,
                    COALESCE(SUM(CASE WHEN latency_ms IS NOT NULL THEN latency_ms ELSE 0 END), 0) AS latency_total,
                    COUNT(latency_ms) AS latency_count,
                    MIN(latency_ms) AS min_latency_ms,
                    MAX(latency_ms) AS max_latency_ms
                FROM ping_samples
                WHERE session_id = ?
                """,
                (session_id,),
            ).fetchone()
            if row is not None:
                summary["sequence"] = int(row["sequence"] or 0)
                summary["sent"] = int(row["sent"] or 0)
                summary["received"] = int(row["received"] or 0)
                summary["lost"] = int(row["lost"] or 0)
                summary["latency_total"] = int(row["latency_total"] or 0)
                summary["latency_count"] = int(row["latency_count"] or 0)
                summary["min_latency_ms"] = row["min_latency_ms"]
                summary["max_latency_ms"] = row["max_latency_ms"]

            last_row = connection.execute(
                """
                SELECT sample_time, latency_ms, message
                FROM ping_samples
                WHERE session_id = ?
                ORDER BY sequence DESC
                LIMIT 1
                """,
                (session_id,),
            ).fetchone()
            if last_row is not None:
                summary["last_sample_at"] = str(last_row["sample_time"])
                summary["current_latency_ms"] = last_row["latency_ms"]
                summary["last_status"] = str(last_row["message"])

            recent_rows = connection.execute(
                """
                SELECT success
                FROM ping_samples
                WHERE session_id = ?
                ORDER BY sequence DESC
                """,
                (session_id,),
            ).fetchall()
            failures = 0
            for failure_row in recent_rows:
                if int(failure_row["success"]):
                    break
                failures += 1
            summary["consecutive_failures"] = failures
        return summary

    def insert_sample(self, monitor_name: str, target: str, sample: Sample) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO ping_samples (
                    session_id, monitor_name, target, sequence, sample_time,
                    success, latency_ms, message, return_code
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (session_id, sequence) DO NOTHING
                """,
                (
                    sample.session_id,
                    monitor_name,
                    target,
                    sample.sequence,
                    sample.timestamp,
                    1 if sample.success else 0,
                    sample.latency_ms,
                    sample.message,
                    sample.return_code,
                ),
            )

    def fetch_window_samples(
        self,
        session_id: int,
        start_at: datetime,
        end_at: datetime,
        max_points: int,
    ) -> tuple[int, list[dict[str, Any]]]:
        start_at_iso = start_at.isoformat(timespec="seconds")
        end_at_iso = end_at.isoformat(timespec="seconds")
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT sequence, sample_time, success, latency_ms, message, return_code
                FROM ping_samples
                WHERE session_id = ?
                  AND sample_time >= ?
                  AND sample_time < ?
                ORDER BY sample_time ASC, sequence ASC
                """,
                (session_id, start_at_iso, end_at_iso),
            ).fetchall()
        samples = [
            Sample(
                sequence=int(row["sequence"]),
                session_id=session_id,
                timestamp=str(row["sample_time"]),
                success=bool(row["success"]),
                latency_ms=row["latency_ms"],
                message=str(row["message"]),
                return_code=int(row["return_code"]),
            )
            for row in rows
        ]
        downsampled = self._downsample_latency_samples(samples, max_points)
        return len(samples), [sample.to_dict() for sample in downsampled]

    def fetch_loss_buckets(
        self,
        session_id: int,
        start_at: datetime,
        end_at: datetime,
        span_ms: int,
    ) -> dict[str, Any]:
        bucket_count = max(24, min(MAX_LOSS_BUCKETS, math.ceil(span_ms / 1000)))
        bucket_ms = max(1, math.ceil(span_ms / bucket_count))
        bucket_counts = [0] * bucket_count
        start_ms = int(start_at.timestamp() * 1000)
        end_at_iso = end_at.isoformat(timespec="seconds")
        start_at_iso = start_at.isoformat(timespec="seconds")
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT sample_time
                FROM ping_samples
                WHERE session_id = ?
                  AND success = 0
                  AND sample_time >= ?
                  AND sample_time < ?
                ORDER BY sample_time ASC
                """,
                (session_id, start_at_iso, end_at_iso),
            ).fetchall()
        for row in rows:
            sample_ms = int(datetime.fromisoformat(str(row["sample_time"])).timestamp() * 1000)
            index = min(bucket_count - 1, max(0, (sample_ms - start_ms) // bucket_ms))
            bucket_counts[index] += 1
        return {
            "bucket_ms": bucket_ms,
            "bucket_count": bucket_count,
            "max_loss_count": max(bucket_counts) if bucket_counts else 0,
            "total_loss_events": sum(bucket_counts),
            "buckets": [{"index": index, "loss_count": count} for index, count in enumerate(bucket_counts)],
        }

    def build_window_payload(self, session_id: int, timeframe_key: str, window_offset: int) -> dict[str, Any]:
        key = normalize_timeframe_key(timeframe_key)
        label, duration_ms = TIMEFRAME_OPTIONS[key]
        end_at = now_utc() - timedelta(milliseconds=duration_ms * window_offset)
        start_at = end_at - timedelta(milliseconds=duration_ms)
        sample_count, sampled_rows = self.fetch_window_samples(session_id, start_at, end_at, MAX_LATENCY_POINTS)
        loss_window = self.fetch_loss_buckets(session_id, start_at, end_at, duration_ms)
        return {
            "timeframe_key": key,
            "timeframe_label": label,
            "duration_ms": duration_ms,
            "offset_windows": window_offset,
            "is_live": window_offset == 0,
            "start_at": iso_or_dash(start_at),
            "end_at": iso_or_dash(end_at),
            "sample_count": sample_count,
            "rendered_sample_count": len(sampled_rows),
            "samples": sampled_rows,
            "loss_window": loss_window,
        }

    def _downsample_latency_samples(self, samples: list[Sample], max_points: int) -> list[Sample]:
        if len(samples) <= max_points:
            return samples
        if max_points < 3:
            return samples[:max_points]
        selected: list[Sample] = [samples[0]]
        middle = samples[1:-1]
        bucket_count = max_points - 2
        chunk_size = max(1, math.ceil(len(middle) / bucket_count))
        for index in range(0, len(middle), chunk_size):
            chunk = middle[index : index + chunk_size]
            if not chunk:
                continue
            failures = [sample for sample in chunk if not sample.success]
            if failures:
                selected.append(failures[-1])
                continue
            successes = [sample for sample in chunk if sample.latency_ms is not None]
            if successes:
                selected.append(max(successes, key=lambda sample: (sample.latency_ms or 0, sample.sequence)))
            else:
                selected.append(chunk[-1])
        if samples[-1].sequence != selected[-1].sequence:
            selected.append(samples[-1])
        selected.sort(key=lambda sample: (sample.timestamp, sample.sequence))
        return selected[:max_points]


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


def ping_worker(
    monitor_name: str,
    target: str,
    interval_seconds: float,
    timeout_ms: int,
    database_file: str,
    sample_queue: multiprocessing.queues.Queue,
    control_queue: multiprocessing.queues.Queue,
    stop_event: multiprocessing.synchronize.Event,
    session_id: int,
    starting_sequence: int = 0,
) -> None:
    repository = SampleRepository(Path(database_file))
    current_target = normalize_target(target)
    current_interval_seconds = interval_seconds
    current_session_id = session_id
    sequence = starting_sequence
    parent_process = multiprocessing.parent_process()
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
                if interval_ms > 0:
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
        success, latency, message, return_code = run_ping(current_target, timeout_ms)
        sequence += 1
        sample = Sample(
            sequence=sequence,
            session_id=current_session_id,
            timestamp=now_iso(),
            success=success,
            latency_ms=latency,
            message=message,
            return_code=return_code,
        )
        try:
            repository.insert_sample(monitor_name, current_target, sample)
        except Exception as error:  # pragma: no cover
            sample_queue.put({"type": "worker_error", "message": f"Database insert failed: {error}"})
            if stop_event.wait(max(current_interval_seconds, 1.0)):
                break
            continue
        sample_queue.put({"type": "sample", "sample": sample.to_dict()})
        if stop_event.wait(current_interval_seconds):
            break


def sample_listener(state: PingState, sample_queue: multiprocessing.queues.Queue, stop_event: threading.Event) -> None:
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
        if message_type == "worker_error":
            state.set_runtime_error(str(message.get("message") or "Database insert failed"))
            continue
        if message_type != "sample":
            continue
        payload = message.get("sample")
        if not isinstance(payload, dict):
            continue
        state.add_sample(
            Sample(
                sequence=int(payload["sequence"]),
                session_id=int(payload["session_id"]),
                timestamp=str(payload["timestamp"]),
                success=bool(payload["success"]),
                latency_ms=payload["latency_ms"],
                message=str(payload["message"]),
                return_code=int(payload["return_code"]),
            )
        )


def build_monitor_payload(
    repository: SampleRepository,
    state: PingState,
    timeframe_key: str,
    window_offset: int,
    reference_time: datetime | None = None,
) -> dict[str, Any]:
    payload = state.snapshot()
    if reference_time is None:
        payload["window"] = repository.build_window_payload(state.get_session_id(), timeframe_key, window_offset)
    else:
        key = normalize_timeframe_key(timeframe_key)
        label, duration_ms = TIMEFRAME_OPTIONS[key]
        end_at = reference_time - timedelta(milliseconds=duration_ms * window_offset)
        start_at = end_at - timedelta(milliseconds=duration_ms)
        sample_count, sampled_rows = repository.fetch_window_samples(state.get_session_id(), start_at, end_at, MAX_LATENCY_POINTS)
        loss_window = repository.fetch_loss_buckets(state.get_session_id(), start_at, end_at, duration_ms)
        payload["window"] = {
            "timeframe_key": key,
            "timeframe_label": label,
            "duration_ms": duration_ms,
            "offset_windows": window_offset,
            "is_live": window_offset == 0,
            "start_at": iso_or_dash(start_at),
            "end_at": iso_or_dash(end_at),
            "sample_count": sample_count,
            "rendered_sample_count": len(sampled_rows),
            "samples": sampled_rows,
            "loss_window": loss_window,
        }
    return payload


def load_html_page() -> str:
    return HTML_PATH.read_text(encoding="utf-8")


def make_handler(
    repository: SampleRepository,
    gateway_state: PingState,
    internet_state: PingState,
    gateway_worker: WorkerControl,
    internet_worker: WorkerControl,
) -> type[BaseHTTPRequestHandler]:
    class PingHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            if parsed.path in {"/", "/index.html"}:
                body = load_html_page().encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Cache-Control", "no-store, max-age=0")
                self.send_header("Pragma", "no-cache")
                self.send_header("Expires", "0")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if parsed.path == "/api/status":
                query = parse_qs(parsed.query)
                try:
                    timeframe_key = normalize_timeframe_key((query.get("timeframe") or ["1h"])[0])
                    window_offset = parse_offset((query.get("offset") or ["0"])[0])
                    reference_time = now_utc()
                    payload = {
                        "gateway": build_monitor_payload(repository, gateway_state, timeframe_key, window_offset, reference_time),
                        "internet": build_monitor_payload(repository, internet_state, timeframe_key, window_offset, reference_time),
                    }
                except ValueError as error:
                    body = json.dumps({"error": str(error)}).encode("utf-8")
                    self.send_response(HTTPStatus.BAD_REQUEST)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return
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
                    new_target = normalize_target(str(payload["target"]))
                    repository.close_session(internet_state.get_session_id())
                    new_session_id, started_at = repository.create_session("internet", new_target)
                    internet_state.start_new_session(new_session_id, new_target, started_at)
                    internet_worker.set_target(new_target, new_session_id)
                    updates_applied = True
                if not updates_applied:
                    raise ValueError("No supported config values provided")
            except (TypeError, ValueError, json.JSONDecodeError) as error:
                body = json.dumps({"error": str(error)}).encode("utf-8")
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
        description="Continuously ping a gateway and internet target, store the data in a local SQLite database, and serve a browser dashboard."
    )
    parser.add_argument("target", nargs="?", default=DEFAULT_INTERNET_TARGET, help="Internet hostname or IP to ping.")
    parser.add_argument("--interval", type=float, default=1.0, help="Seconds between ping attempts.")
    parser.add_argument("--timeout-ms", type=int, default=1000, help="Ping timeout in milliseconds.")
    parser.add_argument(
        "--high-ping-threshold-ms",
        type=int,
        default=DEFAULT_HIGH_PING_THRESHOLD_MS,
        help="Threshold for high-ping events in milliseconds.",
    )
    parser.add_argument("--port", type=int, default=8765, help="Local port for the dashboard.")
    parser.add_argument(
        "--database-file",
        default=DEFAULT_DATABASE_FILE,
        help=f"SQLite database file path. Default: {DEFAULT_DATABASE_FILE}",
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
    if not (1 <= args.port <= 65535):
        raise SystemExit("--port must be between 1 and 65535")
    database_file = Path(args.database_file).resolve()
    repository = SampleRepository(database_file)
    repository.ensure_schema()

    gateway_state = PingState(
        monitor_name="gateway",
        target=DEFAULT_GATEWAY_TARGET,
        interval_seconds=args.interval,
        timeout_ms=args.timeout_ms,
        high_ping_threshold_ms=args.high_ping_threshold_ms,
    )
    internet_state = PingState(
        monitor_name="internet",
        target=normalize_target(args.target),
        interval_seconds=args.interval,
        timeout_ms=args.timeout_ms,
        high_ping_threshold_ms=args.high_ping_threshold_ms,
    )

    gateway_session_id, gateway_started_at, gateway_summary = repository.resume_or_create_session(
        "gateway", gateway_state.get_target()
    )
    gateway_state.apply_session(gateway_session_id, gateway_state.get_target(), gateway_started_at, gateway_summary)
    internet_session_id, internet_started_at, internet_summary = repository.resume_or_create_session(
        "internet", internet_state.get_target()
    )
    internet_state.apply_session(
        internet_session_id,
        internet_state.get_target(),
        internet_started_at,
        internet_summary,
    )

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
            gateway_state.monitor_name,
            gateway_state.get_target(),
            gateway_state.get_interval_seconds(),
            gateway_state.timeout_ms,
            str(database_file),
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
            internet_state.monitor_name,
            internet_state.get_target(),
            internet_state.get_interval_seconds(),
            internet_state.timeout_ms,
            str(database_file),
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
        make_handler(repository, gateway_state, internet_state, gateway_worker, internet_worker),
    )
    print(f"Gateway ping target: {gateway_state.get_target()}")
    print(f"Internet ping target: {internet_state.get_target()}")
    print(f"Gateway worker PID: {gateway_process.pid}")
    print(f"Internet worker PID: {internet_process.pid}")
    print(f"Database file: {database_file}")
    print(f"Dashboard: http://127.0.0.1:{args.port}")
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
        repository.close_session(gateway_state.get_session_id())
        repository.close_session(internet_state.get_session_id())


if __name__ == "__main__":
    main()
