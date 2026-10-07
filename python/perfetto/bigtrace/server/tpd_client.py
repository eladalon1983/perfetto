# Copyright (C) 2026 The Android Open Source Project
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Client and process manager for the tpd trace processor daemon."""

import array
import dataclasses as dc
import hashlib
import logging
import os
import shutil
import socket
import sqlite3
import subprocess
import tempfile
import threading
import time
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

from perfetto.bigtrace.server import query_result

logger = logging.getLogger(__name__)

# TraceFooter.Status values from tpd's envelope.proto.
FOOTER_STATUS_UNKNOWN = 0
FOOTER_STATUS_OK = 1
FOOTER_STATUS_ERROR = 2
FOOTER_STATUS_CANCELLED = 3
FOOTER_STATUS_TRUNCATED = 4


class TpdError(Exception):
  pass


def tpd_trace_uuid(path: str) -> str:
  """Computes the 16-hex-char trace UUID used by tpd's SqliteIndex."""
  canon = os.path.realpath(path)
  return hashlib.sha256(canon.encode("utf-8")).hexdigest()[:16]


def resolve_tpd_server_bin(bin_path: Optional[str] = None) -> Optional[str]:
  """Finds tpd_server: explicit path, env var, or tools/tpd/tpd_server."""
  if bin_path:
    return bin_path if os.path.isfile(bin_path) and os.access(
        bin_path, os.X_OK) else None
  repo_root = os.path.abspath(
      os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
  candidates = [
      os.environ.get("TPD_SERVER"),
      os.path.join(repo_root, "tools", "tpd", "tpd_server"),
      shutil.which("tpd_server"),
  ]
  for cand in candidates:
    if cand and os.path.isfile(cand) and os.access(cand, os.X_OK):
      return os.path.abspath(cand)
  return None


# Wire encoding helpers (standard protobuf binary format) ---------------------


def _varint(value: int) -> bytes:
  if value < 0:
    value += 1 << 64
  out = bytearray()
  while value >= 0x80:
    out.append((value & 0x7F) | 0x80)
    value >>= 7
  out.append(value)
  return bytes(out)


def _signed_int64(value: int) -> int:
  return value - (1 << 64) if value >= (1 << 63) else value


def _tag_varint(field: int, value: int) -> bytes:
  return _varint((field << 3) | 0) + _varint(value)


def _tag_bytes(field: int, payload: bytes) -> bytes:
  return _varint((field << 3) | 2) + _varint(len(payload)) + payload


def _tag_str(field: int, text: str) -> bytes:
  return _tag_bytes(field, text.encode("utf-8"))


def _fields(data: memoryview) -> Iterator[Tuple[int, int, Any]]:
  """Yields (field_number, wire_type, value) for each protobuf field."""
  pos, end = 0, len(data)
  while pos < end:
    tag, pos = query_result._read_varint(data, pos)  # pylint: disable=protected-access
    wire_type = tag & 7
    if wire_type == 0:
      value, pos = query_result._read_varint(data, pos)  # pylint: disable=protected-access
    elif wire_type == 2:
      size, pos = query_result._read_varint(data, pos)  # pylint: disable=protected-access
      value, pos = data[pos:pos + size], pos + size
    elif wire_type == 1:
      value, pos = 0, pos + 8
    elif wire_type == 5:
      value, pos = 0, pos + 4
    else:
      raise TpdError(f"Unsupported wire type {wire_type}")
    yield tag >> 3, wire_type, value


def _read_framed_socket(sock: socket.socket) -> bytes:
  """Reads one varint-length-prefixed frame from a connected socket."""
  length = shift = 0
  for _ in range(10):
    b = sock.recv(1)
    if not b:
      raise EOFError("Socket closed while reading frame header")
    byte = b[0]
    length |= (byte & 0x7F) << shift
    if (byte & 0x80) == 0:
      break
    shift += 7
  else:
    raise TpdError("Frame varint too long")

  buf = bytearray()
  while len(buf) < length:
    chunk = sock.recv(min(65536, length - len(buf)))
    if not chunk:
      raise EOFError("Socket closed mid-frame")
    buf.extend(chunk)
  return bytes(buf)


def _iter_framed_fd(fd: int) -> Iterator[bytes]:
  """Yields varint-length-prefixed frames from a file descriptor until EOF."""
  buf = bytearray()
  eof = False
  while True:
    # Ensure we have at least the varint header.
    pos = 0
    length = shift = 0
    have_len = False
    while not have_len:
      while pos >= len(buf) and not eof:
        chunk = os.read(fd, 65536)
        if not chunk:
          eof = True
        else:
          buf.extend(chunk)
      if pos >= len(buf):
        if pos == 0:
          return
        raise TpdError("Truncated varint header on result stream")
      byte = buf[pos]
      pos += 1
      length |= (byte & 0x7F) << shift
      if (byte & 0x80) == 0:
        have_len = True
      else:
        shift += 7
        if shift >= 70:
          raise TpdError("Result stream varint too long")

    need = pos + length
    while len(buf) < need and not eof:
      chunk = os.read(fd, max(65536, need - len(buf)))
      if not chunk:
        eof = True
      else:
        buf.extend(chunk)
    if len(buf) < need:
      raise TpdError("Truncated frame body on result stream")
    payload = bytes(buf[pos:need])
    del buf[:need]
    yield payload


# Decoded envelope & control models -------------------------------------------


@dc.dataclass
class TpdTraceFooter:
  status: int = FOOTER_STATUS_UNKNOWN
  error_message: str = ""
  elapsed_ns: int = 0
  chunks_sent: int = 0
  queue_ns: int = 0
  acquire_ns: int = 0
  query_ns: int = 0
  acquire_source: str = ""
  tp_pid: int = -1


@dc.dataclass
class TpdQueryComplete:
  traces_total: int = 0
  traces_ok: int = 0
  traces_error: int = 0
  elapsed_ns: int = 0
  traces_truncated: int = 0
  traces_cancelled: int = 0


@dc.dataclass
class TpdResultEnvelope:
  query_id: int = 0
  trace_uuid: str = ""
  trace_path: str = ""
  raw_query_result: Optional[bytes] = None
  footer: Optional[TpdTraceFooter] = None
  complete: Optional[TpdQueryComplete] = None


def _decode_footer(data: memoryview) -> TpdTraceFooter:
  out = TpdTraceFooter()
  for field, _, val in _fields(data):
    if field == 1:
      out.status = int(val)
    elif field == 2:
      out.error_message = bytes(val).decode("utf-8", "replace")
    elif field == 3:
      out.elapsed_ns = _signed_int64(val)
    elif field == 4:
      out.chunks_sent = _signed_int64(val)
    elif field == 7:
      out.queue_ns = _signed_int64(val)
    elif field == 8:
      out.acquire_ns = _signed_int64(val)
    elif field == 9:
      out.query_ns = _signed_int64(val)
    elif field == 10:
      out.acquire_source = bytes(val).decode("utf-8", "replace")
    elif field == 11:
      out.tp_pid = _signed_int64(val)
  return out


def _decode_complete(data: memoryview) -> TpdQueryComplete:
  out = TpdQueryComplete()
  for field, _, val in _fields(data):
    if field == 1:
      out.traces_total = _signed_int64(val)
    elif field == 2:
      out.traces_ok = _signed_int64(val)
    elif field == 3:
      out.traces_error = _signed_int64(val)
    elif field == 4:
      out.elapsed_ns = _signed_int64(val)
    elif field == 5:
      out.traces_truncated = _signed_int64(val)
    elif field == 6:
      out.traces_cancelled = _signed_int64(val)
  return out


def decode_result_envelope(raw: bytes) -> TpdResultEnvelope:
  env = TpdResultEnvelope()
  for field, _, val in _fields(memoryview(raw)):
    if field == 1:
      env.query_id = int(val)
    elif field == 2:
      env.trace_uuid = bytes(val).decode("utf-8", "replace")
    elif field == 3:
      env.trace_path = bytes(val).decode("utf-8", "replace")
    elif field == 4:
      # QueryResultChunk: field 1 is raw_query_result
      for c_field, _, c_val in _fields(val):
        if c_field == 1:
          env.raw_query_result = bytes(c_val)
      if env.raw_query_result is None:
        env.raw_query_result = b""
    elif field == 5:
      env.footer = _decode_footer(val)
    elif field == 6:
      env.complete = _decode_complete(val)
  return env


def _decode_stats(data: memoryview) -> Dict[str, Any]:
  stats: Dict[str, Any] = {
      "residentBytes": 0,
      "arenaBytes": 0,
      "serverRssBytes": 0,
      "aliveCount": 0,
      "hotCount": 0,
      "warmCount": 0,
      "coldCount": 0,
      "memoryBudgetBytes": 0,
      "diskBudgetBytes": 0,
      "aliveCap": 0,
      "cpuWorkers": 0,
      "queriesSubmitted": 0,
      "queriesCompleted": 0,
      "queriesFailed": 0,
      "evictions": 0,
      "spawns": 0,
      "uptimeSeconds": 0,
      "tracesKnown": 0,
      "perTraceMb": 0,
      "cgroupEnabled": False,
      "craEnabled": False,
      "craCaptureCount": 0,
      "craRestoreCount": 0,
      "craStoreBytes": 0,
      "craStoreSnapshots": 0,
      "emptyToHotCount": 0,
      "coldToHotCount": 0,
      "sessionUuid": "",
      "profile": "batch",
      "craCompression": "zstd",
  }
  int_fields = {
      1: "residentBytes",
      2: "arenaBytes",
      3: "serverRssBytes",
      5: "aliveCount",
      6: "hotCount",
      7: "warmCount",
      8: "coldCount",
      9: "memoryBudgetBytes",
      10: "diskBudgetBytes",
      11: "aliveCap",
      12: "cpuWorkers",
      14: "queriesSubmitted",
      15: "queriesCompleted",
      16: "queriesFailed",
      17: "evictions",
      19: "spawns",
      22: "uptimeSeconds",
      23: "tracesKnown",
      24: "perTraceMb",
      77: "craCaptureCount",
      79: "craRestoreCount",
      81: "craStoreBytes",
      82: "craStoreSnapshots",
      85: "emptyToHotCount",
      86: "coldToHotCount",
  }
  str_fields = {
      96: "sessionUuid",
      97: "profile",
      100: "craCompression",
  }
  for field, _, val in _fields(data):
    if field in int_fields:
      stats[int_fields[field]] = _signed_int64(val)
    elif field in str_fields:
      stats[str_fields[field]] = bytes(val).decode("utf-8", "replace")
    elif field == 67:
      stats["cgroupEnabled"] = bool(val)
    elif field == 76:
      stats["craEnabled"] = bool(val)
  empty_count = max(
      0, stats["tracesKnown"] - stats["hotCount"] - stats["warmCount"] -
      stats["coldCount"])
  stats["emptyCount"] = empty_count
  return stats


def _decode_trace_info(data: memoryview) -> Dict[str, Any]:
  info: Dict[str, Any] = {
      "traceUuid": "",
      "tracePath": "",
      "state": "Empty",
      "pid": -1,
      "httpPort": 0,
      "lastUsedUnixMs": 0,
      "residentBytes": 0,
      "arenaBytes": 0,
      "activeQueries": 0,
      "evicting": False,
  }
  for field, _, val in _fields(data):
    if field == 1:
      info["traceUuid"] = bytes(val).decode("utf-8", "replace")
    elif field == 2:
      info["tracePath"] = bytes(val).decode("utf-8", "replace")
    elif field == 3:
      info["state"] = bytes(val).decode("utf-8", "replace")
    elif field == 4:
      info["pid"] = _signed_int64(val)
    elif field == 5:
      info["httpPort"] = _signed_int64(val)
    elif field == 6:
      info["lastUsedUnixMs"] = _signed_int64(val)
    elif field == 7:
      info["residentBytes"] = _signed_int64(val)
    elif field == 8:
      info["arenaBytes"] = _signed_int64(val)
    elif field == 14:
      info["activeQueries"] = _signed_int64(val)
    elif field == 15:
      info["evicting"] = bool(val)
  return info


class TpdClient:
  """Low-level Unix domain socket client for tpd_server."""

  def __init__(self, listen_path: str):
    self.listen_path = listen_path
    self._req_seq = 0
    self._seq_lock = threading.Lock()

  def _next_id(self) -> int:
    with self._seq_lock:
      self._req_seq += 1
      return self._req_seq

  def _round_trip(self,
                  req_field: int,
                  req_payload: bytes,
                  attach_fd: int = -1,
                  timeout: float = 30.0) -> Tuple[int, memoryview]:
    req_id = self._next_id()
    msg = _tag_varint(1, req_id) + _tag_bytes(req_field, req_payload)
    framed = _varint(len(msg)) + msg

    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
      sock.settimeout(timeout)
      sock.connect(self.listen_path)
      if attach_fd >= 0:
        fds = array.array("i", [attach_fd])
        sock.sendmsg([framed], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, fds)])
      else:
        sock.sendall(framed)
      raw_resp = _read_framed_socket(sock)

    resp_field = 0
    resp_val = memoryview(b"")
    for field, _, val in _fields(memoryview(raw_resp)):
      if field == 1:
        continue
      if field == 99:
        # ErrorResponse: 1=code, 2=message
        err_msg = "tpd error"
        for ef, _, ev in _fields(val):
          if ef == 2:
            err_msg = bytes(ev).decode("utf-8", "replace")
        raise TpdError(err_msg)
      resp_field = field
      resp_val = val
    return resp_field, resp_val

  def ping(self, timeout: float = 2.0) -> int:
    _, val = self._round_trip(8, b"", timeout=timeout)
    for field, _, v in _fields(val):
      if field == 1:
        return _signed_int64(v)
    return 0

  def get_stats(self) -> Dict[str, Any]:
    _, val = self._round_trip(4, b"")
    return _decode_stats(val)

  def list_traces(self, state_filter: str = "") -> List[Dict[str, Any]]:
    payload = _tag_str(1, state_filter) if state_filter else b""
    _, val = self._round_trip(5, payload)
    out = []
    for field, _, v in _fields(val):
      if field == 1:
        out.append(_decode_trace_info(v))
    return out

  def rescan(self) -> Dict[str, int]:
    _, val = self._round_trip(11, b"")
    res = {"tracesAdded": 0, "tracesRemoved": 0, "tracesTotal": 0}
    for field, _, v in _fields(val):
      if field == 1:
        res["tracesAdded"] = _signed_int64(v)
      elif field == 2:
        res["tracesRemoved"] = _signed_int64(v)
      elif field == 3:
        res["tracesTotal"] = _signed_int64(v)
    return res

  def reset_session(self) -> Dict[str, Any]:
    _, val = self._round_trip(16, b"")
    res: Dict[str, Any] = {
        "oldSessionUuid": "",
        "newSessionUuid": "",
        "aliveKilled": 0,
        "handlesLogCleared": 0,
    }
    for field, _, v in _fields(val):
      if field == 1:
        res["oldSessionUuid"] = bytes(v).decode("utf-8", "replace")
      elif field == 2:
        res["newSessionUuid"] = bytes(v).decode("utf-8", "replace")
      elif field == 3:
        res["aliveKilled"] = _signed_int64(v)
      elif field == 4:
        res["handlesLogCleared"] = _signed_int64(v)
    return res

  def update_config(
      self,
      memory_budget_mb: Optional[int] = None,
      disk_budget_mb: Optional[int] = None,
      alive_cap: Optional[int] = None,
      cpu_workers: Optional[int] = None,
      per_trace_mb: Optional[int] = None,
  ) -> None:
    parts = []
    if memory_budget_mb is not None:
      parts.append(_tag_varint(1, int(memory_budget_mb) * (1 << 20)))
    if disk_budget_mb is not None:
      parts.append(_tag_varint(2, int(disk_budget_mb) * (1 << 20)))
    if alive_cap is not None:
      parts.append(_tag_varint(3, int(alive_cap)))
    if cpu_workers is not None:
      parts.append(_tag_varint(4, int(cpu_workers)))
    if per_trace_mb is not None:
      parts.append(_tag_varint(5, int(per_trace_mb)))
    self._round_trip(6, b"".join(parts))

  def cancel_query(self,
                   query_id: int,
                   kill_tps: bool = True) -> Dict[str, Any]:
    payload = _tag_varint(1, query_id) + (
        _tag_varint(2, 1) if kill_tps else b"")
    _, val = self._round_trip(3, payload)
    res = {"cancelled": False, "tpsKilled": 0, "pendingSkipped": 0}
    for field, _, v in _fields(val):
      if field == 1:
        res["cancelled"] = bool(v)
      elif field == 2:
        res["tpsKilled"] = _signed_int64(v)
      elif field == 3:
        res["pendingSkipped"] = _signed_int64(v)
    return res

  def shutdown(self) -> None:
    try:
      self._round_trip(25, b"", timeout=5.0)
    except (OSError, EOFError, TpdError):
      pass

  def start_query(
      self,
      sql: str,
      trace_uuids: Optional[Sequence[str]] = None,
      max_rows: int = 0,
      state_filter: int = 0,
      max_traces: int = 0,
  ) -> Tuple[int, int, str, int]:
    """Starts a streaming query; returns (query_id, dispatched, session, r_fd)."""
    parts = [_tag_str(1, sql)]
    if max_rows > 0:
      parts.append(_tag_varint(5, max_rows))
    if state_filter > 0:
      parts.append(_tag_varint(6, state_filter))
    if max_traces > 0:
      parts.append(_tag_varint(7, max_traces))
    if trace_uuids:
      for u in trace_uuids:
        parts.append(_tag_str(8, u))
    payload = b"".join(parts)

    r_fd, w_fd = os.pipe()
    try:
      _, val = self._round_trip(2, payload, attach_fd=w_fd)
    except Exception:
      os.close(r_fd)
      raise
    finally:
      os.close(w_fd)

    query_id = 0
    dispatched = 0
    session_uuid = ""
    for field, _, v in _fields(val):
      if field == 1:
        query_id = int(v)
      elif field == 2:
        dispatched = int(v)
      elif field == 3:
        session_uuid = bytes(v).decode("utf-8", "replace")
    return query_id, dispatched, session_uuid, r_fd


class TpdPoolManager:
  """Manages a tpd_server daemon and syncs the Bigtrace catalog with it."""

  def __init__(
      self,
      tp_binary_path: str,
      tpd_binary_path: Optional[str] = None,
      listen_path: Optional[str] = None,
      session_dir: Optional[str] = None,
      profile: str = "batch",
      memory_budget_mb: int = 0,
      disk_budget_mb: int = -1,
      alive_cap: int = 0,
      cpu_workers: int = 0,
  ):
    self.tp_binary_path = tp_binary_path
    self.tpd_binary_path = resolve_tpd_server_bin(tpd_binary_path)
    self.profile = profile if profile in ("batch", "interactive") else "batch"
    self.memory_budget_mb = memory_budget_mb
    self.disk_budget_mb = disk_budget_mb
    self.alive_cap = alive_cap
    self.cpu_workers = cpu_workers

    self._own_temp_dir: Optional[str] = None
    self._external_daemon = bool(listen_path and os.path.exists(listen_path))
    if session_dir:
      self.session_dir = os.path.abspath(session_dir)
      os.makedirs(self.session_dir, exist_ok=True)
    else:
      self._own_temp_dir = tempfile.mkdtemp(prefix="bigtrace_tpd_")
      self.session_dir = self._own_temp_dir

    # Keep socket path short (<108 bytes for sockaddr_un).
    self.listen_path = listen_path or os.path.join(self.session_dir, "tpd.sock")
    self.index_db_path = os.path.join(self.session_dir, "traces_index.sqlite")
    self._init_index_db()

    self.client = TpdClient(self.listen_path)
    self._proc: Optional[subprocess.Popen] = None
    self._lock = threading.RLock()
    self._tpd_to_bt_uuid: Dict[str, str] = {}
    self._bt_to_tpd_uuid: Dict[str, str] = {}

    if not self._external_daemon:
      self._start_daemon()

  def _init_index_db(self) -> None:
    conn = sqlite3.connect(self.index_db_path)
    try:
      conn.execute("PRAGMA journal_mode=WAL")
      conn.execute("CREATE TABLE IF NOT EXISTS traces ("
                   "trace_uuid TEXT PRIMARY KEY, path TEXT NOT NULL)")
      conn.commit()
    finally:
      conn.close()

  def _start_daemon(self) -> None:
    if not self.tpd_binary_path:
      raise TpdError("tpd_server binary not found")
    if not self.tp_binary_path or not os.path.isfile(self.tp_binary_path):
      raise TpdError(
          f"trace_processor_shell binary not found: {self.tp_binary_path}")

    cmd = [
        self.tpd_binary_path,
        f"--trace-index-db={self.index_db_path}",
        "--trace-index-table=traces",
        "--index-poll-ms=2000",
        f"--session-dir={self.session_dir}",
        f"--listen={self.listen_path}",
        f"--tp-binary={self.tp_binary_path}",
        f"--profile={self.profile}",
        "--events-sqlite=off",
        "--quiet",
    ]
    if self.memory_budget_mb > 0:
      cmd.append(f"--memory-budget-mb={self.memory_budget_mb}")
    if self.disk_budget_mb >= 0:
      cmd.append(f"--disk-budget-mb={self.disk_budget_mb}")
    if self.alive_cap > 0:
      cmd.append(f"--alive-cap={self.alive_cap}")
    if self.cpu_workers > 0:
      cmd.append(f"--cpu-workers={self.cpu_workers}")

    log_file = os.path.join(self.session_dir, "tpd_server.log")
    with open(log_file, "ab") as stderr_f:
      self._proc = subprocess.Popen(
          cmd,
          stdin=subprocess.DEVNULL,
          stdout=stderr_f,
          stderr=stderr_f,
          close_fds=True,
      )

    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
      if self._proc.poll() is not None:
        try:
          with open(log_file, "r", encoding="utf-8", errors="replace") as f:
            err_tail = f.read()[-1000:]
        except OSError:
          err_tail = ""
        raise TpdError(
            f"tpd_server exited during startup (code {self._proc.returncode}): "
            f"{err_tail}")
      if os.path.exists(self.listen_path):
        try:
          self.client.ping(timeout=0.5)
          return
        except (OSError, EOFError, TpdError):
          pass
      time.sleep(0.02)
    self.close()
    raise TpdError("Timed out waiting for tpd_server control socket")

  def sync_catalog(self, traces: Sequence[Any]) -> None:
    """Syncs the Bigtrace catalog entries into tpd's SqliteIndex and rescans."""
    with self._lock:
      rows: List[Tuple[str, str]] = []
      seen_tpd_uuids = set()
      tpd_to_bt: Dict[str, str] = {}
      bt_to_tpd: Dict[str, str] = {}

      for t in traces:
        if t.file_size <= 0 or not os.path.isfile(t.file_path):
          continue
        abs_path = os.path.abspath(t.file_path)
        tuuid = tpd_trace_uuid(abs_path)
        tpd_to_bt[tuuid] = t.trace_uuid
        bt_to_tpd[t.trace_uuid] = tuuid
        if tuuid not in seen_tpd_uuids:
          seen_tpd_uuids.add(tuuid)
          rows.append((tuuid, abs_path))

      self._tpd_to_bt_uuid = tpd_to_bt
      self._bt_to_tpd_uuid = bt_to_tpd

      if not self._external_daemon:
        conn = sqlite3.connect(self.index_db_path)
        try:
          conn.execute("DELETE FROM traces")
          conn.executemany(
              "INSERT INTO traces (trace_uuid, path) VALUES (?, ?)", rows)
          conn.commit()
        finally:
          conn.close()
        try:
          self.client.rescan()
        except TpdError as e:
          logger.warning("tpd rescan failed: %s", e)

  def tpd_uuid_for(self, bt_trace_uuid: str, file_path: str) -> str:
    with self._lock:
      tuuid = self._bt_to_tpd_uuid.get(bt_trace_uuid)
      if tuuid:
        return tuuid
    return tpd_trace_uuid(os.path.abspath(file_path))

  def set_profile(self, profile: str) -> Dict[str, Any]:
    """Switches between 'batch' and 'interactive' profiles by restarting tpd."""
    if profile not in ("batch", "interactive"):
      raise ValueError("profile must be 'batch' or 'interactive'")
    with self._lock:
      if profile == self.profile and (self._external_daemon or
                                      (self._proc and
                                       self._proc.poll() is None)):
        return self.get_status()
      self.profile = profile
      if not self._external_daemon:
        self._stop_daemon()
        self._start_daemon()
        try:
          self.client.rescan()
        except TpdError:
          pass
      return self.get_status()

  def get_status(self) -> Dict[str, Any]:
    try:
      stats = self.client.get_stats()
      stats["enabled"] = True
      stats["listenPath"] = self.listen_path
      stats["sessionDir"] = self.session_dir
      return stats
    except (OSError, EOFError, TpdError) as e:
      return {
          "enabled": False,
          "error": str(e),
          "profile": self.profile,
          "listenPath": self.listen_path,
      }

  def _stop_daemon(self) -> None:
    proc = self._proc
    self._proc = None
    if proc is None:
      return
    if proc.poll() is None:
      self.client.shutdown()
      try:
        proc.wait(timeout=5.0)
      except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=2.0)

  def close(self) -> None:
    with self._lock:
      if not self._external_daemon:
        self._stop_daemon()
      if self._own_temp_dir and os.path.isdir(self._own_temp_dir):
        shutil.rmtree(self._own_temp_dir, ignore_errors=True)
        self._own_temp_dir = None
