# -*- coding: utf-8 -*-
"""
Prometheus Exporter 与采集器：
- 启动 HTTP server 暴露 /metrics
- 周期性采集系统资源与 Redis 指标
"""
from __future__ import annotations

import os
import threading
import time
from typing import Optional

import psutil
import redis
from prometheus_client import start_http_server

from .metrics import (
    CPU_USAGE,
    DB_POOL_INUSE,
    DB_POOL_SIZE,
    DISK_RW_BYTES,
    DISK_USAGE,
    MEM_USAGE,
    NET_CONN,
    PROC_COUNT,
    THREAD_COUNT,
    REDIS_USED_MEMORY_BYTES,
    QUEUE_BACKLOG_SECONDS,
    QUEUE_LENGTH,
    REDIS_CONNECTED_CLIENTS,
    REDIS_KEYSPACE_KEYS,
    REDIS_SLOWLOG_LEN,
    REDIS_UP,
    TCP_HANDLES,
    ENV,
    INSTANCE,
)


class MonitoringExporter:
    def __init__(self, port: int = 9108, redis_url: Optional[str] = None):
        self._port = port
        self._redis_url = redis_url or os.getenv("REDIS_URL", "redis://localhost:6379/0")
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []
        self._redis: Optional[redis.Redis] = None

    def start(self) -> None:
        start_http_server(self._port)
        self._threads.append(threading.Thread(target=self._collect_system_loop, daemon=True))
        self._threads.append(threading.Thread(target=self._collect_redis_loop, daemon=True))
        for t in self._threads:
            t.start()

    def stop(self) -> None:
        self._stop.set()
        for t in self._threads:
            t.join(timeout=1)

    # ---------- system ----------
    def _collect_system_loop(self) -> None:
        while not self._stop.is_set():
            try:
                CPU_USAGE.labels(ENV, INSTANCE).set(psutil.cpu_percent(interval=None))
                MEM_USAGE.labels(ENV, INSTANCE).set(psutil.virtual_memory().percent)
                PROC_COUNT.labels(ENV, INSTANCE).set(len(psutil.pids()))
                THREAD_COUNT.labels(ENV, INSTANCE).set(psutil.Process().num_threads())
                NET_CONN.labels(ENV, INSTANCE).set(len(psutil.net_connections()))
                TCP_HANDLES.labels(ENV, INSTANCE).set(sum(1 for c in psutil.net_connections() if c.type))
                # disk
                for part in psutil.disk_partitions():
                    try:
                        usage = psutil.disk_usage(part.mountpoint)
                        DISK_USAGE.labels(part.mountpoint, ENV, INSTANCE).set(usage.percent)
                    except Exception:
                        continue
                # sleep
                time.sleep(5)
            except Exception:
                time.sleep(5)

    # ---------- redis ----------
    def _ensure_redis(self) -> Optional[redis.Redis]:
        try:
            if self._redis is None:
                self._redis = redis.from_url(self._redis_url)
            self._redis.ping()
            REDIS_UP.labels(ENV, INSTANCE).set(1)
            return self._redis
        except Exception:
            REDIS_UP.labels(ENV, INSTANCE).set(0)
            return None

    def _collect_redis_loop(self) -> None:
        while not self._stop.is_set():
            try:
                r = self._ensure_redis()
                if not r:
                    time.sleep(5)
                    continue
                info = r.info()
                REDIS_CONNECTED_CLIENTS.labels(ENV, INSTANCE).set(info.get("connected_clients", 0))
                REDIS_USED_MEMORY_BYTES.labels(ENV, INSTANCE).set(info.get("used_memory", 0))
                # keyspace
                for k, v in info.items():
                    if k.startswith("db") and isinstance(v, dict):
                        REDIS_KEYSPACE_KEYS.labels(k, ENV, INSTANCE).set(v.get("keys", 0))
                # slowlog
                try:
                    REDIS_SLOWLOG_LEN.labels(ENV, INSTANCE).set(len(r.slowlog_get(32)))
                except Exception:
                    pass
                time.sleep(5)
            except Exception:
                time.sleep(5)
