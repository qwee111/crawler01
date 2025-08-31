# -*- coding: utf-8 -*-
"""
Prometheus 指标定义
"""
from __future__ import annotations

import os
import socket
import threading
from typing import Optional

from prometheus_client import Counter, Gauge, Histogram

ENV = os.getenv("ENV", "dev")
INSTANCE = os.getenv("INSTANCE", socket.gethostname())

# 全局锁，避免并发注册冲突
_registry_lock = threading.Lock()

# 业务指标（统一增加 env/instance 标签）
REQUESTS_IN_FLIGHT = Gauge(
    "crawler_requests_in_flight",
    "当前正在处理的请求数",
    labelnames=("spider", "site", "env", "instance"),
)
REQUEST_TOTAL = Counter(
    "crawler_request_total",
    "请求总数 (按站点/状态码)",
    labelnames=("spider", "site", "status", "env", "instance"),  # status: success/fail
)
REQUEST_LATENCY = Histogram(
    "crawler_request_latency_seconds",
    "请求延迟分布",
    labelnames=("spider", "site", "env", "instance"),
    buckets=(0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10),
)
REQUEST_ERROR = Counter(
    "crawler_request_error_total",
    "请求异常计数",
    labelnames=("spider", "site", "error_type", "env", "instance"),
)

# 数据质量
ITEM_EXTRACTED = Counter(
    "crawler_item_extracted_total",
    "提取到的Item数量",
    labelnames=("spider", "site", "env", "instance"),
)
ITEM_STORED = Counter(
    "crawler_item_stored_total",
    "成功入库的Item数量",
    labelnames=("spider", "site", "env", "instance"),
)
ITEM_DROPPED = Counter(
    "crawler_item_dropped_total",
    "丢弃的Item数量(含重复/校验失败)",
    labelnames=("spider", "site", "reason", "env", "instance"),
)
NEW_CONTENT = Counter(
    "crawler_new_content_total",
    "增量新内容计数",
    labelnames=("spider", "site", "env", "instance"),
)
DUP_CONTENT = Counter(
    "crawler_duplicate_content_total",
    "重复内容计数",
    labelnames=("spider", "site", "env", "instance"),
)

# 队列与去重
QUEUE_LENGTH = Gauge(
    "crawler_queue_length",
    "Redis 队列长度",
    labelnames=("spider", "site", "queue", "env", "instance"),
)
QUEUE_BACKLOG_SECONDS = Gauge(
    "crawler_queue_backlog_seconds",
    "队列积压时长(秒)",
    labelnames=("spider", "site", "queue", "env", "instance"),
)

# 系统资源（统一打上 env/instance 标签）
CPU_USAGE = Gauge(
    "crawler_cpu_usage_percent", "CPU 使用率(%)", labelnames=("env", "instance")
)
MEM_USAGE = Gauge(
    "crawler_mem_usage_percent", "内存使用率(%)", labelnames=("env", "instance")
)
PROC_COUNT = Gauge("crawler_process_count", "进程数", labelnames=("env", "instance"))
THREAD_COUNT = Gauge("crawler_thread_count", "线程数", labelnames=("env", "instance"))
NET_CONN = Gauge("crawler_net_connections", "网络连接数", labelnames=("env", "instance"))
TCP_HANDLES = Gauge("crawler_tcp_handles", "TCP 句柄数(估算)", labelnames=("env", "instance"))
DISK_USAGE = Gauge(
    "crawler_disk_usage_percent", "磁盘使用率(%)", labelnames=("mount", "env", "instance")
)
DISK_RW_BYTES = Gauge(
    "crawler_disk_rw_bytes_per_sec",
    "磁盘读写字节速率/秒",
    labelnames=("direction", "env", "instance"),
)
DB_POOL_INUSE = Gauge(
    "crawler_db_pool_inuse",
    "数据库连接池使用中的连接数",
    labelnames=("db", "env", "instance"),
)
DB_POOL_SIZE = Gauge(
    "crawler_db_pool_size",
    "数据库连接池总大小(如可用)",
    labelnames=("db", "env", "instance"),
)

# Redis 状态
REDIS_UP = Gauge("crawler_redis_up", "Redis 连接是否可用(1/0)", labelnames=("env", "instance"))
REDIS_CONNECTED_CLIENTS = Gauge(
    "crawler_redis_connected_clients", "Redis 连接数", labelnames=("env", "instance")
)
REDIS_USED_MEMORY_BYTES = Gauge(
    "crawler_redis_used_memory_bytes", "Redis 使用内存字节数", labelnames=("env", "instance")
)
REDIS_KEYSPACE_KEYS = Gauge(
    "crawler_redis_keyspace_keys", "Redis 键空间数量", labelnames=("db", "env", "instance")
)
REDIS_SLOWLOG_LEN = Gauge(
    "crawler_redis_slowlog_len", "Redis 慢查询条数", labelnames=("env", "instance")
)

# 便捷工具

def labels_site(spider: str, site: Optional[str]) -> dict[str, str]:
    return {"spider": spider, "site": site or "default", "env": ENV, "instance": INSTANCE}

