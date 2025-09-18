# -*- coding: utf-8 -*-
"""
极简 REST API 提供实时指标查询（可由 Prometheus 外部拉取或运维查看）
依赖：Flask（或 FastAPI），这里使用 Flask 以减小改动面
"""
from __future__ import annotations

import os
from typing import Any, Dict

from flask import Flask, jsonify, request

from .metrics import (
    CPU_USAGE,
    MEM_USAGE,
    REQUEST_TOTAL,
    REQUEST_ERROR,
    ITEM_EXTRACTED,
    ITEM_STORED,
    ITEM_DROPPED,
    QUEUE_LENGTH,
)


def create_app() -> Flask:
    app = Flask(__name__)

    @app.get("/health")
    def health() -> Dict[str, Any]:
        return {"status": "ok"}

    @app.get("/metrics/summary")
    def metrics_summary():
        # 提供一个大致的快照
        data = {
            "cpu": CPU_USAGE._value.get(),  # type: ignore[attr-defined]
            "mem": MEM_USAGE._value.get(),  # type: ignore[attr-defined]
        }
        return jsonify(data)

    @app.get("/metrics/requests")
    def metrics_requests():
        return jsonify(_collect_samples(REQUEST_TOTAL))

    @app.get("/metrics/errors")
    def metrics_errors():
        return jsonify(_collect_samples(REQUEST_ERROR))

    @app.get("/metrics/items")
    def metrics_items():
        return jsonify(
            {
                "extracted": _collect_samples(ITEM_EXTRACTED),
                "stored": _collect_samples(ITEM_STORED),
                "dropped": _collect_samples(ITEM_DROPPED),
            }
        )

    @app.get("/metrics/queues")
    def metrics_queues():
        return jsonify(_collect_samples(QUEUE_LENGTH))

    return app


def _collect_samples(metric) -> Dict[str, Any]:
    # 读取 Prometheus 指标内部样本（仅用于简单展示，不建议做严肃依赖）
    try:
        return {
            str(tuple(s.labels)): s.value
            for s in metric._samples()  # type: ignore[attr-defined]
        }
    except Exception:
        return {}
