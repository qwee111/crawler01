# -*- coding: utf-8 -*-
"""
（已停用）原极简 REST API。项目现不再提供 Web API。
"""
from __future__ import annotations

import os
from typing import Any, Dict

from flask import Flask, jsonify, request

from .metrics import (
    CPU_USAGE,
    ITEM_DROPPED,
    ITEM_EXTRACTED,
    ITEM_STORED,
    MEM_USAGE,
    QUEUE_LENGTH,
    REQUEST_ERROR,
    REQUEST_TOTAL,
)


def create_app() -> Flask:
    # 返回一个占位应用，避免误用
    app = Flask(__name__)

    @app.get("/health")
    def health() -> Dict[str, Any]:
        return {"status": "disabled"}

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
