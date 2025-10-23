# -*- coding: utf-8 -*-
"""
数据库客户端指标采集：
- SQLAlchemy Engine 连接池 checkout/checkin
- PyMongo Client 最大连接数
"""
from __future__ import annotations

from typing import Any

from .metrics import DB_POOL_INUSE, DB_POOL_SIZE, ENV, INSTANCE


def instrument_sqlalchemy_engine(engine: Any, db: str = "postgres") -> None:
    # 已移除 SQLAlchemy 监控（无关系型数据库）
    return


def instrument_mongo_client(client: Any, db: str = "mongodb") -> None:
    # 记录最大池大小（若可用）
    try:
        max_pool = getattr(client.options.pool_options, "max_pool_size", None)
        if max_pool is not None:
            DB_POOL_SIZE.labels(db, ENV, INSTANCE).set(float(max_pool))
    except Exception:
        pass
