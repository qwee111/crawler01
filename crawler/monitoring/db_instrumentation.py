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
    try:
        from sqlalchemy import event
    except Exception:
        return

    # 记录池大小（若可用）
    try:
        size = getattr(engine.pool, "size", None)
        if callable(size):
            DB_POOL_SIZE.labels(db, ENV, INSTANCE).set(size())
    except Exception:
        pass

    def _on_checkout(dbapi_con, con_record, con_proxy):  # type: ignore[no-redef]
        try:
            DB_POOL_INUSE.labels(db, ENV, INSTANCE).inc()
        except Exception:
            pass

    def _on_checkin(dbapi_con, con_record):  # type: ignore[no-redef]
        try:
            DB_POOL_INUSE.labels(db, ENV, INSTANCE).dec()
        except Exception:
            pass

    try:
        event.listen(engine, "checkout", _on_checkout)
        event.listen(engine, "checkin", _on_checkin)
    except Exception:
        pass


def instrument_mongo_client(client: Any, db: str = "mongodb") -> None:
    # 记录最大池大小（若可用）
    try:
        max_pool = getattr(client.options.pool_options, "max_pool_size", None)
        if max_pool is not None:
            DB_POOL_SIZE.labels(db, ENV, INSTANCE).set(float(max_pool))
    except Exception:
        pass

