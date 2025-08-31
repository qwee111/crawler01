# -*- coding: utf-8 -*-
"""
Scrapy 扩展与中间件埋点：
- 请求/响应/异常 监控
- Item 提取/入库/丢弃 监控
- 队列长度与积压
"""
from __future__ import annotations

import time
from typing import Optional

from scrapy import signals
from scrapy.exceptions import NotConfigured

from .metrics import (
    DUP_CONTENT,
    ENV,
    INSTANCE,
    ITEM_DROPPED,
    ITEM_EXTRACTED,
    ITEM_STORED,
    QUEUE_BACKLOG_SECONDS,
    QUEUE_LENGTH,
    REQUEST_ERROR,
    REQUESTS_IN_FLIGHT,
    REQUEST_LATENCY,
    REQUEST_TOTAL,
    labels_site,
)


class MetricsExtension:
    def __init__(self, crawler):
        settings = crawler.settings
        if not settings.getbool("METRICS_ENABLED", True):
            raise NotConfigured
        self.crawler = crawler
        self.server = None
        # 站点名获取函数
        self._get_site = lambda r: (r.meta.get("site") if hasattr(r, "meta") else None)

    @classmethod
    def from_crawler(cls, crawler):
        ext = cls(crawler)
        crawler.signals.connect(ext.request_scheduled, signal=signals.request_scheduled)
        crawler.signals.connect(ext.request_dropped, signal=signals.request_dropped)
        crawler.signals.connect(ext.response_received, signal=signals.response_received)
        crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        crawler.signals.connect(ext.item_scraped, signal=signals.item_scraped)
        crawler.signals.connect(ext.item_dropped, signal=signals.item_dropped)
        crawler.signals.connect(ext.spider_error, signal=signals.spider_error)
        return ext

    # ---------- signals ----------
    def spider_opened(self, spider):
        # 可在此获取 redis server（若启用 scrapy-redis）
        try:
            from scrapy_redis import connection

            self.server = connection.get_redis_from_settings(self.crawler.settings)
        except Exception:
            self.server = None

    def spider_closed(self, spider):
        pass

    def request_scheduled(self, request, spider):
        site = self._get_site(request)
        REQUESTS_IN_FLIGHT.labels(**labels_site(spider.name, site)).inc()
        REQUEST_TOTAL.labels(**labels_site(spider.name, site), status="scheduled").inc()

    def request_dropped(self, request, spider):
        site = self._get_site(request)
        REQUESTS_IN_FLIGHT.labels(**labels_site(spider.name, site)).dec()
        REQUEST_TOTAL.labels(**labels_site(spider.name, site), status="dropped").inc()

    def response_received(self, response, request, spider):
        site = self._get_site(request)
        REQUESTS_IN_FLIGHT.labels(**labels_site(spider.name, site)).dec()
        REQUEST_TOTAL.labels(**labels_site(spider.name, site), status="success").inc()
        REQUEST_LATENCY.labels(**labels_site(spider.name, site)).observe(
            response.meta.get("download_latency", 0)
        )

    def spider_error(self, failure, response, spider):
        req = response.request if response else None
        site = self._get_site(req) if req else None
        REQUEST_TOTAL.labels(**labels_site(spider.name, site), status="fail").inc()
        REQUEST_ERROR.labels(
            **labels_site(spider.name, site), error_type=failure.type.__name__
        ).inc()

    def item_scraped(self, item, response, spider):
        site = response.meta.get("site") if response else None
        ITEM_EXTRACTED.labels(**labels_site(spider.name, site)).inc()

    def item_dropped(self, item, response, exception, spider):
        site = response.meta.get("site") if response else None
        ITEM_DROPPED.labels(
            **labels_site(spider.name, site), reason=exception.__class__.__name__
        ).inc()

    # ---------- queue ----------
    def tick_queue_metrics(self, spider):
        if not self.server:
            return
        name = getattr(spider, "name", "unknown")
        site = getattr(spider, "target_site", None)
        queue_key = getattr(spider, "redis_key", None) or f"{name}:start_urls"
        try:
            llen = self.server.llen(queue_key)
            QUEUE_LENGTH.labels(name, site or "default", queue_key, ENV, INSTANCE).set(llen)
            # 若存在 ZSET 刷新队列，则利用 score 作为“下次刷新时间”，估算积压时间
            refresh_key = f"refresh_queue:{site or 'default'}"
            try:
                # 取最早一条的 score（下一次刷新时间戳），若早于当前则计算滞后
                import time as _t

                res = self.server.zrange(refresh_key, 0, 0, withscores=True)
                if res:
                    _, next_ts = res[0]
                    backlog = max(0.0, float(next_ts) - _t.time())
                else:
                    backlog = 0.0
            except Exception:
                # 退化为按队列长度估算
                backlog = max(0, llen) * 0.1
            QUEUE_BACKLOG_SECONDS.labels(name, site or "default", queue_key, ENV, INSTANCE).set(
                backlog
            )
        except Exception:
            pass

