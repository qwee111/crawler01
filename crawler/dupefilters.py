# -*- coding: utf-8 -*-
"""
站点感知的 Redis 去重器

在 scrapy-redis 的 RFPDupeFilter 基础上扩展，按 spider + site 维度共享去重集合。
兼容原有 scrapy-redis 配置；当 spider 上不存在 target_site 时退化为按 spider 共享。

环境变量/配置项：
- SITE_AWARE_DUPEFILTER_KEY_FMT: 键名格式，默认 "dupefilter:%(spider)s:%(site)s"
- REDIS_URL / REDIS_PARAMS: 复用 scrapy-redis 的连接配置
"""
from __future__ import annotations

import logging

from scrapy.settings import Settings
from scrapy_redis import connection
from scrapy_redis.dupefilter import RFPDupeFilter

logger = logging.getLogger(__name__)


class SiteAwareRFPDupeFilter(RFPDupeFilter):
    """按站点分组的请求去重器。

    - key 形如: dupefilter:<spider>:<site>
    - 如果无法获得站点，退化为 dupefilter:<spider>
    """

    def __init__(self, server, key: str, debug: bool = False):
        super().__init__(server=server, key=key, debug=debug)
        logger.info(f"SiteAwareRFPDupeFilter initialized, key={key}")

    @classmethod
    def from_crawler(cls, crawler):
        settings: Settings = crawler.settings
        server = connection.get_redis_from_settings(settings)

        # 读取 spider 与站点
        spider = crawler.spider
        spider_name = getattr(spider, "name", "spider")
        site = getattr(spider, "target_site", None) or getattr(spider, "site", None)

        # 键名格式
        fmt = settings.get(
            "SITE_AWARE_DUPEFILTER_KEY_FMT", "dupefilter:%(spider)s:%(site)s"
        )
        if site:
            key = fmt % {"spider": spider_name, "site": site}
        else:
            # 退化：按 spider 维度共享
            key = f"dupefilter:{spider_name}"
        debug = settings.getbool("DUPEFILTER_DEBUG")
        return cls(server=server, key=key, debug=debug)
