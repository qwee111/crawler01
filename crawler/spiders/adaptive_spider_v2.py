"""
重构后的自适应爬虫

使用模块化架构，代码简洁清晰
"""

import json
import logging
import time
import asyncio
import hashlib
from typing import Any, Dict, Optional
from urllib.parse import urljoin

import scrapy
from scrapy_redis.spiders import RedisSpider
from scrapy_redis import connection

# 导入核心模块
from crawler.core import ConfigManager, ExtractionEngine, PageAnalyzer, SiteDetector

logger = logging.getLogger(__name__)


class AdaptiveSpiderV2(RedisSpider):
    """重构后的自适应爬虫 (RedisSpider 版本，支持 Redis 动态种子)"""

    name = "adaptive_v2"
    # 默认的全局队列键；若指定 -a site=xxx，将在 __init__ 中切换为按站点分桶的键
    redis_key = "adaptive_v2:start_urls"

    def __init__(
        self,
        target_site: str = None,
        site: str = None,
        redis_key: str = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        # Redis 队列键动态设置：优先使用传入的 redis_key；否则按站点分桶
        if redis_key:
            self.redis_key = redis_key
        elif site:
            self.redis_key = f"adaptive_v2:{site}:start_urls"

        # 初始化核心组件
        self.config_manager = ConfigManager()
        self.site_detector = SiteDetector(self.config_manager)
        self.page_analyzer = PageAnalyzer(self.config_manager)
        self.extraction_engine = ExtractionEngine(self.config_manager)

        # 兼容命令行参数：支持 -a target_site=xxx 和 -a site=xxx
        passed_site = kwargs.pop("site", None)
        passed_target_site = kwargs.pop("target_site", None)
        self.target_site = target_site or site or passed_site or passed_target_site
        self.site_config = None

        if self.target_site:
            self._load_site_config()
        else:
            logger.warning("⚠️ 未提供目标网站参数。请使用 -a target_site=<site> 或 -a site=<site>")
            logger.info(f"💡 可用的网站配置: {self.config_manager.list_sites()}")

        logger.info(f"🚀 自适应爬虫V2启动: 目标网站={self.target_site}")

    def _load_site_config(self):
        """加载网站配置"""
        self.site_config = self.config_manager.get_config_by_site(self.target_site)

        if not self.site_config:
            logger.error(f"❌ 未找到网站配置: {self.target_site}")
            logger.info(f"💡 可用的网站配置: {self.config_manager.list_sites()}")
            return

        logger.info(f"✅ 找到网站配置: {self.target_site}")
        logger.info(f"📊 配置部分: {list(self.site_config.keys())}")

        # 设置起始URL
        start_urls_config = self.site_config.get("start_urls", [])
        if start_urls_config:
            self.start_urls = [
                item["url"] for item in start_urls_config if "url" in item
            ]
            logger.info(f"📋 加载起始URL: {len(self.start_urls)} 个")
            for i, url in enumerate(self.start_urls, 1):
                logger.info(f"   {i}. {url}")
        else:
            logger.warning(f"⚠️ 配置文件中没有start_urls部分")
            self.start_urls = []

        # 设置请求配置
        request_config = self.site_config.get("request", {})
        if request_config:
            self._apply_request_config(request_config)

        logger.info(f"✅ 网站配置加载完成: {self.target_site}")

    def _apply_request_config(self, request_config: Dict):
        """应用请求配置"""
        # 设置请求头
        headers = request_config.get("headers", {})
        if headers:
            self.custom_settings = self.custom_settings or {}
            self.custom_settings["DEFAULT_REQUEST_HEADERS"] = headers

        # 设置延迟
        delays = request_config.get("delays", {})
        if delays:
            download_delay = delays.get("download_delay", 2.0)
            self.custom_settings = self.custom_settings or {}
            self.custom_settings["DOWNLOAD_DELAY"] = download_delay
            if delays.get("randomize_delay", True):
                self.custom_settings["RANDOMIZE_DOWNLOAD_DELAY"] = True

    def make_request_from_data(self, data: bytes):
        """从 Redis 的种子数据创建 Request，兼容 JSON 或 纯字符串 URL"""
        text = data.decode("utf-8").strip()
        try:
            payload = json.loads(text)
            url = payload.get("url") or payload.get("u")
            if not url:
                raise ValueError("seed json missing url")
            meta = payload.get("meta", {}) or {}
            headers = payload.get("headers")
            cb_name = payload.get("callback")
            cb_fn = getattr(self, cb_name, None) if cb_name else self.parse
            req = scrapy.Request(
                url, callback=cb_fn, headers=headers, meta=meta, dont_filter=False
            )
            # 透传 site
            if self.target_site and "site" not in req.meta:
                req.meta["site"] = self.target_site
            elif "site" in payload:
                req.meta.setdefault("site", payload["site"])
            return req
        except Exception:
            # 兼容纯字符串 URL
            req = scrapy.Request(text, callback=self.parse, dont_filter=False)
            if self.target_site and "site" not in req.meta:
                req.meta["site"] = self.target_site
            return req

    async def start(self):
        """生成起始请求（Scrapy 2.13+）：
        1) 先发本地配置/站点配置的起始URL（列表页，强制刷新）
        2) 启动基于Redis的列表周期刷新（ZSET）
        3) 监听Redis队列消费动态种子（兼容RedisSpider）
        """
        # 初始化Redis连接（用于刷新/增量识别）
        try:
            self.server = connection.get_redis_from_settings(self.crawler.settings)
        except Exception as e:
            self.server = None
            logger.warning(f"⚠️ 无法连接Redis，将以降级模式运行: {e}")

        # 1) 先发本地配置的起始URL（作为列表页强制刷新请求）
        start_urls = list(getattr(self, "start_urls", []) or [])
        if not start_urls and self.site_config and "start_urls" in self.site_config:
            start_urls = [u.get("url") for u in self.site_config.get("start_urls", []) if u.get("url")]
        for url in start_urls:
            logger.info(f"📋 列表页初始刷新: {url}")
            yield scrapy.Request(url=url, callback=self.parse, dont_filter=True, meta={"page_type":"list_page","site_name": self.target_site}, errback=self.handle_error)

        # 2) 列表页周期刷新 - 仅在可用时启用
        if self.server and self.settings.getbool("LIST_REFRESH_ENABLED", True):
            site = self.target_site or "default"
            refresh_key = f"refresh_queue:{site}"
            # 将起始URL登记进刷新队列（下次刷新时间 = 现在 + interval）
            interval = int(self.settings.getint("LIST_REFRESH_INTERVAL", 900))
            for url in start_urls:
                mid = hashlib.sha1(url.encode("utf-8")).hexdigest()
                try:
                    self.server.set(f"list_url:{mid}", url)
                    self.server.zadd(refresh_key, {mid: time.time() + interval})
                except Exception as e:
                    logger.warning(f"⚠️ 列表刷新登记失败: {e}")

            async def refresh_loop():
                logger.info("🔁 启动列表页刷新协程")
                while True:
                    try:
                        member_score = self.server.zpopmin(refresh_key)
                        if not member_score:
                            await asyncio.sleep(5)
                            continue
                        member = member_score[0][0]
                        # Redis返回bytes，需要解码
                        if isinstance(member, bytes):
                            member = member.decode("utf-8", errors="ignore")
                        url = self.server.get(f"list_url:{member}")
                        if url:
                            if isinstance(url, bytes):
                                url = url.decode("utf-8", errors="ignore")
                            logger.info(f"⏰ 周期刷新列表: {url}")
                            yield scrapy.Request(url=url, callback=self.parse, dont_filter=True, meta={"page_type":"list_page","site_name": self.target_site}, errback=self.handle_error)
                            # 重新安排下一次刷新
                            try:
                                self.server.zadd(refresh_key, {member: time.time() + interval})
                            except Exception as e:
                                logger.warning(f"⚠️ 刷新队列重入失败: {e}")
                        else:
                            logger.debug(f"🔎 未找到列表URL映射: list_url:{member}")
                    except Exception as e:
                        logger.warning(f"⚠️ 列表刷新循环异常: {e}")
                        await asyncio.sleep(5)
            # 将刷新生成器并入 Scrapy 的异步 start 流
            async for req in refresh_loop():
                yield req

        # 3) 监听 Redis 队列（继承自 RedisSpider / Spider 的 start 实现）
        try:
            async for req in super().start():
                yield req
        except Exception as e:
            logger.warning(f"⚠️ Redis 队列不可用或未配置: {e}")

    def parse(self, response):
        """解析页面的主入口（支持列表页增量与详情页内容指纹）"""
        try:
            logger.info(f"✅ 开始解析页面: {response.url}")

            # 检测网站
            site_name = self._detect_site(response)
            if not site_name:
                logger.warning(f"⚠️ 无法识别网站: {response.url}")
                return

            # 分析页面
            page_analysis = self.page_analyzer.analyze_page(response, site_name)
            page_type = page_analysis.get("page_type")
            logger.info(f"🔍 页面类型: {page_type}")

            # 提取数据
            extracted = self.extraction_engine.extract_data(response, site_name, page_analysis)

            # 列表页：只做增量识别与派发
            if page_type == "list_page":
                items = extracted.get("items", []) if isinstance(extracted, dict) else []
                logger.info(f"🧮 列表项数量: {len(items)}")
                yield from self._handle_list_incremental(response, site_name, items)
                return

            # 详情页：输出数据项，由 ContentUpdatePipeline 负责“内容指纹去重”
            if isinstance(extracted, dict):
                extracted.update(
                    {
                        "spider_name": self.name,
                        "spider_version": "2.0",
                        "site_name": site_name,
                        "page_analysis": page_analysis,
                        "response_meta": {
                            "status_code": response.status,
                            "content_type": response.headers.get("Content-Type", b"").decode(
                                "utf-8", errors="ignore"
                            ),
                            "content_length": len(response.body),
                            "url": response.url,
                        },
                    }
                )
                yield extracted

            logger.info(f"✅ 页面解析完成: {response.url}")

        except Exception as e:
            logger.error(f"❌ 页面解析失败: {response.url}, 错误: {e}")
            yield {
                "url": response.url,
                "error": str(e),
                "spider_name": self.name,
                "status": "parse_failed",
            }

    def _detect_site(self, response) -> Optional[str]:
        """检测网站"""
        # 优先使用配置的网站名
        if self.target_site:
            return self.target_site

        # 自动检测
        return self.site_detector.detect_site(response.url)

    def _schedule_next_refresh(self, list_url: str, interval: int):
        if not self.server:
            return
        try:
            site = self.target_site or "default"
            refresh_key = f"refresh_queue:{site}"
            mid = hashlib.sha1(list_url.encode("utf-8")).hexdigest()
            self.server.set(f"list_url:{mid}", list_url)
            self.server.zadd(refresh_key, {mid: time.time() + interval})
        except Exception as e:
            logger.warning(f"⚠️ 列表刷新登记失败: {e}")

    def _handle_list_incremental(self, response, site_name: str, items: list):
        """增量识别列表中的文章链接并发起请求"""
        interval = int(
            (self.site_config.get("update_detection", {}) or {}).get(
                "list_refresh_interval", self.settings.getint("LIST_REFRESH_INTERVAL", 900)
            )
        )
        self._schedule_next_refresh(response.url, interval)

        links = []
        for item in items:
            url = item.get("url") if isinstance(item, dict) else None
            if not url:
                continue
            absolute_url = urljoin(response.url, url)
            links.append(absolute_url)

        if not links:
            return

        # Redis 增量：只抓新链接
        seen_key = f"seen_articles:{site_name or 'default'}"
        for link in links:
            if not self.server:
                # 降级：不使用增量过滤
                yield scrapy.Request(
                    url=link,
                    callback=self.parse,
                    meta={"site_name": site_name, "page_type": "detail_page"},
                    errback=self.handle_error,
                )
                continue
            try:
                uhash = hashlib.sha1(link.encode("utf-8")).hexdigest()
                if not self.server.sismember(seen_key, uhash):
                    self.server.sadd(seen_key, uhash)
                    yield scrapy.Request(
                        url=link,
                        callback=self.parse,
                        meta={"site_name": site_name, "page_type": "detail_page"},
                        errback=self.handle_error,
                    )
            except Exception as e:
                logger.warning(f"⚠️ Redis 增量识别失败，降级直抓: {e}")
                yield scrapy.Request(
                    url=link,
                    callback=self.parse,
                    meta={"site_name": site_name, "page_type": "detail_page"},
                    errback=self.handle_error,
                )

        return self.site_detector.detect_site(response.url)

    def _follow_links(self, response, site_name: str, extracted_data: Dict):
        """跟进链接"""
        try:
            # 获取提取的链接
            links = []

            # 从提取的数据中获取链接
            if "items" in extracted_data:
                for item in extracted_data["items"]:
                    if "url" in item and item["url"]:
                        links.append(item["url"])

            # 限制链接数量
            max_links = 10  # 可配置
            links = links[:max_links]

            logger.info(f"🔗 准备跟进 {len(links)} 个链接")

            for link in links:
                absolute_url = urljoin(response.url, link)
                yield scrapy.Request(
                    url=absolute_url,
                    callback=self.parse,
                    meta={"site_name": site_name, "page_type": "detail_page"},
                    errback=self.handle_error,
                )

        except Exception as e:
            logger.error(f"❌ 链接跟进失败: {e}")

    def handle_error(self, failure):
        """处理请求错误"""
        logger.error(f"❌ 请求失败: {failure.request.url}")
        logger.error(f"❌ 错误详情: {failure.value}")

        yield {
            "url": failure.request.url,
            "error": str(failure.value),
            "spider_name": self.name,
            "status": "request_failed",
        }

    def closed(self, reason):
        """爬虫关闭时的清理工作"""
        logger.info(f"🏁 自适应爬虫V2关闭")
        logger.info(f"📊 关闭原因: {reason}")

        # 输出统计信息
        stats = self.crawler.stats.get_stats()
        logger.info("📈 爬虫统计信息:")
        for key, value in stats.items():
            if "count" in key.lower():
                logger.info(f"    {key}: {value}")
