"""
重构后的自适应爬虫

使用模块化架构，代码简洁清晰
"""

import json
import logging
from typing import Any, Dict, Optional
from urllib.parse import urljoin

import scrapy
from scrapy_redis.spiders import RedisSpider

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

        delays = request_config.get("delays", {})
        if delays:
            download_delay = delays.get("download_delay", 2.0)
            self.custom_settings = self.custom_settings or {}
            self.custom_settings["DOWNLOAD_DELAY"] = download_delay
            if delays.get("randomize_delay", True):
                self.custom_settings["RANDOMIZE_DOWNLOAD_DELAY"] = True

    def start_requests(self):
        """生成起始请求"""
        # 检查是否有配置的起始URL
        if hasattr(self, "start_urls") and self.start_urls:
            for url in self.start_urls:
                logger.info(f"📋 准备请求: {url}")
                yield scrapy.Request(
                    url=url,
                    callback=self.parse,
                    meta={"site_name": self.target_site},
                    errback=self.handle_error,
                )
        else:
            logger.error("❌ 没有配置起始URL")
            logger.info("💡 请检查以下配置:")
            logger.info(f"   1. 网站配置文件: config/sites/{self.target_site}.yaml")
            logger.info(f"   2. start_urls 配置部分")
            logger.info(f"   3. 当前target_site: {self.target_site}")

            # 尝试从配置文件获取起始URL
            if self.site_config and "start_urls" in self.site_config:
                start_urls_config = self.site_config["start_urls"]
                logger.info(f"🔧 从配置文件获取到 {len(start_urls_config)} 个起始URL")

                for url_config in start_urls_config:
                    url = url_config.get("url")
                    if url:
                        logger.info(f"📋 准备请求: {url}")
                        yield scrapy.Request(
                            url=url,
                            callback=self.parse,
                            meta={
                                "site_name": self.target_site,
                                "url_config": url_config,
                            },
                            errback=self.handle_error,
                        )
            # 2) 始终监听 Redis 队列（scrapy-redis 默认行为）
        try:
            for req in super().start_requests():
                yield req
        except Exception as e:
            logger.warning(f"⚠️ Redis 队列不可用或未配置: {e}")

    def parse(self, response):
        """解析页面的主入口"""
        try:
            logger.info(f"✅ 开始解析页面: {response.url}")
            logger.info(f"📊 状态码: {response.status}")
            logger.info(f"📏 响应大小: {len(response.body)} 字节")

            # 检测网站
            site_name = self._detect_site(response)
            if not site_name:
                logger.warning(f"⚠️ 无法识别网站: {response.url}")
                return

            # 分析页面
            page_analysis = self.page_analyzer.analyze_page(response, site_name)
            logger.info(f"🔍 页面类型: {page_analysis['page_type']}")

            # 提取数据
            extracted_data = self.extraction_engine.extract_data(
                response, site_name, page_analysis
            )

            # 添加元数据
            extracted_data.update(
                {
                    "spider_name": self.name,
                    "spider_version": "2.0",
                    "site_name": site_name,
                    "page_analysis": page_analysis,
                    "response_meta": {
                        "status_code": response.status,
                        "content_type": response.headers.get(
                            "Content-Type", b""
                        ).decode("utf-8", errors="ignore"),
                        "content_length": len(response.body),
                        "url": response.url,
                    },
                }
            )

            # 输出数据
            yield extracted_data

            # 处理链接跟进（如果是列表页）
            if page_analysis["page_type"] == "list_page":
                yield from self._follow_links(response, site_name, extracted_data)

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
