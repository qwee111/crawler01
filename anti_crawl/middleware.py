# -*- coding: utf-8 -*-
"""
反爬虫中间件

集成反爬虫检测和应对策略
"""

import logging
import random
import time
from typing import Dict, List, Optional

from scrapy import signals
from scrapy.exceptions import IgnoreRequest
from scrapy.http import Request

from .detector import AntiCrawlDetector, AntiCrawlStrategy

logger = logging.getLogger(__name__)


class AntiCrawlMiddleware:
    """反爬虫检测和应对中间件"""

    def __init__(
        self,
        enabled: bool = True,
        auto_retry: bool = True,
        max_retries: int = 3,
        retry_delay: int = 5,
    ):
        self.enabled = enabled
        self.auto_retry = auto_retry
        self.max_retries = max_retries
        self.retry_delay = retry_delay

        if self.enabled:
            self.detector = AntiCrawlDetector()
            self.strategy = AntiCrawlStrategy()

        # 统计信息
        self.stats = {
            "total_requests": 0,
            "detected_count": 0,
            "strategy_applied": 0,
            "retries": 0,
        }

        logger.info(f"反爬虫中间件初始化: enabled={enabled}, auto_retry={auto_retry}")

    @classmethod
    def from_crawler(cls, crawler):
        """从爬虫配置创建中间件"""
        settings = crawler.settings

        enabled = settings.getbool("ANTI_CRAWL_ENABLED", True)
        auto_retry = settings.getbool("ANTI_CRAWL_AUTO_RETRY", True)
        max_retries = settings.getint("ANTI_CRAWL_MAX_RETRIES", 3)
        retry_delay = settings.getint("ANTI_CRAWL_RETRY_DELAY", 5)

        middleware = cls(
            enabled=enabled,
            auto_retry=auto_retry,
            max_retries=max_retries,
            retry_delay=retry_delay,
        )

        # 连接信号
        crawler.signals.connect(middleware.spider_closed, signal=signals.spider_closed)

        return middleware

    def process_response(self, request, response, spider):
        """处理响应"""
        if not self.enabled:
            return response

        self.stats["total_requests"] += 1

        # 检测反爬虫机制
        detection_result = self.detector.detect(response, request)

        if detection_result["detected"]:
            self.stats["detected_count"] += 1

            logger.warning(f"检测到反爬虫机制: {detection_result['detected']} - {request.url}")

            # 应用应对策略
            if self.auto_retry:
                strategy_result = self.strategy.apply_strategy(
                    detection_result, request, spider
                )

                if strategy_result["success"]:
                    self.stats["strategy_applied"] += 1

                    # 创建重试请求
                    retry_request = self._create_retry_request(
                        request, detection_result, strategy_result
                    )

                    if retry_request:
                        self.stats["retries"] += 1
                        logger.info(f"创建重试请求: {retry_request.url}")
                        return retry_request

            # 记录检测结果到响应元数据
            response.meta["anti_crawl_detection"] = detection_result

        return response

    def _create_retry_request(
        self, original_request, detection_result, strategy_result
    ) -> Optional[Request]:
        """创建重试请求"""
        # 检查重试次数
        retry_count = original_request.meta.get("anti_crawl_retry_count", 0)
        if retry_count >= self.max_retries:
            logger.warning(f"达到最大重试次数: {original_request.url}")
            return None

        # 创建新请求
        new_request = original_request.copy()
        new_request.meta["anti_crawl_retry_count"] = retry_count + 1
        new_request.meta["anti_crawl_detection"] = detection_result
        new_request.meta["anti_crawl_strategy"] = strategy_result

        # 根据检测结果调整请求参数
        self._adjust_request_parameters(new_request, detection_result, strategy_result)

        # 添加延迟
        delay = self.retry_delay + random.uniform(1, 3)
        new_request.meta["download_delay"] = delay

        return new_request

    def _adjust_request_parameters(self, request, detection_result, strategy_result):
        """根据检测结果调整请求参数"""
        detected_types = detection_result["detected"]

        # 处理User-Agent检查
        if "user_agent_check" in detected_types:
            request.headers["User-Agent"] = self._get_random_user_agent()

        # 处理Referer检查
        if "referer_check" in detected_types:
            request.headers["Referer"] = self._get_referer(request.url)

        # 处理Cookie检查
        if "cookie_check" in detected_types:
            request.meta["cookiejar"] = "anti_crawl"

        # 处理JavaScript挑战
        if "js_challenge" in detected_types:
            request.meta["selenium"] = True

        # 处理验证码
        if "captcha" in detected_types:
            request.meta["selenium"] = True
            request.meta["captcha_required"] = True

        # 处理频率限制
        if "rate_limit" in detected_types:
            request.meta["download_delay"] = max(
                request.meta.get("download_delay", 0), 10 + random.uniform(5, 15)
            )

        # 处理IP封禁
        if "ip_block" in detected_types:
            request.meta["proxy"] = self._get_new_proxy()

    def _get_random_user_agent(self) -> str:
        """获取随机User-Agent"""
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        ]
        return random.choice(user_agents)

    def _get_referer(self, url: str) -> str:
        """获取合适的Referer"""
        from urllib.parse import urlparse

        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}/"

    def _get_new_proxy(self) -> Optional[str]:
        """获取新的代理"""
        # 这里应该集成代理池
        # 暂时返回None，表示不使用代理
        return None

    def spider_closed(self, spider):
        """爬虫关闭时输出统计信息"""
        if not self.enabled:
            return

        logger.info("反爬虫中间件统计信息:")
        logger.info(f"  总请求数: {self.stats['total_requests']}")
        logger.info(f"  检测到反爬虫: {self.stats['detected_count']}")
        logger.info(f"  应用策略: {self.stats['strategy_applied']}")
        logger.info(f"  重试次数: {self.stats['retries']}")

        if self.stats["total_requests"] > 0:
            detection_rate = (
                self.stats["detected_count"] / self.stats["total_requests"] * 100
            )
            logger.info(f"  检测率: {detection_rate:.2f}%")


class CaptchaMiddleware:
    """验证码处理中间件"""

    def __init__(self, captcha_service_url: str = None):
        self.captcha_service_url = captcha_service_url
        self.captcha_count = 0

    @classmethod
    def from_crawler(cls, crawler):
        captcha_service_url = crawler.settings.get("CAPTCHA_SERVICE_URL")
        return cls(captcha_service_url)

    def process_response(self, request, response, spider):
        """处理验证码响应"""
        if request.meta.get("captcha_required"):
            self.captcha_count += 1
            logger.info(f"遇到验证码 #{self.captcha_count}: {request.url}")

            # 这里可以集成验证码识别服务
            if self.captcha_service_url:
                # 调用验证码识别服务
                pass
            else:
                logger.warning("未配置验证码识别服务")

        return response


class BehaviorSimulationMiddleware:
    """行为模拟中间件"""

    def __init__(self, min_delay: float = 1.0, max_delay: float = 5.0):
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.last_request_time = 0

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        min_delay = settings.getfloat("BEHAVIOR_MIN_DELAY", 1.0)
        max_delay = settings.getfloat("BEHAVIOR_MAX_DELAY", 5.0)
        return cls(min_delay, max_delay)

    def process_request(self, request, spider):
        """模拟人类行为延迟"""
        current_time = time.time()

        if self.last_request_time > 0:
            elapsed = current_time - self.last_request_time
            min_interval = random.uniform(self.min_delay, self.max_delay)

            if elapsed < min_interval:
                sleep_time = min_interval - elapsed
                logger.debug(f"行为模拟延迟: {sleep_time:.2f}秒")
                time.sleep(sleep_time)

        self.last_request_time = time.time()
        return None


class HeaderRotationMiddleware:
    """请求头轮换中间件"""

    def __init__(self):
        self.header_sets = [
            {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Accept-Encoding": "gzip, deflate, br",
                "DNT": "1",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
            },
            {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate",
                "Connection": "keep-alive",
                "Cache-Control": "max-age=0",
            },
            {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
                "Accept-Encoding": "gzip, deflate, br",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
            },
        ]

    def process_request(self, request, spider):
        """轮换请求头"""
        headers = random.choice(self.header_sets)
        for key, value in headers.items():
            request.headers[key] = value

        return None
