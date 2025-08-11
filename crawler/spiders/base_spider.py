# -*- coding: utf-8 -*-
"""
基础爬虫

最简单的爬虫示例，用于学习和测试
"""

import scrapy
from scrapy.http import Request


class BaseSpider(scrapy.Spider):
    """基础爬虫类"""

    name = "base"
    allowed_domains = ["httpbin.org", "example.com"]

    # 测试用的起始URL
    start_urls = [
        "http://httpbin.org/ip",  # 获取IP信息
        "http://httpbin.org/headers",  # 获取请求头信息
        "http://httpbin.org/user-agent",  # 获取User-Agent信息
    ]

    custom_settings = {
        "DOWNLOAD_DELAY": 1,
        "CONCURRENT_REQUESTS": 1,
        "ROBOTSTXT_OBEY": False,
        "USER_AGENT": "BaseSpider/1.0 (+http://www.example.com/bot)",
        "LOG_LEVEL": "INFO",
    }

    def start_requests(self):
        """生成初始请求"""
        self.logger.info("🚀 开始基础爬虫...")

        for i, url in enumerate(self.start_urls):
            self.logger.info(f"📋 准备请求第{i+1}个URL: {url}")
            yield Request(
                url=url,
                callback=self.parse,
                meta={"index": i + 1, "url_type": self.get_url_type(url)},
            )

    def get_url_type(self, url):
        """根据URL判断类型"""
        if "ip" in url:
            return "ip_info"
        elif "headers" in url:
            return "headers_info"
        elif "user-agent" in url:
            return "user_agent_info"
        else:
            return "unknown"

    def parse(self, response):
        """解析响应"""
        index = response.meta.get("index", 0)
        url_type = response.meta.get("url_type", "unknown")

        self.logger.info(f"✅ 第{index}个请求成功")
        self.logger.info(f"📊 URL: {response.url}")
        self.logger.info(f"📊 状态码: {response.status}")
        self.logger.info(f"📊 类型: {url_type}")
        self.logger.info(f"📊 响应大小: {len(response.body)} bytes")

        # 尝试解析JSON响应
        try:
            import json

            data = json.loads(response.text)
            self.logger.info(f"📄 JSON数据: {data}")
        except:
            self.logger.info(f"📄 文本内容: {response.text[:200]}...")

        # 返回结构化数据
        yield {
            "index": index,
            "url": response.url,
            "status": response.status,
            "url_type": url_type,
            "content_length": len(response.body),
            "content": response.text[:500] + "..."
            if len(response.text) > 500
            else response.text,
            "timestamp": self.get_timestamp(),
            "spider_name": self.name,
        }

        # 如果是第一个请求，生成额外的测试请求
        if index == 1:
            self.logger.info("🔄 生成额外的测试请求...")
            extra_urls = [
                "http://httpbin.org/json",
                "http://httpbin.org/html",
            ]

            for extra_url in extra_urls:
                yield Request(
                    url=extra_url,
                    callback=self.parse_extra,
                    meta={"source": "extra_request"},
                )

    def parse_extra(self, response):
        """解析额外请求的响应"""
        self.logger.info(f"🔍 额外请求完成: {response.url}")
        self.logger.info(f"📊 状态码: {response.status}")

        yield {
            "type": "extra_request",
            "url": response.url,
            "status": response.status,
            "content_type": response.headers.get("Content-Type", b"").decode(),
            "content_preview": response.text[:200] + "..."
            if len(response.text) > 200
            else response.text,
            "timestamp": self.get_timestamp(),
            "spider_name": self.name,
        }

    def get_timestamp(self):
        """获取当前时间戳"""
        import datetime

        return datetime.datetime.now().isoformat()

    def closed(self, reason):
        """爬虫关闭时的处理"""
        self.logger.info(f"🏁 基础爬虫完成，关闭原因: {reason}")

        # 输出统计信息
        stats = self.crawler.stats.get_stats()
        self.logger.info("📈 爬虫统计信息:")
        for key, value in stats.items():
            if isinstance(value, (int, float, str)):
                self.logger.info(f"   {key}: {value}")


# 为了兼容性，也创建一个别名
class BaseCrawlerSpider(BaseSpider):
    """基础爬虫别名"""

    name = "base_crawler"
