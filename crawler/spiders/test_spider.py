# -*- coding: utf-8 -*-
"""
测试爬虫

用于验证系统基础功能
"""

import scrapy
from scrapy.http import Request


class TestSpider(scrapy.Spider):
    """简单的测试爬虫"""

    name = "test"
    allowed_domains = ["httpbin.org"]
    start_urls = ["http://httpbin.org/ip"]

    custom_settings = {
        "DOWNLOAD_DELAY": 1,
        "CONCURRENT_REQUESTS": 1,
        "DOWNLOADER_MIDDLEWARES": {},
        "ITEM_PIPELINES": {},
        "EXTENSIONS": {},
        "ROBOTSTXT_OBEY": False,
    }

    def parse(self, response):
        """解析响应"""
        self.logger.info(f"访问URL: {response.url}")
        self.logger.info(f"响应状态: {response.status}")
        self.logger.info(f"响应内容: {response.text}")

        # 返回一个简单的数据项
        yield {
            "url": response.url,
            "status": response.status,
            "content": response.text,
            "spider": self.name,
        }

        # 测试更多URL
        test_urls = ["http://httpbin.org/headers", "http://httpbin.org/user-agent"]

        for url in test_urls:
            yield Request(url, callback=self.parse_detail)

    def parse_detail(self, response):
        """解析详情页"""
        self.logger.info(f"详情页URL: {response.url}")
        self.logger.info(f"详情页状态: {response.status}")

        yield {
            "url": response.url,
            "status": response.status,
            "type": "detail",
            "spider": self.name,
        }
