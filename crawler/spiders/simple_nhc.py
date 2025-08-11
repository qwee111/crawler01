# -*- coding: utf-8 -*-
"""
简化版国家卫健委爬虫

用于测试基础功能
"""

import scrapy
from scrapy.http import Request, Response


class SimpleNhcSpider(scrapy.Spider):
    """简化版国家卫健委爬虫"""

    name = "simple_nhc"
    allowed_domains = ["nhc.gov.cn"]

    # 起始URL
    start_urls = [
        "http://www.nhc.gov.cn/xcs/yqtb/list_gzbd.shtml",  # 疫情通报
    ]

    custom_settings = {
        "DOWNLOAD_DELAY": 2,
        "CONCURRENT_REQUESTS": 1,
        "ROBOTSTXT_OBEY": False,
        "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    }

    def parse(self, response):
        """解析列表页"""
        self.logger.info(f"正在解析: {response.url}")
        self.logger.info(f"响应状态: {response.status}")

        # 提取页面标题
        title = response.css("title::text").get()
        if title:
            self.logger.info(f"页面标题: {title.strip()}")

        # 查找新闻链接
        news_links = response.css('a[href*="shtml"]::attr(href)').getall()
        self.logger.info(f"找到 {len(news_links)} 个链接")

        # 处理前5个链接作为测试
        for i, link in enumerate(news_links[:5]):
            if link:
                # 构建完整URL
                full_url = response.urljoin(link)
                self.logger.info(f"准备访问第{i+1}个链接: {full_url}")

                yield Request(
                    url=full_url, callback=self.parse_detail, meta={"index": i + 1}
                )

        # 返回列表页信息
        yield {
            "type": "list_page",
            "url": response.url,
            "title": title.strip() if title else "",
            "links_count": len(news_links),
            "status": response.status,
        }

    def parse_detail(self, response):
        """解析详情页"""
        index = response.meta.get("index", 0)
        self.logger.info(f"正在解析第{index}个详情页: {response.url}")

        # 提取标题
        title = response.css("title::text").get()
        if not title:
            title = response.css("h1::text, h2::text, h3::text").get()

        # 提取正文内容
        content_selectors = [
            ".content p::text",
            ".article-content p::text",
            ".main-content p::text",
            "p::text",
        ]

        content = ""
        for selector in content_selectors:
            texts = response.css(selector).getall()
            if texts:
                content = " ".join(text.strip() for text in texts if text.strip())
                break

        # 提取发布时间
        date_selectors = [
            ".time::text",
            ".date::text",
            ".publish-time::text",
            '*[class*="time"]::text',
            '*[class*="date"]::text',
        ]

        publish_date = ""
        for selector in date_selectors:
            date_text = response.css(selector).get()
            if date_text:
                publish_date = date_text.strip()
                break

        self.logger.info(f"详情页标题: {title[:50] if title else '无标题'}...")
        self.logger.info(f"内容长度: {len(content)}")
        self.logger.info(f"发布时间: {publish_date}")

        # 返回数据
        yield {
            "type": "detail_page",
            "url": response.url,
            "title": title.strip() if title else "",
            "content": content[:500] + "..." if len(content) > 500 else content,
            "publish_date": publish_date,
            "content_length": len(content),
            "status": response.status,
            "index": index,
        }

    def parse_error(self, failure):
        """处理错误"""
        self.logger.error(f"请求失败: {failure.request.url}")
        self.logger.error(f"错误信息: {failure.value}")

        yield {
            "type": "error",
            "url": failure.request.url,
            "error": str(failure.value),
            "status": "failed",
        }
