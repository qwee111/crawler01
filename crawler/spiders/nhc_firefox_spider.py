#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
国家卫健委Firefox爬虫 - Scrapy版本
使用Selenium Firefox + 显式等待 + 反爬虫策略
完全按照example.py的策略，整合到Scrapy框架中
"""

import datetime
import json
import os
import re
import time

import scrapy
import yaml
from scrapy import Request
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# 导入Items
from crawler.items import EpidemicDataItem, NewsItem


class NHCFirefoxSpider(scrapy.Spider):
    """国家卫健委Firefox爬虫 - Scrapy版本"""

    name = "nhc_firefox"
    allowed_domains = ["nhc.gov.cn", "www.nhc.gov.cn", "www.gov.cn"]

    # 自定义设置
    custom_settings = {
        "DOWNLOAD_DELAY": 1,
        "CONCURRENT_REQUESTS": 1,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
        # 处理HTTP错误状态码
        "HTTPERROR_ALLOWED_CODES": [412],  # 允许412状态码通过
        # 启用数据处理管道
        "ITEM_PIPELINES": {
            "crawler.pipelines.ValidationPipeline": 100,
            "crawler.pipelines.CleaningPipeline": 200,
            "crawler.pipelines.DuplicatesPipeline": 300,
            "crawler.pipelines.MongoPipeline": 400,
            "crawler.pipelines.PostgresPipeline": 450,  # 添加PostgreSQL Pipeline
            "crawler.pipelines.JsonWriterPipeline": 500,
        },
        # 禁用一些不需要的中间件
        "DOWNLOADER_MIDDLEWARES": {
            "scrapy.downloadermiddlewares.useragent.UserAgentMiddleware": None,
        },
    }

    def __init__(self, config_path="config/nhc_firefox_config.yaml", *args, **kwargs):
        super(NHCFirefoxSpider, self).__init__(*args, **kwargs)

        # 加载配置
        self.config = self.load_config(config_path)
        self.driver = None
        self.stats = {
            "total_processed": 0,
            "successful_extractions": 0,
            "failed_extractions": 0,
            "start_time": time.time(),
        }

        # 确保目录存在
        for directory in self.config["file_storage"]["directories"].values():
            os.makedirs(directory, exist_ok=True)

        self.logger.info("国家卫健委Firefox爬虫初始化完成")

    def load_config(self, config_path):
        """加载配置文件"""
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
            self.logger.info(f"配置文件加载成功: {config_path}")
            return config
        except Exception as e:
            self.logger.error(f"配置文件加载失败: {e}")
            return self.get_default_config()

    def get_default_config(self):
        """获取默认配置"""
        return {
            "site_info": {"base_url": "http://www.nhc.gov.cn"},
            "target_pages": [
                {"url": "https://www.nhc.gov.cn/wjw/yqbb/list.shtml", "name": "疫情播报"}
            ],
            "browser_config": {
                "headless": True,
                "disable_gpu": True,
                "timeouts": {"explicit_wait": 150},
            },
            "file_storage": {
                "directories": {"texts": "texts", "data": "data", "debug": "debug"}
            },
            "crawling_strategy": {
                "delays": {"after_click": 0.1, "between_requests": 0.5},
                "limits": {"max_pages": 10, "max_items_per_page": 20},
            },
            "selectors": {
                "news_list": [
                    {"xpath": "//ul[@class='zxxx_list']/li"},
                    {"css": "ul.zxxx_list li"},
                    {"css": ".news-list li"},
                ],
                "content_selectors": [
                    {"xpath": "//*[@id='xw_box']"},
                    {"css": ".content"},
                    {"css": ".article-content"},
                ],
            },
        }

    def start_requests(self):
        """生成初始请求"""
        # 检查是否强制使用Selenium
        force_selenium = self.config["browser_config"].get("force_selenium", False)
        skip_scrapy_requests = self.config["browser_config"].get(
            "skip_scrapy_requests", False
        )

        if force_selenium and skip_scrapy_requests:
            # 直接使用Selenium，不发送Scrapy请求
            self.logger.info("配置为强制使用Selenium模式，跳过Scrapy HTTP请求")
            yield from self.parse_all_pages_with_selenium()
            return
        else:
            # 传统模式：先发送Scrapy请求，遇到错误再用Selenium
            for page_config in self.config["target_pages"]:
                yield Request(
                    url=page_config["url"],
                    callback=self.parse_list_page,
                    meta={
                        "page_name": page_config["name"],
                        "page_type": page_config.get("type", "list_page"),
                        "use_selenium": True,
                    },
                )

    def setup_firefox_driver(self):
        """设置Firefox浏览器 - 按照example.py的反爬虫策略"""
        if self.driver:
            return True

        self.logger.info("正在启动Firefox浏览器...")

        try:
            # Firefox选项设置
            options = FirefoxOptions()

            # 基础反爬虫设置（参考example.py）
            browser_config = self.config["browser_config"]
            if browser_config.get("headless", True):
                options.add_argument("--headless")
            if browser_config.get("disable_gpu", True):
                options.add_argument("--disable-gpu")

            # 额外的反检测设置
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-blink-features=AutomationControlled")

            # 设置用户代理
            anti_detection = browser_config.get("anti_detection", {})
            user_agent = anti_detection.get(
                "user_agent",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
            )
            options.set_preference("general.useragent.override", user_agent)

            # 禁用图片加载以提高速度
            if anti_detection.get("disable_images", True):
                options.set_preference("permissions.default.image", 2)

            self.driver = webdriver.Firefox(options=options)

            # 设置超时
            timeouts = browser_config.get("timeouts", {})
            self.driver.set_page_load_timeout(timeouts.get("page_load", 30))
            self.driver.implicitly_wait(timeouts.get("implicit_wait", 10))

            self.logger.info("Firefox浏览器启动成功")
            return True

        except Exception as e:
            self.logger.error(f"Firefox启动失败: {e}")
            return False

    def explicit_wait(self, by, selector, timeout=None):
        """显式等待 - 完全按照example.py的实现"""
        if timeout is None:
            timeout = self.config["browser_config"]["timeouts"].get(
                "explicit_wait", 150
            )

        try:
            element = WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((by, selector))
            )
            return element
        except TimeoutException:
            self.logger.warning(f"元素等待超时: {selector}")
            return None
        except Exception as e:
            self.logger.error(f"元素寻找失败: {str(e)}")
            return None

    def parse_list_page(self, response):
        """解析列表页面"""
        self.logger.info(f"开始解析列表页: {response.url}")

        # 检查是否遇到反爬虫（412错误）
        if response.status == 412:
            self.logger.warning(f"遇到412错误，使用Selenium绕过反爬虫: {response.url}")

        # 设置Firefox驱动
        if not self.setup_firefox_driver():
            self.logger.error("Firefox驱动设置失败")
            return

        try:
            # 使用Selenium访问页面
            self.driver.get(response.url)
            self.logger.info(f"Selenium访问页面: {response.url}")

            # 等待页面加载
            time.sleep(2)

            # 提取新闻列表
            news_items = self.extract_news_list()

            if not news_items:
                self.logger.warning("未找到新闻列表")
                # 保存页面源码用于调试
                debug_file = f"debug/page_source_{int(time.time())}.html"
                with open(debug_file, "w", encoding="utf-8") as f:
                    f.write(self.driver.page_source)
                self.logger.info(f"页面源码已保存到: {debug_file}")
                return

            # 处理新闻项
            max_items = self.config["crawling_strategy"]["limits"]["max_items_per_page"]
            for i, news_info in enumerate(news_items[:max_items]):
                if news_info and news_info.get("url"):
                    self.logger.info(f"处理第 {i + 1} 个新闻: {news_info['title'][:50]}...")

                    # 生成详情页请求
                    yield Request(
                        url=news_info["url"],
                        callback=self.parse_detail_page,
                        meta={"news_info": news_info, "use_selenium": True},
                        dont_filter=True,  # 不过滤重复请求
                    )

                    # 延迟
                    delay = self.config["crawling_strategy"]["delays"][
                        "between_requests"
                    ]
                    time.sleep(delay)
                else:
                    self.logger.warning(f"第 {i + 1} 个新闻信息无效，跳过")

        except Exception as e:
            self.logger.error(f"解析列表页失败: {e}")
        finally:
            # 不在这里关闭driver，在spider_closed中关闭
            pass

    def extract_news_list(self):
        """提取新闻列表 - 返回新闻信息而不是元素引用"""
        self.logger.info("正在提取新闻列表...")

        news_items = []
        selectors = self.config["selectors"]["news_list"]

        for selector_config in selectors:
            try:
                if "xpath" in selector_config:
                    elements = self.driver.find_elements(
                        By.XPATH, selector_config["xpath"]
                    )
                elif "css" in selector_config:
                    elements = self.driver.find_elements(
                        By.CSS_SELECTOR, selector_config["css"]
                    )

                if elements:
                    self.logger.info(f"使用选择器找到 {len(elements)} 个新闻项")

                    # 立即提取所有新闻项的信息，避免元素引用过期
                    for i, element in enumerate(elements):
                        try:
                            news_info = self.extract_news_info_immediately(
                                element, i + 1
                            )
                            if news_info:
                                news_items.append(news_info)
                        except Exception as e:
                            self.logger.warning(f"提取第 {i + 1} 项信息失败: {e}")
                            continue

                    self.logger.info(f"成功提取 {len(news_items)} 个新闻项信息")
                    break
            except Exception as e:
                self.logger.warning(f"选择器失败: {e}")
                continue

        return news_items

    def extract_news_info_immediately(self, element, index):
        """立即从元素中提取新闻信息，避免引用过期 - 性能优化版本"""
        try:
            # 优化1: 使用最有效的选择器，减少尝试次数
            date_text = self.extract_date_fast(element)
            if not date_text:
                return None

            # 优化2: 同时提取链接和标题
            link_info = self.extract_link_fast(element)
            if not link_info:
                return None

            link_url, title_text = link_info

            # 处理相对链接
            if link_url and link_url.startswith("/"):
                base_url = self.config["site_info"]["base_url"]
                link_url = base_url + link_url

            # 验证数据
            if date_text and title_text and link_url:
                return {
                    "date": date_text,
                    "title": title_text,
                    "url": link_url,
                    "index": index,
                }
            else:
                return None

        except Exception as e:
            self.logger.error(f"第 {index} 项快速提取失败: {e}")
            return None

    def extract_date_fast(self, element):
        """快速提取日期 - 只使用最有效的选择器"""
        # 按效果排序的选择器，优先使用最可能成功的
        fast_date_selectors = [
            ("css", "span.ml"),  # 国家卫健委最常用
            ("css", "span.date"),  # 国务院页面
            ("xpath", './/span[@class="ml"]'),  # 备用XPath
            ("tag", "span"),  # 最后备用
        ]

        for selector_type, selector_value in fast_date_selectors:
            try:
                if selector_type == "css":
                    date_element = element.find_element(By.CSS_SELECTOR, selector_value)
                elif selector_type == "xpath":
                    date_element = element.find_element(By.XPATH, selector_value)
                elif selector_type == "tag":
                    date_element = element.find_element(By.TAG_NAME, selector_value)

                if date_element:
                    date_text = date_element.text.strip()
                    if date_text and (
                        "-" in date_text or "20" in date_text
                    ):  # 简单的日期格式验证
                        return date_text
            except:
                continue

        return None

    def extract_link_fast(self, element):
        """快速提取链接和标题 - 只使用最有效的选择器"""
        # 按效果排序的选择器
        fast_link_selectors = [
            ("xpath", ".//a[@href and @title]"),  # 国家卫健委最常用
            ("xpath", ".//h4/a[@href]"),  # 国务院页面
            ("xpath", './/a[@target="_blank"]'),  # 有target属性
            ("tag", "a"),  # 最后备用
        ]

        for selector_type, selector_value in fast_link_selectors:
            try:
                if selector_type == "css":
                    link_element = element.find_element(By.CSS_SELECTOR, selector_value)
                elif selector_type == "xpath":
                    link_element = element.find_element(By.XPATH, selector_value)
                elif selector_type == "tag":
                    link_element = element.find_element(By.TAG_NAME, selector_value)

                if link_element:
                    link_url = link_element.get_attribute("href")
                    if not link_url:
                        continue

                    # 获取标题 - 优先使用title属性
                    title_text = link_element.get_attribute("title")
                    if not title_text:
                        title_text = link_element.text.strip()

                    if title_text and link_url:
                        return (link_url, title_text)
            except:
                continue

        return None

    def parse_detail_page(self, response):
        """解析详情页面"""
        news_info = response.meta["news_info"]
        self.logger.info(f"解析详情页: {news_info['title'][:30]}...")

        try:
            # 使用Selenium访问详情页
            self.driver.get(response.url)
            time.sleep(1)

            # 提取内容
            content = self.extract_page_content()

            if content:
                # 保存文本文件（参考example.py）
                self.save_text_file(news_info["date"], content)

                # 生成Scrapy Item
                item = {
                    "url": response.url,
                    "title": news_info["title"],
                    "date": news_info["date"],
                    "content": content,
                    "source": "国家卫健委",
                    "crawl_time": datetime.datetime.now().isoformat(),
                    "spider_name": self.name,
                }

                self.stats["successful_extractions"] += 1
                self.logger.info(f"成功提取: {news_info['title'][:30]}...")

                yield item
            else:
                self.stats["failed_extractions"] += 1
                self.logger.warning(f"内容提取失败: {response.url}")

        except Exception as e:
            self.stats["failed_extractions"] += 1
            self.logger.error(f"详情页解析失败: {e}")

        self.stats["total_processed"] += 1

    def extract_page_content(self):
        """提取页面内容 - 根据实际详情页结构优化"""
        selectors = self.config["selectors"]["content_selectors"]

        self.logger.info(f"开始尝试 {len(selectors)} 个内容选择器...")

        for i, selector_config in enumerate(selectors, 1):
            try:
                selector_type = None
                selector_value = None

                if "xpath" in selector_config:
                    selector_type = "XPATH"
                    selector_value = selector_config["xpath"]
                    element = self.explicit_wait(
                        By.XPATH, selector_value, timeout=5
                    )  # 减少超时时间
                elif "css" in selector_config:
                    selector_type = "CSS"
                    selector_value = selector_config["css"]
                    element = self.explicit_wait(
                        By.CSS_SELECTOR, selector_value, timeout=5
                    )

                self.logger.info(
                    f"尝试选择器 {i}/{len(selectors)}: {selector_type} = {selector_value}"
                )

                if element:
                    content = element.text.strip()
                    self.logger.info(f"选择器成功，内容长度: {len(content)}")

                    if content and len(content) > 50:
                        # 清理内容（参考example.py）
                        content = content.replace("分享到", "")
                        content = content.replace("来源：", "")
                        content = content.replace("责任编辑：", "")

                        self.logger.info(
                            f"✅ 成功提取内容，使用选择器: {selector_type} = {selector_value}"
                        )
                        self.logger.info(f"内容预览: {content[:100]}...")
                        return content
                    else:
                        self.logger.warning(
                            f"内容太短 ({len(content)} 字符): {content[:50]}..."
                        )
                else:
                    self.logger.warning(f"选择器未找到元素: {selector_type} = {selector_value}")

            except Exception as e:
                self.logger.warning(f"选择器 {i} 失败 ({selector_type}): {e}")
                continue

        # 如果所有选择器都失败，尝试获取页面标题作为内容
        try:
            title_element = self.driver.find_element(By.TAG_NAME, "title")
            if title_element:
                title_content = title_element.text.strip()
                self.logger.warning(f"所有内容选择器失败，使用页面标题: {title_content}")
                return f"标题: {title_content}"
        except:
            pass

        self.logger.error("❌ 所有内容选择器都失败了")
        return None

    def save_text_file(self, date, content):
        """保存文本文件（参考example.py）"""
        try:
            filename = f"texts/{date.replace('/', '-')}.txt"
            with open(filename, "w", encoding="utf-8") as f:
                f.write(content)
            self.logger.info(f"文本文件已保存: {filename}")
        except Exception as e:
            self.logger.error(f"保存文本文件失败: {e}")

    def parse_all_pages_with_selenium(self):
        """直接使用Selenium解析所有页面 - 支持分页"""
        self.logger.info("开始直接使用Selenium模式爬取所有页面")

        # 设置Firefox驱动
        if not self.setup_firefox_driver():
            self.logger.error("Firefox驱动设置失败")
            return

        try:
            for page_config in self.config["target_pages"]:
                self.logger.info(f"处理页面: {page_config['name']} - {page_config['url']}")

                # 处理多页数据，返回生成器
                yield from self.crawl_multiple_pages(page_config)

        except Exception as e:
            self.logger.error(f"Selenium直接模式失败: {e}")
        finally:
            # 在spider_closed中关闭driver
            pass

    def crawl_multiple_pages(self, page_config):
        """爬取多页数据"""
        current_url = page_config["url"]
        page_num = 1
        max_pages = self.config["crawling_strategy"]["limits"].get("max_pages", 3)

        while page_num <= max_pages:
            self.logger.info(f"=== 处理第 {page_num} 页 ===")

            # 访问当前页面
            self.driver.get(current_url)
            self.logger.info(f"Selenium访问第 {page_num} 页: {current_url}")

            # 保存列表页URL，用于生成下一页URL
            list_page_url = current_url

            # 等待页面加载
            time.sleep(2)

            # 提取新闻列表
            news_items = self.extract_news_list()

            if not news_items:
                self.logger.warning(f"第 {page_num} 页未找到新闻列表")
                # 保存页面源码用于调试
                debug_file = f"debug/page_source_{page_config['name']}_{page_num}_{int(time.time())}.html"
                with open(debug_file, "w", encoding="utf-8") as f:
                    f.write(self.driver.page_source)
                self.logger.info(f"页面源码已保存到: {debug_file}")
                break

            # 处理当前页面的新闻项，返回生成器
            yield from self.process_news_items(news_items, page_num, page_config)

            # 尝试找到下一页链接 - 使用保存的列表页URL
            next_url = self.find_next_page_url(list_page_url)
            if next_url and next_url != current_url:
                current_url = next_url
                page_num += 1
                self.logger.info(f"找到下一页链接: {next_url}")

                # 页面间延迟
                time.sleep(1)
            else:
                self.logger.info("未找到下一页链接或已到最后一页")
                break

    def process_news_items(self, news_items, page_num, page_config):
        """处理当前页面的新闻项 - news_items现在是信息字典列表"""
        max_items = self.config["crawling_strategy"]["limits"]["max_items_per_page"]
        processed_count = 0

        for i, news_info in enumerate(news_items[:max_items]):
            self.logger.info(f"第 {page_num} 页 - 处理第 {i + 1} 个新闻项")

            if news_info:
                self.logger.info(f"✅ 新闻信息: {news_info['title'][:50]}...")

                # 直接处理详情页
                detail_data = self.crawl_detail_page_selenium(news_info)
                if detail_data:
                    # 根据页面类型创建不同的Item
                    if self.is_epidemic_content(detail_data):
                        item = self.create_epidemic_item(detail_data, page_num)
                    else:
                        item = self.create_news_item(detail_data, page_num)

                    if item:
                        # 添加页面配置信息 - 根据Item类型设置不同字段
                        if isinstance(item, NewsItem):
                            item["source"] = page_config.get("name", "国家卫健委")
                        elif isinstance(item, EpidemicDataItem):
                            item["source_name"] = page_config.get("name", "国家卫健委")

                        # 通过Scrapy Pipeline处理
                        yield item

                        processed_count += 1
                        self.stats["successful_extractions"] += 1
                    else:
                        self.stats["failed_extractions"] += 1
                        self.logger.warning(f"❌ Item创建失败: {news_info['url']}")
                else:
                    self.stats["failed_extractions"] += 1
                    self.logger.warning(f"❌ 详情页内容提取失败: {news_info['url']}")
            else:
                self.stats["failed_extractions"] += 1
                self.logger.warning(f"❌ 新闻信息为空: 第 {i + 1} 项")

            # 延迟
            delay = self.config["crawling_strategy"]["delays"]["between_requests"]
            time.sleep(delay)

        self.stats["total_processed"] += processed_count
        self.logger.info(f"第 {page_num} 页处理完成，成功 {processed_count} 个")

    def is_epidemic_content(self, detail_data):
        """判断是否为疫情相关内容"""
        if not detail_data:
            return False

        title = detail_data.get("title", "").lower()
        content = detail_data.get("content", "").lower()

        epidemic_keywords = [
            "疫情",
            "传染病",
            "病例",
            "确诊",
            "死亡",
            "治愈",
            "新冠",
            "covid",
            "肺炎",
            "感染",
            "防控",
        ]

        return any(
            keyword in title or keyword in content for keyword in epidemic_keywords
        )

    def find_next_page_url(self, list_page_url):
        """查找下一页链接 - 支持国家卫健委的分页规律"""
        self.logger.info(f"使用列表页URL生成下一页: {list_page_url}")

        # 国家卫健委分页规律: list.shtml -> list_2.shtml -> list_3.shtml
        if "nhc.gov.cn" in list_page_url:
            return self.generate_nhc_next_page_url(list_page_url)

        # 通用分页链接查找 - 需要先返回到列表页
        try:
            self.driver.get(list_page_url)
            time.sleep(1)

            next_page_selectors = [
                "//a[contains(text(), '下一页')]",
                "//a[contains(text(), '下页')]",
                "//a[contains(text(), 'Next')]",
                "//a[contains(text(), '>')]",
                "//a[@class='next']",
                "//a[contains(@class, 'next')]",
                "//div[@class='pagination']//a[last()]",
                "//div[contains(@class, 'page')]//a[contains(text(), '下')]",
            ]

            for selector in next_page_selectors:
                try:
                    next_link = self.driver.find_element(By.XPATH, selector)
                    if next_link and next_link.get_attribute("href"):
                        next_url = next_link.get_attribute("href")
                        self.logger.info(f"找到下一页链接: {next_url}")
                        return next_url
                except:
                    continue
        except Exception as e:
            self.logger.error(f"返回列表页查找下一页链接失败: {e}")

        self.logger.info("未找到下一页链接")
        return None

    def generate_nhc_next_page_url(self, list_page_url):
        """生成国家卫健委的下一页URL"""
        try:
            self.logger.info(f"列表页URL: {list_page_url}")

            if list_page_url.endswith("list.shtml"):
                # 第一页 -> 第二页
                next_url = list_page_url.replace("list.shtml", "list_2.shtml")
                self.logger.info(f"第一页 -> 第二页: {next_url}")
                return next_url
            elif "list_" in list_page_url and list_page_url.endswith(".shtml"):
                # 提取当前页码
                import re

                match = re.search(r"list_(\d+)\.shtml", list_page_url)
                if match:
                    current_page = int(match.group(1))
                    next_page = current_page + 1
                    next_url = list_page_url.replace(
                        f"list_{current_page}.shtml", f"list_{next_page}.shtml"
                    )
                    self.logger.info(f"第{current_page}页 -> 第{next_page}页: {next_url}")
                    return next_url
                else:
                    self.logger.warning(f"无法从URL中提取页码: {list_page_url}")
            else:
                self.logger.warning(f"URL格式不匹配分页规律: {list_page_url}")

            self.logger.info("无法生成下一页URL")
            return None

        except Exception as e:
            self.logger.error(f"生成下一页URL失败: {e}")
            return None

    def crawl_detail_page_selenium(self, news_info):
        """使用Selenium爬取详情页面内容"""
        try:
            # 访问详情页
            self.driver.get(news_info["url"])
            self.logger.info(f"Selenium访问详情页: {news_info['title'][:30]}...")

            # 等待页面加载
            time.sleep(1)

            # 提取内容
            content = self.extract_page_content()

            if content:
                # 保存文本文件（参考example.py）
                self.save_text_file(news_info["date"], content)

                self.logger.info(f"成功提取: {news_info['title'][:30]}...")

                return {
                    "url": news_info["url"],
                    "title": news_info["title"],
                    "date": news_info["date"],
                    "content": content,
                }
            else:
                self.logger.warning(f"内容提取失败: {news_info['url']}")
                return None

        except Exception as e:
            self.logger.error(f"Selenium详情页爬取失败: {e}")
            return None

    def create_news_item(self, detail_data, page_num):
        """创建NewsItem对象"""
        try:
            item = NewsItem()

            # 基础信息
            item["url"] = detail_data["url"]
            item["title"] = detail_data["title"]
            item["content"] = detail_data["content"]

            # 发布信息
            item["publish_date"] = detail_data["date"]
            item["source"] = "国家卫健委"

            # 分类标签
            item["category"] = self.determine_category(detail_data["title"])
            item["tags"] = self.extract_tags(detail_data["content"])

            # 元数据
            item["crawl_time"] = datetime.datetime.now().isoformat()
            item["spider_name"] = self.name

            self.logger.info(f"✅ 创建NewsItem成功: {item['title'][:30]}...")
            return item

        except Exception as e:
            self.logger.error(f"创建NewsItem失败: {e}")
            return None

    def create_epidemic_item(self, detail_data, page_num):
        """创建EpidemicDataItem对象"""
        try:
            item = EpidemicDataItem()

            # 基础信息
            item["source_url"] = detail_data["url"]
            item["source_name"] = "国家卫健委"
            item["crawl_time"] = datetime.datetime.now().isoformat()

            # 内容信息
            item["title"] = detail_data["title"]
            item["content"] = detail_data["content"]

            # 疫情数据提取
            item["region"] = self.extract_region(detail_data["content"])
            item["confirmed_cases"] = self.extract_confirmed_cases(
                detail_data["content"]
            )
            item["death_cases"] = self.extract_death_cases(detail_data["content"])
            item["recovered_cases"] = self.extract_recovered_cases(
                detail_data["content"]
            )

            # 时间信息
            item["report_date"] = detail_data["date"]

            # 元数据
            item["spider_name"] = self.name
            item["crawl_timestamp"] = datetime.datetime.now().timestamp()

            self.logger.info(f"✅ 创建EpidemicDataItem成功: {item['title'][:30]}...")
            return item

        except Exception as e:
            self.logger.error(f"创建EpidemicDataItem失败: {e}")
            return None

    def determine_category(self, title):
        """根据标题确定分类"""
        if not title:
            return "其他"

        title_lower = title.lower()

        if any(keyword in title_lower for keyword in ["疫情", "传染病", "病例", "确诊"]):
            return "疫情播报"
        elif any(keyword in title_lower for keyword in ["政策", "通知", "公告", "规定"]):
            return "政策文件"
        elif any(keyword in title_lower for keyword in ["新闻", "动态", "会议"]):
            return "新闻动态"
        elif any(keyword in title_lower for keyword in ["统计", "数据", "报告"]):
            return "统计数据"
        else:
            return "其他"

    def extract_tags(self, content):
        """从内容中提取标签"""
        if not content:
            return []

        tags = []
        content_lower = content.lower()

        # 疫情相关标签
        if any(keyword in content_lower for keyword in ["新冠", "covid", "肺炎"]):
            tags.append("新冠疫情")
        if any(keyword in content_lower for keyword in ["疫苗", "接种"]):
            tags.append("疫苗接种")
        if any(keyword in content_lower for keyword in ["防控", "防疫"]):
            tags.append("疫情防控")

        # 健康相关标签
        if any(keyword in content_lower for keyword in ["健康", "医疗", "卫生"]):
            tags.append("健康医疗")
        if any(keyword in content_lower for keyword in ["医院", "诊疗"]):
            tags.append("医疗服务")

        return tags

    def extract_region(self, content):
        """从内容中提取地区信息"""
        if not content:
            return "全国"

        # 简单的地区提取逻辑
        regions = ["北京", "上海", "广东", "浙江", "江苏", "山东", "河南", "湖北", "湖南", "四川"]
        for region in regions:
            if region in content:
                return region

        return "全国"

    def extract_confirmed_cases(self, content):
        """从内容中提取确诊病例数"""
        if not content:
            return 0

        # 使用正则表达式提取确诊病例数
        patterns = [r"确诊病例(\d+)例", r"确诊(\d+)例", r"新增确诊病例(\d+)例"]

        for pattern in patterns:
            match = re.search(pattern, content)
            if match:
                return int(match.group(1))

        return 0

    def extract_death_cases(self, content):
        """从内容中提取死亡病例数"""
        if not content:
            return 0

        patterns = [r"死亡病例(\d+)例", r"死亡(\d+)例", r"新增死亡病例(\d+)例"]

        for pattern in patterns:
            match = re.search(pattern, content)
            if match:
                return int(match.group(1))

        return 0

    def extract_recovered_cases(self, content):
        """从内容中提取治愈病例数"""
        if not content:
            return 0

        patterns = [r"治愈出院病例(\d+)例", r"治愈(\d+)例", r"出院病例(\d+)例"]

        for pattern in patterns:
            match = re.search(pattern, content)
            if match:
                return int(match.group(1))

        return 0

    def process_item_through_pipelines(self, item_data):
        """手动通过Pipeline处理Item"""
        try:
            # 确保data目录存在
            os.makedirs("data", exist_ok=True)

            # 保存到JSON文件
            filename = f"data/nhc_selenium_{int(time.time())}.json"
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(item_data, f, ensure_ascii=False, indent=2)

            self.logger.info(f"数据已保存: {filename}")

        except Exception as e:
            self.logger.error(f"Pipeline处理失败: {e}")

    def spider_closed(self, spider):
        """爬虫关闭时的清理工作"""
        if self.driver:
            self.driver.quit()
            self.logger.info("Firefox浏览器已关闭")

        # 输出统计信息
        duration = time.time() - self.stats["start_time"]
        self.logger.info(
            f"""
=== 爬取统计 ===
总处理数: {self.stats['total_processed']}
成功提取: {self.stats['successful_extractions']}
失败提取: {self.stats['failed_extractions']}
耗时: {duration:.2f} 秒
        """
        )
