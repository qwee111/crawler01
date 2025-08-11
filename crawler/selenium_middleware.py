# -*- coding: utf-8 -*-
"""
Selenium中间件

用于处理JavaScript渲染的页面和复杂的反爬虫机制
"""

import logging
import time
from typing import Any, Dict, Optional
from urllib.parse import urlparse

from scrapy import signals
from scrapy.exceptions import NotConfigured
from scrapy.http import HtmlResponse

try:
    from selenium import webdriver
    from selenium.common.exceptions import TimeoutException, WebDriverException
    from selenium.webdriver.chrome.options import Options as ChromeOptions
    from selenium.webdriver.common.by import By
    from selenium.webdriver.firefox.options import Options as FirefoxOptions
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait

    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False

logger = logging.getLogger(__name__)


class SeleniumMiddleware:
    """Selenium下载中间件"""

    def __init__(
        self,
        selenium_grid_url: str = None,
        browser: str = "chrome",
        implicit_wait: int = 10,
        page_load_timeout: int = 30,
        window_size: tuple = (1920, 1080),
    ):
        if not SELENIUM_AVAILABLE:
            raise NotConfigured("Selenium not available")

        self.selenium_grid_url = selenium_grid_url or "http://localhost:4444"
        self.browser = browser.lower()
        self.implicit_wait = implicit_wait
        self.page_load_timeout = page_load_timeout
        self.window_size = window_size

        # 驱动池
        self.drivers = {}
        self.max_drivers = 3

        logger.info(
            f"Selenium中间件初始化: Grid={self.selenium_grid_url}, Browser={self.browser}"
        )

    @classmethod
    def from_crawler(cls, crawler):
        """从爬虫配置创建中间件"""
        settings = crawler.settings

        selenium_grid_url = settings.get("SELENIUM_GRID_URL", "http://localhost:4444")
        browser = settings.get("SELENIUM_BROWSER", "chrome")
        implicit_wait = settings.getint("SELENIUM_IMPLICIT_WAIT", 10)
        page_load_timeout = settings.getint("SELENIUM_PAGE_LOAD_TIMEOUT", 30)
        window_size = settings.get("SELENIUM_WINDOW_SIZE", (1920, 1080))

        middleware = cls(
            selenium_grid_url=selenium_grid_url,
            browser=browser,
            implicit_wait=implicit_wait,
            page_load_timeout=page_load_timeout,
            window_size=window_size,
        )

        # 连接信号
        crawler.signals.connect(middleware.spider_closed, signal=signals.spider_closed)

        return middleware

    def _get_driver_options(self):
        """获取浏览器选项"""
        if self.browser == "chrome":
            options = ChromeOptions()
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--disable-extensions")
            options.add_argument("--disable-plugins")
            options.add_argument("--disable-images")
            options.add_argument("--disable-javascript")  # 可选：禁用JS以提高速度
            options.add_argument(
                f"--window-size={self.window_size[0]},{self.window_size[1]}"
            )
            options.add_argument(
                "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            return options

        elif self.browser == "firefox":
            options = FirefoxOptions()
            options.add_argument("--headless")
            options.add_argument(f"--width={self.window_size[0]}")
            options.add_argument(f"--height={self.window_size[1]}")
            # 设置用户代理
            options.set_preference(
                "general.useragent.override",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
            )
            return options

        else:
            raise ValueError(f"不支持的浏览器: {self.browser}")

    def _get_driver(self) -> webdriver.Remote:
        """获取WebDriver实例"""
        driver_id = f"{self.browser}_{len(self.drivers)}"

        if len(self.drivers) >= self.max_drivers:
            # 重用现有驱动
            driver_id = list(self.drivers.keys())[0]
            return self.drivers[driver_id]

        try:
            options = self._get_driver_options()

            if self.browser == "chrome":
                driver = webdriver.Remote(
                    command_executor=f"{self.selenium_grid_url}/wd/hub", options=options
                )
            elif self.browser == "firefox":
                driver = webdriver.Remote(
                    command_executor=f"{self.selenium_grid_url}/wd/hub", options=options
                )
            else:
                raise ValueError(f"不支持的浏览器: {self.browser}")

            # 设置超时
            driver.implicitly_wait(self.implicit_wait)
            driver.set_page_load_timeout(self.page_load_timeout)

            self.drivers[driver_id] = driver
            logger.info(f"创建新的WebDriver: {driver_id}")

            return driver

        except Exception as e:
            logger.error(f"创建WebDriver失败: {e}")
            raise

    def _should_use_selenium(self, request) -> bool:
        """判断是否应该使用Selenium"""
        # 检查请求元数据
        if request.meta.get("selenium", False):
            return True

        # 检查URL模式
        selenium_patterns = [
            "javascript:",
            "#",  # 单页应用
        ]

        for pattern in selenium_patterns:
            if pattern in request.url:
                return True

        # 检查特定网站
        domain = urlparse(request.url).netloc
        selenium_domains = [
            "spa-website.com",  # 单页应用网站
            "js-heavy.com",  # JavaScript重度网站
        ]

        for selenium_domain in selenium_domains:
            if selenium_domain in domain:
                return True

        return False

    def process_request(self, request, spider):
        """处理请求"""
        if not self._should_use_selenium(request):
            return None

        logger.info(f"使用Selenium处理: {request.url}")

        try:
            driver = self._get_driver()

            # 访问页面
            driver.get(request.url)

            # 等待页面加载
            wait_time = request.meta.get("selenium_wait", 3)
            time.sleep(wait_time)

            # 执行自定义JavaScript
            custom_js = request.meta.get("selenium_js")
            if custom_js:
                driver.execute_script(custom_js)
                time.sleep(1)

            # 等待特定元素
            wait_element = request.meta.get("selenium_wait_element")
            if wait_element:
                try:
                    wait = WebDriverWait(driver, 10)
                    wait.until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, wait_element))
                    )
                except TimeoutException:
                    logger.warning(f"等待元素超时: {wait_element}")

            # 获取页面源码
            body = driver.page_source.encode("utf-8")

            # 创建响应
            response = HtmlResponse(
                url=driver.current_url, body=body, encoding="utf-8", request=request
            )

            logger.info(f"Selenium处理完成: {request.url} -> {len(body)} bytes")
            return response

        except Exception as e:
            logger.error(f"Selenium处理失败 {request.url}: {e}")
            return None

    def spider_closed(self, spider):
        """爬虫关闭时清理资源"""
        logger.info("清理Selenium资源...")

        for driver_id, driver in self.drivers.items():
            try:
                driver.quit()
                logger.info(f"关闭WebDriver: {driver_id}")
            except Exception as e:
                logger.error(f"关闭WebDriver失败 {driver_id}: {e}")

        self.drivers.clear()


class SeleniumRequest:
    """Selenium请求辅助类"""

    @staticmethod
    def create_request(
        url: str,
        callback=None,
        wait_time: int = 3,
        wait_element: str = None,
        custom_js: str = None,
        **kwargs,
    ):
        """创建Selenium请求"""
        from scrapy import Request

        meta = kwargs.get("meta", {})
        meta.update(
            {
                "selenium": True,
                "selenium_wait": wait_time,
                "selenium_wait_element": wait_element,
                "selenium_js": custom_js,
            }
        )

        return Request(
            url=url,
            callback=callback,
            meta=meta,
            **{k: v for k, v in kwargs.items() if k != "meta"},
        )


class JavaScriptMiddleware:
    """JavaScript处理中间件"""

    def __init__(self):
        self.js_patterns = [
            r"window\.location\.href\s*=",
            r"document\.location\s*=",
            r"location\.replace\(",
            r"setTimeout\(",
            r"setInterval\(",
        ]

    def process_response(self, request, response, spider):
        """检测JavaScript重定向"""
        if (
            response.status == 200
            and "text/html" in response.headers.get("Content-Type", b"").decode()
        ):
            # 检查是否包含JavaScript重定向
            body_text = response.text.lower()

            for pattern in self.js_patterns:
                import re

                if re.search(pattern, body_text):
                    logger.info(f"检测到JavaScript重定向: {request.url}")
                    # 标记需要使用Selenium
                    request.meta["selenium"] = True
                    return request

        return response
