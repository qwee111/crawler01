# -*- coding: utf-8 -*-
"""
Selenium中间件

用于处理JavaScript渲染的页面和复杂的反爬虫机制
"""

import logging
import time
from typing import Dict, Tuple
from urllib.parse import urlparse

from scrapy import signals
from scrapy.exceptions import NotConfigured
from scrapy.http import HtmlResponse

try:
    from selenium import webdriver
    from selenium.common.exceptions import TimeoutException
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

    def __init__(self, crawler):
        if not SELENIUM_AVAILABLE:
            raise NotConfigured("Selenium not available")

        self.crawler = crawler
        self.settings = crawler.settings

        self.selenium_grid_url = self.settings.get(
            "SELENIUM_GRID_URL", "http://localhost:4444"
        )
        self.default_browser = self.settings.get(
            "SELENIUM_BROWSER", "firefox"
        ).lower()  # 默认改为firefox
        self.default_implicit_wait = self.settings.getint("SELENIUM_IMPLICIT_WAIT", 10)
        self.default_page_load_timeout = self.settings.getint(
            "SELENIUM_PAGE_LOAD_TIMEOUT", 30
        )
        self.default_window_size = self.settings.get(
            "SELENIUM_WINDOW_SIZE", (1920, 1080)
        )

        # 驱动池，按站点和浏览器类型管理
        self.drivers = {}  # { (site_name, browser_type): [driver1, driver2, ...] }
        self.max_drivers_per_site_browser = self.settings.getint(
            "SELENIUM_MAX_DRIVERS_PER_SITE_BROWSER", 1
        )

        logger.info(
            f"Selenium中间件初始化: Grid={self.selenium_grid_url}, Default Browser={self.default_browser}"
        )

    @classmethod
    def from_crawler(cls, crawler):
        """从爬虫配置创建中间件"""
        middleware = cls(crawler)
        crawler.signals.connect(middleware.spider_closed, signal=signals.spider_closed)
        return middleware

    def _get_driver_options(self, browser_type: str, selenium_config: Dict):
        """获取浏览器选项"""
        options = None
        window_size = selenium_config.get("window_size", self.default_window_size)
        user_agent = selenium_config.get("anti_detection", {}).get("user_agent")

        if browser_type == "chrome":
            options = ChromeOptions()
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            if selenium_config.get("headless", True):
                options.add_argument("--headless")
            if selenium_config.get("disable_gpu", True):
                options.add_argument("--disable-gpu")
            options.add_argument("--disable-extensions")
            options.add_argument("--disable-plugins")
            if selenium_config.get("anti_detection", {}).get("disable_images", False):
                options.add_argument("--disable-images")
            if selenium_config.get("anti_detection", {}).get(
                "disable_javascript", False
            ):
                options.add_argument("--disable-javascript")
            options.add_argument(f"--window-size={window_size[0]},{window_size[1]}")
            if user_agent:
                options.add_argument(f"--user-agent={user_agent}")
            else:
                options.add_argument(
                    "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                )

        elif browser_type == "firefox":
            options = FirefoxOptions()
            if selenium_config.get("headless", True):
                options.add_argument("--headless")
            if selenium_config.get("disable_gpu", True):
                options.add_argument("--disable-gpu")
            options.add_argument(f"--width={window_size[0]}")
            options.add_argument(f"--height={window_size[1]}")
            if user_agent:
                options.set_preference("general.useragent.override", user_agent)
            else:
                options.set_preference(
                    "general.useragent.override",
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
                )
            if selenium_config.get("anti_detection", {}).get(
                "disable_automation_flags", False
            ):
                options.add_argument("--disable-blink-features=AutomationControlled")
            if selenium_config.get("anti_detection", {}).get("disable_images", False):
                options.set_preference("permissions.default.image", 2)

        else:
            raise ValueError(f"不支持的浏览器: {browser_type}")

        return options

    def _get_driver(self, site_name: str, selenium_config: Dict) -> webdriver.Remote:
        """获取WebDriver实例"""
        browser_type = selenium_config.get("browser_type", self.default_browser)
        driver_key = (site_name, browser_type)

        if driver_key not in self.drivers:
            self.drivers[driver_key] = []

        # 尝试重用现有驱动
        if self.drivers[driver_key]:
            # 简单重用第一个可用的驱动
            return self.drivers[driver_key][0]

        # 创建新驱动
        try:
            options = self._get_driver_options(browser_type, selenium_config)

            if browser_type == "chrome":
                driver = webdriver.Remote(
                    command_executor=f"{self.selenium_grid_url}/wd/hub", options=options
                )
            elif browser_type == "firefox":
                driver = webdriver.Remote(
                    command_executor=f"{self.selenium_grid_url}/wd/hub", options=options
                )
            else:
                raise ValueError(f"不支持的浏览器: {browser_type}")

            # 设置超时
            implicit_wait = selenium_config.get("timeouts", {}).get(
                "implicit_wait", self.default_implicit_wait
            )
            page_load_timeout = selenium_config.get("timeouts", {}).get(
                "page_load", self.default_page_load_timeout
            )

            driver.implicitly_wait(implicit_wait)
            driver.set_page_load_timeout(page_load_timeout)

            self.drivers[driver_key].append(driver)
            logger.info(f"创建新的WebDriver: {browser_type} for {site_name}")

            return driver

        except Exception as e:
            logger.error(f"创建WebDriver失败 for {site_name} ({browser_type}): {e}")
            raise

        except Exception as e:
            logger.error(f"创建WebDriver失败: {e}")
            raise

    def _should_use_selenium(self, request, spider) -> bool:
        """判断是否应该使用Selenium"""
        # 1. 检查请求元数据中是否明确要求使用Selenium
        if request.meta.get("use_selenium", False):
            return True

        # 2. 检查站点配置中是否启用了Selenium
        site_name = request.meta.get("site_name") or getattr(
            spider, "target_site", None
        )
        logger.debug(
            f"SeleniumMiddleware: _should_use_selenium - site_name from meta/spider: {site_name}"
        )
        if site_name:
            # 使用 get_config_by_site 方法获取配置
            site_config = spider.config_manager.get_config_by_site(site_name)
            selenium_config_from_site = (
                site_config.get("request", {}).get("selenium", {})
                if site_config
                else {}
            )
            logger.debug(
                f"SeleniumMiddleware: _should_use_selenium - site_config for {site_name}: {selenium_config_from_site}"
            )
            if selenium_config_from_site.get("enabled", False):
                logger.debug(
                    f"SeleniumMiddleware: _should_use_selenium - Selenium enabled in site config for {site_name}"
                )
                return True

        # 3. 检查URL模式（通用规则，优先级最低）
        selenium_patterns = [
            "javascript:",
            "#",  # 单页应用
        ]
        for pattern in selenium_patterns:
            if pattern in request.url:
                return True

        return False

    def process_request(self, request, spider):
        """处理请求"""
        if not self._should_use_selenium(request, spider):
            return None

        site_name = request.meta.get("site_name") or getattr(
            spider, "target_site", None
        )
        if not site_name:
            logger.warning(f"⚠️ 无法确定网站名，无法获取Selenium配置。跳过Selenium处理: {request.url}")
            return None

        # 使用 get_config_by_site 方法获取配置
        site_config = spider.config_manager.get_config_by_site(site_name)
        if not site_config:
            logger.warning(f"⚠️ 未找到网站配置 {site_name}。跳过Selenium处理: {request.url}")
            return None

        selenium_config = site_config.get("request", {}).get("selenium", {})
        if not selenium_config.get("enabled", False):
            logger.info(f"站点 {site_name} 未启用Selenium。跳过Selenium处理: {request.url}")
            return None

        logger.info(f"使用Selenium处理 ({site_name}): {request.url}")

        try:
            driver = self._get_driver(site_name, selenium_config)

            # 获取配置的等待时间
            page_load_wait = (
                selenium_config.get("crawling_strategy", {})
                .get("delays", {})
                .get("page_load_wait", 1)
            )
            explicit_wait_timeout = selenium_config.get("timeouts", {}).get(
                "explicit_wait", 10
            )
            after_click_delay = (
                selenium_config.get("crawling_strategy", {})
                .get("delays", {})
                .get("after_click", 0.5)
            )

            # 获取模拟点击相关元数据
            selenium_click_selector = request.meta.get("selenium_click_selector")
            selenium_item_index = request.meta.get("selenium_item_index")
            detail_page_url_from_meta = request.meta.get("detail_page_url")

            # 访问页面
            driver.get(request.url)
            time.sleep(page_load_wait)  # 等待页面加载

            # 初始化 body 和 current_url
            body = driver.page_source.encode("utf-8")
            current_url = driver.current_url

            # 如果需要模拟点击
            if selenium_click_selector is not None and selenium_item_index is not None:
                logger.info(
                    f"执行Selenium模拟点击: selector='{selenium_click_selector}', index={selenium_item_index}"
                )
                try:
                    # 查找所有符合选择器的元素
                    elements = WebDriverWait(driver, explicit_wait_timeout).until(
                        EC.presence_of_all_elements_located(
                            (By.CSS_SELECTOR, selenium_click_selector)
                        )
                    )
                    if len(elements) > selenium_item_index:
                        target_element = elements[selenium_item_index]
                        target_element.click()
                        time.sleep(after_click_delay)  # 点击后等待

                        # 更新页面源码和URL为点击后的详情页
                        body = driver.page_source.encode("utf-8")
                        current_url = driver.current_url
                        logger.info(f"Selenium点击成功，进入详情页: {current_url}")
                    else:
                        logger.warning(
                            f"⚠️ Selenium点击失败: 索引 {selenium_item_index} 超出元素列表范围 ({len(elements)})。URL: {request.url}"
                        )
                except TimeoutException:
                    logger.warning(
                        f"⚠️ Selenium点击等待元素超时: selector='{selenium_click_selector}', index={selenium_item_index}. URL: {request.url}"
                    )
                except Exception as e:
                    logger.error(
                        f"❌ Selenium点击操作失败: {e}. selector='{selenium_click_selector}', index={selenium_item_index}. URL: {request.url}"
                    )

            # 执行自定义JavaScript (在点击之后执行，如果需要)
            custom_js = request.meta.get("selenium_js")
            if custom_js:
                driver.execute_script(custom_js)
                time.sleep(after_click_delay)  # JS执行后等待

            # 等待特定元素 (在点击之后执行，如果需要)
            wait_element_selector = request.meta.get("selenium_wait_element")
            if wait_element_selector:
                try:
                    wait = WebDriverWait(driver, explicit_wait_timeout)
                    wait.until(
                        EC.presence_of_element_located(
                            (By.CSS_SELECTOR, wait_element_selector)
                        )
                    )
                except TimeoutException:
                    logger.warning(f"等待元素超时 ({site_name}): {wait_element_selector}")

            # 域名校验：确保当前URL的域名与原始请求的域名一致
            # 如果是模拟点击，我们期望跳转到详情页，所以这里应该使用 meta 中存储的 detail_page_url 进行校验
            original_request_url_for_domain_check = (
                detail_page_url_from_meta if detail_page_url_from_meta else request.url
            )
            original_domain = urlparse(original_request_url_for_domain_check).netloc
            current_domain = urlparse(current_url).netloc

            if original_domain and current_domain and original_domain != current_domain:
                logger.warning(
                    f"⚠️ Selenium检测到跨域重定向。原始目标URL域名: {original_domain}, 当前URL域名: {current_domain}。丢弃响应。"
                )
                return HtmlResponse(
                    url=current_url,
                    status=999,  # 自定义状态码表示被中间件丢弃
                    body=b"",
                    encoding="utf-8",
                    request=request,
                )

            # 创建响应
            # 如果是模拟点击，响应的URL应该使用 meta 中存储的详情页URL，以确保后续处理的URL一致性
            response_url = (
                detail_page_url_from_meta if detail_page_url_from_meta else current_url
            )
            response = HtmlResponse(
                url=response_url, body=body, encoding="utf-8", request=request
            )

            logger.info(
                f"Selenium处理完成 ({site_name}): {request.url} -> {len(body)} bytes"
            )
            return response

        except Exception as e:
            logger.error(f"Selenium处理失败 ({site_name}) {request.url}: {e}")
            return HtmlResponse(
                url=request.url,
                status=500,  # 内部错误
                body=b"",
                encoding="utf-8",
                request=request,
                flags=[f"selenium_error: {str(e)}"],
            )

    def spider_closed(self, spider):
        """爬虫关闭时清理资源"""
        logger.info("清理Selenium资源...")

        # 遍历所有站点的所有驱动
        for (site_name, browser_type), drivers_list in self.drivers.items():
            for driver in drivers_list:
                try:
                    driver.quit()
                    logger.info(f"关闭WebDriver: {browser_type} for {site_name}")
                except Exception as e:
                    logger.error(f"关闭WebDriver失败 {browser_type} for {site_name}: {e}")

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
