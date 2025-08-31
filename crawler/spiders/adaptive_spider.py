# -*- coding: utf-8 -*-
"""
自适应爬虫

基于配置规则的通用爬虫，可以适应不同网站结构
"""

import scrapy
from scrapy.http import Request

from ..rule_engine import RuleEngine


class AdaptiveSpider(scrapy.Spider):
    """自适应爬虫 - 根据配置规则自动适应不同网站"""

    name = "adaptive"

    # 默认设置（类级别）
    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        "LOG_LEVEL": "INFO",
        "DOWNLOAD_DELAY": 1,
        "CONCURRENT_REQUESTS": 1,
    }

    def __init__(self, site=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # 初始化规则引擎
        self.rule_engine = RuleEngine()

        # 指定要爬取的网站
        self.target_site = site
        if not site:
            raise ValueError("必须指定要爬取的网站，例如: scrapy crawl adaptive -a site=nhc_new")

        # 加载网站规则
        self.site_rule = self.rule_engine.get_rule(site)
        if not self.site_rule:
            raise ValueError(f"未找到网站 '{site}' 的配置规则")

        self.logger.info(f"🎯 使用网站规则: {site}")
        site_info = self.site_rule.get("site_info", {})
        description = site_info.get("description", "无描述")
        self.logger.info(f"📋 网站描述: {description}")

        # 设置允许的域名
        self.allowed_domains = self.site_rule.get("allowed_domains", [])

        # 设置起始URL
        start_url_configs = self.site_rule.get("target_pages", [])
        self.start_urls = []
        for url_config in start_url_configs:
            if isinstance(url_config, dict):
                self.start_urls.append(url_config["url"])
            else:
                self.start_urls.append(url_config)

        self.logger.info(f"🚀 起始URL数量: {len(self.start_urls)}")

        # 动态更新设置
        self._update_settings_from_rule()

    def _update_settings_from_rule(self):
        """根据规则更新爬虫设置"""
        request_settings = self.site_rule.get("request_settings", {})

        # 更新类级别的custom_settings
        if "download_delay" in request_settings:
            self.custom_settings["DOWNLOAD_DELAY"] = request_settings["download_delay"]

        if "concurrent_requests" in request_settings:
            self.custom_settings["CONCURRENT_REQUESTS"] = request_settings[
                "concurrent_requests"
            ]

        if "user_agent" in request_settings:
            self.custom_settings["USER_AGENT"] = request_settings["user_agent"]

    async def start(self):
        """生成初始请求 - 新的异步方法"""
        for i, url in enumerate(self.start_urls):
            self.logger.info(f"📋 准备请求第{i+1}个URL: {url}")

            # 获取请求头设置
            headers = self.site_rule.get("request_settings", {}).get("headers", {})
            self.logger.info(f"📋 请求头设置: {headers}")
            yield Request(
                url=url,
                callback=self.parse,
                headers=headers,
                meta={
                    "site": self.target_site,
                    "url_index": i,
                    "page_type": "start_page",
                },
                errback=self.parse_error,  # 添加错误处理
            )

    def start_requests(self):
        """保持向后兼容的start_requests方法"""
        # 同步版本，用于兼容旧版本Scrapy
        for i, url in enumerate(self.start_urls):
            self.logger.info(f"📋 准备请求第{i+1}个URL: {url}")

            # 获取请求头设置
            headers = self.site_rule.get("request_settings", {}).get("headers", {})

            yield Request(
                url=url,
                callback=self.parse,
                headers=headers,
                meta={
                    "site": self.target_site,
                    "url_index": i,
                    "page_type": "start_page",
                },
                errback=self.parse_error,  # 添加错误处理
            )

    def parse(self, response):
        """解析响应"""
        self.logger.info(f"✅ 🎉 PARSE方法被调用! 解析页面: {response.url}")
        self.logger.info(f"📊 状态码: {response.status}")
        self.logger.info(f"📏 响应大小: {len(response.body)} 字节")
        self.logger.info(f"🎯 目标网站: {self.target_site}")

        # 检查Content-Type
        content_type = response.headers.get("Content-Type", b"").decode(
            "utf-8", errors="ignore"
        )
        self.logger.info(f"📋 Content-Type: {content_type}")

        # 检查响应状态
        if response.status >= 400:
            self.logger.error(f"❌ HTTP错误状态码: {response.status}")
            yield {
                "url": response.url,
                "status": response.status,
                "error": f"HTTP {response.status}",
                "spider_name": self.name,
                "site": self.target_site,
                "content_type": content_type,
            }
            return

        # 检查是否为文本内容
        if not self._is_text_response(response):
            self.logger.warning(f"⚠️ 非文本响应，跳过处理: {response.url}")
            self.logger.warning(f"⚠️ Content-Type: {content_type}")
            yield {
                "url": response.url,
                "status": "non_text_content",
                "error": f"Non-text content: {content_type}",
                "spider_name": self.name,
                "site": self.target_site,
                "content_type": content_type,
            }
            return

        # 调试：显示页面内容片段
        try:
            content_preview = response.text[:300] if response.text else "无内容"
            self.logger.info(f"📄 页面内容预览: {content_preview}...")
        except Exception as e:
            self.logger.error(f"❌ 无法获取文本内容: {e}")
            yield {
                "url": response.url,
                "status": "text_decode_error",
                "error": f"Text decode error: {e}",
                "spider_name": self.name,
                "site": self.target_site,
            }
            return

        # 智能判断页面类型
        page_type = self._detect_page_type(response)
        self.logger.info(f"🔍 检测到页面类型: {page_type}")

        # 根据配置文件的字段规则进行数据提取
        try:
            self.logger.info("🔧 开始根据配置文件提取数据...")

            # 使用配置文件中的fields规则进行提取
            data = self._extract_data_by_config(response, page_type)

            # 添加元数据
            data["spider_name"] = self.name
            data["site"] = self.target_site
            data["page_type"] = page_type
            data["content_type"] = content_type

            self.logger.info("📄 提取数据字段: %s", list(data.keys()))

            # 调试：显示提取的关键数据
            if data.get("title"):
                self.logger.info(f"📝 提取到标题: {data['title']}")
            else:
                self.logger.warning("⚠️ 未提取到标题")

            if data.get("content"):
                content_length = len(str(data["content"]))
                self.logger.info("📝 提取到内容: %s 字符", content_length)
            else:
                self.logger.warning("⚠️ 未提取到内容")

            self.logger.info("📤 准备yield数据项")
            yield data

        except Exception as e:
            self.logger.error(f"❌ 数据提取失败: {e}")
            import traceback

            self.logger.error(f"❌ 错误详情: {traceback.format_exc()}")
            yield {
                "url": response.url,
                "error": str(e),
                "status": "extraction_failed",
                "spider_name": self.name,
                "site": self.target_site,
                "content_type": content_type,
            }

        # 提取并跟进链接
        try:
            links = self.rule_engine.get_links(response, self.site_rule)
            self.logger.info(f"🔗 找到 {len(links)} 个链接")

            for link_info in links:
                url = link_info["url"]
                link_type = link_info["type"]

                # 检查是否应该跟进
                if self.rule_engine.should_follow(url, self.site_rule):
                    self.logger.debug(f"🔄 跟进链接 ({link_type}): {url}")

                    yield Request(
                        url=url,
                        callback=self.parse,
                        meta={
                            "site": self.target_site,
                            "link_type": link_type,
                            "page_type": "followed_page",
                            "source_url": response.url,
                        },
                        errback=self.parse_error,
                    )
                else:
                    self.logger.debug(f"⏭️ 跳过链接: {url}")

        except Exception as e:
            self.logger.error(f"❌ 链接提取失败: {e}")

    def _is_text_response(self, response):
        """检查响应是否为文本内容"""
        content_type = (
            response.headers.get("Content-Type", b"")
            .decode("utf-8", errors="ignore")
            .lower()
        )

        # 检查是否为文本类型
        text_types = [
            "text/html",
            "text/plain",
            "text/xml",
            "application/xml",
            "application/xhtml+xml",
            "application/json",
        ]

        for text_type in text_types:
            if text_type in content_type:
                return True

        # 如果没有Content-Type头，尝试检查内容
        if not content_type:
            try:
                # 尝试解码前100字节
                sample = response.body[:100].decode("utf-8", errors="ignore")
                # 检查是否包含HTML标签
                return "<" in sample and ">" in sample
            except Exception:
                return False

        return False

    def _detect_page_type(self, response):
        """智能检测页面类型"""
        try:
            # 获取页面文本内容
            url = response.url.lower()

            # 1. 根据URL路径判断
            if any(
                keyword in url for keyword in ["list", "index", "category", "列表", "目录"]
            ):
                # 进一步检查是否真的是列表页
                if self._is_list_page_content(response):
                    return "list_page"

            if any(
                keyword in url
                for keyword in [
                    "detail",
                    "article",
                    "news",
                    "content",
                    "详情",
                    "新闻",
                    "文章",
                ]
            ):
                # 进一步检查是否真的是详情页
                if self._is_detail_page_content(response):
                    return "detail_page"

            # 2. 根据页面内容特征判断
            if self._is_list_page_content(response):
                return "list_page"
            elif self._is_detail_page_content(response):
                return "detail_page"

            # 3. 默认判断
            return "unknown_page"

        except Exception as e:
            self.logger.warning(f"⚠️ 页面类型检测失败: {e}")
            return "unknown_page"

    def _is_list_page_content(self, response):
        """检测是否为列表页内容"""
        try:
            # 检查是否有多个链接项
            links = response.css("a::attr(href)").getall()
            if len(links) < 5:  # 列表页通常有多个链接
                return False

            # 检查是否有列表相关的HTML结构
            list_indicators = [
                "ul li a",  # 无序列表中的链接
                "ol li a",  # 有序列表中的链接
                ".list",  # 包含list类名的元素
                ".news-list",
                ".content_list",
                '[class*="list"]',
            ]

            for indicator in list_indicators:
                elements = response.css(indicator)
                if len(elements) >= 3:  # 至少3个列表项
                    self.logger.debug(f"🔍 发现列表结构: {indicator} ({len(elements)}个)")
                    return True

            # 检查是否有日期模式（列表页常有发布日期）
            date_patterns = response.css("*::text()").re(
                r"\d{4}[-/年]\d{1,2}[-/月]\d{1,2}[日]?"
            )
            if len(date_patterns) >= 3:  # 多个日期表示可能是列表页
                self.logger.debug(f"🔍 发现多个日期模式: {len(date_patterns)}个")
                return True

            return False

        except Exception as e:
            self.logger.warning(f"⚠️ 列表页检测失败: {e}")
            return False

    def _is_detail_page_content(self, response):
        """检测是否为详情页内容"""
        try:
            # 检查是否有详情页的典型结构
            detail_indicators = [
                "article",  # HTML5 article标签
                ".article",
                ".content",
                ".detail",
                ".news-content",
                '[class*="content"]',
                '[class*="article"]',
                '[class*="detail"]',
            ]

            for indicator in detail_indicators:
                elements = response.css(indicator)
                if elements:
                    # 检查内容长度
                    content_text = " ".join(elements.css("*::text()").getall())
                    if len(content_text) > 200:  # 详情页通常有较长内容
                        self.logger.debug(
                            f"🔍 发现详情页结构: {indicator} (内容长度: {len(content_text)})"
                        )
                        return True

            # 检查页面文本总长度
            all_text = " ".join(response.css("*::text()").getall())
            if len(all_text) > 1000:  # 详情页通常内容较多
                # 检查是否有标题
                title_selectors = [
                    "h1",
                    "h2",
                    ".title",
                    ".headline",
                    '[class*="title"]',
                ]
                for selector in title_selectors:
                    if response.css(selector):
                        self.logger.debug(f"🔍 发现详情页特征: 长内容({len(all_text)}) + 标题结构")
                        return True

            return False

        except Exception as e:
            self.logger.warning(f"⚠️ 详情页检测失败: {e}")
            return False

    def _extract_data_by_config(self, response, page_type):
        """根据配置文件的fields规则提取数据"""
        data = {"url": response.url}

        # 获取配置文件中的字段规则
        fields_config = self.site_rule.get("fields", {})
        if not fields_config:
            self.logger.warning("⚠️ 配置文件中未找到fields规则，使用默认提取方法")
            return self._extract_default_data(response, page_type)

        self.logger.info(f"📋 使用配置文件提取 {len(fields_config)} 个字段")

        # 遍历每个字段配置进行提取
        for field_name, field_config in fields_config.items():
            try:
                value = self._extract_field_by_config(
                    response, field_name, field_config, page_type
                )
                if value is not None:
                    data[field_name] = value
                    self.logger.debug(f"✅ 字段 {field_name}: {str(value)[:100]}...")
                else:
                    self.logger.debug(f"⚠️ 字段 {field_name}: 未提取到值")

            except Exception as e:
                self.logger.error(f"❌ 提取字段 {field_name} 失败: {e}")
                data[f"{field_name}_error"] = str(e)

        self.logger.info(
            f"📊 配置文件提取完成: {len([k for k in data.keys() if not k.endswith('_error')])} 个字段成功"
        )
        return data

    def _extract_field_by_config(self, response, field_name, field_config, page_type):
        """根据字段配置提取单个字段"""
        method = field_config.get("method", "xpath")
        selector = field_config.get("selector", "")
        field_type = field_config.get("type", "string")
        multiple = field_config.get("multiple", False)
        required = field_config.get("required", False)

        if not selector:
            if required:
                self.logger.warning(f"⚠️ 必需字段 {field_name} 缺少选择器")
            return None

        try:
            # 根据方法类型进行提取
            if method == "xpath":
                if multiple:
                    values = response.xpath(selector).getall()
                else:
                    values = response.xpath(selector).get()
            elif method == "css":
                if multiple:
                    values = response.css(selector).getall()
                else:
                    values = response.css(selector).get()
            elif method == "regex":
                import re

                text_content = response.text
                if multiple:
                    values = re.findall(selector, text_content)
                else:
                    match = re.search(selector, text_content)
                    values = match.group(1) if match else None
            else:
                self.logger.warning(f"⚠️ 不支持的提取方法: {method}")
                return None

            # 数据清洗和类型转换
            if values:
                cleaned_values = self._clean_extracted_values(
                    values, field_type, multiple
                )

                # 特殊处理：根据页面类型调整提取策略
                if page_type == "list_page" and field_name in [
                    "news_links",
                    "news_titles",
                    "publish_dates",
                ]:
                    # 列表页的特殊处理
                    cleaned_values = self._process_list_page_field(
                        response, field_name, cleaned_values
                    )
                elif page_type == "detail_page" and field_name in ["content", "title"]:
                    # 详情页的特殊处理
                    cleaned_values = self._process_detail_page_field(
                        response, field_name, cleaned_values
                    )

                return cleaned_values
            else:
                if required:
                    self.logger.warning(f"⚠️ 必需字段 {field_name} 未提取到值")
                return None

        except Exception as e:
            self.logger.error(f"❌ 字段 {field_name} 提取异常: {e}")
            return None

    def _clean_extracted_values(self, values, field_type, multiple):
        """清洗提取的值"""
        if multiple and isinstance(values, list):
            # 清洗列表中的每个值
            cleaned = []
            for value in values:
                if isinstance(value, str):
                    cleaned_value = value.strip()
                    if cleaned_value:  # 只保留非空值
                        cleaned.append(cleaned_value)
                else:
                    cleaned.append(value)
            return cleaned
        else:
            # 清洗单个值
            if isinstance(values, str):
                cleaned = values.strip()

                # 根据字段类型进行转换
                if field_type == "integer":
                    try:
                        return int(cleaned) if cleaned else None
                    except ValueError:
                        return None
                elif field_type == "date":
                    # 简单的日期清洗
                    import re

                    date_match = re.search(
                        r"\d{4}[-/年]\d{1,2}[-/月]\d{1,2}[日]?", cleaned
                    )
                    return date_match.group(0) if date_match else cleaned

                return cleaned if cleaned else None
            else:
                return values

    def _process_list_page_field(self, response, field_name, values):
        """处理列表页特定字段"""
        if field_name == "news_links" and values:
            # 转换为绝对URL
            absolute_urls = []
            for url in values:
                if url:
                    absolute_url = response.urljoin(url)
                    absolute_urls.append(absolute_url)
            return absolute_urls

        return values

    def _process_detail_page_field(self, response, field_name, values):
        """处理详情页特定字段"""
        if field_name == "content" and isinstance(values, list):
            # 将多个段落合并为完整内容
            content_text = " ".join([text.strip() for text in values if text.strip()])
            return content_text if content_text else None

        return values

    def _extract_default_data(self, response, page_type):
        """当配置文件缺失时的默认提取方法"""
        if page_type == "list_page":
            return self._extract_list_page_data(response)
        elif page_type == "detail_page":
            return self._extract_detail_page_data(response)
        else:
            # 使用规则引擎
            return self.rule_engine.extract_data(response, self.site_rule)

    def _extract_list_page_data(self, response):
        """提取列表页数据"""
        data = {"url": response.url}

        try:
            # 提取页面标题
            title_selectors = ["title", "h1", "h2", ".title", ".page-title"]
            for selector in title_selectors:
                title_elements = response.css(f"{selector}::text()").getall()
                if title_elements:
                    data["title"] = title_elements[0].strip()
                    break

            # 提取新闻列表
            news_items = []

            # 尝试多种列表选择器
            list_selectors = [
                "ul.content_list li",  # 北京疾控中心的结构
                "ul li a",
                ".news-list li",
                ".list-item",
                '[class*="list"] li',
            ]

            for selector in list_selectors:
                items = response.css(selector)
                if len(items) >= 3:  # 至少3个项目才认为是有效列表
                    self.logger.info(f"📋 使用选择器提取列表: {selector} ({len(items)}项)")

                    for i, item in enumerate(items[:20]):  # 最多提取20项
                        try:
                            # 提取链接
                            link = item.css("a::attr(href)").get()
                            if link:
                                link = response.urljoin(link)

                            # 提取标题
                            title = item.css("a::text()").get()
                            if title:
                                title = title.strip()

                            # 提取日期
                            date = item.css("span::text()").get()
                            if date:
                                date = date.strip()

                            if link and title:
                                news_items.append(
                                    {
                                        "title": title,
                                        "url": link,
                                        "date": date,
                                        "index": i + 1,
                                    }
                                )
                        except Exception as e:
                            self.logger.warning(f"⚠️ 提取第{i+1}项失败: {e}")
                            continue

                    if news_items:
                        break

            data["news_items"] = news_items
            data["news_count"] = len(news_items)

            # 提取页面内容（如果有的话）
            content_selectors = [".content", ".main-content", "#content"]
            for selector in content_selectors:
                content_elements = response.css(f"{selector} *::text()").getall()
                if content_elements:
                    data["content"] = " ".join(
                        [text.strip() for text in content_elements if text.strip()]
                    )
                    break

            self.logger.info(
                f"📋 列表页提取完成: 标题={data.get('title', '无')}, 新闻数={len(news_items)}"
            )
            return data

        except Exception as e:
            self.logger.error(f"❌ 列表页数据提取失败: {e}")
            return {"url": response.url, "error": f"List page extraction failed: {e}"}

    def _extract_detail_page_data(self, response):
        """提取详情页数据"""
        data = {"url": response.url}

        try:
            # 提取标题
            title_selectors = [
                "h1",
                "h2",
                ".title",
                ".article-title",
                ".news-title",
                "title",
            ]
            for selector in title_selectors:
                title_elements = response.css(f"{selector}::text()").getall()
                if title_elements:
                    data["title"] = title_elements[0].strip()
                    break

            # 提取内容
            content_selectors = [
                ".article-content",
                ".content",
                ".news-content",
                ".detail-content",
                "article",
                "#content",
                ".main-content",
            ]

            for selector in content_selectors:
                content_elements = response.css(f"{selector} p::text()").getall()
                if not content_elements:
                    content_elements = response.css(f"{selector} *::text()").getall()

                if content_elements:
                    content_text = " ".join(
                        [text.strip() for text in content_elements if text.strip()]
                    )
                    if len(content_text) > 50:  # 确保内容足够长
                        data["content"] = content_text
                        break

            # 提取发布日期
            date_selectors = [".date", ".time", ".publish-time", ".create-time"]
            for selector in date_selectors:
                date_elements = response.css(f"{selector}::text()").getall()
                if date_elements:
                    data["publish_date"] = date_elements[0].strip()
                    break

            # 提取作者
            author_selectors = [".author", ".writer", ".source"]
            for selector in author_selectors:
                author_elements = response.css(f"{selector}::text()").getall()
                if author_elements:
                    data["author"] = author_elements[0].strip()
                    break

            self.logger.info(
                f"📄 详情页提取完成: 标题={data.get('title', '无')}, 内容长度={len(data.get('content', ''))}"
            )
            return data

        except Exception as e:
            self.logger.error(f"❌ 详情页数据提取失败: {e}")
            return {"url": response.url, "error": f"Detail page extraction failed: {e}"}

    def parse_error(self, failure):
        """处理请求错误"""
        self.logger.error(f"❌ 请求失败: {failure.request.url}")
        self.logger.error(f"❌ 错误类型: {type(failure.value).__name__}")
        self.logger.error(f"❌ 错误信息: {failure.value}")

        # 检查是否为特定的Scrapy错误
        error_type = type(failure.value).__name__
        error_message = str(failure.value)

        # 特殊处理"Response content isn't text"错误
        if "Response content isn't text" in error_message:
            self.logger.warning("⚠️ 检测到非文本响应错误，可能是网站返回了二进制内容")
            yield {
                "url": failure.request.url,
                "error": "Non-text response detected",
                "error_type": "content_type_error",
                "status": "non_text_content",
                "spider_name": self.name,
                "site": self.target_site,
                "suggestion": "检查URL是否正确，或网站是否返回了图片/PDF等二进制文件",
            }
        else:
            yield {
                "url": failure.request.url,
                "error": error_message,
                "error_type": error_type,
                "status": "request_failed",
                "spider_name": self.name,
                "site": self.target_site,
            }

    def closed(self, reason):
        """爬虫关闭时的处理"""
        self.logger.info("🏁 自适应爬虫完成")
        self.logger.info(f"📊 关闭原因: {reason}")
        self.logger.info(f"🎯 目标网站: {self.target_site}")

        # 输出统计信息
        stats = self.crawler.stats.get_stats()
        self.logger.info("📈 爬虫统计信息:")

        key_stats = [
            "item_scraped_count",
            "response_received_count",
            "request_count",
            "downloader/response_status_count/200",
            "downloader/response_status_count/404",
            "elapsed_time_seconds",
        ]

        for key in key_stats:
            if key in stats:
                self.logger.info(f"   {key}: {stats[key]}")


class MultiSiteSpider(scrapy.Spider):
    """多站点爬虫 - 可以同时爬取多个网站"""

    name = "multisite"

    def __init__(self, sites=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # 初始化规则引擎
        self.rule_engine = RuleEngine()

        # 解析要爬取的网站列表
        if not sites:
            # 默认爬取所有配置的网站
            self.target_sites = list(self.rule_engine.rules.keys())
        else:
            self.target_sites = sites.split(",")

        self.logger.info(f"🎯 目标网站: {self.target_sites}")

        # 收集所有起始URL
        self.start_urls = []
        self.allowed_domains = []

        for site in self.target_sites:
            rule = self.rule_engine.get_rule(site)
            if rule:
                # 添加域名
                domains = rule.get("allowed_domains", [])
                self.allowed_domains.extend(domains)

                # 添加起始URL
                start_url_configs = rule.get("start_urls", [])
                for url_config in start_url_configs:
                    if isinstance(url_config, dict):
                        self.start_urls.append(url_config["url"])
                    else:
                        self.start_urls.append(url_config)

        self.logger.info(f"🚀 总起始URL数量: {len(self.start_urls)}")
        self.logger.info(f"🌐 允许域名: {self.allowed_domains}")

    def parse(self, response):
        """解析响应 - 自动识别网站类型"""
        # 根据URL匹配网站规则
        site_name = self.rule_engine.match_site(response.url)

        if not site_name:
            self.logger.warning(f"⚠️ 未找到匹配的网站规则: {response.url}")
            return

        self.logger.info(f"🎯 识别网站: {site_name} - {response.url}")

        # 使用对应的规则处理
        rule = self.rule_engine.get_rule(site_name)

        # 提取数据
        data = self.rule_engine.extract_data(response, rule)
        data["spider_name"] = self.name
        data["site"] = site_name
        data["auto_detected"] = True

        yield data

        # 提取并跟进链接
        links = self.rule_engine.get_links(response, rule)
        for link_info in links:
            url = link_info["url"]
            if self.rule_engine.should_follow(url, rule):
                yield response.follow(url, callback=self.parse)
