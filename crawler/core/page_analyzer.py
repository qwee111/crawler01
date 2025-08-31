"""
页面分析器

分析页面类型、结构和特征
"""

import logging
import re
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class PageAnalyzer:
    """页面分析器"""

    def __init__(self, config_manager):
        self.config_manager = config_manager

    def analyze_page(self, response, site_name: str) -> Dict:
        """分析页面并返回分析结果"""
        analysis = {
            "url": response.url,
            "site_name": site_name,
            "page_type": self._detect_page_type(response, site_name),
            "content_features": self._analyze_content_features(response),
            "structure_info": self._analyze_page_structure(response),
        }

        logger.info(f"📊 页面分析完成: {analysis['page_type']} | {response.url}")
        return analysis

    def _detect_page_type(self, response, site_name: str) -> str:
        """检测页面类型"""
        # 获取检测配置
        detection_config = self.config_manager.get_detection_config(site_name)

        if not detection_config:
            return self._generic_page_type_detection(response)

        # 使用配置化检测
        page_types = detection_config.get("page_types", {})

        for page_type, rules in page_types.items():
            if self._match_page_type_rules(response, rules):
                logger.debug(f"🔍 匹配页面类型: {page_type}")
                return page_type

        # 回退到通用检测
        return self._generic_page_type_detection(response)

    def _match_page_type_rules(self, response, rules: Dict) -> bool:
        """匹配页面类型规则"""
        try:
            # URL模式匹配
            url_patterns = rules.get("url_patterns", [])
            if url_patterns:
                url = response.url.lower()
                for pattern in url_patterns:
                    if re.search(pattern, url):
                        logger.debug(f"✅ URL模式匹配: {pattern}")
                        return True

            # 内容特征匹配
            content_features = rules.get("content_features", {})
            if content_features:
                if self._match_content_features(response, content_features):
                    logger.debug("✅ 内容特征匹配")
                    return True

            # 结构特征匹配
            structure_features = rules.get("structure_features", {})
            if structure_features:
                if self._match_structure_features(response, structure_features):
                    logger.debug("✅ 结构特征匹配")
                    return True

            return False

        except Exception as e:
            logger.error(f"页面类型规则匹配失败: {e}")
            return False

    def _match_content_features(self, response, features: Dict) -> bool:
        """匹配内容特征"""
        # 检查最小链接数
        min_links = features.get("min_links", 0)
        if min_links > 0:
            links = response.css("a::attr(href)").getall()
            if len(links) < min_links:
                return False

        # 检查关键词
        keywords = features.get("keywords", [])
        if keywords:
            text_content = response.text.lower()
            for keyword in keywords:
                if keyword.lower() not in text_content:
                    return False

        # 检查最小内容长度
        min_content_length = features.get("min_content_length", 0)
        if min_content_length > 0:
            if len(response.text) < min_content_length:
                return False

        return True

    def _match_structure_features(self, response, features: Dict) -> bool:
        """匹配结构特征"""
        # 检查必需的选择器
        required_selectors = features.get("required_selectors", [])
        for selector in required_selectors:
            # 判断是XPath还是CSS选择器
            if selector.startswith("/") or selector.startswith("./"):
                # XPath选择器
                if not response.xpath(selector):
                    return False
            else:
                # CSS选择器
                if not response.css(selector):
                    return False

        # 检查最小元素数量
        min_elements = features.get("min_elements", {})
        for selector, min_count in min_elements.items():
            # 判断是XPath还是CSS选择器
            if selector.startswith("/") or selector.startswith("./"):
                # XPath选择器
                elements = response.xpath(selector)
            else:
                # CSS选择器
                elements = response.css(selector)

            if len(elements) < min_count:
                return False

        return True

    def _generic_page_type_detection(self, response) -> str:
        """通用页面类型检测"""
        url = response.url.lower()

        # URL关键词检测
        if any(keyword in url for keyword in ["list", "index", "category", "列表"]):
            if self._is_list_page_content(response):
                return "list_page"

        if any(keyword in url for keyword in ["detail", "article", "news", "详情", "新闻"]):
            if self._is_detail_page_content(response):
                return "detail_page"

        # 内容特征检测
        if self._is_list_page_content(response):
            return "list_page"
        elif self._is_detail_page_content(response):
            return "detail_page"

        return "unknown_page"

    def _is_list_page_content(self, response) -> bool:
        """检测是否为列表页内容"""
        try:
            # 检查链接数量
            links = response.css("a::attr(href)").getall()
            if len(links) < 5:
                return False

            # 检查列表结构
            list_selectors = ["ul li a", "ol li a", ".list", '[class*="list"]']
            for selector in list_selectors:
                elements = response.css(selector)
                if len(elements) >= 3:
                    return True

            # 检查日期模式
            date_patterns = response.css("*::text()").re(
                r"\d{4}[-/年]\d{1,2}[-/月]\d{1,2}[日]?"
            )
            if len(date_patterns) >= 3:
                return True

            return False

        except Exception:
            return False

    def _is_detail_page_content(self, response) -> bool:
        """检测是否为详情页内容"""
        try:
            # 检查详情页结构
            detail_selectors = [
                "article",
                ".article",
                ".content",
                ".detail",
                '[class*="content"]',
            ]
            for selector in detail_selectors:
                elements = response.css(selector)
                if elements:
                    content_text = " ".join(elements.css("*::text()").getall())
                    if len(content_text) > 200:
                        return True

            # 检查内容长度
            all_text = " ".join(response.css("*::text()").getall())
            if len(all_text) > 1000:
                # 检查标题
                title_selectors = ["h1", "h2", ".title", '[class*="title"]']
                for selector in title_selectors:
                    if response.css(selector):
                        return True

            return False

        except Exception:
            return False

    def _analyze_content_features(self, response) -> Dict:
        """分析内容特征"""
        try:
            text_content = response.text
            return {
                "total_length": len(text_content),
                "link_count": len(response.css("a::attr(href)").getall()),
                "image_count": len(response.css("img::attr(src)").getall()),
                "paragraph_count": len(response.css("p").getall()),
                "has_forms": bool(response.css("form")),
                "has_tables": bool(response.css("table")),
            }
        except Exception as e:
            logger.error(f"内容特征分析失败: {e}")
            return {}

    def _analyze_page_structure(self, response) -> Dict:
        """分析页面结构"""
        try:
            return {
                "has_navigation": bool(response.css("nav, .nav, .navigation")),
                "has_sidebar": bool(response.css(".sidebar, .side, aside")),
                "has_footer": bool(response.css("footer, .footer")),
                "has_header": bool(response.css("header, .header")),
                "main_content_selector": self._find_main_content_selector(response),
            }
        except Exception as e:
            logger.error(f"页面结构分析失败: {e}")
            return {}

    def _find_main_content_selector(self, response) -> Optional[str]:
        """查找主要内容选择器"""
        main_selectors = [
            "main",
            ".main",
            "#main",
            ".content",
            "#content",
            ".container",
            "#container",
            "article",
            ".article",
        ]

        for selector in main_selectors:
            if response.css(selector):
                return selector

        return None
