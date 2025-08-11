# -*- coding: utf-8 -*-
"""
增强的数据处理管道

集成第三阶段的所有数据处理功能
"""

import logging
from typing import Any, Dict, Optional

from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem

from .cleaner import CleaningPipeline
from .extractor import ExtractionConfigManager
from .quality_assessor import QualityMonitor
from .validator import ValidationPipeline

logger = logging.getLogger(__name__)


class EnhancedExtractionPipeline:
    """增强数据提取管道"""

    def __init__(self, config_dir=None):
        # 使用新的配置管理器
        from .extractor import ExtractionConfigManager

        self.extraction_manager = ExtractionConfigManager()
        self.config_dir = config_dir

        self.stats = {
            "total_processed": 0,
            "extraction_success": 0,
            "extraction_failed": 0,
            "items_dropped": 0,
        }

        logger.info("🔧 EnhancedExtractionPipeline 初始化完成")

    @classmethod
    def from_crawler(cls, crawler):
        config_dir = crawler.settings.get("EXTRACTION_CONFIG_DIR", "config/extraction")
        return cls(config_dir)

    def process_item(self, item, spider):
        """处理数据项"""
        self.stats["total_processed"] += 1
        adapter = ItemAdapter(item)

        logger.info(
            f"🔧 EnhancedExtractionPipeline 处理数据项: {adapter.get('url', 'unknown')}"
        )

        try:
            # 获取网站名称
            site_name = adapter.get("site") or getattr(
                spider, "target_site", spider.name
            )
            logger.info(f"🎯 使用网站配置: {site_name}")

            # 仅对详情页执行增强提取，列表页直接跳过（避免空内容造成的误判）
            page_type = (
                adapter.get("page_type")
                or (adapter.get("page_analysis") or {}).get("page_type")
                or self._infer_page_type(adapter)
            )
            if page_type != "detail_page":
                logger.info(f"⏭️ 非详情页，跳过增强提取: {page_type}")
                # 仅跳过增强，不丢弃 item
                self.stats["items_skipped"] = self.stats.get("items_skipped", 0) + 1
                return item

            # 创建模拟响应对象（带安全回退策略）
            response = self._create_response_from_item(adapter)
            logger.info(f"📄 创建模拟响应对象，内容长度: {len(response.text)}")

            # 若内容仍为空，则放弃增强提取，避免覆盖已有字段
            if not response.text:
                logger.info("⏭️ 模拟响应无内容，跳过增强提取")
                return item

            # 使用配置化提取器重新提取数据
            extracted_data = self.extraction_manager.extract_data(response, site_name)
            logger.info(f"📊 配置化提取结果: {len(extracted_data)} 个字段")

            # 合并提取的数据（仅更新非空值，并做字段名兼容映射）
            updated_fields = 0
            for key, value in extracted_data.items():
                if value is not None:
                    adapter[key] = value
                    updated_fields += 1

            # 兼容映射：article_title/article_content → title/content
            if (
                adapter.get("title") is None
                and adapter.get("article_title") is not None
            ):
                adapter["title"] = adapter.get("article_title")
                updated_fields += 1
            content_val = adapter.get("content") or adapter.get("article_content")
            if isinstance(content_val, list):
                content_val = " ".join(
                    [str(x).strip() for x in content_val if str(x).strip()]
                )
            if adapter.get("content") is None and content_val:
                adapter["content"] = content_val
                updated_fields += 1

            # 更新内容统计（避免与 DataEnrichmentPipeline 重复）
            if adapter.get("content"):
                try:
                    txt = adapter.get("content")
                    if "content_length" not in adapter:
                        adapter["content_length"] = len(txt)
                    # 统计中文字符数
                    import re

                    if "chinese_char_count" not in adapter:
                        adapter["chinese_char_count"] = len(
                            re.findall(r"[\u4e00-\u9fff]", txt)
                        )
                except Exception:
                    pass

            logger.info(f"📝 更新了 {updated_fields} 个字段")

            # 添加提取元数据
            adapter["_extraction_metadata"] = {
                "extractor_used": site_name,
                "extraction_method": "enhanced",
                "fields_extracted": len(extracted_data),
            }

            self.stats["extraction_success"] += 1
            logger.info(f"✅ 增强提取成功: {adapter.get('url', 'unknown')}")

            return item

        except Exception as e:
            self.stats["extraction_failed"] += 1
            logger.error(f"❌ 增强提取失败: {e}")
            import traceback

            logger.error(f"❌ 错误详情: {traceback.format_exc()}")

            # 保留原始数据，添加错误信息
            adapter["_extraction_error"] = str(e)
            return item

    def _create_response_from_item(self, adapter: ItemAdapter):
        """从数据项创建响应对象（带安全回退）"""

        class MockResponse:
            def __init__(self, url, text, status=200):
                self.url = url
                self.text = text
                self.status = status
                self.body = text.encode("utf-8") if text else b""
                self.headers = {}

            def json(self):
                import json

                return json.loads(self.text)

        url = adapter.get("url", "")
        # 优先顺序：raw_html > content > article_content > full_content > text
        candidates = [
            adapter.get("raw_html"),
            adapter.get("content"),
            adapter.get("article_content"),
            adapter.get("full_content"),
            adapter.get("text"),
        ]
        content = None
        for val in candidates:
            if val:
                content = val
                break

        # 列表/多段内容合并
        if isinstance(content, list):
            content = " ".join([str(c).strip() for c in content if str(c).strip()])

        status = adapter.get("status") or adapter.get("status_code") or 200

        return MockResponse(url, str(content or ""), status)

    def _infer_page_type(self, adapter: ItemAdapter) -> str:
        """根据URL与字段启发式判断页面类型"""
        try:
            url = (adapter.get("url") or "").lower()
            # 明显的URL模式
            if any(kw in url for kw in ["list", "index", "category", "列表"]):
                return "list_page"
            import re

            if re.search(r"/\d+\.s?html$", url) or any(
                kw in url for kw in ["detail", "article"]
            ):
                return "detail_page"
            # 根据字段特征
            items_val = adapter.get("items")
            if items_val:
                if isinstance(items_val, list) and len(items_val) >= 3:
                    return "list_page"
                if (
                    isinstance(items_val, str)
                    and "[" in items_val
                    and "title" in items_val
                ):
                    return "list_page"
            content_val = adapter.get("content") or adapter.get("article_content")
            if content_val:
                if isinstance(content_val, str) and len(content_val.strip()) > 50:
                    return "detail_page"
                if (
                    isinstance(content_val, list)
                    and len(" ".join(map(str, content_val))) > 50
                ):
                    return "detail_page"
        except Exception:
            pass
        return "unknown_page"

    def get_stats(self) -> Dict[str, int]:
        """获取统计信息"""
        return self.stats.copy()


class ComprehensiveDataPipeline:
    """综合数据处理管道"""

    def __init__(self, config=None):
        self.config = config or {}

        # 初始化子管道
        try:
            from .cleaner import CleaningPipeline
            from .quality_assessor import QualityMonitor
            from .validator import ValidationPipeline

            self.cleaning_pipeline = CleaningPipeline()
            self.validation_pipeline = ValidationPipeline()
            self.quality_monitor = QualityMonitor()

            logger.info("✅ 所有子管道初始化成功")

        except ImportError as e:
            logger.warning(f"⚠️ 部分子管道初始化失败: {e}")
            # 创建简化版本
            self.cleaning_pipeline = None
            self.validation_pipeline = None
            self.quality_monitor = None

        self.stats = {
            "total_processed": 0,
            "cleaning_success": 0,
            "validation_success": 0,
            "quality_assessed": 0,
            "items_dropped": 0,
        }

        logger.info("🔧 ComprehensiveDataPipeline 初始化完成")

    @classmethod
    def from_crawler(cls, crawler):
        config = {
            "enable_cleaning": crawler.settings.getbool("ENABLE_DATA_CLEANING", True),
            "enable_validation": crawler.settings.getbool(
                "ENABLE_DATA_VALIDATION", True
            ),
            "enable_quality_assessment": crawler.settings.getbool(
                "ENABLE_QUALITY_ASSESSMENT", True
            ),
            "drop_invalid_items": crawler.settings.getbool("DROP_INVALID_ITEMS", False),
            "min_quality_score": crawler.settings.getfloat("MIN_QUALITY_SCORE", 0.0),
        }
        return cls(config)

    def process_item(self, item, spider):
        """处理数据项"""
        self.stats["total_processed"] += 1
        adapter = ItemAdapter(item)

        try:
            # 1. 数据清洗
            if self.config.get("enable_cleaning", True) and self.cleaning_pipeline:
                item = self.cleaning_pipeline.process_item(item, spider)
                adapter = ItemAdapter(item)
                self.stats["cleaning_success"] += 1
                logger.debug(f"数据清洗完成: {adapter.get('url', 'unknown')}")

            # 2. 数据验证
            if self.config.get("enable_validation", True) and self.validation_pipeline:
                item = self.validation_pipeline.process_item(item, spider)
                adapter = ItemAdapter(item)
                validation_result = adapter.get("_validation", {})

                if validation_result.get("is_valid", True):
                    self.stats["validation_success"] += 1
                else:
                    logger.warning(f"数据验证失败: {validation_result.get('errors', {})}")

                    # 如果配置为丢弃无效数据
                    if self.config.get("drop_invalid_items", False):
                        self.stats["items_dropped"] += 1
                        raise DropItem(f"数据验证失败: {validation_result.get('errors', {})}")

            # 3. 质量评估
            if (
                self.config.get("enable_quality_assessment", True)
                and self.quality_monitor
            ):
                item = self.quality_monitor.monitor_item(item)
                adapter = ItemAdapter(item)
                quality_report = adapter.get("_quality_report", {})
                quality_score = quality_report.get("overall_score", 0.0)

                self.stats["quality_assessed"] += 1

                # 检查最低质量要求
                min_quality_score = self.config.get("min_quality_score", 0.0)
                if quality_score < min_quality_score:
                    self.stats["items_dropped"] += 1
                    raise DropItem(f"数据质量过低: {quality_score} < {min_quality_score}")

                logger.debug(f"质量评估完成: {quality_score}")

            # 4. 添加处理元数据
            adapter["_processing_metadata"] = {
                "processed_at": self._get_current_time(),
                "pipeline_version": "3.0",
                "processing_stages": self._get_processing_stages(),
            }

            return item

        except DropItem:
            raise
        except Exception as e:
            logger.error(f"数据处理失败: {e}")
            return item

    def _get_processing_stages(self):
        """获取处理阶段信息"""
        stages = []
        if self.config.get("enable_cleaning", True):
            stages.append("cleaning")
        if self.config.get("enable_validation", True):
            stages.append("validation")
        if self.config.get("enable_quality_assessment", True):
            stages.append("quality_assessment")
        return stages

    def _get_current_time(self):
        """获取当前时间"""
        from datetime import datetime

        return datetime.now().isoformat()

    def close_spider(self, spider):
        """爬虫关闭时的清理工作"""
        logger.info("📊 ComprehensiveDataPipeline 统计信息:")
        for key, value in self.stats.items():
            logger.info(f"   {key}: {value}")


class DataEnrichmentPipeline:
    """数据丰富化管道"""

    def __init__(self, config=None):
        self.config = config or {}
        self.stats = {
            "total_processed": 0,
            "enrichment_success": 0,
            "enrichment_failed": 0,
        }

        logger.info("🔧 DataEnrichmentPipeline 初始化完成")

    @classmethod
    def from_crawler(cls, crawler):
        config = {
            "enable_enrichment": crawler.settings.getbool(
                "ENABLE_DATA_ENRICHMENT", True
            ),
        }
        return cls(config)

    def process_item(self, item, spider):
        """处理数据项"""
        self.stats["total_processed"] += 1
        adapter = ItemAdapter(item)

        try:
            # 添加爬虫信息
            adapter["spider_name"] = spider.name
            adapter["spider_version"] = getattr(spider, "version", "1.0")

            # 添加时间戳
            adapter["crawl_timestamp"] = self._get_current_time()

            # 计算内容指纹
            content_fingerprint = self._calculate_content_fingerprint(adapter)
            adapter["content_fingerprint"] = content_fingerprint

            # 提取关键信息
            self._extract_key_info(adapter)

            # 标准化字段名
            self._standardize_field_names(adapter)

            self.stats["enrichment_success"] += 1
            return item

        except Exception as e:
            self.stats["enrichment_failed"] += 1
            logger.error(f"数据丰富化失败: {e}")
            return item

    def _calculate_content_fingerprint(self, adapter: ItemAdapter) -> str:
        """计算内容指纹"""
        import hashlib

        # 使用URL和内容计算指纹
        url = adapter.get("url", "")
        content = adapter.get("content", "")
        title = adapter.get("title", "")

        if isinstance(content, list):
            content = " ".join(str(c) for c in content)

        fingerprint_data = f"{url}|{title}|{content}"
        return hashlib.md5(fingerprint_data.encode("utf-8")).hexdigest()

    def _extract_key_info(self, adapter: ItemAdapter):
        """提取关键信息"""
        content = adapter.get("content", "")
        if isinstance(content, list):
            content = " ".join(str(c) for c in content)

        content_str = str(content)

        # 计算内容长度
        adapter["content_length"] = len(content_str)

        # 计算字数（中文）
        import re

        chinese_chars = re.findall(r"[\u4e00-\u9fff]", content_str)
        adapter["chinese_char_count"] = len(chinese_chars)

        # 提取数字信息
        numbers = re.findall(r"\d+", content_str)
        if numbers:
            adapter["numbers_found"] = [int(n) for n in numbers[:10]]  # 最多保存10个数字

    def _standardize_field_names(self, adapter: ItemAdapter):
        """标准化字段名"""
        # 字段名映射
        field_mapping = {
            "source_url": "url",
            "page_title": "title",
            "page_content": "content",
            "publish_time": "publish_date",
            "create_time": "crawl_timestamp",
        }

        # 应用映射
        for old_name, new_name in field_mapping.items():
            if old_name in adapter and new_name not in adapter:
                adapter[new_name] = adapter[old_name]
                del adapter[old_name]

    def _get_current_time(self) -> str:
        """获取当前时间"""
        from datetime import datetime

        return datetime.now().isoformat()

    def get_stats(self) -> Dict[str, int]:
        """获取统计信息"""
        return self.stats.copy()
