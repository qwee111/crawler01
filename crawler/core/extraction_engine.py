"""
数据提取引擎

统一的数据提取接口，支持多种提取策略
"""

import logging
import re
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


class ExtractionEngine:
    """数据提取引擎"""

    def __init__(self, config_manager):
        self.config_manager = config_manager

    def extract_data(self, response, site_name: str, page_analysis: Dict) -> Dict:
        """提取数据的主入口"""
        try:
            # 获取提取配置
            extraction_config = self.config_manager.get_extraction_config(site_name)
            if not extraction_config:
                logger.error(f"❌ 未找到网站 {site_name} 的提取配置")
                return {"url": response.url, "error": "No extraction config found"}

            # 根据页面类型选择提取策略
            page_type = page_analysis.get("page_type", "unknown_page")

            if page_type in extraction_config:
                # 使用页面类型特定的配置
                type_config = extraction_config[page_type]
                logger.info(f"🎯 使用 {page_type} 特定配置提取数据")
                return self._extract_by_config(response, type_config, page_analysis)
            elif "fields" in extraction_config:
                # 使用通用字段配置
                logger.info("🔧 使用通用字段配置提取数据")
                return self._extract_by_fields(
                    response, extraction_config["fields"], page_analysis
                )
            else:
                logger.error("❌ 未找到适用的提取配置")
                return {"url": response.url, "error": "No applicable extraction config"}

        except Exception as e:
            logger.error(f"❌ 数据提取失败: {e}")
            return {"url": response.url, "error": f"Extraction failed: {e}"}

    def _extract_by_config(self, response, config: Dict, page_analysis: Dict) -> Dict:
        """根据配置提取数据（含全局字段兜底）"""
        data = {"url": response.url}

        # 提取字段（类型特定）
        fields = config.get("fields", {})
        for field_name, field_config in fields.items():
            try:
                value = self._extract_field(
                    response, field_name, field_config, page_analysis
                )
                if value is not None:
                    data[field_name] = value
                    logger.debug(f"✅ 字段 {field_name}: {str(value)[:100]}...")
                else:
                    logger.debug(f"⚠️ 字段 {field_name}: 未提取到值")
            except Exception as e:
                logger.error(f"❌ 提取字段 {field_name} 失败: {e}")
                data[f"{field_name}_error"] = str(e)

        # 兜底：若站点有全局 fields 配置，填补缺失字段
        try:
            site_extraction = (
                self.config_manager.get_extraction_config(
                    page_analysis.get("site_name", "")
                )
                or {}
            )
        except Exception:
            site_extraction = {}
        global_fields = (
            site_extraction.get("fields", {})
            if isinstance(site_extraction, dict)
            else {}
        )
        if global_fields:
            for field_name, field_config in global_fields.items():
                if field_name not in data or data.get(field_name) in (None, ""):
                    try:
                        value = self._extract_field(
                            response, field_name, field_config, page_analysis
                        )
                        if value is not None:
                            data[field_name] = value
                            logger.debug(f"🛟 兜底字段 {field_name}: {str(value)[:100]}...")
                    except Exception as e:
                        logger.debug(f"兜底提取 {field_name} 失败: {e}")

        # 提取列表项（如果是列表页）
        if page_analysis.get("page_type") == "list_page" and "list_items" in config:
            list_config = config["list_items"]
            data["items"] = self._extract_list_items(response, list_config)

        return data

    def _extract_by_fields(
        self, response, fields_config: Dict, page_analysis: Dict
    ) -> Dict:
        """根据字段配置提取数据"""
        data = {"url": response.url}

        for field_name, field_config in fields_config.items():
            try:
                value = self._extract_field(
                    response, field_name, field_config, page_analysis
                )
                if value is not None:
                    data[field_name] = value
            except Exception as e:
                logger.error(f"❌ 提取字段 {field_name} 失败: {e}")
                data[f"{field_name}_error"] = str(e)

        return data

    def _extract_field(
        self, response, field_name: str, field_config: Dict, page_analysis: Dict
    ) -> Any:
        """提取单个字段"""
        method = field_config.get("method", "xpath")
        selector = field_config.get("selector", "")
        field_type = field_config.get("type", "string")
        multiple = field_config.get("multiple", False)
        required = field_config.get("required", False)

        if not selector:
            if required:
                logger.warning(f"⚠️ 必需字段 {field_name} 缺少选择器")
            return None

        # 根据方法提取原始值
        raw_values = self._extract_raw_values(response, method, selector, multiple)

        if not raw_values:
            if required:
                logger.warning(f"⚠️ 必需字段 {field_name} 未提取到值")
            return None

        # 数据清洗和转换
        cleaned_values = self._clean_and_convert(raw_values, field_type, multiple)

        # 特殊处理
        processed_values = self._post_process_field(
            cleaned_values, field_name, field_config, page_analysis, response
        )

        return processed_values

    def _extract_raw_values(self, response, method: str, selector: str, multiple: bool):
        """提取原始值"""
        try:
            if method == "xpath":
                if multiple:
                    return response.xpath(selector).getall()
                else:
                    return response.xpath(selector).get()
            elif method == "css":
                if multiple:
                    return response.css(selector).getall()
                else:
                    return response.css(selector).get()
            elif method == "regex":
                text_content = response.text
                if multiple:
                    return re.findall(selector, text_content)
                else:
                    match = re.search(selector, text_content)
                    return match.group(1) if match else None
            else:
                logger.warning(f"⚠️ 不支持的提取方法: {method}")
                return None

        except Exception as e:
            logger.error(f"原始值提取失败: {e}")
            return None

    def _clean_and_convert(self, values, field_type: str, multiple: bool):
        """清洗和转换数据"""
        if multiple and isinstance(values, list):
            cleaned = []
            for value in values:
                if isinstance(value, str):
                    # 移除所有空白字符（包括 \r, \n, \t, \xa0）并替换为单个空格，然后去除首尾空白
                    cleaned_value = re.sub(r"\s+", " ", value).strip()
                    if cleaned_value:
                        converted_value = self._convert_type(cleaned_value, field_type)
                        if converted_value is not None:
                            cleaned.append(converted_value)
            return cleaned if cleaned else None
        else:
            if isinstance(values, str):
                # 移除所有空白字符（包括 \r, \n, \t, \xa0）并替换为单个空格，然后去除首尾空白
                cleaned = re.sub(r"\s+", " ", values).strip()
                if cleaned:
                    return self._convert_type(cleaned, field_type)
            return values

    def _convert_type(self, value: str, field_type: str):
        """类型转换"""
        try:
            if field_type == "integer":
                return int(value) if value else None
            elif field_type == "float":
                return float(value) if value else None
            elif field_type == "date":
                # 简单的日期提取
                date_match = re.search(r"\d{4}[-/年]\d{1,2}[-/月]\d{1,2}[日]?", value)
                return date_match.group(0) if date_match else value
            else:  # string
                return value
        except (ValueError, TypeError):
            return None

    def _post_process_field(
        self, values, field_name: str, field_config: Dict, page_analysis: Dict, response
    ) -> Any:
        """字段后处理"""
        page_type = page_analysis.get("page_type", "unknown_page")

        # URL转换为绝对路径
        if field_name in ["news_links", "links", "href"] and values:
            if isinstance(values, list):
                return [response.urljoin(url) for url in values if url]
            else:
                return response.urljoin(values) if values else None

        # 内容合并（详情页）
        if (
            field_name == "content"
            and page_type == "detail_page"
            and isinstance(values, list)
        ):
            return " ".join(values) if values else None

        return values

    def _extract_list_items(self, response, list_config: Dict) -> List[Dict]:
        """提取列表项"""
        items = []

        try:
            # 获取列表容器（支持 CSS 和 XPath）
            container_selector = list_config.get("container", "ul li, ol li")
            if container_selector.strip().startswith("/"):
                item_elements = response.xpath(container_selector)
            else:
                item_elements = response.css(container_selector)

            if not item_elements:
                logger.warning(f"⚠️ 未找到列表项: {container_selector}")
                return items

            # 提取每个列表项
            item_fields = list_config.get("fields", {})
            max_items = list_config.get("max_items", 50)

            for i, element in enumerate(item_elements[:max_items]):
                item_data = {"index": i + 1}

                for field_name, field_config in item_fields.items():
                    try:
                        value = self._extract_field_from_element(
                            element, field_config, response
                        )
                        if value is not None:
                            item_data[field_name] = value
                    except Exception as e:
                        logger.error(f"❌ 提取列表项字段 {field_name} 失败: {e}")

                if len(item_data) > 1:  # 除了index还有其他字段
                    items.append(item_data)

            logger.info(f"📋 提取列表项完成: {len(items)} 项")
            return items

        except Exception as e:
            logger.error(f"❌ 列表项提取失败: {e}")
            return items

    def _extract_field_from_element(self, element, field_config: Dict, response):
        """从元素中提取字段"""
        method = field_config.get("method", "css")
        selector = field_config.get("selector", "")
        attr = field_config.get("attr", "text")

        if not selector:
            return None

        try:
            raw_value = None
            if method == "css":
                if attr == "text":
                    raw_value = element.css(f"{selector}::text").get()
                else:
                    raw_value = element.css(f"{selector}::attr({attr})").get()
            elif method == "xpath":
                if attr == "text":
                    raw_value = element.xpath(f"{selector}/text()").get()
                else:
                    raw_value = element.xpath(f"{selector}/@{attr}").get()
            else:
                return None

            # 对提取到的值进行清洗
            if raw_value is not None:
                # 假设列表项中的字段类型默认为 'string'
                return self._clean_and_convert(raw_value, "string", False)
            return None

        except Exception as e:
            logger.error(f"元素字段提取失败: {e}")
            return None
