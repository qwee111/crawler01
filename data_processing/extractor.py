# -*- coding: utf-8 -*-
"""
配置化数据提取引擎

支持多种提取方法：XPath、CSS选择器、正则表达式、JSON路径
"""

import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml

try:
    from lxml import etree, html

    LXML_AVAILABLE = True
except ImportError:
    LXML_AVAILABLE = False

try:
    from bs4 import BeautifulSoup

    BS4_AVAILABLE = True
except ImportError:
    BS4_AVAILABLE = False

try:
    import jsonpath_ng

    JSONPATH_AVAILABLE = True
except ImportError:
    JSONPATH_AVAILABLE = False

try:
    from dateutil import parser as date_parser

    DATEUTIL_AVAILABLE = True
except ImportError:
    DATEUTIL_AVAILABLE = False

logger = logging.getLogger(__name__)


class ConfigurableExtractor:
    """配置化数据提取器"""

    def __init__(self, config_path: Optional[str] = None):
        self.config = {}
        if config_path:
            self.load_config(config_path)

        # 注册提取方法
        self.extractors = {
            "xpath": self.extract_by_xpath,
            "css": self.extract_by_css,
            "regex": self.extract_by_regex,
            "json": self.extract_by_json,
            "jsonpath": self.extract_by_jsonpath,
            "text": self.extract_by_text,
        }

        logger.info("配置化数据提取器初始化完成")

    def load_config(self, config_path: str) -> Dict:
        """加载提取配置"""
        config_file = Path(config_path)

        if not config_file.exists():
            logger.error(f"配置文件不存在: {config_path}")
            return {}

        try:
            with open(config_file, "r", encoding="utf-8") as f:
                if config_file.suffix.lower() in [".yaml", ".yml"]:
                    self.config = yaml.safe_load(f)
                elif config_file.suffix.lower() == ".json":
                    self.config = json.load(f)
                else:
                    logger.error(f"不支持的配置文件格式: {config_file.suffix}")
                    return {}

            logger.info(f"配置加载成功: {config_path}")
            return self.config

        except Exception as e:
            logger.error(f"配置加载失败: {e}")
            return {}

    def extract_data(self, response, site_name: str) -> Dict[str, Any]:
        """根据配置提取数据"""
        if not self.config:
            logger.warning("未加载配置，使用默认提取")
            return self._default_extract(response)

        site_config = self.config.get(site_name, {})
        if not site_config:
            logger.warning(f"未找到网站配置: {site_name}")
            return self._default_extract(response)

        extracted_data = {
            "url": response.url,
            "status_code": response.status,
            "extraction_time": self._get_current_time(),
            "site_name": site_name,
        }

        # 提取配置的字段
        fields_config = site_config.get("fields", {})
        for field_name, field_config in fields_config.items():
            try:
                value = self.extract_field(response, field_config)
                extracted_data[field_name] = self.clean_value(value, field_config)
                logger.debug(f"提取字段 {field_name}: {type(value)} - {str(value)[:100]}")

            except Exception as e:
                logger.error(f"提取字段 {field_name} 失败: {e}")
                extracted_data[field_name] = None

        # 提取元数据
        metadata = self._extract_metadata(response, site_config)
        extracted_data.update(metadata)

        return extracted_data

    def extract_field(self, response, field_config: Dict) -> Any:
        """提取单个字段"""
        method = field_config.get("method", "xpath")
        selector = field_config.get("selector")

        if not selector:
            logger.warning(f"字段配置缺少selector: {field_config}")
            return None

        if method not in self.extractors:
            logger.error(f"不支持的提取方法: {method}")
            return None

        try:
            return self.extractors[method](response, selector, field_config)
        except Exception as e:
            logger.error(f"提取方法 {method} 执行失败: {e}")
            return None

    def extract_by_xpath(
        self, response, selector: str, config: Dict
    ) -> Union[str, List[str], None]:
        """XPath提取"""
        if not LXML_AVAILABLE:
            logger.error("lxml库未安装，无法使用XPath提取")
            return None

        try:
            # 安全获取内容
            content = self._get_response_content(response)
            if not content:
                logger.warning("无法获取响应内容")
                return None

            tree = html.fromstring(content)
            elements = tree.xpath(selector)

            if config.get("multiple", False):
                results = []
                for elem in elements:
                    if hasattr(elem, "text_content"):
                        results.append(elem.text_content().strip())
                    elif hasattr(elem, "text"):
                        results.append(elem.text.strip() if elem.text else "")
                    else:
                        results.append(str(elem).strip())
                return results
            else:
                if elements:
                    elem = elements[0]
                    if hasattr(elem, "text_content"):
                        return elem.text_content().strip()
                    elif hasattr(elem, "text"):
                        return elem.text.strip() if elem.text else ""
                    else:
                        return str(elem).strip()
                return None

        except Exception as e:
            logger.error(f"XPath提取失败: {e}")
            return None

    def extract_by_css(
        self, response, selector: str, config: Dict
    ) -> Union[str, List[str], None]:
        """CSS选择器提取"""
        if not BS4_AVAILABLE:
            logger.error("BeautifulSoup库未安装，无法使用CSS选择器")
            return None

        try:
            content = self._get_response_content(response)
            if not content:
                logger.warning("无法获取响应内容")
                return None

            soup = BeautifulSoup(content, "html.parser")
            elements = soup.select(selector)

            if config.get("multiple", False):
                return [elem.get_text().strip() for elem in elements]
            else:
                return elements[0].get_text().strip() if elements else None

        except Exception as e:
            logger.error(f"CSS选择器提取失败: {e}")
            return None

    def extract_by_regex(
        self, response, pattern: str, config: Dict
    ) -> Union[str, List[str], None]:
        """正则表达式提取"""
        try:
            content = self._get_response_content(response)
            if not content:
                logger.warning("无法获取响应内容")
                return None

            flags = re.DOTALL
            if config.get("ignorecase", False):
                flags |= re.IGNORECASE

            matches = re.findall(pattern, content, flags)

            if config.get("multiple", False):
                return matches
            else:
                return matches[0] if matches else None

        except Exception as e:
            logger.error(f"正则表达式提取失败: {e}")
            return None

    def extract_by_json(
        self, response, selector: str, config: Dict
    ) -> Union[str, List[str], None]:
        """JSON提取"""
        try:
            content = self._get_response_content(response)
            if not content:
                return None

            data = json.loads(content)

            # 简单的JSON路径提取
            keys = selector.split(".")
            current = data

            for key in keys:
                if isinstance(current, dict) and key in current:
                    current = current[key]
                else:
                    return None

            return current

        except Exception as e:
            logger.error(f"JSON提取失败: {e}")
            return None

    def extract_by_jsonpath(
        self, response, selector: str, config: Dict
    ) -> Union[str, List[str], None]:
        """JSONPath提取"""
        if not JSONPATH_AVAILABLE:
            logger.error("jsonpath_ng库未安装，无法使用JSONPath提取")
            return None

        try:
            content = self._get_response_content(response)
            if not content:
                return None

            data = json.loads(content)
            jsonpath_expr = jsonpath_ng.parse(selector)
            matches = [match.value for match in jsonpath_expr.find(data)]

            if config.get("multiple", False):
                return matches
            else:
                return matches[0] if matches else None

        except Exception as e:
            logger.error(f"JSONPath提取失败: {e}")
            return None

    def extract_by_text(self, response, pattern: str, config: Dict) -> str:
        """纯文本提取"""
        try:
            content = self._get_response_content(response)
            if not content:
                return None

            # 移除HTML标签
            if BS4_AVAILABLE:
                soup = BeautifulSoup(content, "html.parser")
                content = soup.get_text()

            # 清理文本
            content = re.sub(r"\s+", " ", content).strip()

            return content

        except Exception as e:
            logger.error(f"纯文本提取失败: {e}")
            return None

    def clean_value(self, value: Any, config: Dict) -> Any:
        """清洗提取的值"""
        if value is None:
            return None

        value_type = config.get("type", "string")

        try:
            if value_type == "string":
                if isinstance(value, list):
                    value = " ".join(str(v) for v in value)
                return str(value).strip()

            elif value_type == "integer":
                if isinstance(value, str):
                    # 提取数字
                    numbers = re.findall(r"\d+", value)
                    return int(numbers[0]) if numbers else 0
                return int(value)

            elif value_type == "float":
                if isinstance(value, str):
                    numbers = re.findall(r"\d+\.?\d*", value)
                    return float(numbers[0]) if numbers else 0.0
                return float(value)

            elif value_type == "date":
                if DATEUTIL_AVAILABLE and isinstance(value, str):
                    return date_parser.parse(value).isoformat()
                return str(value)

            elif value_type == "list":
                if not isinstance(value, list):
                    return [value]
                return value

            else:
                return value

        except Exception as e:
            logger.error(f"值清洗失败: {e}")
            return value

    def parse_date(self, date_str: str) -> Optional[str]:
        """解析日期字符串"""
        if not DATEUTIL_AVAILABLE:
            logger.warning("dateutil库未安装，无法解析日期")
            return str(date_str)

        try:
            parsed_date = date_parser.parse(str(date_str))
            return parsed_date.strftime("%Y-%m-%d %H:%M:%S")
        except Exception as e:
            logger.error(f"日期解析失败: {e}")
            return str(date_str)

    def _extract_metadata(self, response, config: Dict) -> Dict[str, Any]:
        """提取元数据"""
        metadata = {}

        try:
            # 响应元数据
            if hasattr(response, "headers"):
                metadata["content_type"] = response.headers.get(
                    "Content-Type", b""
                ).decode("utf-8", errors="ignore")
                metadata["content_length"] = response.headers.get(
                    "Content-Length", b""
                ).decode("utf-8", errors="ignore")

            # 提取时间
            metadata["extraction_timestamp"] = self._get_current_time()

            # 配置信息
            metadata["extraction_config"] = config.get("site_info", {}).get(
                "name", "unknown"
            )

        except Exception as e:
            logger.error(f"元数据提取失败: {e}")

        return metadata

    def _default_extract(self, response) -> Dict[str, Any]:
        """默认提取方法"""
        try:
            content = self._get_response_content(response)

            return {
                "url": getattr(response, "url", "unknown"),
                "status_code": getattr(response, "status", "unknown"),
                "content": content[:1000] if content else None,
                "extraction_time": self._get_current_time(),
            }
        except Exception as e:
            logger.error(f"默认提取失败: {e}")
            return {
                "url": getattr(response, "url", "unknown"),
                "error": str(e),
                "extraction_time": self._get_current_time(),
            }

    def _get_current_time(self) -> str:
        """获取当前时间"""
        from datetime import datetime

        return datetime.now().isoformat()

    def _get_response_content(self, response) -> Optional[str]:
        """安全获取响应内容"""
        try:
            # 检查Content-Type
            content_type = ""
            if hasattr(response, "headers"):
                content_type = response.headers.get("Content-Type", b"")
                if isinstance(content_type, bytes):
                    content_type = content_type.decode("utf-8", errors="ignore")
                content_type = content_type.lower()

            # 检查是否为文本类型
            text_types = [
                "text/html",
                "text/plain",
                "text/xml",
                "application/xml",
                "application/xhtml+xml",
            ]
            is_text_type = any(t in content_type for t in text_types)

            if not is_text_type and content_type:
                logger.warning(f"非文本类型响应: {content_type}")
                return None

            # 尝试多种方式获取内容
            if hasattr(response, "text"):
                try:
                    return response.text
                except Exception as e:
                    logger.warning(f"获取response.text失败: {e}")

            # 尝试从body解码
            if hasattr(response, "body"):
                try:
                    if isinstance(response.body, bytes):
                        # 尝试多种编码
                        for encoding in ["utf-8", "gbk", "gb2312", "latin1"]:
                            try:
                                return response.body.decode(encoding)
                            except UnicodeDecodeError:
                                continue
                    else:
                        return str(response.body)
                except Exception as e:
                    logger.warning(f"从body解码失败: {e}")

            # 最后尝试直接转换
            try:
                return str(response)
            except Exception as e:
                logger.error(f"无法获取响应内容: {e}")
                return None

        except Exception as e:
            logger.error(f"获取响应内容异常: {e}")
            return None


class ExtractionConfigManager:
    """提取配置管理器"""

    def __init__(self):
        self.configs = {}
        self.config_dirs = ["config/extraction", "config/sites"]
        self.load_all_configs()

    def load_all_configs(self):
        """加载所有配置文件"""
        from pathlib import Path

        for config_dir in self.config_dirs:
            config_path = Path(config_dir)
            if not config_path.exists():
                logger.warning(f"配置目录不存在: {config_dir}")
                continue

            # 加载该目录下的所有yaml文件
            for config_file in config_path.glob("*.yaml"):
                try:
                    site_name = config_file.stem
                    with open(config_file, "r", encoding="utf-8") as f:
                        config = yaml.safe_load(f)

                    self.configs[site_name] = config
                    logger.info(f"✅ 加载配置: {site_name} <- {config_file}")

                except Exception as e:
                    logger.error(f"❌ 加载配置失败 {config_file}: {e}")

        logger.info(f"📋 总共加载了 {len(self.configs)} 个网站配置")
        logger.info(f"📋 可用配置: {list(self.configs.keys())}")

    def get_config(self, site_name: str) -> Dict:
        """获取网站配置"""
        if site_name in self.configs:
            return self.configs[site_name]

        # 尝试重新加载配置
        logger.warning(f"配置 {site_name} 不存在，尝试重新加载...")
        self.load_all_configs()

        return self.configs.get(site_name, {})

    def extract_data(self, response, site_name: str) -> Dict[str, Any]:
        """提取数据"""
        config = self.get_config(site_name)

        if not config:
            logger.warning(f"未找到网站配置: {site_name}，使用默认提取")
            return self._default_extract(response)

        logger.info(f"🎯 使用配置提取数据: {site_name}")

        # 创建提取器实例
        extractor = ConfigurableExtractor()
        extractor.config = {site_name: config}

        return extractor.extract_data(response, site_name)

    def _default_extract(self, response) -> Dict[str, Any]:
        """默认提取方法"""
        try:
            # 安全获取基本信息
            url = getattr(response, "url", "unknown")
            status = getattr(response, "status", "unknown")

            # 尝试获取内容
            content = None
            try:
                if hasattr(response, "text"):
                    content = response.text[:500]  # 只取前500字符
                elif hasattr(response, "body"):
                    content = response.body.decode("utf-8", errors="ignore")[:500]
            except:
                content = None

            return {
                "url": url,
                "status_code": status,
                "content": content,
                "extraction_time": self._get_current_time(),
                "extraction_method": "default",
            }
        except Exception as e:
            logger.error(f"默认提取失败: {e}")
            return {
                "url": getattr(response, "url", "unknown"),
                "error": str(e),
                "extraction_time": self._get_current_time(),
                "extraction_method": "default_failed",
            }

    def _get_current_time(self) -> str:
        """获取当前时间"""
        from datetime import datetime

        return datetime.now().isoformat()
