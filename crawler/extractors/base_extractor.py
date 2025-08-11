"""
基础提取器

所有提取器的基类
"""

import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class BaseExtractor(ABC):
    """基础提取器抽象类"""

    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.logger = logger

    @abstractmethod
    def extract(self, response, **kwargs) -> Dict[str, Any]:
        """提取数据的抽象方法"""
        pass

    def validate_response(self, response) -> bool:
        """验证响应是否有效"""
        if not response:
            self.logger.error("❌ 响应对象为空")
            return False

        if hasattr(response, "status") and response.status >= 400:
            self.logger.error(f"❌ HTTP错误状态: {response.status}")
            return False

        return True

    def clean_text(self, text: str) -> str:
        """清洗文本"""
        if not isinstance(text, str):
            return str(text) if text is not None else ""

        # 去除多余空白
        cleaned = " ".join(text.split())

        # 去除特殊字符（可配置）
        if self.config.get("remove_special_chars", False):
            import re

            cleaned = re.sub(r"[^\w\s\u4e00-\u9fff]", "", cleaned)

        return cleaned.strip()

    def convert_to_absolute_url(self, url: str, response) -> str:
        """转换为绝对URL"""
        if not url:
            return url

        try:
            return response.urljoin(url)
        except Exception as e:
            self.logger.warning(f"⚠️ URL转换失败: {url}, 错误: {e}")
            return url

    def extract_with_fallback(
        self, response, selectors: list, method: str = "css"
    ) -> Optional[str]:
        """使用备选选择器提取"""
        for selector in selectors:
            try:
                if method == "css":
                    result = response.css(f"{selector}::text").get()
                elif method == "xpath":
                    result = response.xpath(f"{selector}/text()").get()
                else:
                    continue

                if result and result.strip():
                    return result.strip()

            except Exception as e:
                self.logger.debug(f"选择器 {selector} 失败: {e}")
                continue

        return None

    def log_extraction_result(self, field_name: str, value: Any, success: bool = True):
        """记录提取结果"""
        if success and value is not None:
            if isinstance(value, (list, tuple)):
                self.logger.debug(f"✅ {field_name}: 提取到 {len(value)} 个值")
            else:
                preview = (
                    str(value)[:100] + "..." if len(str(value)) > 100 else str(value)
                )
                self.logger.debug(f"✅ {field_name}: {preview}")
        else:
            self.logger.debug(f"⚠️ {field_name}: 未提取到值")

    def get_config_value(self, key: str, default: Any = None) -> Any:
        """获取配置值"""
        return self.config.get(key, default)
