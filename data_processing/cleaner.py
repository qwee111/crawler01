# -*- coding: utf-8 -*-
"""
数据清洗模块

提供数据清洗流水线，包括文本清理、数字标准化、日期解析、地区标准化等
"""

import json
import logging
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

try:
    import jieba

    JIEBA_AVAILABLE = True
except ImportError:
    JIEBA_AVAILABLE = False

try:
    from dateutil import parser as date_parser

    DATEUTIL_AVAILABLE = True
except ImportError:
    DATEUTIL_AVAILABLE = False

logger = logging.getLogger(__name__)


class DataCleaner:
    """数据清洗器"""

    def __init__(self, config_path: Optional[str] = None):
        self.config = self.load_config(config_path) if config_path else {}

        # 注册清洗规则
        self.cleaning_rules = {
            "text": self.clean_text,
            "number": self.clean_number,
            "date": self.clean_date,
            "region": self.clean_region,
            "url": self.clean_url,
            "email": self.clean_email,
            "phone": self.clean_phone,
            "html": self.clean_html,
            "source": self.clean_source,
        }

        # 加载资源文件
        self.region_mapping = self.load_region_mapping()
        self.stopwords = self.load_stopwords()

        logger.info("数据清洗器初始化完成")

    def load_config(self, config_path: str) -> Dict:
        """加载清洗配置"""
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"清洗配置加载失败: {e}")
            return {}

    def load_region_mapping(self) -> Dict[str, str]:
        """加载地区映射表"""
        # 简化的地区映射
        return {
            "北京市": "北京",
            "上海市": "上海",
            "天津市": "天津",
            "重庆市": "重庆",
            "香港特别行政区": "香港",
            "澳门特别行政区": "澳门",
            "台湾省": "台湾",
            # 可以扩展更多映射
        }

    def load_stopwords(self) -> set:
        """加载停用词"""
        # 简化的停用词列表
        return {
            "的",
            "了",
            "在",
            "是",
            "我",
            "有",
            "和",
            "就",
            "不",
            "人",
            "都",
            "一",
            "一个",
            "上",
            "也",
            "很",
            "到",
            "说",
            "要",
            "去",
            "你",
            "会",
            "着",
            "没有",
            "看",
            "好",
            "自己",
            "这",
            "那",
            "它",
        }

    def clean_data(
        self, data: Dict[str, Any], cleaning_config: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """清洗数据字典"""
        if not isinstance(data, dict):
            logger.warning("输入数据不是字典格式")
            return data

        cleaned_data = {}
        config = cleaning_config or self.config

        for field_name, field_value in data.items():
            try:
                # 对列表/字典类型字段保持原样，避免被当作文本处理
                if isinstance(field_value, (list, tuple, set, dict)):
                    if isinstance(field_value, dict):
                        cleaned_data[field_name] = field_value
                    else:
                        cleaned_data[field_name] = list(field_value)
                    continue

                # 媒体相关字段直接透传（image_urls/file_urls/images/files 可能被上游规范化为列表）
                if field_name in {"image_urls", "file_urls", "images", "files"}:
                    cleaned_data[field_name] = field_value
                    continue

                # HTML 保留字段（不做去标签处理）
                if field_name == "content_html":
                    cleaned_data[field_name] = str(field_value)
                    continue

                # 获取字段清洗配置
                field_config = config.get("fields", {}).get(field_name, {})
                clean_type = field_config.get("type")

                # 针对常见字段的默认类型兜底
                if not clean_type:
                    if field_name in {"publish_date", "pub_date", "date"}:
                        clean_type = "date"
                    elif field_name in {"source", "news_source", "来源"}:
                        clean_type = "source"
                    elif field_name in {"url", "link"}:
                        clean_type = "url"
                    else:
                        clean_type = "text"

                # 应用清洗规则
                if clean_type in self.cleaning_rules:
                    cleaned_value = self.cleaning_rules[clean_type](
                        field_value, field_config
                    )
                else:
                    cleaned_value = self.clean_text(field_value, field_config)

                cleaned_data[field_name] = cleaned_value

            except Exception as e:
                logger.error(f"清洗字段 {field_name} 失败: {e}")
                cleaned_data[field_name] = field_value

        # 添加清洗元数据
        cleaned_data["_cleaning_metadata"] = {
            "cleaned_at": datetime.now().isoformat(),
            "cleaner_version": "1.0",
            "fields_cleaned": len(cleaned_data) - 1,  # 排除元数据字段
        }

        return cleaned_data

    def clean_text(self, text: Any, config: Dict = None) -> Optional[str]:
        """清洗文本数据"""
        if text is None:
            return None

        config = config or {}
        text_str = str(text)

        # 移除HTML标签
        if config.get("remove_html", True):
            text_str = self.clean_html(text_str)

        # 标准化空白字符
        text_str = re.sub(r"\s+", " ", text_str)
        text_str = text_str.strip()

        # 移除特殊字符
        if config.get("remove_special_chars", False):
            text_str = re.sub(r"[^\w\s\u4e00-\u9fff]", "", text_str)

        # 长度限制
        max_length = config.get("max_length")
        if max_length and len(text_str) > max_length:
            text_str = text_str[:max_length]

        # 最小长度检查
        min_length = config.get("min_length", 0)
        if len(text_str) < min_length:
            return None

        return text_str if text_str else None

    def clean_number(
        self, value: Any, config: Dict = None
    ) -> Optional[Union[int, float]]:
        """清洗数字数据"""
        if value is None:
            return None

        config = config or {}

        try:
            # 如果已经是数字
            if isinstance(value, (int, float)):
                number = float(value)
            else:
                # 从字符串中提取数字
                text = str(value)

                # 移除千分位分隔符
                text = text.replace(",", "").replace("，", "")

                # 提取数字（包括小数点）
                number_match = re.search(r"-?\d+\.?\d*", text)
                if not number_match:
                    return None

                number = float(number_match.group())

            # 范围检查
            min_value = config.get("min_value")
            max_value = config.get("max_value")

            if min_value is not None and number < min_value:
                return None
            if max_value is not None and number > max_value:
                return None

            # 返回整数或浮点数
            if config.get("as_integer", False) or number.is_integer():
                return int(number)
            else:
                return number

        except Exception as e:
            logger.error(f"数字清洗失败: {e}")
            return None

    def clean_date(self, date_value: Any, config: Dict = None) -> Optional[str]:
        """清洗日期数据"""
        if date_value is None:
            return None

        config = config or {}

        try:
            # 如果已经是日期对象
            if isinstance(date_value, (datetime, date)):
                parsed_date = date_value
            else:
                # 解析日期字符串
                date_str = str(date_value).strip()

                # 中文日期格式预处理
                date_str = date_str.replace("年", "-").replace("月", "-").replace("日", "")
                # 保留 ISO8601 关键字符 T/Z/+，避免被清理掉导致解析失败
                date_str = re.sub(r"[^\d\-\s:.\+TZtz]", "", date_str)

                if not DATEUTIL_AVAILABLE:
                    # 简单的日期解析
                    date_patterns = [
                        r"(\d{4})-(\d{1,2})-(\d{1,2})",
                        r"(\d{4})\.(\d{1,2})\.(\d{1,2})",
                        r"(\d{4})/(\d{1,2})/(\d{1,2})",
                    ]

                    for pattern in date_patterns:
                        match = re.search(pattern, date_str)
                        if match:
                            year, month, day = match.groups()
                            parsed_date = datetime(int(year), int(month), int(day))
                            break
                    else:
                        return None
                else:
                    try:
                        parsed_date = date_parser.parse(date_str)
                    except Exception:
                        # 回退：当复杂时间串解析失败时，提取首个 YYYY-MM-DD 作为日期
                        m = re.search(r"(\d{4})-(\d{1,2})-(\d{1,2})", date_str)
                        if m:
                            year, month, day = m.groups()
                            parsed_date = datetime(int(year), int(month), int(day))
                        else:
                            # 进一步尝试常见的其他分隔符
                            for pattern in [r"(\d{4})\.(\d{1,2})\.(\d{1,2})", r"(\d{4})/(\d{1,2})/(\d{1,2})"]:
                                m2 = re.search(pattern, date_str)
                                if m2:
                                    year, month, day = m2.groups()
                                    parsed_date = datetime(int(year), int(month), int(day))
                                    break
                            else:
                                raise

            # 格式化输出
            output_format = config.get("format", "%Y-%m-%d")
            return parsed_date.strftime(output_format)

        except Exception as e:
            logger.error(f"日期清洗失败: {e}")
            return None

    def clean_region(self, region: Any, config: Dict = None) -> Optional[str]:
        """清洗地区数据"""
        if region is None:
            return None

        region_str = str(region).strip()

        # 标准化地区名称
        for full_name, short_name in self.region_mapping.items():
            if full_name in region_str:
                region_str = region_str.replace(full_name, short_name)

        # 移除常见后缀
        suffixes = ["省", "市", "区", "县", "自治区", "特别行政区"]
        for suffix in suffixes:
            if region_str.endswith(suffix) and len(region_str) > len(suffix):
                region_str = region_str[: -len(suffix)]

        return region_str if region_str else None

    def clean_url(self, url: Any, config: Dict = None) -> Optional[str]:
        """清洗URL数据"""
        if url is None:
            return None

        url_str = str(url).strip()

        # URL格式验证
        url_pattern = r'https?://[^\s<>"{}|\\^`\[\]]+'
        if not re.match(url_pattern, url_str):
            return None

        # 移除URL参数（可选）
        config = config or {}
        if config.get("remove_params", False):
            url_str = url_str.split("?")[0]

        return url_str

    def clean_email(self, email: Any, config: Dict = None) -> Optional[str]:
        """清洗邮箱数据"""
        if email is None:
            return None
        email_str = str(email).strip().lower()
        email_pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        import re

        if re.match(email_pattern, email_str):
            return email_str
        return None

    def clean_source(self, value: Any, config: Dict = None) -> Optional[str]:
        """清洗来源/来源单位字段，仅保留真实来源机构名
        规则：
        - 先去标签/脚本，转为纯文本
        - 丢弃时间、作者、审核、浏览次数、分享等冗余词与后续内容
        - 在白名单/关键词表中匹配机构名或媒体名
        - 若出现多个，以第一个命中为准
        配置：
        - allow_list: 允许的来源名集合（完全匹配优先）
        - keywords: 关键词映射（正则或子串 -> 规范名）
        """
        if value is None:
            return None

        cfg = config or {}
        text = str(value)

        # 1) 去脚本/样式/标签
        text = self.clean_html(text, {"remove_css_blocks": True})

        # 2) 在去噪前优先提取“来源：”后紧跟的不含空格内容（如：来源：新华网 作者：… -> 新华网）
        import re

        m = re.search(r"(?:新闻来源|来源|发布机构)\s*[:：]\s*([^\s]+)", text, flags=re.IGNORECASE)
        if m:
            return m.group(1)

        # 3) 去常见冗余片段（顺序很重要）
        noise_patterns = [
            r"发布时间\s*[:：].*",
            r"发布\s*时间\s*[:：].*",
            r"浏览次数.*",
            r"分享到.*",
            r"微信\s*微博.*",
            r"作者\s*[:：].*",
            r"审稿\s*[:：].*",
            r"来源\s*[:：]\s*",
            r"新闻来源\s*[:：]\s*",
            r"发布时间为.*",
        ]
        for pat in noise_patterns:
            text = re.sub(pat, "", text, flags=re.IGNORECASE)

        text = text.strip()

        if not text:
            return None

        # 3) 白名单优先（完全包含匹配）
        allow_list = set(cfg.get("allow_list", [])) or set()
        for name in allow_list:
            if name and name in text:
                return name

        # 4) 关键词映射（正则或子串）
        # 默认内置若干常见媒体/机构
        keyword_map = {
            r"内蒙古自治区卫生健康委员会": "内蒙古自治区卫生健康委员会",
            r"职业卫生所-市疾控中心": "职业卫生所-市疾控中心",
            r"中国新闻网": "中国新闻网",
            r"省卫生健康委网站|省卫健委网站": "省卫生健康委网站",
            r"卫生健康委|卫健委": "卫生健康委",
            r"疾控中心|疾控|CDC": "疾控中心",
        }
        # 合并配置里的关键词
        for k, v in (cfg.get("keywords") or {}).items():
            keyword_map[k] = v

        for pattern, norm in keyword_map.items():
            if re.search(pattern, text):
                return norm

        # 5) 兜底：截短到第一个分隔符之前
        sep_patterns = [r"\s+作者.*", r"\s+审稿.*", r"\s+发布时间.*", r"\s+来源.*", r"\s+分享.*"]
        for pat in sep_patterns:
            text = re.sub(pat, "", text)
        text = text.strip()

        # 6) 控制长度
        if 0 < len(text) <= 30:
            return text
        return None

    def clean_phone(self, phone: Any, config: Dict = None) -> Optional[str]:
        """清洗电话号码数据"""
        if phone is None:
            return None

        phone_str = str(phone).strip()

        # 移除非数字字符
        phone_digits = re.sub(r"[^\d]", "", phone_str)

        # 中国手机号码验证
        if len(phone_digits) == 11 and phone_digits.startswith("1"):
            return phone_digits

        # 固定电话号码（简化）
        if len(phone_digits) >= 7:
            return phone_digits

        return None

    def clean_html(self, html: Any, config: Dict = None) -> str:
        """清洗HTML数据
        - 先移除 <script>/<style> 整块内容（避免仅去标签后留下 CSS 文本，如 .TRS_Editor {...}）
        - 再移除 HTML 注释与标签
        - 解码 HTML 实体
        - 可选：兜底移除裸露的 CSS 规则块（当上游传入的已是纯文本但仍包含 .class{...} 时）
        """
        if html is None:
            return ""

        cfg = config or {}
        html_str = str(html)

        try:
            # 1) 移除 <script>/<style> 块（大小写不敏感，多行）
            if cfg.get("remove_style_script", True):
                html_str = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", html_str)
                html_str = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", html_str)

            # 2) 移除 HTML 注释
            html_str = re.sub(r"(?is)<!--.*?-->", " ", html_str)

            # 3) 移除所有 HTML 标签
            clean_text = re.sub(r"<[^>]+>", " ", html_str)

            # 4) 解码HTML实体
            html_entities = {
                "&amp;": "&",
                "&lt;": "<",
                "&gt;": ">",
                "&quot;": '"',
                "&#39;": "'",
                "&nbsp;": " ",
            }
            for entity, char in html_entities.items():
                clean_text = clean_text.replace(entity, char)

            # 5) 兜底：移除裸露的 CSS 规则块（如 .TRS_Editor P{...}），防止作为正文残留
            if cfg.get("remove_css_blocks", True):
                clean_text = re.sub(
                    r"(?is)(?:^[\s\uFEFF\u200B]*|\s)(?:[.#][\w\-\u4e00-\u9fff]+[^{}]{0,80}\{[^{}]*\})",
                    " ",
                    clean_text,
                )

            # 6) 标准化空白字符
            clean_text = re.sub(r"\s+", " ", clean_text).strip()

            return clean_text
        except Exception:
            # 发生异常时，至少返回去标签版本
            fallback = re.sub(r"<[^>]+>", " ", html_str)
            return re.sub(r"\s+", " ", fallback).strip()

    def remove_stopwords(self, text: str) -> str:
        """移除停用词"""
        if not JIEBA_AVAILABLE:
            # 简单的空格分词
            words = text.split()
        else:
            # 使用jieba分词
            words = list(jieba.cut(text))

        # 过滤停用词
        filtered_words = [word for word in words if word not in self.stopwords]

        return " ".join(filtered_words)

    def validate_cleaned_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """验证清洗后的数据"""
        validation_result = {
            "is_valid": True,
            "errors": [],
            "warnings": [],
        }

        # 检查必需字段
        required_fields = ["url", "title", "content"]
        for field in required_fields:
            if field not in data or data[field] is None:
                validation_result["errors"].append(f"缺少必需字段: {field}")
                validation_result["is_valid"] = False

        # 检查数据质量
        if "content" in data:
            content = data["content"]
            if isinstance(content, str) and len(content) < 10:
                validation_result["warnings"].append("内容过短")

        return validation_result


class CleaningPipeline:
    """数据清洗流水线"""

    def __init__(self, config_path: Optional[str] = None):
        self.cleaner = DataCleaner(config_path)
        self.stats = {
            "total_processed": 0,
            "total_cleaned": 0,
            "total_errors": 0,
        }

    def process_item(self, item: Dict[str, Any], spider=None) -> Dict[str, Any]:
        """处理数据项"""
        self.stats["total_processed"] += 1

        try:
            # 清洗数据
            cleaned_item = self.cleaner.clean_data(item)

            # 验证数据
            validation_result = self.cleaner.validate_cleaned_data(cleaned_item)
            cleaned_item["_validation"] = validation_result

            if validation_result["is_valid"]:
                self.stats["total_cleaned"] += 1

            return cleaned_item

        except Exception as e:
            self.stats["total_errors"] += 1
            logger.error(f"数据清洗失败: {e}")
            return item

    def get_stats(self) -> Dict[str, int]:
        """获取统计信息"""
        return self.stats.copy()
