# -*- coding: utf-8 -*-
"""
爬取规则引擎

支持配置化的网站爬取规则，避免为每个网站编写特定代码
"""

import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urljoin, urlparse

import yaml

logger = logging.getLogger(__name__)


class RuleEngine:
    """爬取规则引擎"""

    def __init__(self, config_dir: str = "config/sites"):
        self.config_dir = Path(config_dir)
        self.rules = {}
        self.logger = logger  # 添加logger属性
        self.load_all_rules()

    def load_all_rules(self):
        """加载所有网站规则"""
        if not self.config_dir.exists():
            logger.warning(f"配置目录不存在: {self.config_dir}")
            return

        for config_file in self.config_dir.glob("*.yaml"):
            try:
                with open(config_file, "r", encoding="utf-8") as f:
                    config = yaml.safe_load(f)
                    site_name = config_file.stem
                    self.rules[site_name] = config
                    logger.info(f"加载网站规则: {site_name}")
            except Exception as e:
                logger.error(f"加载规则文件失败 {config_file}: {e}")

    def get_rule(self, site_name: str) -> Optional[Dict]:
        """获取指定网站的规则"""
        return self.rules.get(site_name)

    def match_site(self, url: str) -> Optional[str]:
        """根据URL匹配网站规则"""
        domain = urlparse(url).netloc

        for site_name, rule in self.rules.items():
            allowed_domains = rule.get("allowed_domains", [])
            for allowed_domain in allowed_domains:
                if domain.endswith(allowed_domain):
                    return site_name
        return None

    def extract_data(self, response, rule):
        """从响应中提取数据"""
        try:
            # 检查响应是否有效
            if not response:
                self.logger.error("❌ 响应对象为空")
                return {}

            # 检查响应状态
            if hasattr(response, "status") and response.status >= 400:
                self.logger.error(f"❌ HTTP错误状态: {response.status}")
                return {"url": response.url, "error": f"HTTP {response.status}"}

            # 安全检查响应类型
            content_type = self._get_content_type(response)
            self.logger.info(f"📋 响应类型: {content_type}")

            # 检查是否为文本响应
            if not self._is_text_response_safe(response):
                self.logger.warning(f"⚠️ 非文本响应: {content_type}")
                return {
                    "url": response.url,
                    "status": "non_text_content",
                    "content_type": content_type,
                    "error": "Response content is not text",
                }

            # 获取字段提取规则
            fields_config = rule.get("fields", {})
            if not fields_config:
                self.logger.warning("⚠️ 未找到字段提取规则")
                return {"url": response.url, "error": "No field extraction rules"}

            extracted_data = {"url": response.url}

            # 提取每个字段
            for field_name, field_config in fields_config.items():
                try:
                    value = self._extract_field(response, field_config)
                    if value is not None:
                        extracted_data[field_name] = value
                        self.logger.debug(f"✅ 提取字段 {field_name}: {str(value)[:100]}...")
                    else:
                        self.logger.debug(f"⚠️ 字段 {field_name} 未提取到值")
                except Exception as e:
                    self.logger.error(f"❌ 提取字段 {field_name} 失败: {e}")
                    extracted_data[f"{field_name}_error"] = str(e)

            self.logger.info(f"📊 成功提取 {len(extracted_data)} 个字段")
            return extracted_data

        except Exception as e:
            self.logger.error(f"❌ 数据提取异常: {e}")
            import traceback

            self.logger.error(f"❌ 异常详情: {traceback.format_exc()}")
            return {
                "url": getattr(response, "url", "unknown"),
                "error": str(e),
                "status": "extraction_error",
            }

    def _get_content_type(self, response):
        """安全获取Content-Type"""
        try:
            if hasattr(response, "headers"):
                content_type = response.headers.get("Content-Type", b"")
                if isinstance(content_type, bytes):
                    content_type = content_type.decode("utf-8", errors="ignore")
                return content_type
            return "unknown"
        except Exception:
            return "unknown"

    def _is_text_response_safe(self, response):
        """安全检查响应是否为文本内容"""
        try:
            # 检查Content-Type头
            content_type = self._get_content_type(response).lower()

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

            # 如果没有明确的Content-Type，尝试检查响应体
            if not content_type or content_type == "unknown":
                if hasattr(response, "body") and response.body:
                    try:
                        # 尝试解码前100字节
                        sample = response.body[:100]
                        if isinstance(sample, bytes):
                            sample = sample.decode("utf-8", errors="ignore")
                        # 检查是否包含HTML标签
                        return "<" in sample and ">" in sample
                    except Exception:
                        # 二进制/编码解码错误等：视为非文本，交由后续判断
                        return False

            # 如果Content-Type不明确，假设为文本
            if not content_type or content_type == "unknown":
                self.logger.warning("⚠️ 无法确定响应类型，假设为文本")
                return True

            # 非文本类型
            return False

        except Exception as e:
            self.logger.error(f"❌ 检查响应类型失败: {e}")
            # 出错时假设为文本，让后续处理决定
            return True

    def _extract_field(self, response, field_rule: Union[str, Dict]) -> Any:
        """提取单个字段"""
        if isinstance(field_rule, str):
            # 简单CSS选择器
            return self._extract_by_css(response, field_rule)

        elif isinstance(field_rule, dict):
            method = field_rule.get("method", "css")
            selector = field_rule.get("selector", "")
            attr = field_rule.get("attr", "text")
            multiple = field_rule.get("multiple", False)
            regex = field_rule.get("regex")
            default = field_rule.get("default")

            if method == "css":
                result = self._extract_by_css(response, selector, attr, multiple)
            elif method == "xpath":
                result = self._extract_by_xpath(response, selector, attr, multiple)
            elif method == "regex":
                result = self._extract_by_regex(response, selector)
            else:
                result = default

            # 应用正则表达式处理
            if regex and result:
                if isinstance(result, list):
                    result = [self._apply_regex(item, regex) for item in result]
                else:
                    result = self._apply_regex(result, regex)

            return result if result is not None else default

        return None

    def _extract_by_css(
        self, response, selector: str, attr: str = "text", multiple: bool = False
    ):
        """使用CSS选择器提取"""
        elements = response.css(selector)

        if not elements:
            return [] if multiple else None

        if attr == "text":
            values = [elem.get().strip() for elem in elements if elem.get()]
        else:
            values = [elem.attrib.get(attr, "").strip() for elem in elements]

        values = [v for v in values if v]  # 过滤空值

        if multiple:
            return values
        else:
            return values[0] if values else None

    def _extract_by_xpath(
        self, response, selector: str, attr: str = "text", multiple: bool = False
    ):
        """使用XPath选择器提取"""
        if attr == "text":
            xpath = f"{selector}//text()"
        else:
            xpath = f"{selector}/@{attr}"

        values = response.xpath(xpath).getall()
        values = [v.strip() for v in values if v.strip()]

        if multiple:
            return values
        else:
            return values[0] if values else None

    def _extract_by_regex(self, response, pattern: str):
        """使用正则表达式提取"""
        matches = re.findall(pattern, response.text)
        return matches[0] if matches else None

    def _apply_regex(self, text: str, pattern: str) -> str:
        """应用正则表达式处理文本"""
        if not text:
            return text

        match = re.search(pattern, text)
        return match.group(1) if match and match.groups() else text

    def get_links(self, response, rule: Dict) -> List[str]:
        """根据规则提取链接"""
        links = []
        link_rules = rule.get("links", {})

        for link_type, link_rule in link_rules.items():
            try:
                if isinstance(link_rule, str):
                    # 简单CSS选择器
                    found_links = response.css(f"{link_rule}::attr(href)").getall()
                else:
                    # 复杂规则
                    selector = link_rule.get("selector", "")
                    attr = link_rule.get("attr", "href")
                    found_links = response.css(f"{selector}::attr({attr})").getall()

                # 转换为绝对URL
                for link in found_links:
                    if link:
                        absolute_url = urljoin(response.url, link)
                        links.append(
                            {
                                "url": absolute_url,
                                "type": link_type,
                                "source": response.url,
                            }
                        )

            except Exception as e:
                logger.warning(f"提取链接类型 {link_type} 失败: {e}")

        return links

    def should_follow(self, url: str, rule: Dict) -> bool:
        """判断是否应该跟进链接"""
        follow_rules = rule.get("follow", {})

        # 检查允许的URL模式
        allow_patterns = follow_rules.get("allow", [])
        if allow_patterns:
            for pattern in allow_patterns:
                if re.search(pattern, url):
                    break
            else:
                return False

        # 检查禁止的URL模式
        deny_patterns = follow_rules.get("deny", [])
        for pattern in deny_patterns:
            if re.search(pattern, url):
                return False

        return True

    def get_request_settings(self, rule: Dict) -> Dict:
        """获取请求设置"""
        return rule.get("request_settings", {})


class AdaptiveSpider:
    """自适应爬虫混入类"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.rule_engine = RuleEngine()

    def parse_with_rules(self, response):
        """使用规则引擎解析响应"""
        # 匹配网站规则
        site_name = self.rule_engine.match_site(response.url)
        if not site_name:
            self.logger.warning(f"未找到匹配的网站规则: {response.url}")
            return

        rule = self.rule_engine.get_rule(site_name)
        self.logger.info(f"使用规则 {site_name} 解析: {response.url}")

        # 提取数据
        data = self.rule_engine.extract_data(response, rule)
        yield data

        # 提取并跟进链接
        links = self.rule_engine.get_links(response, rule)
        for link_info in links:
            url = link_info["url"]
            if self.rule_engine.should_follow(url, rule):
                yield response.follow(
                    url,
                    callback=self.parse_with_rules,
                    meta={"link_type": link_info["type"]},
                )


# 使用示例的配置生成器
def generate_site_config(site_name: str, config: Dict):
    """生成网站配置文件"""
    config_dir = Path("config/sites")
    config_dir.mkdir(parents=True, exist_ok=True)

    config_file = config_dir / f"{site_name}.yaml"
    with open(config_file, "w", encoding="utf-8") as f:
        yaml.dump(config, f, default_flow_style=False, allow_unicode=True)

    logger.info(f"生成配置文件: {config_file}")
