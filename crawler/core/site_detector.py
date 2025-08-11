"""
网站检测器

自动识别网站类型和特征
"""

import logging
from typing import Dict, Optional
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


class SiteDetector:
    """网站检测器"""

    def __init__(self, config_manager):
        self.config_manager = config_manager

    def detect_site(self, url: str) -> Optional[str]:
        """检测URL对应的网站配置"""
        site_name = self.config_manager.get_site_name_by_url(url)
        if site_name:
            logger.info(f"🎯 检测到网站: {site_name} (URL: {url})")
            return site_name
        else:
            logger.warning(f"⚠️ 未识别的网站: {url}")
            return None

    def get_site_info(self, site_name: str) -> Optional[Dict]:
        """获取网站信息"""
        config = self.config_manager.get_config_by_site(site_name)
        return config.get("site_info") if config else None

    def is_supported_site(self, url: str) -> bool:
        """检查是否为支持的网站"""
        return self.detect_site(url) is not None

    def get_site_domains(self, site_name: str) -> list:
        """获取网站支持的域名列表"""
        site_info = self.get_site_info(site_name)
        return site_info.get("domains", []) if site_info else []

    def match_domain_pattern(self, url: str, patterns: list) -> bool:
        """匹配域名模式"""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()

            for pattern in patterns:
                if pattern in domain or domain.endswith(pattern):
                    return True
            return False

        except Exception as e:
            logger.error(f"域名模式匹配失败: {url}, 错误: {e}")
            return False
