"""
配置管理器

统一管理所有网站配置，提供配置加载、验证、缓存等功能
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlparse

import yaml

logger = logging.getLogger(__name__)


class ConfigManager:
    """统一配置管理器"""

    def __init__(self, config_dir: str = "config/sites"):
        self.config_dir = Path(config_dir)
        self.configs = {}
        self.domain_mapping = {}  # 域名到配置的映射
        self._load_all_configs()

    def _load_all_configs(self):
        """加载所有网站配置"""
        if not self.config_dir.exists():
            logger.warning(f"配置目录不存在: {self.config_dir}")
            return

        config_files = list(self.config_dir.glob("*.yaml"))
        logger.info(f"🔍 扫描配置目录: {self.config_dir}")
        logger.info(f"📁 找到配置文件: {len(config_files)} 个")

        for config_file in config_files:
            try:
                site_name = config_file.stem
                logger.info(f"📋 正在加载: {site_name} ({config_file.name})")

                config = self._load_config_file(config_file)

                if self._validate_config(config, site_name):
                    self.configs[site_name] = config
                    self._build_domain_mapping(site_name, config)
                    logger.info(f"✅ 加载配置成功: {site_name}")

                    # 显示配置摘要
                    sections = list(config.keys())
                    logger.info(f"   📊 配置部分: {sections}")

                    if "start_urls" in config:
                        start_urls_count = len(config["start_urls"])
                        logger.info(f"   🔗 起始URL数量: {start_urls_count}")
                else:
                    logger.error(f"❌ 配置验证失败: {site_name}")

            except Exception as e:
                logger.error(f"❌ 加载配置文件失败 {config_file}: {e}")
                import traceback

                logger.error(f"详细错误: {traceback.format_exc()}")

        logger.info(f"🎉 配置加载完成: 成功加载 {len(self.configs)} 个网站配置")

    def _load_config_file(self, config_file: Path) -> Dict:
        """加载单个配置文件"""
        with open(config_file, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def _validate_config(self, config: Dict, site_name: str) -> bool:
        """验证配置文件格式"""
        required_sections = ["site_info", "detection", "extraction"]

        for section in required_sections:
            if section not in config:
                logger.error(f"配置 {site_name} 缺少必需部分: {section}")
                return False

        # 验证site_info
        site_info = config["site_info"]
        if not all(key in site_info for key in ["name", "domains"]):
            logger.error(f"配置 {site_name} 的site_info部分不完整")
            return False

        # 验证extraction
        extraction = config["extraction"]
        if "fields" not in extraction:
            logger.error(f"配置 {site_name} 缺少字段提取规则")
            return False

        return True

    def _build_domain_mapping(self, site_name: str, config: Dict):
        """构建域名到配置的映射"""
        domains = config["site_info"].get("domains", [])
        for domain in domains:
            self.domain_mapping[domain] = site_name

    def get_config_by_site(self, site_name: str) -> Optional[Dict]:
        """根据网站名获取配置"""
        return self.configs.get(site_name)

    def get_config_by_url(self, url: str) -> Optional[Dict]:
        """根据URL获取配置"""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()

            # 精确匹配
            if domain in self.domain_mapping:
                site_name = self.domain_mapping[domain]
                return self.configs.get(site_name)

            # 模糊匹配（子域名）
            for config_domain, site_name in self.domain_mapping.items():
                if domain.endswith(config_domain):
                    return self.configs.get(site_name)

            return None

        except Exception as e:
            logger.error(f"URL解析失败: {url}, 错误: {e}")
            return None

    def get_site_name_by_url(self, url: str) -> Optional[str]:
        """根据URL获取网站名"""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()

            # 精确匹配
            if domain in self.domain_mapping:
                return self.domain_mapping[domain]

            # 模糊匹配
            for config_domain, site_name in self.domain_mapping.items():
                if domain.endswith(config_domain):
                    return site_name

            return None

        except Exception as e:
            logger.error(f"URL解析失败: {url}, 错误: {e}")
            return None

    def list_sites(self) -> List[str]:
        """列出所有已配置的网站"""
        return list(self.configs.keys())

    def reload_config(self, site_name: str) -> bool:
        """重新加载指定网站的配置"""
        config_file = self.config_dir / f"{site_name}.yaml"
        if not config_file.exists():
            logger.error(f"配置文件不存在: {config_file}")
            return False

        try:
            config = self._load_config_file(config_file)
            if self._validate_config(config, site_name):
                self.configs[site_name] = config
                self._build_domain_mapping(site_name, config)
                logger.info(f"✅ 重新加载配置: {site_name}")
                return True
            else:
                logger.error(f"❌ 配置验证失败: {site_name}")
                return False

        except Exception as e:
            logger.error(f"❌ 重新加载配置失败 {site_name}: {e}")
            return False

    def get_extraction_config(self, site_name: str) -> Optional[Dict]:
        """获取数据提取配置"""
        config = self.get_config_by_site(site_name)
        return config.get("extraction") if config else None

    def get_detection_config(self, site_name: str) -> Optional[Dict]:
        """获取页面检测配置"""
        config = self.get_config_by_site(site_name)
        return config.get("detection") if config else None
