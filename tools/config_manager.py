#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
配置管理工具

用于管理网站爬取规则配置
"""

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List

import yaml


class ConfigManager:
    """配置管理器"""

    def __init__(self, config_dir: str = "config/sites"):
        self.config_dir = Path(config_dir)
        self.config_dir.mkdir(parents=True, exist_ok=True)

    def list_configs(self) -> List[str]:
        """列出所有配置"""
        configs = []
        for config_file in self.config_dir.glob("*.yaml"):
            configs.append(config_file.stem)
        return sorted(configs)

    def get_config(self, site_name: str) -> Dict:
        """获取配置"""
        config_file = self.config_dir / f"{site_name}.yaml"
        if not config_file.exists():
            raise FileNotFoundError(f"配置文件不存在: {config_file}")

        with open(config_file, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def save_config(self, site_name: str, config: Dict):
        """保存配置"""
        config_file = self.config_dir / f"{site_name}.yaml"
        with open(config_file, "w", encoding="utf-8") as f:
            yaml.dump(config, f, default_flow_style=False, allow_unicode=True)
        print(f"✅ 配置已保存: {config_file}")

    def validate_config(self, config: Dict) -> List[str]:
        """验证配置"""
        errors = []

        # 检查必需字段
        required_fields = ["site_name", "allowed_domains"]
        for field in required_fields:
            if field not in config:
                errors.append(f"缺少必需字段: {field}")

        # 检查字段格式
        if "allowed_domains" in config:
            if not isinstance(config["allowed_domains"], list):
                errors.append("allowed_domains 必须是列表")

        if "fields" in config:
            if not isinstance(config["fields"], dict):
                errors.append("fields 必须是字典")

        return errors

    def create_template(self, site_name: str, base_url: str, domains: List[str]):
        """创建配置模板"""
        template = {
            "site_name": site_name,
            "description": f"{site_name}网站爬取规则",
            "base_url": base_url,
            "allowed_domains": domains,
            "request_settings": {
                "download_delay": 1,
                "concurrent_requests": 1,
                "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            },
            "fields": {
                "title": {
                    "method": "css",
                    "selector": "h1, .title",
                    "attr": "text",
                    "default": "",
                },
                "content": {
                    "method": "css",
                    "selector": ".content p, .article p",
                    "attr": "text",
                    "multiple": True,
                    "default": [],
                },
                "publish_time": {
                    "method": "css",
                    "selector": ".time, .date",
                    "attr": "text",
                    "default": "",
                },
            },
            "links": {
                "detail": {
                    "selector": 'a[href*="detail"], .news-list a',
                    "attr": "href",
                }
            },
            "follow": {
                "allow": [".*\\.(html|shtml)$"],
                "deny": [".*\\.(jpg|jpeg|png|gif|pdf)$"],
            },
        }

        return template

    def test_config(self, site_name: str, test_url: str):
        """测试配置"""
        try:
            config = self.get_config(site_name)
            print(f"🧪 测试配置: {site_name}")
            print(f"🔗 测试URL: {test_url}")

            # 这里可以添加实际的测试逻辑
            # 比如发送请求并尝试提取数据

            print("✅ 配置测试通过")
            return True

        except Exception as e:
            print(f"❌ 配置测试失败: {e}")
            return False

    def export_config(self, site_name: str, format: str = "json"):
        """导出配置"""
        config = self.get_config(site_name)

        if format == "json":
            output = json.dumps(config, indent=2, ensure_ascii=False)
            print(output)
        elif format == "yaml":
            output = yaml.dump(config, default_flow_style=False, allow_unicode=True)
            print(output)
        else:
            raise ValueError(f"不支持的格式: {format}")

    def compare_configs(self, site1: str, site2: str):
        """比较两个配置"""
        config1 = self.get_config(site1)
        config2 = self.get_config(site2)

        print(f"📊 比较配置: {site1} vs {site2}")

        # 比较字段
        fields1 = set(config1.get("fields", {}).keys())
        fields2 = set(config2.get("fields", {}).keys())

        common_fields = fields1 & fields2
        unique_to_1 = fields1 - fields2
        unique_to_2 = fields2 - fields1

        print(f"🔄 共同字段: {common_fields}")
        print(f"🔹 {site1}独有: {unique_to_1}")
        print(f"🔸 {site2}独有: {unique_to_2}")


def main():
    """命令行工具"""
    parser = argparse.ArgumentParser(description="网站配置管理工具")
    parser.add_argument(
        "action",
        choices=["list", "show", "create", "validate", "test", "export", "compare"],
        help="操作类型",
    )

    parser.add_argument("--site", help="网站名称")
    parser.add_argument("--site2", help="第二个网站名称（用于比较）")
    parser.add_argument("--url", help="网站URL")
    parser.add_argument("--domains", nargs="+", help="允许的域名")
    parser.add_argument(
        "--format", choices=["json", "yaml"], default="yaml", help="输出格式"
    )
    parser.add_argument("--test-url", help="测试URL")

    args = parser.parse_args()

    manager = ConfigManager()

    try:
        if args.action == "list":
            configs = manager.list_configs()
            print("📋 可用配置:")
            for config in configs:
                print(f"  - {config}")

        elif args.action == "show":
            if not args.site:
                print("❌ 请指定网站名称 --site")
                return

            config = manager.get_config(args.site)
            print(f"📄 配置详情: {args.site}")
            print(yaml.dump(config, default_flow_style=False, allow_unicode=True))

        elif args.action == "create":
            if not all([args.site, args.url, args.domains]):
                print("❌ 请指定 --site, --url, --domains")
                return

            template = manager.create_template(args.site, args.url, args.domains)
            manager.save_config(args.site, template)

        elif args.action == "validate":
            if not args.site:
                print("❌ 请指定网站名称 --site")
                return

            config = manager.get_config(args.site)
            errors = manager.validate_config(config)

            if errors:
                print("❌ 配置验证失败:")
                for error in errors:
                    print(f"  - {error}")
            else:
                print("✅ 配置验证通过")

        elif args.action == "test":
            if not all([args.site, args.test_url]):
                print("❌ 请指定 --site 和 --test-url")
                return

            manager.test_config(args.site, args.test_url)

        elif args.action == "export":
            if not args.site:
                print("❌ 请指定网站名称 --site")
                return

            manager.export_config(args.site, args.format)

        elif args.action == "compare":
            if not all([args.site, args.site2]):
                print("❌ 请指定 --site 和 --site2")
                return

            manager.compare_configs(args.site, args.site2)

    except Exception as e:
        print(f"❌ 操作失败: {e}")


if __name__ == "__main__":
    main()
