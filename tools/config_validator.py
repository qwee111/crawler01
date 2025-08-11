#!/usr/bin/env python3
"""
配置文件验证工具
"""
from pathlib import Path

import yaml


class ConfigValidator:
    """配置文件验证器"""

    def __init__(self):
        self.new_format_schema = {
            "required_sections": [
                "site_info",
                "target_pages",
                "browser_config",
                "selectors",
                "crawling_strategy",
            ],
            "site_info_fields": ["name", "base_url"],
            "browser_config_fields": ["type", "headless", "timeouts"],
        }

        self.old_format_schema = {
            "required_sections": ["name", "base_url", "fields"],
            "field_structure": ["method", "selector", "type"],
        }

    def detect_format(self, config):
        """检测配置格式"""
        if "site_info" in config and "target_pages" in config:
            return "new"
        elif "fields" in config and isinstance(config["fields"], dict):
            return "old"
        else:
            return "unknown"

    def validate_config(self, config_path):
        """验证配置文件"""
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)

            format_type = self.detect_format(config)

            print(f"📋 配置文件: {config_path}")
            print(f"🔍 检测到格式: {format_type}")

            if format_type == "new":
                return self.validate_new_format(config)
            elif format_type == "old":
                return self.validate_old_format(config)
            else:
                print("❌ 未知的配置格式")
                return False

        except Exception as e:
            print(f"❌ 配置文件读取失败: {e}")
            return False

    def validate_new_format(self, config):
        """验证新格式配置"""
        print("✅ 新格式配置验证...")

        for section in self.new_format_schema["required_sections"]:
            if section in config:
                print(f"   ✅ {section}")
            else:
                print(f"   ❌ 缺少节: {section}")
                return False

        return True

    def validate_old_format(self, config):
        """验证旧格式配置"""
        print("⚠️  旧格式配置验证...")

        for section in self.old_format_schema["required_sections"]:
            if section in config:
                print(f"   ✅ {section}")
            else:
                print(f"   ❌ 缺少节: {section}")
                return False

        print("💡 建议转换为新格式")
        return True


def main():
    validator = ConfigValidator()

    # 检查所有配置文件
    config_dirs = [Path("config/sites"), Path("config")]

    for config_dir in config_dirs:
        if config_dir.exists():
            for config_file in config_dir.glob("*.yaml"):
                print("=" * 50)
                validator.validate_config(config_file)


if __name__ == "__main__":
    main()
