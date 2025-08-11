#!/usr/bin/env python3
"""
配置文件格式转换工具
"""
import argparse
from pathlib import Path

import yaml


def convert_old_to_new_format(old_config):
    """将旧格式转换为新格式"""
    new_config = {
        "site_info": {
            "name": old_config.get("name", ""),
            "base_url": old_config.get("base_url", ""),
            "description": f"{old_config.get('name', '')}数据爬取",
        },
        "target_pages": [],
        "browser_config": {
            "type": "requests",
            "headless": True,
            "timeouts": {"page_load": 30, "explicit_wait": 150},
        },
        "selectors": {},
        "crawling_strategy": {
            "delays": {
                "between_requests": old_config.get("request_settings", {}).get(
                    "delay", 2.0
                ),
                "random_delay": old_config.get("request_settings", {}).get(
                    "random_delay", True
                ),
            },
            "limits": {"max_pages": 10, "max_items_per_page": 30},
            "retry": {
                "max_attempts": old_config.get("request_settings", {}).get(
                    "retry_times", 3
                ),
                "retry_delay": old_config.get("request_settings", {}).get(
                    "retry_delay", 5
                ),
            },
        },
        "data_extraction": {"required_fields": [], "cleaning_rules": {}},
        "follow_rules": {
            "allow": old_config.get("follow", {}).get("allow", []),
            "deny": old_config.get("follow", {}).get("deny", []),
        },
        "file_storage": {
            "directories": {"texts": "texts", "data": "data", "debug": "debug"},
            "encoding": "utf-8",
        },
    }

    # 转换起始URL
    if "start_urls" in old_config:
        for url_config in old_config["start_urls"]:
            new_config["target_pages"].append(
                {
                    "url": url_config["url"],
                    "name": url_config.get("description", ""),
                    "type": url_config.get("type", "list_page"),
                }
            )

    # 转换字段选择器
    if "fields" in old_config:
        selectors = {}
        required_fields = []

        for field_name, field_config in old_config["fields"].items():
            if field_config.get("method") == "xpath":
                selector_key = f"{field_name}_selectors"
                selectors[selector_key] = [field_config["selector"]]

                if field_config.get("required"):
                    required_fields.append(field_name)

        new_config["selectors"] = selectors
        new_config["data_extraction"]["required_fields"] = required_fields

    return new_config


def main():
    parser = argparse.ArgumentParser(description="配置文件格式转换")
    parser.add_argument("input_file", help="输入的旧格式配置文件")
    parser.add_argument("output_file", help="输出的新格式配置文件")

    args = parser.parse_args()

    input_path = Path(args.input_file)
    output_path = Path(args.output_file)

    if not input_path.exists():
        print(f"❌ 输入文件不存在: {input_path}")
        return

    try:
        # 读取旧配置
        with open(input_path, "r", encoding="utf-8") as f:
            old_config = yaml.safe_load(f)

        # 转换格式
        new_config = convert_old_to_new_format(old_config)

        # 保存新配置
        with open(output_path, "w", encoding="utf-8") as f:
            yaml.dump(
                new_config, f, default_flow_style=False, allow_unicode=True, indent=2
            )

        print(f"✅ 配置转换完成: {input_path} -> {output_path}")

    except Exception as e:
        print(f"❌ 转换失败: {e}")


if __name__ == "__main__":
    main()
