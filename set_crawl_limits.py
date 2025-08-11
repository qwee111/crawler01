#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
快速设置爬取限制
"""

import argparse

import yaml


def update_config_limits(config_path, max_pages=None, max_items_per_page=None):
    """更新配置文件中的限制"""
    try:
        # 读取配置文件
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        # 更新限制
        if max_pages is not None:
            config["crawling_strategy"]["limits"]["max_pages"] = max_pages
            print(f"✅ 最大页数设置为: {max_pages}")

        if max_items_per_page is not None:
            config["crawling_strategy"]["limits"][
                "max_items_per_page"
            ] = max_items_per_page
            print(f"✅ 每页最大项目数设置为: {max_items_per_page}")

        # 写回配置文件
        with open(config_path, "w", encoding="utf-8") as f:
            yaml.dump(config, f, default_flow_style=False, allow_unicode=True, indent=2)

        print(f"✅ 配置文件已更新: {config_path}")

        # 显示当前配置
        print("\n📋 当前爬取限制:")
        print(f"  最大页数: {config['crawling_strategy']['limits']['max_pages']}")
        print(
            f"  每页最大项目数: {config['crawling_strategy']['limits']['max_items_per_page']}"
        )
        print(
            f"  最大内容长度: {config['crawling_strategy']['limits']['max_content_length']}"
        )
        print(
            f"  最小内容长度: {config['crawling_strategy']['limits']['min_content_length']}"
        )

        return True

    except Exception as e:
        print(f"❌ 更新配置失败: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="设置爬虫爬取限制")
    parser.add_argument("--pages", type=int, help="最大页数")
    parser.add_argument("--items", type=int, help="每页最大项目数")
    parser.add_argument(
        "--config", default="config/nhc_firefox_config.yaml", help="配置文件路径"
    )

    args = parser.parse_args()

    if args.pages is None and args.items is None:
        # 交互式设置
        print("🔧 爬虫限制设置工具")
        print("当前配置文件:", args.config)
        print()

        try:
            pages = input("请输入最大页数 (回车跳过): ").strip()
            pages = int(pages) if pages else None
        except ValueError:
            pages = None

        try:
            items = input("请输入每页最大项目数 (回车跳过): ").strip()
            items = int(items) if items else None
        except ValueError:
            items = None

        if pages is None and items is None:
            print("⚠️  没有设置任何限制")
            return
    else:
        pages = args.pages
        items = args.items

    # 更新配置
    if update_config_limits(args.config, pages, items):
        print("\n🎉 设置完成！现在可以运行爬虫:")
        print("scrapy crawl nhc_firefox")


if __name__ == "__main__":
    main()
