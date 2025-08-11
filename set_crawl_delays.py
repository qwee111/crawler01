#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
设置爬虫延迟配置
"""

import argparse

import yaml


def update_delays(config_path, **delays):
    """更新配置文件中的延迟设置"""
    try:
        # 读取配置文件
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        # 更新延迟设置
        updated = []
        for key, value in delays.items():
            if value is not None:
                config["crawling_strategy"]["delays"][key] = value
                updated.append(f"{key}: {value}s")

        if updated:
            # 写回配置文件
            with open(config_path, "w", encoding="utf-8") as f:
                yaml.dump(
                    config, f, default_flow_style=False, allow_unicode=True, indent=2
                )

            print(f"✅ 已更新延迟设置: {', '.join(updated)}")
        else:
            print("⚠️  没有更新任何延迟设置")

        # 显示当前配置
        print("\n📋 当前延迟配置:")
        delays_config = config["crawling_strategy"]["delays"]
        for key, value in delays_config.items():
            print(f"  {key}: {value}s")

        return True

    except Exception as e:
        print(f"❌ 更新配置失败: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="设置爬虫延迟配置")
    parser.add_argument("--between-requests", type=float, help="请求间延迟(秒)")
    parser.add_argument("--between-selectors", type=float, help="选择器尝试间延迟(秒)")
    parser.add_argument("--selector-timeout", type=float, help="单个选择器超时时间(秒)")
    parser.add_argument("--page-load-wait", type=float, help="页面加载等待时间(秒)")
    parser.add_argument("--after-click", type=float, help="点击后延迟(秒)")
    parser.add_argument(
        "--config", default="config/nhc_firefox_config.yaml", help="配置文件路径"
    )

    args = parser.parse_args()

    # 检查是否有任何参数
    delay_args = {
        "between_requests": args.between_requests,
        "between_selectors": args.between_selectors,
        "selector_timeout": args.selector_timeout,
        "page_load_wait": args.page_load_wait,
        "after_click": args.after_click,
    }

    if all(v is None for v in delay_args.values()):
        # 交互式设置
        print("🔧 爬虫延迟设置工具")
        print("当前配置文件:", args.config)
        print("\n💡 建议值:")
        print("  请求间延迟: 0.5-2.0秒")
        print("  选择器间延迟: 0.1-0.5秒")
        print("  选择器超时: 2-10秒")
        print("  页面加载等待: 1-3秒")
        print("  点击后延迟: 0.1-0.5秒")
        print()

        try:
            delay_args["between_requests"] = input("请求间延迟(秒) [回车跳过]: ").strip()
            delay_args["between_requests"] = (
                float(delay_args["between_requests"])
                if delay_args["between_requests"]
                else None
            )
        except ValueError:
            delay_args["between_requests"] = None

        try:
            delay_args["between_selectors"] = input("选择器间延迟(秒) [回车跳过]: ").strip()
            delay_args["between_selectors"] = (
                float(delay_args["between_selectors"])
                if delay_args["between_selectors"]
                else None
            )
        except ValueError:
            delay_args["between_selectors"] = None

        try:
            delay_args["selector_timeout"] = input("选择器超时(秒) [回车跳过]: ").strip()
            delay_args["selector_timeout"] = (
                float(delay_args["selector_timeout"])
                if delay_args["selector_timeout"]
                else None
            )
        except ValueError:
            delay_args["selector_timeout"] = None

        try:
            delay_args["page_load_wait"] = input("页面加载等待(秒) [回车跳过]: ").strip()
            delay_args["page_load_wait"] = (
                float(delay_args["page_load_wait"])
                if delay_args["page_load_wait"]
                else None
            )
        except ValueError:
            delay_args["page_load_wait"] = None

        try:
            delay_args["after_click"] = input("点击后延迟(秒) [回车跳过]: ").strip()
            delay_args["after_click"] = (
                float(delay_args["after_click"]) if delay_args["after_click"] else None
            )
        except ValueError:
            delay_args["after_click"] = None

    # 更新配置
    if update_delays(args.config, **delay_args):
        print("\n🎉 设置完成！")
        print("\n📝 延迟说明:")
        print("  • between_requests: 每个新闻项处理间的延迟")
        print("  • between_selectors: 尝试不同选择器间的延迟")
        print("  • selector_timeout: 等待单个选择器的最大时间")
        print("  • page_load_wait: 页面加载后的等待时间")
        print("  • after_click: 点击操作后的延迟")


if __name__ == "__main__":
    main()
