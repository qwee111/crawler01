#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
调试配置加载问题
"""

import logging

from data_processing.extractor import ExtractionConfigManager

# 设置日志
logging.basicConfig(level=logging.INFO)


def test_config_loading():
    """测试配置加载"""
    print("🔧 测试配置加载...")

    # 创建配置管理器
    manager = ExtractionConfigManager()

    print(f"📋 加载的配置: {list(manager.configs.keys())}")

    # 测试获取chinacdc配置
    config = manager.get_config("chinacdc")
    if config:
        print("✅ chinacdc配置加载成功")
        print(f"📋 配置字段: {list(config.keys())}")
        if "fields" in config:
            print(f"📋 提取字段: {list(config['fields'].keys())}")
    else:
        print("❌ chinacdc配置加载失败")

    return bool(config)


if __name__ == "__main__":
    test_config_loading()
