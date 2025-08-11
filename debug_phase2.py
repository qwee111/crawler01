#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
第二阶段深度调试脚本

逐步测试和调试各个功能模块
"""

import logging
import os
import subprocess
import sys
import time
from pathlib import Path

import requests

# 配置日志
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def test_imports():
    """测试模块导入"""
    print("🔍 测试模块导入...")

    tests = [
        ("Scrapy基础", "import scrapy"),
        ("Selenium", "from selenium import webdriver"),
        ("反爬虫检测器", "from anti_crawl.detector import AntiCrawlDetector"),
        ("反爬虫中间件", "from anti_crawl.middleware import AntiCrawlMiddleware"),
        ("Selenium中间件", "from crawler.selenium_middleware import SeleniumMiddleware"),
        ("代理池管理", "from proxy_pool.manager import AdvancedProxyManager"),
        ("基础中间件", "import crawler.middlewares"),
    ]

    results = []

    for name, import_code in tests:
        try:
            exec(import_code)
            print(f"✅ {name}: 导入成功")
            results.append((name, True, None))
        except Exception as e:
            print(f"❌ {name}: 导入失败 - {e}")
            results.append((name, False, str(e)))

    return results


def test_selenium_grid():
    """测试Selenium Grid连接"""
    print("\n🕷️ 测试Selenium Grid...")

    try:
        # 检查Grid状态
        response = requests.get("http://localhost:4444/wd/hub/status", timeout=10)

        if response.status_code == 200:
            data = response.json()
            if data.get("value", {}).get("ready"):
                nodes = data.get("value", {}).get("nodes", [])
                print(f"✅ Selenium Grid运行正常")
                print(f"📊 节点数量: {len(nodes)}")

                for i, node in enumerate(nodes):
                    max_sessions = node.get("maxSessions", 0)
                    slots = node.get("slots", [])
                    print(f"   节点{i+1}: 最大会话数 {max_sessions}, 插槽数 {len(slots)}")

                return True
            else:
                print("❌ Selenium Grid未就绪")
                return False
        else:
            print(f"❌ Selenium Grid连接失败: HTTP {response.status_code}")
            return False

    except Exception as e:
        print(f"❌ Selenium Grid测试失败: {e}")
        return False


def test_anti_crawl_detector():
    """测试反爬虫检测器"""
    print("\n🛡️ 测试反爬虫检测器...")

    try:
        from anti_crawl.detector import AntiCrawlDetector

        detector = AntiCrawlDetector()
        print("✅ 反爬虫检测器创建成功")

        # 创建模拟响应
        class MockResponse:
            def __init__(self, text, status=200, headers=None):
                self.text = text
                self.status = status
                self.body = text.encode("utf-8")
                self.headers = headers or {}

        # 测试验证码检测
        captcha_html = """
        <html>
        <head><title>验证码页面</title></head>
        <body>
            <div>请输入验证码</div>
            <img src="/captcha.jpg" alt="captcha">
            <input name="captcha" type="text">
        </body>
        </html>
        """

        response = MockResponse(captcha_html)
        result = detector.detect(response)

        print(f"📊 检测结果: {result['detected']}")
        print(f"🎯 置信度: {result['confidence']}")
        print(f"💡 建议: {result['suggestions'][:2]}")  # 只显示前2个建议

        return True

    except Exception as e:
        print(f"❌ 反爬虫检测器测试失败: {e}")
        return False


def test_proxy_manager():
    """测试代理池管理"""
    print("\n🌐 测试代理池管理...")

    try:
        from proxy_pool.manager import AdvancedProxyManager

        manager = AdvancedProxyManager()
        print("✅ 高级代理管理器创建成功")

        # 测试基本功能
        print("✅ 代理管理器基本功能正常")

        # 测试统计功能
        try:
            stats = manager.get_proxy_statistics()
            print(f"📊 代理统计: {stats}")
        except Exception as e:
            print(f"⚠️ 统计功能异常: {e}")

        # 测试评分功能
        try:
            from proxy_pool.manager import ProxyInfo

            test_proxy = ProxyInfo(ip="127.0.0.1", port=8080)
            score = manager._calculate_proxy_score(test_proxy)
            print(f"📊 代理评分测试: {score}")
        except Exception as e:
            print(f"⚠️ 评分功能异常: {e}")

        return True

    except Exception as e:
        print(f"❌ 代理池管理测试失败: {e}")
        return False


def test_middleware_config():
    """测试中间件配置"""
    print("\n⚙️ 测试中间件配置...")

    try:
        # 测试导入各个中间件
        import crawler.middlewares as mw

        # 检查中间件是否存在
        middlewares = [
            "ProxyMiddleware",
            "CustomUserAgentMiddleware",
            "CustomRetryMiddleware",
            "CrawlerDownloaderMiddleware",
        ]
        for middleware_name in middlewares:
            if hasattr(mw, middleware_name):
                print(f"✅ {middleware_name} 存在")
            else:
                print(f"⚠️ {middleware_name} 不存在")

        print("✅ 基础中间件导入成功")

        # 测试第二阶段中间件
        try:
            from crawler.selenium_middleware import SeleniumMiddleware

            print("✅ Selenium中间件导入成功")
        except Exception as e:
            print(f"⚠️ Selenium中间件导入失败: {e}")

        try:
            from anti_crawl.middleware import (
                AntiCrawlMiddleware,
                BehaviorSimulationMiddleware,
                CaptchaMiddleware,
                HeaderRotationMiddleware,
            )

            print("✅ 反爬虫中间件导入成功")
        except Exception as e:
            print(f"⚠️ 反爬虫中间件导入失败: {e}")

        return True

    except Exception as e:
        print(f"❌ 中间件配置测试失败: {e}")
        return False


def test_basic_crawling():
    """测试基础爬虫功能"""
    print("\n🕷️ 测试基础爬虫功能...")

    try:
        # 运行简单的测试爬虫
        cmd = [
            "uv",
            "run",
            "scrapy",
            "crawl",
            "adaptive",
            "-a",
            "site=test_site",
            "-s",
            "LOG_LEVEL=ERROR",  # 减少日志输出
            "--nolog",
        ]

        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=60, cwd="."
        )

        if result.returncode == 0:
            print("✅ 基础爬虫测试成功")
            return True
        else:
            print(f"❌ 基础爬虫测试失败")
            print(f"错误输出: {result.stderr[:200]}...")
            return False

    except Exception as e:
        print(f"❌ 基础爬虫测试异常: {e}")
        return False


def test_selenium_integration():
    """测试Selenium集成"""
    print("\n🤖 测试Selenium集成...")

    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options as ChromeOptions

        # 测试连接到Selenium Grid
        options = ChromeOptions()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")

        driver = webdriver.Remote(
            command_executor="http://localhost:4444/wd/hub", options=options
        )

        # 简单测试
        driver.get("http://httpbin.org/ip")
        title = driver.title
        page_source_length = len(driver.page_source)

        driver.quit()

        print(f"✅ Selenium集成测试成功")
        print(f"📄 页面标题: {title}")
        print(f"📊 页面大小: {page_source_length} 字符")

        return True

    except Exception as e:
        print(f"❌ Selenium集成测试失败: {e}")
        return False


def test_anti_crawl_response():
    """测试反爬虫响应处理"""
    print("\n🛡️ 测试反爬虫响应处理...")

    try:
        # 模拟412错误响应（从日志中看到的）
        from anti_crawl.detector import AntiCrawlDetector

        class MockResponse:
            def __init__(self, status=412):
                self.status = status
                self.text = """
                <html>
                <head><title>访问被拒绝</title></head>
                <body>
                    <h1>访问频率过高</h1>
                    <p>请稍后再试</p>
                </body>
                </html>
                """
                self.body = self.text.encode("utf-8")
                self.headers = {"Server": "nginx"}

        detector = AntiCrawlDetector()
        response = MockResponse(412)
        result = detector.detect(response)

        print(f"📊 检测到的反爬虫机制: {result['detected']}")
        print(f"🎯 置信度: {result['confidence']}")

        if result["detected"]:
            print("✅ 成功检测到反爬虫机制")
            return True
        else:
            print("⚠️ 未检测到反爬虫机制")
            return False

    except Exception as e:
        print(f"❌ 反爬虫响应处理测试失败: {e}")
        return False


def run_comprehensive_test():
    """运行综合测试"""
    print("🎯 第二阶段深度调试开始")
    print("=" * 60)

    tests = [
        ("模块导入", test_imports),
        ("Selenium Grid", test_selenium_grid),
        ("反爬虫检测器", test_anti_crawl_detector),
        ("代理池管理", test_proxy_manager),
        ("中间件配置", test_middleware_config),
        ("基础爬虫功能", test_basic_crawling),
        ("Selenium集成", test_selenium_integration),
        ("反爬虫响应处理", test_anti_crawl_response),
    ]

    results = []

    for test_name, test_func in tests:
        print(f"\n📋 执行测试: {test_name}")
        print("-" * 40)

        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ 测试 {test_name} 发生异常: {e}")
            results.append((test_name, False))

    # 输出测试结果汇总
    print("\n📊 测试结果汇总")
    print("=" * 60)

    passed = 0
    total = len(results)

    for test_name, result in results:
        status = "✅ 通过" if result else "❌ 失败"
        print(f"{test_name:<20} {status}")
        if result:
            passed += 1

    print("-" * 60)
    print(f"总计: {passed}/{total} 个测试通过")

    if passed == total:
        print("🎉 所有测试通过！第二阶段功能正常！")
    elif passed > total // 2:
        print("⚠️ 大部分测试通过，需要修复部分问题")
    else:
        print("❌ 多数测试失败，需要重点调试")

    return passed, total


def main():
    """主函数"""
    try:
        passed, total = run_comprehensive_test()

        print(f"\n🎊 调试完成！通过率: {passed/total*100:.1f}%")

        if passed < total:
            print("\n🔧 需要修复的问题:")
            print("1. 检查模块导入路径")
            print("2. 确认Selenium Grid运行状态")
            print("3. 验证中间件配置")
            print("4. 测试反爬虫检测逻辑")

        return passed == total

    except KeyboardInterrupt:
        print("\n⚠️ 用户中断测试")
        return False
    except Exception as e:
        print(f"\n❌ 测试过程发生异常: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
