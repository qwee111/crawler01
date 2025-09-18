#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Selenium Grid测试脚本
"""

from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.firefox.options import Options as FirefoxOptions


def test_chrome():
    """测试Chrome节点"""
    print("🧪 测试Chrome节点...")

    try:
        options = ChromeOptions()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")

        driver = webdriver.Remote(
            command_executor="http://localhost:4444/wd/hub", options=options
        )

        driver.get("http://httpbin.org/ip")
        print(f"✅ Chrome测试成功: {driver.title}")
        print(f"📄 页面内容: {driver.page_source[:100]}...")

        driver.quit()
        return True

    except Exception as e:
        print(f"❌ Chrome测试失败: {e}")
        return False


def test_firefox():
    """测试Firefox节点"""
    print("🧪 测试Firefox节点...")

    try:
        options = FirefoxOptions()
        options.add_argument("--headless")

        driver = webdriver.Remote(
            command_executor="http://localhost:4444/wd/hub", options=options
        )

        driver.get("http://httpbin.org/ip")
        print(f"✅ Firefox测试成功: {driver.title}")
        print(f"📄 页面内容: {driver.page_source[:100]}...")

        driver.quit()
        return True

    except Exception as e:
        print(f"❌ Firefox测试失败: {e}")
        return False


def main():
    print("🎯 Selenium Grid功能测试")
    print("=" * 50)

    chrome_ok = test_chrome()
    firefox_ok = test_firefox()

    if chrome_ok and firefox_ok:
        print("🎉 所有测试通过！")
    elif chrome_ok or firefox_ok:
        print("⚠️ 部分测试通过")
    else:
        print("❌ 所有测试失败")


if __name__ == "__main__":
    main()
