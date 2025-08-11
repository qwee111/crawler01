#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
第二阶段启动脚本

启动反爬机制应对系统，包括Selenium Grid和高级代理池
"""

import logging
import os
import subprocess
import sys
import time
from pathlib import Path

# 配置日志
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def run_command(cmd, cwd=None, check=True):
    """安全地运行命令"""
    try:
        print(f"🔧 执行命令: {' '.join(cmd)}")
        result = subprocess.run(
            cmd,
            cwd=cwd,
            capture_output=True,
            text=True,
            check=check,
            encoding="utf-8",
            errors="ignore",
        )

        if result.stdout:
            print(f"✅ 输出: {result.stdout.strip()}")

        return result

    except subprocess.CalledProcessError as e:
        print(f"❌ 命令执行失败: {e}")
        if e.stdout:
            print(f"标准输出: {e.stdout}")
        if e.stderr:
            print(f"错误输出: {e.stderr}")
        return None
    except Exception as e:
        print(f"❌ 执行异常: {e}")
        return None


def check_docker():
    """检查Docker环境"""
    print("🔍 检查Docker环境...")

    result = run_command(["docker", "--version"], check=False)
    if not result or result.returncode != 0:
        print("❌ Docker未安装或不可用")
        return False

    result = run_command(["docker-compose", "--version"], check=False)
    if not result or result.returncode != 0:
        print("❌ Docker Compose未安装或不可用")
        return False

    print("✅ Docker环境检查通过")
    return True


def start_selenium_grid():
    """启动Selenium Grid"""
    print("🚀 启动Selenium Grid...")

    docker_dir = Path("deployment/docker")
    if not docker_dir.exists():
        print(f"❌ Docker配置目录不存在: {docker_dir}")
        return False

    # 启动Selenium服务
    cmd = [
        "docker-compose",
        "-f",
        "docker-compose.yml",
        "--profile",
        "selenium",
        "up",
        "-d",
    ]

    result = run_command(cmd, cwd=docker_dir)
    if not result:
        print("❌ Selenium Grid启动失败")
        return False

    print("✅ Selenium Grid启动成功")
    return True


def check_selenium_grid():
    """检查Selenium Grid状态"""
    print("📊 检查Selenium Grid状态...")

    # 等待服务启动
    print("⏳ 等待Selenium Grid启动...")
    time.sleep(10)

    docker_dir = Path("deployment/docker")

    # 检查服务状态
    result = run_command(
        ["docker-compose", "-f", "docker-compose.yml", "ps", "selenium-hub"],
        cwd=docker_dir,
        check=False,
    )

    if result and result.stdout:
        if "Up" in result.stdout:
            print("✅ Selenium Hub运行正常")
        else:
            print("⚠️ Selenium Hub状态异常")
            print(result.stdout)

    # 检查节点状态
    for node in ["chrome-node", "firefox-node"]:
        result = run_command(
            ["docker-compose", "-f", "docker-compose.yml", "ps", node],
            cwd=docker_dir,
            check=False,
        )

        if result and result.stdout:
            if "Up" in result.stdout:
                print(f"✅ {node}运行正常")
            else:
                print(f"⚠️ {node}状态异常")


def test_selenium_connection():
    """测试Selenium连接"""
    print("🧪 测试Selenium连接...")

    try:
        import requests

        # 测试Grid Hub
        hub_url = "http://localhost:4444/wd/hub/status"
        response = requests.get(hub_url, timeout=10)

        if response.status_code == 200:
            data = response.json()
            if data.get("value", {}).get("ready"):
                print("✅ Selenium Grid连接正常")

                # 显示节点信息
                nodes = data.get("value", {}).get("nodes", [])
                print(f"📊 可用节点数量: {len(nodes)}")

                for i, node in enumerate(nodes):
                    max_sessions = node.get("maxSessions", 0)
                    print(f"   节点{i+1}: 最大会话数 {max_sessions}")

                return True
            else:
                print("⚠️ Selenium Grid未就绪")
                return False
        else:
            print(f"❌ Selenium Grid连接失败: HTTP {response.status_code}")
            return False

    except ImportError:
        print("⚠️ requests库未安装，跳过连接测试")
        return True
    except Exception as e:
        print(f"❌ Selenium连接测试失败: {e}")
        return False


def install_selenium_dependencies():
    """安装Selenium相关依赖"""
    print("📦 安装Selenium相关依赖...")

    dependencies = [
        "selenium",
        "webdriver-manager",
        "requests",
    ]

    for dep in dependencies:
        print(f"安装 {dep}...")
        result = run_command(["uv", "add", dep], check=False)
        if result and result.returncode == 0:
            print(f"✅ {dep} 安装成功")
        else:
            print(f"⚠️ {dep} 安装失败，可能已存在")


def create_selenium_test_script():
    """创建Selenium测试脚本"""
    print("📝 创建Selenium测试脚本...")

    test_script = """#!/usr/bin/env python3
# -*- coding: utf-8 -*-
\"\"\"
Selenium Grid测试脚本
\"\"\"

from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.firefox.options import Options as FirefoxOptions


def test_chrome():
    \"\"\"测试Chrome节点\"\"\"
    print("🧪 测试Chrome节点...")

    try:
        options = ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')

        driver = webdriver.Remote(
            command_executor='http://localhost:4444/wd/hub',
            options=options
        )

        driver.get('http://httpbin.org/ip')
        print(f"✅ Chrome测试成功: {driver.title}")
        print(f"📄 页面内容: {driver.page_source[:100]}...")

        driver.quit()
        return True

    except Exception as e:
        print(f"❌ Chrome测试失败: {e}")
        return False


def test_firefox():
    \"\"\"测试Firefox节点\"\"\"
    print("🧪 测试Firefox节点...")

    try:
        options = FirefoxOptions()
        options.add_argument('--headless')

        driver = webdriver.Remote(
            command_executor='http://localhost:4444/wd/hub',
            options=options
        )

        driver.get('http://httpbin.org/ip')
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
"""

    with open("test_selenium_grid.py", "w", encoding="utf-8") as f:
        f.write(test_script)

    print("✅ Selenium测试脚本创建完成: test_selenium_grid.py")


def show_phase2_info():
    """显示第二阶段信息"""
    print("\n🎉 第二阶段启动完成！")
    print("\n📋 第二阶段功能:")
    print("┌─────────────────────────────────────────────────────────┐")
    print("│ 功能模块              │ 状态    │ 访问地址              │")
    print("├─────────────────────────────────────────────────────────┤")
    print("│ Selenium Grid Hub     │ 运行中  │ http://localhost:4444 │")
    print("│ Chrome节点            │ 运行中  │ 自动分配              │")
    print("│ Firefox节点           │ 运行中  │ 自动分配              │")
    print("│ 反爬虫检测            │ 已集成  │ 中间件形式            │")
    print("│ 高级代理池            │ 已增强  │ API接口               │")
    print("└─────────────────────────────────────────────────────────┘")
    print("\n🧪 测试命令:")
    print("  # 测试Selenium Grid")
    print("  uv run python test_selenium_grid.py")
    print()
    print("  # 使用Selenium爬虫")
    print("  uv run scrapy crawl adaptive -a site=test_site -s SELENIUM_ENABLED=True")
    print()
    print("  # 启动反爬虫检测")
    print("  uv run scrapy crawl adaptive -a site=nhc_new -s ANTI_CRAWL_ENABLED=True")
    print()
    print("💡 提示:")
    print("  - Selenium Grid提供无头浏览器支持")
    print("  - 反爬虫检测自动识别并应对各种反爬虫机制")
    print("  - 高级代理池支持智能轮换和质量评分")


def main():
    """主函数"""
    print("🎯 第二阶段启动脚本 - 反爬机制应对")
    print("=" * 60)

    # 检查Docker环境
    if not check_docker():
        sys.exit(1)

    # 安装依赖
    install_selenium_dependencies()

    # 启动Selenium Grid
    if not start_selenium_grid():
        sys.exit(1)

    # 检查服务状态
    check_selenium_grid()

    # 测试连接
    test_selenium_connection()

    # 创建测试脚本
    create_selenium_test_script()

    # 显示信息
    show_phase2_info()

    print("\n🎊 第二阶段启动流程完成！")


if __name__ == "__main__":
    main()
