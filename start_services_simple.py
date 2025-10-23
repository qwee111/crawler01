#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简化的服务启动脚本

避免编码问题，提供更稳定的服务启动
"""

import os
import subprocess
import sys
import time
from pathlib import Path


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
            errors="ignore",  # 忽略编码错误
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
    """检查Docker是否可用"""
    print("🔍 检查Docker...")

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


def start_services():
    """启动服务"""
    print("🚀 启动存储服务...")

    # 切换到docker目录
    docker_dir = Path("deployment/docker")
    if not docker_dir.exists():
        print(f"❌ Docker配置目录不存在: {docker_dir}")
        return False

    # 启动服务
    cmd = [
        "docker-compose",
        "-f",
        "docker-compose.yml",
        "--profile",
        "tools",
        "up",
        "-d",
        "--remove-orphans",
    ]

    result = run_command(cmd, cwd=docker_dir)
    if not result:
        print("❌ 服务启动失败")
        return False

    print("✅ 服务启动命令执行成功")
    return True


def check_services():
    """检查服务状态"""
    print("📊 检查服务状态...")

    docker_dir = Path("deployment/docker")

    # 等待几秒让服务启动
    print("⏳ 等待服务启动...")
    time.sleep(5)

    # 检查服务状态
    result = run_command(
        ["docker-compose", "-f", "docker-compose.yml", "ps"],
        cwd=docker_dir,
        check=False,
    )

    if result and result.stdout:
        print("📋 服务状态:")
        print(result.stdout)

        # 简单统计运行中的服务
        running_count = result.stdout.count("Up")
        print(f"🔢 运行中的服务数量: {running_count}")

        if running_count >= 3:
            print("✅ 主要服务已启动")
            return True
        else:
            print("⚠️ 部分服务可能未完全启动")
            return False
    else:
        print("⚠️ 无法获取服务状态")
        return False


def show_access_info():
    """显示访问信息"""
    print("\n🎉 服务启动完成！")
    print("\n📋 服务访问地址:")
    print("┌─────────────────────────────────────────────────────────┐")
    print("│ 服务名称          │ 访问地址                          │")
    print("├─────────────────────────────────────────────────────────┤")
    print("│ Redis Commander   │ http://localhost:8081             │")
    print("│ MongoDB Express   │ http://localhost:8082             │")
    print("│ MinIO Console     │ http://localhost:9001             │")
    print("└─────────────────────────────────────────────────────────┘")
    print("\n💡 提示:")
    print("  - 如果服务未完全启动，请等待1-2分钟后再访问")
    print("  - 可以运行以下命令查看详细状态:")
    print("    docker-compose -f deployment/docker/docker-compose.yml ps")
    print("    docker-compose -f deployment/docker/docker-compose.yml logs")


def main():
    """主函数"""
    print("🎯 简化服务启动脚本")
    print("=" * 50)

    # 检查Docker环境
    if not check_docker():
        sys.exit(1)

    # 启动服务
    if not start_services():
        sys.exit(1)

    # 检查服务状态
    check_services()

    # 显示访问信息
    show_access_info()

    print("\n🎊 启动流程完成！")


if __name__ == "__main__":
    main()
