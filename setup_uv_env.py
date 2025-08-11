#!/usr/bin/env python3
"""
UV环境设置脚本

专门为uv包管理器设计的环境设置脚本
"""

import logging
import os
import subprocess
import sys
from pathlib import Path

# 配置日志
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def run_command(command, check=True, shell=False):
    """执行命令并返回结果"""
    print(f"执行命令: {' '.join(command) if isinstance(command, list) else command}")
    try:
        if isinstance(command, str) and not shell:
            command = command.split()

        result = subprocess.run(
            command, check=check, capture_output=True, text=True, shell=shell
        )

        if result.stdout:
            print(f"输出: {result.stdout}")
        if result.stderr and result.returncode != 0:
            print(f"错误: {result.stderr}")

        return result
    except subprocess.CalledProcessError as e:
        print(f"命令执行失败: {e}")
        if e.stdout:
            print(f"标准输出: {e.stdout}")
        if e.stderr:
            print(f"错误输出: {e.stderr}")
        return None


def check_uv():
    """检查uv是否安装"""
    print("检查uv包管理器...")

    try:
        result = subprocess.run(
            ["uv", "--version"], capture_output=True, text=True, check=True
        )
        print(f"✅ UV版本: {result.stdout.strip()}")
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("❌ uv未安装或不可用")
        print("请先安装uv: https://docs.astral.sh/uv/getting-started/installation/")
        return False


def sync_dependencies():
    """同步依赖"""
    print("同步项目依赖...")

    # 使用uv sync来安装依赖
    result = run_command(["uv", "sync"])
    if result is not None:
        print("✅ 依赖同步完成")
        return True
    else:
        print("❌ 依赖同步失败")
        return False


def install_pre_commit():
    """安装pre-commit钩子"""
    print("设置pre-commit钩子...")

    if not Path(".pre-commit-config.yaml").exists():
        print("⚠️ .pre-commit-config.yaml 不存在")
        return

    # 使用uv run来运行pre-commit
    result = run_command(["uv", "run", "pre-commit", "install"])
    if result is not None:
        print("✅ pre-commit钩子安装成功")
    else:
        print("⚠️ pre-commit钩子安装失败")


def create_env_file():
    """创建环境变量文件"""
    print("检查环境变量文件...")

    env_file = Path(".env")
    if env_file.exists():
        print("✅ .env文件已存在")
        return

    env_example = Path("deployment/docker/.env.example")
    if env_example.exists():
        print("从示例文件创建.env...")
        import shutil

        shutil.copy(env_example, env_file)
        print("✅ .env文件创建成功")
        print("⚠️ 请编辑.env文件设置正确的配置")
    else:
        print("⚠️ 示例配置文件不存在，建议运行: python quick_setup.py")


def setup_vscode_config():
    """设置VSCode配置"""
    print("设置VSCode配置...")

    vscode_dir = Path(".vscode")
    vscode_dir.mkdir(exist_ok=True)

    # settings.json for uv
    settings_content = """{
    "python.defaultInterpreterPath": "./.venv/bin/python",
    "python.terminal.activateEnvironment": true,
    "python.linting.enabled": true,
    "python.linting.flake8Enabled": true,
    "python.linting.mypyEnabled": true,
    "python.formatting.provider": "black",
    "python.formatting.blackArgs": ["--line-length=88"],
    "python.sortImports.args": ["--profile=black"],
    "editor.formatOnSave": true,
    "editor.codeActionsOnSave": {
        "source.organizeImports": true
    },
    "files.exclude": {
        "**/__pycache__": true,
        "**/*.pyc": true,
        ".pytest_cache": true,
        ".mypy_cache": true,
        "*.egg-info": true
    },
    "python.testing.pytestEnabled": true,
    "python.testing.pytestArgs": [
        "tests"
    ]
}"""

    with open(vscode_dir / "settings.json", "w", encoding="utf-8") as f:
        f.write(settings_content)

    # launch.json for debugging
    launch_content = """{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Python: 当前文件",
            "type": "python",
            "request": "launch",
            "program": "${file}",
            "console": "integratedTerminal",
            "justMyCode": true
        },
        {
            "name": "Python: 爬虫调试",
            "type": "python",
            "request": "launch",
            "module": "scrapy",
            "args": ["crawl", "nhc"],
            "console": "integratedTerminal",
            "justMyCode": false
        },
        {
            "name": "Python: 配置验证",
            "type": "python",
            "request": "launch",
            "program": "${workspaceFolder}/validate_config.py",
            "console": "integratedTerminal",
            "justMyCode": true
        }
    ]
}"""

    with open(vscode_dir / "launch.json", "w", encoding="utf-8") as f:
        f.write(launch_content)

    print("✅ VSCode配置完成")


def show_next_steps():
    """显示下一步操作"""
    print("\n🎉 UV环境设置完成!")
    print("\n下一步:")
    print("1. 生成配置文件:")
    print("   uv run quick_setup.py")
    print()
    print("2. 验证配置:")
    print("   uv run validate_config.py")
    print()
    print("3. 启动存储服务:")
    print("   uv run deployment/scripts/start_storage.py start --with-tools")
    print()
    print("4. 运行爬虫:")
    print("   uv run scrapy crawl nhc")
    print()
    print("5. 运行测试:")
    print("   uv run pytest")
    print()
    print("💡 提示:")
    print("   - 使用 'uv run <script>' 来运行Python脚本")
    print("   - 使用 'uv add <package>' 来添加新依赖")
    print("   - 使用 'uv sync' 来同步依赖")


def main():
    """主函数"""
    print("🚀 开始设置UV环境...")

    # 检查uv
    if not check_uv():
        sys.exit(1)

    # 同步依赖
    if not sync_dependencies():
        print("⚠️ 依赖同步失败，但继续设置其他配置...")

    # 安装pre-commit
    install_pre_commit()

    # 创建环境变量文件
    create_env_file()

    # 设置VSCode配置
    setup_vscode_config()

    # 显示下一步
    show_next_steps()


if __name__ == "__main__":
    main()
