#!/usr/bin/env python3
"""
开发环境设置脚本
自动化配置Python环境、依赖安装和开发工具
"""

import os
import sys
import subprocess
import platform
from pathlib import Path


def run_command(command, check=True, shell=False):
    """执行命令并返回结果"""
    print(f"执行命令: {command}")
    try:
        if isinstance(command, str) and not shell:
            command = command.split()

        result = subprocess.run(
            command, check=check, capture_output=True, text=True, shell=shell
        )

        if result.stdout:
            print(f"输出: {result.stdout}")
        if result.stderr:
            print(f"错误: {result.stderr}")

        return result
    except subprocess.CalledProcessError as e:
        print(f"命令执行失败: {e}")
        if e.stdout:
            print(f"标准输出: {e.stdout}")
        if e.stderr:
            print(f"错误输出: {e.stderr}")
        return None


def check_python_version():
    """检查Python版本"""
    print("检查Python版本...")
    version = sys.version_info

    if version.major != 3 or version.minor < 9:
        print(f"错误: 需要Python 3.9+，当前版本: {version.major}.{version.minor}")
        sys.exit(1)

    print(f"✅ Python版本检查通过: {version.major}.{version.minor}.{version.micro}")


def setup_virtual_environment():
    """设置虚拟环境"""
    print("设置虚拟环境...")

    venv_path = Path(".venv")

    if venv_path.exists():
        print("虚拟环境已存在")
        return

    # 创建虚拟环境
    result = run_command([sys.executable, "-m", "venv", ".venv"])
    if result is None:
        print("创建虚拟环境失败")
        sys.exit(1)

    print("✅ 虚拟环境创建成功")


def get_venv_python():
    """获取虚拟环境中的Python路径"""
    if platform.system() == "Windows":
        return Path(".venv/Scripts/python.exe")
    else:
        return Path(".venv/bin/python")


def install_dependencies():
    """安装项目依赖"""
    print("安装项目依赖...")

    python_path = get_venv_python()

    if not python_path.exists():
        print("虚拟环境Python不存在，请先创建虚拟环境")
        sys.exit(1)

    # 升级pip
    run_command([str(python_path), "-m", "pip", "install", "--upgrade", "pip"])

    # 安装依赖
    if Path("requirements.txt").exists():
        run_command(
            [str(python_path), "-m", "pip", "install", "-r", "requirements.txt"]
        )
        print("✅ 项目依赖安装完成")
    else:
        print("⚠️ requirements.txt 不存在")


def setup_pre_commit():
    """设置pre-commit钩子"""
    print("设置pre-commit钩子...")

    python_path = get_venv_python()

    if not Path(".pre-commit-config.yaml").exists():
        print("⚠️ .pre-commit-config.yaml 不存在")
        return

    # 安装pre-commit钩子
    result = run_command([str(python_path), "-m", "pre_commit", "install"])
    if result:
        print("✅ pre-commit钩子安装成功")


def create_env_file():
    """创建环境变量文件"""
    print("创建环境变量文件...")

    env_file = Path(".env")
    if env_file.exists():
        print(".env文件已存在")
        return

    env_content = """# 开发环境配置
DEBUG=True
LOG_LEVEL=DEBUG

# Redis配置
REDIS_URL=redis://localhost:6379/0
REDIS_PASSWORD=

# MongoDB配置
MONGODB_URL=mongodb://localhost:27017/crawler_db
MONGODB_USERNAME=
MONGODB_PASSWORD=

# PostgreSQL配置
POSTGRES_URL=postgresql://postgres:password@localhost:5432/crawler_db
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=crawler_db
POSTGRES_USER=postgres
POSTGRES_PASSWORD=password

# 代理配置
PROXY_POOL_SIZE=100
PROXY_VALIDATION_TIMEOUT=10

# 爬虫配置
CONCURRENT_REQUESTS=16
DOWNLOAD_DELAY=1
RANDOMIZE_DOWNLOAD_DELAY=0.5

# 监控配置
PROMETHEUS_PORT=8000
METRICS_ENABLED=True

# 安全配置
SECRET_KEY=your-secret-key-here
API_TOKEN=your-api-token-here
"""

    with open(env_file, "w", encoding="utf-8") as f:
        f.write(env_content)

    print("✅ .env文件创建成功")


def setup_vscode_config():
    """设置VSCode配置"""
    print("设置VSCode配置...")

    vscode_dir = Path(".vscode")
    vscode_dir.mkdir(exist_ok=True)

    # settings.json
    settings_content = """{
    "python.defaultInterpreterPath": "./.venv/Scripts/python.exe",
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
    }
}"""

    with open(vscode_dir / "settings.json", "w", encoding="utf-8") as f:
        f.write(settings_content)

    # launch.json
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
            "args": ["crawl", "test_spider"],
            "console": "integratedTerminal",
            "justMyCode": false
        }
    ]
}"""

    with open(vscode_dir / "launch.json", "w", encoding="utf-8") as f:
        f.write(launch_content)

    print("✅ VSCode配置完成")


def main():
    """主函数"""
    print("🚀 开始设置开发环境...")

    # 检查Python版本
    check_python_version()

    # 设置虚拟环境
    setup_virtual_environment()

    # 安装依赖
    install_dependencies()

    # 设置pre-commit
    setup_pre_commit()

    # 创建环境变量文件
    create_env_file()

    # 设置VSCode配置
    setup_vscode_config()

    print("\n🎉 开发环境设置完成!")
    print("\n下一步:")
    print("1. 激活虚拟环境:")
    if platform.system() == "Windows":
        print("   .venv\\Scripts\\activate")
    else:
        print("   source .venv/bin/activate")
    print("2. 运行测试: pytest")
    print("3. 启动开发服务器: python -m crawler.main")


if __name__ == "__main__":
    main()
