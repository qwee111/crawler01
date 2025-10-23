#!/usr/bin/env python3
"""
配置向导脚本

自动化生成和配置系统环境变量
"""

import os
import sys
import secrets
import string
import getpass
import shutil
from pathlib import Path
from typing import Dict, Any


class ConfigWizard:
    """配置向导"""

    def __init__(self):
        self.project_root = Path(__file__).parent
        self.env_example_path = (
            self.project_root / "deployment" / "docker" / ".env.example"
        )
        self.env_path = self.project_root / ".env"
        self.docker_env_path = self.project_root / "deployment" / "docker" / ".env"

        self.config = {}

    def welcome(self):
        """欢迎信息"""
        print("🚀 企业级分布式爬虫系统配置向导")
        print("=" * 50)
        print("本向导将帮助您配置系统环境变量")
        print("包括数据库密码、安全密钥、告警配置等")
        print()

    def generate_secure_password(self, length: int = 16) -> str:
        """生成安全密码"""
        alphabet = string.ascii_letters + string.digits + "!@#$%^&*"
        return "".join(secrets.choice(alphabet) for _ in range(length))

    def generate_secret_key(self, length: int = 50) -> str:
        """生成密钥"""
        alphabet = string.ascii_letters + string.digits + "!@#$%^&*()-_=+[]{}|;:,.<>?"
        return "".join(secrets.choice(alphabet) for _ in range(length))

    def ask_yes_no(self, question: str, default: bool = True) -> bool:
        """询问是否问题"""
        default_str = "Y/n" if default else "y/N"
        while True:
            answer = input(f"{question} [{default_str}]: ").strip().lower()
            if not answer:
                return default
            if answer in ["y", "yes", "是"]:
                return True
            elif answer in ["n", "no", "否"]:
                return False
            print("请输入 y/yes 或 n/no")

    def ask_input(
        self,
        question: str,
        default: str = "",
        required: bool = False,
        password: bool = False,
    ) -> str:
        """询问输入"""
        while True:
            if password:
                value = getpass.getpass(f"{question}: ")
            else:
                prompt = f"{question}"
                if default:
                    prompt += f" [{default}]"
                prompt += ": "
                value = input(prompt).strip()

            if not value and default:
                return default
            elif not value and required:
                print("此项为必填项，请输入值")
                continue
            else:
                return value

    def configure_environment(self):
        """配置环境"""
        print("📋 1. 环境配置")
        print("-" * 30)

        env_choices = {"1": "development", "2": "testing", "3": "production"}

        print("选择运行环境:")
        for key, value in env_choices.items():
            print(f"  {key}. {value}")

        while True:
            choice = input("请选择 [1]: ").strip() or "1"
            if choice in env_choices:
                self.config["ENVIRONMENT"] = env_choices[choice]
                break
            print("无效选择，请重新输入")

        self.config["DEBUG"] = (
            "true" if self.config["ENVIRONMENT"] == "development" else "false"
        )
        self.config["LOG_LEVEL"] = (
            "DEBUG" if self.config["ENVIRONMENT"] == "development" else "INFO"
        )

        print(f"✅ 环境设置为: {self.config['ENVIRONMENT']}")
        print()

    def configure_databases(self):
        """配置数据库"""
        print("🗄️ 2. 数据库配置")
        print("-" * 30)

        # 已移除PostgreSQL配置

        # MongoDB配置
        print("\nMongoDB 配置:")
        self.config["MONGODB_DATABASE"] = self.ask_input("数据库名", "crawler_db")
        self.config["MONGODB_ROOT_USERNAME"] = self.ask_input("管理员用户名", "admin")

        if self.ask_yes_no("自动生成MongoDB密码?"):
            self.config["MONGODB_ROOT_PASSWORD"] = self.generate_secure_password()
            print(f"✅ 已生成密码: {self.config['MONGODB_ROOT_PASSWORD']}")
        else:
            self.config["MONGODB_ROOT_PASSWORD"] = self.ask_input(
                "MongoDB密码", required=True, password=True
            )

        # Redis配置
        print("\nRedis 配置:")
        if self.ask_yes_no("为Redis设置密码? (推荐)"):
            if self.ask_yes_no("自动生成Redis密码?"):
                self.config["REDIS_PASSWORD"] = self.generate_secure_password()
                print(f"✅ 已生成密码: {self.config['REDIS_PASSWORD']}")
            else:
                self.config["REDIS_PASSWORD"] = self.ask_input("Redis密码", password=True)
        else:
            self.config["REDIS_PASSWORD"] = ""

        print("✅ 数据库配置完成")
        print()

    def configure_security(self):
        """配置安全设置"""
        print("🔐 3. 安全配置")
        print("-" * 30)

        print("生成安全密钥...")
        self.config["SECRET_KEY"] = self.generate_secret_key(64)
        self.config["API_TOKEN"] = self.generate_secret_key(32)
        self.config["JWT_SECRET"] = self.generate_secret_key(64)

        print("✅ 安全密钥已生成")

        # 管理界面密码
        print("\n管理界面配置:")

        # MongoDB Express
        if self.ask_yes_no("自动生成MongoDB管理界面密码?"):
            self.config["MONGO_EXPRESS_PASSWORD"] = self.generate_secure_password()
            print(f"✅ MongoDB Express密码: {self.config['MONGO_EXPRESS_PASSWORD']}")
        else:
            self.config["MONGO_EXPRESS_PASSWORD"] = self.ask_input(
                "MongoDB Express密码", password=True
            )

        # 已移除 pgAdmin 配置

        # MinIO
        if self.ask_yes_no("自动生成MinIO密码?"):
            self.config["MINIO_ROOT_PASSWORD"] = self.generate_secure_password()
            print(f"✅ MinIO密码: {self.config['MINIO_ROOT_PASSWORD']}")
        else:
            self.config["MINIO_ROOT_PASSWORD"] = self.ask_input(
                "MinIO密码", password=True
            )

        print("✅ 安全配置完成")
        print()

    def configure_notifications(self):
        """配置通知设置"""
        print("📧 4. 通知配置 (可选)")
        print("-" * 30)

        # 邮件配置
        if self.ask_yes_no("配置邮件告警?", False):
            print("邮件配置:")
            self.config["SMTP_HOST"] = self.ask_input("SMTP服务器", "smtp.gmail.com")
            self.config["SMTP_PORT"] = self.ask_input("SMTP端口", "587")
            self.config["SMTP_USERNAME"] = self.ask_input("邮箱用户名", required=True)
            self.config["SMTP_PASSWORD"] = self.ask_input(
                "邮箱密码/应用密码", required=True, password=True
            )
            self.config["SMTP_FROM"] = self.ask_input(
                "发件人邮箱", self.config["SMTP_USERNAME"]
            )
        else:
            self.config.update(
                {
                    "SMTP_HOST": "smtp.gmail.com",
                    "SMTP_PORT": "587",
                    "SMTP_USERNAME": "",
                    "SMTP_PASSWORD": "",
                    "SMTP_FROM": "",
                }
            )

        # Slack配置
        if self.ask_yes_no("配置Slack告警?", False):
            print("Slack配置:")
            self.config["SLACK_WEBHOOK_URL"] = self.ask_input(
                "Slack Webhook URL", required=True
            )
            self.config["SLACK_CHANNEL"] = self.ask_input("Slack频道", "#crawler-alerts")
        else:
            self.config.update(
                {"SLACK_WEBHOOK_URL": "", "SLACK_CHANNEL": "#crawler-alerts"}
            )

        print("✅ 通知配置完成")
        print()

    def configure_crawler_settings(self):
        """配置爬虫设置"""
        print("🕷️ 5. 爬虫配置")
        print("-" * 30)

        self.config["CONCURRENT_REQUESTS"] = self.ask_input("并发请求数", "16")
        self.config["DOWNLOAD_DELAY"] = self.ask_input("下载延迟(秒)", "1")
        self.config["PROXY_POOL_SIZE"] = self.ask_input("代理池大小", "100")

        # 第三方服务API
        if self.ask_yes_no("配置验证码识别服务?", False):
            self.config["CAPTCHA_SERVICE_API_KEY"] = self.ask_input("验证码服务API Key")
        else:
            self.config["CAPTCHA_SERVICE_API_KEY"] = ""

        if self.ask_yes_no("配置代理服务?", False):
            self.config["PROXY_SERVICE_API_KEY"] = self.ask_input("代理服务API Key")
        else:
            self.config["PROXY_SERVICE_API_KEY"] = ""

        print("✅ 爬虫配置完成")
        print()

    def load_template(self) -> str:
        """加载模板文件"""
        if not self.env_example_path.exists():
            raise FileNotFoundError(f"模板文件不存在: {self.env_example_path}")

        with open(self.env_example_path, "r", encoding="utf-8") as f:
            return f.read()

    def generate_env_content(self) -> str:
        """生成环境变量文件内容"""
        template = self.load_template()

        # 设置默认值
        default_config = {
            "REDIS_HOST": "localhost",
            "REDIS_PORT": "6379",
            "REDIS_DB": "0",
            "MONGODB_HOST": "localhost",
            "MONGODB_PORT": "27017",
            "MINIO_ROOT_USER": "minioadmin",
            "MINIO_HOST": "localhost",
            "MINIO_PORT": "9000",
            "MONGO_EXPRESS_USER": "admin",
            "PROMETHEUS_PORT": "8000",
            "METRICS_ENABLED": "true",
            "ALERT_WEBHOOK_URL": "",
            "RANDOMIZE_DOWNLOAD_DELAY": "0.5",
            "RETRY_TIMES": "3",
            "PROXY_VALIDATION_TIMEOUT": "10",
            "PROXY_ROTATION_INTERVAL": "100",
            "MIN_QUALITY_SCORE": "0.7",
            "MAX_ERROR_RATE": "0.1",
            "MIN_SUCCESS_RATE": "0.8",
            "TZ": "UTC",
        }

        # 合并配置
        final_config = {**default_config, **self.config}

        # 替换模板中的值
        content = template
        for key, value in final_config.items():
            content = content.replace(f"{key}=", f"{key}={value}")

        return content

    def save_config_files(self):
        """保存配置文件"""
        print("💾 6. 保存配置")
        print("-" * 30)

        content = self.generate_env_content()

        # 保存到项目根目录
        with open(self.env_path, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"✅ 已保存: {self.env_path}")

        # 保存到Docker目录
        self.docker_env_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.docker_env_path, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"✅ 已保存: {self.docker_env_path}")

        print()

    def show_summary(self):
        """显示配置摘要"""
        print("📋 配置摘要")
        print("=" * 50)
        print(f"环境: {self.config.get('ENVIRONMENT', 'development')}")
        # 已移除PostgreSQL用户输出
        print(f"MongoDB用户: {self.config.get('MONGODB_ROOT_USERNAME', 'admin')}")
        print(f"Redis密码: {'已设置' if self.config.get('REDIS_PASSWORD') else '未设置'}")
        print(f"邮件告警: {'已配置' if self.config.get('SMTP_USERNAME') else '未配置'}")
        print(f"Slack告警: {'已配置' if self.config.get('SLACK_WEBHOOK_URL') else '未配置'}")
        print()

        print("🔐 重要信息 (请妥善保管)")
        print("-" * 30)
        # 已移除PostgreSQL密码输出
        if self.config.get("MONGODB_ROOT_PASSWORD"):
            print(f"MongoDB密码: {self.config['MONGODB_ROOT_PASSWORD']}")
        if self.config.get("REDIS_PASSWORD"):
            print(f"Redis密码: {self.config['REDIS_PASSWORD']}")
        if self.config.get("MONGO_EXPRESS_PASSWORD"):
            print(f"MongoDB管理界面密码: {self.config['MONGO_EXPRESS_PASSWORD']}")
        # 已移除pgAdmin密码输出
        if self.config.get("MINIO_ROOT_PASSWORD"):
            print(f"MinIO密码: {self.config['MINIO_ROOT_PASSWORD']}")
        print()

        print("🚀 下一步")
        print("-" * 30)
        print("1. 启动存储服务:")
        print("   python deployment/scripts/start_storage.py start --with-tools")
        print()
        print("2. 运行爬虫:")
        print("   scrapy crawl nhc")
        print()
        print("3. 访问管理界面:")
        print("   - MongoDB: http://localhost:8082")
        print("   - pgAdmin: http://localhost:8083")
        print("   - Redis: http://localhost:8081")
        print("   - MinIO: http://localhost:9001")
        print()

    def run(self):
        """运行配置向导"""
        try:
            self.welcome()
            self.configure_environment()
            self.configure_databases()
            self.configure_security()
            self.configure_notifications()
            self.configure_crawler_settings()
            self.save_config_files()
            self.show_summary()

            print("🎉 配置完成！")

        except KeyboardInterrupt:
            print("\n\n❌ 配置已取消")
            sys.exit(1)
        except Exception as e:
            print(f"\n❌ 配置失败: {e}")
            sys.exit(1)


def main():
    """主函数"""
    wizard = ConfigWizard()
    wizard.run()


if __name__ == "__main__":
    main()
