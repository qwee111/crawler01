#!/usr/bin/env python3
"""
配置验证脚本

验证环境配置是否正确
"""

import os
import sys
import time
from pathlib import Path
from typing import Dict, List, Tuple


class ConfigValidator:
    """配置验证器"""

    def __init__(self):
        self.project_root = Path(__file__).parent
        self.env_path = self.project_root / ".env"
        self.docker_env_path = self.project_root / "deployment" / "docker" / ".env"
        self.config = {}
        self.errors = []
        self.warnings = []

    def load_env_file(self, env_path: Path) -> Dict[str, str]:
        """加载环境变量文件"""
        config = {}

        if not env_path.exists():
            return config

        try:
            with open(env_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        config[key.strip()] = value.strip()
        except Exception as e:
            self.errors.append(f"读取配置文件失败 {env_path}: {e}")

        return config

    def check_file_exists(self):
        """检查配置文件是否存在"""
        print("📁 检查配置文件...")

        if not self.env_path.exists():
            self.errors.append(f"主配置文件不存在: {self.env_path}")
        else:
            print(f"✅ 主配置文件存在: {self.env_path}")

        if not self.docker_env_path.exists():
            self.warnings.append(f"Docker配置文件不存在: {self.docker_env_path}")
        else:
            print(f"✅ Docker配置文件存在: {self.docker_env_path}")

        print()

    def check_required_variables(self):
        """检查必需的环境变量"""
        print("🔍 检查必需的环境变量...")

        required_vars = [
            'ENVIRONMENT',
            'POSTGRES_DB',
            'POSTGRES_USER',
            'POSTGRES_PASSWORD',
            'MONGODB_ROOT_USERNAME',
            'MONGODB_ROOT_PASSWORD',
            'MONGODB_DATABASE',
            'SECRET_KEY',
            'API_TOKEN',
            'JWT_SECRET'
        ]

        missing_vars = []

        for var in required_vars:
            if var not in self.config or not self.config[var]:
                missing_vars.append(var)
            else:
                print(f"✅ {var}: 已设置")

        if missing_vars:
            self.errors.append(f"缺少必需的环境变量: {', '.join(missing_vars)}")

        print()

    def check_password_strength(self):
        """检查密码强度"""
        print("🔐 检查密码强度...")

        password_vars = [
            'POSTGRES_PASSWORD',
            'MONGODB_ROOT_PASSWORD',
            'REDIS_PASSWORD',
            'MONGO_EXPRESS_PASSWORD',
            'PGADMIN_PASSWORD',
            'MINIO_ROOT_PASSWORD'
        ]

        for var in password_vars:
            password = self.config.get(var, '')
            if not password:
                if var == 'REDIS_PASSWORD':
                    self.warnings.append(f"{var}: 未设置密码（可选但推荐）")
                else:
                    self.errors.append(f"{var}: 密码为空")
                continue

            # 检查密码强度
            issues = []
            if len(password) < 8:
                issues.append("长度少于8位")
            if not any(c.isupper() for c in password):
                issues.append("缺少大写字母")
            if not any(c.islower() for c in password):
                issues.append("缺少小写字母")
            if not any(c.isdigit() for c in password):
                issues.append("缺少数字")
            if not any(c in "!@#$%^&*()_+-=[]{}|;:,.<>?" for c in password):
                issues.append("缺少特殊字符")

            if issues:
                self.warnings.append(f"{var}: 密码强度不足 - {', '.join(issues)}")
            else:
                print(f"✅ {var}: 密码强度良好")

        print()

    def check_security_keys(self):
        """检查安全密钥"""
        print("🔑 检查安全密钥...")

        key_vars = ['SECRET_KEY', 'API_TOKEN', 'JWT_SECRET']

        for var in key_vars:
            key = self.config.get(var, '')
            if not key:
                self.errors.append(f"{var}: 密钥为空")
                continue

            if len(key) < 32:
                self.warnings.append(f"{var}: 密钥长度不足32位")
            elif key.startswith('your-') or 'change-this' in key:
                self.errors.append(f"{var}: 使用了默认值，请更改")
            else:
                print(f"✅ {var}: 密钥设置正确")

        print()

    def check_database_config(self):
        """检查数据库配置"""
        print("🗄️ 检查数据库配置...")

        # PostgreSQL
        pg_config = {
            'host': self.config.get('POSTGRES_HOST', 'localhost'),
            'port': self.config.get('POSTGRES_PORT', '5432'),
            'database': self.config.get('POSTGRES_DB', ''),
            'user': self.config.get('POSTGRES_USER', ''),
            'password': self.config.get('POSTGRES_PASSWORD', '')
        }

        if all(pg_config.values()):
            print("✅ PostgreSQL配置完整")
        else:
            missing = [k for k, v in pg_config.items() if not v]
            self.errors.append(f"PostgreSQL配置不完整: {', '.join(missing)}")

        # MongoDB
        mongo_config = {
            'host': self.config.get('MONGODB_HOST', 'localhost'),
            'port': self.config.get('MONGODB_PORT', '27017'),
            'database': self.config.get('MONGODB_DATABASE', ''),
            'username': self.config.get('MONGODB_ROOT_USERNAME', ''),
            'password': self.config.get('MONGODB_ROOT_PASSWORD', '')
        }

        if all(mongo_config.values()):
            print("✅ MongoDB配置完整")
        else:
            missing = [k for k, v in mongo_config.items() if not v]
            self.errors.append(f"MongoDB配置不完整: {', '.join(missing)}")

        # Redis
        redis_host = self.config.get('REDIS_HOST', 'localhost')
        redis_port = self.config.get('REDIS_PORT', '6379')

        if redis_host and redis_port:
            print("✅ Redis配置完整")
        else:
            self.errors.append("Redis配置不完整")

        print()

    def check_crawler_config(self):
        """检查爬虫配置"""
        print("🕷️ 检查爬虫配置...")

        crawler_vars = {
            'CONCURRENT_REQUESTS': (1, 100),
            'DOWNLOAD_DELAY': (0, 10),
            'PROXY_POOL_SIZE': (1, 1000),
            'RETRY_TIMES': (1, 10)
        }

        for var, (min_val, max_val) in crawler_vars.items():
            value = self.config.get(var, '')
            if not value:
                self.warnings.append(f"{var}: 未设置，将使用默认值")
                continue

            try:
                num_value = float(value)
                if min_val <= num_value <= max_val:
                    print(f"✅ {var}: {value}")
                else:
                    self.warnings.append(f"{var}: 值 {value} 超出推荐范围 [{min_val}, {max_val}]")
            except ValueError:
                self.errors.append(f"{var}: 无效的数值 '{value}'")

        print()

    def check_notification_config(self):
        """检查通知配置"""
        print("📧 检查通知配置...")

        # 邮件配置
        smtp_vars = ['SMTP_HOST', 'SMTP_PORT', 'SMTP_USERNAME', 'SMTP_PASSWORD']
        smtp_configured = any(self.config.get(var) for var in smtp_vars)

        if smtp_configured:
            missing_smtp = [var for var in smtp_vars if not self.config.get(var)]
            if missing_smtp:
                self.warnings.append(f"邮件配置不完整，缺少: {', '.join(missing_smtp)}")
            else:
                print("✅ 邮件配置完整")
        else:
            print("ℹ️ 邮件通知未配置")

        # Slack配置
        slack_webhook = self.config.get('SLACK_WEBHOOK_URL')
        if slack_webhook:
            if slack_webhook.startswith('https://hooks.slack.com/'):
                print("✅ Slack配置正确")
            else:
                self.warnings.append("Slack Webhook URL格式可能不正确")
        else:
            print("ℹ️ Slack通知未配置")

        print()

    def test_connections(self):
        """测试连接（可选）"""
        print("🔗 测试数据库连接...")

        # 这里可以添加实际的连接测试
        # 但需要确保服务已启动
        print("ℹ️ 连接测试需要服务运行，请使用以下命令启动服务:")
        print("   python deployment/scripts/start_storage.py start")
        print()

    def generate_report(self):
        """生成验证报告"""
        print("📋 验证报告")
        print("=" * 50)

        if not self.errors and not self.warnings:
            print("🎉 配置验证通过！所有检查项目都正常。")
        else:
            if self.errors:
                print("❌ 发现错误:")
                for i, error in enumerate(self.errors, 1):
                    print(f"   {i}. {error}")
                print()

            if self.warnings:
                print("⚠️ 发现警告:")
                for i, warning in enumerate(self.warnings, 1):
                    print(f"   {i}. {warning}")
                print()

        print("📊 统计:")
        print(f"   错误: {len(self.errors)}")
        print(f"   警告: {len(self.warnings)}")
        print(f"   状态: {'❌ 需要修复' if self.errors else '✅ 可以使用'}")
        print()

        if self.errors:
            print("🔧 修复建议:")
            print("   1. 运行配置向导: python setup_config.py")
            print("   2. 或快速配置: python quick_setup.py")
            print("   3. 手动编辑 .env 文件")
            print()

    def validate(self):
        """执行验证"""
        print("🔍 开始配置验证...")
        print()

        # 加载配置
        self.config = self.load_env_file(self.env_path)

        # 执行检查
        self.check_file_exists()
        self.check_required_variables()
        self.check_password_strength()
        self.check_security_keys()
        self.check_database_config()
        self.check_crawler_config()
        self.check_notification_config()
        self.test_connections()

        # 生成报告
        self.generate_report()

        return len(self.errors) == 0


def main():
    """主函数"""
    validator = ConfigValidator()

    try:
        success = validator.validate()
        return 0 if success else 1
    except Exception as e:
        print(f"❌ 验证过程中发生错误: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
