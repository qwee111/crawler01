#!/usr/bin/env python3
"""
快速配置脚本

一键生成开发环境配置，使用安全的默认值
"""

import os
import secrets
import string
from pathlib import Path


def generate_password(length=16):
    """生成安全密码"""
    alphabet = string.ascii_letters + string.digits + "!@#$%^&*"
    return "".join(secrets.choice(alphabet) for _ in range(length))


def generate_secret_key(length=50):
    """生成密钥"""
    alphabet = string.ascii_letters + string.digits + "!@#$%^&*()-_=+[]{}|;:,.<>?"
    return "".join(secrets.choice(alphabet) for _ in range(length))


def create_env_file():
    """创建环境变量文件"""
    project_root = Path(__file__).parent
    env_path = project_root / ".env"
    docker_env_path = project_root / "deployment" / "docker" / ".env"

    # 生成密码
    postgres_password = generate_password()
    mongodb_password = generate_password()
    redis_password = generate_password()
    mongo_express_password = generate_password()
    pgadmin_password = generate_password()
    minio_password = generate_password()

    # 生成密钥
    secret_key = generate_secret_key(64)
    api_token = generate_secret_key(32)
    jwt_secret = generate_secret_key(64)

    # 环境变量内容
    env_content = f"""# 自动生成的开发环境配置
# 生成时间: {os.popen('date').read().strip()}

# 应用环境
ENVIRONMENT=development
DEBUG=true
LOG_LEVEL=DEBUG

# Redis配置
REDIS_PASSWORD={redis_password}
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0

# MongoDB配置
MONGODB_ROOT_USERNAME=admin
MONGODB_ROOT_PASSWORD={mongodb_password}
MONGODB_DATABASE=crawler_db
MONGODB_HOST=localhost
MONGODB_PORT=27017

# PostgreSQL配置
POSTGRES_DB=crawler_db
POSTGRES_USER=crawler_user
POSTGRES_PASSWORD={postgres_password}
POSTGRES_HOST=localhost
POSTGRES_PORT=5432

# MinIO配置
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD={minio_password}
MINIO_HOST=localhost
MINIO_PORT=9000

# 管理界面配置
MONGO_EXPRESS_USER=admin
MONGO_EXPRESS_PASSWORD={mongo_express_password}
PGADMIN_EMAIL=admin@crawler.com
PGADMIN_PASSWORD={pgadmin_password}

# 爬虫配置
CONCURRENT_REQUESTS=16
DOWNLOAD_DELAY=1
RANDOMIZE_DOWNLOAD_DELAY=0.5
RETRY_TIMES=3

# 代理配置
PROXY_POOL_SIZE=100
PROXY_VALIDATION_TIMEOUT=10
PROXY_ROTATION_INTERVAL=100

# 监控配置
PROMETHEUS_PORT=8000
METRICS_ENABLED=true
ALERT_WEBHOOK_URL=

# 安全配置
SECRET_KEY={secret_key}
API_TOKEN={api_token}
JWT_SECRET={jwt_secret}

# 邮件配置（用于告警）
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USERNAME=
SMTP_PASSWORD=
SMTP_FROM=

# Slack配置（用于告警）
SLACK_WEBHOOK_URL=
SLACK_CHANNEL=#crawler-alerts

# 数据质量配置
MIN_QUALITY_SCORE=0.7
MAX_ERROR_RATE=0.1
MIN_SUCCESS_RATE=0.8

# 存储配置
DATA_RETENTION_DAYS=90
LOG_RETENTION_DAYS=30
BACKUP_ENABLED=false
BACKUP_SCHEDULE=0 2 * * *

# 网络配置
CRAWLER_NETWORK_SUBNET=172.20.0.0/16

# 资源限制
MAX_MEMORY_USAGE=2GB
MAX_CPU_USAGE=80%

# 时区配置
TZ=UTC

# 外部服务配置
EXTERNAL_API_TIMEOUT=30
EXTERNAL_API_RETRIES=3

# 缓存配置
CACHE_TTL=3600
CACHE_MAX_SIZE=1000

# 文件上传配置
MAX_FILE_SIZE=10MB
ALLOWED_FILE_TYPES=json,csv,txt

# 数据库连接池配置
DB_POOL_SIZE=20
DB_POOL_TIMEOUT=30
DB_POOL_RECYCLE=3600

# 队列配置
TASK_QUEUE_SIZE=10000
RESULT_QUEUE_SIZE=5000
PRIORITY_QUEUE_SIZE=1000

# 爬虫特定配置
USER_AGENT_ROTATION=true
RESPECT_ROBOTS_TXT=false
ENABLE_COOKIES=true
ENABLE_AUTOTHROTTLE=true

# 反爬配置
ENABLE_PROXY_ROTATION=true
ENABLE_USER_AGENT_ROTATION=true
ENABLE_DELAY_RANDOMIZATION=true
CAPTCHA_SOLVING_ENABLED=false

# 数据处理配置
ENABLE_DATA_VALIDATION=true
ENABLE_DATA_CLEANING=true
ENABLE_DUPLICATE_DETECTION=true
ENABLE_QUALITY_SCORING=true

# 监控和告警配置
ENABLE_HEALTH_CHECKS=true
HEALTH_CHECK_INTERVAL=30
ENABLE_PERFORMANCE_MONITORING=true
ENABLE_ERROR_TRACKING=true

# 备份配置
BACKUP_TYPE=incremental
BACKUP_COMPRESSION=gzip
BACKUP_ENCRYPTION=false
BACKUP_RETENTION_DAYS=30

# 集群配置（如果使用）
CLUSTER_MODE=false
CLUSTER_NODES=3
CLUSTER_REPLICATION_FACTOR=2

# 开发配置
ENABLE_DEBUG_TOOLBAR=true
ENABLE_PROFILING=false
ENABLE_TESTING_MODE=true

# 生产配置
ENABLE_SSL=false
SSL_CERT_PATH=/etc/ssl/certs/crawler.crt
SSL_KEY_PATH=/etc/ssl/private/crawler.key

# API配置
API_RATE_LIMIT=1000
API_RATE_LIMIT_WINDOW=3600
API_CORS_ORIGINS=*

# 第三方服务配置
CAPTCHA_SERVICE_API_KEY=
PROXY_SERVICE_API_KEY=
NOTIFICATION_SERVICE_API_KEY=
"""

    # 保存到项目根目录
    with open(env_path, "w", encoding="utf-8") as f:
        f.write(env_content)

    # 保存到Docker目录
    docker_env_path.parent.mkdir(parents=True, exist_ok=True)
    with open(docker_env_path, "w", encoding="utf-8") as f:
        f.write(env_content)

    return {
        "postgres_password": postgres_password,
        "mongodb_password": mongodb_password,
        "redis_password": redis_password,
        "mongo_express_password": mongo_express_password,
        "pgadmin_password": pgadmin_password,
        "minio_password": minio_password,
        "env_path": env_path,
        "docker_env_path": docker_env_path,
    }


def main():
    """主函数"""
    print("🚀 快速配置开发环境")
    print("=" * 50)
    print("正在生成安全的环境配置...")

    try:
        result = create_env_file()

        print("✅ 配置文件已生成:")
        print(f"   - {result['env_path']}")
        print(f"   - {result['docker_env_path']}")
        print()

        print("🔐 生成的密码 (请妥善保管):")
        print("-" * 30)
        print(f"PostgreSQL密码: {result['postgres_password']}")
        print(f"MongoDB密码: {result['mongodb_password']}")
        print(f"Redis密码: {result['redis_password']}")
        print(f"MongoDB管理界面密码: {result['mongo_express_password']}")
        print(f"pgAdmin密码: {result['pgadmin_password']}")
        print(f"MinIO密码: {result['minio_password']}")
        print()

        print("🚀 下一步:")
        print("-" * 30)
        print("1. 启动存储服务:")
        print("   python deployment/scripts/start_storage.py start --with-tools")
        print()
        print("2. 设置开发环境:")
        print("   python setup_dev_env.py")
        print()
        print("3. 运行爬虫:")
        print("   scrapy crawl nhc")
        print()
        print("4. 访问管理界面:")
        print("   - MongoDB: http://localhost:8082")
        print("   - pgAdmin: http://localhost:8083")
        print("   - Redis: http://localhost:8081")
        print("   - MinIO: http://localhost:9001")
        print()

        print("💡 提示:")
        print("   如需自定义配置，请运行: python setup_config.py")
        print()

    except Exception as e:
        print(f"❌ 配置失败: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
