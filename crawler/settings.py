# -*- coding: utf-8 -*-
"""
Scrapy settings for crawler project

For simplicity, this file contains only settings considered important or
commonly used. You can find more settings consulting the documentation:

    https://docs.scrapy.org/en/latest/topics/settings.html
    https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
    https://docs.scrapy.org/en/latest/topics/spider-middleware.html
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# 加载.env文件中的环境变量
def load_env_file() -> None:
    """加载.env文件中的环境变量"""
    # 获取项目根目录（settings.py的上两级目录）
    project_root = Path(__file__).parent.parent
    env_file = project_root / ".env"

    if env_file.exists():
        print(f"加载环境变量文件: {env_file}")  # 移除emoji
        with open(env_file, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    try:
                        key, value = line.split("=", 1)
                        key = key.strip()
                        value = value.strip()
                        # 移除引号（如果有的话）
                        if value.startswith('"') and value.endswith('"'):
                            value = value[1:-1]
                        elif value.startswith("'") and value.endswith("'"):
                            value = value[1:-1]
                        os.environ[key] = value
                        # print(f"  ✅ {key}={'*' * len(value) if 'PASSWORD' in key else value}")
                    except ValueError:
                        print(f"  跳过无效行 {line_num}: {line}")  # 移除emoji
        print("环境变量加载完成")  # 移除emoji
    else:
        print(f"环境变量文件不存在: {env_file}")  # 移除emoji


# 在导入时立即加载环境变量
load_env_file()
# load_dotenv()

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent

# Scrapy settings for crawler project
BOT_NAME = "crawler"

SPIDER_MODULES = ["crawler.spiders"]
NEWSPIDER_MODULE = "crawler.spiders"

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Configure maximum concurrent requests performed by Scrapy (default: 16)
CONCURRENT_REQUESTS = 32

# Configure a delay for requests for the same website (default: 0)
DOWNLOAD_DELAY = 1
# The download delay setting will honor only one of:
CONCURRENT_REQUESTS_PER_DOMAIN = 16
CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
COOKIES_ENABLED = True

# Disable Telnet Console (enabled by default)
TELNETCONSOLE_ENABLED = False

MYEXT_ENABLED = True  # 开启扩展
IDLE_NUMBER = 12  # 配置空闲持续时间单位为 12个 ，一个时间单位为5s

# Override the default request headers:
DEFAULT_REQUEST_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

# Enable or disable spider middlewares
# SPIDER_MIDDLEWARES = {
#     'crawler.middlewares.CrawlerSpiderMiddleware': 543,
# }

# Enable or disable downloader middlewares
DOWNLOADER_MIDDLEWARES = {
    # 第一阶段中间件
    # "crawler.middlewares.ProxyMiddleware": 350,
    "crawler.middlewares.CustomUserAgentMiddleware": 400,
    "crawler.middlewares.CustomRetryMiddleware": 550,
    "crawler.middlewares.CrawlerDownloaderMiddleware": 543,
    # 第二阶段中间件 - 反爬机制应对
    "crawler.selenium_middleware.SeleniumMiddleware": 585,
    # "anti_crawl.middleware.AntiCrawlMiddleware": 590,
    # 'anti_crawl.middleware.CaptchaMiddleware': 595,  # 可选
    # "anti_crawl.middleware.BehaviorSimulationMiddleware": 600,
    # 'anti_crawl.middleware.HeaderRotationMiddleware': 605,  # 可选
}

# Enable or disable extensions
EXTENSIONS = {
    "scrapy.extensions.telnet.TelnetConsole": None,
    "crawler.monitoring.scrapy_ext.MetricsExtension": 500,
    'crawler.extensions.PrometheusExtension': 600,
    "crawler.extensions.RedisSpiderSmartIdleClosedExensions": 700,
}

# Configure item pipelines - 数据处理管道
ITEM_PIPELINES = {
    # AI判断管道，在数据清洗和去重后，内容更新前执行
    "crawler.pipelines.AIPipeline": 200, # 新增：AI判断管道
    # （可选）增强数据处理管道
    # "data_processing.enhanced_pipelines.EnhancedExtractionPipeline": 200,
    "data_processing.enhanced_pipelines.DataEnrichmentPipeline": 300,
    "data_processing.enhanced_pipelines.ComprehensiveDataPipeline": 400,
    # 文件/图片下载（自定义管道，在存储前执行，保证 files/images 字段可用）
    "crawler.media_pipelines.ArticleFilesPipeline": 520,
    "crawler.media_pipelines.ArticleImagesPipeline": 530,
    # 内容更新检测（在存储前执行）
    "crawler.pipelines.ContentUpdatePipeline": 590,
    # 存储管道
    "crawler.pipelines.MongoPipeline": 600,
}

# 下载文件与图片的本地存储目录（保持根目录）
FILES_STORE = "storage/files"
IMAGES_STORE = "storage/images"

# 可选的图片过滤参数（减少小图标保存）
# IMAGES_MIN_HEIGHT = 100
# IMAGES_MIN_WIDTH = 100

# MinIO / S3 媒体存储配置（示例，可通过环境变量启用）
# 说明：当 MINIO_ENABLED=True 且配置了访问凭证时，
#       将把 FILES_STORE/IMAGES_STORE 切换为 s3://<bucket>/<prefix>
#       Scrapy 会通过 AWS_* 配置使用 boto3 与 S3 兼容端点（MinIO）交互。
MINIO_ENABLED = os.getenv("MINIO_ENABLED", "False").lower() == "true"
MINIO_ENDPOINT = os.getenv(
    "MINIO_ENDPOINT", "http://localhost:9000"
)  # MinIO服务地址（含协议与端口）
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "")
MINIO_REGION = os.getenv("MINIO_REGION", "us-east-1")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "crawler-media")
MINIO_FILES_PREFIX = os.getenv("MINIO_FILES_PREFIX", "files")
MINIO_IMAGES_PREFIX = os.getenv("MINIO_IMAGES_PREFIX", "images")
# 可选：地址风格与 SSL
S3_ADDRESSING_STYLE = os.getenv(
    "S3_ADDRESSING_STYLE", "path"
)  # path|virtual（MinIO 推荐 path）
S3_USE_SSL = os.getenv("S3_USE_SSL", "False").lower() == "true"

if MINIO_ENABLED and MINIO_BUCKET and MINIO_ACCESS_KEY and MINIO_SECRET_KEY:
    # Scrapy 的 S3 客户端配置（boto3）
    AWS_ACCESS_KEY_ID = MINIO_ACCESS_KEY
    AWS_SECRET_ACCESS_KEY = MINIO_SECRET_KEY
    AWS_ENDPOINT_URL = MINIO_ENDPOINT  # S3 兼容端点（Scrapy ≥2.8 支持）
    AWS_REGION_NAME = MINIO_REGION
    # 注意：addressing_style/ssl 等高级项依赖 boto3 的 client config，
    # 以下两项作为文档性设置保留
    AWS_S3_ADDRESSING_STYLE = S3_ADDRESSING_STYLE
    AWS_USE_SSL = S3_USE_SSL

    # 切换媒体存储到 MinIO（S3）
    FILES_STORE = f"s3://{MINIO_BUCKET}/{MINIO_FILES_PREFIX}"
    IMAGES_STORE = f"s3://{MINIO_BUCKET}/{MINIO_IMAGES_PREFIX}"


# Enable autothrottling
AUTOTHROTTLE_ENABLED = True  # 自动限速
AUTOTHROTTLE_START_DELAY = 1  # 初始下载延迟
AUTOTHROTTLE_MAX_DELAY = 5  # 最大延迟
AUTOTHROTTLE_TARGET_CONCURRENCY = 16.0  # 目标并发数
AUTOTHROTTLE_DEBUG = False  # 调试模式

# Enable and configure HTTP caching
HTTPCACHE_ENABLED = True
HTTPCACHE_EXPIRATION_SECS = 3600
HTTPCACHE_DIR = "httpcache"
HTTPCACHE_IGNORE_HTTP_CODES = [503, 504, 505, 500, 403, 404, 408, 429]

# Redis settings for Scrapy-Redis
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
# 可选Redis参数（容错与保活）
REDIS_PARAMS = {
    "socket_timeout": int(os.getenv("REDIS_SOCKET_TIMEOUT", 5)),
    "retry_on_timeout": True,
    "health_check_interval": 30,
}

# 使用 Redis 调度器（已启用）
SCHEDULER = "scrapy_redis.scheduler.Scheduler"

# 按站点共享的请求去重（自定义去重器）
DUPEFILTER_CLASS = "crawler.dupefilters.SiteAwareRFPDupeFilter"
# 键名格式，可按需覆盖（如按站点分组）
SITE_AWARE_DUPEFILTER_KEY_FMT = "dupefilter:%(spider)s:%(site)s"


# 默认的请求序列化器是 pickle，但可将其更改为任何包含 loads 和 dumps 函数的模块。
# 请注意，pickle 在不同 Python 版本之间不兼容。
SCHEDULER_SERIALIZER = "scrapy_redis.picklecompat"

# 确保在暂停后请求队列不会丢失
SCHEDULER_PERSIST = True

# 使用优先级队列调度请求
SCHEDULER_QUEUE_CLASS = "scrapy_redis.queue.PriorityQueue"

# 此外，还可以使用后进先出（LIFO）队列。
# SCHEDULER_QUEUE_CLASS = 'scrapy_redis.queue.LifoQueue'
# 或者先进先出（FIFO）队列。
# SCHEDULER_QUEUE_CLASS = 'scrapy_redis.queue.FifoQueue'

# 列表刷新与更新检测配置
LIST_REFRESH_ENABLED = False
# 刷新间隔
LIST_REFRESH_INTERVAL = int(os.getenv("LIST_REFRESH_INTERVAL", 900))  # 秒
# 内容去重
CONTENT_DEDUP_ENABLED = True
CONTENT_GLOBAL_DEDUP_ENABLED = os.getenv("CONTENT_GLOBAL_DEDUP_ENABLED", "True").lower() == "true"
# 作用域：per_site | global
CONTENT_GLOBAL_DEDUP_SCOPE = os.getenv("CONTENT_GLOBAL_DEDUP_SCOPE", "per_site")
# 去重集合的 TTL（秒），0 或未设置表示不设置 TTL
CONTENT_DEDUP_TTL_SECONDS = int(os.getenv("CONTENT_DEDUP_TTL_SECONDS", 0))

# Database settings
# 构建MongoDB连接URI，支持容器环境
MONGODB_HOST = os.getenv("MONGODB_HOST", "localhost")
MONGODB_PORT = os.getenv("MONGODB_PORT", "27017")
MONGODB_USERNAME = os.getenv("MONGODB_ROOT_USERNAME", "")
MONGODB_PASSWORD = os.getenv("MONGODB_ROOT_PASSWORD", "")

# 构建完整的MongoDB URI
if MONGODB_USERNAME and MONGODB_PASSWORD:
    MONGODB_URI = f"mongodb://{MONGODB_USERNAME}:{MONGODB_PASSWORD}@{MONGODB_HOST}:{MONGODB_PORT}/"
else:
    MONGODB_URI = f"mongodb://{MONGODB_HOST}:{MONGODB_PORT}/"

# 也支持直接设置完整URI
MONGODB_URI = os.getenv("MONGODB_URL", MONGODB_URI)
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "crawler_db")

# 已移除PostgreSQL配置（仅保留MongoDB）

# 代理池配置
PROXY_POOL_SIZE = int(os.getenv("PROXY_POOL_SIZE", 100))
PROXY_VALIDATION_TIMEOUT = int(os.getenv("PROXY_VALIDATION_TIMEOUT", 10))

# 监控设置
PROMETHEUS_PORT = int(os.getenv("PROMETHEUS_PORT", 9108))
METRICS_ENABLED = os.getenv("METRICS_ENABLED", "True").lower() == "true"

# 日志配置
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE = os.path.join(BASE_DIR, 'logs', 'scrapy.log')

# 显式设置 pymongo 日志级别
import logging

# logging.getLogger('pymongo').setLevel(logging.INFO)
# logging.getLogger('boto3').setLevel(logging.WARNING)
# logging.getLogger('botocore').setLevel(logging.WARNING)

# 安全设置
SECRET_KEY = os.getenv("SECRET_KEY", "your-secret-key-here")
API_TOKEN = os.getenv("API_TOKEN", "your-api-token-here")

# ============================================================================
# AI 模型配置
# ============================================================================
# AI 模型提供商: deepseek 或 zhipuai
AI_MODEL_PROVIDER = os.getenv("AI_MODEL_PROVIDER", "deepseek")
# DeepSeek 模型名称
DEEPSEEK_MODEL_NAME = os.getenv("DEEPSEEK_MODEL_NAME", "deepseek-chat")
# DeepSeek API Key
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "<DeepSeek API Key>")
# 智谱 AI 模型名称
ZHIPUAI_MODEL_NAME = os.getenv("ZHIPUAI_MODEL_NAME", "glm-4.5")
# 智谱 AI API Key
ZHIPUAI_API_KEY = os.getenv("ZHIPUAI_API_KEY", "<ZhipuAI API Key>")
# ============================================================================


# Custom settings
RANDOMIZE_DOWNLOAD_DELAY = 0.5
DOWNLOAD_TIMEOUT = 60
RETRY_TIMES = 2
RETRY_HTTP_CODES = [500, 502, 503, 504, 408, 429]

# 当调度器在指定时间内没有新的请求被接收时，自动关闭爬虫（单位：秒）
# 这是 scrapy-redis 调度器使用的设置
SCHEDULER_IDLE_BEFORE_CLOSE = 60

# User-Agent settings
USER_AGENT_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
]

# Feed exports
FEEDS = {
    "data/%(name)s_%(time)s.json": {
        "format": "json",
        "encoding": "utf8",
        "store_empty": False,
        "fields": None,
        "indent": 2,
    },
}

# Request fingerprinting
REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"

# ============================================================================
# 第二阶段配置 - 反爬机制应对
# ============================================================================

# Selenium Grid配置
SELENIUM_ENABLED = True  # 默认关闭，可通过命令行参数启用
SELENIUM_GRID_URL = "http://localhost:4444"
SELENIUM_BROWSER = "firefox"  # chrome 或 firefox
SELENIUM_IMPLICIT_WAIT = 10
SELENIUM_PAGE_LOAD_TIMEOUT = 30
SELENIUM_WINDOW_SIZE = (1920, 1080)

# 反爬虫检测配置
ANTI_CRAWL_ENABLED = False
ANTI_CRAWL_AUTO_RETRY = False
ANTI_CRAWL_MAX_RETRIES = 3
ANTI_CRAWL_RETRY_DELAY = 5

# 验证码识别配置
CAPTCHA_SERVICE_URL = None  # 第三方验证码识别服务URL

# 行为模拟配置
BEHAVIOR_MIN_DELAY = 1.0
BEHAVIOR_MAX_DELAY = 5.0

# ============================================================================
# 第三阶段配置 - 数据处理和质量保证
# ============================================================================

# 数据提取配置
EXTRACTION_CONFIG_DIR = "config/extraction"
ENABLE_ENHANCED_EXTRACTION = True

# 数据清洗配置
ENABLE_DATA_CLEANING = True
DATA_CLEANING_CONFIG = None

# 数据验证配置
ENABLE_DATA_VALIDATION = True
DROP_INVALID_ITEMS = False
VALIDATION_SCHEMA = None

# 数据质量配置
ENABLE_QUALITY_ASSESSMENT = True
MIN_QUALITY_SCORE = 0.0
QUALITY_REPORT_ENABLED = True

# 数据丰富化配置
ENABLE_DATA_ENRICHMENT = True

# 数据存储优化配置
ENABLE_BATCH_INSERT = True
BATCH_SIZE = 100
CONNECTION_POOL_SIZE = 10

# 报告生成配置
REPORTS_DIR = "reports"
ENABLE_QUALITY_REPORTS = True
ENABLE_PROCESSING_REPORTS = True
