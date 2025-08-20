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


# 加载.env文件中的环境变量
def load_env_file():
    """加载.env文件中的环境变量"""
    # 获取项目根目录（settings.py的上两级目录）
    project_root = Path(__file__).parent.parent
    env_file = project_root / ".env"

    if env_file.exists():
        print(f"📁 加载环境变量文件: {env_file}")
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
                        print(f"  ⚠️  跳过无效行 {line_num}: {line}")
        print("✅ 环境变量加载完成")
    else:
        print(f"⚠️  环境变量文件不存在: {env_file}")


# 在导入时立即加载环境变量
load_env_file()

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
    "crawler.middlewares.ProxyMiddleware": 350,
    "crawler.middlewares.CustomUserAgentMiddleware": 400,
    "crawler.middlewares.CustomRetryMiddleware": 550,
    "crawler.middlewares.CrawlerDownloaderMiddleware": 543,
    # 第二阶段中间件 - 反爬机制应对
    "crawler.selenium_middleware.SeleniumMiddleware": 585,
    "anti_crawl.middleware.AntiCrawlMiddleware": 590,
    # 'anti_crawl.middleware.CaptchaMiddleware': 595,  # 可选
    "anti_crawl.middleware.BehaviorSimulationMiddleware": 600,
    # 'anti_crawl.middleware.HeaderRotationMiddleware': 605,  # 可选
}

# Enable or disable extensions
# EXTENSIONS = {
#     'scrapy.extensions.telnet.TelnetConsole': None,
#     'crawler.extensions.PrometheusExtension': 500,
# }

# Configure item pipelines - 数据处理管道
ITEM_PIPELINES = {
    # （可选）增强数据处理管道
    # "data_processing.enhanced_pipelines.EnhancedExtractionPipeline": 200,
    "data_processing.enhanced_pipelines.DataEnrichmentPipeline": 300,
    "data_processing.enhanced_pipelines.ComprehensiveDataPipeline": 400,
    # 内容更新检测（在存储前执行）
    "crawler.pipelines.ContentUpdatePipeline": 590,
    # 存储管道
    "crawler.pipelines.MongoPipeline": 600,
    # 'crawler.pipelines.PostgresPipeline': 700,
}

# Enable autothrottling
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 1
AUTOTHROTTLE_MAX_DELAY = 60
AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
AUTOTHROTTLE_DEBUG = False

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

# Default requests serializer is pickle, but it can be changed to any module
# with loads and dumps functions. Note that pickle is not compatible between
# python versions.
SCHEDULER_SERIALIZER = "scrapy_redis.picklecompat"

# Don't cleanup redis queues, allows to pause/resume crawls.
SCHEDULER_PERSIST = True

# Schedule requests using a priority queue. (default)
SCHEDULER_QUEUE_CLASS = "scrapy_redis.queue.PriorityQueue"

# Alternatively, you can use the LIFO queue.
# SCHEDULER_QUEUE_CLASS = 'scrapy_redis.queue.LifoQueue'

# Or the FIFO queue.
# SCHEDULER_QUEUE_CLASS = 'scrapy_redis.queue.FifoQueue'

# 列表刷新与更新检测配置
LIST_REFRESH_ENABLED = True
LIST_REFRESH_INTERVAL = int(os.getenv("LIST_REFRESH_INTERVAL", 900))  # 秒
CONTENT_DEDUP_ENABLED = True

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

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", 5432))
POSTGRES_DATABASE = os.getenv("POSTGRES_DB", "crawler_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "123456")

# Proxy settings
PROXY_POOL_SIZE = int(os.getenv("PROXY_POOL_SIZE", 100))
PROXY_VALIDATION_TIMEOUT = int(os.getenv("PROXY_VALIDATION_TIMEOUT", 10))

# Monitoring settings
PROMETHEUS_PORT = int(os.getenv("PROMETHEUS_PORT", 8000))
METRICS_ENABLED = os.getenv("METRICS_ENABLED", "True").lower() == "true"

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
# LOG_FILE = os.path.join(BASE_DIR, 'logs', 'scrapy.log')

# Security
SECRET_KEY = os.getenv("SECRET_KEY", "your-secret-key-here")
API_TOKEN = os.getenv("API_TOKEN", "your-api-token-here")

# Custom settings
RANDOMIZE_DOWNLOAD_DELAY = 0.5
DOWNLOAD_TIMEOUT = 180
RETRY_TIMES = 3
RETRY_HTTP_CODES = [500, 502, 503, 504, 408, 429]

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
SELENIUM_ENABLED = False  # 默认关闭，可通过命令行参数启用
SELENIUM_GRID_URL = "http://localhost:4444"
SELENIUM_BROWSER = "chrome"  # chrome 或 firefox
SELENIUM_IMPLICIT_WAIT = 10
SELENIUM_PAGE_LOAD_TIMEOUT = 30
SELENIUM_WINDOW_SIZE = (1920, 1080)

# 反爬虫检测配置
ANTI_CRAWL_ENABLED = True
ANTI_CRAWL_AUTO_RETRY = True
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
