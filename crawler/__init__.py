"""
企业级分布式爬虫系统

这是一个基于Scrapy-Redis的分布式爬虫系统，专门用于爬取疫情相关信息。
系统具备智能反爬、数据质量保证、任务调度等企业级特性。
"""

__version__ = "1.0.0"
__author__ = "Crawler Team"
__email__ = "team@crawler.com"

# 导入核心组件
from .settings import *  # noqa
