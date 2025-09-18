# -*- coding: utf-8 -*-
"""
Scrapy扩展模块

自定义扩展功能
"""

import logging
import time

from scrapy import signals
from scrapy.exceptions import NotConfigured

logger = logging.getLogger(__name__)


class PrometheusExtension:
    """Prometheus监控扩展"""

    def __init__(self, stats, settings):
        self.stats = stats
        self.settings = settings
        self.enabled = settings.getbool("METRICS_ENABLED", True)

        if not self.enabled:
            raise NotConfigured("Prometheus metrics disabled")

        self.start_time = time.time()

        # 初始化指标
        self.metrics = {
            "requests_total": 0,
            "requests_success": 0,
            "requests_failed": 0,
            "items_scraped": 0,
            "items_dropped": 0,
            "response_time_total": 0.0,
            "spider_opened": 0,
            "spider_closed": 0,
        }

        logger.info("Prometheus扩展已启用")

    @classmethod
    def from_crawler(cls, crawler):
        """从爬虫创建扩展实例"""
        if not crawler.settings.getbool("METRICS_ENABLED", True):
            raise NotConfigured("Prometheus metrics disabled")

        ext = cls(crawler.stats, crawler.settings)

        # 连接信号
        crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        crawler.signals.connect(ext.request_scheduled, signal=signals.request_scheduled)
        crawler.signals.connect(ext.response_received, signal=signals.response_received)
        crawler.signals.connect(ext.item_scraped, signal=signals.item_scraped)
        crawler.signals.connect(ext.item_dropped, signal=signals.item_dropped)

        return ext

    def spider_opened(self, spider):
        """爬虫开始时的处理"""
        self.metrics["spider_opened"] += 1
        self.start_time = time.time()
        logger.info(f"爬虫 {spider.name} 开始运行")

    def spider_closed(self, spider, reason):
        """爬虫关闭时的处理"""
        self.metrics["spider_closed"] += 1
        end_time = time.time()
        duration = end_time - self.start_time

        # 记录统计信息
        logger.info(f"爬虫 {spider.name} 运行完成")
        logger.info(f"运行时间: {duration:.2f}秒")
        logger.info(f"总请求数: {self.metrics['requests_total']}")
        logger.info(f"成功请求: {self.metrics['requests_success']}")
        logger.info(f"失败请求: {self.metrics['requests_failed']}")
        logger.info(f"抓取数据: {self.metrics['items_scraped']}")
        logger.info(f"丢弃数据: {self.metrics['items_dropped']}")

        # 计算平均响应时间
        if self.metrics["requests_success"] > 0:
            avg_response_time = (
                self.metrics["response_time_total"] / self.metrics["requests_success"]
            )
            logger.info(f"平均响应时间: {avg_response_time:.3f}秒")

    def request_scheduled(self, request, spider):
        """请求调度时的处理"""
        self.metrics["requests_total"] += 1

    def response_received(self, response, request, spider):
        """收到响应时的处理"""
        if response.status < 400:
            self.metrics["requests_success"] += 1
        else:
            self.metrics["requests_failed"] += 1

        # 记录响应时间
        if hasattr(request, "meta") and "download_latency" in request.meta:
            self.metrics["response_time_total"] += request.meta["download_latency"]

    def item_scraped(self, item, response, spider):
        """数据抓取时的处理"""
        self.metrics["items_scraped"] += 1

    def item_dropped(self, item, response, exception, spider):
        """数据丢弃时的处理"""
        self.metrics["items_dropped"] += 1
        logger.warning(f"数据被丢弃: {exception}")

    def get_metrics(self):
        """获取当前指标"""
        return self.metrics.copy()


class StatsExtension:
    """统计扩展"""

    def __init__(self, stats):
        self.stats = stats

    @classmethod
    def from_crawler(cls, crawler):
        ext = cls(crawler.stats)

        # 连接信号
        crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)

        return ext

    def spider_opened(self, spider):
        """爬虫开始时记录统计"""
        self.stats.set_value("spider_start_time", time.time())
        logger.info(f"统计扩展: 爬虫 {spider.name} 开始")

    def spider_closed(self, spider, reason):
        """爬虫结束时记录统计"""
        start_time = self.stats.get_value("spider_start_time", time.time())
        duration = time.time() - start_time
        self.stats.set_value("spider_duration", duration)

        logger.info(f"统计扩展: 爬虫 {spider.name} 结束，原因: {reason}")
        logger.info(f"统计扩展: 运行时长: {duration:.2f}秒")


class MemoryUsageExtension:
    """内存使用监控扩展"""

    def __init__(self, settings):
        self.settings = settings
        self.enabled = settings.getbool("MEMORY_MONITORING_ENABLED", False)

        if not self.enabled:
            raise NotConfigured("Memory monitoring disabled")

    @classmethod
    def from_crawler(cls, crawler):
        if not crawler.settings.getbool("MEMORY_MONITORING_ENABLED", False):
            raise NotConfigured("Memory monitoring disabled")

        ext = cls(crawler.settings)

        # 连接信号
        crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)

        return ext

    def spider_opened(self, spider):
        """爬虫开始时记录内存使用"""
        try:
            import psutil

            process = psutil.Process()
            memory_info = process.memory_info()
            logger.info(f"内存监控: 爬虫启动时内存使用: {memory_info.rss / 1024 / 1024:.2f} MB")
        except ImportError:
            logger.warning("psutil未安装，无法监控内存使用")

    def spider_closed(self, spider, reason):
        """爬虫结束时记录内存使用"""
        try:
            import psutil

            process = psutil.Process()
            memory_info = process.memory_info()
            logger.info(f"内存监控: 爬虫结束时内存使用: {memory_info.rss / 1024 / 1024:.2f} MB")
        except ImportError:
            pass


class ErrorReportingExtension:
    """错误报告扩展"""

    def __init__(self, settings):
        self.settings = settings
        self.error_count = 0
        self.max_errors = settings.getint("MAX_ERRORS_BEFORE_ALERT", 100)

    @classmethod
    def from_crawler(cls, crawler):
        ext = cls(crawler.settings)

        # 连接信号
        crawler.signals.connect(ext.spider_error, signal=signals.spider_error)

        return ext

    def spider_error(self, failure, response, spider):
        """处理爬虫错误"""
        self.error_count += 1

        logger.error(f"爬虫错误 #{self.error_count}: {failure.value}")

        if self.error_count >= self.max_errors:
            logger.critical(f"错误数量达到阈值 {self.max_errors}，建议检查爬虫状态")
            # 这里可以添加告警逻辑，如发送邮件或Slack消息


class RedisSpiderSmartIdleClosedExensions(object):
    def __init__(self, idle_number, crawler):
        self.crawler = crawler
        self.idle_number = idle_number
        self.idle_list = []
        self.idle_count = 0

    @classmethod
    def from_crawler(cls, crawler):
        # 首先检查是否应该启用和提高扩展
        # 否则不配置
        if not crawler.settings.getbool("MYEXT_ENABLED"):
            raise NotConfigured

        # 获取配置中的时间片个数，默认为360个，30分钟
        idle_number = crawler.settings.getint("IDLE_NUMBER", 12)

        # 实例化扩展对象
        ext = cls(idle_number, crawler)

        # 将扩展对象连接到信号， 将signals.spider_idle 与 spider_idle() 方法关联起来。
        crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        crawler.signals.connect(ext.spider_idle, signal=signals.spider_idle)

        # return the extension object
        return ext

    def spider_opened(self, spider):
        logger.info(
            "opened spider %s redis spider Idle, Continuous idle limit： %d",
            spider.name,
            self.idle_number,
        )

    def spider_closed(self, spider):
        logger.info(
            "closed spider %s, idle count %d , Continuous idle count %d",
            spider.name,
            self.idle_count,
            len(self.idle_list),
        )

    def spider_idle(self, spider):
        self.idle_count += 1  # 空闲计数
        self.idle_list.append(time.time())  # 每次触发 spider_idle时，记录下触发时间戳
        idle_list_len = len(self.idle_list)  # 获取当前已经连续触发的次数

        # 判断 当前触发时间与上次触发时间 之间的间隔是否大于5秒，如果大于5秒，说明redis 中还有key
        if idle_list_len > 2 and self.idle_list[-1] - self.idle_list[-2] > 6:
            self.idle_list = [self.idle_list[-1]]

        elif idle_list_len > self.idle_number:
            # 连续触发的次数达到配置次数后关闭爬虫
            logger.info(
                "\n continued idle number exceed {} Times"
                "\n meet the idle shutdown conditions, will close the reptile operation"
                "\n idle start time: {},  close spider time: {}".format(
                    self.idle_number, self.idle_list[0], self.idle_list[0]
                )
            )
            # 执行关闭爬虫操作
            self.crawler.engine.close_spider(spider, "closespider_pagecount")
