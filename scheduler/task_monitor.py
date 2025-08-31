#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
任务监控器

实现任务执行的实时监控、性能统计和异常检测
"""

import json
import logging
import time
from collections import defaultdict, deque
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional

try:
    import redis

    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class TaskMetrics:
    """任务性能指标"""

    task_id: str
    worker_id: str
    start_time: float
    end_time: float = None
    duration: float = None
    status: str = "running"
    items_scraped: int = 0
    pages_crawled: int = 0
    errors_count: int = 0
    memory_usage: float = 0.0
    cpu_usage: float = 0.0

    def to_dict(self) -> Dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict) -> "TaskMetrics":
        return cls(**data)


class TaskMonitor:
    """任务监控器"""

    def __init__(self, redis_url="redis://localhost:6379/0"):
        self.redis_url = redis_url
        self.redis = None

        # Redis键名
        self.metrics_key = "crawler:task_metrics"
        self.performance_key = "crawler:performance"
        self.alerts_key = "crawler:alerts"
        self.hourly_stats_key = "crawler:hourly_stats"

        # 监控配置
        self.alert_thresholds = {
            "task_duration_max": 3600,  # 任务最大执行时间(秒)
            "error_rate_max": 0.1,  # 最大错误率
            "memory_usage_max": 1024,  # 最大内存使用(MB)
            "cpu_usage_max": 90,  # 最大CPU使用率(%)
            "queue_size_max": 1000,  # 最大队列大小
        }

        # 性能统计窗口
        self.stats_window_size = 100  # 保留最近100个任务的统计

        # 初始化Redis连接
        self.connect_redis()

        logger.info("任务监控器初始化完成")

    def connect_redis(self) -> bool:
        """连接Redis"""
        if not REDIS_AVAILABLE:
            logger.error("Redis不可用，请安装redis-py")
            return False

        try:
            self.redis = redis.from_url(self.redis_url)
            self.redis.ping()
            logger.info("Redis连接成功")
            return True
        except Exception as e:
            logger.error(f"Redis连接失败: {e}")
            return False

    def start_task_monitoring(self, task_id: str, worker_id: str) -> bool:
        """开始监控任务"""
        if not self.redis:
            return False

        try:
            metrics = TaskMetrics(
                task_id=task_id, worker_id=worker_id, start_time=time.time()
            )

            self.redis.hset(self.metrics_key, task_id, json.dumps(metrics.to_dict()))

            logger.debug(f"开始监控任务: {task_id}")
            return True

        except Exception as e:
            logger.error(f"开始任务监控失败: {e}")
            return False

    def update_task_metrics(self, task_id: str, metrics_update: Dict) -> bool:
        """更新任务指标"""
        if not self.redis:
            return False

        try:
            # 获取现有指标
            metrics_data = self.redis.hget(self.metrics_key, task_id)
            if not metrics_data:
                logger.warning(f"任务指标不存在: {task_id}")
                return False

            metrics = TaskMetrics.from_dict(json.loads(metrics_data))

            # 更新指标
            for key, value in metrics_update.items():
                if hasattr(metrics, key):
                    setattr(metrics, key, value)

            # 保存更新后的指标
            self.redis.hset(self.metrics_key, task_id, json.dumps(metrics.to_dict()))

            # 检查是否需要触发告警
            self.check_task_alerts(metrics)

            return True

        except Exception as e:
            logger.error(f"更新任务指标失败: {e}")
            return False

    def complete_task_monitoring(
        self, task_id: str, final_metrics: Dict = None
    ) -> bool:
        """完成任务监控"""
        if not self.redis:
            return False

        try:
            # 获取任务指标
            metrics_data = self.redis.hget(self.metrics_key, task_id)
            if not metrics_data:
                logger.warning(f"任务指标不存在: {task_id}")
                return False

            metrics = TaskMetrics.from_dict(json.loads(metrics_data))

            # 设置结束时间和状态
            metrics.end_time = time.time()
            metrics.duration = metrics.end_time - metrics.start_time
            metrics.status = "completed"

            # 应用最终指标
            if final_metrics:
                for key, value in final_metrics.items():
                    if hasattr(metrics, key):
                        setattr(metrics, key, value)

            # 保存最终指标
            self.redis.hset(self.metrics_key, task_id, json.dumps(metrics.to_dict()))

            # 更新性能统计
            self.update_performance_stats(metrics)

            # 更新小时统计
            self.update_hourly_stats(metrics)

            logger.debug(f"完成任务监控: {task_id} (耗时: {metrics.duration:.2f}秒)")
            return True

        except Exception as e:
            logger.error(f"完成任务监控失败: {e}")
            return False

    def fail_task_monitoring(self, task_id: str, error_info: Dict) -> bool:
        """任务失败监控"""
        if not self.redis:
            return False

        try:
            # 获取任务指标
            metrics_data = self.redis.hget(self.metrics_key, task_id)
            if not metrics_data:
                logger.warning(f"任务指标不存在: {task_id}")
                return False

            metrics = TaskMetrics.from_dict(json.loads(metrics_data))

            # 设置失败状态
            metrics.end_time = time.time()
            metrics.duration = metrics.end_time - metrics.start_time
            metrics.status = "failed"
            metrics.errors_count += 1

            # 保存失败指标
            self.redis.hset(self.metrics_key, task_id, json.dumps(metrics.to_dict()))

            # 记录错误告警
            self.record_alert(
                "task_failed",
                {
                    "task_id": task_id,
                    "worker_id": metrics.worker_id,
                    "error": error_info,
                    "duration": metrics.duration,
                },
            )

            # 更新性能统计
            self.update_performance_stats(metrics)

            logger.warning(f"任务失败监控: {task_id}")
            return True

        except Exception as e:
            logger.error(f"任务失败监控失败: {e}")
            return False

    def check_task_alerts(self, metrics: TaskMetrics):
        """检查任务告警"""
        current_time = time.time()

        # 检查任务执行时间
        if (
            metrics.start_time
            and current_time - metrics.start_time
            > self.alert_thresholds["task_duration_max"]
        ):
            self.record_alert(
                "task_timeout",
                {
                    "task_id": metrics.task_id,
                    "worker_id": metrics.worker_id,
                    "duration": current_time - metrics.start_time,
                },
            )

        # 检查内存使用
        if metrics.memory_usage > self.alert_thresholds["memory_usage_max"]:
            self.record_alert(
                "high_memory_usage",
                {
                    "task_id": metrics.task_id,
                    "worker_id": metrics.worker_id,
                    "memory_usage": metrics.memory_usage,
                },
            )

        # 检查CPU使用率
        if metrics.cpu_usage > self.alert_thresholds["cpu_usage_max"]:
            self.record_alert(
                "high_cpu_usage",
                {
                    "task_id": metrics.task_id,
                    "worker_id": metrics.worker_id,
                    "cpu_usage": metrics.cpu_usage,
                },
            )

    def record_alert(self, alert_type: str, alert_data: Dict):
        """记录告警"""
        if not self.redis:
            return

        try:
            alert = {
                "type": alert_type,
                "data": alert_data,
                "timestamp": time.time(),
                "severity": self.get_alert_severity(alert_type),
            }

            # 使用时间戳作为键确保唯一性
            alert_key = f"{alert_type}:{int(time.time() * 1000)}"

            self.redis.hset(self.alerts_key, alert_key, json.dumps(alert))

            # 设置告警过期时间（7天）
            self.redis.expire(self.alerts_key, 7 * 24 * 3600)

            logger.warning(f"记录告警: {alert_type} - {alert_data}")

        except Exception as e:
            logger.error(f"记录告警失败: {e}")

    def get_alert_severity(self, alert_type: str) -> str:
        """获取告警严重程度"""
        severity_map = {
            "task_timeout": "high",
            "task_failed": "medium",
            "high_memory_usage": "medium",
            "high_cpu_usage": "low",
            "queue_overflow": "high",
        }
        return severity_map.get(alert_type, "low")

    def update_performance_stats(self, metrics: TaskMetrics):
        """更新性能统计"""
        if not self.redis:
            return

        try:
            # 获取现有统计
            stats_data = self.redis.get(self.performance_key)
            if stats_data:
                stats = json.loads(stats_data)
            else:
                stats = {
                    "total_tasks": 0,
                    "completed_tasks": 0,
                    "failed_tasks": 0,
                    "total_duration": 0.0,
                    "total_items": 0,
                    "total_pages": 0,
                    "avg_duration": 0.0,
                    "success_rate": 0.0,
                    "throughput": 0.0,
                    "last_updated": time.time(),
                }

            # 更新统计
            stats["total_tasks"] += 1
            if metrics.status == "completed":
                stats["completed_tasks"] += 1
            elif metrics.status == "failed":
                stats["failed_tasks"] += 1

            if metrics.duration:
                stats["total_duration"] += metrics.duration

            stats["total_items"] += metrics.items_scraped
            stats["total_pages"] += metrics.pages_crawled

            # 计算平均值和比率
            if stats["total_tasks"] > 0:
                stats["avg_duration"] = stats["total_duration"] / stats["total_tasks"]
                stats["success_rate"] = stats["completed_tasks"] / stats["total_tasks"]

            # 计算吞吐量（任务/小时）
            time_diff = time.time() - stats["last_updated"]
            if time_diff > 0:
                stats["throughput"] = stats["total_tasks"] / (time_diff / 3600)

            stats["last_updated"] = time.time()

            # 保存统计
            self.redis.set(self.performance_key, json.dumps(stats))

        except Exception as e:
            logger.error(f"更新性能统计失败: {e}")

    def update_hourly_stats(self, metrics: TaskMetrics):
        """更新小时统计"""
        if not self.redis:
            return

        try:
            # 获取当前小时
            current_hour = datetime.now().strftime("%Y-%m-%d-%H")
            hour_key = f"{self.hourly_stats_key}:{current_hour}"

            # 获取小时统计
            hour_stats_data = self.redis.get(hour_key)
            if hour_stats_data:
                hour_stats = json.loads(hour_stats_data)
            else:
                hour_stats = {
                    "hour": current_hour,
                    "tasks_count": 0,
                    "completed_count": 0,
                    "failed_count": 0,
                    "total_duration": 0.0,
                    "total_items": 0,
                    "worker_stats": defaultdict(int),
                }

            # 更新小时统计
            hour_stats["tasks_count"] += 1
            if metrics.status == "completed":
                hour_stats["completed_count"] += 1
            elif metrics.status == "failed":
                hour_stats["failed_count"] += 1

            if metrics.duration:
                hour_stats["total_duration"] += metrics.duration

            hour_stats["total_items"] += metrics.items_scraped
            hour_stats["worker_stats"][metrics.worker_id] += 1

            # 保存小时统计
            self.redis.set(hour_key, json.dumps(hour_stats))
            self.redis.expire(hour_key, 30 * 24 * 3600)  # 保留30天

        except Exception as e:
            logger.error(f"更新小时统计失败: {e}")

    def get_performance_stats(self) -> Dict:
        """获取性能统计"""
        if not self.redis:
            return {}

        try:
            stats_data = self.redis.get(self.performance_key)
            if stats_data:
                return json.loads(stats_data)
            return {}

        except Exception as e:
            logger.error(f"获取性能统计失败: {e}")
            return {}

    def get_recent_alerts(self, hours: int = 24) -> List[Dict]:
        """获取最近的告警"""
        if not self.redis:
            return []

        try:
            alerts_data = self.redis.hgetall(self.alerts_key)
            recent_alerts = []
            cutoff_time = time.time() - (hours * 3600)

            for alert_key, alert_data in alerts_data.items():
                alert = json.loads(alert_data)
                if alert["timestamp"] > cutoff_time:
                    recent_alerts.append(alert)

            # 按时间排序
            recent_alerts.sort(key=lambda x: x["timestamp"], reverse=True)
            return recent_alerts

        except Exception as e:
            logger.error(f"获取最近告警失败: {e}")
            return []

    def get_hourly_stats(self, hours: int = 24) -> List[Dict]:
        """获取小时统计"""
        if not self.redis:
            return []

        try:
            hourly_stats = []
            current_time = datetime.now()

            for i in range(hours):
                hour_time = current_time - timedelta(hours=i)
                hour_key = (
                    f"{self.hourly_stats_key}:{hour_time.strftime('%Y-%m-%d-%H')}"
                )

                stats_data = self.redis.get(hour_key)
                if stats_data:
                    stats = json.loads(stats_data)
                    hourly_stats.append(stats)
                else:
                    # 填充空数据
                    hourly_stats.append(
                        {
                            "hour": hour_time.strftime("%Y-%m-%d-%H"),
                            "tasks_count": 0,
                            "completed_count": 0,
                            "failed_count": 0,
                            "total_duration": 0.0,
                            "total_items": 0,
                            "worker_stats": {},
                        }
                    )

            return list(reversed(hourly_stats))

        except Exception as e:
            logger.error(f"获取小时统计失败: {e}")
            return []

    def get_worker_performance(self, worker_id: str = None) -> Dict:
        """获取工作节点性能"""
        if not self.redis:
            return {}

        try:
            # 获取所有任务指标
            all_metrics = self.redis.hgetall(self.metrics_key)
            worker_stats = defaultdict(
                lambda: {
                    "total_tasks": 0,
                    "completed_tasks": 0,
                    "failed_tasks": 0,
                    "total_duration": 0.0,
                    "avg_duration": 0.0,
                    "success_rate": 0.0,
                    "total_items": 0,
                }
            )

            for task_id, metrics_data in all_metrics.items():
                metrics = TaskMetrics.from_dict(json.loads(metrics_data))

                if worker_id and metrics.worker_id != worker_id:
                    continue

                stats = worker_stats[metrics.worker_id]
                stats["total_tasks"] += 1

                if metrics.status == "completed":
                    stats["completed_tasks"] += 1
                elif metrics.status == "failed":
                    stats["failed_tasks"] += 1

                if metrics.duration:
                    stats["total_duration"] += metrics.duration

                stats["total_items"] += metrics.items_scraped

            # 计算平均值和比率
            for wid, stats in worker_stats.items():
                if stats["total_tasks"] > 0:
                    stats["avg_duration"] = (
                        stats["total_duration"] / stats["total_tasks"]
                    )
                    stats["success_rate"] = (
                        stats["completed_tasks"] / stats["total_tasks"]
                    )

            if worker_id:
                return worker_stats.get(worker_id, {})
            else:
                return dict(worker_stats)

        except Exception as e:
            logger.error(f"获取工作节点性能失败: {e}")
            return {}

    def cleanup_old_metrics(self, days: int = 7):
        """清理旧的指标数据"""
        if not self.redis:
            return

        try:
            cutoff_time = time.time() - (days * 24 * 3600)
            all_metrics = self.redis.hgetall(self.metrics_key)

            removed_count = 0
            for task_id, metrics_data in all_metrics.items():
                metrics = TaskMetrics.from_dict(json.loads(metrics_data))
                if metrics.start_time < cutoff_time:
                    self.redis.hdel(self.metrics_key, task_id)
                    removed_count += 1

            logger.info(f"清理了 {removed_count} 个旧的任务指标")

        except Exception as e:
            logger.error(f"清理旧指标失败: {e}")


def main():
    """主函数 - 测试任务监控器"""
    print("📊 任务监控器测试")
    print("=" * 60)

    if not REDIS_AVAILABLE:
        print("❌ Redis不可用，请安装: pip install redis")
        return False

    # 创建监控器
    monitor = TaskMonitor()

    if not monitor.redis:
        print("❌ Redis连接失败")
        return False

    # 模拟任务监控
    test_tasks = [
        {"task_id": "task_001", "worker_id": "worker_001"},
        {"task_id": "task_002", "worker_id": "worker_002"},
        {"task_id": "task_003", "worker_id": "worker_001"},
    ]

    print("🚀 开始监控测试任务...")
    for task in test_tasks:
        success = monitor.start_task_monitoring(task["task_id"], task["worker_id"])
        print(f"   {task['task_id']}: {'✅' if success else '❌'}")

    # 模拟任务执行和指标更新
    print("\n📈 更新任务指标...")
    for i, task in enumerate(test_tasks):
        metrics_update = {
            "items_scraped": 10 + i * 5,
            "pages_crawled": 2 + i,
            "memory_usage": 100 + i * 50,
            "cpu_usage": 30 + i * 20,
        }
        success = monitor.update_task_metrics(task["task_id"], metrics_update)
        print(f"   {task['task_id']}: {'✅' if success else '❌'}")

    # 模拟任务完成
    print("\n✅ 完成任务监控...")
    for i, task in enumerate(test_tasks):
        if i < 2:  # 前两个任务成功
            final_metrics = {"items_scraped": 15 + i * 5}
            success = monitor.complete_task_monitoring(task["task_id"], final_metrics)
        else:  # 最后一个任务失败
            error_info = {"error": "Connection timeout", "code": "TIMEOUT"}
            success = monitor.fail_task_monitoring(task["task_id"], error_info)

        print(f"   {task['task_id']}: {'✅' if success else '❌'}")

    # 获取性能统计
    print("\n📊 性能统计:")
    stats = monitor.get_performance_stats()
    for key, value in stats.items():
        if isinstance(value, float):
            print(f"   {key}: {value:.2f}")
        else:
            print(f"   {key}: {value}")

    # 获取告警信息
    print("\n🚨 最近告警:")
    alerts = monitor.get_recent_alerts(hours=1)
    if alerts:
        for alert in alerts[:3]:  # 显示前3个
            print(f"   {alert['type']}: {alert['data']}")
    else:
        print("   无告警")

    # 获取工作节点性能
    print("\n👥 工作节点性能:")
    worker_perf = monitor.get_worker_performance()
    for worker_id, perf in worker_perf.items():
        print(f"   {worker_id}:")
        print(f"      总任务: {perf['total_tasks']}")
        print(f"      成功率: {perf['success_rate']:.2%}")
        print(f"      平均耗时: {perf['avg_duration']:.2f}秒")

    print("\n✅ 任务监控器测试完成")
    return True


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
