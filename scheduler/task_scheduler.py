#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
分布式任务调度器

实现分布式爬虫任务的调度、分发和管理
"""

import hashlib
import json
import logging
import time
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Dict, List, Optional

try:
    import redis

    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TaskPriority(Enum):
    """任务优先级"""

    LOW = 1
    NORMAL = 2
    HIGH = 3
    URGENT = 4


class TaskStatus(Enum):
    """任务状态"""

    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"


@dataclass
class CrawlTask:
    """爬虫任务数据结构"""

    spider_name: str
    url: str
    priority: TaskPriority
    site_config: Dict
    task_id: str = None
    retry_count: int = 0
    max_retries: int = 3
    created_at: float = None
    scheduled_at: float = None
    status: TaskStatus = TaskStatus.PENDING
    metadata: Dict = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = time.time()
        if self.task_id is None:
            self.task_id = self.generate_task_id()
        if self.metadata is None:
            self.metadata = {}

    def generate_task_id(self) -> str:
        """生成唯一任务ID"""
        # 如果 url 为空，则使用 site_name 作为任务 ID 的一部分
        identifier = self.url if self.url else self.site_config.get("site", "default")
        content = f"{self.spider_name}:{identifier}:{self.created_at}"
        return hashlib.md5(content.encode()).hexdigest()

    def to_dict(self) -> Dict:
        """转换为字典"""
        data = asdict(self)
        data["priority"] = self.priority.value
        data["status"] = self.status.value
        return data

    @classmethod
    def from_dict(cls, data: Dict) -> "CrawlTask":
        """从字典创建任务"""
        data["priority"] = TaskPriority(data["priority"])
        data["status"] = TaskStatus(data["status"])
        return cls(**data)


class DistributedTaskScheduler:
    """分布式任务调度器"""

    def __init__(self, redis_url="redis://localhost:6379/0"):
        self.redis_url = redis_url
        self.redis = None

        # Redis键名
        self.task_queue_key = "crawler:task_queue"
        self.processing_key = "crawler:processing"
        self.completed_key = "crawler:completed"
        self.failed_key = "crawler:failed"
        self.stats_key = "crawler:stats"

        # 初始化Redis连接
        self.connect_redis()

        logger.info("分布式任务调度器初始化完成")

    def connect_redis(self) -> bool:
        """连接Redis"""
        if not REDIS_AVAILABLE:
            logger.error("Redis不可用，请安装redis-py")
            return False

        try:
            self.redis = redis.from_url(self.redis_url)
            # 测试连接
            self.redis.ping()
            logger.info("Redis连接成功")
            return True
        except Exception as e:
            logger.error(f"Redis连接失败: {e}")
            return False

    def submit_task(self, task: CrawlTask) -> bool:
        """提交任务到队列"""
        if not self.redis:
            logger.error("Redis未连接")
            return False

        try:
            # 如果 url 为空，则使用 site_name 作为任务的标识
            if not task.url:
                task.url = task.site_config.get("site", "default")
                task.task_id = task.generate_task_id()  # 重新生成任务ID以反映新的url

            # 检查任务是否已存在
            if self.is_task_exists(task.task_id):
                logger.warning(f"任务已存在: {task.task_id}")
                return False

            # 根据优先级分配到不同队列
            queue_key = f"{self.task_queue_key}:{task.priority.name.lower()}"

            # 序列化任务
            task_data = json.dumps(task.to_dict())

            # 添加到Redis队列
            self.redis.lpush(queue_key, task_data)

            # 更新统计
            self.update_stats("tasks_submitted", 1)

            logger.info(f"任务提交成功: {task.task_id} (优先级: {task.priority.name})")
            return True

        except Exception as e:
            logger.error(f"提交任务失败: {e}")
            return False

    def get_next_task(self, worker_id: str) -> Optional[CrawlTask]:
        """获取下一个任务"""
        if not self.redis:
            return None

        # 按优先级顺序检查队列
        priority_queues = [
            f"{self.task_queue_key}:urgent",
            f"{self.task_queue_key}:high",
            f"{self.task_queue_key}:normal",
            f"{self.task_queue_key}:low",
        ]

        for queue_key in priority_queues:
            try:
                # 非阻塞获取任务
                task_data = self.redis.rpop(queue_key)
                if task_data:
                    task_dict = json.loads(task_data)
                    task = CrawlTask.from_dict(task_dict)

                    # 标记任务为处理中
                    self.mark_task_processing(task, worker_id)

                    logger.info(f"分配任务给工作节点 {worker_id}: {task.task_id}")
                    return task

            except Exception as e:
                logger.error(f"获取任务失败: {e}")
                continue

        return None

    def mark_task_processing(self, task: CrawlTask, worker_id: str):
        """标记任务为处理中"""
        if not self.redis:
            return

        task.status = TaskStatus.PROCESSING
        processing_data = {
            "task": task.to_dict(),
            "worker_id": worker_id,
            "start_time": time.time(),
        }

        self.redis.hset(self.processing_key, task.task_id, json.dumps(processing_data))

        # 更新统计
        self.update_stats("tasks_processing", 1)

    def complete_task(self, task_id: str, result: Dict):
        """完成任务"""
        if not self.redis:
            return

        try:
            # 从处理中移除
            processing_data = self.redis.hget(self.processing_key, task_id)
            if processing_data:
                self.redis.hdel(self.processing_key, task_id)
                self.update_stats("tasks_processing", -1)

            # 添加到完成队列
            completion_data = {
                "task_id": task_id,
                "result": result,
                "completed_at": time.time(),
            }

            self.redis.hset(self.completed_key, task_id, json.dumps(completion_data))

            # 更新统计
            self.update_stats("tasks_completed", 1)

            logger.info(f"任务完成: {task_id}")

        except Exception as e:
            logger.error(f"完成任务失败: {e}")

    def fail_task(self, task_id: str, error: str, retry: bool = True):
        """任务失败处理"""
        if not self.redis:
            return

        try:
            processing_data = self.redis.hget(self.processing_key, task_id)
            if not processing_data:
                logger.warning(f"未找到处理中的任务: {task_id}")
                return

            processing_info = json.loads(processing_data)
            task_dict = processing_info["task"]
            task = CrawlTask.from_dict(task_dict)

            # 检查是否需要重试
            if retry and task.retry_count < task.max_retries:
                task.retry_count += 1
                task.status = TaskStatus.RETRYING
                # 指数退避策略
                delay = (2**task.retry_count) * 60
                task.scheduled_at = time.time() + delay

                # 重新提交任务
                self.submit_task(task)
                logger.info(f"任务重试: {task_id} (第{task.retry_count}次)")
            else:
                # 标记为最终失败
                task.status = TaskStatus.FAILED
                failure_data = {
                    "task": task.to_dict(),
                    "error": error,
                    "failed_at": time.time(),
                    "retry_count": task.retry_count,
                }

                self.redis.hset(self.failed_key, task_id, json.dumps(failure_data))

                # 更新统计
                self.update_stats("tasks_failed", 1)
                logger.error(f"任务最终失败: {task_id} - {error}")

            # 从处理中移除
            self.redis.hdel(self.processing_key, task_id)
            self.update_stats("tasks_processing", -1)

        except Exception as e:
            logger.error(f"处理任务失败: {e}")

    def is_task_exists(self, task_id: str) -> bool:
        """检查任务是否已存在"""
        if not self.redis:
            return False

        # 检查各个状态的任务
        return (
            self.redis.hexists(self.processing_key, task_id)
            or self.redis.hexists(self.completed_key, task_id)
            or self.redis.hexists(self.failed_key, task_id)
        )

    def get_queue_size(self, priority: TaskPriority = None) -> int:
        """获取队列大小"""
        if not self.redis:
            return 0

        if priority:
            queue_key = f"{self.task_queue_key}:{priority.name.lower()}"
            return self.redis.llen(queue_key)
        else:
            # 返回所有队列的总大小
            total = 0
            for p in TaskPriority:
                queue_key = f"{self.task_queue_key}:{p.name.lower()}"
                total += self.redis.llen(queue_key)
            return total

    def get_stats(self) -> Dict:
        """获取调度器统计信息"""
        if not self.redis:
            return {}

        try:
            stats = {}
            for key in [
                "tasks_submitted",
                "tasks_processing",
                "tasks_completed",
                "tasks_failed",
            ]:
                value = self.redis.hget(self.stats_key, key)
                stats[key] = int(value) if value else 0

            # 添加队列大小信息
            stats["queue_sizes"] = {}
            for priority in TaskPriority:
                queue_key = f"{self.task_queue_key}:{priority.name.lower()}"
                stats["queue_sizes"][priority.name.lower()] = self.redis.llen(queue_key)

            stats["total_queue_size"] = sum(stats["queue_sizes"].values())
            stats["processing_count"] = self.redis.hlen(self.processing_key)

            return stats

        except Exception as e:
            logger.error(f"获取统计信息失败: {e}")
            return {}

    def update_stats(self, key: str, increment: int):
        """更新统计信息"""
        if not self.redis:
            return

        try:
            self.redis.hincrby(self.stats_key, key, increment)
        except Exception as e:
            logger.error(f"更新统计失败: {e}")

    def clear_completed_tasks(self, older_than_hours: int = 24):
        """清理已完成的任务"""
        if not self.redis:
            return

        try:
            cutoff_time = time.time() - (older_than_hours * 3600)
            completed_tasks = self.redis.hgetall(self.completed_key)

            removed_count = 0
            for task_id, task_data in completed_tasks.items():
                task_info = json.loads(task_data)
                if task_info.get("completed_at", 0) < cutoff_time:
                    self.redis.hdel(self.completed_key, task_id)
                    removed_count += 1

            logger.info(f"清理了 {removed_count} 个已完成的任务")

        except Exception as e:
            logger.error(f"清理任务失败: {e}")

    def get_task_status(self, task_id: str) -> Optional[Dict]:
        """获取任务状态"""
        if not self.redis:
            return None

        # 检查处理中的任务
        processing_data = self.redis.hget(self.processing_key, task_id)
        if processing_data:
            return json.loads(processing_data)

        # 检查已完成的任务
        completed_data = self.redis.hget(self.completed_key, task_id)
        if completed_data:
            return json.loads(completed_data)

        # 检查失败的任务
        failed_data = self.redis.hget(self.failed_key, task_id)
        if failed_data:
            return json.loads(failed_data)

        return None

    def submit_ai_report_task(self, site_name: str, days_ago: int = 7, priority: TaskPriority = TaskPriority.NORMAL) -> bool:
        """
        提交AI报告生成任务到队列。
        """
        if not self.redis:
            logger.error("Redis未连接")
            return False

        # AI报告生成任务的spider_name固定为 'ai_report_generator'
        # url 使用 site_name 作为唯一标识
        task = CrawlTask(
            spider_name="ai_report_generator",
            url=site_name,  # 使用站点名称作为URL，方便识别
            priority=priority,
            site_config={"site": site_name, "days_ago": days_ago},
            metadata={"task_type": "ai_report_generation", "site_name": site_name, "days_ago": days_ago}
        )
        
        return self.submit_task(task)

    def submit_bochaai_task(self, priority: TaskPriority = TaskPriority.NORMAL) -> bool:
        """提交 bochaai_spider 任务（无需URL与附加参数）。"""
        if not self.redis:
            logger.error("Redis未连接")
            return False

        task = CrawlTask(
            spider_name="bochaai_spider",
            url="",  # 不需要 URL
            priority=priority,
            site_config={},
            metadata={"task_type": "bochaai"},
        )
        return self.submit_task(task)


def main():
    """主函数 - 测试调度器"""
    print("🚀 分布式任务调度器测试")
    print("=" * 60)

    if not REDIS_AVAILABLE:
        print("❌ Redis不可用，请安装: pip install redis")
        return False

    # 创建调度器
    scheduler = DistributedTaskScheduler()

    if not scheduler.redis:
        print("❌ Redis连接失败")
        return False

    # 创建测试任务
    test_tasks = [
        CrawlTask(
            spider_name="adaptive",
            url="https://www.bjcdc.org/cdcmodule/jkdt/bsxw/index.shtml",
            priority=TaskPriority.HIGH,
            site_config={"site": "bjcdc"},
        ),
        CrawlTask(
            spider_name="adaptive",
            url="https://www.bjcdc.org/cdcmodule/jkdt/jcdt/index.shtml",
            priority=TaskPriority.NORMAL,
            site_config={"site": "bjcdc"},
        ),
        CrawlTask(
            spider_name="adaptive",
            url="https://www.bjcdc.org/cdcmodule/jkdt/yqbb/index.shtml",
            priority=TaskPriority.URGENT,
            site_config={"site": "bjcdc"},
        ),
    ]

    # 提交爬虫任务
    print("📤 提交测试爬虫任务...")
    for task in test_tasks:
        success = scheduler.submit_task(task)
        print(f"   爬虫任务 {task.task_id[:8]}... : {'✅' if success else '❌'}")

    # 提交AI报告生成任务
    print("\n📤 提交AI报告生成任务...")
    ai_report_site = "jxcdc" # 示例站点
    ai_report_task_success = scheduler.submit_ai_report_task(ai_report_site, days_ago=7, priority=TaskPriority.HIGH)
    print(f"   AI报告任务 ({ai_report_site}) : {'✅' if ai_report_task_success else '❌'}")

    # 获取统计信息
    print("\n📊 调度器统计:")
    stats = scheduler.get_stats()
    for key, value in stats.items():
        print(f"   {key}: {value}")

    # 模拟工作节点获取任务
    print("\n🔄 模拟工作节点获取任务...")
    worker_id = "test_worker_001"

    for i in range(3):
        task = scheduler.get_next_task(worker_id)
        if task:
            print(f"   获取任务: {task.task_id[:8]}... (优先级: {task.priority.name})")

            # 模拟任务完成
            result = {"status": "success", "items_count": 10}
            scheduler.complete_task(task.task_id, result)
            print(f"   完成任务: {task.task_id[:8]}...")
        else:
            print("   没有更多任务")
            break

    # 最终统计
    print("\n📈 最终统计:")
    final_stats = scheduler.get_stats()
    for key, value in final_stats.items():
        print(f"   {key}: {value}")

    print("\n✅ 调度器测试完成")
    return True


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
