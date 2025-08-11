#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
负载均衡器

实现工作节点的负载均衡和任务分发策略
"""

import json
import logging
import time
from collections import defaultdict
from dataclasses import asdict, dataclass
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
class WorkerInfo:
    """工作节点信息"""

    worker_id: str
    capabilities: Dict
    registered_at: float
    last_heartbeat: float
    active_tasks: int = 0
    completed_tasks: int = 0
    failed_tasks: int = 0
    cpu_usage: float = 0.0
    memory_usage: float = 0.0
    status: str = "active"  # active, busy, offline

    def to_dict(self) -> Dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict) -> "WorkerInfo":
        return cls(**data)


class LoadBalancer:
    """负载均衡器"""

    def __init__(self, redis_url="redis://localhost:6379/0"):
        self.redis_url = redis_url
        self.redis = None

        # Redis键名
        self.worker_stats_key = "crawler:worker_stats"
        self.site_workers_key = "crawler:site_workers"
        self.worker_heartbeat_key = "crawler:worker_heartbeat"

        # 配置参数
        self.heartbeat_timeout = 300  # 5分钟心跳超时
        self.max_tasks_per_worker = 10  # 每个工作节点最大任务数

        # 初始化Redis连接
        self.connect_redis()

        logger.info("负载均衡器初始化完成")

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

    def register_worker(self, worker_id: str, capabilities: Dict) -> bool:
        """注册工作节点"""
        if not self.redis:
            return False

        try:
            worker_info = WorkerInfo(
                worker_id=worker_id,
                capabilities=capabilities,
                registered_at=time.time(),
                last_heartbeat=time.time(),
            )

            # 保存工作节点信息
            self.redis.hset(
                self.worker_stats_key, worker_id, json.dumps(worker_info.to_dict())
            )

            # 注册到站点工作节点映射
            supported_sites = capabilities.get("supported_sites", [])
            for site in supported_sites:
                self.redis.sadd(f"{self.site_workers_key}:{site}", worker_id)

            logger.info(f"工作节点注册成功: {worker_id}")
            return True

        except Exception as e:
            logger.error(f"注册工作节点失败: {e}")
            return False

    def unregister_worker(self, worker_id: str) -> bool:
        """注销工作节点"""
        if not self.redis:
            return False

        try:
            # 获取工作节点信息
            worker_data = self.redis.hget(self.worker_stats_key, worker_id)
            if worker_data:
                worker_info = WorkerInfo.from_dict(json.loads(worker_data))

                # 从站点工作节点映射中移除
                supported_sites = worker_info.capabilities.get("supported_sites", [])
                for site in supported_sites:
                    self.redis.srem(f"{self.site_workers_key}:{site}", worker_id)

            # 删除工作节点信息
            self.redis.hdel(self.worker_stats_key, worker_id)
            self.redis.hdel(self.worker_heartbeat_key, worker_id)

            logger.info(f"工作节点注销成功: {worker_id}")
            return True

        except Exception as e:
            logger.error(f"注销工作节点失败: {e}")
            return False

    def update_worker_heartbeat(self, worker_id: str, stats: Dict = None) -> bool:
        """更新工作节点心跳"""
        if not self.redis:
            return False

        try:
            # 更新心跳时间
            heartbeat_data = {"timestamp": time.time(), "stats": stats or {}}

            self.redis.hset(
                self.worker_heartbeat_key, worker_id, json.dumps(heartbeat_data)
            )

            # 更新工作节点统计信息
            worker_data = self.redis.hget(self.worker_stats_key, worker_id)
            if worker_data:
                worker_info = WorkerInfo.from_dict(json.loads(worker_data))
                worker_info.last_heartbeat = time.time()

                # 更新性能统计
                if stats:
                    worker_info.cpu_usage = stats.get(
                        "cpu_usage", worker_info.cpu_usage
                    )
                    worker_info.memory_usage = stats.get(
                        "memory_usage", worker_info.memory_usage
                    )
                    worker_info.active_tasks = stats.get(
                        "active_tasks", worker_info.active_tasks
                    )

                self.redis.hset(
                    self.worker_stats_key, worker_id, json.dumps(worker_info.to_dict())
                )

            return True

        except Exception as e:
            logger.error(f"更新心跳失败: {e}")
            return False

    def get_best_worker(
        self, site: str = None, task_requirements: Dict = None
    ) -> Optional[str]:
        """获取最佳工作节点"""
        if not self.redis:
            return None

        try:
            # 获取可用的工作节点
            available_workers = self.get_available_workers(site)

            if not available_workers:
                logger.warning(f"没有可用的工作节点 (site: {site})")
                return None

            # 根据负载均衡策略选择最佳节点
            best_worker = self.select_worker_by_strategy(
                available_workers, task_requirements
            )

            if best_worker:
                logger.debug(f"选择工作节点: {best_worker} (site: {site})")

            return best_worker

        except Exception as e:
            logger.error(f"获取最佳工作节点失败: {e}")
            return None

    def get_available_workers(self, site: str = None) -> List[str]:
        """获取可用的工作节点列表"""
        if not self.redis:
            return []

        try:
            # 获取所有工作节点或特定站点的工作节点
            if site:
                worker_ids = self.redis.smembers(f"{self.site_workers_key}:{site}")
                worker_ids = [
                    w.decode() if isinstance(w, bytes) else w for w in worker_ids
                ]
            else:
                worker_ids = self.redis.hkeys(self.worker_stats_key)
                worker_ids = [
                    w.decode() if isinstance(w, bytes) else w for w in worker_ids
                ]

            available_workers = []
            current_time = time.time()

            for worker_id in worker_ids:
                worker_data = self.redis.hget(self.worker_stats_key, worker_id)
                if not worker_data:
                    continue

                worker_info = WorkerInfo.from_dict(json.loads(worker_data))

                # 检查心跳是否超时
                if current_time - worker_info.last_heartbeat > self.heartbeat_timeout:
                    worker_info.status = "offline"
                    continue

                # 检查是否过载
                if worker_info.active_tasks >= self.max_tasks_per_worker:
                    worker_info.status = "busy"
                    continue

                # 检查CPU和内存使用率
                if worker_info.cpu_usage > 90 or worker_info.memory_usage > 90:
                    worker_info.status = "busy"
                    continue

                worker_info.status = "active"
                available_workers.append(worker_id)

            return available_workers

        except Exception as e:
            logger.error(f"获取可用工作节点失败: {e}")
            return []

    def select_worker_by_strategy(
        self, workers: List[str], task_requirements: Dict = None
    ) -> Optional[str]:
        """根据策略选择工作节点"""
        if not workers:
            return None

        if not self.redis:
            return workers[0]  # 降级到简单轮询

        try:
            # 获取所有工作节点的详细信息
            worker_scores = []

            for worker_id in workers:
                worker_data = self.redis.hget(self.worker_stats_key, worker_id)
                if not worker_data:
                    continue

                worker_info = WorkerInfo.from_dict(json.loads(worker_data))

                # 计算工作节点得分（越低越好）
                score = self.calculate_worker_score(worker_info, task_requirements)
                worker_scores.append((worker_id, score))

            if not worker_scores:
                return None

            # 按得分排序，选择最佳节点
            worker_scores.sort(key=lambda x: x[1])
            return worker_scores[0][0]

        except Exception as e:
            logger.error(f"选择工作节点失败: {e}")
            return workers[0] if workers else None

    def calculate_worker_score(
        self, worker_info: WorkerInfo, task_requirements: Dict = None
    ) -> float:
        """计算工作节点得分"""
        score = 0.0

        # 基于当前负载的得分
        score += worker_info.active_tasks * 10  # 活跃任务数权重
        score += worker_info.cpu_usage * 0.5  # CPU使用率权重
        score += worker_info.memory_usage * 0.3  # 内存使用率权重

        # 基于历史表现的得分
        total_tasks = worker_info.completed_tasks + worker_info.failed_tasks
        if total_tasks > 0:
            failure_rate = worker_info.failed_tasks / total_tasks
            score += failure_rate * 100  # 失败率权重

        # 基于任务要求的得分
        if task_requirements:
            # 检查能力匹配度
            required_capabilities = task_requirements.get("capabilities", [])
            worker_capabilities = worker_info.capabilities.get("features", [])

            missing_capabilities = set(required_capabilities) - set(worker_capabilities)
            score += len(missing_capabilities) * 50  # 缺失能力惩罚

        return score

    def update_worker_task_count(self, worker_id: str, increment: int):
        """更新工作节点任务计数"""
        if not self.redis:
            return

        try:
            worker_data = self.redis.hget(self.worker_stats_key, worker_id)
            if worker_data:
                worker_info = WorkerInfo.from_dict(json.loads(worker_data))
                worker_info.active_tasks = max(0, worker_info.active_tasks + increment)

                if increment > 0:
                    # 任务开始
                    pass
                else:
                    # 任务完成或失败
                    if increment == -1:
                        worker_info.completed_tasks += 1
                    elif increment == -2:  # 特殊标记表示失败
                        worker_info.failed_tasks += 1
                        worker_info.active_tasks = max(
                            0, worker_info.active_tasks + 1
                        )  # 补偿-2

                self.redis.hset(
                    self.worker_stats_key, worker_id, json.dumps(worker_info.to_dict())
                )

        except Exception as e:
            logger.error(f"更新工作节点任务计数失败: {e}")

    def get_worker_stats(self, worker_id: str = None) -> Dict:
        """获取工作节点统计信息"""
        if not self.redis:
            return {}

        try:
            if worker_id:
                # 获取单个工作节点信息
                worker_data = self.redis.hget(self.worker_stats_key, worker_id)
                if worker_data:
                    return json.loads(worker_data)
                return {}
            else:
                # 获取所有工作节点信息
                all_workers = {}
                worker_data = self.redis.hgetall(self.worker_stats_key)

                for wid, data in worker_data.items():
                    worker_id_str = wid.decode() if isinstance(wid, bytes) else wid
                    worker_info = json.loads(data)
                    all_workers[worker_id_str] = worker_info

                return all_workers

        except Exception as e:
            logger.error(f"获取工作节点统计失败: {e}")
            return {}

    def cleanup_offline_workers(self):
        """清理离线的工作节点"""
        if not self.redis:
            return

        try:
            current_time = time.time()
            worker_data = self.redis.hgetall(self.worker_stats_key)

            offline_workers = []
            for wid, data in worker_data.items():
                worker_id = wid.decode() if isinstance(wid, bytes) else wid
                worker_info = WorkerInfo.from_dict(json.loads(data))

                # 检查是否长时间离线
                if (
                    current_time - worker_info.last_heartbeat
                    > self.heartbeat_timeout * 2
                ):
                    offline_workers.append(worker_id)

            # 清理离线工作节点
            for worker_id in offline_workers:
                self.unregister_worker(worker_id)
                logger.info(f"清理离线工作节点: {worker_id}")

        except Exception as e:
            logger.error(f"清理离线工作节点失败: {e}")

    def get_load_balance_stats(self) -> Dict:
        """获取负载均衡统计信息"""
        if not self.redis:
            return {}

        try:
            stats = {
                "total_workers": 0,
                "active_workers": 0,
                "busy_workers": 0,
                "offline_workers": 0,
                "total_active_tasks": 0,
                "average_cpu_usage": 0.0,
                "average_memory_usage": 0.0,
                "site_distribution": defaultdict(int),
            }

            worker_data = self.redis.hgetall(self.worker_stats_key)
            current_time = time.time()

            cpu_sum = 0.0
            memory_sum = 0.0

            for wid, data in worker_data.items():
                worker_id = wid.decode() if isinstance(wid, bytes) else wid
                worker_info = WorkerInfo.from_dict(json.loads(data))

                stats["total_workers"] += 1
                stats["total_active_tasks"] += worker_info.active_tasks
                cpu_sum += worker_info.cpu_usage
                memory_sum += worker_info.memory_usage

                # 统计工作节点状态
                if current_time - worker_info.last_heartbeat > self.heartbeat_timeout:
                    stats["offline_workers"] += 1
                elif worker_info.active_tasks >= self.max_tasks_per_worker:
                    stats["busy_workers"] += 1
                else:
                    stats["active_workers"] += 1

                # 统计站点分布
                supported_sites = worker_info.capabilities.get("supported_sites", [])
                for site in supported_sites:
                    stats["site_distribution"][site] += 1

            # 计算平均值
            if stats["total_workers"] > 0:
                stats["average_cpu_usage"] = cpu_sum / stats["total_workers"]
                stats["average_memory_usage"] = memory_sum / stats["total_workers"]

            stats["site_distribution"] = dict(stats["site_distribution"])
            return stats

        except Exception as e:
            logger.error(f"获取负载均衡统计失败: {e}")
            return {}


def main():
    """主函数 - 测试负载均衡器"""
    print("⚖️ 负载均衡器测试")
    print("=" * 60)

    if not REDIS_AVAILABLE:
        print("❌ Redis不可用，请安装: pip install redis")
        return False

    # 创建负载均衡器
    balancer = LoadBalancer()

    if not balancer.redis:
        print("❌ Redis连接失败")
        return False

    # 注册测试工作节点
    test_workers = [
        {
            "worker_id": "worker_001",
            "capabilities": {
                "supported_sites": ["bjcdc", "general"],
                "features": ["javascript", "selenium"],
                "max_concurrent_tasks": 5,
            },
        },
        {
            "worker_id": "worker_002",
            "capabilities": {
                "supported_sites": ["bjcdc"],
                "features": ["basic_crawling"],
                "max_concurrent_tasks": 3,
            },
        },
        {
            "worker_id": "worker_003",
            "capabilities": {
                "supported_sites": ["general"],
                "features": ["javascript", "proxy"],
                "max_concurrent_tasks": 8,
            },
        },
    ]

    print("📝 注册测试工作节点...")
    for worker in test_workers:
        success = balancer.register_worker(worker["worker_id"], worker["capabilities"])
        print(f"   {worker['worker_id']}: {'✅' if success else '❌'}")

    # 模拟心跳更新
    print(f"\n💓 更新工作节点心跳...")
    for worker in test_workers:
        stats = {"cpu_usage": 45.0, "memory_usage": 60.0, "active_tasks": 2}
        success = balancer.update_worker_heartbeat(worker["worker_id"], stats)
        print(f"   {worker['worker_id']}: {'✅' if success else '❌'}")

    # 测试工作节点选择
    print(f"\n🎯 测试工作节点选择...")
    test_sites = ["bjcdc", "general", "unknown"]

    for site in test_sites:
        best_worker = balancer.get_best_worker(site)
        print(f"   站点 {site}: {best_worker if best_worker else '无可用节点'}")

    # 获取统计信息
    print(f"\n📊 负载均衡统计:")
    stats = balancer.get_load_balance_stats()
    for key, value in stats.items():
        print(f"   {key}: {value}")

    # 获取工作节点详细信息
    print(f"\n👥 工作节点详细信息:")
    worker_stats = balancer.get_worker_stats()
    for worker_id, info in worker_stats.items():
        print(f"   {worker_id}:")
        print(f"      状态: {info.get('status', 'unknown')}")
        print(f"      活跃任务: {info.get('active_tasks', 0)}")
        print(f"      CPU使用率: {info.get('cpu_usage', 0):.1f}%")
        print(f"      内存使用率: {info.get('memory_usage', 0):.1f}%")

    print(f"\n✅ 负载均衡器测试完成")
    return True


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
