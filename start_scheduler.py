#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
分布式调度系统启动脚本

启动和管理分布式爬虫调度系统的各个组件
"""

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Dict, List

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from scheduler.config_manager import ConfigManager
    from scheduler.load_balancer import LoadBalancer
    from scheduler.task_monitor import TaskMonitor
    from scheduler.task_scheduler import (
        CrawlTask,
        DistributedTaskScheduler,
        TaskPriority,
    )
    from scheduler.worker_node import DistributedWorkerNode, WorkerConfig

    SCHEDULER_AVAILABLE = True
except ImportError:
    SCHEDULER_AVAILABLE = False

# 配置日志
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
)
logger = logging.getLogger(__name__)


class SchedulerSystemManager:
    """调度系统管理器"""

    def __init__(self, redis_url="redis://localhost:6379/0"):
        self.redis_url = redis_url
        self.scheduler = None
        self.load_balancer = None
        self.task_monitor = None
        self.config_manager = None

        logger.info("调度系统管理器初始化")

    def initialize_components(self):
        """初始化所有组件"""
        if not SCHEDULER_AVAILABLE:
            logger.error("调度器组件不可用")
            return False

        try:
            logger.info("初始化调度系统组件...")

            # 初始化任务调度器
            self.scheduler = DistributedTaskScheduler(self.redis_url)
            if not self.scheduler.redis:
                logger.error("任务调度器初始化失败")
                return False

            # 初始化负载均衡器
            self.load_balancer = LoadBalancer(self.redis_url)
            if not self.load_balancer.redis:
                logger.error("负载均衡器初始化失败")
                return False

            # 初始化任务监控器
            self.task_monitor = TaskMonitor(self.redis_url)
            if not self.task_monitor.redis:
                logger.error("任务监控器初始化失败")
                return False

            # 初始化配置管理器
            config_dirs = ["config", "config/sites", "config/extraction"]
            self.config_manager = ConfigManager(config_dirs, self.redis_url)

            logger.info("所有组件初始化成功")
            return True

        except Exception as e:
            logger.error(f"组件初始化失败: {e}")
            return False

    def start_config_monitoring(self):
        """启动配置监控"""
        if self.config_manager:
            self.config_manager.start_file_monitoring()
            self.config_manager.subscribe_config_updates()
            logger.info("配置监控已启动")

    def submit_test_tasks(self):
        """提交测试任务"""
        if not self.scheduler:
            logger.error("调度器不可用")
            return

        logger.info("提交测试任务...")

        test_tasks = [
            {
                "spider_name": "adaptive",
                "url": "https://www.bjcdc.org/cdcmodule/jkdt/bsxw/index.shtml",
                "priority": TaskPriority.HIGH,
                "site_config": {"site": "bjcdc"},
            },
            {
                "spider_name": "adaptive",
                "url": "https://www.bjcdc.org/cdcmodule/jkdt/jcdt/index.shtml",
                "priority": TaskPriority.NORMAL,
                "site_config": {"site": "bjcdc"},
            },
            {
                "spider_name": "adaptive",
                "url": "https://www.bjcdc.org/cdcmodule/jkdt/yqbb/index.shtml",
                "priority": TaskPriority.URGENT,
                "site_config": {"site": "bjcdc"},
            },
        ]

        submitted_count = 0
        for task_data in test_tasks:
            task = CrawlTask(**task_data)
            success = self.scheduler.submit_task(task)
            if success:
                submitted_count += 1
                logger.info(
                    f"任务提交成功: {task.task_id[:8]}... (优先级: {task.priority.name})"
                )
            else:
                logger.error(f"任务提交失败: {task.url}")

        logger.info(f"共提交 {submitted_count}/{len(test_tasks)} 个任务")

    def submit_custom_task(
        self, spider_name: str, url: str, site: str, priority: str = "NORMAL"
    ):
        """提交自定义任务"""
        if not self.scheduler:
            logger.error("调度器不可用")
            return False

        try:
            # 转换优先级
            priority_map = {
                "LOW": TaskPriority.LOW,
                "NORMAL": TaskPriority.NORMAL,
                "HIGH": TaskPriority.HIGH,
                "URGENT": TaskPriority.URGENT,
            }

            task_priority = priority_map.get(priority.upper(), TaskPriority.NORMAL)

            # 创建任务
            task = CrawlTask(
                spider_name=spider_name,
                url=url,
                priority=task_priority,
                site_config={"site": site},
            )

            # 提交任务
            success = self.scheduler.submit_task(task)

            if success:
                logger.info(
                    f"任务提交成功: {task.task_id[:8]}... (URL: {url}, 优先级: {priority})"
                )
                return True
            else:
                logger.error(f"任务提交失败: {url}")
                return False

        except Exception as e:
            logger.error(f"提交任务失败: {e}")
            return False

    def submit_batch_tasks(self, task_file: str):
        """从文件批量提交任务"""
        if not self.scheduler:
            logger.error("调度器不可用")
            return

        try:
            import json
            from pathlib import Path

            task_file_path = Path(task_file)
            if not task_file_path.exists():
                logger.error(f"任务文件不存在: {task_file}")
                return

            # 读取任务文件
            with open(task_file_path, "r", encoding="utf-8") as f:
                if task_file_path.suffix.lower() == ".json":
                    tasks_data = json.load(f)
                else:
                    # 支持简单的文本格式：每行一个URL
                    lines = f.readlines()
                    tasks_data = []
                    for line in lines:
                        line = line.strip()
                        if line and not line.startswith("#"):
                            tasks_data.append(
                                {
                                    "url": line,
                                    "spider_name": "adaptive",
                                    "site": "bjcdc",
                                    "priority": "NORMAL",
                                }
                            )

            logger.info(f"从文件加载了 {len(tasks_data)} 个任务")

            # 批量提交任务
            submitted_count = 0
            for task_data in tasks_data:
                success = self.submit_custom_task(
                    spider_name=task_data.get("spider_name", "adaptive"),
                    url=task_data["url"],
                    site=task_data.get("site", "bjcdc"),
                    priority=task_data.get("priority", "NORMAL"),
                )
                if success:
                    submitted_count += 1

            logger.info(f"批量提交完成: {submitted_count}/{len(tasks_data)} 个任务成功")

        except Exception as e:
            logger.error(f"批量提交任务失败: {e}")

    def submit_site_tasks(self, site: str):
        """提交指定站点的所有任务"""
        if not self.scheduler:
            logger.error("调度器不可用")
            return

        # 预定义的站点任务
        site_tasks = {
            "bjcdc": [
                "https://www.bjcdc.org/cdcmodule/jkdt/bsxw/index.shtml",  # 中心要闻
                "https://www.bjcdc.org/cdcmodule/jkdt/jcdt/index.shtml",  # 基层动态
                "https://www.bjcdc.org/cdcmodule/jkdt/yqbb/index.shtml",  # 疫情播报
                "https://www.bjcdc.org/cdcmodule/jkdt/zytz/index.shtml",  # 重要通知
            ],
            "nhc": [
                "http://www.nhc.gov.cn/xcs/yqtb/list_gzbd.shtml",  # 疫情通报
                "http://www.nhc.gov.cn/xcs/yqfkdt/list_gzbd.shtml",  # 防控动态
            ],
        }

        if site not in site_tasks:
            logger.error(f"不支持的站点: {site}")
            logger.info(f"支持的站点: {list(site_tasks.keys())}")
            return

        urls = site_tasks[site]
        logger.info(f"提交 {site} 站点的 {len(urls)} 个任务...")

        submitted_count = 0
        for url in urls:
            success = self.submit_custom_task(
                spider_name="adaptive", url=url, site=site, priority="NORMAL"
            )
            if success:
                submitted_count += 1

        logger.info(f"站点任务提交完成: {submitted_count}/{len(urls)} 个任务成功")

    def get_system_status(self) -> Dict:
        """获取系统状态"""
        status = {
            "timestamp": time.time(),
            "scheduler": {},
            "load_balancer": {},
            "task_monitor": {},
            "config_manager": {},
        }

        try:
            # 调度器状态
            if self.scheduler:
                status["scheduler"] = self.scheduler.get_stats()

            # 负载均衡器状态
            if self.load_balancer:
                status["load_balancer"] = self.load_balancer.get_load_balance_stats()

            # 任务监控器状态
            if self.task_monitor:
                status["task_monitor"] = {
                    "performance_stats": self.task_monitor.get_performance_stats(),
                    "recent_alerts": len(self.task_monitor.get_recent_alerts(hours=1)),
                }

            # 配置管理器状态
            if self.config_manager:
                status["config_manager"] = {
                    "config_versions": self.config_manager.get_config_versions()
                }

        except Exception as e:
            logger.error(f"获取系统状态失败: {e}")

        return status

    def print_system_status(self):
        """打印系统状态"""
        status = self.get_system_status()

        print("\n" + "=" * 60)
        print("📊 分布式调度系统状态")
        print("=" * 60)

        # 调度器状态
        scheduler_stats = status.get("scheduler", {})
        print(f"🚀 任务调度器:")
        print(f"   提交任务数: {scheduler_stats.get('tasks_submitted', 0)}")
        print(f"   处理中任务: {scheduler_stats.get('processing_count', 0)}")
        print(f"   完成任务数: {scheduler_stats.get('tasks_completed', 0)}")
        print(f"   失败任务数: {scheduler_stats.get('tasks_failed', 0)}")
        print(f"   队列大小: {scheduler_stats.get('total_queue_size', 0)}")

        # 负载均衡器状态
        lb_stats = status.get("load_balancer", {})
        print(f"\n⚖️ 负载均衡器:")
        print(f"   总工作节点: {lb_stats.get('total_workers', 0)}")
        print(f"   活跃节点: {lb_stats.get('active_workers', 0)}")
        print(f"   繁忙节点: {lb_stats.get('busy_workers', 0)}")
        print(f"   离线节点: {lb_stats.get('offline_workers', 0)}")
        print(f"   平均CPU: {lb_stats.get('average_cpu_usage', 0):.1f}%")
        print(f"   平均内存: {lb_stats.get('average_memory_usage', 0):.1f}%")

        # 任务监控器状态
        monitor_stats = status.get("task_monitor", {})
        perf_stats = monitor_stats.get("performance_stats", {})
        print(f"\n📊 任务监控器:")
        print(f"   总任务数: {perf_stats.get('total_tasks', 0)}")
        print(f"   成功率: {perf_stats.get('success_rate', 0):.1%}")
        print(f"   平均耗时: {perf_stats.get('avg_duration', 0):.2f}秒")
        print(f"   吞吐量: {perf_stats.get('throughput', 0):.2f}任务/小时")
        print(f"   最近告警: {monitor_stats.get('recent_alerts', 0)}个")

        # 配置管理器状态
        config_stats = status.get("config_manager", {})
        config_versions = config_stats.get("config_versions", {})
        print(f"\n⚙️ 配置管理器:")
        print(f"   配置文件数: {len(config_versions)}")
        for name, info in list(config_versions.items())[:3]:  # 显示前3个
            print(f"   {name}: v{info.get('version', 'unknown')}")

    def cleanup_system(self):
        """清理系统"""
        logger.info("清理系统资源...")

        try:
            # 清理任务监控器
            if self.task_monitor:
                self.task_monitor.cleanup_old_metrics(days=7)

            # 清理负载均衡器
            if self.load_balancer:
                self.load_balancer.cleanup_offline_workers()

            # 清理调度器
            if self.scheduler:
                self.scheduler.clear_completed_tasks(older_than_hours=24)

            # 清理配置管理器
            if self.config_manager:
                self.config_manager.cleanup_old_versions(keep_versions=5)

            logger.info("系统清理完成")

        except Exception as e:
            logger.error(f"系统清理失败: {e}")


def create_worker_node(worker_id: str, capabilities: Dict) -> DistributedWorkerNode:
    """创建工作节点"""
    config = WorkerConfig(
        worker_id=worker_id,
        capabilities=capabilities,
        max_concurrent_tasks=3,
        heartbeat_interval=30,
        config_dirs=["config"],
    )

    return DistributedWorkerNode(config)


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="分布式调度系统管理")
    parser.add_argument(
        "--mode",
        choices=["manager", "worker", "status", "submit", "cleanup"],
        default="manager",
        help="运行模式",
    )
    parser.add_argument("--worker-id", help="工作节点ID")
    parser.add_argument(
        "--redis-url", default="redis://localhost:6379/0", help="Redis连接URL"
    )

    # 任务提交相关参数
    parser.add_argument("--spider", default="adaptive", help="爬虫名称")
    parser.add_argument("--url", help="要爬取的URL")
    parser.add_argument("--site", help="站点名称")
    parser.add_argument(
        "--priority",
        choices=["LOW", "NORMAL", "HIGH", "URGENT"],
        default="NORMAL",
        help="任务优先级",
    )
    parser.add_argument("--task-file", help="批量任务文件路径")
    parser.add_argument("--site-tasks", help="提交指定站点的所有任务")

    args = parser.parse_args()

    print("🚀 分布式调度系统")
    print("=" * 60)

    if not SCHEDULER_AVAILABLE:
        print("❌ 调度器组件不可用，请检查依赖")
        return False

    # 创建系统管理器
    manager = SchedulerSystemManager(args.redis_url)

    if args.mode == "manager":
        # 管理器模式 - 启动调度系统
        print("📋 启动调度系统管理器...")

        if not manager.initialize_components():
            print("❌ 组件初始化失败")
            return False

        # 启动配置监控
        manager.start_config_monitoring()

        print("✅ 调度系统启动成功")
        print("💡 使用以下命令:")
        print("   python start_scheduler.py --mode status    # 查看状态")
        print("   python start_scheduler.py --mode submit    # 提交测试任务")
        print(
            "   python start_scheduler.py --mode submit --url <URL> --site <SITE>  # 提交单个任务"
        )
        print("   python start_scheduler.py --mode submit --site-tasks bjcdc  # 提交站点任务")
        print(
            "   python start_scheduler.py --mode submit --task-file tasks.json  # 批量提交"
        )
        print(
            "   python start_scheduler.py --mode worker --worker-id worker_001  # 启动工作节点"
        )
        print("   python start_scheduler.py --mode cleanup   # 清理系统")

        try:
            while True:
                time.sleep(10)
                manager.print_system_status()
        except KeyboardInterrupt:
            print("\n🛑 停止调度系统...")
            if manager.config_manager:
                manager.config_manager.stop()

    elif args.mode == "worker":
        # 工作节点模式
        worker_id = args.worker_id or f"worker_{int(time.time())}"
        print(f"🤖 启动工作节点: {worker_id}")

        capabilities = {
            "supported_sites": ["bjcdc", "general"],
            "features": ["basic_crawling", "javascript"],
            "max_concurrent_tasks": 3,
        }

        worker = create_worker_node(worker_id, capabilities)

        try:
            worker.start()
            print(f"✅ 工作节点启动成功")

            while worker.running:
                time.sleep(1)

        except KeyboardInterrupt:
            print(f"\n🛑 停止工作节点...")
        finally:
            worker.stop()

    elif args.mode == "status":
        # 状态查看模式
        print("📊 查看系统状态...")

        if not manager.initialize_components():
            print("❌ 组件初始化失败")
            return False

        manager.print_system_status()

    elif args.mode == "submit":
        # 任务提交模式
        if not manager.initialize_components():
            print("❌ 组件初始化失败")
            return False

        if args.task_file:
            # 批量提交任务
            print(f"📤 从文件批量提交任务: {args.task_file}")
            manager.submit_batch_tasks(args.task_file)

        elif args.site_tasks:
            # 提交站点任务
            print(f"📤 提交 {args.site_tasks} 站点任务...")
            manager.submit_site_tasks(args.site_tasks)

        elif args.url and args.site:
            # 提交单个任务
            print(f"📤 提交单个任务: {args.url}")
            success = manager.submit_custom_task(
                spider_name=args.spider,
                url=args.url,
                site=args.site,
                priority=args.priority,
            )
            if success:
                print("✅ 任务提交成功")
            else:
                print("❌ 任务提交失败")

        else:
            # 默认提交测试任务
            print("📤 提交测试任务...")
            manager.submit_test_tasks()

    elif args.mode == "cleanup":
        # 清理模式
        print("🧹 清理系统...")

        if not manager.initialize_components():
            print("❌ 组件初始化失败")
            return False

        manager.cleanup_system()
        print("✅ 清理完成")

    return True


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
