#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
分布式工作节点

实现分布式爬虫的工作节点，负责任务执行和状态报告
"""

import json
import logging
import os
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

import psutil

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from scheduler.config_manager import ConfigManager
    from scheduler.load_balancer import LoadBalancer
    from scheduler.task_monitor import TaskMonitor
    from scheduler.task_scheduler import CrawlTask, DistributedTaskScheduler

    SCHEDULER_AVAILABLE = True
except ImportError:
    SCHEDULER_AVAILABLE = False

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class WorkerConfig:
    """工作节点配置"""

    worker_id: str
    capabilities: Dict
    max_concurrent_tasks: int = 5
    heartbeat_interval: int = 30
    task_timeout: int = 3600
    redis_url: str = "redis://localhost:6379/0"
    config_dirs: List[str] = None


class DistributedWorkerNode:
    """分布式工作节点"""

    def __init__(self, config: WorkerConfig):
        self.config = config
        self.running = False
        self.active_tasks = {}
        self.task_threads = {}

        # 初始化组件
        self.scheduler = None
        self.load_balancer = None
        self.task_monitor = None
        self.config_manager = None

        # 线程控制
        self.stop_event = threading.Event()
        self.heartbeat_thread = None
        self.task_polling_thread = None

        # 性能统计
        self.stats = {
            "tasks_completed": 0,
            "tasks_failed": 0,
            "total_runtime": 0,
            "start_time": time.time(),
        }

        # 初始化组件
        self.initialize_components()

        logger.info(f"工作节点初始化完成: {self.config.worker_id}")

    def initialize_components(self):
        """初始化组件"""
        if not SCHEDULER_AVAILABLE:
            logger.error("调度器组件不可用")
            return

        try:
            # 初始化调度器
            self.scheduler = DistributedTaskScheduler(self.config.redis_url)

            # 初始化负载均衡器
            self.load_balancer = LoadBalancer(self.config.redis_url)

            # 初始化任务监控器
            self.task_monitor = TaskMonitor(self.config.redis_url)

            # 初始化配置管理器
            if self.config.config_dirs:
                self.config_manager = ConfigManager(
                    self.config.config_dirs, self.config.redis_url
                )
                self.config_manager.start_file_monitoring()
                self.config_manager.subscribe_config_updates()

            logger.info("组件初始化成功")

        except Exception as e:
            logger.error(f"组件初始化失败: {e}")

    def start(self):
        """启动工作节点"""
        if self.running:
            logger.warning("工作节点已在运行")
            return

        logger.info(f"启动工作节点: {self.config.worker_id}")

        # 注册工作节点
        if not self.register_worker():
            logger.error("工作节点注册失败")
            return

        self.running = True

        # 启动心跳线程
        self.heartbeat_thread = threading.Thread(
            target=self.heartbeat_worker, daemon=True
        )
        self.heartbeat_thread.start()

        # 启动任务轮询线程
        self.task_polling_thread = threading.Thread(
            target=self.task_polling_worker, daemon=True
        )
        self.task_polling_thread.start()

        # 设置信号处理
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        logger.info("工作节点启动成功")

    def stop(self):
        """停止工作节点"""
        if not self.running:
            return

        logger.info("停止工作节点...")

        self.running = False
        self.stop_event.set()

        # 等待活跃任务完成
        self.wait_for_active_tasks()

        # 注销工作节点
        self.unregister_worker()

        # 停止配置管理器
        if self.config_manager:
            self.config_manager.stop()

        logger.info("工作节点已停止")

    def signal_handler(self, signum, frame):
        """信号处理器"""
        logger.info(f"收到信号 {signum}，准备停止...")
        self.stop()

    def register_worker(self) -> bool:
        """注册工作节点"""
        if not self.load_balancer:
            return False

        try:
            success = self.load_balancer.register_worker(
                self.config.worker_id, self.config.capabilities
            )

            if success:
                logger.info("工作节点注册成功")
            else:
                logger.error("工作节点注册失败")

            return success

        except Exception as e:
            logger.error(f"注册工作节点失败: {e}")
            return False

    def unregister_worker(self) -> bool:
        """注销工作节点"""
        if not self.load_balancer:
            return False

        try:
            success = self.load_balancer.unregister_worker(self.config.worker_id)

            if success:
                logger.info("工作节点注销成功")
            else:
                logger.error("工作节点注销失败")

            return success

        except Exception as e:
            logger.error(f"注销工作节点失败: {e}")
            return False

    def heartbeat_worker(self):
        """心跳工作线程"""
        logger.info("心跳线程启动")

        while self.running and not self.stop_event.is_set():
            try:
                # 收集系统统计信息
                stats = self.collect_system_stats()

                # 发送心跳
                if self.load_balancer:
                    self.load_balancer.update_worker_heartbeat(
                        self.config.worker_id, stats
                    )

                # 等待下次心跳
                self.stop_event.wait(self.config.heartbeat_interval)

            except Exception as e:
                logger.error(f"心跳发送失败: {e}")
                time.sleep(5)  # 错误时短暂等待

    def task_polling_worker(self):
        """任务轮询工作线程"""
        logger.info("任务轮询线程启动")

        while self.running and not self.stop_event.is_set():
            try:
                # 检查是否可以接受新任务
                if len(self.active_tasks) >= self.config.max_concurrent_tasks:
                    time.sleep(1)
                    continue

                # 获取下一个任务
                if self.scheduler:
                    task = self.scheduler.get_next_task(self.config.worker_id)

                    if task:
                        logger.info(f"获取到新任务: {task.task_id}")
                        self.execute_task(task)
                    else:
                        # 没有任务时等待
                        time.sleep(5)

            except Exception as e:
                logger.error(f"任务轮询失败: {e}")
                time.sleep(5)

    def execute_task(self, task: CrawlTask):
        """执行任务"""
        task_thread = threading.Thread(
            target=self.task_worker, args=(task,), daemon=True
        )

        # 记录任务
        self.active_tasks[task.task_id] = {
            "task": task,
            "start_time": time.time(),
            "thread": task_thread,
        }

        # 启动任务监控
        if self.task_monitor:
            self.task_monitor.start_task_monitoring(task.task_id, self.config.worker_id)

        # 启动任务线程
        task_thread.start()
        self.task_threads[task.task_id] = task_thread

    def task_worker(self, task: CrawlTask):
        """任务工作线程"""
        task_id = task.task_id
        start_time = time.time()

        try:
            logger.info(f"开始执行任务: {task_id}")
            # 根据任务类型选择启动命令（ AI 报告或 Scrapy 爬虫）
            task_type = (task.metadata or {}).get("task_type") if task.metadata else None

            spider_name_lc = (task.spider_name or "").lower()
            if (task_type or "").lower() == "ai_report_generation" or spider_name_lc == "ai_report_generator":
                cmd = self.build_ai_report_command(task)
                result = self.run_process(cmd, task_id)
            else:
                cmd = self.build_scrapy_command(task)
                result = self.run_scrapy_spider(cmd, task_id)

            # 计算执行时间
            duration = time.time() - start_time

            if result["success"]:
                # 任务成功
                self.handle_task_success(task_id, result, duration)
            else:
                # 任务失败
                self.handle_task_failure(task_id, result["error"], duration)

        except Exception as e:
            logger.error(f"任务执行异常 {task_id}: {e}")
            duration = time.time() - start_time
            self.handle_task_failure(task_id, str(e), duration)

        finally:
            # 清理任务记录
            self.cleanup_task(task_id)

    def build_scrapy_command(self, task: CrawlTask) -> List[str]:
        """构建Scrapy命令"""
        # 保护：报告生成任务不应走 Scrapy
        if (task.spider_name or "").lower() == "ai_report_generator":
            return self.build_ai_report_command(task)
        # bochaai_spider 不需要额外参数，使用极简命令
        if (task.spider_name or "").lower() == "bochaai_spider":
            return [
                sys.executable,
                "-m",
                "scrapy",
                "crawl",
                task.spider_name,
            ]
        cmd = [
            sys.executable,
            "-m",
            "scrapy",
            "crawl",
            task.spider_name,
            "-a",
            f"site={task.site_config.get('site', 'default')}",
            "-s",
            "LOG_LEVEL=INFO",
            # START_URLS 不再通过命令行参数传递，由 AdaptiveSpiderV2 内部处理
        ]

        # 添加其他配置参数
        for key, value in task.site_config.items():
            if key != "site":
                cmd.extend(["-s", f"{key.upper()}={value}"])

        return cmd

    def build_ai_report_command(self, task: CrawlTask) -> List[str]:
        """构建 AI 报告生成命令 (python -m reports.ai_report_generator)。"""
        site = task.site_config.get("site", "default") if task.site_config else "default"
        days = task.site_config.get("days_ago", 7) if task.site_config else 7
        no_pdf = task.site_config.get("no_pdf", False) if task.site_config else False

        cmd = [
            sys.executable,
            "-m",
            "reports.ai_report_generator",
            "--site",
            str(site),
            "--days",
            str(days),
        ]
        if no_pdf:
            cmd.append("--no-pdf")
        return cmd

    def run_scrapy_spider(self, cmd: List[str], task_id: str) -> Dict:
        """运行Scrapy爬虫"""
        try:
            logger.info(f"执行命令: {' '.join(cmd)}")

            # 设置环境变量
            env = os.environ.copy()
            env["SCRAPY_TASK_ID"] = task_id
            env["SCRAPY_WORKER_ID"] = self.config.worker_id

            # 执行命令 - 简化版本，不使用timeout
            process = subprocess.Popen(
                # cmd, stdout=sys.stdout, stderr=sys.stderr, text=True, env=env
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=env,
            )

            # 等待进程完成
            stdout, stderr = process.communicate()

            if process.returncode == 0:
                # 解析输出获取统计信息
                stats = self.parse_scrapy_output(stdout)
                return {
                    "success": True,
                    "stats": stats,
                    "stdout": stdout,
                    "stderr": stderr,
                }
            else:
                return {
                    "success": False,
                    "error": f"Scrapy退出码: {process.returncode}",
                    "stdout": stdout,
                    "stderr": stderr,
                }

        # # 获取任务超时时间，如果未配置则使用默认值
        #             timeout = self.config.task_timeout
        #             process.wait()

        #             try:
        #                 # 等待进程完成，设置超时
        #                 process.wait(timeout=timeout)
        #                 logger.info(f"Scrapy进程完成 (退出码: {process.returncode}, 任务ID: {task_id})")
        #                 if process.returncode == 0:
        #                     return {
        #                         "success": True,
        #                         "stats": {}, # 由于直接重定向输出，无法直接解析统计信息
        #                         "stdout": "日志已直接输出到终端",
        #                         "stderr": "日志已直接输出到终端",
        #                     }
        #                 else:
        #                     return {
        #                         "success": False,
        #                         "error": f"Scrapy退出码: {process.returncode}",
        #                         "stdout": "日志已直接输出到终端",
        #                         "stderr": "日志已直接输出到终端",
        #                     }
        #             except subprocess.TimeoutExpired:
        #                 logger.error(f"Scrapy进程超时，强制终止 (任务ID: {task_id})")
        #                 process.terminate() # 尝试优雅终止
        #                 process.wait(timeout=5) # 等待一段时间
        #                 if process.poll() is None:
        #                     process.kill() # 如果仍未终止，则强制杀死
        #                 return {
        #                     "success": False,
        #                     "error": f"Scrapy进程超时 ({timeout}秒)",
        #                     "stdout": "日志已直接输出到终端",
        #                     "stderr": "日志已直接输出到终端",
        #                 }

        except Exception as e:
            logger.error(f"运行Scrapy爬虫时发生异常: {e}")
            return {"success": False, "error": str(e)}

            if process.returncode == 0:
                # 解析输出获取统计信息
                stats = self.parse_scrapy_output(stdout)
                return {
                    "success": True,
                    "stats": stats,
                    "stdout": stdout,
                    "stderr": stderr,
                }
            else:
                return {
                    "success": False,
                    "error": f"Scrapy退出码: {process.returncode}",
                    "stdout": stdout,
                    "stderr": stderr,
                }

        except Exception as e:
            return {"success": False, "error": str(e)}

    def run_process(self, cmd: List[str], task_id: str) -> Dict:
        """运行通用子进程（用于 AI 报告生成）。"""
        try:
            logger.info(f"执行命令: {' '.join(cmd)}")

            env = os.environ.copy()
            env["TASK_ID"] = task_id
            env["SCRAPY_WORKER_ID"] = self.config.worker_id

            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=env,
            )
            stdout, stderr = process.communicate()
            if process.returncode == 0:
                # 尝试从输出中解析报告保存路径
                report_path = None
                try:
                    import re as _re

                    m = _re.search(r"Report successfully generated and saved to:\s*(.+)", stdout)
                    if m:
                        report_path = m.group(1).strip()
                except Exception:
                    report_path = None

                stats = {"report_path": report_path} if report_path else {}
                return {"success": True, "stats": stats, "stdout": stdout, "stderr": stderr}
            else:
                return {"success": False, "error": f"Process exit code: {process.returncode}", "stdout": stdout, "stderr": stderr}
        except Exception as e:
            logger.error(f"运行进程时发生异常: {e}")
            return {"success": False, "error": str(e)}

    def parse_scrapy_output(self, output: str) -> Dict:
        """解析Scrapy输出"""
        stats = {"items_scraped": 0, "pages_crawled": 0, "errors_count": 0}

        try:
            # 简单的输出解析
            lines = output.split("\n")
            for line in lines:
                if "item_scraped_count" in line:
                    # 提取数字
                    import re

                    match = re.search(r"item_scraped_count[:\s]+(\d+)", line)
                    if match:
                        stats["items_scraped"] = int(match.group(1))

                elif "response_received_count" in line:
                    match = re.search(r"response_received_count[:\s]+(\d+)", line)
                    if match:
                        stats["pages_crawled"] = int(match.group(1))

                elif "ERROR" in line:
                    stats["errors_count"] += 1

        except Exception as e:
            logger.error(f"解析Scrapy输出失败: {e}")

        return stats

    def handle_task_success(self, task_id: str, result: Dict, duration: float):
        """处理任务成功"""
        logger.info(f"任务成功完成: {task_id} (耗时: {duration:.2f}秒)")

        # 更新统计
        self.stats["tasks_completed"] += 1
        self.stats["total_runtime"] += duration

        # 完成任务监控
        if self.task_monitor:
            final_metrics = result.get("stats", {})
            self.task_monitor.complete_task_monitoring(task_id, final_metrics)

        # 通知调度器任务完成
        if self.scheduler:
            self.scheduler.complete_task(task_id, result)

        # 更新负载均衡器
        if self.load_balancer:
            self.load_balancer.update_worker_task_count(self.config.worker_id, -1)

    def handle_task_failure(self, task_id: str, error: str, duration: float):
        """处理任务失败"""
        logger.error(f"任务执行失败: {task_id} - {error} (耗时: {duration:.2f}秒)")

        # 更新统计
        self.stats["tasks_failed"] += 1
        self.stats["total_runtime"] += duration

        # 失败任务监控
        if self.task_monitor:
            error_info = {"error": error, "duration": duration}
            self.task_monitor.fail_task_monitoring(task_id, error_info)

        # 通知调度器任务失败
        if self.scheduler:
            self.scheduler.fail_task(task_id, error, retry=True)

        # 更新负载均衡器
        if self.load_balancer:
            self.load_balancer.update_worker_task_count(
                self.config.worker_id, -2
            )  # -2表示失败

    def cleanup_task(self, task_id: str):
        """清理任务记录"""
        if task_id in self.active_tasks:
            del self.active_tasks[task_id]

        if task_id in self.task_threads:
            del self.task_threads[task_id]

    def collect_system_stats(self) -> Dict:
        """收集系统统计信息"""
        try:
            stats = {
                "cpu_usage": psutil.cpu_percent(interval=1),
                "memory_usage": psutil.virtual_memory().percent,
                "active_tasks": len(self.active_tasks),
                "tasks_completed": self.stats["tasks_completed"],
                "tasks_failed": self.stats["tasks_failed"],
                "uptime": time.time() - self.stats["start_time"],
            }

            return stats

        except Exception as e:
            logger.error(f"收集系统统计失败: {e}")
            return {
                "cpu_usage": 0,
                "memory_usage": 0,
                "active_tasks": len(self.active_tasks),
            }

    def wait_for_active_tasks(self, timeout: int = 60):
        """等待活跃任务完成"""
        if not self.active_tasks:
            return

        logger.info(f"等待 {len(self.active_tasks)} 个活跃任务完成...")

        start_time = time.time()
        while self.active_tasks and (time.time() - start_time) < timeout:
            time.sleep(1)

        if self.active_tasks:
            logger.warning(f"超时，仍有 {len(self.active_tasks)} 个任务未完成")

    def get_status(self) -> Dict:
        """获取工作节点状态"""
        return {
            "worker_id": self.config.worker_id,
            "running": self.running,
            "active_tasks": len(self.active_tasks),
            "capabilities": self.config.capabilities,
            "stats": self.stats,
            "system_stats": self.collect_system_stats(),
        }


def main():
    """主函数"""
    print("🤖 分布式工作节点")
    print("=" * 60)

    if not SCHEDULER_AVAILABLE:
        print("❌ 调度器组件不可用")
        return False

    # 创建工作节点配置
    config = WorkerConfig(
        worker_id="test_worker_001",
        capabilities={
            "supported_sites": ["general", "nhc"],
            "features": ["basic_crawling", "javascript", "selenium", "firefox"],
            "max_concurrent_tasks": 3,
        },
        max_concurrent_tasks=3,
        heartbeat_interval=10,
        config_dirs=["config"],
    )

    # 创建工作节点
    worker = DistributedWorkerNode(config)

    try:
        # 启动工作节点
        worker.start()

        print(f"✅ 工作节点启动成功: {config.worker_id}")
        print("📊 状态信息:")
        status = worker.get_status()
        for key, value in status.items():
            if key != "system_stats":
                print(f"   {key}: {value}")

        print("\n💡 工作节点正在运行...")
        print("   按 Ctrl+C 停止")

        # 保持运行
        while worker.running:
            time.sleep(1)

    except KeyboardInterrupt:
        print("\n🛑 收到停止信号")
    finally:
        worker.stop()
        print("👋 工作节点已停止")

    return True


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
