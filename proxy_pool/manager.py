# -*- coding: utf-8 -*-
"""
代理池管理器

负责代理的获取、验证、管理和分发
"""

import json
import logging
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from typing import Dict, List, Optional, Tuple

import redis
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


@dataclass
class ProxyInfo:
    """代理信息"""

    ip: str
    port: int
    protocol: str = "http"
    username: str = ""
    password: str = ""
    country: str = ""
    anonymity: str = ""
    speed: float = 0.0
    success_rate: float = 0.0
    last_check: float = 0.0
    fail_count: int = 0
    total_requests: int = 0
    successful_requests: int = 0

    @property
    def url(self) -> str:
        """获取代理URL"""
        if self.username and self.password:
            return f"{self.protocol}://{self.username}:{self.password}@{self.ip}:{self.port}"
        return f"{self.protocol}://{self.ip}:{self.port}"

    @property
    def is_valid(self) -> bool:
        """检查代理是否有效"""
        return self.fail_count < 3 and self.success_rate > 0.3


class ProxyManager:
    """代理池管理器"""

    def __init__(self, redis_url: str = "redis://localhost:6379/0"):
        self.redis_client = redis.Redis.from_url(redis_url)
        self.proxy_pool_key = "proxy:pool"
        self.proxy_stats_key = "proxy:stats"
        self.proxy_blacklist_key = "proxy:blacklist"

        # 验证配置
        self.validation_timeout = 10
        self.validation_urls = [
            "http://httpbin.org/ip",
            "http://httpbin.org/headers",
        ]

        # 线程池
        self.executor = ThreadPoolExecutor(max_workers=20)

        # 锁
        self.lock = threading.Lock()

        # 启动后台任务
        self._start_background_tasks()

    def add_proxy(self, proxy_info: ProxyInfo) -> bool:
        """添加代理到池中"""
        try:
            # 验证代理
            if self.validate_proxy(proxy_info):
                proxy_data = json.dumps(asdict(proxy_info))
                self.redis_client.hset(self.proxy_pool_key, proxy_info.url, proxy_data)
                logger.info(f"代理已添加: {proxy_info.url}")
                return True
            else:
                logger.warning(f"代理验证失败: {proxy_info.url}")
                return False
        except Exception as e:
            logger.error(f"添加代理失败: {e}")
            return False

    def get_proxy(self, exclude_list: List[str] = None) -> Optional[ProxyInfo]:
        """获取可用代理"""
        try:
            exclude_list = exclude_list or []

            # 获取所有代理
            all_proxies = self.redis_client.hgetall(self.proxy_pool_key)

            if not all_proxies:
                logger.warning("代理池为空")
                return None

            # 过滤可用代理
            available_proxies = []
            for proxy_url, proxy_data in all_proxies.items():
                proxy_url = (
                    proxy_url.decode() if isinstance(proxy_url, bytes) else proxy_url
                )

                if proxy_url in exclude_list:
                    continue

                try:
                    proxy_info = ProxyInfo(**json.loads(proxy_data))
                    if proxy_info.is_valid:
                        available_proxies.append(proxy_info)
                except Exception as e:
                    logger.error(f"解析代理数据失败: {e}")
                    continue

            if not available_proxies:
                logger.warning("没有可用代理")
                return None

            # 按成功率排序，选择最佳代理
            available_proxies.sort(key=lambda x: x.success_rate, reverse=True)

            # 从前20%中随机选择
            top_count = max(1, len(available_proxies) // 5)
            selected_proxy = random.choice(available_proxies[:top_count])

            logger.debug(f"选择代理: {selected_proxy.url}")
            return selected_proxy

        except Exception as e:
            logger.error(f"获取代理失败: {e}")
            return None

    def validate_proxy(self, proxy_info: ProxyInfo) -> bool:
        """验证代理可用性"""
        try:
            # 创建会话
            session = requests.Session()

            # 设置重试策略
            retry_strategy = Retry(
                total=2,
                backoff_factor=1,
                status_forcelist=[429, 500, 502, 503, 504],
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            session.mount("http://", adapter)
            session.mount("https://", adapter)

            # 设置代理
            proxies = {"http": proxy_info.url, "https": proxy_info.url}

            # 测试代理
            start_time = time.time()
            success_count = 0

            for test_url in self.validation_urls:
                try:
                    response = session.get(
                        test_url,
                        proxies=proxies,
                        timeout=self.validation_timeout,
                        headers={
                            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                        },
                    )

                    if response.status_code == 200:
                        success_count += 1

                        # 检查匿名性
                        if "httpbin.org/headers" in test_url:
                            headers = response.json().get("headers", {})
                            if "X-Forwarded-For" not in headers:
                                proxy_info.anonymity = "elite"
                            elif proxy_info.ip not in headers.get(
                                "X-Forwarded-For", ""
                            ):
                                proxy_info.anonymity = "anonymous"
                            else:
                                proxy_info.anonymity = "transparent"

                except Exception as e:
                    logger.debug(f"代理测试失败 {test_url}: {e}")
                    continue

            # 计算性能指标
            end_time = time.time()
            proxy_info.speed = end_time - start_time
            proxy_info.success_rate = success_count / len(self.validation_urls)
            proxy_info.last_check = time.time()

            # 更新统计
            proxy_info.total_requests += len(self.validation_urls)
            proxy_info.successful_requests += success_count

            if success_count > 0:
                proxy_info.fail_count = 0
                return True
            else:
                proxy_info.fail_count += 1
                return False

        except Exception as e:
            logger.error(f"代理验证异常: {e}")
            proxy_info.fail_count += 1
            return False
        finally:
            session.close()

    def update_proxy_stats(
        self, proxy_url: str, success: bool, response_time: float = 0
    ):
        """更新代理统计信息"""
        try:
            proxy_data = self.redis_client.hget(self.proxy_pool_key, proxy_url)
            if not proxy_data:
                return

            proxy_info = ProxyInfo(**json.loads(proxy_data))

            # 更新统计
            proxy_info.total_requests += 1
            if success:
                proxy_info.successful_requests += 1
                proxy_info.fail_count = 0
            else:
                proxy_info.fail_count += 1

            # 重新计算成功率
            proxy_info.success_rate = (
                proxy_info.successful_requests / proxy_info.total_requests
            )

            # 更新响应时间
            if response_time > 0:
                proxy_info.speed = (proxy_info.speed + response_time) / 2

            # 检查是否需要移除
            if proxy_info.fail_count >= 5 or proxy_info.success_rate < 0.2:
                self.remove_proxy(proxy_url)
                logger.info(f"代理已移除: {proxy_url}")
            else:
                # 更新代理信息
                updated_data = json.dumps(asdict(proxy_info))
                self.redis_client.hset(self.proxy_pool_key, proxy_url, updated_data)

        except Exception as e:
            logger.error(f"更新代理统计失败: {e}")

    def remove_proxy(self, proxy_url: str):
        """移除代理"""
        try:
            self.redis_client.hdel(self.proxy_pool_key, proxy_url)
            # 添加到黑名单
            self.redis_client.sadd(self.proxy_blacklist_key, proxy_url)
            logger.info(f"代理已移除并加入黑名单: {proxy_url}")
        except Exception as e:
            logger.error(f"移除代理失败: {e}")

    def get_pool_stats(self) -> Dict:
        """获取代理池统计信息"""
        try:
            total_proxies = self.redis_client.hlen(self.proxy_pool_key)
            blacklisted = self.redis_client.scard(self.proxy_blacklist_key)

            # 统计可用代理
            available_count = 0
            all_proxies = self.redis_client.hgetall(self.proxy_pool_key)

            for proxy_data in all_proxies.values():
                try:
                    proxy_info = ProxyInfo(**json.loads(proxy_data))
                    if proxy_info.is_valid:
                        available_count += 1
                except:
                    continue

            return {
                "total_proxies": total_proxies,
                "available_proxies": available_count,
                "blacklisted_proxies": blacklisted,
                "availability_rate": available_count / max(total_proxies, 1),
            }

        except Exception as e:
            logger.error(f"获取统计信息失败: {e}")
            return {}

    def _start_background_tasks(self):
        """启动后台任务"""

        # 定期验证代理
        def validate_proxies_periodically():
            while True:
                try:
                    self._validate_all_proxies()
                    time.sleep(300)  # 5分钟验证一次
                except Exception as e:
                    logger.error(f"后台验证任务异常: {e}")
                    time.sleep(60)

        # 启动后台线程
        validation_thread = threading.Thread(
            target=validate_proxies_periodically, daemon=True
        )
        validation_thread.start()

    def _validate_all_proxies(self):
        """验证所有代理"""
        try:
            all_proxies = self.redis_client.hgetall(self.proxy_pool_key)

            if not all_proxies:
                return

            # 并发验证
            futures = []
            for proxy_url, proxy_data in all_proxies.items():
                try:
                    proxy_info = ProxyInfo(**json.loads(proxy_data))

                    # 只验证超过5分钟未检查的代理
                    if time.time() - proxy_info.last_check > 300:
                        future = self.executor.submit(
                            self._validate_and_update, proxy_info
                        )
                        futures.append(future)

                except Exception as e:
                    logger.error(f"解析代理数据失败: {e}")
                    continue

            # 等待验证完成
            for future in as_completed(futures, timeout=60):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"代理验证任务失败: {e}")

        except Exception as e:
            logger.error(f"批量验证代理失败: {e}")

    def _validate_and_update(self, proxy_info: ProxyInfo):
        """验证并更新代理"""
        try:
            if self.validate_proxy(proxy_info):
                # 更新代理信息
                updated_data = json.dumps(asdict(proxy_info))
                self.redis_client.hset(
                    self.proxy_pool_key, proxy_info.url, updated_data
                )
            else:
                # 移除无效代理
                self.remove_proxy(proxy_info.url)

        except Exception as e:
            logger.error(f"验证更新代理失败: {e}")

    def close(self):
        """关闭代理管理器"""
        try:
            self.executor.shutdown(wait=True)
            logger.info("代理管理器已关闭")
        except Exception as e:
            logger.error(f"关闭代理管理器失败: {e}")


class AdvancedProxyManager(ProxyManager):
    """高级代理管理器 - 第二阶段增强版"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # 代理质量评分
        self.proxy_scores = {}

        # 代理使用统计
        self.proxy_stats = {}

        # 地理位置信息
        self.proxy_locations = {}

        # 代理类型分类
        self.proxy_types = {"datacenter": [], "residential": [], "mobile": []}

        logger.info("高级代理管理器初始化完成")

    def add_proxy_with_metadata(self, proxy_url: str, metadata: Dict = None):
        """添加带元数据的代理"""
        proxy_info = self.add_proxy(proxy_url)

        if proxy_info and metadata:
            proxy_id = proxy_info.url

            # 保存元数据
            if "score" in metadata:
                self.proxy_scores[proxy_id] = metadata["score"]

            if "location" in metadata:
                self.proxy_locations[proxy_id] = metadata["location"]

            if "type" in metadata:
                proxy_type = metadata["type"]
                if proxy_type in self.proxy_types:
                    self.proxy_types[proxy_type].append(proxy_id)

            # 初始化统计
            self.proxy_stats[proxy_id] = {
                "requests": 0,
                "successes": 0,
                "failures": 0,
                "avg_response_time": 0,
                "last_used": None,
                "consecutive_failures": 0,
            }

        return proxy_info

    def get_best_proxy(self, criteria: Dict = None) -> Optional[ProxyInfo]:
        """根据条件获取最佳代理"""
        available_proxies = self.get_available_proxies()

        if not available_proxies:
            return None

        # 应用筛选条件
        if criteria:
            available_proxies = self._filter_proxies(available_proxies, criteria)

        if not available_proxies:
            return None

        # 根据评分排序
        scored_proxies = []
        for proxy in available_proxies:
            score = self._calculate_proxy_score(proxy)
            scored_proxies.append((proxy, score))

        # 按评分降序排序
        scored_proxies.sort(key=lambda x: x[1], reverse=True)

        best_proxy = scored_proxies[0][0]
        self._update_proxy_usage(best_proxy.url)

        return best_proxy

    def _filter_proxies(
        self, proxies: List[ProxyInfo], criteria: Dict
    ) -> List[ProxyInfo]:
        """根据条件筛选代理"""
        filtered = proxies

        # 按地理位置筛选
        if "location" in criteria:
            target_location = criteria["location"]
            filtered = [
                p
                for p in filtered
                if self.proxy_locations.get(p.url, {}).get("country") == target_location
            ]

        # 按代理类型筛选
        if "type" in criteria:
            target_type = criteria["type"]
            if target_type in self.proxy_types:
                type_proxy_ids = set(self.proxy_types[target_type])
                filtered = [p for p in filtered if p.url in type_proxy_ids]

        # 按最小评分筛选
        if "min_score" in criteria:
            min_score = criteria["min_score"]
            filtered = [
                p for p in filtered if self.proxy_scores.get(p.url, 0) >= min_score
            ]

        return filtered

    def _calculate_proxy_score(self, proxy: ProxyInfo) -> float:
        """计算代理评分"""
        proxy_id = proxy.url

        # 基础评分
        base_score = self.proxy_scores.get(proxy_id, 0.5)

        # 统计信息
        stats = self.proxy_stats.get(proxy_id, {})

        # 成功率加分
        total_requests = stats.get("requests", 0)
        if total_requests > 0:
            success_rate = stats.get("successes", 0) / total_requests
            base_score += success_rate * 0.3

        # 响应时间加分（越快越好）
        avg_response_time = stats.get("avg_response_time", 5.0)
        if avg_response_time > 0:
            time_score = max(0, (5.0 - avg_response_time) / 5.0) * 0.2
            base_score += time_score

        # 连续失败扣分
        consecutive_failures = stats.get("consecutive_failures", 0)
        failure_penalty = min(consecutive_failures * 0.1, 0.5)
        base_score -= failure_penalty

        return max(0, min(1, base_score))

    def _update_proxy_usage(self, proxy_id: str):
        """更新代理使用统计"""
        import time

        if proxy_id not in self.proxy_stats:
            self.proxy_stats[proxy_id] = {
                "requests": 0,
                "successes": 0,
                "failures": 0,
                "avg_response_time": 0,
                "last_used": None,
                "consecutive_failures": 0,
            }

        stats = self.proxy_stats[proxy_id]
        stats["requests"] += 1
        stats["last_used"] = time.time()

    def record_proxy_result(
        self, proxy_id: str, success: bool, response_time: float = None
    ):
        """记录代理使用结果"""
        if proxy_id not in self.proxy_stats:
            return

        stats = self.proxy_stats[proxy_id]

        if success:
            stats["successes"] += 1
            stats["consecutive_failures"] = 0
        else:
            stats["failures"] += 1
            stats["consecutive_failures"] += 1

        # 更新平均响应时间
        if response_time is not None:
            current_avg = stats["avg_response_time"]
            total_requests = stats["requests"]

            if total_requests > 1:
                stats["avg_response_time"] = (
                    current_avg * (total_requests - 1) + response_time
                ) / total_requests
            else:
                stats["avg_response_time"] = response_time

        # 如果连续失败过多，标记为不可用
        if stats["consecutive_failures"] >= 5:
            self.mark_proxy_failed(proxy_id)

    def get_proxy_statistics(self) -> Dict:
        """获取代理统计信息"""
        total_proxies = len(self.proxies)
        active_proxies = len([p for p in self.proxies if p.status == "active"])
        failed_proxies = len([p for p in self.proxies if p.status == "failed"])

        # 计算平均评分
        scores = list(self.proxy_scores.values())
        avg_score = sum(scores) / len(scores) if scores else 0

        # 统计代理类型分布
        type_distribution = {
            proxy_type: len(proxy_ids)
            for proxy_type, proxy_ids in self.proxy_types.items()
        }

        return {
            "total_proxies": total_proxies,
            "active_proxies": active_proxies,
            "failed_proxies": failed_proxies,
            "average_score": avg_score,
            "type_distribution": type_distribution,
            "locations": len(
                set(
                    loc.get("country", "unknown")
                    for loc in self.proxy_locations.values()
                )
            ),
        }
