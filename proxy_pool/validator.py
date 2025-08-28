# -*- coding: utf-8 -*-
"""
代理验证器模块

验证代理的可用性、匿名性和性能
"""

import time
import logging
import requests
from typing import List, Dict, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

from .manager import ProxyInfo


logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """验证结果"""
    is_valid: bool
    response_time: float
    anonymity_level: str
    error_message: str = ""
    test_results: Dict[str, bool] = None


class ProxyValidator:
    """代理验证器"""

    def __init__(self, config: Dict = None):
        self.config = config or {}

        # 验证配置
        self.timeout = self.config.get('timeout', 10)
        self.max_retries = self.config.get('max_retries', 2)

        # 测试URL
        self.test_urls = self.config.get('test_urls', [
            'http://httpbin.org/ip',
            'http://httpbin.org/headers',
            'https://www.google.com'
        ])

        # 匿名性检查头部
        self.anonymity_headers = self.config.get('check_headers', [
            'X-Forwarded-For',
            'X-Real-IP',
            'Via',
            'Proxy-Connection'
        ])

        # 线程池
        self.max_workers = self.config.get('max_workers', 20)

    def validate_proxy(self, proxy_info: ProxyInfo) -> ValidationResult:
        """验证单个代理"""
        start_time = time.time()

        try:
            # 创建会话
            session = requests.Session()

            # 设置代理
            proxies = {
                'http': proxy_info.url,
                'https': proxy_info.url
            }

            # 设置请求头
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
            }

            # 执行测试
            test_results = {}
            successful_tests = 0

            for test_url in self.test_urls:
                try:
                    response = session.get(
                        test_url,
                        proxies=proxies,
                        headers=headers,
                        timeout=self.timeout,
                        verify=False
                    )

                    if response.status_code == 200:
                        test_results[test_url] = True
                        successful_tests += 1
                    else:
                        test_results[test_url] = False

                except Exception as e:
                    test_results[test_url] = False
                    logger.debug(f"测试失败 {test_url}: {e}")

            # 计算响应时间
            response_time = time.time() - start_time

            # 检查匿名性
            anonymity_level = self._check_anonymity(session, proxies, proxy_info.ip)

            # 判断是否有效
            success_rate = successful_tests / len(self.test_urls)
            is_valid = success_rate >= 0.5  # 至少50%的测试通过

            return ValidationResult(
                is_valid=is_valid,
                response_time=response_time,
                anonymity_level=anonymity_level,
                test_results=test_results
            )

        except Exception as e:
            response_time = time.time() - start_time
            return ValidationResult(
                is_valid=False,
                response_time=response_time,
                anonymity_level='unknown',
                error_message=str(e)
            )
        finally:
            session.close()

    def _check_anonymity(self, session: requests.Session, proxies: Dict, real_ip: str) -> str:
        """检查代理匿名性"""
        try:
            response = session.get(
                'http://httpbin.org/headers',
                proxies=proxies,
                timeout=self.timeout
            )

            if response.status_code != 200:
                return 'unknown'

            headers = response.json().get('headers', {})

            # 检查是否暴露真实IP
            for header_name in self.anonymity_headers:
                header_value = headers.get(header_name, '')
                if real_ip in header_value:
                    return 'transparent'  # 透明代理

            # 检查是否有代理相关头部
            has_proxy_headers = any(
                header_name in headers for header_name in self.anonymity_headers
            )

            if has_proxy_headers:
                return 'anonymous'  # 匿名代理
            else:
                return 'elite'  # 高匿代理

        except Exception as e:
            logger.debug(f"匿名性检查失败: {e}")
            return 'unknown'

    def validate_proxies_batch(self, proxies: List[ProxyInfo]) -> List[Tuple[ProxyInfo, ValidationResult]]:
        """批量验证代理"""
        results = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 提交验证任务
            future_to_proxy = {
                executor.submit(self.validate_proxy, proxy): proxy
                for proxy in proxies
            }

            # 收集结果
            for future in as_completed(future_to_proxy, timeout=60):
                proxy = future_to_proxy[future]
                try:
                    result = future.result()
                    results.append((proxy, result))
                except Exception as e:
                    logger.error(f"验证代理失败 {proxy.url}: {e}")
                    error_result = ValidationResult(
                        is_valid=False,
                        response_time=0,
                        anonymity_level='unknown',
                        error_message=str(e)
                    )
                    results.append((proxy, error_result))

        return results

    def get_validation_stats(self, results: List[Tuple[ProxyInfo, ValidationResult]]) -> Dict:
        """获取验证统计信息"""
        if not results:
            return {}

        total_count = len(results)
        valid_count = sum(1 for _, result in results if result.is_valid)

        # 按匿名性分类
        anonymity_stats = {}
        response_times = []

        for proxy, result in results:
            if result.is_valid:
                anonymity_level = result.anonymity_level
                anonymity_stats[anonymity_level] = anonymity_stats.get(anonymity_level, 0) + 1
                response_times.append(result.response_time)

        # 计算平均响应时间
        avg_response_time = sum(response_times) / len(response_times) if response_times else 0

        return {
            'total_proxies': total_count,
            'valid_proxies': valid_count,
            'success_rate': valid_count / total_count,
            'average_response_time': avg_response_time,
            'anonymity_distribution': anonymity_stats
        }

    def filter_valid_proxies(self, results: List[Tuple[ProxyInfo, ValidationResult]]) -> List[ProxyInfo]:
        """过滤出有效代理"""
        valid_proxies = []

        for proxy, result in results:
            if result.is_valid:
                # 更新代理信息
                proxy.speed = result.response_time
                proxy.anonymity = result.anonymity_level
                proxy.success_rate = 1.0  # 初始成功率
                proxy.last_check = time.time()
                proxy.fail_count = 0

                valid_proxies.append(proxy)

        return valid_proxies

    def score_proxy(self, proxy: ProxyInfo, result: ValidationResult) -> float:
        """为代理打分"""
        if not result.is_valid:
            return 0.0

        score = 0.0

        # 响应时间评分 (40%)
        if result.response_time < 2:
            score += 40
        elif result.response_time < 5:
            score += 30
        elif result.response_time < 10:
            score += 20
        else:
            score += 10

        # 匿名性评分 (30%)
        anonymity_scores = {
            'elite': 30,
            'anonymous': 25,
            'transparent': 15,
            'unknown': 5
        }
        score += anonymity_scores.get(result.anonymity_level, 0)

        # 成功率评分 (20%)
        if hasattr(proxy, 'success_rate'):
            score += proxy.success_rate * 20
        else:
            score += 20  # 新代理给满分

        # 稳定性评分 (10%)
        if hasattr(proxy, 'fail_count'):
            if proxy.fail_count == 0:
                score += 10
            elif proxy.fail_count < 3:
                score += 5
        else:
            score += 10  # 新代理给满分

        return min(100, max(0, score))
