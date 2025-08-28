# -*- coding: utf-8 -*-
"""
代理提供商模块

从各种来源获取代理
"""

import re
import json
import time
import logging
import requests
from typing import List, Dict, Any
from abc import ABC, abstractmethod

from .manager import ProxyInfo


logger = logging.getLogger(__name__)


class ProxyProvider(ABC):
    """代理提供商基类"""

    def __init__(self, name: str):
        self.name = name
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

    @abstractmethod
    def fetch_proxies(self) -> List[ProxyInfo]:
        """获取代理列表"""
        pass

    def parse_proxy_line(self, line: str) -> ProxyInfo:
        """解析代理行"""
        # 匹配 IP:PORT 格式
        match = re.match(r'(\d+\.\d+\.\d+\.\d+):(\d+)', line.strip())
        if match:
            ip, port = match.groups()
            return ProxyInfo(ip=ip, port=int(port))
        return None


class FreeProxyProvider(ProxyProvider):
    """免费代理提供商"""

    def __init__(self):
        super().__init__("FreeProxy")
        self.sources = [
            {
                'name': 'proxy-list',
                'url': 'https://www.proxy-list.download/api/v1/get?type=http',
                'format': 'text'
            },
            {
                'name': 'proxyscrape',
                'url': 'https://api.proxyscrape.com/v2/?request=get&protocol=http&timeout=10000&country=all',
                'format': 'text'
            }
        ]

    def fetch_proxies(self) -> List[ProxyInfo]:
        """获取免费代理"""
        all_proxies = []

        for source in self.sources:
            try:
                proxies = self._fetch_from_source(source)
                all_proxies.extend(proxies)
                logger.info(f"从 {source['name']} 获取到 {len(proxies)} 个代理")

                # 避免请求过于频繁
                time.sleep(1)

            except Exception as e:
                logger.error(f"从 {source['name']} 获取代理失败: {e}")
                continue

        return all_proxies

    def _fetch_from_source(self, source: Dict[str, str]) -> List[ProxyInfo]:
        """从单个源获取代理"""
        try:
            response = self.session.get(source['url'], timeout=30)
            response.raise_for_status()

            if source['format'] == 'text':
                return self._parse_text_format(response.text)
            elif source['format'] == 'json':
                return self._parse_json_format(response.json())
            else:
                logger.warning(f"不支持的格式: {source['format']}")
                return []

        except Exception as e:
            logger.error(f"请求失败 {source['url']}: {e}")
            return []

    def _parse_text_format(self, text: str) -> List[ProxyInfo]:
        """解析文本格式的代理列表"""
        proxies = []

        for line in text.strip().split('\n'):
            proxy_info = self.parse_proxy_line(line)
            if proxy_info:
                proxies.append(proxy_info)

        return proxies

    def _parse_json_format(self, data: Dict[str, Any]) -> List[ProxyInfo]:
        """解析JSON格式的代理列表"""
        proxies = []

        # 根据不同API的响应格式进行解析
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict) and 'ip' in item and 'port' in item:
                    proxy_info = ProxyInfo(
                        ip=item['ip'],
                        port=int(item['port']),
                        protocol=item.get('protocol', 'http'),
                        country=item.get('country', ''),
                        anonymity=item.get('anonymity', '')
                    )
                    proxies.append(proxy_info)

        return proxies


class PremiumProxyProvider(ProxyProvider):
    """付费代理提供商"""

    def __init__(self, api_config: Dict[str, str]):
        super().__init__("PremiumProxy")
        self.api_url = api_config.get('api_url')
        self.api_key = api_config.get('api_key')
        self.username = api_config.get('username')
        self.password = api_config.get('password')

    def fetch_proxies(self) -> List[ProxyInfo]:
        """获取付费代理"""
        try:
            # 构建请求参数
            params = {
                'api_key': self.api_key,
                'format': 'json',
                'limit': 100
            }

            response = self.session.get(self.api_url, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()
            return self._parse_premium_response(data)

        except Exception as e:
            logger.error(f"获取付费代理失败: {e}")
            return []

    def _parse_premium_response(self, data: Dict[str, Any]) -> List[ProxyInfo]:
        """解析付费代理响应"""
        proxies = []

        proxy_list = data.get('proxies', [])
        for proxy_data in proxy_list:
            proxy_info = ProxyInfo(
                ip=proxy_data['ip'],
                port=int(proxy_data['port']),
                protocol=proxy_data.get('protocol', 'http'),
                username=self.username,
                password=self.password,
                country=proxy_data.get('country', ''),
                anonymity=proxy_data.get('anonymity', 'elite')
            )
            proxies.append(proxy_info)

        return proxies


class LocalProxyProvider(ProxyProvider):
    """本地代理提供商"""

    def __init__(self, proxy_file: str):
        super().__init__("LocalProxy")
        self.proxy_file = proxy_file

    def fetch_proxies(self) -> List[ProxyInfo]:
        """从本地文件获取代理"""
        try:
            proxies = []

            with open(self.proxy_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue

                    # 支持多种格式
                    if ':' in line:
                        proxy_info = self.parse_proxy_line(line)
                        if proxy_info:
                            proxies.append(proxy_info)
                    else:
                        # JSON格式
                        try:
                            proxy_data = json.loads(line)
                            proxy_info = ProxyInfo(**proxy_data)
                            proxies.append(proxy_info)
                        except:
                            continue

            logger.info(f"从本地文件获取到 {len(proxies)} 个代理")
            return proxies

        except Exception as e:
            logger.error(f"读取本地代理文件失败: {e}")
            return []


class ProxyCollector:
    """代理收集器"""

    def __init__(self):
        self.providers = []

    def add_provider(self, provider: ProxyProvider):
        """添加代理提供商"""
        self.providers.append(provider)
        logger.info(f"已添加代理提供商: {provider.name}")

    def collect_all_proxies(self) -> List[ProxyInfo]:
        """收集所有代理"""
        all_proxies = []

        for provider in self.providers:
            try:
                proxies = provider.fetch_proxies()
                all_proxies.extend(proxies)
                logger.info(f"从 {provider.name} 收集到 {len(proxies)} 个代理")

                # 避免请求过于频繁
                time.sleep(2)

            except Exception as e:
                logger.error(f"从 {provider.name} 收集代理失败: {e}")
                continue

        # 去重
        unique_proxies = self._deduplicate_proxies(all_proxies)
        logger.info(f"去重后共有 {len(unique_proxies)} 个代理")

        return unique_proxies

    def _deduplicate_proxies(self, proxies: List[ProxyInfo]) -> List[ProxyInfo]:
        """代理去重"""
        seen = set()
        unique_proxies = []

        for proxy in proxies:
            proxy_key = f"{proxy.ip}:{proxy.port}"
            if proxy_key not in seen:
                seen.add(proxy_key)
                unique_proxies.append(proxy)

        return unique_proxies


def create_proxy_collector(config: Dict[str, Any]) -> ProxyCollector:
    """创建代理收集器"""
    collector = ProxyCollector()

    # 添加免费代理提供商
    if config.get('free_proxies', {}).get('enabled', True):
        collector.add_provider(FreeProxyProvider())

    # 添加付费代理提供商
    premium_config = config.get('premium_proxies', {})
    if premium_config.get('enabled', False):
        for provider_config in premium_config.get('providers', []):
            collector.add_provider(PremiumProxyProvider(provider_config))

    # 添加本地代理提供商
    local_file = config.get('local_proxy_file')
    if local_file:
        collector.add_provider(LocalProxyProvider(local_file))

    return collector
