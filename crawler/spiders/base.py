# -*- coding: utf-8 -*-
"""
基础爬虫类

提供通用的爬虫功能和配置
"""

import time
import yaml
import logging
from pathlib import Path
from typing import Dict, Any, Optional, Generator
from urllib.parse import urljoin, urlparse

import scrapy
from scrapy_redis.spiders import RedisSpider
from scrapy.http import Request, Response

from ..items import EpidemicDataItem, NewsItem, PolicyItem


logger = logging.getLogger(__name__)


class BaseEpidemicSpider(RedisSpider):
    """基础疫情爬虫类"""
    
    # Redis配置
    redis_key = None  # 子类需要设置
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # 加载网站配置
        self.site_config = self.load_site_config()
        
        # 设置请求头
        self.headers = self.get_default_headers()
        
        # 统计信息
        self.stats = {
            'pages_crawled': 0,
            'items_scraped': 0,
            'errors': 0,
            'start_time': time.time()
        }
        
        logger.info(f"爬虫 {self.name} 初始化完成")
    
    def load_site_config(self) -> Dict[str, Any]:
        """加载网站配置"""
        try:
            config_path = Path(__file__).parent.parent.parent / "config" / "sites" / f"{self.name}.yaml"
            
            if config_path.exists():
                with open(config_path, 'r', encoding='utf-8') as f:
                    config = yaml.safe_load(f)
                logger.info(f"已加载配置文件: {config_path}")
                return config
            else:
                logger.warning(f"配置文件不存在: {config_path}")
                return self.get_default_config()
                
        except Exception as e:
            logger.error(f"加载配置文件失败: {e}")
            return self.get_default_config()
    
    def get_default_config(self) -> Dict[str, Any]:
        """获取默认配置"""
        return {
            'site_name': self.name,
            'base_url': '',
            'encoding': 'utf-8',
            'rate_limit': {
                'delay': 1.0,
                'random_delay': True
            },
            'fields': {},
            'pagination': {
                'max_pages': 100
            }
        }
    
    def get_default_headers(self) -> Dict[str, str]:
        """获取默认请求头"""
        return {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
    
    def make_request(self, url: str, callback=None, meta: Dict = None, **kwargs) -> Request:
        """创建请求"""
        if callback is None:
            callback = self.parse
        
        if meta is None:
            meta = {}
        
        # 添加网站配置到meta
        meta['site_config'] = self.site_config
        
        return Request(
            url=url,
            callback=callback,
            headers=self.headers,
            meta=meta,
            dont_filter=kwargs.get('dont_filter', False),
            **kwargs
        )
    
    def parse(self, response: Response) -> Generator:
        """解析响应 - 子类需要实现"""
        raise NotImplementedError("子类必须实现parse方法")
    
    def extract_data(self, response: Response) -> Dict[str, Any]:
        """提取数据 - 子类需要实现"""
        raise NotImplementedError("子类必须实现extract_data方法")
    
    def create_item(self, data: Dict[str, Any], response: Response) -> scrapy.Item:
        """创建数据项"""
        item = EpidemicDataItem()
        
        # 基础信息
        item['source_url'] = response.url
        item['source_name'] = self.site_config.get('site_name', self.name)
        item['crawl_time'] = time.time()
        item['spider_name'] = self.name
        item['crawl_timestamp'] = time.time()
        
        # 填充提取的数据
        for field, value in data.items():
            if field in item.fields:
                item[field] = value
        
        return item
    
    def get_next_page_url(self, response: Response) -> Optional[str]:
        """获取下一页URL"""
        pagination_config = self.site_config.get('pagination', {})
        next_page_xpath = pagination_config.get('next_page_xpath')
        
        if next_page_xpath:
            next_page_urls = response.xpath(next_page_xpath).getall()
            if next_page_urls:
                next_url = next_page_urls[0]
                return urljoin(response.url, next_url)
        
        return None
    
    def should_continue_pagination(self, response: Response) -> bool:
        """判断是否继续翻页"""
        pagination_config = self.site_config.get('pagination', {})
        max_pages = pagination_config.get('max_pages', 100)
        
        current_page = response.meta.get('page_number', 1)
        return current_page < max_pages
    
    def handle_error(self, failure):
        """处理错误"""
        self.stats['errors'] += 1
        logger.error(f"请求失败: {failure.request.url}, 错误: {failure.value}")
    
    def closed(self, reason):
        """爬虫关闭时的处理"""
        end_time = time.time()
        duration = end_time - self.stats['start_time']
        
        logger.info(f"爬虫 {self.name} 关闭")
        logger.info(f"运行时间: {duration:.2f}秒")
        logger.info(f"爬取页面: {self.stats['pages_crawled']}")
        logger.info(f"提取数据: {self.stats['items_scraped']}")
        logger.info(f"错误次数: {self.stats['errors']}")


class BaseNewsSpider(BaseEpidemicSpider):
    """基础新闻爬虫类"""
    
    def create_item(self, data: Dict[str, Any], response: Response) -> scrapy.Item:
        """创建新闻数据项"""
        item = NewsItem()
        
        # 基础信息
        item['url'] = response.url
        item['crawl_time'] = time.time()
        item['spider_name'] = self.name
        
        # 填充提取的数据
        for field, value in data.items():
            if field in item.fields:
                item[field] = value
        
        return item


class BasePolicySpider(BaseEpidemicSpider):
    """基础政策爬虫类"""
    
    def create_item(self, data: Dict[str, Any], response: Response) -> scrapy.Item:
        """创建政策数据项"""
        item = PolicyItem()
        
        # 基础信息
        item['url'] = response.url
        item['crawl_time'] = time.time()
        item['spider_name'] = self.name
        
        # 填充提取的数据
        for field, value in data.items():
            if field in item.fields:
                item[field] = value
        
        return item


class ConfigurableSpider(BaseEpidemicSpider):
    """可配置的爬虫类"""
    
    def parse(self, response: Response) -> Generator:
        """基于配置的通用解析方法"""
        self.stats['pages_crawled'] += 1
        
        try:
            # 提取数据
            data = self.extract_data_by_config(response)
            
            if data:
                item = self.create_item(data, response)
                self.stats['items_scraped'] += 1
                yield item
            
            # 处理翻页
            if self.should_continue_pagination(response):
                next_page_url = self.get_next_page_url(response)
                if next_page_url:
                    current_page = response.meta.get('page_number', 1)
                    yield self.make_request(
                        next_page_url,
                        meta={'page_number': current_page + 1}
                    )
            
            # 处理详情页链接
            detail_links = self.extract_detail_links(response)
            for link in detail_links:
                yield self.make_request(link, callback=self.parse_detail)
                
        except Exception as e:
            logger.error(f"解析页面失败 {response.url}: {e}")
            self.stats['errors'] += 1
    
    def extract_data_by_config(self, response: Response) -> Dict[str, Any]:
        """基于配置提取数据"""
        data = {}
        fields_config = self.site_config.get('fields', {})
        
        for field_name, field_config in fields_config.items():
            try:
                value = self.extract_field_value(response, field_config)
                if value:
                    data[field_name] = value
            except Exception as e:
                logger.warning(f"提取字段 {field_name} 失败: {e}")
                continue
        
        return data
    
    def extract_field_value(self, response: Response, field_config: Dict[str, Any]):
        """提取字段值"""
        method = field_config.get('method', 'xpath')
        selector = field_config.get('selector')
        
        if not selector:
            return None
        
        if method == 'xpath':
            elements = response.xpath(selector)
        elif method == 'css':
            elements = response.css(selector)
        else:
            logger.warning(f"不支持的提取方法: {method}")
            return None
        
        if not elements:
            return None
        
        # 处理多值
        if field_config.get('multiple', False):
            return elements.getall()
        else:
            return elements.get()
    
    def extract_detail_links(self, response: Response) -> list:
        """提取详情页链接"""
        links = []
        detail_config = self.site_config.get('detail_links', {})
        
        if detail_config:
            selector = detail_config.get('selector')
            if selector:
                link_elements = response.xpath(selector)
                for element in link_elements:
                    link = element.get()
                    if link:
                        full_url = urljoin(response.url, link)
                        links.append(full_url)
        
        return links
    
    def parse_detail(self, response: Response) -> Generator:
        """解析详情页"""
        try:
            data = self.extract_data_by_config(response)
            if data:
                item = self.create_item(data, response)
                self.stats['items_scraped'] += 1
                yield item
        except Exception as e:
            logger.error(f"解析详情页失败 {response.url}: {e}")
            self.stats['errors'] += 1
