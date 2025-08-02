# -*- coding: utf-8 -*-
"""
省级疾控中心爬虫

爬取各省疾控中心的疫情信息
"""

import re
import time
from typing import Dict, Any, Generator, List
from urllib.parse import urljoin

import scrapy
from scrapy.http import Request, Response

from ..base import ConfigurableSpider
from ...items import EpidemicDataItem


class ProvincialCdcSpider(ConfigurableSpider):
    """省级疾控中心爬虫"""
    
    name = 'provincial_cdc'
    redis_key = 'provincial_cdc:start_urls'
    
    # 省级疾控中心网站配置
    cdc_sites = {
        'beijing': {
            'name': '北京市疾控中心',
            'base_url': 'https://www.bjcdc.org',
            'start_urls': ['https://www.bjcdc.org/article/list/?category=1'],
            'region': '北京市'
        },
        'shanghai': {
            'name': '上海市疾控中心',
            'base_url': 'https://www.scdc.sh.cn',
            'start_urls': ['https://www.scdc.sh.cn/yqfk/'],
            'region': '上海市'
        },
        'guangdong': {
            'name': '广东省疾控中心',
            'base_url': 'https://www.cdcp.org.cn',
            'start_urls': ['https://www.cdcp.org.cn/ztlm/yqfk/'],
            'region': '广东省'
        },
        'jiangsu': {
            'name': '江苏省疾控中心',
            'base_url': 'http://www.jscdc.cn',
            'start_urls': ['http://www.jscdc.cn/zxzx/'],
            'region': '江苏省'
        },
        'zhejiang': {
            'name': '浙江省疾控中心',
            'base_url': 'https://www.cdc.zj.cn',
            'start_urls': ['https://www.cdc.zj.cn/ztlm/xgfy/'],
            'region': '浙江省'
        }
    }
    
    custom_settings = {
        'DOWNLOAD_DELAY': 1,
        'RANDOMIZE_DOWNLOAD_DELAY': 0.5,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 2,
    }
    
    def __init__(self, province=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # 指定省份
        self.target_province = province
        
        # 通用提取规则
        self.common_rules = {
            'list_selectors': [
                "//ul[@class='news-list']/li/a/@href",
                "//div[@class='list']/ul/li/a/@href",
                "//div[@class='article-list']/div/a/@href",
                "//ul[@class='article-list']/li/a/@href"
            ],
            'title_selectors': [
                "//h1/text()",
                "//div[@class='title']/text()",
                "//div[@class='article-title']/text()",
                "//title/text()"
            ],
            'content_selectors': [
                "//div[@class='content']//text()",
                "//div[@class='article-content']//text()",
                "//div[@class='main-content']//text()",
                "//div[@id='content']//text()"
            ],
            'date_selectors': [
                "//div[@class='time']/text()",
                "//span[@class='date']/text()",
                "//div[@class='publish-time']/text()",
                "//div[@class='article-info']/span/text()"
            ]
        }
    
    def start_requests(self) -> Generator[Request, None, None]:
        """生成起始请求"""
        target_sites = self.get_target_sites()
        
        for site_key, site_config in target_sites.items():
            for start_url in site_config['start_urls']:
                yield self.make_request(
                    start_url,
                    callback=self.parse_list,
                    meta={
                        'site_key': site_key,
                        'site_config': site_config,
                        'page_number': 1
                    }
                )
    
    def get_target_sites(self) -> Dict[str, Dict]:
        """获取目标网站"""
        if self.target_province:
            if self.target_province in self.cdc_sites:
                return {self.target_province: self.cdc_sites[self.target_province]}
            else:
                self.logger.warning(f"未找到省份配置: {self.target_province}")
                return {}
        else:
            return self.cdc_sites
    
    def parse_list(self, response: Response) -> Generator:
        """解析列表页"""
        self.stats['pages_crawled'] += 1
        site_config = response.meta['site_config']
        
        try:
            # 尝试多种选择器提取链接
            article_links = []
            for selector in self.common_rules['list_selectors']:
                links = response.xpath(selector).getall()
                if links:
                    article_links = links
                    break
            
            if not article_links:
                self.logger.warning(f"未找到文章链接: {response.url}")
                return
            
            # 处理每篇文章
            for link in article_links[:20]:  # 限制每页处理数量
                if not link:
                    continue
                
                full_url = urljoin(response.url, link)
                
                # 过滤无关链接
                if self.is_relevant_link(full_url, link):
                    yield self.make_request(
                        full_url,
                        callback=self.parse_detail,
                        meta={
                            'site_config': site_config,
                            'list_url': response.url
                        }
                    )
            
            # 处理翻页
            if self.should_continue_pagination(response):
                next_url = self.find_next_page(response)
                if next_url:
                    current_page = response.meta.get('page_number', 1)
                    yield self.make_request(
                        next_url,
                        callback=self.parse_list,
                        meta={
                            'site_config': site_config,
                            'page_number': current_page + 1
                        }
                    )
                    
        except Exception as e:
            self.logger.error(f"解析列表页失败 {response.url}: {e}")
            self.stats['errors'] += 1
    
    def parse_detail(self, response: Response) -> Generator:
        """解析详情页"""
        site_config = response.meta['site_config']
        
        try:
            # 提取基础信息
            title = self.extract_title_flexible(response)
            content = self.extract_content_flexible(response)
            publish_date = self.extract_date_flexible(response)
            
            # 检查内容相关性
            if not self.is_epidemic_related(title, content):
                self.logger.debug(f"内容不相关，跳过: {response.url}")
                return
            
            # 提取疫情数据
            epidemic_data = self.extract_epidemic_data_flexible(content, site_config['region'])
            
            # 创建数据项
            item = EpidemicDataItem()
            
            # 基础信息
            item['source_url'] = response.url
            item['source_name'] = site_config['name']
            item['crawl_time'] = time.time()
            item['spider_name'] = self.name
            item['crawl_timestamp'] = time.time()
            
            # 内容信息
            item['title'] = title
            item['content'] = content
            
            # 地理信息
            item['region'] = site_config['region']
            
            # 时间信息
            item['report_date'] = publish_date
            item['update_time'] = time.time()
            
            # 疫情数据
            item.update(epidemic_data)
            
            self.stats['items_scraped'] += 1
            yield item
            
        except Exception as e:
            self.logger.error(f"解析详情页失败 {response.url}: {e}")
            self.stats['errors'] += 1
    
    def extract_title_flexible(self, response: Response) -> str:
        """灵活提取标题"""
        for selector in self.common_rules['title_selectors']:
            elements = response.xpath(selector)
            if elements:
                title = elements.get().strip()
                if title and len(title) > 5:
                    # 清理标题
                    title = re.sub(r'_.*?疾控|_.*?CDC', '', title)
                    return title.strip()
        return ''
    
    def extract_content_flexible(self, response: Response) -> str:
        """灵活提取内容"""
        for selector in self.common_rules['content_selectors']:
            elements = response.xpath(selector)
            if elements:
                content_list = elements.getall()
                content = ' '.join([text.strip() for text in content_list if text.strip()])
                if content and len(content) > 50:
                    return content
        return ''
    
    def extract_date_flexible(self, response: Response) -> str:
        """灵活提取日期"""
        # 从页面元素提取
        for selector in self.common_rules['date_selectors']:
            elements = response.xpath(selector)
            if elements:
                date_text = elements.get().strip()
                date_match = re.search(r'(\d{4}[-/年]\d{1,2}[-/月]\d{1,2})', date_text)
                if date_match:
                    date_str = date_match.group(1)
                    # 标准化日期格式
                    date_str = re.sub(r'[年月]', '-', date_str)
                    date_str = re.sub(r'日', '', date_str)
                    return date_str
        
        # 从URL提取
        url_date_match = re.search(r'/(\d{4})/(\d{2})/(\d{2})/', response.url)
        if url_date_match:
            year, month, day = url_date_match.groups()
            return f"{year}-{month}-{day}"
        
        return None
    
    def extract_epidemic_data_flexible(self, content: str, region: str) -> Dict[str, Any]:
        """灵活提取疫情数据"""
        data = {
            'region': region,
            'confirmed_cases': 0,
            'death_cases': 0,
            'recovered_cases': 0,
            'active_cases': 0,
            'new_confirmed': 0,
            'new_deaths': 0,
            'new_recovered': 0
        }
        
        if not content:
            return data
        
        # 数字提取模式
        patterns = {
            'confirmed_cases': [
                r'确诊病例(\d+)例',
                r'累计确诊(\d+)例',
                r'现有确诊病例(\d+)例',
                r'确诊(\d+)例'
            ],
            'new_confirmed': [
                r'新增确诊病例(\d+)例',
                r'新增确诊(\d+)例',
                r'当日新增(\d+)例'
            ],
            'death_cases': [
                r'死亡病例(\d+)例',
                r'累计死亡(\d+)例',
                r'死亡(\d+)例'
            ],
            'new_deaths': [
                r'新增死亡病例(\d+)例',
                r'新增死亡(\d+)例'
            ],
            'recovered_cases': [
                r'治愈出院病例(\d+)例',
                r'累计治愈出院(\d+)例',
                r'治愈(\d+)例',
                r'出院(\d+)例'
            ],
            'new_recovered': [
                r'新增治愈出院病例(\d+)例',
                r'新增治愈(\d+)例',
                r'新增出院(\d+)例'
            ]
        }
        
        # 提取各类数据
        for field, field_patterns in patterns.items():
            for pattern in field_patterns:
                match = re.search(pattern, content)
                if match:
                    data[field] = int(match.group(1))
                    break
        
        # 计算现有确诊
        if data['confirmed_cases'] > 0:
            data['active_cases'] = data['confirmed_cases'] - data['death_cases'] - data['recovered_cases']
            data['active_cases'] = max(0, data['active_cases'])
        
        return data
    
    def is_relevant_link(self, full_url: str, link: str) -> bool:
        """判断链接是否相关"""
        # 排除无关链接
        exclude_patterns = [
            r'javascript:',
            r'mailto:',
            r'#',
            r'\.pdf$',
            r'\.doc$',
            r'\.xls$'
        ]
        
        for pattern in exclude_patterns:
            if re.search(pattern, link, re.IGNORECASE):
                return False
        
        # 包含相关关键词
        relevant_keywords = [
            '疫情', '防控', '病例', '确诊', '新冠', 'covid', '肺炎',
            '通报', '公告', '动态', '情况'
        ]
        
        for keyword in relevant_keywords:
            if keyword in link.lower() or keyword in full_url.lower():
                return True
        
        return True
    
    def is_epidemic_related(self, title: str, content: str) -> bool:
        """判断内容是否与疫情相关"""
        if not title and not content:
            return False
        
        text = f"{title} {content}".lower()
        
        # 疫情相关关键词
        epidemic_keywords = [
            '疫情', '新冠', 'covid', '肺炎', '病例', '确诊',
            '防控', '隔离', '核酸', '疫苗', '传播', '感染'
        ]
        
        for keyword in epidemic_keywords:
            if keyword in text:
                return True
        
        return False
    
    def find_next_page(self, response: Response) -> str:
        """查找下一页链接"""
        next_page_selectors = [
            "//a[contains(text(), '下一页')]/@href",
            "//a[contains(text(), '下页')]/@href",
            "//a[contains(text(), 'Next')]/@href",
            "//a[@class='next']/@href",
            "//a[contains(@class, 'next')]/@href"
        ]
        
        for selector in next_page_selectors:
            elements = response.xpath(selector)
            if elements:
                next_url = elements.get()
                if next_url:
                    return urljoin(response.url, next_url)
        
        return None
