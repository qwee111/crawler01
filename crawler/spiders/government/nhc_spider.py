# -*- coding: utf-8 -*-
"""
国家卫健委爬虫

爬取国家卫生健康委员会官网的疫情信息
"""

import re
import time
from typing import Dict, Any, Generator
from urllib.parse import urljoin

import scrapy
from scrapy.http import Request, Response

from ..base import BaseEpidemicSpider
from ...items import EpidemicDataItem


class NhcSpider(BaseEpidemicSpider):
    """国家卫健委爬虫"""
    
    name = 'nhc'
    allowed_domains = ['nhc.gov.cn']
    redis_key = 'nhc:start_urls'
    
    # 起始URL
    start_urls = [
        'http://www.nhc.gov.cn/xcs/yqtb/list_gzbd.shtml',  # 疫情通报
        'http://www.nhc.gov.cn/xcs/yqfkdt/list_gzbd.shtml',  # 疫情防控动态
    ]
    
    custom_settings = {
        'DOWNLOAD_DELAY': 2,
        'RANDOMIZE_DOWNLOAD_DELAY': 0.5,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
    }
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # 数据提取规则
        self.extraction_rules = {
            'list_page': {
                'title_xpath': '//ul[@class="zxxx_list"]/li/a/@title',
                'link_xpath': '//ul[@class="zxxx_list"]/li/a/@href',
                'date_xpath': '//ul[@class="zxxx_list"]/li/span/text()',
                'next_page_xpath': '//a[contains(text(), "下一页")]/@href'
            },
            'detail_page': {
                'title_xpath': '//div[@class="tit"]/text()',
                'content_xpath': '//div[@id="xw_box"]//text()',
                'publish_date_xpath': '//div[@class="source"]/span[1]/text()',
                'source_xpath': '//div[@class="source"]/span[2]/text()'
            }
        }
    
    def start_requests(self) -> Generator[Request, None, None]:
        """生成起始请求"""
        for url in self.start_urls:
            yield self.make_request(url, callback=self.parse_list)
    
    def parse_list(self, response: Response) -> Generator:
        """解析列表页"""
        self.stats['pages_crawled'] += 1
        
        try:
            # 提取文章链接
            article_links = response.xpath(self.extraction_rules['list_page']['link_xpath']).getall()
            article_titles = response.xpath(self.extraction_rules['list_page']['title_xpath']).getall()
            article_dates = response.xpath(self.extraction_rules['list_page']['date_xpath']).getall()
            
            # 处理每篇文章
            for i, link in enumerate(article_links):
                if not link:
                    continue
                
                full_url = urljoin(response.url, link)
                
                # 构建meta信息
                meta = {
                    'article_title': article_titles[i] if i < len(article_titles) else '',
                    'article_date': article_dates[i] if i < len(article_dates) else '',
                    'list_url': response.url
                }
                
                yield self.make_request(full_url, callback=self.parse_detail, meta=meta)
            
            # 处理翻页
            next_page_links = response.xpath(self.extraction_rules['list_page']['next_page_xpath']).getall()
            if next_page_links and self.should_continue_pagination(response):
                next_url = urljoin(response.url, next_page_links[0])
                current_page = response.meta.get('page_number', 1)
                
                yield self.make_request(
                    next_url,
                    callback=self.parse_list,
                    meta={'page_number': current_page + 1}
                )
                
        except Exception as e:
            self.logger.error(f"解析列表页失败 {response.url}: {e}")
            self.stats['errors'] += 1
    
    def parse_detail(self, response: Response) -> Generator:
        """解析详情页"""
        try:
            # 提取基础信息
            title = self.extract_title(response)
            content = self.extract_content(response)
            publish_date = self.extract_publish_date(response)
            
            # 提取疫情数据
            epidemic_data = self.extract_epidemic_data(content)
            
            # 创建数据项
            item = EpidemicDataItem()
            
            # 基础信息
            item['source_url'] = response.url
            item['source_name'] = '国家卫生健康委员会'
            item['crawl_time'] = time.time()
            item['spider_name'] = self.name
            item['crawl_timestamp'] = time.time()
            
            # 内容信息
            item['title'] = title
            item['content'] = content
            
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
    
    def extract_title(self, response: Response) -> str:
        """提取标题"""
        title_elements = response.xpath(self.extraction_rules['detail_page']['title_xpath'])
        if title_elements:
            return title_elements.get().strip()
        
        # 备用提取方法
        title_elements = response.xpath('//title/text()')
        if title_elements:
            title = title_elements.get().strip()
            # 移除网站名称
            title = re.sub(r'_.*?国家卫生健康委员会', '', title)
            return title.strip()
        
        return response.meta.get('article_title', '')
    
    def extract_content(self, response: Response) -> str:
        """提取正文内容"""
        content_elements = response.xpath(self.extraction_rules['detail_page']['content_xpath'])
        if content_elements:
            content_list = content_elements.getall()
            content = ' '.join([text.strip() for text in content_list if text.strip()])
            return content
        
        # 备用提取方法
        content_elements = response.xpath('//div[@class="con"]//text()')
        if content_elements:
            content_list = content_elements.getall()
            content = ' '.join([text.strip() for text in content_list if text.strip()])
            return content
        
        return ''
    
    def extract_publish_date(self, response: Response) -> str:
        """提取发布日期"""
        # 从页面提取
        date_elements = response.xpath(self.extraction_rules['detail_page']['publish_date_xpath'])
        if date_elements:
            date_text = date_elements.get().strip()
            # 提取日期
            date_match = re.search(r'(\d{4}-\d{2}-\d{2})', date_text)
            if date_match:
                return date_match.group(1)
        
        # 从meta信息提取
        article_date = response.meta.get('article_date', '')
        if article_date:
            date_match = re.search(r'(\d{4}-\d{2}-\d{2})', article_date)
            if date_match:
                return date_match.group(1)
        
        # 从URL提取
        url_date_match = re.search(r'/(\d{4})/(\d{2})/(\d{2})/', response.url)
        if url_date_match:
            year, month, day = url_date_match.groups()
            return f"{year}-{month}-{day}"
        
        return None
    
    def extract_epidemic_data(self, content: str) -> Dict[str, Any]:
        """从内容中提取疫情数据"""
        data = {
            'region': '全国',
            'confirmed_cases': 0,
            'death_cases': 0,
            'recovered_cases': 0,
            'active_cases': 0
        }
        
        if not content:
            return data
        
        # 提取确诊病例
        confirmed_patterns = [
            r'确诊病例(\d+)例',
            r'累计确诊(\d+)例',
            r'现有确诊病例(\d+)例'
        ]
        
        for pattern in confirmed_patterns:
            match = re.search(pattern, content)
            if match:
                data['confirmed_cases'] = int(match.group(1))
                break
        
        # 提取死亡病例
        death_patterns = [
            r'死亡病例(\d+)例',
            r'累计死亡(\d+)例',
            r'死亡(\d+)例'
        ]
        
        for pattern in death_patterns:
            match = re.search(pattern, content)
            if match:
                data['death_cases'] = int(match.group(1))
                break
        
        # 提取治愈病例
        recovered_patterns = [
            r'治愈出院病例(\d+)例',
            r'累计治愈出院(\d+)例',
            r'治愈(\d+)例'
        ]
        
        for pattern in recovered_patterns:
            match = re.search(pattern, content)
            if match:
                data['recovered_cases'] = int(match.group(1))
                break
        
        # 计算现有确诊
        if data['confirmed_cases'] > 0:
            data['active_cases'] = data['confirmed_cases'] - data['death_cases'] - data['recovered_cases']
            data['active_cases'] = max(0, data['active_cases'])
        
        return data
    
    def extract_data(self, response: Response) -> Dict[str, Any]:
        """实现基类的抽象方法"""
        return self.extract_epidemic_data(self.extract_content(response))
