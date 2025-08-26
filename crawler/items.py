# -*- coding: utf-8 -*-
"""
数据模型定义

定义爬虫抓取的数据结构
"""

import scrapy
from itemloaders.processors import TakeFirst, MapCompose, Compose
from w3lib.html import remove_tags
import re
from datetime import datetime


def clean_text(value):
    """清理文本"""
    if value:
        # 移除HTML标签
        value = remove_tags(value)
        # 移除多余空白字符
        value = re.sub(r'\s+', ' ', value).strip()
    return value


def parse_number(value):
    """解析数字"""
    if value:
        # 提取数字
        numbers = re.findall(r'\d+', str(value))
        if numbers:
            return int(''.join(numbers))
    return 0


def parse_date(value):
    """解析日期"""
    if value:
        # 尝试解析常见日期格式
        date_patterns = [
            r'(\d{4})-(\d{1,2})-(\d{1,2})',
            r'(\d{4})年(\d{1,2})月(\d{1,2})日',
            r'(\d{1,2})/(\d{1,2})/(\d{4})',
        ]

        for pattern in date_patterns:
            match = re.search(pattern, str(value))
            if match:
                try:
                    groups = match.groups()
                    if len(groups) == 3:
                        year, month, day = groups
                        # 处理年份在后面的情况
                        if len(year) == 2:
                            year, month, day = day, year, month

                        date_obj = datetime(int(year), int(month), int(day))
                        return date_obj.strftime('%Y-%m-%d')
                except ValueError:
                    continue
    return None


class EpidemicDataItem(scrapy.Item):
    """疫情数据项"""

    # 基础信息
    source_url = scrapy.Field(
        output_processor=TakeFirst()
    )
    source_name = scrapy.Field(
        input_processor=MapCompose(clean_text),
        output_processor=TakeFirst()
    )
    crawl_time = scrapy.Field(
        output_processor=TakeFirst()
    )

    # 内容信息
    title = scrapy.Field(
        input_processor=MapCompose(clean_text),
        output_processor=TakeFirst()
    )
    content = scrapy.Field(
        input_processor=MapCompose(clean_text),
        output_processor=Compose(lambda x: ' '.join(x) if x else '')
    )

    # 疫情数据
    region = scrapy.Field(
        input_processor=MapCompose(clean_text),
        output_processor=TakeFirst()
    )
    confirmed_cases = scrapy.Field(
        input_processor=MapCompose(parse_number),
        output_processor=TakeFirst()
    )
    death_cases = scrapy.Field(
        input_processor=MapCompose(parse_number),
        output_processor=TakeFirst()
    )
    recovered_cases = scrapy.Field(
        input_processor=MapCompose(parse_number),
        output_processor=TakeFirst()
    )
    active_cases = scrapy.Field(
        input_processor=MapCompose(parse_number),
        output_processor=TakeFirst()
    )

    # 时间信息
    report_date = scrapy.Field(
        input_processor=MapCompose(parse_date),
        output_processor=TakeFirst()
    )
    update_time = scrapy.Field(
        input_processor=MapCompose(clean_text),
        output_processor=TakeFirst()
    )

    # 数据质量
    data_quality_score = scrapy.Field(
        output_processor=TakeFirst()
    )
    validation_status = scrapy.Field(
        output_processor=TakeFirst()
    )

    # 元数据
    spider_name = scrapy.Field(
        output_processor=TakeFirst()
    )
    crawl_timestamp = scrapy.Field(
        output_processor=TakeFirst()
    )


class NewsItem(scrapy.Item):
    """新闻数据项"""

    # 基础信息
    url = scrapy.Field(
        output_processor=TakeFirst()
    )
    title = scrapy.Field(
        input_processor=MapCompose(clean_text),
        output_processor=TakeFirst()
    )
    content = scrapy.Field(
        input_processor=MapCompose(clean_text),
        output_processor=Compose(lambda x: ' '.join(x) if x else '')
    )

    # 发布信息
    publish_date = scrapy.Field(
        input_processor=MapCompose(parse_date),
        output_processor=TakeFirst()
    )
    author = scrapy.Field(
        input_processor=MapCompose(clean_text),
        output_processor=TakeFirst()
    )
    source = scrapy.Field(
        input_processor=MapCompose(clean_text),
        output_processor=TakeFirst()
    )

    # 分类标签
    category = scrapy.Field(
        input_processor=MapCompose(clean_text),
        output_processor=TakeFirst()
    )
    tags = scrapy.Field(
        input_processor=MapCompose(clean_text)
    )

    # 统计信息
    view_count = scrapy.Field(
        input_processor=MapCompose(parse_number),
        output_processor=TakeFirst()
    )
    comment_count = scrapy.Field(
        input_processor=MapCompose(parse_number),
        output_processor=TakeFirst()
    )

    # 元数据
    crawl_time = scrapy.Field(
        output_processor=TakeFirst()
    )
    spider_name = scrapy.Field(
        output_processor=TakeFirst()
    )

    # 多媒体扩展字段
    content_type = scrapy.Field()
    content_html = scrapy.Field()
    image_urls = scrapy.Field()
    images = scrapy.Field()
    file_urls = scrapy.Field()
    files = scrapy.Field()
    cover_image = scrapy.Field()


class PolicyItem(scrapy.Item):
    """政策文件数据项"""

    # 基础信息
    url = scrapy.Field(
        output_processor=TakeFirst()
    )
    title = scrapy.Field(
        input_processor=MapCompose(clean_text),
        output_processor=TakeFirst()
    )
    content = scrapy.Field(
        input_processor=MapCompose(clean_text),
        output_processor=Compose(lambda x: ' '.join(x) if x else '')
    )

    # 政策信息
    policy_number = scrapy.Field(
        input_processor=MapCompose(clean_text),
        output_processor=TakeFirst()
    )
    issue_date = scrapy.Field(
        input_processor=MapCompose(parse_date),
        output_processor=TakeFirst()
    )
    effective_date = scrapy.Field(
        input_processor=MapCompose(parse_date),
        output_processor=TakeFirst()
    )
    issuing_authority = scrapy.Field(
        input_processor=MapCompose(clean_text),
        output_processor=TakeFirst()
    )

    # 分类信息
    policy_type = scrapy.Field(
        input_processor=MapCompose(clean_text),
        output_processor=TakeFirst()
    )
    policy_level = scrapy.Field(
        input_processor=MapCompose(clean_text),
        output_processor=TakeFirst()
    )

    # 关键词
    keywords = scrapy.Field(
        input_processor=MapCompose(clean_text)
    )

    # 元数据
    crawl_time = scrapy.Field(
        output_processor=TakeFirst()
    )
    spider_name = scrapy.Field(
        output_processor=TakeFirst()
    )


class StatisticsItem(scrapy.Item):
    """统计数据项"""

    # 基础信息
    source_url = scrapy.Field(
        output_processor=TakeFirst()
    )
    region = scrapy.Field(
        input_processor=MapCompose(clean_text),
        output_processor=TakeFirst()
    )

    # 统计数据
    total_cases = scrapy.Field(
        input_processor=MapCompose(parse_number),
        output_processor=TakeFirst()
    )
    new_cases = scrapy.Field(
        input_processor=MapCompose(parse_number),
        output_processor=TakeFirst()
    )
    total_deaths = scrapy.Field(
        input_processor=MapCompose(parse_number),
        output_processor=TakeFirst()
    )
    new_deaths = scrapy.Field(
        input_processor=MapCompose(parse_number),
        output_processor=TakeFirst()
    )
    total_recovered = scrapy.Field(
        input_processor=MapCompose(parse_number),
        output_processor=TakeFirst()
    )
    new_recovered = scrapy.Field(
        input_processor=MapCompose(parse_number),
        output_processor=TakeFirst()
    )

    # 时间信息
    statistics_date = scrapy.Field(
        input_processor=MapCompose(parse_date),
        output_processor=TakeFirst()
    )

    # 元数据
    crawl_time = scrapy.Field(
        output_processor=TakeFirst()
    )
    spider_name = scrapy.Field(
        output_processor=TakeFirst()
    )
