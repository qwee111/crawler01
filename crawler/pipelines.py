# -*- coding: utf-8 -*-
"""
数据管道模块

包含各种数据处理管道：
- 数据验证管道
- 数据清洗管道
- 去重管道
- 存储管道
"""

import json
import logging
import hashlib
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem


logger = logging.getLogger(__name__)


class ValidationPipeline:
    """数据验证管道"""
    
    def process_item(self, item, spider):
        """验证数据项"""
        adapter = ItemAdapter(item)
        
        # 检查必需字段
        required_fields = ['source_url', 'crawl_time']
        for field in required_fields:
            if not adapter.get(field):
                raise DropItem(f"缺少必需字段: {field}")
        
        # 验证URL格式
        source_url = adapter.get('source_url')
        if not source_url.startswith(('http://', 'https://')):
            raise DropItem(f"无效的URL格式: {source_url}")
        
        logger.debug(f"数据验证通过: {adapter.get('source_url')}")
        return item


class CleaningPipeline:
    """数据清洗管道"""
    
    def process_item(self, item, spider):
        """清洗数据项"""
        adapter = ItemAdapter(item)
        
        # 清洗文本字段
        text_fields = ['title', 'content', 'region']
        for field in text_fields:
            value = adapter.get(field)
            if value:
                # 去除多余空白字符
                cleaned_value = ' '.join(value.split())
                adapter[field] = cleaned_value
        
        # 清洗数字字段
        number_fields = ['confirmed_cases', 'death_cases', 'recovered_cases']
        for field in number_fields:
            value = adapter.get(field)
            if value is not None:
                try:
                    # 确保是整数
                    adapter[field] = int(value)
                except (ValueError, TypeError):
                    adapter[field] = 0
        
        logger.debug(f"数据清洗完成: {adapter.get('source_url')}")
        return item


class DuplicatesPipeline:
    """去重管道"""
    
    def __init__(self):
        self.ids_seen = set()
    
    def process_item(self, item, spider):
        """检查重复数据"""
        adapter = ItemAdapter(item)
        
        # 生成数据指纹
        fingerprint = self.generate_fingerprint(adapter)
        
        if fingerprint in self.ids_seen:
            raise DropItem(f"重复数据: {adapter.get('source_url')}")
        else:
            self.ids_seen.add(fingerprint)
            logger.debug(f"新数据: {adapter.get('source_url')}")
            return item
    
    def generate_fingerprint(self, adapter):
        """生成数据指纹"""
        # 使用URL和关键字段生成指纹
        key_fields = ['source_url', 'region', 'report_date']
        fingerprint_data = []
        
        for field in key_fields:
            value = adapter.get(field, '')
            fingerprint_data.append(str(value))
        
        fingerprint_string = '|'.join(fingerprint_data)
        return hashlib.md5(fingerprint_string.encode()).hexdigest()


class MongoPipeline:
    """MongoDB存储管道"""
    
    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.client = None
        self.db = None
    
    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get("MONGODB_URI"),
            mongo_db=crawler.settings.get("MONGODB_DATABASE", "crawler_db"),
        )
    
    def open_spider(self, spider):
        """爬虫开始时连接MongoDB"""
        try:
            import pymongo
            self.client = pymongo.MongoClient(self.mongo_uri)
            self.db = self.client[self.mongo_db]
            logger.info("MongoDB连接成功")
        except ImportError:
            logger.error("pymongo未安装，无法使用MongoDB管道")
        except Exception as e:
            logger.error(f"MongoDB连接失败: {e}")
    
    def close_spider(self, spider):
        """爬虫结束时关闭MongoDB连接"""
        if self.client:
            self.client.close()
            logger.info("MongoDB连接已关闭")
    
    def process_item(self, item, spider):
        """存储数据到MongoDB"""
        if not self.db:
            return item
        
        try:
            adapter = ItemAdapter(item)
            collection_name = f"{spider.name}_data"
            collection = self.db[collection_name]
            
            # 插入数据
            result = collection.insert_one(adapter.asdict())
            logger.debug(f"数据已存储到MongoDB: {result.inserted_id}")
            
        except Exception as e:
            logger.error(f"MongoDB存储失败: {e}")
            raise DropItem(f"MongoDB存储失败: {e}")
        
        return item


class PostgresPipeline:
    """PostgreSQL存储管道"""
    
    def __init__(self, postgres_settings):
        self.postgres_settings = postgres_settings
        self.connection = None
        self.cursor = None
    
    @classmethod
    def from_crawler(cls, crawler):
        postgres_settings = {
            'host': crawler.settings.get("POSTGRES_HOST"),
            'port': crawler.settings.get("POSTGRES_PORT"),
            'database': crawler.settings.get("POSTGRES_DATABASE"),
            'user': crawler.settings.get("POSTGRES_USER"),
            'password': crawler.settings.get("POSTGRES_PASSWORD"),
        }
        return cls(postgres_settings)
    
    def open_spider(self, spider):
        """爬虫开始时连接PostgreSQL"""
        try:
            import psycopg2
            self.connection = psycopg2.connect(**self.postgres_settings)
            self.cursor = self.connection.cursor()
            logger.info("PostgreSQL连接成功")
        except ImportError:
            logger.error("psycopg2未安装，无法使用PostgreSQL管道")
        except Exception as e:
            logger.error(f"PostgreSQL连接失败: {e}")
    
    def close_spider(self, spider):
        """爬虫结束时关闭PostgreSQL连接"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("PostgreSQL连接已关闭")
    
    def process_item(self, item, spider):
        """存储数据到PostgreSQL"""
        if not self.connection:
            return item
        
        try:
            adapter = ItemAdapter(item)
            
            # 构建插入SQL
            fields = list(adapter.keys())
            values = list(adapter.values())
            
            placeholders = ', '.join(['%s'] * len(fields))
            fields_str = ', '.join(fields)
            
            sql = f"INSERT INTO epidemic_data ({fields_str}) VALUES ({placeholders})"
            
            # 执行插入
            self.cursor.execute(sql, values)
            self.connection.commit()
            
            logger.debug(f"数据已存储到PostgreSQL: {adapter.get('source_url')}")
            
        except Exception as e:
            self.connection.rollback()
            logger.error(f"PostgreSQL存储失败: {e}")
            raise DropItem(f"PostgreSQL存储失败: {e}")
        
        return item


class JsonWriterPipeline:
    """JSON文件写入管道"""
    
    def open_spider(self, spider):
        """爬虫开始时打开文件"""
        self.file = open(f'data/{spider.name}_items.jl', 'w', encoding='utf-8')
    
    def close_spider(self, spider):
        """爬虫结束时关闭文件"""
        self.file.close()
    
    def process_item(self, item, spider):
        """写入数据到JSON文件"""
        line = json.dumps(ItemAdapter(item).asdict(), ensure_ascii=False) + "\n"
        self.file.write(line)
        return item
