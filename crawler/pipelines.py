# -*- coding: utf-8 -*-
"""
数据管道模块

包含各种数据处理管道：
- 数据验证管道
- 数据清洗管道
- 去重管道
- 存储管道
"""

import hashlib
import json
import logging
import re

from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem

logger = logging.getLogger(__name__)


class ValidationPipeline:
    """数据验证管道"""

    def process_item(self, item, spider):
        """验证数据项"""
        adapter = ItemAdapter(item)

        # 根据Item类型确定必需字段
        item_type = type(item).__name__

        if item_type == "NewsItem":
            required_fields = ["url", "title", "crawl_time"]
            url_field = "url"
        elif item_type == "EpidemicDataItem":
            required_fields = ["source_url", "title", "crawl_time"]
            url_field = "source_url"
        elif item_type == "PolicyItem":
            required_fields = ["url", "title", "crawl_time"]
            url_field = "url"
        elif item_type == "StatisticsItem":
            required_fields = ["source_url", "crawl_time"]
            url_field = "source_url"
        else:
            # 默认验证
            required_fields = ["crawl_time"]
            url_field = None

        # 检查必需字段
        for field in required_fields:
            if not adapter.get(field):
                raise DropItem(f"缺少必需字段: {field} (Item类型: {item_type})")

        # 验证URL格式
        if url_field:
            url_value = adapter.get(url_field)
            if url_value and not url_value.startswith(("http://", "https://")):
                raise DropItem(f"无效的URL格式: {url_value} (字段: {url_field})")

        logger.debug(
            f"数据验证通过: {item_type} - {adapter.get(url_field) if url_field else 'No URL'}"
        )
        return item


class CleaningPipeline:
    """数据清洗管道"""

    def process_item(self, item, spider):
        """清洗数据项"""
        adapter = ItemAdapter(item)

        # 清洗文本字段
        text_fields = ["title", "content", "region"]
        for field in text_fields:
            value = adapter.get(field)
            if value:
                # 去除多余空白字符
                cleaned_value = " ".join(value.split())
                adapter[field] = cleaned_value

        # 清洗数字字段
        number_fields = ["confirmed_cases", "death_cases", "recovered_cases"]
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
        key_fields = ["source_url", "region", "report_date"]
        fingerprint_data = []

        for field in key_fields:
            value = adapter.get(field, "")
            fingerprint_data.append(str(value))

        fingerprint_string = "|".join(fingerprint_data)
        return hashlib.md5(fingerprint_string.encode()).hexdigest()

class ContentUpdatePipeline:
    """基于内容指纹的更新检测与去重（分布式，Redis原子CAS）。

    - 对 item['content'] 做规范化后计算 SHA256 指纹
    - Redis HASH: content_fp:<site>，field=sha1(url)，value=sha256(content)
    - Lua 原子脚本返回码：
        1 => created（首次出现）
        2 => modified（内容变化）
        0 => unchanged（无变化，丢弃）
    """

    def __init__(self, redis_url: str = None):
        self.redis_url = redis_url
        self.redis = None
        self.lua = None

    @classmethod
    def from_crawler(cls, crawler):
        redis_url = crawler.settings.get("REDIS_URL")
        pipe = cls(redis_url)
        pipe._connect()
        return pipe

    def _connect(self):
        try:
            import redis

            self.redis = redis.from_url(self.redis_url)
            self.redis.ping()
            # 注册Lua脚本
            script = """
            local key = KEYS[1]
            local field = ARGV[1]
            local newv = ARGV[2]
            local oldv = redis.call('HGET', key, field)
            if (not oldv) then
                redis.call('HSET', key, field, newv)
                return 1
            end
            if (oldv ~= newv) then
                redis.call('HSET', key, field, newv)
                return 2
            end
            return 0
            """
            self.lua = self.redis.register_script(script)
        except Exception as e:
            logger.warning(f"⚠️ ContentUpdatePipeline 无法连接Redis，将降级运行: {e}")
            self.redis = None

    def _normalize(self, text: str) -> str:
        if not text:
            return ""
        # 简单规范化：去HTML标签残留（若已清洗则基本无影响）、压缩空白
        try:
            stripped = "".join(text.split())  # 强压缩空白
            return stripped
        except Exception:
            return text

    def process_item(self, item, spider):
        if not spider.settings.getbool("CONTENT_DEDUP_ENABLED", True):
            return item
        url = item.get("source_url") or item.get("url")
        content = item.get("content")
        if not url or content is None:
            return item

        # 计算指纹
        ufield = hashlib.sha1(url.encode("utf-8")).hexdigest()
        cfp = hashlib.sha256(self._normalize(content).encode("utf-8")).hexdigest()

        # Redis 不可用 -> 直接通过（保留 item）
        if not self.redis or not self.lua:
            item["dedup_status"] = "unknown"
            item["content_fingerprint"] = cfp
            return item

        site = getattr(spider, "target_site", None) or "default"
        key = f"content_fp:{site}"
        try:
            rc = self.lua(keys=[key], args=[ufield, cfp])
        except Exception as e:
            logger.warning(f"⚠️ Redis Lua 执行失败，降级通过: {e}")
            item["dedup_status"] = "unknown"
            item["content_fingerprint"] = cfp
            return item

        status = {0: "unchanged", 1: "created", 2: "modified"}.get(int(rc), "unknown")
        item["dedup_status"] = status
        item["content_fingerprint"] = cfp

        if status == "unchanged":
            raise DropItem("内容未变化，丢弃以节省存储")
        return item



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
        if self.db is None:
            logger.warning("❌ MongoDB数据库连接为空")
            return item

        logger.info(f"💾 MongoPipeline 处理数据项")

        try:
            adapter = ItemAdapter(item)
            collection_name = f"{spider.name}_data"
            collection = self.db[collection_name]

            logger.info(f"📊 准备存储到集合: {collection_name}")
            logger.info(f"📄 数据项字段数: {len(adapter.asdict())}")

            # 插入数据
            result = collection.insert_one(adapter.asdict())
            logger.info(f"✅ 数据已存储到MongoDB: {result.inserted_id}")

        except Exception as e:
            logger.error(f"❌ MongoDB存储失败: {e}")
            import traceback

            logger.error(f"❌ 错误详情: {traceback.format_exc()}")
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
        # 调试信息：显示所有PostgreSQL相关的设置
        logger.info("🔍 PostgresPipeline 调试信息:")
        logger.info(f"  POSTGRES_HOST (原始): {crawler.settings.get('POSTGRES_HOST')}")
        logger.info(f"  POSTGRES_PORT: {crawler.settings.get('POSTGRES_PORT')}")
        logger.info(f"  POSTGRES_DATABASE: {crawler.settings.get('POSTGRES_DATABASE')}")
        logger.info(f"  POSTGRES_USER: {crawler.settings.get('POSTGRES_USER')}")
        logger.info(
            f"  POSTGRES_PASSWORD: {'*' * len(str(crawler.settings.get('POSTGRES_PASSWORD', '')))}"
        )

        # 获取主机地址，如果是Docker服务名则转换为localhost
        postgres_host = crawler.settings.get("POSTGRES_HOST")
        original_host = postgres_host
        if postgres_host == "postgresql":
            postgres_host = "localhost"
            logger.info(f"  主机地址转换: {original_host} -> {postgres_host}")
        else:
            logger.info(f"  使用原始主机地址: {postgres_host}")

        postgres_settings = {
            "host": postgres_host,
            "port": crawler.settings.get("POSTGRES_PORT"),
            "database": crawler.settings.get("POSTGRES_DATABASE"),
            "user": crawler.settings.get("POSTGRES_USER"),
            "password": crawler.settings.get("POSTGRES_PASSWORD"),
            "client_encoding": "utf8",  # 添加编码设置
            "connect_timeout": 10,  # 添加连接超时
        }

        # 调试信息：显示最终的连接配置
        logger.info("📋 最终连接配置:")
        for key, value in postgres_settings.items():
            if key == "password":
                logger.info(f"  {key}: {'*' * len(str(value)) if value else 'None'}")
            else:
                logger.info(f"  {key}: {value}")

        return cls(postgres_settings)

    def open_spider(self, spider):
        """爬虫开始时连接PostgreSQL"""
        try:
            import psycopg2
            import psycopg2.extensions

            logger.info("🚀 开始PostgreSQL连接过程...")

            # 再次显示连接参数（确保没有被修改）
            logger.info("🔍 连接参数验证:")
            for key, value in self.postgres_settings.items():
                if key == "password":
                    logger.info(
                        f"  {key}: {'*' * len(str(value)) if value else 'None'}"
                    )
                else:
                    logger.info(f"  {key}: {value}")

            # 设置连接编码
            psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
            psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

            logger.info(
                f"🔌 尝试连接PostgreSQL: {self.postgres_settings['host']}:{self.postgres_settings['port']}"
            )
            logger.info(f"📊 数据库: {self.postgres_settings['database']}")
            logger.info(f"👤 用户: {self.postgres_settings['user']}")

            self.connection = psycopg2.connect(**self.postgres_settings)
            self.connection.set_client_encoding("UTF8")
            self.cursor = self.connection.cursor()

            # 测试连接
            self.cursor.execute("SELECT version();")
            version = self.cursor.fetchone()
            logger.info(f"✅ PostgreSQL连接成功!")
            logger.info(f"📋 版本信息: {version[0]}")

            # 测试编码
            self.cursor.execute("SHOW client_encoding;")
            encoding = self.cursor.fetchone()
            logger.info(f"🔤 客户端编码: {encoding[0]}")

            # 测试当前用户和数据库
            self.cursor.execute("SELECT current_user, current_database();")
            user_db = self.cursor.fetchone()
            logger.info(f"👤 当前用户: {user_db[0]}")
            logger.info(f"📊 当前数据库: {user_db[1]}")

        except ImportError:
            logger.error("❌ psycopg2未安装，无法使用PostgreSQL管道")
            logger.error("请运行: pip install psycopg2-binary")
        except psycopg2.OperationalError as e:
            logger.error(f"❌ PostgreSQL连接失败 (操作错误): {e}")
            logger.error("可能的原因:")
            logger.error("  1. 数据库服务未运行")
            logger.error("  2. 用户名或密码错误")
            logger.error("  3. 数据库不存在")
            logger.error("  4. 网络连接问题")
        except psycopg2.DatabaseError as e:
            logger.error(f"❌ PostgreSQL数据库错误: {e}")
        except UnicodeDecodeError as e:
            logger.error(f"❌ UTF-8解码错误: {e}")
            logger.error("这通常表示用户名或密码包含特殊字符，或PostgreSQL返回了非UTF-8编码的错误消息")
            logger.error("建议检查用户名和密码是否正确")
        except Exception as e:
            logger.error(f"❌ PostgreSQL连接失败 (未知错误): {e}")
            import traceback

            logger.error(f"详细错误信息: {traceback.format_exc()}")

    def close_spider(self, spider):
        """爬虫结束时关闭PostgreSQL连接"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("PostgreSQL连接已关闭")

    def process_item(self, item, spider):
        """存储数据到PostgreSQL"""
        if self.connection is None or self.cursor is None:
            logger.warning("PostgreSQL连接未建立，跳过存储")
            return item

        try:
            adapter = ItemAdapter(item)

            # 根据Item类型确定表名
            item_type = type(item).__name__
            table_mapping = {
                "NewsItem": "news_data",
                "EpidemicDataItem": "epidemic_data",
                "PolicyItem": "policy_data",
                "StatisticsItem": "statistics_data",
            }

            # 对于adaptive spider的通用数据，使用crawler_data表
            if item_type == "dict" or "adaptive" in adapter.get("spider_name", ""):
                table_name = "crawler_data"
            else:
                table_name = table_mapping.get(item_type, "crawler_data")

            # 数据预处理：转换数据类型
            processed_data = self._preprocess_data(adapter, item_type)

            # 调试：检查数据类型
            logger.debug(f"PostgreSQL存储调试 - 表: {table_name}")
            for key, value in processed_data.items():
                logger.debug(f"  {key}: {type(value)} = {str(value)[:100]}")

            # 构建插入SQL
            fields = list(processed_data.keys())
            values = list(processed_data.values())

            placeholders = ", ".join(["%s"] * len(fields))
            fields_str = ", ".join(fields)

            sql = f"INSERT INTO {table_name} ({fields_str}) VALUES ({placeholders})"

            # 执行插入
            logger.debug(f"执行SQL: {sql}")
            self.cursor.execute(sql, values)
            self.connection.commit()

            logger.info(
                f"✅ 数据已保存到PostgreSQL表 {table_name}: {adapter.get('title', 'No Title')[:50]}..."
            )

        except Exception as e:
            self.connection.rollback()
            logger.error(f"❌ PostgreSQL存储失败: {e}")
            logger.error(f"❌ 数据内容: {dict(adapter)}")

            # 详细错误分析
            if "can't adapt type" in str(e):
                logger.error("❌ 数据类型适配错误，检查以下字段的数据类型:")
                for key, value in adapter.items():
                    if isinstance(value, (dict, list, tuple, set)):
                        logger.error(f"   问题字段: {key} = {type(value)} -> {value}")

            raise DropItem(f"PostgreSQL存储失败: {e}")

        return item

    def _preprocess_data(self, adapter, item_type):
        """预处理数据，转换数据类型以匹配PostgreSQL表结构"""
        import datetime
        import json

        # 获取所有数据
        data = adapter.asdict()

        processed_data = {}

        for key, value in adapter.items():
            # 处理字典和列表类型 - 转换为JSON
            if isinstance(value, (dict, list)):
                try:
                    processed_data[key] = json.dumps(value, ensure_ascii=False)
                    logger.debug(f"字段 {key} 转换为JSON: {type(value)} -> str")
                except (TypeError, ValueError) as e:
                    logger.warning(f"字段 {key} JSON序列化失败: {e}")
                    processed_data[key] = str(value)

            # 处理时间戳字段
            elif key in ["crawl_timestamp"] and value is not None:
                if isinstance(value, (int, float)):
                    # 将Unix时间戳转换为datetime对象
                    processed_data[key] = datetime.datetime.fromtimestamp(
                        value, tz=datetime.timezone.utc
                    )
                else:
                    processed_data[key] = value

            # 处理crawl_time字段
            elif key in ["crawl_time"] and value is not None:
                if isinstance(value, str):
                    try:
                        # 尝试解析ISO格式的时间字符串
                        processed_data[key] = datetime.datetime.fromisoformat(
                            value.replace("Z", "+00:00")
                        )
                    except ValueError:
                        processed_data[key] = value
                else:
                    processed_data[key] = value

            # 常见数值规范化
            elif (
                key in ["status_code", "content_length", "chinese_char_count"]
                and value is not None
            ):
                try:
                    processed_data[key] = int(value)
                except Exception:
                    processed_data[key] = None

            # 处理日期字段
            elif (
                key in ["publish_date", "report_date", "issue_date", "effective_date"]
                and value is not None
            ):
                if isinstance(value, str):
                    try:
                        # 尝试解析日期字符串
                        processed_data[key] = datetime.datetime.strptime(
                            value, "%Y-%m-%d"
                        ).date()
                    except ValueError:
                        processed_data[key] = value
                else:
                    processed_data[key] = value

            # 处理数组字段（PostgreSQL的TEXT[]类型）
            elif key in ["tags", "keywords"] and value is not None:
                if isinstance(value, list):
                    # 确保列表中都是字符串
                    processed_data[key] = [
                        str(item) for item in value if item is not None
                    ]
                elif isinstance(value, str):
                    processed_data[key] = [
                        tag.strip() for tag in value.split(",") if tag.strip()
                    ]
                else:
                    processed_data[key] = []

            # 处理数值字段
            elif (
                key
                in [
                    "confirmed_cases",
                    "death_cases",
                    "recovered_cases",
                    "active_cases",
                    "new_confirmed",
                    "new_deaths",
                    "new_recovered",
                    "view_count",
                    "comment_count",
                ]
                and value is not None
            ):
                try:
                    processed_data[key] = int(value) if value != "" else 0
                except (ValueError, TypeError):
                    processed_data[key] = 0

            # 处理浮点数字段
            elif key in ["data_quality_score"] and value is not None:
                try:
                    processed_data[key] = float(value) if value != "" else 0.0
                except (ValueError, TypeError):
                    processed_data[key] = 0.0

            # 处理None值
            elif value is None:
                processed_data[key] = None

            # 其他字段确保是字符串类型
            else:
                if isinstance(value, (str, int, float, bool)):
                    processed_data[key] = value
                else:
                    # 复杂对象转换为字符串
                    processed_data[key] = str(value)
                    logger.debug(f"字段 {key} 转换为字符串: {type(value)} -> str")

        return processed_data


class JsonWriterPipeline:
    """JSON文件写入管道"""

    def open_spider(self, spider):
        """爬虫开始时打开文件"""
        self.file = open(f"data/{spider.name}_items.jl", "w", encoding="utf-8")

    def close_spider(self, spider):
        """爬虫结束时关闭文件"""
        self.file.close()

    def process_item(self, item, spider):
        """写入数据到JSON文件"""
        line = json.dumps(ItemAdapter(item).asdict(), ensure_ascii=False) + "\n"
        self.file.write(line)
        return item
