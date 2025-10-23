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
import os # 导入os模块用于获取环境变量

from openai import OpenAI # 导入OpenAI库
from zai import ZhipuAiClient # 导入ZhipuAiClient库
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
        for field in field_text:
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


class AIPipeline:
    """
    AI判断管道，用于识别文章标题是否与传染病疫情相关。
    支持DeepSeek和智谱AI大模型进行分类。
    """

    def __init__(self, settings):
        self.model_provider = settings.get("AI_MODEL_PROVIDER", "deepseek")
        self.client = None
        self.model_name = None

        if self.model_provider == "deepseek":
            self.deepseek_api_key = settings.get("DEEPSEEK_API_KEY", "<DeepSeek API Key>")
            self.deepseek_base_url = "https://api.deepseek.com"
            self.model_name = settings.get("DEEPSEEK_MODEL_NAME", "deepseek-chat")

            if not self.deepseek_api_key or self.deepseek_api_key == "<DeepSeek API Key>":
                logger.error("DeepSeek API Key 未配置或使用默认占位符，AIPipeline将跳过DeepSeek初始化。")
                return
            try:
                self.client = OpenAI(
                    api_key=self.deepseek_api_key, base_url=self.deepseek_base_url
                )
                logger.info("DeepSeek OpenAI客户端初始化成功。")
            except Exception as e:
                logger.error(f"DeepSeek OpenAI客户端初始化失败: {e}")

        elif self.model_provider == "zhipuai":
            self.zhipuai_api_key = settings.get("ZHIPUAI_API_KEY", "<ZhipuAI API Key>")
            self.model_name = settings.get("ZHIPUAI_MODEL_NAME", "glm-4.5")

            if not self.zhipuai_api_key or self.zhipuai_api_key == "<ZhipuAI API Key>":
                logger.error("ZhipuAI API Key 未配置或使用默认占位符，AIPipeline将跳过ZhipuAI初始化。")
                return
            try:
                self.client = ZhipuAiClient(api_key=self.zhipuai_api_key)
                logger.info("ZhipuAI客户端初始化成功。")
            except Exception as e:
                logger.error(f"ZhipuAI客户端初始化失败: {e}")
        else:
            logger.error(f"不支持的AI模型提供商: {self.model_provider}。AIPipeline将跳过初始化。")

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def process_item(self, item, spider):
        """
        处理数据项，通过AI判断标题相关性。
        """
        adapter = ItemAdapter(item)
        title = adapter.get("title")

        if not title:
            logger.debug("Item缺少标题，跳过AI判断。")
            return item

        if not self.client:
            logger.warning(f"{self.model_provider}客户端未初始化成功，AI判断功能将跳过。")
            return item

        try:
            system_content = (
                                """ 
你是一个专业的文章标题分类助手，专门根据精确标准判断标题是否与传染病及疫情相关主题相关。请只回答“相关”或“不相关”。

**相关标准：**  
如果标题涉及以下任何主题，则回答“相关”：  
- 疫情（包括但不限于COVID-19、流感、埃博拉等传染病的流行、防控或影响）。  
- 传染病的动向（如传播趋势、新增病例、死亡人数、变异情况）。  
- 各地疫情流行或爆发情况（包括本地、区域或全球范围的爆发事件、聚集性疫情）。  
- 官方统计表（如疾控中心、WHO、卫生部门发布的疫情数据、图表、报告）。  
- 当季流行性病毒（如季节性流感、呼吸道合胞病毒、诺如病毒等流行情况）。  
- 传染病研究新动向（包括疫苗研发、治疗方法、病原体发现、学术突破）。  
- 传染病重要会议和政策（如国际卫生条例、防控指南、公共卫生政策发布）。  
- 其他直接相关主题（如疫情预警、流行病学调查、防控措施、疫情经济影响）。  

**不相关标准：**  
如果标题明显与以上主题无关，例如涉及非传染性疾病（如癌症、糖尿病）、一般健康话题（如健身、营养）、非疫情新闻（如政治、体育、娱乐）、或其他无关领域，则回答“不相关”。  

**判断原则：**  
- 确保准确率：避免将明显不沾边的信息（如日常天气、普通科技新闻）或者举办的比赛、活动、培训、工作检查等无关信息误判为相关。  

请严格遵循以上标准进行分类。
                """
            )
            user_content = f"请判断以下标题是否相关：'{title}'"

            messages = [
                {"role": "system", "content": system_content},
                {"role": "user", "content": user_content},
            ]

            if self.model_provider == "deepseek":
                response = self.client.chat.completions.create(
                    model=self.model_name, messages=messages, stream=False, temperature=0.0
                )
                ai_response_content = response.choices[0].message.content.strip()
            elif self.model_provider == "zhipuai":
                response = self.client.chat.completions.create(
                    model=self.model_name, messages=messages, stream=False, temperature=0.0
                )
                ai_response_content = response.choices[0].message.content.strip()
            else:
                logger.error(f"不支持的AI模型提供商: {self.model_provider}。无法进行AI判断。")
                adapter["ai_relevant"] = "error"
                return item

            logger.info(f"{self.model_provider} AI对标题 '{title}' 的响应: '{ai_response_content}'")

            is_relevant = (ai_response_content == "相关")

            if is_relevant:
                logger.info(f"AI判断标题 '{title}' 与疫情相关，进行保存。")
                adapter["ai_relevant"] = True
                return item
            else:
                logger.info(f"AI判断标题 '{title}' 与疫情不相关，进行丢弃。")
                adapter["ai_relevant"] = False
                raise DropItem(f"AI判断标题不相关: {title}")
        except DropItem:
            raise
        except Exception as e:
            logger.error(f"{self.model_provider} AI判断失败或API调用错误: {e}，标题: {title}")
            adapter["ai_relevant"] = "error"
            return item


class ContentUpdatePipeline:
    """基于内容指纹的更新检测与去重（分布式，Redis原子CAS）。

    - 对 item['content'] 做规范化后计算 SHA256 指纹
    - Redis HASH: content_fp:<site>，field=sha1(url)，value=sha256(content)
    - Redis SET: content_seen:<scope>，member=sha256(content)（跨URL内容去重）
    - Lua 原子脚本返回码：
        1 => created（首次出现）
        2 => modified（内容变化）
        0 => unchanged（无变化，丢弃）
    """

    def __init__(self, redis_url: str = None):
        self.redis_url = redis_url
        self.redis = None
        self.lua = None
        # 去重配置（默认值，可被 settings 覆盖）
        self.global_dedup_enabled = True
        self.dedup_scope = "per_site"  # per_site | global
        self.dedup_ttl_seconds = 0

    @classmethod
    def from_crawler(cls, crawler):
        redis_url = crawler.settings.get("REDIS_URL")
        pipe = cls(redis_url)
        pipe._connect()
        # 读取配置项
        try:
            pipe.global_dedup_enabled = crawler.settings.getbool("CONTENT_GLOBAL_DEDUP_ENABLED", True)
        except Exception:
            pipe.global_dedup_enabled = True
        pipe.dedup_scope = crawler.settings.get("CONTENT_GLOBAL_DEDUP_SCOPE", "per_site")
        try:
            pipe.dedup_ttl_seconds = int(crawler.settings.getint("CONTENT_DEDUP_TTL_SECONDS", 0))
        except Exception:
            pipe.dedup_ttl_seconds = 0
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

        site = (item.get("site") or item.get("site_name") or getattr(spider, "target_site", None) or spider.name or "default")
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

        # 跨 URL 内容去重（基于内容指纹）。当不同 URL/标题的内容相同，仅保留首次出现
        if self.global_dedup_enabled:
            scope = "global" if self.dedup_scope == "global" else site
            set_key = f"content_seen:{scope}"
            try:
                added = self.redis.sadd(set_key, cfp)  # 1=首次，0=已存在
                if self.dedup_ttl_seconds > 0:
                    try:
                        ttl = self.redis.ttl(set_key)
                    except Exception:
                        ttl = -1
                    if ttl == -1:
                        self.redis.expire(set_key, self.dedup_ttl_seconds)
                if added == 0:
                    raise DropItem("跨URL内容重复，按指纹去重丢弃")
            except DropItem:
                raise
            except Exception as e:
                logger.warning(f"⚠️ 全局内容去重失败，降级通过: {e}")
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
            try:
                from crawler.monitoring.db_instrumentation import (
                    instrument_mongo_client,
                )

                instrument_mongo_client(self.client, db="mongodb")
            except Exception:
                pass
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

        logger.info("💾 MongoPipeline 处理数据项")

        try:
            adapter = ItemAdapter(item)
            site = adapter.get("site_name") or adapter.get("site")
            collection_name = f"{site or spider.name}_data"
            # collection_name = f"{spider.name}_data"
            collection = self.db[collection_name]

            logger.info("📊 准备存储到集合: %s", collection_name)
            logger.info("📄 数据项字段数: %s", len(adapter.asdict()))

            # 存前校验
            title = str(adapter.get("title", ""))[:30]
            clen = len(adapter.get("content", "") or "")
            logger.info("🧾 存前校验: title='%s' content_len=%s", title, clen)

            # 插入数据
            result = collection.insert_one(adapter.asdict())
            logger.info("✅ 数据已存储到MongoDB: %s", result.inserted_id)
            try:
                from crawler.monitoring.metrics import ITEM_STORED, labels_site

                ITEM_STORED.labels(**labels_site(spider.name, site)).inc()
            except Exception:
                pass

        except Exception as e:
            logger.error("❌ MongoDB存储失败: %s", e)
            import traceback

            logger.error("❌ 错误详情: %s", traceback.format_exc())
            raise DropItem(f"MongoDB存储失败: {e}")

        return item


"""
已移除 PostgresPipeline 类（系统不再支持 PostgreSQL 存储）。
"""


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
