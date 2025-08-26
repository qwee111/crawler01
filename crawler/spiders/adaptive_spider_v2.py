"""
重构后的自适应爬虫

使用模块化架构，代码简洁清晰
"""

import asyncio
import hashlib
import json
import logging
import time
from typing import Dict, Optional
from urllib.parse import urljoin

import scrapy
from scrapy_redis import connection
from scrapy_redis.spiders import RedisSpider

# 导入核心模块
from crawler.core import ConfigManager, ExtractionEngine, PageAnalyzer, SiteDetector

logger = logging.getLogger(__name__)


class AdaptiveSpiderV2(RedisSpider):
    """重构后的自适应爬虫 (RedisSpider 版本，支持 Redis 动态种子)"""

    name = "adaptive_v2"
    # 默认的全局队列键；若指定 -a site=xxx，将在 __init__ 中切换为按站点分桶的键
    redis_key = "adaptive_v2:start_urls"
    # 关闭页面类型自动识别，优先使用 Request.meta['page_type']（默认开启，可按需改为 False）
    disable_page_detection = True

    def __init__(
        self,
        target_site: str = None,
        site: str = None,
        redis_key: str = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        # Redis 队列键动态设置：优先使用传入的 redis_key；否则按站点分桶
        if redis_key:
            self.redis_key = redis_key
        elif site:
            self.redis_key = f"adaptive_v2:{site}:start_urls"

        # 初始化核心组件
        self.config_manager = ConfigManager()
        self.site_detector = SiteDetector(self.config_manager)
        self.page_analyzer = PageAnalyzer(self.config_manager)
        self.extraction_engine = ExtractionEngine(self.config_manager)

        # 兼容命令行参数：支持 -a target_site=xxx 和 -a site=xxx
        passed_site = kwargs.pop("site", None)
        passed_target_site = kwargs.pop("target_site", None)
        self.target_site = target_site or site or passed_site or passed_target_site
        self.site_config = None

        if self.target_site:
            self._load_site_config()
        else:
            logger.warning("⚠️ 未提供目标网站参数。请使用 -a target_site=<site> 或 -a site=<site>")
            logger.info(f"💡 可用的网站配置: {self.config_manager.list_sites()}")

        logger.info(f"🚀 自适应爬虫V2启动: 目标网站={self.target_site}")

    def _load_site_config(self):
        """加载网站配置"""
        self.site_config = self.config_manager.get_config_by_site(self.target_site)

        if not self.site_config:
            logger.error(f"❌ 未找到网站配置: {self.target_site}")
            logger.info(f"💡 可用的网站配置: {self.config_manager.list_sites()}")
            return

        logger.info(f"✅ 找到网站配置: {self.target_site}")
        logger.info(f"📊 配置部分: {list(self.site_config.keys())}")

        # 设置起始URL
        start_urls_config = self.site_config.get("start_urls", [])
        if start_urls_config:
            self.start_urls = [
                item["url"] for item in start_urls_config if "url" in item
            ]
            logger.info(f"📋 加载起始URL: {len(self.start_urls)} 个")
            for i, url in enumerate(self.start_urls, 1):
                logger.info(f"   {i}. {url}")
        else:
            logger.warning(f"⚠️ 配置文件中没有start_urls部分")
            self.start_urls = []

        # 设置请求配置
        request_config = self.site_config.get("request", {})
        if request_config:
            self._apply_request_config(request_config)

        logger.info(f"✅ 网站配置加载完成: {self.target_site}")

    def _apply_request_config(self, request_config: Dict):
        """应用请求配置"""
        # 设置请求头
        headers = request_config.get("headers", {})
        if headers:
            self.custom_settings = self.custom_settings or {}
            self.custom_settings["DEFAULT_REQUEST_HEADERS"] = headers

        # 设置延迟
        delays = request_config.get("delays", {})
        if delays:
            download_delay = delays.get("download_delay", 2.0)
            self.custom_settings = self.custom_settings or {}
            self.custom_settings["DOWNLOAD_DELAY"] = download_delay
            if delays.get("randomize_delay", True):
                self.custom_settings["RANDOMIZE_DOWNLOAD_DELAY"] = True

    def _detect_direct_file(self, response) -> Optional[str]:
        """
        检测是否为可直接下载的文件（非仅限PDF）。
        返回文件扩展名（不含点，如 'pdf','docx','xls','zip'），否则返回 None。
        判定依据：
        - URL 扩展名命中白名单
        - 或 Content-Type 命中常见文件类型
        """
        try:
            url = (response.url or "").lower()
            ctype_bytes = (response.headers.get(b"Content-Type") or b"")
            ctype = ctype_bytes.decode("utf-8", errors="ignore").lower()
        except Exception:
            url, ctype = response.url.lower(), ""

        # 1) 基于URL扩展名
        try:
            from urllib.parse import urlparse
            import os as _os
            path = urlparse(url).path
            _, ext = _os.path.splitext(path)
            ext = (ext or "").lower().lstrip(".")
        except Exception:
            ext = ""

        known_exts = {
            "pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx",
            "csv", "txt", "zip", "rar", "7z", "gz", "tar", "xml", "json"
        }
        if ext in known_exts:
            return ext

        # 2) 基于 Content-Type
        ctype_map = {
            "application/pdf": "pdf",
            "application/msword": "doc",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
            "application/vnd.ms-excel": "xls",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx",
            "application/vnd.ms-powerpoint": "ppt",
            "application/vnd.openxmlformats-officedocument.presentationml.presentation": "pptx",
            "text/csv": "csv",
            "text/plain": "txt",
            "application/zip": "zip",
            "application/x-rar-compressed": "rar",
            "application/x-7z-compressed": "7z",
            "application/gzip": "gz",
            "application/x-tar": "tar",
            "application/xml": "xml",
            "text/xml": "xml",
            "application/json": "json",
        }
        for ct, mapped_ext in ctype_map.items():
            if ct in ctype:
                return mapped_ext

        # 某些服务使用通用的 octet-stream 作为附件
        if "application/octet-stream" in ctype:
            # 尝试再从URL猜测
            return ext or None

        return None

    def make_request_from_data(self, data: bytes):
        """从 Redis 的种子数据创建 Request，兼容 JSON 或 纯字符串 URL"""
        text = data.decode("utf-8").strip()
        try:
            payload = json.loads(text)
            url = payload.get("url") or payload.get("u")
            if not url:
                raise ValueError("seed json missing url")
            meta = payload.get("meta", {}) or {}
            headers = payload.get("headers")
            cb_name = payload.get("callback")
            cb_fn = getattr(self, cb_name, None) if cb_name else self.parse
            req = scrapy.Request(
                url, callback=cb_fn, headers=headers, meta=meta, dont_filter=False
            )
            # 透传 site
            if self.target_site and "site" not in req.meta:
                req.meta["site"] = self.target_site
            elif "site" in payload:
                req.meta.setdefault("site", payload["site"])
            return req
        except Exception:
            # 兼容纯字符串 URL
            req = scrapy.Request(text, callback=self.parse, dont_filter=False)
            if self.target_site and "site" not in req.meta:
                req.meta["site"] = self.target_site
            return req

    async def start(self):
        """生成起始请求（Scrapy 2.13+）：
        1) 先发本地配置/站点配置的起始URL（列表页，强制刷新）
        2) 启动基于Redis的列表周期刷新（ZSET）
        3) 监听Redis队列消费动态种子（兼容RedisSpider）
        """
        # 初始化Redis连接（用于刷新/增量识别）
        try:
            self.server = connection.get_redis_from_settings(self.crawler.settings)
        except Exception as e:
            self.server = None
            logger.warning(f"⚠️ 无法连接Redis，将以降级模式运行: {e}")

        # 1) 先发本地配置的起始URL（作为列表页强制刷新请求）
        start_urls = list(getattr(self, "start_urls", []) or [])
        if not start_urls and self.site_config and "start_urls" in self.site_config:
            start_urls = [
                u.get("url")
                for u in self.site_config.get("start_urls", [])
                if u.get("url")
            ]
        for url in start_urls:
            logger.info(f"📋 列表页初始刷新: {url}")
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                dont_filter=True,
                meta={"page_type": "list_page", "site_name": self.target_site},
                errback=self.handle_error,
            )

        # 2) 列表页周期刷新 - 仅在可用时启用
        if self.server and self.settings.getbool("LIST_REFRESH_ENABLED", True):
            site = self.target_site or "default"
            refresh_key = f"refresh_queue:{site}"
            # 将起始URL登记进刷新队列（下次刷新时间 = 现在 + interval）
            interval = int(self.settings.getint("LIST_REFRESH_INTERVAL", 900))
            for url in start_urls:
                mid = hashlib.sha1(url.encode("utf-8")).hexdigest()
                try:
                    self.server.set(f"list_url:{mid}", url)
                    self.server.zadd(refresh_key, {mid: time.time() + interval})
                except Exception as e:
                    logger.warning(f"⚠️ 列表刷新登记失败: {e}")

            async def refresh_loop():
                logger.info("🔁 启动列表页刷新协程")
                while True:
                    try:
                        popped = self._pop_due_refresh(refresh_key)
                        if not popped:
                            await asyncio.sleep(5)
                            continue
                        member, _ = popped
                        url = (
                            self.server.get(f"list_url:{member}")
                            if self.server
                            else None
                        )
                        if url:
                            if isinstance(url, bytes):
                                url = url.decode("utf-8", errors="ignore")
                            logger.info(f"⏰ 周期刷新列表: {url}")
                            yield scrapy.Request(
                                url=url,
                                callback=self.parse,
                                dont_filter=True,
                                meta={
                                    "page_type": "list_page",
                                    "site_name": self.target_site,
                                },
                                errback=self.handle_error,
                            )
                            # 重新安排下一次刷新
                            try:
                                self.server.zadd(
                                    refresh_key, {member: time.time() + interval}
                                )
                            except Exception as e:
                                logger.warning(f"⚠️ 刷新队列重入失败: {e}")
                        else:
                            logger.debug(f"🔎 未找到列表URL映射: list_url:{member}")
                    except Exception as e:
                        logger.warning(f"⚠️ 列表刷新循环异常: {e}")
                        await asyncio.sleep(5)

            # 将刷新生成器并入 Scrapy 的异步 start 流
            async for req in refresh_loop():
                yield req

        # 3) 监听 Redis 队列（继承自 RedisSpider / Spider 的 start 实现）
        try:
            async for req in super().start():
                yield req
        except Exception as e:
            logger.warning(f"⚠️ Redis 队列不可用或未配置: {e}")

    def parse(self, response):
        """解析页面的主入口（支持列表页增量与详情页内容指纹）"""
        try:
            logger.info(f"✅ 开始解析页面: {response.url}")

            # 检测网站
            site_name = self._detect_site(response)
            if not site_name:
                logger.warning(f"⚠️ 无法识别网站: {response.url}")
                return

            # 页面类型：当禁用自动检测时，优先使用 meta 提供的 page_type
            if getattr(self, "disable_page_detection", False):
                page_type = response.meta.get("page_type") or "unknown_page"
                page_analysis = {"page_type": page_type, "site_name": site_name}
                logger.info(f"🔍 页面类型(禁用自动检测): {page_type}")
            else:
                # 分析页面
                page_analysis = self.page_analyzer.analyze_page(response, site_name)
                page_type = page_analysis.get("page_type")
                logger.info(f"🔍 页面类型: {page_type}")

            # 提取数据
            extracted = self.extraction_engine.extract_data(
                response, site_name, page_analysis
            )


            # 若为直链可下载文件（不限于PDF），直接产出并返回
            try:
                direct_ext = self._detect_direct_file(response)
                if direct_ext:
                    title = response.meta.get("list_title") or response.url.split("/")[-1]
                    publish_date = response.meta.get("list_date")
                    yield {
                        "url": response.url,
                        "title": title,
                        "publish_date": publish_date,
                        "file_urls": [response.url],
                        "content_type": direct_ext,
                        "spider_name": self.name,
                        "site_name": site_name,
                    }
                    return
            except Exception:
                pass

            # 列表页：只做增量识别与派发
            if page_type == "list_page":
                items = (
                    extracted.get("items", []) if isinstance(extracted, dict) else []
                )
                logger.info(f"🧮 列表项数量: {len(items)}")
                if items:
                    yield from self._handle_list_incremental(response, site_name, items)
                    return

                # 列表页为空时，尝试使用配置的列表API获取数据
                api_cfg = (
                    (self.site_config.get("extraction", {}) or {})
                    .get("list_page", {})
                    .get("api")
                )
                if api_cfg:
                    try:
                        from urllib.parse import urlencode, urljoin
                        base_url = api_cfg.get("url") or ""
                        api_url = urljoin(response.url, base_url)
                        params = api_cfg.get("params") or {}
                        if params:
                            api_url = f"{api_url}{'&' if '?' in api_url else '?'}{urlencode(params)}"
                        headers = api_cfg.get("headers") or {}
                        logger.info(f"🧪 通过API获取列表: {api_url}")
                        yield scrapy.Request(
                            url=api_url,
                            callback=self.parse_list_api,
                            headers=headers,
                            meta={
                                "site_name": site_name,
                                "page_type": "list_api",
                                "api_config": api_cfg,
                                "origin_url": response.url,
                            },
                            dont_filter=True,
                        )
                        return
                    except Exception as e:
                        logger.warning(f"⚠️ 触发列表API失败: {e}")

                # 无API配置则结束
                yield from self._handle_list_incremental(response, site_name, items)
                return


            # 详情页：优先检测直链可下载文件（命中则直接产出）
            try:
                direct_ext = self._detect_direct_file(response)
                if direct_ext:
                    title = response.meta.get("list_title") or response.url.split("/")[-1]
                    publish_date = response.meta.get("list_date")
                    yield {
                        "url": response.url,
                        "title": title,
                        "publish_date": publish_date,
                        "file_urls": [response.url],
                        "content_type": direct_ext,
                        "spider_name": self.name,
                        "site_name": site_name,
                    }
                    return
            except Exception:
                pass

            # 详情页：输出数据项，由 ContentUpdatePipeline 负责“内容指纹去重”
            if isinstance(extracted, dict):
                # 附带原始HTML以增强后续处理可靠性（仅限文本响应）
                try:
                    from scrapy.http import TextResponse

                    raw_html = (
                        response.text if isinstance(response, TextResponse) else None
                    )
                except Exception:
                    raw_html = None

                # 若详情页未提到标题或标题异常，优先回退列表标题
                try:
                    if not extracted.get("title") or str(extracted.get("title")).strip() == "":
                        lt = response.meta.get("list_title")
                        if lt:
                            extracted["title"] = lt
                except Exception:
                    pass

                # 透传列表页日期作为回退发布时间
                if not extracted.get("publish_date") and response.meta.get("list_date"):
                    extracted["publish_date"] = response.meta.get("list_date")

                extracted.update(
                    {
                        "spider_name": self.name,
                        "spider_version": "2.0",
                        "site_name": site_name,
                        "page_analysis": page_analysis,
                        "response_meta": {
                            "status_code": response.status,
                            "content_type": response.headers.get(
                                "Content-Type", b""
                            ).decode("utf-8", errors="ignore"),
                            "content_length": len(response.body),
                            "url": response.url,
                        },
                    }
                )
                if raw_html and (not extracted.get("content")):
                    extracted["raw_html"] = raw_html
                yield extracted

            logger.info(f"✅ 页面解析完成: {response.url}")

        except Exception as e:
            logger.error(f"❌ 页面解析失败: {response.url}, 错误: {e}")
            yield {
                "url": response.url,
                "error": str(e),
                "spider_name": self.name,
                "status": "parse_failed",
            }

    def _detect_site(self, response) -> Optional[str]:
        """检测网站"""
        # 优先使用配置的网站名
        if self.target_site:
            return self.target_site

        # 自动检测
        return self.site_detector.detect_site(response.url)

    def _schedule_next_refresh(self, list_url: str, interval: int):
        if not self.server:
            return
        try:
            site = self.target_site or "default"
            refresh_key = f"refresh_queue:{site}"
            mid = hashlib.sha1(list_url.encode("utf-8")).hexdigest()
            self.server.set(f"list_url:{mid}", list_url)
            self.server.zadd(refresh_key, {mid: time.time() + interval})
        except Exception:
            logger.warning("⚠️ 列表刷新登记失败（异常已忽略）")

    def _pop_due_refresh(self, refresh_key: str):
        """弹出到期的刷新成员。
        优先用 ZPOPMIN；不支持时回退：ZRANGE 最小 + ZREM 原子性保证靠返回值。
        返回 (member:str, score:float) 或 None
        """
        if not self.server:
            return None
        now = time.time()
        try:
            # 优先使用ZPOPMIN
            res = self.server.zpopmin(refresh_key)
            if not res:
                return None
            member, score = res[0]
            if isinstance(member, bytes):
                member = member.decode("utf-8", errors="ignore")
            try:
                score = float(score)
            except Exception:
                score = now
            if score > now:
                # 未到期，放回
                try:
                    self.server.zadd(refresh_key, {member: score})
                except Exception:
                    pass
                return None
            return member, score
        except Exception:
            # 回退方案：ZRANGE + ZREM（以ZREM返回1保证只有一方成功）
            try:
                res = self.server.zrange(refresh_key, 0, 0, withscores=True)
                if not res:
                    return None
                member, score = res[0]
                if isinstance(member, bytes):
                    member = member.decode("utf-8", errors="ignore")
                if float(score) > now:
                    return None
                # 仅当ZREM成功（返回1）时视为抢到
                if self.server.zrem(refresh_key, member) == 1:
                    return member, float(score)
                return None
            except Exception:
                return None

    def _handle_list_incremental(self, response, site_name: str, items: list):
        """增量识别列表中的文章链接并发起请求"""
        interval = int(
            (self.site_config.get("update_detection", {}) or {}).get(
                "list_refresh_interval",
                self.settings.getint("LIST_REFRESH_INTERVAL", 900),
            )
        )
        self._schedule_next_refresh(response.url, interval)

        to_follow = []
        for it in items:
            if not isinstance(it, dict):
                continue
            url = it.get("url")
            if not url:
                continue
            absolute_url = urljoin(response.url, url)
            to_follow.append({
                "url": absolute_url,
                "list_title": it.get("title"),
                "list_date": it.get("date") or it.get("publish_date"),
            })

        if not to_follow:
            return

        # Redis 增量：只抓新链接
        seen_key = f"seen_articles:{site_name or 'default'}"
        for entry in to_follow:
            link = entry["url"]
            meta = {"site_name": site_name, "page_type": "detail_page"}
            if entry.get("list_title"):
                meta["list_title"] = entry["list_title"]
            if entry.get("list_date"):
                meta["list_date"] = entry["list_date"]

            if not self.server:
                # 降级：不使用增量过滤
                yield scrapy.Request(
                    url=link,
                    callback=self.parse,
                    meta=meta,
                    errback=self.handle_error,
                )
                continue
            try:
                uhash = hashlib.sha1(link.encode("utf-8")).hexdigest()
                if not self.server.sismember(seen_key, uhash):
                    self.server.sadd(seen_key, uhash)
                    yield scrapy.Request(
                        url=link,
                        callback=self.parse,
                        meta=meta,
                        errback=self.handle_error,
                    )
            except Exception as e:
                logger.warning(f"⚠️ Redis 增量识别失败，降级直抓: {e}")
                yield scrapy.Request(
                    url=link,
                    callback=self.parse,
                    meta=meta,
                    errback=self.handle_error,
                )

        return self.site_detector.detect_site(response.url)


    def parse_list_api(self, response):
        """解析列表API的响应，将其转换成 items 结构。
        支持三种形式：
        1) JSON + 列表数组（json_path 指到数组，field_mappings 指定字段名）
        2) JSON + HTML字符串（json_html_field 指到 HTML 字段，html_item_selector 提取 li）
        3) 纯 HTML 片段（html_item_selector 提取 li）
        """
        import json
        from parsel import Selector

        site_name = response.meta.get("site_name")
        api_cfg = response.meta.get("api_config") or {}
        resp_type = (api_cfg.get("response_type") or "json").lower()
        items = []
        self.logger.info(f"resp_type: {resp_type} ")

        def parse_li_elements(elements):
            out = []
            for i, el in enumerate(elements):
                title = el.css("a::attr(title)").get() or (el.css("a::text").get() or "").strip()
                url = el.css("a::attr(href)").get()
                li_text = " ".join([t.strip() for t in el.css("::text").getall() if t and t.strip()])
                import re
                m = re.search(r"\d{4}-\d{2}-\d{2}", li_text)
                date = m.group(0) if m else None
                if not url:
                    continue
                out.append({"title": title, "url": url, "date": date, "index": i+1})
            return out

        try:
            if resp_type == "json":
                self.logger.info(f"json")

                data = json.loads(response.text)
                path = (api_cfg.get("json_path") or "").strip()
                node = data
                if path:
                    for part in path.split('.'):
                        if not part:
                            continue
                        if isinstance(node, dict):
                            node = node.get(part)
                        else:
                            node = None
                        if node is None:
                            break
                if isinstance(node, list):
                    fmap = api_cfg.get("field_mappings") or {}
                    url_template = api_cfg.get("url_template")
                    for i, it in enumerate(node):
                        try:
                            title = it.get(fmap.get("title", "title"))
                            url = it.get(fmap.get("url", "url"))
                            date = it.get(fmap.get("date", "date"))

                            # 处理URL模板
                            if url_template and url:
                                temp = url
                                url = url_template.format(
                                    url=temp
                                )

                            self.logger.info(f"json title: {title}, url: {url}, date: {date}")


                            if not url:
                                continue
                            items.append({"title": title, "url": url, "date": date, "index": i+1})
                        except Exception:
                            continue
                else:
                    html_field = (api_cfg.get("json_html_field") or "").strip()
                    if html_field:
                        node = data
                        for part in html_field.split('.'):
                            if not part:
                                continue
                            if isinstance(node, dict):
                                node = node.get(part)
                            else:
                                node = None
                            if node is None:
                                break
                        if isinstance(node, str) and node.strip():
                            sel = Selector(text=node)
                            li_sel = (api_cfg.get("html_item_selector") or "div.page-content ul li").strip()
                            elements = sel.css(li_sel)
                            items = parse_li_elements(elements)
            else:
                sel = Selector(text=response.text)
                li_sel = (api_cfg.get("html_item_selector") or "div.page-content ul li").strip()
                elements = sel.css(li_sel)
                items = parse_li_elements(elements)
        except Exception as e:
            self.logger.warning(f"⚠️ 解析列表API失败: {e}")

        self.logger.info(f"🧪 列表API提取到 {len(items)} 项")
        if items:
            # 直接复用统一的增量处理逻辑
            yield from self._handle_list_incremental(response, site_name, items)

    def _follow_links(self, response, site_name: str, extracted_data: Dict):
        """跟进链接"""
        try:
            # 获取提取的链接
            links = []

            # 从提取的数据中获取链接
            if "items" in extracted_data:
                for item in extracted_data["items"]:
                    if "url" in item and item["url"]:
                        links.append(item["url"])

            # 限制链接数量
            max_links = 10  # 可配置
            links = links[:max_links]

            logger.info(f"🔗 准备跟进 {len(links)} 个链接")

            for link in links:
                absolute_url = urljoin(response.url, link)
                yield scrapy.Request(
                    url=absolute_url,
                    callback=self.parse,
                    meta={"site_name": site_name, "page_type": "detail_page"},
                    errback=self.handle_error,
                )

        except Exception as e:
            logger.error(f"❌ 链接跟进失败: {e}")

    def handle_error(self, failure):
        """处理请求错误"""
        logger.error(f"❌ 请求失败: {failure.request.url}")
        logger.error(f"❌ 错误详情: {failure.value}")

        yield {
            "url": failure.request.url,
            "error": str(failure.value),
            "spider_name": self.name,
            "status": "request_failed",
        }

    def closed(self, reason):
        """爬虫关闭时的清理工作"""
        logger.info(f"🏁 自适应爬虫V2关闭")
        logger.info(f"📊 关闭原因: {reason}")

        # 输出统计信息
        stats = self.crawler.stats.get_stats()
        logger.info("📈 爬虫统计信息:")
        for key, value in stats.items():
            if "count" in key.lower():
                logger.info(f"    {key}: {value}")
