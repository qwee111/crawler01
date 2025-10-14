import json
import os
from datetime import datetime

import scrapy
from scrapy_redis.spiders import RedisSpider


class BochaaiSpider(RedisSpider):
    name = "bochaai_spider"
    redis_key = "bochaai:start_urls"  # Redis队列的键名
    api_url = "https://api.bochaai.com/v1/web-search"
    # 允许将 415 等非 2xx 响应也交给 parse 处理，便于排查
    handle_httpstatus_list = [415]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.api_key = os.getenv("API_TOKEN")
        if not self.api_key or self.api_key == "your-api-token-change-this-in-production":
            self.logger.error("API_TOKEN 未在 .env 文件中配置或使用默认占位符。请设置有效的API KEY。")
            raise ValueError("API_TOKEN is not configured correctly.")

    def make_request_from_data(self, data):
        """
        从Redis中读取序列化后的请求数据，并重新构建Scrapy请求。
        data 是从Redis队列中取出的字节字符串，需要解码。
        """
        try:
            request_data = json.loads(data.decode("utf-8"))
            url = request_data.get("url")
            method = request_data.get("method", "POST")
            headers = request_data.get("headers", {})
            body = request_data.get("body", None)
            callback_name = request_data.get("callback", "parse")
            meta = request_data.get("meta", {})

            if not url:
                self.logger.warning(f"从Redis获取的请求数据缺少 'url' 字段: {request_data}")
                return None

            # 确保回调方法存在
            callback = getattr(self, callback_name, None)
            if not callback:
                self.logger.error(f"爬虫中未找到回调方法: {callback_name}")
                return None

            # 将 body 统一为 bytes，以满足 Scrapy 的要求
            body_bytes = None
            if body is None:
                body_bytes = None
            elif isinstance(body, (bytes, bytearray)):
                body_bytes = bytes(body)
            elif isinstance(body, str):
                body_bytes = body.encode("utf-8")
            else:
                # 如果传入的是可序列化对象，则转为 JSON bytes
                try:
                    body_bytes = json.dumps(body, ensure_ascii=False).encode("utf-8")
                except Exception:
                    body_bytes = str(body).encode("utf-8")

            # 安全打印请求信息
            self.logger.info(f"正在从Redis构建请求: {method} {url}")
            self.logger.debug(f"请求头: {headers}")
            try:
                body_preview = (
                    body_bytes.decode("utf-8", errors="replace")[:500] + ("..." if body_bytes and len(body_bytes) > 500 else "")
                ) if body_bytes else "None"
            except Exception:
                body_preview = "<body decode error>"
            self.logger.debug(f"请求体(预览): {body_preview}")

            return scrapy.Request(
                url=url,
                method=method,
                headers=headers,
                body=body_bytes,
                callback=callback,
                meta=meta,
                dont_filter=True # 确保请求不会被去重，因为请求体可能不同
            )
        except json.JSONDecodeError:
            self.logger.error(f"无法解码Redis数据为JSON: {data}")
            return None
        except Exception as e:
            self.logger.error(f"构建请求时发生错误: {e}, 数据: {data}")
            return None

    def parse(self, response):
        """
        解析API响应。
        """
        query_params = response.meta.get("query_params", {})
        self.logger.info(f"正在解析API响应，原始查询参数: {query_params.get('query')}")

        try:
            # 如果服务端返回 415，直接打印诊断信息
            if response.status == 415:
                content_type = response.headers.get("Content-Type", b"").decode("utf-8", errors="replace")
                req = response.request
                self.logger.error(
                    "API返回 415 Unsupported Media Type | URL: %s | Content-Type: %s | 请求头: %s | 请求体(前500): %s",
                    response.url,
                    content_type,
                    dict(req.headers or {}),
                    (req.body.decode("utf-8", errors="replace")[:500] + ("..." if req.body and len(req.body) > 500 else "")) if req and getattr(req, "body", None) else "None",
                )
                return

            # 优先检查响应是否为 JSON
            content_type = response.headers.get("Content-Type", b"").decode("utf-8", errors="replace").lower()
            if "application/json" not in content_type and "json" not in content_type:
                self.logger.error(
                    "响应的 Content-Type 非 JSON: %s | 状态: %s | URL: %s | 文本(前200): %s",
                    content_type,
                    response.status,
                    response.url,
                    response.text[:200] + ("..." if len(response.text) > 200 else ""),
                )
                return

            data = json.loads(response.text)
            if data.get("code") != 200:
                self.logger.error(f"API返回错误码: {data.get('code')}, 消息: {data.get('msg')}")
                return

            webpages = data.get("data", {}).get("webPages", {}).get("value", [])
            if not webpages:
                self.logger.info(f"API响应中没有找到网页数据: {response.url}")
                return

            for page in webpages:
                # 规范化发布时间为 YYYY-MM-DD（若可解析）
                raw_dt = page.get("datePublished")
                pub_date = None
                try:
                    if isinstance(raw_dt, str) and raw_dt:
                        # 优先使用fromisoformat；若含Z则替换为+00:00
                        dt_str = raw_dt.replace("Z", "+00:00")
                        try:
                            from datetime import datetime as _dt

                            parsed = _dt.fromisoformat(dt_str)
                        except Exception:
                            # 回退：仅截取前10位 'YYYY-MM-DD'
                            parsed = None
                        if parsed:
                            pub_date = parsed.date().isoformat()
                        else:
                            if len(raw_dt) >= 10 and raw_dt[4] in "-/.":
                                pub_date = raw_dt[:10].replace("/", "-")
                except Exception:
                    pub_date = None

                # 按照 adaptive_spider_v2 的风格，直接产出 dict，便于 MongoDB 管道入库
                yield {
                    "url": page.get("url"),
                    "title": page.get("name") if page.get("name") else page.get("snippet"),
                    "content": page.get("summary"),
                    "publish_date": pub_date or raw_dt,
                    "source": page.get("siteName"),
                    "spider_name": self.name,
                    "crawl_time": datetime.now().isoformat(),
                }

        except json.JSONDecodeError:
            self.logger.error(f"无法解码API响应为JSON: {response.url}, 响应内容: {response.text[:200]}...")
        except Exception as e:
            self.logger.error(f"解析API响应时发生错误: {e}, URL: {response.url}")
