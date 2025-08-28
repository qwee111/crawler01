# -*- coding: utf-8 -*-
"""
自定义媒体下载管道：按站点/日期/类型组织文件，并写入更丰富的元数据
- ArticleFilesPipeline: 自定义 通用文件 保存路径与元数据
- ArticleImagesPipeline: 自定义图片保存路径与元数据

目录结构：
  files: <site>/<yyyy-mm-dd>/files/<ext>/<slug>-<hash10>.<ext>
  images: <site>/<yyyy-mm-dd>/images/<slug>-<index>-<hash10>.<ext>

说明：
- 依赖 DataEnrichmentPipeline 生成 article_id 与 title_slug（若无则自动降级）
- 依赖 settings 中的 FILES_STORE / IMAGES_STORE
"""

import hashlib
import os
import re
from datetime import datetime
from typing import Iterable, List, Tuple

from itemadapter import ItemAdapter
from scrapy import Request
from scrapy.pipelines.files import FilesPipeline
from scrapy.pipelines.images import ImagesPipeline


def _short_hash(text: str, length: int = 10) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:length]


def _safe_slug(text: str, max_len: int = 60) -> str:
    text = str(text or "").strip()
    # 允许中英文、数字、下划线、连字符；空白压缩为-
    slug = re.sub(r"[\s]+", "-", re.sub(r"[^\w\-\u4e00-\u9fff]", "", text))
    return slug.strip("-")[:max_len] or "untitled"


def _safe_date(date_val) -> str:
    # 优先 ISO 字符串前 10 位；否则用当前日期
    if not date_val:
        return datetime.now().strftime("%Y-%m-%d")
    s = str(date_val)
    # 支持 'YYYY-MM-DD...' 或 'YYYY/MM/DD'
    m = re.match(r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})", s)
    if m:
        y, mo, d = m.groups()
        return f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"
    # 尝试 ISO
    if len(s) >= 10 and s[4] in "-/.":
        try:
            return s[:10].replace("/", "-")
        except Exception:
            pass
    return datetime.now().strftime("%Y-%m-%d")


def _get_ext_from_url(url: str, default_ext: str = "") -> str:
    try:
        from urllib.parse import urlparse

        path = urlparse(url).path
        _, ext = os.path.splitext(path)
        if ext:
            return ext.lower()
    except Exception:
        pass
    return default_ext


class _ArticleMediaMixin:
    media_type: str = "media"

    def _build_base_parts(
        self, item, request: Request
    ) -> Tuple[str, str, str, str, str]:
        """
        返回 (site, date_str, slug, url, idx_str)
        """
        ad = ItemAdapter(item)
        site = ad.get("site_name") or ad.get("site") or "unknown"
        date_str = _safe_date(ad.get("publish_date") or ad.get("crawl_timestamp"))
        slug = ad.get("title_slug") or _safe_slug(
            ad.get("title") or ad.get("article_id") or site
        )
        url = request.url
        idx = request.meta.get("media_index")
        idx_str = f"{int(idx):02d}" if isinstance(idx, int) else "00"
        return site, date_str, slug, url, idx_str

    def _enrich_results(
        self, results: List[Tuple[bool, dict]], item, store_field: str, urls_field: str
    ) -> List[dict]:
        """将 Scrapy 的 results 转为带更多元数据的列表。"""
        ad = ItemAdapter(item)
        urls: List[str] = list(ad.get(urls_field) or [])
        enriched: List[dict] = []
        now_iso = datetime.now().isoformat()
        for i, (ok, info) in enumerate(results):
            if not ok or not info:
                enriched.append(
                    {
                        "status": "failed",
                        "original_url": urls[i] if i < len(urls) else None,
                    }
                )
                continue
            enriched.append(
                {
                    "path": info.get("path"),
                    "checksum": info.get("checksum"),
                    "original_url": urls[i] if i < len(urls) else None,
                    "downloaded_at": now_iso,
                    "site_name": ad.get("site_name"),
                    "article_id": ad.get("article_id"),
                    "title_slug": ad.get("title_slug"),
                    "content_type": ad.get("content_type"),
                }
            )
        return enriched


class ArticleFilesPipeline(_ArticleMediaMixin, FilesPipeline):
    media_type = "files"

    def get_media_requests(self, item, info) -> Iterable[Request]:
        ad = ItemAdapter(item)
        urls = ad.get("file_urls") or []
        for i, u in enumerate(urls):
            yield Request(
                u,
                meta={
                    "media_type": self.media_type,
                    "media_index": i,
                },
                callback=None,
            )

    # Scrapy >=2.6 支持 item 传参；低版本忽略也兼容
    def file_path(self, request, response=None, info=None, *, item=None) -> str:  # type: ignore[override]
        site, date_str, slug, url, idx_str = self._build_base_parts(item, request)
        ext = _get_ext_from_url(url, default_ext=".bin")
        short = _short_hash(url)
        # 目录：<site>/<date>/files/<ext_without_dot>/
        # 文件：<slug>-<hash10><ext>
        type_dir = (
            ext[1:].lower() if ext and ext.startswith(".") else (ext or "bin")
        ).strip() or "bin"
        filename = f"{slug}-{short}{ext}"
        return os.path.join(
            site, date_str, self.media_type, type_dir, filename
        ).replace("\\", "/")

    def item_completed(self, results, item, info):
        enriched = self._enrich_results(
            results, item, store_field="files", urls_field="file_urls"
        )
        ItemAdapter(item)["files"] = enriched
        return item


class ArticleImagesPipeline(_ArticleMediaMixin, ImagesPipeline):
    media_type = "images"

    def get_media_requests(self, item, info) -> Iterable[Request]:
        ad = ItemAdapter(item)
        urls = ad.get("image_urls") or []
        for i, u in enumerate(urls):
            yield Request(
                u,
                meta={
                    "media_type": self.media_type,
                    "media_index": i,
                },
                callback=None,
            )

    def file_path(self, request, response=None, info=None, *, item=None) -> str:  # type: ignore[override]
        site, date_str, slug, url, idx_str = self._build_base_parts(item, request)
        ext = _get_ext_from_url(url, default_ext=".jpg")
        short = _short_hash(url)
        # 目录：<site>/<date>/images/
        # 文件：<slug>-<index>-<hash10><ext>
        filename = f"{slug}-{idx_str}-{short}{ext}"
        return os.path.join(site, date_str, self.media_type, filename).replace(
            "\\", "/"
        )

    def item_completed(self, results, item, info):
        enriched = self._enrich_results(
            results, item, store_field="images", urls_field="image_urls"
        )
        ItemAdapter(item)["images"] = enriched
        return item
