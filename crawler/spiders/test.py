import urllib.parse

import scrapy
from bs4 import BeautifulSoup
from scrapy_redis.spiders import RedisSpider


class BaiduSpider(RedisSpider):
    name = "baidu_spider"
    redis_key = "baidu_search:start_urls"
    max_pages = 3  # 最大爬取页数，可根据需要修改

    def parse(self, response):
        self.logger.info(f"正在请求百度搜索结果页: {response.url}")
        soup = BeautifulSoup(response.text, "html.parser")
        results = []
        for item in soup.find_all("div", class_="result"):
            a_tag = item.find("a")
            if a_tag and a_tag.get("href"):
                title = a_tag.get_text(strip=True)
                link = a_tag["href"]
                results.append((title, link))
        if not results:
            for item in soup.find_all("div", attrs={"data-click": True}):
                a_tag = item.find("a")
                if a_tag and a_tag.get("href"):
                    title = a_tag.get_text(strip=True)
                    link = a_tag["href"]
                    results.append((title, link))
        if not results:
            self.logger.info("未获取到任何搜索结果，可能redis队列为空或页面无结果。")
        for title, link in results:
            # 对每个结果链接发起请求，回调parse_detail
            yield scrapy.Request(
                url=link, callback=self.parse_detail, meta={"title": title, "url": link}
            )

        # 翻页处理
        page_num = response.meta.get("page_num", 1)
        if page_num < self.max_pages:
            # 解析当前URL，构造下一页URL
            parsed = urllib.parse.urlparse(response.url)
            params = urllib.parse.parse_qs(parsed.query)
            wd = params.get("wd", [""])[0]
            next_pn = page_num * 10
            next_query = urllib.parse.urlencode({"wd": wd, "pn": next_pn})
            next_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{next_query}"
            self.logger.info(f"准备请求第{page_num+1}页: {next_url}")
            yield scrapy.Request(
                url=next_url, callback=self.parse, meta={"page_num": page_num + 1}
            )

    def parse_detail(self, response):
        # 提取正文内容
        soup = BeautifulSoup(response.text, "html.parser")
        body = soup.body
        content = body.get_text(separator="\n", strip=True) if body else ""
        yield {
            "title": response.meta.get("title"),
            "url": response.meta.get("url"),
            "content": content,
        }


# 使用说明：
# 1. 在settings.py中配置scrapy-redis调度器和pipeline。
# 2. 启动redis服务，推送百度搜索URL到 baidu_search:start_urls 队列。
# 3. 运行 scrapy crawl baidu_spider
