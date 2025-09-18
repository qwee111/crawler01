import redis
import urllib.parse
import json


def push_baidu_search_urls(
    keywords,
    redis_host="localhost",
    redis_port=6379,
    redis_key="baidu_search:start_urls",
):
    r = redis.StrictRedis(host=redis_host, port=redis_port, db=0)
    for kw in keywords:
        query = urllib.parse.quote(kw)
        url = f"https://www.baidu.com/s?ie=utf-8&medium=0&rtt=4&bsst=1&rsv_dl=news_t_sk&cl=2&tn=news&rsv_bp=1&rsv_sug3=11&oq=&rsv_sug1=1&rsv_sug7=100&rsv_sug2=0&rsv_btype=t&f=8&inputT=3448&rsv_sug4=4202&wd={query}"
        req = {"url": url, "method": "GET"}
        print(f"Pushing JSON Request: {req}")
        r.lpush(redis_key, json.dumps(req))


if __name__ == "__main__":
    # 示例关键词列表，可根据需要修改
    keywords = ["狼疮"]
    push_baidu_search_urls(keywords)
