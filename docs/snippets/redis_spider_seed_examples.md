# RedisSpider 种子投放示例

## 按站点分桶的键名
- adaptive_v2:bjcdc:start_urls
- adaptive_v2:nhc_new:start_urls

## 纯字符串 URL
```
redis-cli lpush adaptive_v2:bjcdc:start_urls "https://www.bjcdc.org/cdcmodule/jkdt/bsxw/index.shtml"
```

## JSON 种子（携带上下文）
```
redis-cli lpush adaptive_v2:bjcdc:start_urls '{"url":"https://www.bjcdc.org/cdcmodule/jkdt/bsxw/index.shtml","site":"bjcdc","meta":{"page_type":"list_page"}}'
```

## 全局队列（JSON 内自带 site）
```
redis-cli lpush adaptive_v2:start_urls '{"url":"https://...","site":"nhc_new"}'
```
