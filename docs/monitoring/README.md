# 分布式爬虫监控与告警指南

## 功能概述
- Prometheus 暴露 /metrics（默认端口 9108）
- 采集系统/Redis/队列/爬取/数据质量等指标
- Prometheus 告警规则 + Alertmanager 多渠道通知
- Grafana 仪表板模板
- 提供 REST API 简易查询

## 快速开始（开发环境）
1. 安装依赖
```
pip install prometheus-client psutil redis flask requests
```

2. 启动 Exporter（建议在 Scrapy 进程启动时调用）
```python
# 例如在 crawl 入口脚本中：
from crawler.monitoring import MonitoringExporter
exp = MonitoringExporter(port=9108)
exp.start()
```

3. 启用 Scrapy 扩展
- settings.py 已启用：
```
EXTENSIONS = {
    'scrapy.extensions.telnet.TelnetConsole': None,
    'crawler.monitoring.scrapy_ext.MetricsExtension': 500,
}
```

4. 启动 Prometheus
```
prometheus --config.file=deployment/monitoring/prometheus.yml \
           --web.listen-address=:9090 \
           --web.enable-lifecycle \
           --storage.tsdb.path=./prom_data
```

5. 导入 Grafana 仪表板
- 打开 Grafana 导入 deployment/monitoring/grafana-dashboard.json

6. 配置 Alertmanager（可选）
- 参考 deployment/monitoring/alert_rules.yml
- 配置钉钉/企业微信/邮件 webhook 到 Alertmanager

## 指标说明
- crawler_request_total{spider,site,status}
- crawler_request_latency_seconds_bucket/sum
- crawler_item_extracted_total / stored / dropped
- crawler_queue_length / crawler_queue_backlog_seconds
- crawler_cpu_usage_percent / crawler_mem_usage_percent / crawler_disk_usage_percent
- crawler_redis_up / connected_clients / used_memory_bytes / slowlog_len

## 生产建议
- 将 Exporter 做成独立进程或 sidecar，避免与爬虫竞争资源
- Prometheus 使用持久化存储并开启压缩，保留30天
- 针对不同环境（dev/staging/prod）区分 label（如 env）
- 队列积压估计可结合 ZSET 的 score 精确计算

