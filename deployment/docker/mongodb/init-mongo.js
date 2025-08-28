// MongoDB初始化脚本

// 切换到crawler_db数据库
db = db.getSiblingDB('crawler_db');

// 创建用户
db.createUser({
  user: 'crawler_user',
  pwd: 'crawler_pass',
  roles: [
    {
      role: 'readWrite',
      db: 'crawler_db'
    }
  ]
});

// 创建集合并设置索引

// 原始爬取数据集合
db.createCollection('raw_epidemic_data');
db.raw_epidemic_data.createIndex({ "source_url": 1 }, { unique: true });
db.raw_epidemic_data.createIndex({ "spider_name": 1, "crawl_time": -1 });
db.raw_epidemic_data.createIndex({ "region": 1, "report_date": -1 });
db.raw_epidemic_data.createIndex({ "crawl_time": -1 });
db.raw_epidemic_data.createIndex({ "data_fingerprint": 1 });

// 处理后的数据集合
db.createCollection('processed_epidemic_data');
db.processed_epidemic_data.createIndex({ "source_url": 1 });
db.processed_epidemic_data.createIndex({ "region": 1, "report_date": -1 });
db.processed_epidemic_data.createIndex({ "data_quality_score": -1 });
db.processed_epidemic_data.createIndex({ "validation_status": 1 });

// 新闻数据集合
db.createCollection('news_data');
db.news_data.createIndex({ "url": 1 }, { unique: true });
db.news_data.createIndex({ "publish_date": -1 });
db.news_data.createIndex({ "source": 1 });
db.news_data.createIndex({ "category": 1 });
db.news_data.createIndex({ "title": "text", "content": "text" });

// 政策数据集合
db.createCollection('policy_data');
db.policy_data.createIndex({ "url": 1 }, { unique: true });
db.policy_data.createIndex({ "issue_date": -1 });
db.policy_data.createIndex({ "issuing_authority": 1 });
db.policy_data.createIndex({ "policy_type": 1 });
db.policy_data.createIndex({ "title": "text", "content": "text" });

// 爬虫日志集合
db.createCollection('crawler_logs');
db.crawler_logs.createIndex({ "spider_name": 1, "timestamp": -1 });
db.crawler_logs.createIndex({ "level": 1, "timestamp": -1 });
db.crawler_logs.createIndex({ "timestamp": -1 });

// 任务队列集合
db.createCollection('crawler_tasks');
db.crawler_tasks.createIndex({ "task_id": 1 }, { unique: true });
db.crawler_tasks.createIndex({ "spider_name": 1, "status": 1 });
db.crawler_tasks.createIndex({ "priority": -1, "created_at": 1 });
db.crawler_tasks.createIndex({ "status": 1, "scheduled_at": 1 });

// 代理池集合
db.createCollection('proxy_pool');
db.proxy_pool.createIndex({ "proxy_url": 1 }, { unique: true });
db.proxy_pool.createIndex({ "success_rate": -1 });
db.proxy_pool.createIndex({ "last_check": -1 });
db.proxy_pool.createIndex({ "anonymity_level": 1 });

// 系统配置集合
db.createCollection('system_config');
db.system_config.createIndex({ "config_key": 1 }, { unique: true });

// 监控指标集合
db.createCollection('monitoring_metrics');
db.monitoring_metrics.createIndex({ "metric_name": 1, "timestamp": -1 });
db.monitoring_metrics.createIndex({ "timestamp": -1 });

// 错误记录集合
db.createCollection('error_records');
db.error_records.createIndex({ "spider_name": 1, "timestamp": -1 });
db.error_records.createIndex({ "error_type": 1, "timestamp": -1 });
db.error_records.createIndex({ "timestamp": -1 });

// 数据质量报告集合
db.createCollection('quality_reports');
db.quality_reports.createIndex({ "data_table": 1, "data_id": 1 });
db.quality_reports.createIndex({ "overall_score": -1 });
db.quality_reports.createIndex({ "evaluated_at": -1 });

// 插入初始配置数据
db.system_config.insertMany([
  {
    config_key: "crawler_settings",
    config_value: {
      concurrent_requests: 16,
      download_delay: 1,
      randomize_download_delay: 0.5,
      retry_times: 3,
      retry_http_codes: [500, 502, 503, 504, 408, 429]
    },
    created_at: new Date(),
    updated_at: new Date()
  },
  {
    config_key: "proxy_settings",
    config_value: {
      pool_size: 100,
      validation_timeout: 10,
      validation_interval: 300,
      max_failures: 3,
      rotation_strategy: "round_robin"
    },
    created_at: new Date(),
    updated_at: new Date()
  },
  {
    config_key: "data_quality_settings",
    config_value: {
      min_quality_score: 0.7,
      required_fields: ["title", "content", "region", "report_date"],
      validation_rules: {
        title: { min_length: 5, max_length: 200 },
        content: { min_length: 50 },
        confirmed_cases: { min_value: 0, max_value: 10000000 }
      }
    },
    created_at: new Date(),
    updated_at: new Date()
  },
  {
    config_key: "monitoring_settings",
    config_value: {
      alert_on_errors: true,
      max_error_rate: 0.1,
      min_success_rate: 0.8,
      metrics_retention_days: 30
    },
    created_at: new Date(),
    updated_at: new Date()
  }
]);

// 插入示例数据
db.raw_epidemic_data.insertOne({
  source_url: "http://example.com/test",
  source_name: "测试数据源",
  spider_name: "test_spider",
  title: "测试疫情数据",
  content: "这是一条测试疫情数据",
  region: "测试地区",
  confirmed_cases: 100,
  death_cases: 5,
  recovered_cases: 80,
  active_cases: 15,
  report_date: new Date(),
  crawl_time: new Date(),
  crawl_timestamp: new Date(),
  data_fingerprint: "test_fingerprint_123",
  data_quality_score: 0.85,
  validation_status: "valid"
});

// 创建TTL索引（自动删除过期数据）
// 爬虫日志保留30天
db.crawler_logs.createIndex({ "timestamp": 1 }, { expireAfterSeconds: 2592000 });

// 监控指标保留90天
db.monitoring_metrics.createIndex({ "timestamp": 1 }, { expireAfterSeconds: 7776000 });

// 错误记录保留60天
db.error_records.createIndex({ "timestamp": 1 }, { expireAfterSeconds: 5184000 });

// 已完成的任务保留7天
db.crawler_tasks.createIndex({ "completed_at": 1 }, {
  expireAfterSeconds: 604800,
  partialFilterExpression: { "status": "completed" }
});

// 创建分片键（如果使用分片）
// sh.shardCollection("crawler_db.raw_epidemic_data", { "spider_name": 1, "crawl_time": 1 });
// sh.shardCollection("crawler_db.crawler_logs", { "spider_name": 1, "timestamp": 1 });

print("MongoDB初始化完成");
print("数据库: crawler_db");
print("用户: crawler_user");
print("集合数量: " + db.getCollectionNames().length);

// 显示集合信息
db.getCollectionNames().forEach(function(collection) {
  print("集合: " + collection + ", 索引数量: " + db[collection].getIndexes().length);
});
