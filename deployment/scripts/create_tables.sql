-- Active: 1753286756321@@127.0.0.1@5432@crawler_db@public
-- 企业级分布式爬虫系统数据库表结构
-- PostgreSQL版本

-- 创建数据库（如果不存在）
CREATE DATABASE crawler_db;

-- 使用数据库
\c crawler_db;

-- 创建扩展
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- 疫情数据主表
CREATE TABLE IF NOT EXISTS epidemic_data (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_url VARCHAR(1000) NOT NULL,
    source_name VARCHAR(200) NOT NULL,
    crawl_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    -- 内容信息
    title TEXT,
    content TEXT,
    -- 地理信息
    region VARCHAR(100) NOT NULL,
    region_code VARCHAR(20),
    region_level VARCHAR(20), -- country, province, city, district
    -- 疫情数据
    confirmed_cases INTEGER DEFAULT 0,
    death_cases INTEGER DEFAULT 0,
    recovered_cases INTEGER DEFAULT 0,
    active_cases INTEGER DEFAULT 0,
    new_confirmed INTEGER DEFAULT 0,
    new_deaths INTEGER DEFAULT 0,
    new_recovered INTEGER DEFAULT 0,
    -- 时间信息
    report_date DATE NOT NULL,
    update_time TIMESTAMP WITH TIME ZONE,
    -- 数据质量
    data_quality_score DECIMAL(3,2) DEFAULT 0.00,
    validation_status VARCHAR(20) DEFAULT 'pending', -- pending, valid, invalid
    validation_details JSONB,
    -- 元数据
    spider_name VARCHAR(100),
    crawl_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    data_fingerprint VARCHAR(64), -- MD5哈希用于去重
    -- 审计字段
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    -- 约束
    CONSTRAINT chk_quality_score CHECK (data_quality_score >= 0 AND data_quality_score <= 1),
    CONSTRAINT chk_validation_status CHECK (validation_status IN ('pending', 'valid', 'invalid')),
    CONSTRAINT chk_cases_positive CHECK (
        confirmed_cases >= 0 AND
        death_cases >= 0 AND
        recovered_cases >= 0 AND
        active_cases >= 0
    )
);

-- 新闻数据表
CREATE TABLE IF NOT EXISTS news_data (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    url VARCHAR(1000) NOT NULL UNIQUE,
    title TEXT NOT NULL,
    content TEXT,
    -- 发布信息
    publish_date DATE,
    author VARCHAR(200),
    source VARCHAR(200),
    -- 分类信息
    category VARCHAR(100),
    tags TEXT[], -- PostgreSQL数组类型
    -- 统计信息
    view_count INTEGER DEFAULT 0,
    comment_count INTEGER DEFAULT 0,
    -- 元数据
    crawl_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    spider_name VARCHAR(100),
    data_fingerprint VARCHAR(64),
    -- 审计字段
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 政策文件表
CREATE TABLE IF NOT EXISTS policy_data (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    url VARCHAR(1000) NOT NULL UNIQUE,
    title TEXT NOT NULL,
    content TEXT,
    -- 政策信息
    policy_number VARCHAR(100),
    issue_date DATE,
    effective_date DATE,
    issuing_authority VARCHAR(200),
    -- 分类信息
    policy_type VARCHAR(100),
    policy_level VARCHAR(50), -- national, provincial, municipal
    keywords TEXT[],
    -- 元数据
    crawl_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    spider_name VARCHAR(100),
    data_fingerprint VARCHAR(64),
    -- 审计字段
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 数据质量报告表
CREATE TABLE IF NOT EXISTS data_quality_reports (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    data_table VARCHAR(100) NOT NULL, -- 数据表名
    data_id UUID NOT NULL, -- 数据记录ID
    -- 质量评分
    overall_score DECIMAL(3,2) NOT NULL,
    completeness_score DECIMAL(3,2),
    accuracy_score DECIMAL(3,2),
    consistency_score DECIMAL(3,2),
    timeliness_score DECIMAL(3,2),
    -- 详细信息
    quality_details JSONB,
    validation_rules TEXT[],
    failed_rules TEXT[],
    -- 时间信息
    evaluated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    evaluator VARCHAR(100), -- 评估器名称
    -- 审计字段
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 爬虫统计表
CREATE TABLE IF NOT EXISTS crawler_statistics (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    spider_name VARCHAR(100) NOT NULL,
    -- 统计时间
    statistics_date DATE NOT NULL,
    statistics_hour INTEGER, -- 0-23，用于小时级统计
    -- 爬取统计
    total_requests INTEGER DEFAULT 0,
    successful_requests INTEGER DEFAULT 0,
    failed_requests INTEGER DEFAULT 0,
    -- 数据统计
    items_scraped INTEGER DEFAULT 0,
    items_dropped INTEGER DEFAULT 0,
    duplicate_items INTEGER DEFAULT 0,
    -- 性能统计
    avg_response_time DECIMAL(8,3),
    min_response_time DECIMAL(8,3),
    max_response_time DECIMAL(8,3),
    -- 错误统计
    error_types JSONB, -- 错误类型及数量
    -- 审计字段
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    -- 唯一约束
    UNIQUE(spider_name, statistics_date, statistics_hour)
);

-- 代理统计表
CREATE TABLE IF NOT EXISTS proxy_statistics (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    proxy_url VARCHAR(500) NOT NULL,
    -- 统计时间
    statistics_date DATE NOT NULL,
    -- 使用统计
    total_requests INTEGER DEFAULT 0,
    successful_requests INTEGER DEFAULT 0,
    failed_requests INTEGER DEFAULT 0,
    -- 性能统计
    avg_response_time DECIMAL(8,3),
    success_rate DECIMAL(5,4),
    -- 代理信息
    proxy_provider VARCHAR(100),
    country VARCHAR(10),
    anonymity_level VARCHAR(20),
    -- 审计字段
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    -- 唯一约束
    UNIQUE(proxy_url, statistics_date)
);

-- 任务队列表
CREATE TABLE IF NOT EXISTS crawler_tasks (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    task_id VARCHAR(100) NOT NULL UNIQUE,
    spider_name VARCHAR(100) NOT NULL,
    -- 任务信息
    url VARCHAR(1000) NOT NULL,
    priority INTEGER DEFAULT 0, -- 优先级，数字越大优先级越高
    task_type VARCHAR(50) DEFAULT 'crawl', -- crawl, validate, process
    -- 任务状态
    status VARCHAR(20) DEFAULT 'pending', -- pending, running, completed, failed, cancelled
    retry_count INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 3,
    -- 配置信息
    task_config JSONB,
    -- 时间信息
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    scheduled_at TIMESTAMP WITH TIME ZONE,
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    -- 结果信息
    result JSONB,
    error_message TEXT,
    -- 审计字段
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    -- 约束
    CONSTRAINT chk_task_status CHECK (status IN ('pending', 'running', 'completed', 'failed', 'cancelled')),
    CONSTRAINT chk_priority CHECK (priority >= 0)
);

-- 创建索引
-- 疫情数据表索引
CREATE INDEX IF NOT EXISTS idx_epidemic_region_date ON epidemic_data(region, report_date);
CREATE INDEX IF NOT EXISTS idx_epidemic_source ON epidemic_data(source_name);
CREATE INDEX IF NOT EXISTS idx_epidemic_crawl_time ON epidemic_data(crawl_time);
CREATE INDEX IF NOT EXISTS idx_epidemic_quality ON epidemic_data(data_quality_score);
CREATE INDEX IF NOT EXISTS idx_epidemic_fingerprint ON epidemic_data(data_fingerprint);
CREATE INDEX IF NOT EXISTS idx_epidemic_spider ON epidemic_data(spider_name);

-- 新闻数据表索引
CREATE INDEX IF NOT EXISTS idx_news_publish_date ON news_data(publish_date);
CREATE INDEX IF NOT EXISTS idx_news_source ON news_data(source);
CREATE INDEX IF NOT EXISTS idx_news_category ON news_data(category);
CREATE INDEX IF NOT EXISTS idx_news_fingerprint ON news_data(data_fingerprint);

-- 政策数据表索引
CREATE INDEX IF NOT EXISTS idx_policy_issue_date ON policy_data(issue_date);
CREATE INDEX IF NOT EXISTS idx_policy_authority ON policy_data(issuing_authority);
CREATE INDEX IF NOT EXISTS idx_policy_type ON policy_data(policy_type);
CREATE INDEX IF NOT EXISTS idx_policy_fingerprint ON policy_data(data_fingerprint);

-- 质量报告表索引
CREATE INDEX IF NOT EXISTS idx_quality_table_id ON data_quality_reports(data_table, data_id);
CREATE INDEX IF NOT EXISTS idx_quality_score ON data_quality_reports(overall_score);
CREATE INDEX IF NOT EXISTS idx_quality_evaluated_at ON data_quality_reports(evaluated_at);

-- 统计表索引
CREATE INDEX IF NOT EXISTS idx_crawler_stats_spider_date ON crawler_statistics(spider_name, statistics_date);
CREATE INDEX IF NOT EXISTS idx_proxy_stats_date ON proxy_statistics(statistics_date);
CREATE INDEX IF NOT EXISTS idx_proxy_stats_url ON proxy_statistics(proxy_url);

-- 任务表索引
CREATE INDEX IF NOT EXISTS idx_tasks_status ON crawler_tasks(status);
CREATE INDEX IF NOT EXISTS idx_tasks_priority ON crawler_tasks(priority DESC);
CREATE INDEX IF NOT EXISTS idx_tasks_spider ON crawler_tasks(spider_name);
CREATE INDEX IF NOT EXISTS idx_tasks_created_at ON crawler_tasks(created_at);
CREATE INDEX IF NOT EXISTS idx_tasks_scheduled_at ON crawler_tasks(scheduled_at);

-- 全文搜索索引
CREATE INDEX IF NOT EXISTS idx_epidemic_content_gin ON epidemic_data USING gin(to_tsvector('english', content));
CREATE INDEX IF NOT EXISTS idx_news_content_gin ON news_data USING gin(to_tsvector('english', content));
CREATE INDEX IF NOT EXISTS idx_policy_content_gin ON policy_data USING gin(to_tsvector('english', content));

-- 创建更新时间触发器函数
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- 为需要的表创建更新时间触发器
CREATE TRIGGER update_epidemic_data_updated_at BEFORE UPDATE ON epidemic_data FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_news_data_updated_at BEFORE UPDATE ON news_data FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_policy_data_updated_at BEFORE UPDATE ON policy_data FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_crawler_statistics_updated_at BEFORE UPDATE ON crawler_statistics FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_proxy_statistics_updated_at BEFORE UPDATE ON proxy_statistics FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_crawler_tasks_updated_at BEFORE UPDATE ON crawler_tasks FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
