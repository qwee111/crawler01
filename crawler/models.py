# -*- coding: utf-8 -*-
"""
数据库模型定义

使用SQLAlchemy定义数据库表结构
"""

import uuid
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import (
    ARRAY,
    JSON,
    Boolean,
    CheckConstraint,
    Column,
    Date,
    DateTime,
    Float,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    create_engine,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.sql import text

Base = declarative_base()


class EpidemicData(Base):
    """疫情数据模型"""

    __tablename__ = "epidemic_data"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source_url = Column(String(1000), nullable=False)
    source_name = Column(String(200), nullable=False)
    crawl_time = Column(DateTime(timezone=True), default=datetime.utcnow)

    # 内容信息
    title = Column(Text)
    content = Column(Text)

    # 地理信息
    region = Column(String(100), nullable=False)
    region_code = Column(String(20))
    region_level = Column(String(20))

    # 疫情数据
    confirmed_cases = Column(Integer, default=0)
    death_cases = Column(Integer, default=0)
    recovered_cases = Column(Integer, default=0)
    active_cases = Column(Integer, default=0)
    new_confirmed = Column(Integer, default=0)
    new_deaths = Column(Integer, default=0)
    new_recovered = Column(Integer, default=0)

    # 时间信息
    report_date = Column(Date, nullable=False)
    update_time = Column(DateTime(timezone=True))

    # 数据质量
    data_quality_score = Column(Float, default=0.0)
    validation_status = Column(String(20), default="pending")
    validation_details = Column(JSONB)

    # 元数据
    spider_name = Column(String(100))
    crawl_timestamp = Column(DateTime(timezone=True), default=datetime.utcnow)
    data_fingerprint = Column(String(64))

    # 审计字段
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(
        DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow
    )

    # 约束
    __table_args__ = (
        CheckConstraint(
            "data_quality_score >= 0 AND data_quality_score <= 1",
            name="chk_quality_score",
        ),
        CheckConstraint(
            "validation_status IN ('pending', 'valid', 'invalid')",
            name="chk_validation_status",
        ),
        CheckConstraint(
            "confirmed_cases >= 0 AND death_cases >= 0 AND recovered_cases >= 0 AND active_cases >= 0",
            name="chk_cases_positive",
        ),
        Index("idx_epidemic_region_date", "region", "report_date"),
        Index("idx_epidemic_source", "source_name"),
        Index("idx_epidemic_crawl_time", "crawl_time"),
        Index("idx_epidemic_quality", "data_quality_score"),
        Index("idx_epidemic_fingerprint", "data_fingerprint"),
        Index("idx_epidemic_spider", "spider_name"),
    )


class NewsData(Base):
    """新闻数据模型"""

    __tablename__ = "news_data"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    url = Column(String(1000), nullable=False, unique=True)
    title = Column(Text, nullable=False)
    content = Column(Text)

    # 发布信息
    publish_date = Column(Date)
    author = Column(String(200))
    source = Column(String(200))

    # 分类信息
    category = Column(String(100))
    tags = Column(ARRAY(String))

    # 统计信息
    view_count = Column(Integer, default=0)
    comment_count = Column(Integer, default=0)

    # 元数据
    crawl_time = Column(DateTime(timezone=True), default=datetime.utcnow)
    spider_name = Column(String(100))
    data_fingerprint = Column(String(64))

    # 审计字段
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(
        DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow
    )

    __table_args__ = (
        Index("idx_news_publish_date", "publish_date"),
        Index("idx_news_source", "source"),
        Index("idx_news_category", "category"),
        Index("idx_news_fingerprint", "data_fingerprint"),
    )


class PolicyData(Base):
    """政策文件模型"""

    __tablename__ = "policy_data"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    url = Column(String(1000), nullable=False, unique=True)
    title = Column(Text, nullable=False)
    content = Column(Text)

    # 政策信息
    policy_number = Column(String(100))
    issue_date = Column(Date)
    effective_date = Column(Date)
    issuing_authority = Column(String(200))

    # 分类信息
    policy_type = Column(String(100))
    policy_level = Column(String(50))
    keywords = Column(ARRAY(String))

    # 元数据
    crawl_time = Column(DateTime(timezone=True), default=datetime.utcnow)
    spider_name = Column(String(100))
    data_fingerprint = Column(String(64))

    # 审计字段
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(
        DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow
    )

    __table_args__ = (
        Index("idx_policy_issue_date", "issue_date"),
        Index("idx_policy_authority", "issuing_authority"),
        Index("idx_policy_type", "policy_type"),
        Index("idx_policy_fingerprint", "data_fingerprint"),
    )


class DataQualityReport(Base):
    """数据质量报告模型"""

    __tablename__ = "data_quality_reports"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    data_table = Column(String(100), nullable=False)
    data_id = Column(UUID(as_uuid=True), nullable=False)

    # 质量评分
    overall_score = Column(Float, nullable=False)
    completeness_score = Column(Float)
    accuracy_score = Column(Float)
    consistency_score = Column(Float)
    timeliness_score = Column(Float)

    # 详细信息
    quality_details = Column(JSONB)
    validation_rules = Column(ARRAY(String))
    failed_rules = Column(ARRAY(String))

    # 时间信息
    evaluated_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    evaluator = Column(String(100))

    # 审计字段
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)

    __table_args__ = (
        Index("idx_quality_table_id", "data_table", "data_id"),
        Index("idx_quality_score", "overall_score"),
        Index("idx_quality_evaluated_at", "evaluated_at"),
    )


class CrawlerStatistics(Base):
    """爬虫统计模型"""

    __tablename__ = "crawler_statistics"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    spider_name = Column(String(100), nullable=False)

    # 统计时间
    statistics_date = Column(Date, nullable=False)
    statistics_hour = Column(Integer)

    # 爬取统计
    total_requests = Column(Integer, default=0)
    successful_requests = Column(Integer, default=0)
    failed_requests = Column(Integer, default=0)

    # 数据统计
    items_scraped = Column(Integer, default=0)
    items_dropped = Column(Integer, default=0)
    duplicate_items = Column(Integer, default=0)

    # 性能统计
    avg_response_time = Column(Float)
    min_response_time = Column(Float)
    max_response_time = Column(Float)

    # 错误统计
    error_types = Column(JSONB)

    # 审计字段
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(
        DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow
    )

    __table_args__ = (
        UniqueConstraint("spider_name", "statistics_date", "statistics_hour"),
        Index("idx_crawler_stats_spider_date", "spider_name", "statistics_date"),
    )


class ProxyStatistics(Base):
    """代理统计模型"""

    __tablename__ = "proxy_statistics"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    proxy_url = Column(String(500), nullable=False)

    # 统计时间
    statistics_date = Column(Date, nullable=False)

    # 使用统计
    total_requests = Column(Integer, default=0)
    successful_requests = Column(Integer, default=0)
    failed_requests = Column(Integer, default=0)

    # 性能统计
    avg_response_time = Column(Float)
    success_rate = Column(Float)

    # 代理信息
    proxy_provider = Column(String(100))
    country = Column(String(10))
    anonymity_level = Column(String(20))

    # 审计字段
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(
        DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow
    )

    __table_args__ = (
        UniqueConstraint("proxy_url", "statistics_date"),
        Index("idx_proxy_stats_date", "statistics_date"),
        Index("idx_proxy_stats_url", "proxy_url"),
    )


class CrawlerTask(Base):
    """爬虫任务模型"""

    __tablename__ = "crawler_tasks"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    task_id = Column(String(100), nullable=False, unique=True)
    spider_name = Column(String(100), nullable=False)

    # 任务信息
    url = Column(String(1000), nullable=False)
    priority = Column(Integer, default=0)
    task_type = Column(String(50), default="crawl")

    # 任务状态
    status = Column(String(20), default="pending")
    retry_count = Column(Integer, default=0)
    max_retries = Column(Integer, default=3)

    # 配置信息
    task_config = Column(JSONB)

    # 时间信息
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    scheduled_at = Column(DateTime(timezone=True))
    started_at = Column(DateTime(timezone=True))
    completed_at = Column(DateTime(timezone=True))

    # 结果信息
    result = Column(JSONB)
    error_message = Column(Text)

    # 审计字段
    updated_at = Column(
        DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow
    )

    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'running', 'completed', 'failed', 'cancelled')",
            name="chk_task_status",
        ),
        CheckConstraint("priority >= 0", name="chk_priority"),
        Index("idx_tasks_status", "status"),
        Index("idx_tasks_priority", "priority"),
        Index("idx_tasks_spider", "spider_name"),
        Index("idx_tasks_created_at", "created_at"),
        Index("idx_tasks_scheduled_at", "scheduled_at"),
    )


class DatabaseManager:
    """数据库管理器"""

    def __init__(self, database_url: str):
        self.engine = create_engine(database_url, echo=False)
        self.SessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine
        )

    def create_tables(self):
        """创建所有表"""
        Base.metadata.create_all(bind=self.engine)

    def get_session(self) -> Session:
        """获取数据库会话"""
        return self.SessionLocal()

    def close(self):
        """关闭数据库连接"""
        self.engine.dispose()
