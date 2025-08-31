# -*- coding: utf-8 -*-
"""
数据库模型定义

使用SQLAlchemy定义数据库表结构
"""

import uuid
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import (
    ARRAY,
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
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker


class Base(DeclarativeBase):
    pass


class EpidemicData(Base):
    """疫情数据模型"""

    __tablename__ = "epidemic_data"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    source_url: Mapped[str] = mapped_column(String(1000), nullable=False)
    source_name: Mapped[str] = mapped_column(String(200), nullable=False)
    crawl_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )

    # 内容信息
    title: Mapped[Optional[str]] = mapped_column(Text)
    content: Mapped[Optional[str]] = mapped_column(Text)

    # 地理信息
    region: Mapped[str] = mapped_column(String(100), nullable=False)
    region_code: Mapped[Optional[str]] = mapped_column(String(20))
    region_level: Mapped[Optional[str]] = mapped_column(String(20))

    # 疫情数据
    confirmed_cases: Mapped[int] = mapped_column(Integer, default=0)
    death_cases: Mapped[int] = mapped_column(Integer, default=0)
    recovered_cases: Mapped[int] = mapped_column(Integer, default=0)
    active_cases: Mapped[int] = mapped_column(Integer, default=0)
    new_confirmed: Mapped[int] = mapped_column(Integer, default=0)
    new_deaths: Mapped[int] = mapped_column(Integer, default=0)
    new_recovered: Mapped[int] = mapped_column(Integer, default=0)

    # 时间信息
    report_date: Mapped[date] = mapped_column(Date, nullable=False)
    update_time: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    # 数据质量
    data_quality_score: Mapped[float] = mapped_column(Float, default=0.0)
    validation_status: Mapped[str] = mapped_column(String(20), default="pending")
    validation_details: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSONB)

    # 元数据
    spider_name: Mapped[Optional[str]] = mapped_column(String(100))
    crawl_timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    data_fingerprint: Mapped[Optional[str]] = mapped_column(String(64))

    # 审计字段
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
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

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    url: Mapped[str] = mapped_column(String(1000), nullable=False, unique=True)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    content: Mapped[Optional[str]] = mapped_column(Text)

    # 发布信息
    publish_date: Mapped[Optional[date]] = mapped_column(Date)
    author: Mapped[Optional[str]] = mapped_column(String(200))
    source: Mapped[Optional[str]] = mapped_column(String(200))

    # 分类信息
    category: Mapped[Optional[str]] = mapped_column(String(100))
    tags: Mapped[Optional[List[str]]] = mapped_column(ARRAY(String))

    # 统计信息
    view_count: Mapped[int] = mapped_column(Integer, default=0)
    comment_count: Mapped[int] = mapped_column(Integer, default=0)

    # 元数据
    crawl_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    spider_name: Mapped[Optional[str]] = mapped_column(String(100))
    data_fingerprint: Mapped[Optional[str]] = mapped_column(String(64))

    # 审计字段
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
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

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    url: Mapped[str] = mapped_column(String(1000), nullable=False, unique=True)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    content: Mapped[Optional[str]] = mapped_column(Text)

    # 政策信息
    policy_number: Mapped[Optional[str]] = mapped_column(String(100))
    issue_date: Mapped[Optional[date]] = mapped_column(Date)
    effective_date: Mapped[Optional[date]] = mapped_column(Date)
    issuing_authority: Mapped[Optional[str]] = mapped_column(String(200))

    # 分类信息
    policy_type: Mapped[Optional[str]] = mapped_column(String(100))
    policy_level: Mapped[Optional[str]] = mapped_column(String(50))
    keywords: Mapped[Optional[List[str]]] = mapped_column(ARRAY(String))

    # 元数据
    crawl_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    spider_name: Mapped[Optional[str]] = mapped_column(String(100))
    data_fingerprint: Mapped[Optional[str]] = mapped_column(String(64))

    # 审计字段
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
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

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    data_table: Mapped[str] = mapped_column(String(100), nullable=False)
    data_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)

    # 质量评分
    overall_score: Mapped[float] = mapped_column(Float, nullable=False)
    completeness_score: Mapped[Optional[float]] = mapped_column(Float)
    accuracy_score: Mapped[Optional[float]] = mapped_column(Float)
    consistency_score: Mapped[Optional[float]] = mapped_column(Float)
    timeliness_score: Mapped[Optional[float]] = mapped_column(Float)

    # 详细信息
    quality_details: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSONB)
    validation_rules: Mapped[Optional[List[str]]] = mapped_column(ARRAY(String))
    failed_rules: Mapped[Optional[List[str]]] = mapped_column(ARRAY(String))

    # 时间信息
    evaluated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    evaluator: Mapped[Optional[str]] = mapped_column(String(100))

    # 审计字段
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )

    __table_args__ = (
        Index("idx_quality_table_id", "data_table", "data_id"),
        Index("idx_quality_score", "overall_score"),
        Index("idx_quality_evaluated_at", "evaluated_at"),
    )


class CrawlerStatistics(Base):
    """爬虫统计模型"""

    __tablename__ = "crawler_statistics"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    spider_name: Mapped[str] = mapped_column(String(100), nullable=False)

    # 统计时间
    statistics_date: Mapped[date] = mapped_column(Date, nullable=False)
    statistics_hour: Mapped[Optional[int]] = mapped_column(Integer)

    # 爬取统计
    total_requests: Mapped[int] = mapped_column(Integer, default=0)
    successful_requests: Mapped[int] = mapped_column(Integer, default=0)
    failed_requests: Mapped[int] = mapped_column(Integer, default=0)

    # 数据统计
    items_scraped: Mapped[int] = mapped_column(Integer, default=0)
    items_dropped: Mapped[int] = mapped_column(Integer, default=0)
    duplicate_items: Mapped[int] = mapped_column(Integer, default=0)

    # 性能统计
    avg_response_time: Mapped[Optional[float]] = mapped_column(Float)
    min_response_time: Mapped[Optional[float]] = mapped_column(Float)
    max_response_time: Mapped[Optional[float]] = mapped_column(Float)

    # 错误统计
    error_types: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSONB)

    # 审计字段
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        UniqueConstraint("spider_name", "statistics_date", "statistics_hour"),
        Index("idx_crawler_stats_spider_date", "spider_name", "statistics_date"),
    )


class ProxyStatistics(Base):
    """代理统计模型"""

    __tablename__ = "proxy_statistics"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    proxy_url: Mapped[str] = mapped_column(String(500), nullable=False)

    # 统计时间
    statistics_date: Mapped[date] = mapped_column(Date, nullable=False)

    # 使用统计
    total_requests: Mapped[int] = mapped_column(Integer, default=0)
    successful_requests: Mapped[int] = mapped_column(Integer, default=0)
    failed_requests: Mapped[int] = mapped_column(Integer, default=0)

    # 性能统计
    avg_response_time: Mapped[Optional[float]] = mapped_column(Float)
    success_rate: Mapped[Optional[float]] = mapped_column(Float)

    # 代理信息
    proxy_provider: Mapped[Optional[str]] = mapped_column(String(100))
    country: Mapped[Optional[str]] = mapped_column(String(10))
    anonymity_level: Mapped[Optional[str]] = mapped_column(String(20))

    # 审计字段
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        UniqueConstraint("proxy_url", "statistics_date"),
        Index("idx_proxy_stats_date", "statistics_date"),
        Index("idx_proxy_stats_url", "proxy_url"),
    )


class CrawlerTask(Base):
    """爬虫任务模型"""

    __tablename__ = "crawler_tasks"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    task_id: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)
    spider_name: Mapped[str] = mapped_column(String(100), nullable=False)

    # 任务信息
    url: Mapped[str] = mapped_column(String(1000), nullable=False)
    priority: Mapped[int] = mapped_column(Integer, default=0)
    task_type: Mapped[str] = mapped_column(String(50), default="crawl")

    # 任务状态
    status: Mapped[str] = mapped_column(String(20), default="pending")
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    max_retries: Mapped[int] = mapped_column(Integer, default=3)

    # 配置信息
    task_config: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSONB)

    # 时间信息
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    scheduled_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    # 结果信息
    result: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSONB)
    error_message: Mapped[Optional[str]] = mapped_column(Text)

    # 审计字段
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
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
        try:
            from crawler.monitoring.db_instrumentation import instrument_sqlalchemy_engine

            instrument_sqlalchemy_engine(self.engine, db="postgres")
        except Exception:
            pass
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)

    def create_tables(self):
        """创建所有表"""
        Base.metadata.create_all(bind=self.engine)

    def get_session(self) -> Session:
        """获取数据库会话"""
        return self.SessionLocal()

    def close(self):
        """关闭数据库连接"""
        self.engine.dispose()
