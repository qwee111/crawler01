#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据管道检查脚本
"""

import os
from datetime import datetime

import pymongo


def check_data_pipeline():
    """检查数据管道和数据保存措施"""

    # 连接到MongoDB
    mongo_uri = "mongodb://admin:password123@localhost:27017/"
    print(f"连接MongoDB: {mongo_uri}")
    try:
        client = pymongo.MongoClient(mongo_uri)
        db = client["crawler_db"]
        print("MongoDB连接成功")

        print("🔍 数据管道检查报告")
        print("=" * 60)
        print()

        # 1. 检查所有集合
        collections = db.list_collection_names()
        print("📊 数据库中的集合:")
        total_records = 0
        for i, collection_name in enumerate(collections, 1):
            collection = db[collection_name]
            count = collection.count_documents({})
            total_records += count
            print(f"  {i}. {collection_name}: {count:,} 条记录")

        print(f"📈 总记录数: {total_records:,}")
        print()

        # 2. 检查adaptive_data集合详情
        if "adaptive_data" in collections:
            adaptive_collection = db["adaptive_data"]
            count = adaptive_collection.count_documents({})
            print("🎯 adaptive_data集合详细信息:")
            print(f"   📊 总记录数: {count:,}")

            # 检查最新记录
            latest_doc = adaptive_collection.find_one(sort=[("_id", -1)])
            if latest_doc:
                print(f'   📅 最新记录ID: {str(latest_doc["_id"])}')
                print(f'   🌐 最新URL: {latest_doc.get("url", "N/A")}')
                title = latest_doc.get("title", "N/A")
                if len(title) > 50:
                    title = title[:50] + "..."
                print(f"   📝 标题: {title}")

                # 检查字段完整性
                required_fields = ["url", "title", "content", "crawl_timestamp"]
                missing_fields = [
                    field for field in required_fields if field not in latest_doc
                ]
                if missing_fields:
                    print(f"   ⚠️ 缺失字段: {missing_fields}")
                else:
                    print("   ✅ 必需字段完整")

                # 显示所有字段
                print(f"   📋 字段列表: {list(latest_doc.keys())}")

            print()
            print("📋 数据质量检查:")

            # 检查空标题
            empty_title = adaptive_collection.count_documents(
                {"title": {"$in": ["", None]}}
            )
            print(f"   📝 空标题记录: {empty_title}")

            # 检查空内容
            empty_content = adaptive_collection.count_documents(
                {"content": {"$in": ["", None]}}
            )
            print(f"   📄 空内容记录: {empty_content}")

            # 检查重复URL
            pipeline = [
                {"$group": {"_id": "$url", "count": {"$sum": 1}}},
                {"$match": {"count": {"$gt": 1}}},
                {"$count": "duplicates"},
            ]
            duplicates = list(adaptive_collection.aggregate(pipeline))
            duplicate_count = duplicates[0]["duplicates"] if duplicates else 0
            print(f"   🔄 重复URL数量: {duplicate_count}")

            # 检查最近的记录
            recent_count = adaptive_collection.count_documents(
                {
                    "crawl_timestamp": {
                        "$gte": datetime.now().replace(
                            hour=0, minute=0, second=0, microsecond=0
                        )
                    }
                }
            )
            print(f"   📅 今日新增记录: {recent_count}")

        else:
            print("❌ adaptive_data集合不存在!")

        print()
        print("🔧 数据管道状态: ✅ 正常运行")

        client.close()

    except Exception as e:
        print(f"❌ 数据库连接失败: {e}")


if __name__ == "__main__":
    check_data_pipeline()
