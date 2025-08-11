#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Web可视化仪表板

基于Flask的疾病监测可视化仪表板
"""

import json
import logging
import os
import sys
from datetime import datetime, timedelta

from flask import Flask, jsonify, render_template, request

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from analysis.data_analyzer import DiseaseDataAnalyzer

    ANALYZER_AVAILABLE = True
except ImportError:
    ANALYZER_AVAILABLE = False

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 创建Flask应用
app = Flask(__name__)
app.config["SECRET_KEY"] = "disease_monitoring_dashboard_2025"

# 全局分析器实例
analyzer = None
if ANALYZER_AVAILABLE:
    analyzer = DiseaseDataAnalyzer()


@app.route("/")
def index():
    """主页"""
    return render_template("dashboard.html")


@app.route("/api/overview")
def api_overview():
    """获取概览数据"""
    try:
        if not analyzer:
            return jsonify({"error": "分析器不可用"})

        # 运行快速分析
        results = analyzer.run_full_analysis(days_back=7)

        if "error" in results:
            return jsonify({"error": results["error"]})

        # 提取概览数据
        summary = results.get("summary", {}).get("summary", {})
        trends = results.get("trends", {})

        overview = {
            "total_articles": summary.get("total_articles", 0),
            "date_range": summary.get("date_range", {}),
            "top_diseases": summary.get("top_diseases", [])[:5],
            "anomaly_count": summary.get("anomaly_count", 0),
            "daily_trend": list(trends.get("daily_counts", {}).values())[-7:],  # 最近7天
            "status": "normal" if summary.get("anomaly_count", 0) == 0 else "warning",
        }

        return jsonify(overview)

    except Exception as e:
        logger.error(f"获取概览数据失败: {e}")
        return jsonify({"error": str(e)})


@app.route("/api/trends")
def api_trends():
    """获取趋势数据"""
    try:
        days = request.args.get("days", 30, type=int)

        if not analyzer:
            return jsonify({"error": "分析器不可用"})

        results = analyzer.run_full_analysis(days_back=days)

        if "error" in results:
            return jsonify({"error": results["error"]})

        trends = results.get("trends", {})

        # 格式化趋势数据
        trend_data = {
            "daily_counts": trends.get("daily_counts", {}),
            "category_trends": trends.get("category_trends", {}),
            "keyword_frequency": trends.get("keyword_frequency", {}),
            "total_articles": trends.get("total_articles", 0),
        }

        return jsonify(trend_data)

    except Exception as e:
        logger.error(f"获取趋势数据失败: {e}")
        return jsonify({"error": str(e)})


@app.route("/api/distribution")
def api_distribution():
    """获取地区分布数据"""
    try:
        if not analyzer:
            return jsonify({"error": "分析器不可用"})

        results = analyzer.run_full_analysis(days_back=30)

        if "error" in results:
            return jsonify({"error": results["error"]})

        distribution = results.get("distribution", {})

        return jsonify(distribution)

    except Exception as e:
        logger.error(f"获取地区分布数据失败: {e}")
        return jsonify({"error": str(e)})


@app.route("/api/anomalies")
def api_anomalies():
    """获取异常检测数据"""
    try:
        if not analyzer:
            return jsonify({"error": "分析器不可用"})

        results = analyzer.run_full_analysis(days_back=30)

        if "error" in results:
            return jsonify({"error": results["error"]})

        anomalies = results.get("anomalies", {})

        return jsonify(anomalies)

    except Exception as e:
        logger.error(f"获取异常数据失败: {e}")
        return jsonify({"error": str(e)})


@app.route("/api/keywords")
def api_keywords():
    """获取关键词数据"""
    try:
        if not analyzer:
            return jsonify({"error": "分析器不可用"})

        results = analyzer.run_full_analysis(days_back=30)

        if "error" in results:
            return jsonify({"error": results["error"]})

        keywords = results.get("keywords", {})

        return jsonify(keywords)

    except Exception as e:
        logger.error(f"获取关键词数据失败: {e}")
        return jsonify({"error": str(e)})


@app.route("/api/report")
def api_report():
    """生成分析报告"""
    try:
        days = request.args.get("days", 30, type=int)

        if not analyzer:
            return jsonify({"error": "分析器不可用"})

        results = analyzer.run_full_analysis(days_back=days)

        if "error" in results:
            return jsonify({"error": results["error"]})

        # 生成详细报告
        report = {
            "generated_at": datetime.now().isoformat(),
            "analysis_period": f"最近{days}天",
            "summary": results.get("summary", {}),
            "trends": results.get("trends", {}),
            "distribution": results.get("distribution", {}),
            "anomalies": results.get("anomalies", {}),
            "keywords": results.get("keywords", {}),
        }

        return jsonify(report)

    except Exception as e:
        logger.error(f"生成报告失败: {e}")
        return jsonify({"error": str(e)})


@app.route("/health")
def health_check():
    """健康检查"""
    status = {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "analyzer_available": ANALYZER_AVAILABLE,
        "database_connected": False,
    }

    if analyzer:
        try:
            status["database_connected"] = analyzer.connect_database()
        except:
            pass

    return jsonify(status)


def create_templates():
    """创建模板文件"""
    templates_dir = os.path.join(os.path.dirname(__file__), "templates")
    os.makedirs(templates_dir, exist_ok=True)

    # 创建主仪表板模板
    dashboard_html = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>疾病监测仪表板</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/axios/dist/axios.min.js"></script>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: 'Microsoft YaHei', sans-serif; background: #f5f5f5; }
        .header { background: #2c3e50; color: white; padding: 1rem; text-align: center; }
        .container { max-width: 1200px; margin: 0 auto; padding: 2rem; }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 2rem; }
        .card { background: white; border-radius: 8px; padding: 1.5rem; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
        .card h3 { color: #2c3e50; margin-bottom: 1rem; }
        .metric { text-align: center; padding: 1rem; }
        .metric-value { font-size: 2rem; font-weight: bold; color: #3498db; }
        .metric-label { color: #7f8c8d; margin-top: 0.5rem; }
        .status-normal { color: #27ae60; }
        .status-warning { color: #e74c3c; }
        .chart-container { position: relative; height: 300px; }
        .loading { text-align: center; color: #7f8c8d; padding: 2rem; }
        .error { color: #e74c3c; text-align: center; padding: 1rem; }
    </style>
</head>
<body>
    <div class="header">
        <h1>🦠 疾病监测仪表板</h1>
        <p>北京市疾病预防控制中心数据分析</p>
    </div>

    <div class="container">
        <!-- 概览指标 -->
        <div class="grid">
            <div class="card">
                <h3>📊 数据概览</h3>
                <div id="overview-content" class="loading">加载中...</div>
            </div>

            <div class="card">
                <h3>📈 趋势分析</h3>
                <div class="chart-container">
                    <canvas id="trendChart"></canvas>
                </div>
            </div>

            <div class="card">
                <h3>🗺️ 地区分布</h3>
                <div class="chart-container">
                    <canvas id="regionChart"></canvas>
                </div>
            </div>

            <div class="card">
                <h3>🔍 关键词分析</h3>
                <div class="chart-container">
                    <canvas id="keywordChart"></canvas>
                </div>
            </div>

            <div class="card">
                <h3>⚠️ 异常检测</h3>
                <div id="anomaly-content" class="loading">加载中...</div>
            </div>

            <div class="card">
                <h3>📋 系统状态</h3>
                <div id="health-content" class="loading">加载中...</div>
            </div>
        </div>
    </div>

    <script>
        // 全局变量
        let trendChart, regionChart, keywordChart;

        // 初始化
        document.addEventListener('DOMContentLoaded', function() {
            loadOverview();
            loadTrends();
            loadDistribution();
            loadKeywords();
            loadAnomalies();
            loadHealth();

            // 定时刷新
            setInterval(loadOverview, 60000); // 每分钟刷新概览
            setInterval(loadHealth, 30000);   // 每30秒检查健康状态
        });

        // 加载概览数据
        async function loadOverview() {
            try {
                const response = await axios.get('/api/overview');
                const data = response.data;

                if (data.error) {
                    document.getElementById('overview-content').innerHTML =
                        `<div class="error">错误: ${data.error}</div>`;
                    return;
                }

                const statusClass = data.status === 'normal' ? 'status-normal' : 'status-warning';
                const statusText = data.status === 'normal' ? '正常' : '警告';

                document.getElementById('overview-content').innerHTML = `
                    <div class="metric">
                        <div class="metric-value">${data.total_articles}</div>
                        <div class="metric-label">总文章数</div>
                    </div>
                    <div class="metric">
                        <div class="metric-value ${statusClass}">${statusText}</div>
                        <div class="metric-label">系统状态</div>
                    </div>
                    <div class="metric">
                        <div class="metric-value">${data.anomaly_count}</div>
                        <div class="metric-label">异常数量</div>
                    </div>
                `;
            } catch (error) {
                document.getElementById('overview-content').innerHTML =
                    `<div class="error">加载失败: ${error.message}</div>`;
            }
        }

        // 加载趋势数据
        async function loadTrends() {
            try {
                const response = await axios.get('/api/trends?days=7');
                const data = response.data;

                if (data.error) return;

                const ctx = document.getElementById('trendChart').getContext('2d');
                const dates = Object.keys(data.daily_counts).sort();
                const counts = dates.map(date => data.daily_counts[date]);

                if (trendChart) trendChart.destroy();

                trendChart = new Chart(ctx, {
                    type: 'line',
                    data: {
                        labels: dates,
                        datasets: [{
                            label: '每日文章数',
                            data: counts,
                            borderColor: '#3498db',
                            backgroundColor: 'rgba(52, 152, 219, 0.1)',
                            tension: 0.4
                        }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        scales: {
                            y: { beginAtZero: true }
                        }
                    }
                });
            } catch (error) {
                console.error('加载趋势数据失败:', error);
            }
        }

        // 加载地区分布
        async function loadDistribution() {
            try {
                const response = await axios.get('/api/distribution');
                const data = response.data;

                if (data.error) return;

                const ctx = document.getElementById('regionChart').getContext('2d');
                const regions = Object.keys(data.region_counts);
                const counts = Object.values(data.region_counts);

                if (regionChart) regionChart.destroy();

                regionChart = new Chart(ctx, {
                    type: 'doughnut',
                    data: {
                        labels: regions,
                        datasets: [{
                            data: counts,
                            backgroundColor: [
                                '#3498db', '#e74c3c', '#2ecc71', '#f39c12',
                                '#9b59b6', '#1abc9c', '#34495e', '#e67e22'
                            ]
                        }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false
                    }
                });
            } catch (error) {
                console.error('加载地区分布失败:', error);
            }
        }

        // 加载关键词
        async function loadKeywords() {
            try {
                const response = await axios.get('/api/keywords');
                const data = response.data;

                if (data.error) return;

                const ctx = document.getElementById('keywordChart').getContext('2d');
                const keywords = Object.keys(data.top_keywords).slice(0, 10);
                const frequencies = keywords.map(k => data.top_keywords[k]);

                if (keywordChart) keywordChart.destroy();

                keywordChart = new Chart(ctx, {
                    type: 'bar',
                    data: {
                        labels: keywords,
                        datasets: [{
                            label: '出现频次',
                            data: frequencies,
                            backgroundColor: '#2ecc71'
                        }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        scales: {
                            y: { beginAtZero: true }
                        }
                    }
                });
            } catch (error) {
                console.error('加载关键词失败:', error);
            }
        }

        // 加载异常数据
        async function loadAnomalies() {
            try {
                const response = await axios.get('/api/anomalies');
                const data = response.data;

                if (data.error) {
                    document.getElementById('anomaly-content').innerHTML =
                        `<div class="error">错误: ${data.error}</div>`;
                    return;
                }

                const dailyAnomalies = data.daily_anomalies || [];
                const keywordAnomalies = data.keyword_anomalies || [];

                let content = '';
                if (dailyAnomalies.length === 0 && keywordAnomalies.length === 0) {
                    content = '<div class="status-normal">✅ 未检测到异常</div>';
                } else {
                    content = `
                        <div>日期异常: ${dailyAnomalies.length} 个</div>
                        <div>关键词异常: ${keywordAnomalies.length} 个</div>
                    `;
                }

                document.getElementById('anomaly-content').innerHTML = content;
            } catch (error) {
                document.getElementById('anomaly-content').innerHTML =
                    `<div class="error">加载失败: ${error.message}</div>`;
            }
        }

        // 加载健康状态
        async function loadHealth() {
            try {
                const response = await axios.get('/health');
                const data = response.data;

                const statusClass = data.status === 'healthy' ? 'status-normal' : 'status-warning';
                const dbStatus = data.database_connected ? '✅ 已连接' : '❌ 未连接';

                document.getElementById('health-content').innerHTML = `
                    <div class="metric">
                        <div class="metric-value ${statusClass}">${data.status}</div>
                        <div class="metric-label">系统状态</div>
                    </div>
                    <div class="metric">
                        <div class="metric-label">数据库: ${dbStatus}</div>
                        <div class="metric-label">分析器: ${data.analyzer_available ? '✅' : '❌'}</div>
                    </div>
                `;
            } catch (error) {
                document.getElementById('health-content').innerHTML =
                    `<div class="error">检查失败: ${error.message}</div>`;
            }
        }
    </script>
</body>
</html>"""

    with open(
        os.path.join(templates_dir, "dashboard.html"), "w", encoding="utf-8"
    ) as f:
        f.write(dashboard_html)

    logger.info("模板文件创建完成")


def main():
    """主函数"""
    print("🌐 启动疾病监测仪表板")
    print("=" * 60)

    # 创建模板文件
    create_templates()

    if not ANALYZER_AVAILABLE:
        print("⚠️ 警告: 数据分析器不可用，部分功能可能受限")

    print("🚀 启动Web服务器...")
    print("📊 访问地址: http://localhost:5000")
    print("💡 按 Ctrl+C 停止服务")

    try:
        app.run(host="0.0.0.0", port=5000, debug=True)
    except KeyboardInterrupt:
        print("\n👋 服务已停止")


if __name__ == "__main__":
    main()
