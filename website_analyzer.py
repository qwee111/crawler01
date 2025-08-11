#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
网站访问和结构分析工具

先确保能够访问目标网站，获取源码，然后分析页面结构生成提取规则
"""

import json
import logging
import os
import sys
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    from bs4 import BeautifulSoup

    BS4_AVAILABLE = True
except ImportError:
    BS4_AVAILABLE = False
    logger.warning("BeautifulSoup未安装，将跳过HTML分析功能")

try:
    from lxml import html

    LXML_AVAILABLE = True
except ImportError:
    LXML_AVAILABLE = False
    logger.warning("lxml未安装，将跳过XPath分析功能")


class WebsiteAnalyzer:
    """网站分析器"""

    def __init__(self, base_url):
        self.base_url = base_url
        self.domain = urlparse(base_url).netloc
        self.session = requests.Session()
        self.results = {
            "base_url": base_url,
            "domain": self.domain,
            "access_tests": [],
            "page_analysis": {},
            "extraction_rules": {},
            "recommendations": [],
        }

        # 创建输出目录
        self.output_dir = Path(f"analysis_{self.domain.replace('.', '_')}")
        self.output_dir.mkdir(exist_ok=True)

        logger.info(f"初始化网站分析器: {base_url}")

    def test_access_methods(self):
        """测试多种访问方式"""
        logger.info("🌐 测试网站访问方式...")

        # 不同的User-Agent
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        ]

        # 测试URL变体
        test_urls = [
            self.base_url,
            self.base_url.replace("https://", "http://"),
            self.base_url.replace("http://", "https://"),
        ]

        # 去重
        test_urls = list(set(test_urls))

        for url in test_urls:
            for i, ua in enumerate(user_agents):
                test_name = f"{url} (UA{i+1})"
                logger.info(f"   测试: {test_name}")

                try:
                    headers = {
                        "User-Agent": ua,
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                        "Accept-Encoding": "gzip, deflate, br",
                        "Connection": "keep-alive",
                        "Upgrade-Insecure-Requests": "1",
                        "Sec-Fetch-Dest": "document",
                        "Sec-Fetch-Mode": "navigate",
                        "Sec-Fetch-Site": "none",
                        "Cache-Control": "max-age=0",
                    }

                    start_time = time.time()
                    response = self.session.get(
                        url,
                        headers=headers,
                        timeout=30,
                        verify=False,  # 忽略SSL证书问题
                        allow_redirects=True,
                    )
                    end_time = time.time()

                    test_result = {
                        "url": url,
                        "user_agent": ua,
                        "status_code": response.status_code,
                        "response_time": round(end_time - start_time, 2),
                        "content_length": len(response.content),
                        "content_type": response.headers.get("Content-Type", ""),
                        "final_url": response.url,
                        "redirects": len(response.history),
                        "success": response.status_code == 200
                        and len(response.content) > 1000,
                        "error": None,
                    }

                    logger.info(
                        f"     ✅ 状态码: {response.status_code}, 大小: {len(response.content)} 字节, 时间: {test_result['response_time']}秒"
                    )

                    # 如果成功，保存内容
                    if test_result["success"]:
                        self.save_page_content(response, f"success_{i+1}")
                        test_result["content_saved"] = True

                        # 如果是第一次成功，进行详细分析
                        if not any(
                            t.get("analyzed") for t in self.results["access_tests"]
                        ):
                            self.analyze_page_structure(response)
                            test_result["analyzed"] = True

                    self.results["access_tests"].append(test_result)

                    # 如果成功了，可以跳过其他User-Agent测试
                    if test_result["success"]:
                        logger.info(f"   ✅ 访问成功，跳过其他User-Agent测试")
                        return True

                except requests.exceptions.Timeout:
                    error_msg = "请求超时"
                    logger.warning(f"     ⏰ {error_msg}")
                except requests.exceptions.ConnectionError as e:
                    error_msg = f"连接错误: {str(e)[:100]}"
                    logger.warning(f"     🔌 {error_msg}")
                except requests.exceptions.SSLError as e:
                    error_msg = f"SSL错误: {str(e)[:100]}"
                    logger.warning(f"     🔒 {error_msg}")
                except Exception as e:
                    error_msg = f"其他错误: {str(e)[:100]}"
                    logger.warning(f"     ❌ {error_msg}")

                # 添加失败记录
                self.results["access_tests"].append(
                    {"url": url, "user_agent": ua, "success": False, "error": error_msg}
                )

                # 添加延迟避免触发反爬虫
                time.sleep(2)

        return False

    def save_page_content(self, response, suffix=""):
        """保存页面内容"""
        # 保存HTML源码
        html_file = self.output_dir / f"page_source_{suffix}.html"
        with open(html_file, "w", encoding="utf-8") as f:
            f.write(response.text)

        # 保存响应头
        headers_file = self.output_dir / f"response_headers_{suffix}.json"
        with open(headers_file, "w", encoding="utf-8") as f:
            json.dump(dict(response.headers), f, indent=2, ensure_ascii=False)

        logger.info(f"   💾 页面内容已保存: {html_file}")

    def analyze_page_structure(self, response):
        """分析页面结构"""
        logger.info("🔍 分析页面结构...")

        html_content = response.text

        analysis = {
            "url": response.url,
            "title": "",
            "meta_info": {},
            "structure_analysis": {},
            "potential_selectors": {},
        }

        if BS4_AVAILABLE:
            soup = BeautifulSoup(html_content, "html.parser")

            # 基本信息
            if soup.title:
                analysis["title"] = soup.title.get_text().strip()
                logger.info(f"   📝 页面标题: {analysis['title']}")

            # Meta信息
            for meta in soup.find_all("meta"):
                name = (
                    meta.get("name") or meta.get("property") or meta.get("http-equiv")
                )
                content = meta.get("content")
                if name and content:
                    analysis["meta_info"][name] = content

            # 结构分析
            analysis["structure_analysis"] = {
                "total_elements": len(soup.find_all()),
                "div_count": len(soup.find_all("div")),
                "p_count": len(soup.find_all("p")),
                "a_count": len(soup.find_all("a")),
                "img_count": len(soup.find_all("img")),
                "h1_count": len(soup.find_all("h1")),
                "h2_count": len(soup.find_all("h2")),
                "h3_count": len(soup.find_all("h3")),
            }

            # 寻找潜在的内容选择器
            self.find_content_selectors(soup, analysis)

        self.results["page_analysis"] = analysis

        # 保存分析结果
        analysis_file = self.output_dir / "page_analysis.json"
        with open(analysis_file, "w", encoding="utf-8") as f:
            json.dump(analysis, f, indent=2, ensure_ascii=False)

        logger.info(f"   📊 页面分析完成，结果已保存: {analysis_file}")

    def find_content_selectors(self, soup, analysis):
        """寻找内容选择器"""
        logger.info("🎯 寻找潜在的内容选择器...")

        selectors = {}

        # 标题选择器
        title_candidates = []
        for tag in ["h1", "h2", "h3"]:
            elements = soup.find_all(tag)
            for elem in elements:
                text = elem.get_text().strip()
                if len(text) > 5 and len(text) < 200:  # 合理的标题长度
                    selector_info = {
                        "tag": tag,
                        "text": text[:50] + "..." if len(text) > 50 else text,
                        "class": elem.get("class"),
                        "id": elem.get("id"),
                    }
                    title_candidates.append(selector_info)

        selectors["title_candidates"] = title_candidates[:5]  # 只保留前5个

        # 内容选择器
        content_candidates = []
        for class_name in [
            "content",
            "article",
            "main",
            "text",
            "body",
            "article-content",
            "post-content",
        ]:
            elements = soup.find_all(
                attrs={"class": lambda x: x and class_name in " ".join(x).lower()}
            )
            for elem in elements:
                text = elem.get_text().strip()
                if len(text) > 100:  # 内容应该比较长
                    selector_info = {
                        "class": elem.get("class"),
                        "id": elem.get("id"),
                        "tag": elem.name,
                        "text_length": len(text),
                        "text_preview": text[:100] + "..." if len(text) > 100 else text,
                    }
                    content_candidates.append(selector_info)

        selectors["content_candidates"] = content_candidates[:5]

        # 日期选择器
        date_candidates = []
        import re

        date_pattern = r"\d{4}[-/年]\d{1,2}[-/月]\d{1,2}"

        for elem in soup.find_all(text=re.compile(date_pattern)):
            parent = elem.parent
            if parent:
                selector_info = {
                    "tag": parent.name,
                    "class": parent.get("class"),
                    "id": parent.get("id"),
                    "text": elem.strip(),
                }
                date_candidates.append(selector_info)

        selectors["date_candidates"] = date_candidates[:5]

        analysis["potential_selectors"] = selectors

        # 输出发现的选择器
        logger.info(f"   📝 找到 {len(title_candidates)} 个标题候选")
        logger.info(f"   📄 找到 {len(content_candidates)} 个内容候选")
        logger.info(f"   📅 找到 {len(date_candidates)} 个日期候选")

    def generate_extraction_rules(self):
        """生成提取规则"""
        logger.info("⚙️ 生成提取规则...")

        if not self.results["page_analysis"]:
            logger.warning("   ⚠️ 没有页面分析数据，无法生成规则")
            return

        selectors = self.results["page_analysis"].get("potential_selectors", {})
        rules = {}

        # 标题规则
        title_candidates = selectors.get("title_candidates", [])
        if title_candidates:
            title_xpaths = []
            for candidate in title_candidates[:3]:  # 取前3个
                if candidate.get("class"):
                    class_str = " ".join(candidate["class"])
                    title_xpaths.append(
                        f"//{candidate['tag']}[@class='{class_str}']//text()"
                    )
                elif candidate.get("id"):
                    title_xpaths.append(
                        f"//{candidate['tag']}[@id='{candidate['id']}']//text()"
                    )
                else:
                    title_xpaths.append(f"//{candidate['tag']}//text()")

            rules["title"] = {
                "method": "xpath",
                "selector": " | ".join(title_xpaths),
                "type": "string",
                "required": True,
                "description": "页面标题",
            }

        # 内容规则
        content_candidates = selectors.get("content_candidates", [])
        if content_candidates:
            content_xpaths = []
            for candidate in content_candidates[:3]:
                if candidate.get("class"):
                    class_str = " ".join(candidate["class"])
                    content_xpaths.append(f"//div[@class='{class_str}']//text()")
                elif candidate.get("id"):
                    content_xpaths.append(f"//div[@id='{candidate['id']}']//text()")

            if content_xpaths:
                rules["content"] = {
                    "method": "xpath",
                    "selector": " | ".join(content_xpaths),
                    "type": "string",
                    "multiple": True,
                    "required": True,
                    "description": "页面内容",
                }

        # 日期规则
        date_candidates = selectors.get("date_candidates", [])
        if date_candidates:
            date_xpaths = []
            for candidate in date_candidates[:3]:
                if candidate.get("class"):
                    class_str = " ".join(candidate["class"])
                    date_xpaths.append(f"//*[@class='{class_str}']//text()")
                elif candidate.get("id"):
                    date_xpaths.append(f"//*[@id='{candidate['id']}']//text()")

            if date_xpaths:
                rules["publish_date"] = {
                    "method": "xpath",
                    "selector": " | ".join(date_xpaths),
                    "type": "date",
                    "description": "发布日期",
                }

        self.results["extraction_rules"] = rules

        # 保存规则
        rules_file = self.output_dir / "extraction_rules.yaml"
        import yaml

        with open(rules_file, "w", encoding="utf-8") as f:
            yaml.dump(
                {"fields": rules}, f, default_flow_style=False, allow_unicode=True
            )

        logger.info(f"   ✅ 提取规则已生成: {rules_file}")
        logger.info(f"   📊 生成了 {len(rules)} 个字段规则")

    def test_extraction_rules(self):
        """测试提取规则"""
        logger.info("🧪 测试提取规则...")

        if not self.results["extraction_rules"]:
            logger.warning("   ⚠️ 没有提取规则，跳过测试")
            return

        # 获取保存的HTML文件
        html_files = list(self.output_dir.glob("page_source_*.html"))
        if not html_files:
            logger.warning("   ⚠️ 没有找到保存的HTML文件")
            return

        html_file = html_files[0]
        with open(html_file, "r", encoding="utf-8") as f:
            html_content = f.read()

        if LXML_AVAILABLE:
            tree = html.fromstring(html_content)

            test_results = {}
            for field_name, rule in self.results["extraction_rules"].items():
                try:
                    selector = rule["selector"]
                    elements = tree.xpath(selector)

                    if rule.get("multiple"):
                        result = [elem.strip() for elem in elements if elem.strip()]
                    else:
                        result = elements[0].strip() if elements else None

                    test_results[field_name] = {
                        "success": bool(result),
                        "result": result,
                        "count": len(elements)
                        if isinstance(elements, list)
                        else (1 if result else 0),
                    }

                    if result:
                        preview = (
                            str(result)[:100] + "..."
                            if len(str(result)) > 100
                            else str(result)
                        )
                        logger.info(f"   ✅ {field_name}: {preview}")
                    else:
                        logger.warning(f"   ❌ {field_name}: 未提取到数据")

                except Exception as e:
                    test_results[field_name] = {"success": False, "error": str(e)}
                    logger.error(f"   ❌ {field_name}: 提取失败 - {e}")

            # 保存测试结果
            test_file = self.output_dir / "extraction_test_results.json"
            with open(test_file, "w", encoding="utf-8") as f:
                json.dump(test_results, f, indent=2, ensure_ascii=False)

            logger.info(f"   📊 提取测试完成: {test_file}")

    def generate_report(self):
        """生成分析报告"""
        logger.info("📋 生成分析报告...")

        # 统计成功的访问测试
        successful_tests = [t for t in self.results["access_tests"] if t.get("success")]

        report = {
            "website": self.base_url,
            "analysis_time": time.strftime("%Y-%m-%d %H:%M:%S"),
            "summary": {
                "total_access_tests": len(self.results["access_tests"]),
                "successful_tests": len(successful_tests),
                "access_success_rate": len(successful_tests)
                / len(self.results["access_tests"])
                if self.results["access_tests"]
                else 0,
                "page_analyzed": bool(self.results["page_analysis"]),
                "rules_generated": len(self.results["extraction_rules"]),
            },
            "recommendations": [],
        }

        # 生成建议
        if successful_tests:
            report["recommendations"].append("✅ 网站可以正常访问")
            best_test = max(successful_tests, key=lambda x: x.get("content_length", 0))
            report["recommendations"].append(f"🎯 推荐使用的访问方式: {best_test['url']}")
        else:
            report["recommendations"].append("❌ 网站访问失败，需要检查网络连接或反爬虫策略")

        if self.results["extraction_rules"]:
            report["recommendations"].append(
                f"⚙️ 已生成 {len(self.results['extraction_rules'])} 个提取规则"
            )
        else:
            report["recommendations"].append("⚠️ 未能生成提取规则，需要手动分析页面结构")

        # 保存完整报告
        report_file = self.output_dir / "analysis_report.json"
        with open(report_file, "w", encoding="utf-8") as f:
            json.dump(
                {**report, "detailed_results": self.results},
                f,
                indent=2,
                ensure_ascii=False,
            )

        logger.info(f"📊 分析报告已生成: {report_file}")

        # 输出摘要
        print("\n" + "=" * 60)
        print("📋 网站分析报告摘要")
        print("=" * 60)
        print(f"🌐 目标网站: {self.base_url}")
        print(
            f"📊 访问测试: {report['summary']['successful_tests']}/{report['summary']['total_access_tests']} 成功"
        )
        print(f"📄 页面分析: {'✅ 完成' if report['summary']['page_analyzed'] else '❌ 失败'}")
        print(f"⚙️ 提取规则: {report['summary']['rules_generated']} 个")
        print("\n💡 建议:")
        for rec in report["recommendations"]:
            print(f"   {rec}")
        print(f"\n📁 详细结果保存在: {self.output_dir}")

        return report


def main():
    """主函数"""
    # 目标网站
    target_url = "https://www.bjcdc.org/"

    print("🎯 网站访问和结构分析工具")
    print("=" * 60)
    print(f"🌐 目标网站: {target_url}")
    print()

    # 创建分析器
    analyzer = WebsiteAnalyzer(target_url)

    try:
        # 1. 测试访问
        access_success = analyzer.test_access_methods()

        if access_success:
            # 2. 生成提取规则
            analyzer.generate_extraction_rules()

            # 3. 测试提取规则
            analyzer.test_extraction_rules()

        # 4. 生成报告
        analyzer.generate_report()

        return access_success

    except KeyboardInterrupt:
        print("\n⚠️ 用户中断分析")
        return False
    except Exception as e:
        logger.error(f"❌ 分析过程出错: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
