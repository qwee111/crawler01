#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试 chinacdc 网站访问和反爬虫机制检测脚本

用于检测：
1. 网站是否可访问
2. 响应类型和内容
3. 反爬虫机制
4. 压缩编码支持
"""

import json
import re
import time
from urllib.parse import urlparse

import requests


class ChinaCDCTester:
    """中国疾控中心网站测试器"""

    def __init__(self):
        self.test_urls = [
            "https://www.chinacdc.cn/jksj/jksj01/",
            "https://www.chinacdc.cn/jksj/xgbdyq/",
            "https://www.chinacdc.cn/jksj/jksj02/",
            "https://www.chinacdc.cn/jksj/jksj03/",
        ]

        # 不同的请求头配置
        self.headers_configs = {
            "scrapy_default": {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Accept-Encoding": "gzip, deflate, br",
                "DNT": "1",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "User-Agent": "Scrapy/2.11.0 (+https://scrapy.org)",
            },
            "chrome_browser": {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Accept-Encoding": "gzip, deflate, br",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Cache-Control": "max-age=0",
                "sec-ch-ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
                "Upgrade-Insecure-Requests": "1",
            },
            "no_br_encoding": {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Accept-Encoding": "gzip, deflate",  # 移除 br
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            },
        }

    def test_single_url(self, url, headers_name, headers, timeout=30):
        """测试单个URL"""
        print(f"\n{'='*60}")
        print(f"🔍 测试URL: {url}")
        print(f"📋 请求头配置: {headers_name}")
        print(f"{'='*60}")

        try:
            # 发送请求
            start_time = time.time()
            response = requests.get(
                url, headers=headers, timeout=timeout, allow_redirects=True
            )
            response_time = time.time() - start_time

            # 基本信息
            print(f"✅ 请求成功")
            print(f"📊 状态码: {response.status_code}")
            print(f"⏱️ 响应时间: {response_time:.2f}秒")
            print(f"📏 内容长度: {len(response.content)} 字节")

            # 响应头分析
            print(f"\n📋 响应头信息:")
            important_headers = [
                "Content-Type",
                "Content-Encoding",
                "Content-Length",
                "Server",
                "Set-Cookie",
                "Cache-Control",
                "X-Frame-Options",
                "X-Content-Type-Options",
                "Strict-Transport-Security",
            ]

            for header in important_headers:
                value = response.headers.get(header)
                if value:
                    print(f"  {header}: {value}")

            # 重定向信息
            if response.history:
                print(f"\n🔄 重定向历史:")
                for i, resp in enumerate(response.history):
                    print(f"  {i+1}. {resp.status_code} -> {resp.url}")
                print(f"  最终: {response.status_code} -> {response.url}")

            # 内容类型分析
            content_type = response.headers.get("Content-Type", "").lower()
            print(f"\n📄 内容分析:")
            print(f"  Content-Type: {content_type}")

            # 检查是否为文本内容
            is_text = any(
                t in content_type
                for t in [
                    "text/",
                    "application/json",
                    "application/xml",
                    "application/xhtml",
                ]
            )
            print(f"  是否为文本: {'是' if is_text else '否'}")

            if is_text:
                try:
                    text_content = response.text
                    print(f"  文本长度: {len(text_content)} 字符")

                    # 检查页面内容特征
                    self.analyze_page_content(text_content)

                except Exception as e:
                    print(f"  ❌ 文本解码失败: {e}")
                    print(f"  原始内容前100字节: {response.content[:100]}")
            else:
                print(f"  ⚠️ 非文本内容，前100字节: {response.content[:100]}")

            # 反爬虫机制检测
            self.detect_anti_crawl(response)

            return {
                "success": True,
                "status_code": response.status_code,
                "content_type": content_type,
                "is_text": is_text,
                "response_time": response_time,
                "content_length": len(response.content),
                "headers": dict(response.headers),
            }

        except requests.exceptions.Timeout:
            print(f"❌ 请求超时 (>{timeout}秒)")
            return {"success": False, "error": "timeout"}

        except requests.exceptions.ConnectionError as e:
            print(f"❌ 连接错误: {e}")
            return {"success": False, "error": f"connection_error: {e}"}

        except requests.exceptions.RequestException as e:
            print(f"❌ 请求异常: {e}")
            return {"success": False, "error": f"request_error: {e}"}

        except Exception as e:
            print(f"❌ 未知错误: {e}")
            return {"success": False, "error": f"unknown_error: {e}"}

    def analyze_page_content(self, text_content):
        """分析页面内容特征"""
        print(f"\n🔍 页面内容特征:")

        # HTML结构检查
        has_html = bool(re.search(r"<html[^>]*>", text_content, re.IGNORECASE))
        has_head = bool(re.search(r"<head[^>]*>", text_content, re.IGNORECASE))
        has_body = bool(re.search(r"<body[^>]*>", text_content, re.IGNORECASE))

        print(f"  HTML结构: {'完整' if all([has_html, has_head, has_body]) else '不完整'}")

        # 标题提取
        title_match = re.search(
            r"<title[^>]*>(.*?)</title>", text_content, re.IGNORECASE | re.DOTALL
        )
        if title_match:
            title = title_match.group(1).strip()
            print(f"  页面标题: {title[:100]}{'...' if len(title) > 100 else ''}")

        # JavaScript检查
        js_count = len(re.findall(r"<script[^>]*>", text_content, re.IGNORECASE))
        print(f"  JavaScript脚本数量: {js_count}")

        # 链接检查
        link_count = len(re.findall(r"<a[^>]+href[^>]*>", text_content, re.IGNORECASE))
        print(f"  链接数量: {link_count}")

        # 内容预览
        # 移除HTML标签获取纯文本
        clean_text = re.sub(r"<[^>]+>", "", text_content)
        clean_text = re.sub(r"\s+", " ", clean_text).strip()

        if clean_text:
            preview = clean_text[:200] + ("..." if len(clean_text) > 200 else "")
            print(f"  内容预览: {preview}")

    def detect_anti_crawl(self, response):
        """检测反爬虫机制"""
        print(f"\n🛡️ 反爬虫机制检测:")

        detected_mechanisms = []

        # 状态码检查
        if response.status_code in [403, 412, 429, 503]:
            detected_mechanisms.append(f"可疑状态码: {response.status_code}")

        # 响应头检查
        headers = response.headers
        if "cf-ray" in headers or "cloudflare" in str(headers).lower():
            detected_mechanisms.append("Cloudflare防护")

        if "x-rate-limit" in str(headers).lower():
            detected_mechanisms.append("频率限制")

        # 内容检查（如果是文本）
        try:
            content = response.text.lower()

            # 验证码检测
            captcha_patterns = [
                r"captcha",
                r"验证码",
                r"recaptcha",
                r"hcaptcha",
                r"geetest",
                r"slider.*verify",
                r"puzzle.*verify",
            ]
            for pattern in captcha_patterns:
                if re.search(pattern, content):
                    detected_mechanisms.append(f"验证码检测: {pattern}")
                    break

            # JavaScript挑战检测
            js_challenge_patterns = [
                r"challenge.*js",
                r"anti.*bot",
                r"protection.*mode",
                r"ddos.*guard",
                r"bot.*detection",
            ]
            for pattern in js_challenge_patterns:
                if re.search(pattern, content):
                    detected_mechanisms.append(f"JS挑战: {pattern}")
                    break

            # 频率限制检测
            rate_limit_patterns = [
                r"rate.*limit",
                r"too.*many.*requests",
                r"请求过于频繁",
                r"访问频率",
                r"429",
            ]
            for pattern in rate_limit_patterns:
                if re.search(pattern, content):
                    detected_mechanisms.append(f"频率限制: {pattern}")
                    break

            # IP封禁检测
            ip_block_patterns = [
                r"ip.*block",
                r"access.*denied",
                r"forbidden",
                r"banned",
                r"blocked",
                r"ip.*禁止",
                r"访问被拒绝",
            ]
            for pattern in ip_block_patterns:
                if re.search(pattern, content):
                    detected_mechanisms.append(f"IP封禁: {pattern}")
                    break

        except Exception:
            pass  # 非文本内容，跳过内容检测

        if detected_mechanisms:
            print(f"  ⚠️ 检测到反爬虫机制:")
            for mechanism in detected_mechanisms:
                print(f"    - {mechanism}")
        else:
            print(f"  ✅ 未检测到明显的反爬虫机制")

    def run_comprehensive_test(self):
        """运行综合测试"""
        print(f"🚀 开始 chinacdc 网站访问测试")
        print(f"📅 测试时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")

        results = {}

        for url in self.test_urls:
            results[url] = {}

            for headers_name, headers in self.headers_configs.items():
                print(f"\n⏳ 等待1秒避免请求过快...")
                time.sleep(1)

                result = self.test_single_url(url, headers_name, headers)
                results[url][headers_name] = result

        # 生成测试报告
        self.generate_report(results)

        return results

    def generate_report(self, results):
        """生成测试报告"""
        print(f"\n{'='*80}")
        print(f"📊 测试报告汇总")
        print(f"{'='*80}")

        for url in results:
            print(f"\n🌐 URL: {url}")
            url_results = results[url]

            success_count = sum(1 for r in url_results.values() if r.get("success"))
            total_count = len(url_results)

            print(
                f"  成功率: {success_count}/{total_count} ({success_count/total_count*100:.1f}%)"
            )

            for headers_name, result in url_results.items():
                if result.get("success"):
                    status = result.get("status_code")
                    content_type = result.get("content_type", "")[:50]
                    is_text = "文本" if result.get("is_text") else "非文本"
                    time_taken = result.get("response_time", 0)
                    print(
                        f"    {headers_name}: ✅ {status} | {is_text} | {time_taken:.2f}s | {content_type}"
                    )
                else:
                    error = result.get("error", "unknown")
                    print(f"    {headers_name}: ❌ {error}")

        # 保存详细结果到文件
        with open("chinacdc_test_results.json", "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)

        print(f"\n💾 详细结果已保存到: chinacdc_test_results.json")


if __name__ == "__main__":
    tester = ChinaCDCTester()
    results = tester.run_comprehensive_test()
