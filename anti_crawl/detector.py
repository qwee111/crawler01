# -*- coding: utf-8 -*-
"""
反爬虫检测模块

检测各种反爬虫机制并提供应对策略
"""

import json
import logging
import re
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

logger = logging.getLogger(__name__)


class AntiCrawlDetector:
    """反爬虫检测器"""

    def __init__(self):
        self.detection_rules = {
            "captcha": self._detect_captcha,
            "js_challenge": self._detect_js_challenge,
            "rate_limit": self._detect_rate_limit,
            "ip_block": self._detect_ip_block,
            "user_agent_check": self._detect_user_agent_check,
            "cookie_check": self._detect_cookie_check,
            "referer_check": self._detect_referer_check,
            "fingerprint": self._detect_fingerprint,
            "honeypot": self._detect_honeypot,
            "behavior_analysis": self._detect_behavior_analysis,
        }

        # 检测模式
        self.captcha_patterns = [
            r"captcha",
            r"验证码",
            r"recaptcha",
            r"hcaptcha",
            r"geetest",
            r"slider.*verify",
            r"puzzle.*verify",
        ]

        self.js_challenge_patterns = [
            r"challenge.*js",
            r"anti.*bot",
            r"protection.*mode",
            r"cloudflare",
            r"ddos.*guard",
            r"bot.*detection",
        ]

        self.rate_limit_patterns = [
            r"rate.*limit",
            r"too.*many.*requests",
            r"请求过于频繁",
            r"访问频率",
            r"429",
        ]

        logger.info("反爬虫检测器初始化完成")

    def detect(self, response, request=None) -> Dict[str, any]:
        """检测反爬虫机制"""
        results = {
            "detected": [],
            "confidence": {},
            "suggestions": [],
            "response_analysis": self._analyze_response(response),
        }

        # 运行所有检测规则
        for rule_name, rule_func in self.detection_rules.items():
            try:
                detection_result = rule_func(response, request)
                if detection_result["detected"]:
                    results["detected"].append(rule_name)
                    results["confidence"][rule_name] = detection_result["confidence"]
                    results["suggestions"].extend(detection_result["suggestions"])

            except Exception as e:
                logger.error(f"检测规则 {rule_name} 执行失败: {e}")

        # 生成综合建议
        if results["detected"]:
            results["suggestions"].extend(
                self._generate_comprehensive_suggestions(results)
            )

        return results

    def _analyze_response(self, response) -> Dict:
        """分析响应基本信息"""
        # 安全地获取body长度
        body_length = 0
        if hasattr(response, "body"):
            if isinstance(response.body, bytes):
                body_length = len(response.body)
            elif isinstance(response.body, str):
                body_length = len(response.body.encode("utf-8"))

        # 安全地获取headers
        def safe_header_get(headers, key, default=""):
            value = headers.get(key, default)
            if isinstance(value, bytes):
                return value.decode("utf-8", errors="ignore")
            return str(value)

        return {
            "status_code": getattr(response, "status", 200),
            "content_length": body_length,
            "content_type": safe_header_get(response.headers, "Content-Type"),
            "server": safe_header_get(response.headers, "Server"),
            "has_javascript": "javascript" in response.text.lower(),
            "has_forms": "<form" in response.text.lower(),
            "title": self._extract_title(response),
        }

    def _extract_title(self, response) -> str:
        """提取页面标题"""
        try:
            title_match = re.search(
                r"<title[^>]*>(.*?)</title>", response.text, re.IGNORECASE | re.DOTALL
            )
            return title_match.group(1).strip() if title_match else ""
        except:
            return ""

    def _detect_captcha(self, response, request=None) -> Dict:
        """检测验证码"""
        content = response.text.lower()
        confidence = 0
        suggestions = []

        # 检测验证码关键词
        for pattern in self.captcha_patterns:
            if re.search(pattern, content, re.IGNORECASE):
                confidence += 0.3

        # 检测验证码相关元素
        captcha_elements = [
            r"<img[^>]*captcha",
            r"<canvas[^>]*captcha",
            r"<div[^>]*captcha",
            r"data-sitekey",
            r"g-recaptcha",
        ]

        for element in captcha_elements:
            if re.search(element, content, re.IGNORECASE):
                confidence += 0.4

        detected = confidence > 0.5

        if detected:
            suggestions.extend(
                [
                    "使用OCR识别验证码",
                    "集成第三方验证码识别服务",
                    "使用Selenium模拟人工操作",
                    "尝试绕过验证码页面",
                ]
            )

        return {
            "detected": detected,
            "confidence": min(confidence, 1.0),
            "suggestions": suggestions,
        }

    def _detect_js_challenge(self, response, request=None) -> Dict:
        """检测JavaScript挑战"""
        content = response.text.lower()
        confidence = 0
        suggestions = []

        # 检测JS挑战关键词
        for pattern in self.js_challenge_patterns:
            if re.search(pattern, content, re.IGNORECASE):
                confidence += 0.3

        # 检测特定的JS挑战代码
        js_challenge_codes = [
            r"eval\s*\(",
            r"document\.write\s*\(",
            r"setTimeout.*location",
            r"challenge.*solve",
            r"anti.*automation",
        ]

        for code in js_challenge_codes:
            if re.search(code, content, re.IGNORECASE):
                confidence += 0.2

        # 检测页面是否几乎为空但有大量JS
        if len(response.text.strip()) < 1000 and content.count("<script") > 3:
            confidence += 0.4

        detected = confidence > 0.5

        if detected:
            suggestions.extend(
                [
                    "使用Selenium执行JavaScript",
                    "分析JS代码逻辑并模拟执行",
                    "使用无头浏览器",
                    "尝试直接访问API接口",
                ]
            )

        return {
            "detected": detected,
            "confidence": min(confidence, 1.0),
            "suggestions": suggestions,
        }

    def _detect_rate_limit(self, response, request=None) -> Dict:
        """检测频率限制"""
        content = response.text.lower()
        confidence = 0
        suggestions = []

        # 检测状态码
        if response.status == 429:
            confidence += 0.8
        elif response.status == 412:  # 前置条件失败，常见的反爬虫状态码
            confidence += 0.7
        elif response.status in [503, 502, 504]:
            confidence += 0.3

        # 检测频率限制关键词
        for pattern in self.rate_limit_patterns:
            if re.search(pattern, content, re.IGNORECASE):
                confidence += 0.4

        # 检测Retry-After头
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            confidence += 0.5
            suggestions.append(f"等待 {retry_after} 秒后重试")

        detected = confidence > 0.5

        if detected:
            suggestions.extend(
                [
                    "增加请求间隔",
                    "使用代理池轮换IP",
                    "降低并发数",
                    "实现指数退避重试",
                ]
            )

        return {
            "detected": detected,
            "confidence": min(confidence, 1.0),
            "suggestions": suggestions,
        }

    def _detect_ip_block(self, response, request=None) -> Dict:
        """检测IP封禁"""
        confidence = 0
        suggestions = []

        # 检测状态码
        if response.status in [403, 451]:
            confidence += 0.4

        # 检测封禁关键词
        block_patterns = [
            r"ip.*block",
            r"access.*denied",
            r"forbidden",
            r"banned",
            r"blocked",
            r"ip.*禁止",
            r"访问被拒绝",
        ]

        content = response.text.lower()
        for pattern in block_patterns:
            if re.search(pattern, content, re.IGNORECASE):
                confidence += 0.3

        detected = confidence > 0.5

        if detected:
            suggestions.extend(
                [
                    "更换代理IP",
                    "使用住宅代理",
                    "降低请求频率",
                    "等待一段时间后重试",
                ]
            )

        return {
            "detected": detected,
            "confidence": min(confidence, 1.0),
            "suggestions": suggestions,
        }

    def _detect_user_agent_check(self, response, request=None) -> Dict:
        """检测User-Agent检查"""
        content = response.text.lower()
        confidence = 0
        suggestions = []

        # 检测User-Agent相关错误
        ua_patterns = [
            r"user.*agent.*invalid",
            r"browser.*not.*supported",
            r"please.*update.*browser",
            r"浏览器不支持",
            r"请更新浏览器",
        ]

        for pattern in ua_patterns:
            if re.search(pattern, content, re.IGNORECASE):
                confidence += 0.4

        # 检测是否返回了浏览器升级页面
        if "upgrade" in content and "browser" in content:
            confidence += 0.3

        detected = confidence > 0.5

        if detected:
            suggestions.extend(
                [
                    "使用真实浏览器User-Agent",
                    "随机轮换User-Agent",
                    "模拟最新版本浏览器",
                ]
            )

        return {
            "detected": detected,
            "confidence": min(confidence, 1.0),
            "suggestions": suggestions,
        }

    def _detect_cookie_check(self, response, request=None) -> Dict:
        """检测Cookie检查"""
        content = response.text.lower()
        confidence = 0
        suggestions = []

        # 检测Cookie相关提示
        cookie_patterns = [
            r"enable.*cookie",
            r"cookie.*disabled",
            r"please.*enable.*cookie",
            r"需要启用cookie",
            r"请启用cookie",
        ]

        for pattern in cookie_patterns:
            if re.search(pattern, content, re.IGNORECASE):
                confidence += 0.4

        # 检测Set-Cookie头
        set_cookie = response.headers.get("Set-Cookie")
        if set_cookie and b"test" in set_cookie.lower():
            confidence += 0.3

        detected = confidence > 0.5

        if detected:
            suggestions.extend(
                [
                    "启用Cookie支持",
                    "保持会话状态",
                    "模拟浏览器Cookie行为",
                ]
            )

        return {
            "detected": detected,
            "confidence": min(confidence, 1.0),
            "suggestions": suggestions,
        }

    def _detect_referer_check(self, response, request=None) -> Dict:
        """检测Referer检查"""
        content = response.text.lower()
        confidence = 0
        suggestions = []

        # 检测Referer相关错误
        referer_patterns = [
            r"invalid.*referer",
            r"referer.*required",
            r"direct.*access.*not.*allowed",
            r"非法来源",
            r"需要来源页面",
        ]

        for pattern in referer_patterns:
            if re.search(pattern, content, re.IGNORECASE):
                confidence += 0.4

        detected = confidence > 0.5

        if detected:
            suggestions.extend(
                [
                    "设置正确的Referer头",
                    "模拟从首页访问",
                    "构建完整的访问路径",
                ]
            )

        return {
            "detected": detected,
            "confidence": min(confidence, 1.0),
            "suggestions": suggestions,
        }

    def _detect_fingerprint(self, response, request=None) -> Dict:
        """检测浏览器指纹识别"""
        content = response.text.lower()
        confidence = 0
        suggestions = []

        # 检测指纹识别相关代码
        fingerprint_patterns = [
            r"fingerprint",
            r"canvas.*fingerprint",
            r"webgl.*fingerprint",
            r"audio.*fingerprint",
            r"screen\.width",
            r"navigator\.platform",
            r"navigator\.plugins",
        ]

        for pattern in fingerprint_patterns:
            if re.search(pattern, content, re.IGNORECASE):
                confidence += 0.2

        detected = confidence > 0.4

        if detected:
            suggestions.extend(
                [
                    "使用Selenium模拟真实浏览器",
                    "随机化浏览器特征",
                    "使用指纹伪造工具",
                ]
            )

        return {
            "detected": detected,
            "confidence": min(confidence, 1.0),
            "suggestions": suggestions,
        }

    def _detect_honeypot(self, response, request=None) -> Dict:
        """检测蜜罐陷阱"""
        content = response.text
        confidence = 0
        suggestions = []

        # 检测隐藏链接
        hidden_link_patterns = [
            r"<a[^>]*style[^>]*display\s*:\s*none",
            r"<a[^>]*style[^>]*visibility\s*:\s*hidden",
            r"<a[^>]*class[^>]*hidden",
        ]

        for pattern in hidden_link_patterns:
            if re.search(pattern, content, re.IGNORECASE):
                confidence += 0.3

        # 检测robots.txt陷阱
        if request and "robots.txt" in request.url:
            if "disallow" in content.lower():
                confidence += 0.2

        detected = confidence > 0.3

        if detected:
            suggestions.extend(
                [
                    "避免访问隐藏链接",
                    "遵守robots.txt规则",
                    "只爬取可见内容",
                ]
            )

        return {
            "detected": detected,
            "confidence": min(confidence, 1.0),
            "suggestions": suggestions,
        }

    def _detect_behavior_analysis(self, response, request=None) -> Dict:
        """检测行为分析"""
        content = response.text.lower()
        confidence = 0
        suggestions = []

        # 检测行为分析相关代码
        behavior_patterns = [
            r"mouse.*track",
            r"click.*track",
            r"scroll.*track",
            r"behavior.*analysis",
            r"user.*behavior",
            r"interaction.*track",
        ]

        for pattern in behavior_patterns:
            if re.search(pattern, content, re.IGNORECASE):
                confidence += 0.2

        detected = confidence > 0.3

        if detected:
            suggestions.extend(
                [
                    "模拟人类行为模式",
                    "添加随机延迟",
                    "模拟鼠标移动和点击",
                    "使用Selenium模拟真实操作",
                ]
            )

        return {
            "detected": detected,
            "confidence": min(confidence, 1.0),
            "suggestions": suggestions,
        }

    def _generate_comprehensive_suggestions(self, results) -> List[str]:
        """生成综合建议"""
        suggestions = []
        detected = results["detected"]

        if len(detected) > 3:
            suggestions.append("网站有多重反爬虫机制，建议使用综合解决方案")

        if "captcha" in detected and "js_challenge" in detected:
            suggestions.append("建议使用Selenium + 验证码识别服务")

        if "rate_limit" in detected and "ip_block" in detected:
            suggestions.append("建议使用代理池 + 请求频率控制")

        return suggestions


class AntiCrawlStrategy:
    """反爬虫应对策略"""

    def __init__(self):
        self.strategies = {
            "captcha": self._handle_captcha,
            "js_challenge": self._handle_js_challenge,
            "rate_limit": self._handle_rate_limit,
            "ip_block": self._handle_ip_block,
            "user_agent_check": self._handle_user_agent,
            "cookie_check": self._handle_cookie,
            "referer_check": self._handle_referer,
            "fingerprint": self._handle_fingerprint,
            "honeypot": self._handle_honeypot,
            "behavior_analysis": self._handle_behavior,
        }

    def apply_strategy(self, detection_result: Dict, request, spider) -> Dict:
        """应用应对策略"""
        strategies_applied = []

        for detected_type in detection_result["detected"]:
            if detected_type in self.strategies:
                strategy_result = self.strategies[detected_type](
                    detection_result, request, spider
                )
                strategies_applied.append(
                    {"type": detected_type, "result": strategy_result}
                )

        return {
            "strategies_applied": strategies_applied,
            "success": len(strategies_applied) > 0,
        }

    def _handle_captcha(self, detection_result, request, spider):
        """处理验证码"""
        # 这里可以集成验证码识别服务
        return {"action": "captcha_recognition", "recommendation": "使用OCR或第三方验证码识别服务"}

    def _handle_js_challenge(self, detection_result, request, spider):
        """处理JavaScript挑战"""
        return {"action": "use_selenium", "recommendation": "使用Selenium执行JavaScript"}

    def _handle_rate_limit(self, detection_result, request, spider):
        """处理频率限制"""
        return {"action": "delay_request", "recommendation": "增加请求延迟，使用代理轮换"}

    def _handle_ip_block(self, detection_result, request, spider):
        """处理IP封禁"""
        return {"action": "change_proxy", "recommendation": "更换代理IP"}

    def _handle_user_agent(self, detection_result, request, spider):
        """处理User-Agent检查"""
        return {"action": "update_user_agent", "recommendation": "使用真实浏览器User-Agent"}

    def _handle_cookie(self, detection_result, request, spider):
        """处理Cookie检查"""
        return {"action": "enable_cookies", "recommendation": "启用Cookie支持"}

    def _handle_referer(self, detection_result, request, spider):
        """处理Referer检查"""
        return {"action": "set_referer", "recommendation": "设置正确的Referer头"}

    def _handle_fingerprint(self, detection_result, request, spider):
        """处理指纹识别"""
        return {"action": "randomize_fingerprint", "recommendation": "随机化浏览器指纹"}

    def _handle_honeypot(self, detection_result, request, spider):
        """处理蜜罐陷阱"""
        return {"action": "avoid_honeypot", "recommendation": "避免访问隐藏链接"}

    def _handle_behavior(self, detection_result, request, spider):
        """处理行为分析"""
        return {"action": "simulate_human_behavior", "recommendation": "模拟人类行为模式"}
