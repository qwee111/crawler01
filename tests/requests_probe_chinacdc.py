import argparse
import re
import sys
import time
from typing import Dict, List

import requests

ANTI_PATTERNS = [
    r"captcha|验证码|recaptcha|hcaptcha|geetest",
    r"__jsl_clearance|jsl_.*challenge|anti.*bot|cloudflare|ddos|challenge",
    r"too\s*many\s*requests|rate\s*limit|访问频率|请求过于频繁|429",
    r"forbidden|access\s*denied|banned|blocked|403|451|非法访问|禁止访问|访问被拒绝",
    r"enable\s*javascript|请.*启用.*javascript",
    r"enable\s*cookie|cookie.*disabled|请.*启用.*cookie",
]

TEXT_CT_HINTS = [
    "text/",
    "html",
    "xml",
    "json",
    "javascript",
    "xhtml",
]


def _looks_like_html(sample_text: str) -> bool:
    t = sample_text.lower()
    return ("<html" in t) or ("<!doctype" in t) or ("<head" in t and "<body" in t)


def _best_effort_decode(data: bytes) -> str:
    for enc in ("utf-8", "gbk", "gb2312", "latin-1"):
        try:
            return data.decode(enc, errors="ignore")
        except Exception:
            continue
    return ""


def probe_url(url: str, with_br: bool = False, timeout: int = 15) -> Dict:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br" if with_br else "gzip, deflate",
        "Connection": "keep-alive",
        "DNT": "1",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-User": "?1",
        "Sec-Fetch-Dest": "document",
        "Referer": "https://www.baidu.com/",
    }

    s = requests.Session()
    s.headers.update(headers)

    started = time.time()
    try:
        r = s.get(url, timeout=timeout, allow_redirects=True, verify=True)
        elapsed_ms = int((time.time() - started) * 1000)
    except requests.RequestException as e:
        return {
            "ok": False,
            "error": str(e),
            "elapsed_ms": int((time.time() - started) * 1000),
        }

    ct = r.headers.get("Content-Type", "")
    ct_lower = ct.lower()
    server = r.headers.get("Server", "")

    # 判断是否文本
    is_text_by_ct = any(hint in ct_lower for hint in TEXT_CT_HINTS)

    text_content: str = ""
    mislabelled_html = False

    if is_text_by_ct:
        # 使用 requests 的自动解码
        try:
            text_content = r.text or ""
        except Exception:
            text_content = _best_effort_decode(r.content)
    else:
        # 尝试从字节采样判断是不是被错标的 HTML
        sample = r.content[:4096]
        sample_text = _best_effort_decode(sample)
        if _looks_like_html(sample_text):
            mislabelled_html = True
            text_content = _best_effort_decode(r.content)

    # 反爬信号检测（仅在有文本时扫描）
    anti_signals: List[str] = []
    content_for_scan = (text_content or "").lower()
    if content_for_scan:
        for p in ANTI_PATTERNS:
            if re.search(p, content_for_scan, re.IGNORECASE):
                anti_signals.append(p)

    # 结合状态码的信号
    status_based_signals: List[str] = []
    if r.status_code in (403, 412, 429):
        status_based_signals.append(f"http_{r.status_code}")

    return {
        "ok": (r.status_code < 400),
        "status": r.status_code,
        "url_final": r.url,
        "content_type": ct,
        "server": server,
        "content_length": len(r.content),
        "is_text": is_text_by_ct or mislabelled_html,
        "mislabelled_html": mislabelled_html,
        "anti_signals": anti_signals,
        "status_signals": status_based_signals,
        "elapsed_ms": elapsed_ms,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Probe accessibility and anti-crawl signals for a URL (chinacdc)."
    )
    parser.add_argument(
        "--url",
        default="https://www.chinacdc.cn/jksj/jksj01/",
        help="Target URL to test",
    )
    parser.add_argument(
        "--with-br",
        action="store_true",
        help="Send Accept-Encoding including br (brotli)",
    )
    args = parser.parse_args()

    result = probe_url(args.url, with_br=args.with_br)

    print("==== Probe Result ====")
    for k in [
        "ok",
        "status",
        "url_final",
        "content_type",
        "server",
        "content_length",
        "is_text",
        "mislabelled_html",
        "elapsed_ms",
    ]:
        print(f"{k}: {result.get(k)}")

    print("anti_signals:", result.get("anti_signals"))
    print("status_signals:", result.get("status_signals"))

    # 简要结论
    if not result.get("ok"):
        print("Conclusion: 不可访问或返回错误状态，可能存在访问限制/反爬。")
    else:
        if result.get("anti_signals") or result.get("status_signals"):
            print("Conclusion: 可访问，但检测到反爬迹象（关键字/状态码）。")
        else:
            print("Conclusion: 可访问，未发现明显反爬特征。")


if __name__ == "__main__":
    sys.exit(main())
