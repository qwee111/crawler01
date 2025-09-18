# -*- coding: utf-8 -*-
"""
告警通知服务：邮件、钉钉、企业微信（占位实现）
- 可由 Prometheus Alertmanager 调用 webhook 触发
- 也可在代码中直接调用 send_alert
"""
from __future__ import annotations

import json
import os
import smtplib
from email.mime.text import MIMEText
from typing import Dict, Optional

import requests


def send_alert(subject: str, content: str, level: str = "warning") -> None:
    if os.getenv("ALERT_EMAIL_SMTP"):
        _send_email(subject, content)
    if os.getenv("ALERT_DINGTALK_WEBHOOK"):
        _send_dingtalk(subject, content, level)
    if os.getenv("ALERT_WECHAT_WEBHOOK"):
        _send_wechat(subject, content, level)


def _send_email(subject: str, content: str) -> None:
    host = os.getenv("ALERT_EMAIL_SMTP")
    port = int(os.getenv("ALERT_EMAIL_PORT", "465"))
    user = os.getenv("ALERT_EMAIL_USER")
    password = os.getenv("ALERT_EMAIL_PASS")
    to = os.getenv("ALERT_EMAIL_TO", "").split(",")

    msg = MIMEText(content, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = user
    msg["To"] = ",".join(to)

    with smtplib.SMTP_SSL(host, port) as server:
        server.login(user, password)
        server.sendmail(user, to, msg.as_string())


def _send_dingtalk(subject: str, content: str, level: str) -> None:
    url = os.getenv("ALERT_DINGTALK_WEBHOOK")
    if not url:
        return
    payload = {"msgtype": "markdown", "markdown": {"title": subject, "text": content}}
    requests.post(url, json=payload, timeout=5)


def _send_wechat(subject: str, content: str, level: str) -> None:
    url = os.getenv("ALERT_WECHAT_WEBHOOK")
    if not url:
        return
    payload = {
        "msgtype": "markdown",
        "markdown": {"content": f"**{subject}**\n{content}"},
    }
    requests.post(url, json=payload, timeout=5)
