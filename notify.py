from __future__ import annotations

import os
import smtplib
from email.mime.text import MIMEText
from email.header import Header
from typing import Optional


def send_email(
    smtp_host: str,
    smtp_port: int,
    user: str,
    password: str,
    to_addr: str,
    subject: str,
    body: str,
) -> None:
    msg = MIMEText(body, "plain", "utf-8")
    msg["From"] = user
    msg["To"] = to_addr
    msg["Subject"] = Header(subject, "utf-8")

    # QQ 邮箱推荐 465 SSL
    with smtplib.SMTP_SSL(smtp_host, smtp_port) as server:
        server.login(user, password)
        server.sendmail(user, [to_addr], msg.as_string())
