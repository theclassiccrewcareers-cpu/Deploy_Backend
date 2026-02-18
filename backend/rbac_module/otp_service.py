import secrets
import smtplib
from datetime import datetime, timedelta, timezone
from email.mime.text import MIMEText

from .config import settings


class OtpDispatchError(Exception):
    pass


def generate_otp(length: int = 6) -> str:
    alphabet = "0123456789"
    return "".join(secrets.choice(alphabet) for _ in range(length))


def otp_expiration() -> datetime:
    return datetime.now(timezone.utc) + timedelta(minutes=settings.otp_exp_minutes)


def send_school_otp(*, recipient_email: str, otp: str, sender_email: str, sender_role: str) -> None:
    # Business rule: OTP for school activation must come only from root-level actor email.
    if sender_role not in {"root_admin", "super_admin"}:
        raise OtpDispatchError("Only Root Admin (or Super Admin with Root privileges) can send school OTP")
    if not settings.root_admin_email:
        raise OtpDispatchError("ROOT_ADMIN_EMAIL is not configured")
    if sender_email.lower() != settings.root_admin_email.lower():
        raise OtpDispatchError("OTP sending is restricted to configured ROOT_ADMIN_EMAIL")

    if not settings.smtp_username or not settings.smtp_password:
        raise OtpDispatchError("SMTP credentials are missing")

    body = (
        "Your school activation OTP is: "
        f"{otp}\n\n"
        f"It expires in {settings.otp_exp_minutes} minutes."
    )
    msg = MIMEText(body)
    msg["Subject"] = "School Account Activation OTP"
    msg["From"] = sender_email
    msg["To"] = recipient_email

    try:
        with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=15) as server:
            server.starttls()
            server.login(settings.smtp_username, settings.smtp_password)
            server.sendmail(sender_email, [recipient_email], msg.as_string())
    except Exception as exc:
        raise OtpDispatchError(f"Failed to send OTP email: {exc}") from exc
