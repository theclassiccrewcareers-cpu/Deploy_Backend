import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    jwt_secret: str = os.getenv("RBAC_JWT_SECRET", os.getenv("JWT_SECRET", "change-me-in-production"))
    jwt_algorithm: str = os.getenv("RBAC_JWT_ALGORITHM", "HS256")
    jwt_exp_minutes: int = int(os.getenv("RBAC_JWT_EXP_MINUTES", "60"))
    otp_exp_minutes: int = int(os.getenv("RBAC_OTP_EXP_MINUTES", "10"))
    root_admin_email: str = os.getenv("ROOT_ADMIN_EMAIL", "")
    smtp_host: str = os.getenv("SMTP_SERVER", "smtp.gmail.com")
    smtp_port: int = int(os.getenv("SMTP_PORT", "587"))
    smtp_username: str = os.getenv("SMTP_EMAIL", "")
    smtp_password: str = os.getenv("SMTP_PASSWORD", "")


settings = Settings()
