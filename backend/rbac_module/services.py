from datetime import datetime, timezone
import re

from fastapi import HTTPException, status
from sqlalchemy.orm import Session

from .models import School, Student, User, UserRole
from .config import settings
from .otp_service import OtpDispatchError, generate_otp, otp_expiration, send_school_otp
from .security import create_access_token, hash_otp, hash_password, verify_otp, verify_password


EMAIL_PATTERN = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")


def _normalize_email(value: str) -> str:
    normalized = value.lower().strip()
    if not EMAIL_PATTERN.match(normalized):
        raise HTTPException(status_code=400, detail="Invalid email format")
    return normalized


def login_user(db: Session, *, email: str, password: str) -> str:
    user = db.query(User).filter(User.email == _normalize_email(email)).first()
    if not user or not user.is_active or not verify_password(password, user.password_hash):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")
    return create_access_token(subject=user.email, role=user.role.value)


def create_student(
    db: Session,
    *,
    full_name: str,
    email: str,
    raw_password: str,
    school_id: int,
    actor_user_id: int,
) -> Student:
    email = _normalize_email(email)
    if db.query(Student).filter(Student.email == email).first():
        raise HTTPException(status_code=409, detail="Student email already exists")
    school = db.query(School).filter(School.id == school_id).first()
    if not school:
        raise HTTPException(status_code=404, detail="School not found")

    student = Student(
        full_name=full_name.strip(),
        email=email,
        password_hash=hash_password(raw_password),
        school_id=school_id,
        created_by_user_id=actor_user_id,
    )
    db.add(student)
    db.commit()
    db.refresh(student)
    return student


def update_student_email(db: Session, *, student_id: int, new_email: str) -> Student:
    student = db.query(Student).filter(Student.id == student_id).first()
    if not student:
        raise HTTPException(status_code=404, detail="Student not found")

    normalized = _normalize_email(new_email)
    exists = db.query(Student).filter(Student.email == normalized, Student.id != student_id).first()
    if exists:
        raise HTTPException(status_code=409, detail="Email already in use")

    student.email = normalized
    db.commit()
    db.refresh(student)
    return student


def update_student_password(db: Session, *, student_id: int, new_password: str) -> Student:
    student = db.query(Student).filter(Student.id == student_id).first()
    if not student:
        raise HTTPException(status_code=404, detail="Student not found")

    student.password_hash = hash_password(new_password)
    db.commit()
    db.refresh(student)
    return student


def create_school_with_otp(
    db: Session,
    *,
    school_name: str,
    school_email: str,
    school_password: str,
    actor: User,
) -> School:
    normalized_email = _normalize_email(school_email)
    if db.query(School).filter(School.email == normalized_email).first():
        raise HTTPException(status_code=409, detail="School email already exists")

    raw_otp = generate_otp()
    expires_at = otp_expiration()

    school = School(
        name=school_name.strip(),
        email=normalized_email,
        password_hash=hash_password(school_password),
        is_active=False,
        otp_hash=hash_otp(raw_otp),
        otp_expires_at=expires_at,
        otp_sent_by_email=settings.root_admin_email or actor.email,
        created_by_user_id=actor.id,
    )
    db.add(school)
    db.flush()

    try:
        send_school_otp(
            recipient_email=normalized_email,
            otp=raw_otp,
            sender_email=settings.root_admin_email or actor.email,
            sender_role=actor.role.value,
        )
    except OtpDispatchError as exc:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    db.commit()
    db.refresh(school)
    return school


def activate_school_account(db: Session, *, school_email: str, otp: str) -> School:
    school = db.query(School).filter(School.email == _normalize_email(school_email)).first()
    if not school:
        raise HTTPException(status_code=404, detail="School not found")
    if school.is_active:
        return school
    if not school.otp_hash or not school.otp_expires_at:
        raise HTTPException(status_code=400, detail="Activation OTP not generated")

    now = datetime.now(timezone.utc)
    expires_at = school.otp_expires_at
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)

    if now > expires_at:
        raise HTTPException(status_code=400, detail="OTP expired")
    if not verify_otp(otp, school.otp_hash):
        raise HTTPException(status_code=400, detail="Invalid OTP")

    school.is_active = True
    school.otp_hash = None
    school.otp_expires_at = None
    db.commit()
    db.refresh(school)
    return school


def seed_default_users(db: Session) -> None:
    defaults = [
        ("superadmin@school.local", UserRole.SUPER_ADMIN),
        ("rootadmin@school.local", UserRole.ROOT_ADMIN),
        ("admin@school.local", UserRole.ADMIN),
    ]

    for email, role in defaults:
        exists = db.query(User).filter(User.email == email).first()
        if exists:
            continue
        db.add(
            User(
                email=email,
                role=role,
                password_hash=hash_password("ChangeMe@123"),
                is_active=True,
            )
        )
    db.commit()
