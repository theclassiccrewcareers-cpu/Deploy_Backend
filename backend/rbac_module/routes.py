from fastapi import APIRouter, Depends, status
from sqlalchemy.orm import Session

from .database import get_db_session
from .middleware import get_current_user, require_roles
from .models import User, UserRole
from .schemas import (
    LoginRequest,
    LoginResponse,
    RoleRouteResponse,
    SchoolActivateRequest,
    SchoolCreateRequest,
    SchoolOut,
    StudentCreateRequest,
    StudentEmailUpdateRequest,
    StudentOut,
    StudentPasswordUpdateRequest,
    UserOut,
)
from .services import (
    activate_school_account,
    create_school_with_otp,
    create_student,
    login_user,
    update_student_email,
    update_student_password,
)

router = APIRouter(prefix="/api/v1/rbac", tags=["RBAC Auth"])


@router.post("/auth/login", response_model=LoginResponse)
def login(payload: LoginRequest, db: Session = Depends(get_db_session)):
    token = login_user(db, email=payload.email, password=payload.password)
    user = db.query(User).filter(User.email == payload.email.strip().lower()).first()
    return LoginResponse(access_token=token, role=user.role)


@router.get("/me", response_model=UserOut)
def me(current_user: User = Depends(get_current_user)):
    return UserOut(id=current_user.id, email=current_user.email, role=current_user.role)


@router.post(
    "/root/students",
    response_model=StudentOut,
    status_code=status.HTTP_201_CREATED,
)
def root_add_student(
    payload: StudentCreateRequest,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(require_roles(UserRole.ROOT_ADMIN, UserRole.SUPER_ADMIN)),
):
    student = create_student(
        db,
        full_name=payload.full_name,
        email=payload.email,
        raw_password=payload.password,
        school_id=payload.school_id,
        actor_user_id=current_user.id,
    )
    return StudentOut(id=student.id, full_name=student.full_name, email=student.email, school_id=student.school_id)


@router.patch("/root/students/{student_id}/email", response_model=StudentOut)
def root_update_student_email(
    student_id: int,
    payload: StudentEmailUpdateRequest,
    db: Session = Depends(get_db_session),
    _: User = Depends(require_roles(UserRole.ROOT_ADMIN, UserRole.SUPER_ADMIN)),
):
    student = update_student_email(db, student_id=student_id, new_email=payload.email)
    return StudentOut(id=student.id, full_name=student.full_name, email=student.email, school_id=student.school_id)


@router.patch("/root/students/{student_id}/password", response_model=RoleRouteResponse)
def root_update_student_password(
    student_id: int,
    payload: StudentPasswordUpdateRequest,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(require_roles(UserRole.ROOT_ADMIN, UserRole.SUPER_ADMIN)),
):
    update_student_password(db, student_id=student_id, new_password=payload.password)
    return RoleRouteResponse(message="Student password updated", acting_role=current_user.role)


@router.post("/root/schools", response_model=SchoolOut, status_code=status.HTTP_201_CREATED)
def root_create_school(
    payload: SchoolCreateRequest,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(require_roles(UserRole.ROOT_ADMIN, UserRole.SUPER_ADMIN)),
):
    school = create_school_with_otp(
        db,
        school_name=payload.name,
        school_email=payload.email,
        school_password=payload.password,
        actor=current_user,
    )
    return SchoolOut(
        id=school.id,
        name=school.name,
        email=school.email,
        is_active=school.is_active,
        created_at=school.created_at,
    )


@router.post("/schools/activate", response_model=SchoolOut)
def activate_school(payload: SchoolActivateRequest, db: Session = Depends(get_db_session)):
    school = activate_school_account(db, school_email=payload.school_email, otp=payload.otp)
    return SchoolOut(
        id=school.id,
        name=school.name,
        email=school.email,
        is_active=school.is_active,
        created_at=school.created_at,
    )


@router.get("/admin/example", response_model=RoleRouteResponse)
def admin_example(current_user: User = Depends(require_roles(UserRole.ADMIN, UserRole.SUPER_ADMIN))):
    return RoleRouteResponse(message="Admin protected route", acting_role=current_user.role)


@router.get("/root/example", response_model=RoleRouteResponse)
def root_example(current_user: User = Depends(require_roles(UserRole.ROOT_ADMIN, UserRole.SUPER_ADMIN))):
    return RoleRouteResponse(message="Root Admin protected route", acting_role=current_user.role)


@router.get("/super/example", response_model=RoleRouteResponse)
def super_example(current_user: User = Depends(require_roles(UserRole.SUPER_ADMIN))):
    return RoleRouteResponse(message="Super Admin protected route", acting_role=current_user.role)
