from fastapi import APIRouter

from app.api.channels import router as channels_router
from app.api.other import router as other_router
from app.api.users import router as users_router


router = APIRouter(prefix='/api')
router.include_router(channels_router)
router.include_router(users_router)
router.include_router(other_router)
