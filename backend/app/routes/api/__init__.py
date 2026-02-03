from fastapi import APIRouter

from app.routes.api.channels import router as channels_router
from app.routes.api.other import router as other_router
from app.routes.api.users import router as users_router


router = APIRouter(prefix='/api')
router.include_router(channels_router)
router.include_router(users_router)
router.include_router(other_router)
