from fastapi import APIRouter
from fastapi import Query
from fastapi import Request

from app.schemas import AnalyzeRequest
from app.schemas import AnalyzeResponse
from app.schemas import UserListResponse


router = APIRouter(prefix='/users')


@router.post('/analyze', response_model=AnalyzeResponse)
async def analyze_users(payload: AnalyzeRequest, request: Request):
    result = await request.app.state.mediator.analyze_activity(
        payload.date_from,
        payload.date_to,
        payload.channel_ids,
    )
    return result


@router.get('', response_model=UserListResponse)
async def list_users(
    request: Request,
    offset: int = Query(0, ge=0),
    limit: int = Query(30, ge=1, le=200),
):
    items = await request.app.state.storage.users.list(offset, limit)
    next_offset = offset + limit if len(items) == limit else None
    return {'items': items, 'next_offset': next_offset}
