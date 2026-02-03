from fastapi import APIRouter
from fastapi import Query
from fastapi import Request

from app.schemas import AnalyzeRequest
from app.schemas import AnalyzeResponse
from app.schemas import UserDetailsResponse
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
    offset: int = Query(0),
    limit: int = Query(30),
):
    items = await request.app.state.storage.users.list(offset, limit)
    next_offset = offset + limit if len(items) == limit else None
    return {'items': items, 'next_offset': next_offset}


@router.get('/{user_id}', response_model=UserDetailsResponse)
async def get_user_details(user_id: int, request: Request):
    entity, about = await request.app.state.mediator.get_user_details(user_id)
    user_data = request.app.state.mediator.format_user_details(entity, about)
    await request.app.state.storage.users.upsert_profile(
        {
            'id': user_data['id'],
            'username': user_data.get('username'),
            'first_name': user_data.get('first_name'),
            'last_name': user_data.get('last_name'),
            'bio': user_data.get('bio'),
            'photo': user_data.get('photo'),
        }
    )
    groups = await request.app.state.storage.users.list_user_groups(user_id)
    return {**user_data, 'groups': groups}
