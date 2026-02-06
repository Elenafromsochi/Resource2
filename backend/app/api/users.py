from fastapi import APIRouter
from fastapi import Query
from fastapi import Request

from app.schemas import RefreshUserStatsResponse
from app.schemas import UserDetailsResponse
from app.schemas import UserListResponse


router = APIRouter(prefix='/users')


@router.get('', response_model=UserListResponse)
async def list_users(
    request: Request,
    offset: int = Query(0),
    limit: int = Query(30),
    search: str | None = Query(None),
):
    raw_items = await request.app.state.storage.users.list(offset, limit, search)
    items = []
    seen_ids = set()
    for item in raw_items:
        user_id = item.get('id')
        if user_id in seen_ids:
            continue
        seen_ids.add(user_id)
        items.append(item)
    if items:
        user_ids = [item['id'] for item in items]
        stats = (
            await request.app.state.storage.messages.aggregate_user_message_stats_for_users(
                user_ids
            )
        )
        stats_map = {
            entry['user_id']: entry.get('channels', []) or [] for entry in stats
        }
        for item in items:
            item['channel_messages'] = stats_map.get(item['id'], [])
    else:
        items = []
    next_offset = offset + limit if len(raw_items) == limit else None
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
    stats = (
        await request.app.state.storage.messages.aggregate_user_message_stats_for_users(
            [user_id]
        )
    )
    channel_ids: set[int] = set()
    for entry in stats:
        for channel in entry.get('channels', []) or []:
            channel_id = channel.get('channel_id')
            if channel_id is None:
                continue
            try:
                channel_ids.add(int(channel_id))
            except (TypeError, ValueError):
                continue
    groups = []
    if channel_ids:
        channels = await request.app.state.storage.channels.get_by_ids(
            list(channel_ids)
        )
        groups = [
            channel
            for channel in channels
            if channel.get('channel_type') == 'group'
        ]
        groups.sort(key=lambda item: item.get('id') or 0)
    return {**user_data, 'groups': groups}


@router.post('/refresh-message-stats', response_model=RefreshUserStatsResponse)
async def refresh_message_stats(request: Request):
    return await request.app.state.mediator.refresh_user_message_stats()
