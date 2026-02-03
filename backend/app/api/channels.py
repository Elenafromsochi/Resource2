from fastapi import APIRouter
from fastapi import HTTPException
from fastapi import Query
from fastapi import Request

from app.schemas import ChannelCreate
from app.schemas import ChannelDetailsResponse
from app.schemas import ChannelListResponse
from app.schemas import ChannelOut


router = APIRouter(prefix='/channels')


@router.post('', response_model=ChannelOut)
async def add_channel(payload: ChannelCreate, request: Request):
    mediator = request.app.state.mediator
    try:
        entity = await mediator.get_channel_entity(payload.value)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    channel = mediator.format_channel(entity)
    saved = await request.app.state.storage.channels.upsert(channel)
    return saved


@router.delete('/{channel_id}')
async def delete_channel(channel_id: int, request: Request):
    await request.app.state.storage.channels.delete(channel_id)
    return {'status': 'deleted'}


@router.get('', response_model=ChannelListResponse)
async def list_channels(
    request: Request,
    offset: int = Query(0),
    limit: int = Query(30),
):
    items = await request.app.state.storage.channels.list(offset, limit)
    next_offset = offset + limit if len(items) == limit else None
    return {'items': items, 'next_offset': next_offset}


@router.get('/all', response_model=list[ChannelOut])
async def list_all_channels(request: Request):
    return await request.app.state.storage.channels.list_all()


@router.get('/{channel_id}', response_model=ChannelDetailsResponse)
async def get_channel_details(channel_id: int, request: Request):
    entity, about, members_count = await request.app.state.mediator.get_channel_details(
        channel_id
    )
    channel_data = request.app.state.mediator.format_channel_details(
        entity,
        about,
        members_count,
    )
    channel_base = request.app.state.mediator.format_channel(entity)
    await request.app.state.storage.channels.upsert(channel_base)
    return channel_data


@router.post('/import-dialogs')
async def import_dialogs(request: Request):
    saved = await request.app.state.mediator.import_dialogs()
    return {'imported': len(saved)}
