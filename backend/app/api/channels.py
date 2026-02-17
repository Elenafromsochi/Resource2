from fastapi import APIRouter
from fastapi import Query
from fastapi import Request

from app.schemas import ChannelCreate
from app.schemas import ChannelDetailsResponse
from app.schemas import ChannelListResponse
from app.schemas import ChannelOut
from app.schemas import AnalyzeRenderedMessagesRequest
from app.schemas import AnalyzeRenderedMessagesResponse
from app.schemas import AnalyzeSelectedChannelsRequest
from app.schemas import RefreshMessagesRequest
from app.schemas import RefreshMessagesResponse
from app.schemas import RenderMessagesRequest
from app.schemas import RenderMessagesResponse


router = APIRouter(prefix='/channels')


@router.post('', response_model=ChannelOut)
async def add_channel(payload: ChannelCreate, request: Request):
    mediator = request.app.state.mediator
    entity = await mediator.get_channel_entity_by_identifier(payload.value)
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
    search: str | None = Query(None),
):
    items = await request.app.state.storage.channels.list(offset, limit, search)
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


@router.post('/refresh-messages', response_model=RefreshMessagesResponse)
async def refresh_messages(payload: RefreshMessagesRequest, request: Request):
    return await request.app.state.mediator.refresh_messages_cache(
        payload.date_from,
        payload.date_to,
        payload.channel_ids,
    )


@router.post('/render-messages', response_model=RenderMessagesResponse)
async def render_messages(payload: RenderMessagesRequest, request: Request):
    messages = await request.app.state.mediator.render_messages(
        payload.channel_id,
        payload.date_from,
        payload.date_to,
    )
    return {'channel_id': payload.channel_id, 'messages': messages}


@router.post(
    '/analyze-rendered-messages',
    response_model=AnalyzeRenderedMessagesResponse,
)
async def analyze_rendered_messages(
    payload: AnalyzeRenderedMessagesRequest,
    request: Request,
):
    return await request.app.state.mediator.analyze_rendered_messages(
        payload.prompt_id,
        payload.messages,
    )


@router.post(
    '/analyze-selected-channels',
    response_model=AnalyzeRenderedMessagesResponse,
)
async def analyze_selected_channels(
    payload: AnalyzeSelectedChannelsRequest,
    request: Request,
):
    return await request.app.state.mediator.analyze_selected_channels(
        payload.prompt_id,
        payload.channel_ids,
        payload.date_from,
        payload.date_to,
    )
