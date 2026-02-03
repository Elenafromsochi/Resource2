from fastapi import APIRouter
from fastapi import HTTPException
from fastapi import Query
from fastapi import Request

from app.schemas import AnalyzeRequest
from app.schemas import AnalyzeResponse
from app.schemas import ChannelCreate
from app.schemas import ChannelListResponse
from app.schemas import ChannelOut
from app.schemas import UserListResponse
from app.schemas import UserOut


router = APIRouter(prefix="/api")


@router.get("/health")
async def health():
    return {"status": "ok"}


@router.post("/channels", response_model=ChannelOut)
async def add_channel(payload: ChannelCreate, request: Request):
    mediator = request.app.state.mediator
    try:
        entity = await mediator.get_channel_entity(payload.value)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    channel = mediator.format_channel(entity)
    saved = await request.app.state.storage.channels.upsert(channel)
    return saved


@router.delete("/channels/{channel_id}")
async def delete_channel(channel_id: int, request: Request):
    await request.app.state.storage.channels.delete(channel_id)
    return {"status": "deleted"}


@router.get("/channels", response_model=ChannelListResponse)
async def list_channels(
    request: Request,
    offset: int = Query(0, ge=0),
    limit: int = Query(30, ge=1, le=200),
):
    items = await request.app.state.storage.channels.list(offset, limit)
    next_offset = offset + limit if len(items) == limit else None
    return {"items": items, "next_offset": next_offset}


@router.get("/channels/all", response_model=list[ChannelOut])
async def list_all_channels(request: Request):
    return await request.app.state.storage.channels.list_all()


@router.post("/channels/import-dialogs")
async def import_dialogs(request: Request):
    saved = await request.app.state.mediator.import_dialogs()
    return {"imported": len(saved)}


@router.post("/users/analyze", response_model=AnalyzeResponse)
async def analyze_users(payload: AnalyzeRequest, request: Request):
    result = await request.app.state.mediator.analyze_activity(
        payload.date_from,
        payload.date_to,
        payload.channel_ids,
    )
    return result


@router.get("/users", response_model=UserListResponse)
async def list_users(
    request: Request,
    offset: int = Query(0, ge=0),
    limit: int = Query(30, ge=1, le=200),
):
    items = await request.app.state.storage.users.list(offset, limit)
    next_offset = offset + limit if len(items) == limit else None
    return {"items": items, "next_offset": next_offset}
