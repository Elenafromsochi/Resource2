from fastapi import APIRouter
from fastapi import Request

from app.exceptions import AppException
from app.exceptions import PromptNotFoundError
from app.schemas import PromptCreate
from app.schemas import PromptOut
from app.schemas import PromptUpdate


router = APIRouter(prefix='/prompts')


@router.get('', response_model=list[PromptOut])
async def list_prompts(request: Request):
    return await request.app.state.storage.prompts.list()


@router.get('/{prompt_id}', response_model=PromptOut)
async def get_prompt(prompt_id: int, request: Request):
    prompt = await request.app.state.storage.prompts.get(prompt_id)
    if not prompt:
        raise PromptNotFoundError()
    return prompt


@router.post('', response_model=PromptOut)
async def create_prompt(payload: PromptCreate, request: Request):
    title = payload.title.strip()
    text = payload.text.strip()
    if not title or not text:
        raise AppException('Prompt title and text are required')
    prompt = await request.app.state.storage.prompts.create(title, text)
    return prompt


@router.put('/{prompt_id}', response_model=PromptOut)
async def update_prompt(prompt_id: int, payload: PromptUpdate, request: Request):
    title = payload.title.strip()
    text = payload.text.strip()
    if not title or not text:
        raise AppException('Prompt title and text are required')
    prompt = await request.app.state.storage.prompts.update(prompt_id, title, text)
    if not prompt:
        raise PromptNotFoundError()
    return prompt


@router.delete('/{prompt_id}')
async def delete_prompt(prompt_id: int, request: Request):
    prompt = await request.app.state.storage.prompts.get(prompt_id)
    if not prompt:
        raise PromptNotFoundError()
    await request.app.state.storage.prompts.delete(prompt_id)
    return {'status': 'deleted'}
