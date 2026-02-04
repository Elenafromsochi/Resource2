from datetime import datetime

from pydantic import BaseModel
from pydantic import Field


class ChannelCreate(BaseModel):
    value: str


class ChannelOut(BaseModel):
    id: int
    username: str | None
    title: str
    channel_type: str
    link: str | None
    updated_at: datetime | None = None


class ChannelListResponse(BaseModel):
    items: list[ChannelOut]
    next_offset: int | None


class ChannelDetailsResponse(BaseModel):
    id: int
    username: str | None
    title: str
    channel_type: str
    link: str | None
    about: str | None
    members_count: int | None


class AnalyzeRequest(BaseModel):
    date_from: datetime
    date_to: datetime
    channel_ids: list[int] | None = None


class AnalyzeResponse(BaseModel):
    users_analyzed: int
    errors: list[str] = Field(default_factory=list)


class RefreshMessagesResponse(BaseModel):
    channels_processed: int
    messages_processed: int
    messages_upserted: int
    messages_updated: int
    errors: list[str] = Field(default_factory=list)


class UserChannelMessagesOut(BaseModel):
    channel_id: int
    messages_count: int


class UserOut(BaseModel):
    id: int
    username: str | None
    first_name: str | None
    last_name: str | None
    bio: str | None
    photo: str | None
    messages_count: int
    channel_messages: list[UserChannelMessagesOut] = Field(default_factory=list)
    updated_at: datetime | None = None


class UserListResponse(BaseModel):
    items: list[UserOut]
    next_offset: int | None


class UserGroupOut(BaseModel):
    id: int
    username: str | None
    title: str
    channel_type: str
    link: str | None


class UserDetailsResponse(BaseModel):
    id: int
    username: str | None
    first_name: str | None
    last_name: str | None
    bio: str | None
    photo: str | None
    phone: str | None
    groups: list[UserGroupOut]
