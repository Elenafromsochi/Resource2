from .base import BaseStorage
from .channels import ChannelsRepository
from .messages import MessagesRepository
from .users import UsersRepository


class Storage(BaseStorage):
    def __init__(self) -> None:
        self.channels: ChannelsRepository = ChannelsRepository()
        self.messages: MessagesRepository = MessagesRepository()
        self.users: UsersRepository = UsersRepository()
