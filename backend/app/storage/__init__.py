from .base import BaseStorage
from .channels import ChannelsRepository
from .messages import MessagesRepository
from .prompts import PromptsRepository
from .users import UsersRepository


class Storage(BaseStorage):
    def __init__(self) -> None:
        self.channels: ChannelsRepository = ChannelsRepository()
        self.messages: MessagesRepository = MessagesRepository()
        self.prompts: PromptsRepository = PromptsRepository()
        self.users: UsersRepository = UsersRepository()
