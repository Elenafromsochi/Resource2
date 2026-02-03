from .base import BaseStorage
from .channels import ChannelsRepository
from .users import UsersRepository


class Storage(BaseStorage):
    def __init__(self) -> None:
        self.channels: ChannelsRepository = ChannelsRepository()
        self.users: UsersRepository = UsersRepository()
