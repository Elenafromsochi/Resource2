from .base import BaseStorage
from .channels import ChannelsRepository
from .messages import MessagesRepository
from .monitoring_runs import MonitoringRunsRepository
from .prompts import PromptsRepository
from .users import UsersRepository


class Storage(BaseStorage):
    def __init__(self) -> None:
        self.channels: ChannelsRepository = ChannelsRepository()
        self.messages: MessagesRepository = MessagesRepository()
        self.monitoring_runs: MonitoringRunsRepository = MonitoringRunsRepository()
        self.prompts: PromptsRepository = PromptsRepository()
        self.users: UsersRepository = UsersRepository()
