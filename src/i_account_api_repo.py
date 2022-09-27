

from abc import ABC, abstractmethod
from .account_dto import AccountDto

class IAccountApiRepo(ABC):
  @abstractmethod
  def getBy(self, params: dict[str, str], jwt: str) -> list[AccountDto]:
    raise NotImplementedError