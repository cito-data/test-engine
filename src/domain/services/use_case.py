from abc import ABC, abstractmethod
from typing import Generic, TypeVar

IRequest = TypeVar('IRequest')
IResponse = TypeVar('IResponse')
IAuth = TypeVar('IAuth')
IDbConnection = TypeVar('IDbConnection', bound = None)


class IUseCase(ABC, Generic[IRequest, IResponse, IAuth, IDbConnection]):
  @abstractmethod
  def execute(self, request: IRequest, auth: IAuth, dbConnection: IDbConnection) -> IResponse:
    raise NotImplementedError