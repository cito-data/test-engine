from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import IntEnum
from typing import TypeVar, Union

from src.domain.account_api.get_accounts import GetAccounts, GetAccountsAuthDto, GetAccountsRequestDto

from flask import request
from src.infrastructure.shared.token_required import ProcessedAuth

from src.domain.value_types.transient_types.result import Result
import logging

logger = logging.getLogger(__name__)


T = TypeVar("T")

class CodeHttp(IntEnum):
  OK = 200,
  CREATED = 201,
  BAD_REQUEST = 400,
  UNAUTHORIZED = 401,
  FORBIDDEN = 403,
  NOT_FOUND = 404,
  CONFLICT = 409,
  SERVER_ERROR = 500,

@dataclass
class UserAccountInfo:
  userId: str
  accountId: str 
  organizationId: str

@dataclass
class Response:
  payload: Union[str, None]
  statusCode: int

class BaseController(ABC):
  
  @staticmethod
  def jsonResponse(code: int, message: str) -> Response:
    return Response(message, code)

  @staticmethod
  def ok(dto: Union[T, None], code: Union[CodeHttp, None]) -> Response:
    codeHttp = code if code  else CodeHttp.OK.value
    return Response(dto, codeHttp)

  @staticmethod
  def badRequest(res: Response, message: Union[str, None]) -> Response:
    return BaseController.jsonResponse(CodeHttp.BAD_REQUEST.value, (message if message else 'Bad Request'))

  @staticmethod
  def unauthorized(message: Union[str, None]) -> Response:
    return BaseController.jsonResponse(CodeHttp.UNAUTHORIZED.value, (message if message else 'Unauthorized'))

  @staticmethod
  def notFound(message: Union[str, None]) -> Response:
    return BaseController.jsonResponse(CodeHttp.NOT_FOUND.value, (message if message else 'Not found'))

  @staticmethod
  def fail(error: Union[str, Exception]) -> Response:
    return Response(error, CodeHttp.SERVER_ERROR.value)

  @abstractmethod
  def executeImpl(self, req: request, processedAuth: ProcessedAuth) -> Response:
    raise NotImplementedError

  def execute(self, req: request, processedAuth: ProcessedAuth) -> Response:
    try:
      return self.executeImpl(req, processedAuth)
    except Exception as e:
      logger.error(e)
      return BaseController.fail('An unexpected error occurred')
    
  def getUserAccountInfo(processedAuth: ProcessedAuth, getAccounts: GetAccounts) -> Result[UserAccountInfo]:
    if not processedAuth.payload:
      return Result.fail('Unauthorized - No auth payload')

    try:
        getAccountResult = getAccounts.execute(GetAccountsRequestDto(processedAuth.payload['username']), GetAccountsAuthDto(processedAuth.token))

        if not getAccountResult.value:
          raise Exception('No account found')
        if not len(getAccountResult.value) > 0:
          raise Exception('No account found')

        return Result.ok(UserAccountInfo(processedAuth.payload['username'], getAccountResult.value[0].id, getAccountResult.value[0].organizationId))
    except Exception as e:
      logger.error(e)
      return Result.fail(e)