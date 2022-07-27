from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, TypeVar, Union

from src.domain.account_api.get_accounts import GetAccounts, GetAccountsAuthDto, GetAccountsRequestDto

from flask import request, Response
from src.infrastructure.shared.token_required import ProcessedAuth

from src.domain.value_types.transient_types.result import Result
import logging

logger = logging.getLogger(__name__)


T = TypeVar("T")

class CodeHttp(Enum):
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

class BaseController(ABC):
  
  @staticmethod
  def jsonResponse(res: Response, code: int, message: str) -> Response:
    res.status = code
    res.mimetype = 'application/json'
    res.response = "{'message': " + message + "}"
    return res

  @staticmethod
  def ok(res: Response, dto: Union[T, None], code: Union[CodeHttp, None]) -> Response:
    codeHttp = code if code  else CodeHttp.OK
    if dto:
      res.mimetype = 'application/json'
      res.status = codeHttp
      res.response = dto
    return res

  @staticmethod
  def badRequest(res: Response, message: Union[str, None]) -> Response:
    return BaseController.jsonResponse(res, CodeHttp.BAD_REQUEST, (message if message else 'Bad Request'))

  @staticmethod
  def unauthorized(res: Response, message: Union[str, None]) -> Response:
    return BaseController.jsonResponse(res, CodeHttp.UNAUTHORIZED, (message if message else 'Unauthorized'))

  @staticmethod
  def notFound(res: Response, message: Union[str, None]) -> Response:
    return BaseController.jsonResponse(res, CodeHttp.NOT_FOUND, (message if message else 'Not found'))

  @staticmethod
  def fail(res: Response, error: Union[str, Exception]) -> Response:
    res.status = CodeHttp.SERVER_ERROR
    res.mimetype = 'application/json'
    res.response = "{'message': " + str(error) + "}"
    return res

  @abstractmethod
  def executeImpl(self, req: request, res: Response, processedAuth: ProcessedAuth) -> Response:
    raise NotImplementedError

  def execute(self, req: request, res: Response, processedAuth: ProcessedAuth) -> Union[Response, None]:
    try:
      self.executeImpl(req, res, processedAuth)
    except Exception as e:
      logger.error(e)
      BaseController.fail(res, 'An unexpected error occurred')
    
  def getUserAccountInfo(processedAuth: ProcessedAuth, getAccounts: GetAccounts) -> Result[UserAccountInfo]:
    if not processedAuth.payload:
      return Result.fail('Unauthorized - No auth payload')

    try:
        getAccountResult = getAccounts.execute(GetAccountsRequestDto(processedAuth.payload.username), GetAccountsAuthDto(processedAuth.token))

        if not getAccountResult.value:
          raise Exception('No account found')
        if not len(getAccountResult.value) > 0:
          raise Exception('No account found')

        return Result.ok(UserAccountInfo(processedAuth.payload.username, getAccountResult.value[0].id, getAccountResult.value[0].organizationId))
    except Exception as e:
      logger.error(e)
      return Result.fail(e)