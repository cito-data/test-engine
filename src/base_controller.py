from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, TypeVar, Union, Literal

from get_accounts import GetAccounts, GetAccountsAuthDto, GetAccountsRequestDto

from token_required import ProcessedAuth

from result import Result
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


T = TypeVar("T")


@dataclass
class UserAccountInfo:
    userId: Union[str, None]
    accountId: Union[str, None]
    callerOrgId: Union[str, None]
    isSystemInternal: bool


@dataclass
class Response:
    body: Union[Any, None]
    statusCode: int


@dataclass
class Request:
    headers: Union["dict[str, str]", None]
    pathParams: Union["dict[str, str]", None]
    queryParams: Union["dict[str, str]", None]
    body: Union["dict[str, Any]", None]
    auth: ProcessedAuth


class BaseController(ABC):

    #     OK = 200,
    #     CREATED = 201,
    #     BAD_REQUEST = 400,
    #     UNAUTHORIZED = 401,
    #     FORBIDDEN = 403,
    #     NOT_FOUND = 404,
    #     CONFLICT = 409,
    #     SERVER_ERROR = 500,

    @staticmethod
    def jsonResponse(code: Literal[200, 201, 400, 401, 403, 404, 409, 500], message: str) -> Response:
        return Response(message, code)

    @staticmethod
    def ok(dto: Union[T, None], code: Literal[200, 201, 400, 401, 403, 404, 409, 500]) -> Response:
        return Response(dto, code)

    @staticmethod
    def badRequest(message: Union[str, None]) -> Response:
        return BaseController.jsonResponse(400, (message if message else 'Bad Request'))

    @staticmethod
    def unauthorized(message: Union[str, None]) -> Response:
        return BaseController.jsonResponse(401, (message if message else 'Unauthorized'))

    @staticmethod
    def notFound(message: Union[str, None]) -> Response:
        return BaseController.jsonResponse(404, (message if message else 'Not found'))

    @staticmethod
    def fail(error: Union[str, Exception]) -> Response:
        return Response(str(error), 500)

    @abstractmethod
    def executeImpl(self, req: Request) -> Response:
        raise NotImplementedError

    def execute(self, req: Request) -> Response:
        try:
            return self.executeImpl(req)
        except Exception as e:
            logger.exception(f'error: {e}' if e.args[0] else f'error: unknown')
            return BaseController.fail('An unexpected error occurred')

    @staticmethod
    def getUserAccountInfo(processedAuth: ProcessedAuth, getAccounts: GetAccounts) -> Result[UserAccountInfo]:
        if not processedAuth.payload:
            return Result.fail('Unauthorized - No auth payload')

        try:
            isSystemInternal = 'system-internal/system-internal' in processedAuth.payload[
                'scope'] if 'scope' in processedAuth.payload else False

            if isSystemInternal:
                return Result.ok(UserAccountInfo(None, None, None, isSystemInternal))

            getAccountResult = getAccounts.execute(GetAccountsRequestDto(
                processedAuth.payload['username']), GetAccountsAuthDto(processedAuth.token))

            if not getAccountResult.value:
                raise Exception('No account found')
            if not len(getAccountResult.value) > 0:
                raise Exception('No account found')

            return Result.ok(UserAccountInfo(processedAuth.payload['username'], getAccountResult.value[0].id, getAccountResult.value[0].organizationId, isSystemInternal))
        except Exception as e:
            logger.exception(f'error: {e}' if e.args[0] else f'error: unknown')
            return Result.fail('')
