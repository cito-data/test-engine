from dataclasses import dataclass
from src.domain.account_api.account_dto import AccountDto
from account_api.i_account_api_repo import IAccountApiRepo
from src.domain.services.use_case import IUseCase
from domain.value_types.transient_types.result import Result
import logging

from src.domain.services.validate_json import validateJson

logger = logging.getLogger(__name__)

@dataclass
class GetAccountsRequestDto:
  userId: str

@dataclass
class GetAccountsAuthDto:
  jwt: str

GetAccountsResponseDto = Result[AccountDto]

class GetAccounts(IUseCase):
  
  def __init__(self, accountApiRepo: IAccountApiRepo) -> None:
    self._accountApiRepo = accountApiRepo

  def execute(self, request: GetAccountsRequestDto, auth: GetAccountsAuthDto) -> GetAccountsResponseDto:
    try:
      getAccountsResponse = self._accountApiRepo.getBy({'userId': request.userId}, auth.jwt)

      isExpectedResponse = validateJson(getAccountsResponse)


      if not isExpectedResponse:
        raise Exception('Unexpected response format')

      return Result.ok(getAccountsResponse)
    except Exception as e:
      logger.error(e)
      return Result.fail(e)