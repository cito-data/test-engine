from dataclasses import dataclass
from test_engine.src.domain.account_api.account_dto import AccountDto
from test_engine.src.domain.account_api.i_account_api_repo import IAccountApiRepo
from test_engine.src.domain.services.use_case import IUseCase
from test_engine.src.domain.value_types.transient_types.result import Result
import logging


logger = logging.getLogger(__name__)

@dataclass
class GetAccountsRequestDto:
  userId: str

@dataclass
class GetAccountsAuthDto:
  jwt: str

GetAccountsResponseDto = Result[list[AccountDto]]

class GetAccounts(IUseCase):
  
  def __init__(self, accountApiRepo: IAccountApiRepo) -> None:
    self._accountApiRepo = accountApiRepo

  def execute(self, request: GetAccountsRequestDto, auth: GetAccountsAuthDto) -> GetAccountsResponseDto:
    try:
      getAccountsResponse = self._accountApiRepo.getBy({'userId': request.userId}, auth.jwt)

      return Result.ok(getAccountsResponse)
    except Exception as e:
      logger.error(e)
      return Result.fail(e)