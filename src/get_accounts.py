from dataclasses import dataclass
from account_dto import AccountDto
from i_account_api_repo import IAccountApiRepo
from use_case import IUseCase
from result import Result
import logging


logger = logging.getLogger(__name__)

@dataclass
class GetAccountsRequestDto:
  userId: str

@dataclass
class GetAccountsAuthDto:
  jwt: str

GetAccountsResponseDto = Result["list[AccountDto]"]

class GetAccounts(IUseCase):
  
  def __init__(self, accountApiRepo: IAccountApiRepo) -> None:
    self._accountApiRepo = accountApiRepo

  def execute(self, request: GetAccountsRequestDto, auth: GetAccountsAuthDto) -> GetAccountsResponseDto:
    try:
      getAccountsResponse = self._accountApiRepo.getBy({'userId': request.userId}, auth.jwt)

      return Result.ok(getAccountsResponse)
    except Exception as e:
      logger.exception(f'error: {e}' if e.args[0] else f'error: unknown')
      return Result.fail('')