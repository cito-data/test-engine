import requests
from src.domain.account_api.i_account_api_repo import IAccountApiRepo
from src.domain.account_api.account_dto import AccountDto
from src.infrastructure.shared.api_root_builder import getRoot
import logging

logger = logging.getLogger(__name__)

class AccountApiRepo(IAccountApiRepo):
  def __init__(self) -> None:
    self._path = 'api/v1'
    self._serviceName: str = 'account'
    self._port = '8081'

  def getBy(self, params: dict[str, str], jwt: str) -> list[AccountDto]:
    try:
      apiRoot = getRoot(self._serviceName, self._port, self._path)

      response = requests.get(f'{apiRoot}/accounts', params=params, headers={'Authorization': f'Bearer {jwt}'})
      jsonPayload = response.json()

      if response.status_code == 200:
        return list(map((lambda x: AccountDto(x['id'], x['userId'], x['organizationId'], x['modifiedOn'])), jsonPayload))
      raise Exception(jsonPayload.message)
    except Exception as e:
      logger.error(e)

