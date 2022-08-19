import requests
from domain.account_api.i_account_api_repo import IAccountApiRepo
from domain.account_api.account_dto import AccountDto
from infrastructure.shared.api_root_builder import getRoot
import logging
from config import getMode

logger = logging.getLogger(__name__)

class AccountApiRepo(IAccountApiRepo):
  def __init__(self) -> None:
    self._path = 'api/v1'
    self._port = '8081'
    self._prodGateway = 'p2krek4fsj'

    self._mode = getMode()

  def getBy(self, params: dict[str, str], jwt: str) -> list[AccountDto]:
    try:  
      gateway = self._port
      if(self._mode == 'production'):
        gateway = self._prodGateway

      apiRoot = getRoot(gateway, self._path)

      response = requests.get(f'{apiRoot}/accounts', params=params, headers={'Authorization': f'Bearer {jwt}'})
      jsonPayload = response.json()

      if response.status_code == 200:
        return list(map((lambda x: AccountDto(x['id'], x['userId'], x['organizationId'], x['modifiedOn'])), jsonPayload))
      raise Exception(jsonPayload['message'] if jsonPayload['message'] else 'Unknown Error')
    except Exception as e:
      logger.error(e)

