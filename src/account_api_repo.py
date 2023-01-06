import requests

from i_account_api_repo import IAccountApiRepo
from account_dto import AccountDto
import logging
from config import getAccountApiRoot

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class AccountApiRepo(IAccountApiRepo):
    def __init__(self) -> None:
        self._version = 'v1'
        self._apiRoot = getAccountApiRoot()

    def getBy(self, params: "dict[str, str]", jwt: str) -> "list[AccountDto]":
        response = requests.get(f'{self._apiRoot}/api/{self._version}/accounts',
                                params=params, headers={'Authorization': f'Bearer {jwt}'})
        jsonPayload = response.json()

        if response.status_code == 200:
            return list(map((lambda x: AccountDto(x['id'], x['userId'], x['organizationId'], x['modifiedOn'])), jsonPayload))
        raise Exception(
            jsonPayload['message'] if jsonPayload['message'] else 'Unknown Error')
