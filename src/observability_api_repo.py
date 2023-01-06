from typing import Union
import requests

from i_observability_api_repo import IObservabilityApiRepo
from snowflake_query_result_dto import SnowflakeQueryResultDto
import logging
from config import getObservabilityApiRoot

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class ObservabilityApiRepo(IObservabilityApiRepo):
    def __init__(self) -> None:
        self._version = 'v1'
        self._apiRoot = getObservabilityApiRoot()

    def querySnowflake(self, query: str, jwt: str, targetOrgId: Union[str, None]) -> SnowflakeQueryResultDto:
        data = {'query': query}

        data['targetOrgId'] = targetOrgId if targetOrgId else None

        response = requests.post(f'{self._apiRoot}/api/{self._version}/snowflake/query',
                                 data=data, headers={'Authorization': f'Bearer {jwt}'})
        jsonPayload = response.json()
        if response.status_code == 201:
            return SnowflakeQueryResultDto(jsonPayload)
        raise Exception(
            jsonPayload['message'] if jsonPayload['message'] else 'Unknown Error')
