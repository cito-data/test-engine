from typing import Union
import requests
from .i_integration_api_repo import IIntegrationApiRepo
from .snowflake_query_result_dto import SnowflakeQueryResultDto
from .api_root_builder import getRoot
import logging
from .config import getMode

logger = logging.getLogger(__name__)

class IntegrationApiRepo(IIntegrationApiRepo):
  def __init__(self) -> None:
    self._path = 'api/v1'
    self._port = '3002'
    self._prodGateway = 'wej7xjkvug'

    self._mode = getMode()


  def querySnowflake(self, query: str, jwt: str, targetOrganizationId: Union[str, None]) -> SnowflakeQueryResultDto:
    try:
      gateway = self._port
      if(self._mode == 'production'):
        gateway = self._prodGateway

      apiRoot = getRoot(gateway, self._path)

      data = {'query': query}

      data['targetOrganizationId'] = targetOrganizationId if targetOrganizationId else None

      response = requests.post(f'{apiRoot}/snowflake/query', data=data, headers={'Authorization': f'Bearer {jwt}'})
      jsonPayload = response.json()
      if response.status_code == 201:
        return SnowflakeQueryResultDto(jsonPayload)
      raise Exception(jsonPayload['message'] if jsonPayload['message'] else 'Unknown Error')
    except Exception as e:
      logger.error(e)
