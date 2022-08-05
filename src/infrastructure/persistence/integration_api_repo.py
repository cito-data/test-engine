import requests
from src.domain.integration_api.i_integration_api_repo import IIntegrationApiRepo
from src.domain.integration_api.snowflake.snowflake_query_result_dto import SnowflakeQueryResultDto
from src.infrastructure.shared.api_root_builder import getRoot
import logging

logger = logging.getLogger(__name__)

class IntegrationApiRepo(IIntegrationApiRepo):
  def __init__(self) -> None:
    self._path = 'api/v1'
    self._serviceName: str = 'integration'
    self._port = '3002'

  def querySnowflake(self, query: str, targetOrganizationId: str,  jwt: str) -> SnowflakeQueryResultDto:
    try:
      apiRoot = getRoot(self._serviceName, self._port, self._path)

      response = requests.post(f'{apiRoot}/snowflake/query', data={'query': query, 'targetOrganizationId': targetOrganizationId}, headers={'Authorization': f'Bearer {jwt}'})
      jsonPayload = response.json()
      if response.status_code == 201:
        return SnowflakeQueryResultDto(jsonPayload)
      raise Exception(jsonPayload['message'] if jsonPayload['message'] else 'Unknown Error')
    except Exception as e:
      logger.error(e)
