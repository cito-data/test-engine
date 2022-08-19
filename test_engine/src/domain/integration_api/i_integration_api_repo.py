

from abc import ABC, abstractmethod
from domain.integration_api.snowflake.snowflake_query_result_dto import SnowflakeQueryResultDto


class IIntegrationApiRepo(ABC):
  @abstractmethod
  def querySnowflake(self, query: str, targetOrganizationId: str, jwt: str) -> SnowflakeQueryResultDto:
    raise NotImplementedError