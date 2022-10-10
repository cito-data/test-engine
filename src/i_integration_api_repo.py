

from abc import ABC, abstractmethod
from typing import Union
from snowflake_query_result_dto import SnowflakeQueryResultDto


class IIntegrationApiRepo(ABC):
  @abstractmethod
  def querySnowflake(self, query: str, jwt: str, targetOrganizationId: Union[str, None]) -> SnowflakeQueryResultDto:
    raise NotImplementedError