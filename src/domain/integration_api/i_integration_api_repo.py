

from abc import ABC, abstractmethod
from snowflake.snowflake_query_result_dto import SnowflakeQueryResultDto


class IIntegrationApiRepo(ABC):
  @abstractmethod
  def querySnowflake(self, query: str, jwt: str) -> SnowflakeQueryResultDto:
    raise NotImplementedError