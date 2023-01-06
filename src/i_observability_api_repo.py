

from abc import ABC, abstractmethod
from typing import Union
from snowflake_query_result_dto import SnowflakeQueryResultDto


class IObservabilityApiRepo(ABC):
    @abstractmethod
    def sendResult(self, result: str, jwt: str, targetOrgId: Union[str, None]) -> SnowflakeQueryResultDto:
        raise NotImplementedError
