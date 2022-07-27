from dataclasses import dataclass
from src.domain.integration_api.snowflake.snowflake_query_result_dto import SnowflakeQueryResultDto
from integration_api.i_integration_api_repo import IIntegrationApiRepo
from src.domain.services.use_case import IUseCase
import logging

from src.domain.services.validate_json import validateJson
from src.domain.value_types.transient_types.result import Result

logger = logging.getLogger(__name__)

@dataclass
class QuerySnowflakeRequestDto:
  query: str

@dataclass
class QuerySnowflakeAuthDto:
  jwt: str

QuerySnowflakeResponseDto = Result[SnowflakeQueryResultDto]

class QuerySnowflake(IUseCase):
  
  def __init__(self, integrationApiRepo: IIntegrationApiRepo) -> None:
    self._integrationApiRepo = integrationApiRepo

  def execute(self, request: QuerySnowflakeRequestDto, auth: QuerySnowflakeAuthDto) -> QuerySnowflakeResponseDto:
    try:
      querySnowflakeResponse = self._integrationApiRepo.querySnowflake(request.query, auth.jwt)

      isExpectedResponse = validateJson(querySnowflakeResponse)


      if not isExpectedResponse:
        raise Exception('Unexpected response format')

      return querySnowflakeResponse
    except Exception as e:
      logger.error(e)
      return Result.fail(e)