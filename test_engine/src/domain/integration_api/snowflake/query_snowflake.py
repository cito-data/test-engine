from dataclasses import dataclass
from test_engine.src.domain.integration_api.snowflake.snowflake_query_result_dto import SnowflakeQueryResultDto
from test_engine.src.domain.integration_api.i_integration_api_repo import IIntegrationApiRepo
from test_engine.src.domain.services.use_case import IUseCase
import logging

from test_engine.src.domain.value_types.transient_types.result import Result

logger = logging.getLogger(__name__)

@dataclass
class QuerySnowflakeRequestDto:
  query: str
  targetOrganizationId: str

@dataclass
class QuerySnowflakeAuthDto:
  jwt: str

QuerySnowflakeResponseDto = Result[SnowflakeQueryResultDto]

class QuerySnowflake(IUseCase):
  
  def __init__(self, integrationApiRepo: IIntegrationApiRepo) -> None:
    self._integrationApiRepo = integrationApiRepo

  def execute(self, request: QuerySnowflakeRequestDto, auth: QuerySnowflakeAuthDto) -> QuerySnowflakeResponseDto:
    try:
      querySnowflakeResponse = self._integrationApiRepo.querySnowflake(request.query, request.targetOrganizationId, auth.jwt)

      return Result.ok(querySnowflakeResponse)
    except Exception as e:
      logger.error(e)
      return Result.fail(e)