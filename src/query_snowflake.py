from dataclasses import dataclass
from snowflake_query_result_dto import SnowflakeQueryResultDto
from i_integration_api_repo import IIntegrationApiRepo
from use_case import IUseCase
import logging

from result import Result

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