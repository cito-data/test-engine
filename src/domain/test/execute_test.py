from dataclasses import dataclass
import jwt
from src.domain.integration_api.snowflake.query_snowflake import QuerySnowflake, QuerySnowflakeAuthDto, QuerySnowflakeRequestDto
from src.domain.services.use_case import IUseCase
from src.domain.test.test_result_dto import TestResultDto
from src.domain.integration_api.i_integration_api_repo import IIntegrationApiRepo
import logging

from src.domain.value_types.transient_types.result import Result

logger = logging.getLogger(__name__)

@dataclass
class ExecuteTestRequestDto:
  newDataQuery: str
  historyDataQuery: str

@dataclass
class ExecuteTestAuthDto:
  jwt: str
  organizationId: str

ExecuteTestResponseDto = Result[TestResultDto]

class ExecuteTest(IUseCase):
  
  def __init__(self, integrationApiRepo: IIntegrationApiRepo, querySnowflake: QuerySnowflake) -> None:
    self._integrationApiRepo = integrationApiRepo
    self._querySnowflake = querySnowflake

  def execute(self, request: ExecuteTestRequestDto, auth: ExecuteTestAuthDto) -> ExecuteTestResponseDto:
    try:

      newDataQueryResult = self._querySnowflake.execute(QuerySnowflakeRequestDto(request.newDataQuery), QuerySnowflakeAuthDto(jwt))
      print(newDataQueryResult.value)

      historyDataQueryResult = self._querySnowflake.execute(QuerySnowflakeRequestDto(request.historyDataQuery), QuerySnowflakeAuthDto(auth.jwt))
      print(historyDataQueryResult.value)

      if not newDataQueryResult.success:
        raise Exception(newDataQueryResult.error)
      if not newDataQueryResult.value:
        raise Exception('No new data received')

      return Result.ok(TestResultDto(newDataQueryResult.value.content))
      
      # // POST test run - req to test-engine-service

      # // Write SF resources - write test result

      # // Write SF resources - write alert

      # // Write SF resources - write new history values

      # const currentRowCount = testRowCount;
      # const currentHistory = testHistory;

      # executeTestResponse = self._testApiRepo.execute({'userId': request.userId}, auth.jwt)

      # isExpectedResponse = validateJson(executeTestResponse)


      # if not isExpectedResponse:
      #   raise Exception('Unexpected response format')

      # return executeTestResponse
    except Exception as e:
      logger.error(e)
      return Result.fail(e)