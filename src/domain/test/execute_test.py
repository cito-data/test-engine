from dataclasses import dataclass
import jwt
from src.domain.integration_api.snowflake.query_snowflake import QuerySnowflake, QuerySnowflakeAuthDto, QuerySnowflakeRequestDto
from src.domain.test_api.test_dto import TestDto
from integration_api.i_integration_api_repo import IIntegrationApiRepo
from src.domain.services.use_case import IUseCase
import logging

from src.domain.services.validate_json import validateJson
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

ExecuteTestResponseDto = Result[TestDto]

class ExecuteTest(IUseCase):
  
  def __init__(self, integrationApiRepo: IIntegrationApiRepo, querySnowflake: QuerySnowflake) -> None:
    self._integrationApiRepo = integrationApiRepo
    self._querySnowflake = querySnowflake

  def execute(self, request: ExecuteTestRequestDto, auth: ExecuteTestAuthDto) -> ExecuteTestResponseDto:
    try:

      newDataQueryResult = self._querySnowflake.execute(QuerySnowflakeRequestDto(request.newDataQuery), QuerySnowflakeAuthDto(jwt))
      print(newDataQueryResult)

      historyDataQueryResult = self._querySnowflake.execute(QuerySnowflakeRequestDto(request.historyDataQuery), QuerySnowflakeAuthDto(request.jwt))
      print(historyDataQueryResult)
      
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