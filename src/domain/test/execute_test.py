from dataclasses import dataclass
from typing import Union
import jwt
from domain.value_types.new_data_query import getRowCount
from domain.value_types.statistical_model import ResultDto, RowCountModel
from src.domain.integration_api.snowflake.query_snowflake import QuerySnowflake, QuerySnowflakeAuthDto, QuerySnowflakeRequestDto
from src.domain.services.use_case import IUseCase
from src.domain.integration_api.i_integration_api_repo import IIntegrationApiRepo
import logging

from src.domain.value_types.transient_types.result import Result

logger = logging.getLogger(__name__)

@dataclass
class ExecuteTestRequestDto:
  materializationAddress: str
  columnName: Union[str, None]
  threshold: int
  executionId: str

@dataclass
class ExecuteTestAuthDto:
  jwt: str
  organizationId: str

ExecuteTestResponseDto = Result[ResultDto]

class ExecuteTest(IUseCase):
  
  def __init__(self, integrationApiRepo: IIntegrationApiRepo, querySnowflake: QuerySnowflake) -> None:
    self._integrationApiRepo = integrationApiRepo
    self._querySnowflake = querySnowflake

  def execute(self, request: ExecuteTestRequestDto, auth: ExecuteTestAuthDto) -> ExecuteTestResponseDto:
    try:

      newDataQuery = getRowCount('dbt_test.information_schema.tables', 'PUBLIC', 'APPLIED_RULE_ID')
      newDataQueryResult = self._querySnowflake.execute(QuerySnowflakeRequestDto(newDataQuery), QuerySnowflakeAuthDto(jwt))
      print(newDataQueryResult.value)

      # todo - filter for non anomaly history values
      historyDataQueryResult = self._querySnowflake.execute(QuerySnowflakeRequestDto(newDataQuery), QuerySnowflakeAuthDto(auth.jwt))
      print(historyDataQueryResult.value)

      if not newDataQueryResult.success:
        raise Exception(newDataQueryResult.error)
      if not newDataQueryResult.value:
        raise Exception('No new data received')

      result = RowCountModel([1, 2, 3], [100, 101, 102, 103], request.threshold, request.executionId).run()
      
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

      return Result.ok(result)

    except Exception as e:
      logger.error(e)
      return Result.fail(e)