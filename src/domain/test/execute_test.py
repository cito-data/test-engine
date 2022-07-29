from dataclasses import dataclass
from typing import Union
from unittest import case
import jwt
from domain.services.models.cito_insert import CitoSnowflakeClient, MaterializationType, getInsert
from domain.value_types.new_data_query import getRowCount
from domain.value_types.statistical_model import AnomalyMessage, ModelType, ResultDto, RowCountModel
from src.domain.integration_api.snowflake.query_snowflake import QuerySnowflake, QuerySnowflakeAuthDto, QuerySnowflakeRequestDto
from src.domain.services.use_case import IUseCase
from src.domain.integration_api.i_integration_api_repo import IIntegrationApiRepo
import logging
import datetime
import uuid

from src.domain.value_types.transient_types.result import Result

logger = logging.getLogger(__name__)

@dataclass
class ExecuteTestRequestDto:
  executionId: str
  materializationAddress: str
  columnName: Union[str, None]
  testType: str
  threshold: int

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
      # todo - what if empty
      # what if mat does not exist
      historyDataQueryResult = self._querySnowflake.execute(QuerySnowflakeRequestDto(newDataQuery), QuerySnowflakeAuthDto(auth.jwt))
      print(historyDataQueryResult.value)

      if not newDataQueryResult.success:
        raise Exception(newDataQueryResult.error)
      if not newDataQueryResult.value:
        raise Exception('No new data received')

      result = None
      anomalyMessage = None
      if request.testType == ModelType.ROW_COUNT.value:
        result = RowCountModel([1, 2, 3], [100, 101, 102, 103], request.threshold).run()
        anomalyMessage = AnomalyMessage.ROW_COUNT.value
      else:
        raise Exception('Test type mismatch')   

      testsQuery = getInsert([(str(uuid.uuid4()), result.type, result.threshold, request.materializationAddress, request.columnName, datetime.datetime.now().isoformat(), request.executionId)], MaterializationType.TESTS)
      testsQueryResult = self._querySnowflake.execute(QuerySnowflakeRequestDto(testsQuery), QuerySnowflakeAuthDto(auth.jwt))
      print(testsQueryResult.value)


      testHistoryQuery = getInsert([(str(uuid.uuid4()), result.type, result.newDatapoint, result.isAnomaly, None, request.executionId)], MaterializationType.TEST_HISTORY)
      testHistoryResult = self._querySnowflake.execute(QuerySnowflakeRequestDto(testHistoryQuery), QuerySnowflakeAuthDto(auth.jwt))
      print(testHistoryResult.value)

      testResultQuery = getInsert([(str(uuid.uuid4()), result.type, result.meanAbsoluteDeviation, result.medianAbsoluteDeviation, result.modifiedZScore, result.expectedValue, result.expectedValueUpperBoundary, result.expectedValueLowerBoundary, result.deviation, result.isAnomaly, request.executionId)], MaterializationType.TEST_RESULTS)
      testResultResult = self._querySnowflake.execute(QuerySnowflakeRequestDto(testResultQuery), QuerySnowflakeAuthDto(auth.jwt))
      print(testResultResult.value)

      if result.isAnomaly:
        testAlertQuery = getInsert([(str(uuid.uuid4()), result.type, anomalyMessage, request.executionId)], MaterializationType.ALERTS)
        testAlertResult = self._querySnowflake.execute(QuerySnowflakeRequestDto(testAlertQuery), QuerySnowflakeAuthDto(auth.jwt))
        print(testAlertResult.value)

      return Result.ok(result)

    except Exception as e:
      logger.error(e)
      return Result.fail(e)