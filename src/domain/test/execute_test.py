from dataclasses import dataclass
import json
from typing import Union
from unittest import case
import jwt
from domain.services.models.cito_data_query import MaterializationType, getInsertQuery, getTestQuery
from domain.services.models.new_materialization_data_query import getRowCountQuery
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
    testId: str


@dataclass
class ExecuteTestAuthDto:
    jwt: str
    organizationId: str


ExecuteTestResponseDto = Result[ResultDto]


class ExecuteTest(IUseCase):

    _testId: str
    _executionId: str

    def __init__(self, integrationApiRepo: IIntegrationApiRepo, querySnowflake: QuerySnowflake) -> None:
        self._integrationApiRepo = integrationApiRepo
        self._querySnowflake = querySnowflake

    def _insertExecutionEntry(self, jwt):
      executedOn = datetime.datetime.utcnow().isoformat()

      valueSets = [
        {'name': 'id', 'value': self._executionId, 'type': 'string'},
        {'name': 'executedOn', 'value': executedOn, 'type': 'timestamp_tz'},
        {'name': 'testId', 'value': self._testId, 'type': 'string'},
      ]

      executionQuery = getInsertQuery(valueSets, MaterializationType.EXECUTIONS)
      return self._querySnowflake.execute(
          QuerySnowflakeRequestDto(executionQuery), QuerySnowflakeAuthDto(jwt))

    def _insertHistoryEntry(self, testType, value, isAnomaly, jwt):
      valueSets = [
        { 'name': 'id', 'type': 'string', 'value': str(uuid.uuid4()) },
        { 'name': 'test_type', 'type': 'string','value': testType },
        { 'name': 'value', 'type': 'float', 'value': value },
        { 'name': 'is_anomaly', 'type': 'boolean', 'value': isAnomaly },
        { 'name': 'user_feedback_is_anomaly', 'type': 'integer', 'value': -1 },
        { 'name': 'test_id', 'type': 'string', 'value': self._testId },
        { 'name': 'execution_id', 'type': 'string', 'value': self._executionId },
      ]

      testHistoryQuery = getInsertQuery(valueSets, MaterializationType.TEST_HISTORY)
      return self._querySnowflake.execute(
          QuerySnowflakeRequestDto(testHistoryQuery), QuerySnowflakeAuthDto(jwt))

    def _insertResultEntry(self, testResult, jwt):
      valueSets = [
        { 'name': 'id', 'type': 'string', 'value': str(uuid.uuid4()) },
        { 'name': 'test_type', 'type': 'string', 'value': testResult.type},
        { 'name': 'mean_ad', 'type': 'float', 'value': testResult.meanAbsoluteDeviation},
        { 'name': 'median_ad', 'type': 'float', 'value': testResult.medianAbsoluteDeviation},
        { 'name': 'modified_z_score', 'type': 'float', 'value': testResult.modifiedZScore},
        { 'name': 'expected_value', 'type': 'float', 'value': testResult.expectedValue},
        { 'name': 'expected_value_upper_bound', 'type': 'float', 'value': testResult.expectedValueUpperBoundary},
        { 'name': 'expected_value_lower_bound', 'type': 'float', 'value': testResult.expectedValueLowerBoundary},
        { 'name': 'deviation', 'type': 'float', 'value': testResult.deviation},
        { 'name': 'is_anomalous', 'type': 'boolean', 'value': testResult.isAnomaly},
        { 'name': 'test_id', 'type': 'string', 'value': self._testId},
        { 'name': 'execution_id', 'type': 'string', 'value': self._executionId},
      ]

      testResultQuery = getInsertQuery(valueSets, MaterializationType.TEST_RESULTS)
      return self._querySnowflake.execute(
          QuerySnowflakeRequestDto(testResultQuery), QuerySnowflakeAuthDto(jwt))

    def _insertAlertEntry(self, testType, message, jwt):
      valueSets = [
        { 'name': 'id', 'type': 'string', 'value': str(uuid.uuid4())},
        { 'name': 'test_type', 'type': 'string', 'value': testType},
        { 'name': 'message', 'type': 'string', 'value': message},
        { 'name': 'test_id', 'type': 'string', 'value': self._testId},
        { 'name': 'execution_id', 'type': 'string', 'value': self._executionId},
      ]

      testAlertQuery = getInsertQuery(
          valueSets, MaterializationType.ALERTS)
      return self._querySnowflake.execute(
          QuerySnowflakeRequestDto(testAlertQuery), QuerySnowflakeAuthDto(jwt))

    def _getTestEntry(self, jwt):
      query = getTestQuery(self._testId)

      return self._querySnowflake.execute(QuerySnowflakeRequestDto(query), QuerySnowflakeAuthDto(jwt))

    def execute(self, request: ExecuteTestRequestDto, auth: ExecuteTestAuthDto) -> ExecuteTestResponseDto:
        try:
            self._testId = request.testId
            self._executionId = str(uuid.uuid4())
            
            test = self._getTestEntry(auth.jwt)

            print(test)

            # newDataQuery = getRowCount(
            #     'snowflake_sample_data.information_schema.tables', 'tpcds_sf100tcl', 'warehouse')
            # newDataQueryResult = self._querySnowflake.execute(
            #     QuerySnowflakeRequestDto(newDataQuery), QuerySnowflakeAuthDto(jwt))

            # # todo - filter for non anomaly history values
            # # todo - what if empty
            # # what if mat does not exist
            # historyDataQueryResult = self._querySnowflake.execute(
            #     QuerySnowflakeRequestDto(newDataQuery), QuerySnowflakeAuthDto(auth.jwt))

            # if not newDataQueryResult.success:
            #     raise Exception(newDataQueryResult.error)
            # if not newDataQueryResult.value:
            #     raise Exception('No new data received')

            # result = None
            # anomalyMessage = None
            # if request.testType == ModelType.ROW_COUNT.value:
            #     result = RowCountModel(
            #         [1, 2, 3], [100, 101, 102, 103], request.threshold).run()
            #     anomalyMessage = AnomalyMessage.ROW_COUNT.value
            # else:
            #     raise Exception('Test type mismatch')

            # executionEntryInsertResult = self._insertExecutionEntry(auth.jwt)
            # historyEntryInsertResult = self._insertHistoryEntry(result.newDatapoint, result.isAnomaly, auth.jwt)
            # resultEntryInsertResult = self._insertResultEntry(result, auth.jwt)
            
            # if result.isAnomaly:
            #   alertEntryInsertResult = self._insertAlertEntry(result.type, anomalyMessage, auth.jwt)



            return Result.ok(result)

        except Exception as e:
            logger.error(e)
            return Result.fail(e)
