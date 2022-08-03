from dataclasses import dataclass
from enum import Enum
import json
from typing import Any, Union
from unittest import case
import jwt
from numpy import integer
from domain.services.models.cito_data_query import MaterializationType, getHistoryQuery, getInsertQuery, getTestQuery
from domain.services.models.new_materialization_data_query import getRowCountQuery
from domain.value_types.statistical_model import ResultDto, RowCountModel
from src.domain.integration_api.snowflake.query_snowflake import QuerySnowflake, QuerySnowflakeAuthDto, QuerySnowflakeRequestDto
from src.domain.services.use_case import IUseCase
from src.domain.integration_api.i_integration_api_repo import IIntegrationApiRepo
import logging
import datetime
import uuid

from src.domain.value_types.transient_types.result import Result

logger = logging.getLogger(__name__)

class TestType(Enum):
  ColumnFreshness = 'ColumnFreshness'
  ColumnCardinality = 'ColumnCardinality'
  ColumnUniqueness = 'ColumnUniqueness'
  ColumnNullness = 'ColumnNullness'
  ColumnSortednessIncreasing = 'ColumnSortednessIncreasing'
  ColumnSortednessDecreasing = 'ColumnSortednessDecreasing'
  ColumnDistribution = 'ColumnDistribution'
  MaterializationRowCount = 'MaterializationRowCount'
  MaterializationColumnCount = 'MaterializationColumnCount'
  MaterializationFreshness = 'MaterializationFreshness'

class AnomalyMessage(Enum):
  ColumnFreshness = 'todo - anomaly message1'
  ColumnCardinality = 'todo - anomaly message2'
  ColumnUniqueness = 'todo - anomaly message0'
  ColumnNullness = 'todo - anomaly message3'
  ColumnSortednessIncreasing = 'todo - anomaly message4'
  ColumnSortednessDecreasing = 'todo - anomaly message5'
  ColumnDistribution = 'todo - anomaly message6'
  MaterializationRowCount = 'todo - anomaly message7'
  MaterializationColumnCount = 'todo - anomaly message8'
  MaterializationFreshness = 'todo - anomaly message9'

@dataclass
class ExecuteTestRequestDto:
    testSuiteId: str


@dataclass
class ExecuteTestAuthDto:
    jwt: str
    organizationId: str


ExecuteTestResponseDto = Result[ResultDto]


class ExecuteTest(IUseCase):

    _testSuiteId: str
    _executionId: str
    _jwt: str

    def __init__(self, integrationApiRepo: IIntegrationApiRepo, querySnowflake: QuerySnowflake) -> None:
        self._integrationApiRepo = integrationApiRepo
        self._querySnowflake = querySnowflake

    def _insertExecutionEntry(self):
      executedOn = datetime.datetime.utcnow().isoformat()

      valueSets = [
        {'name': 'id', 'value': self._executionId, 'type': 'string'},
        {'name': 'executedOn', 'value': executedOn, 'type': 'timestamp_tz'},
        {'name': 'testSuiteId', 'value': self._testSuiteId, 'type': 'string'},
      ]

      executionQuery = getInsertQuery(valueSets, MaterializationType.EXECUTIONS)
      return self._querySnowflake.execute(
          QuerySnowflakeRequestDto(executionQuery), QuerySnowflakeAuthDto(self._jwt))

    def _insertHistoryEntry(self, testType: TestType, value: str, isAnomaly: bool):
      valueSets = [
        { 'name': 'id', 'type': 'string', 'value': str(uuid.uuid4()) },
        { 'name': 'test_type', 'type': 'string','value': testType.value },
        { 'name': 'value', 'type': 'float', 'value': value },
        { 'name': 'is_anomaly', 'type': 'boolean', 'value': isAnomaly },
        { 'name': 'user_feedback_is_anomaly', 'type': 'integer', 'value': -1 },
        { 'name': 'test_id', 'type': 'string', 'value': self._testSuiteId },
        { 'name': 'execution_id', 'type': 'string', 'value': self._executionId },
      ]

      testHistoryQuery = getInsertQuery(valueSets, MaterializationType.TEST_HISTORY)
      return self._querySnowflake.execute(
          QuerySnowflakeRequestDto(testHistoryQuery), QuerySnowflakeAuthDto(self._jwt))

    def _insertResultEntry(self, testResult: ResultDto):
      valueSets = [
        { 'name': 'id', 'type': 'string', 'value': str(uuid.uuid4()) },
        { 'name': 'test_type', 'type': 'string', 'value': testResult.type},
        { 'name': 'mean_ad', 'type': 'float', 'value': testResult.meanAbsoluteDeviation},
        { 'name': 'median_ad', 'type': 'float', 'value': testResult.medianAbsoluteDeviation},
        { 'name': 'modified_z_score', 'type': 'float', 'value': testResult.modifiedZScore},
        { 'name': 'expected_value', 'type': 'float', 'value': testResult.expectedValue},
        { 'name': 'expected_value_upper_bound', 'type': 'float', 'value': testResult.expectedValueUpperBound},
        { 'name': 'expected_value_lower_bound', 'type': 'float', 'value': testResult.expectedValueLowerBound},
        { 'name': 'deviation', 'type': 'float', 'value': testResult.deviation},
        { 'name': 'is_anomalous', 'type': 'boolean', 'value': testResult.isAnomaly},
        { 'name': 'test_id', 'type': 'string', 'value': self._testSuiteId},
        { 'name': 'execution_id', 'type': 'string', 'value': self._executionId},
      ]

      testResultQuery = getInsertQuery(valueSets, MaterializationType.TEST_RESULTS)
      return self._querySnowflake.execute(
          QuerySnowflakeRequestDto(testResultQuery), QuerySnowflakeAuthDto(self._jwt))

    def _insertAlertEntry(self, testType: TestType, message: str):
      valueSets = [
        { 'name': 'id', 'type': 'string', 'value': str(uuid.uuid4())},
        { 'name': 'test_type', 'type': 'string', 'value': testType.value},
        { 'name': 'message', 'type': 'string', 'value': message},
        { 'name': 'test_id', 'type': 'string', 'value': self._testSuiteId},
        { 'name': 'execution_id', 'type': 'string', 'value': self._executionId},
      ]

      testAlertQuery = getInsertQuery(
          valueSets, MaterializationType.ALERTS)
      return self._querySnowflake.execute(
          QuerySnowflakeRequestDto(testAlertQuery), QuerySnowflakeAuthDto(self._jwt))

    def _getTestEntry(self):
      query = getTestQuery(self._testSuiteId)

      return self._querySnowflake.execute(QuerySnowflakeRequestDto(query), QuerySnowflakeAuthDto(self._jwt))

    def _getNewData(self, db: str, schema: str, materializationName: str, materializationType: str):
      newDataQuery = getRowCountQuery(db, schema, materializationName, materializationType)
      return self._querySnowflake.execute(
          QuerySnowflakeRequestDto(newDataQuery), QuerySnowflakeAuthDto(self._jwt))

    def _getHistoryData(self):
      query = getHistoryQuery(self._testSuiteId)
      return self._querySnowflake.execute(
        QuerySnowflakeRequestDto(query), QuerySnowflakeAuthDto(self._jwt))

    def _runTest(self, testType: TestType, threshold: integer, newData: list[Any], historicalData: list[float]) -> ResultDto:
      if testType == TestType.MaterializationRowCount:
        return RowCountModel(newData, historicalData, threshold).run()
      else:
          raise Exception('Test type mismatch')
      
    def execute(self, request: ExecuteTestRequestDto, auth: ExecuteTestAuthDto) -> ExecuteTestResponseDto:
        try:
            self._testSuiteId = request.testSuiteId
            self._executionId = str(uuid.uuid4())
            self._jwt = auth.jwt
            
            getTestEntryResult = self._getTestEntry()

            if not getTestEntryResult.success:
              raise Exception(getTestEntryResult.error)
            if not getTestEntryResult.value:
              raise Exception(f' Test with id {self._testSuiteId} not found')

            test = getTestEntryResult.value

            print(test)

            # getNewDataResult = self._getNewData('cito', 'public', 'test_suites', 'table')

            # if not getNewDataResult.success:
            #   raise Exception(getTestEntryResult.error)
            # if not getNewDataResult.value:
            #   raise Exception('todo - - implement specific to test type')

            # getHistoryDataResult = self._getHistoryData()

            # if not getHistoryDataResult.success:
            #     raise Exception(getHistoryDataResult.error)
            # if not getHistoryDataResult.value:
            #     raise Exception('todo - No history data received')

            # testResult = self._runTest(test.)

            # result = None
            # anomalyMessage = None


            # executionEntryInsertResult = self._insertExecutionEntry(auth.jwt)
            # historyEntryInsertResult = self._insertHistoryEntry(result.newDatapoint, result.isAnomaly, auth.jwt)
            # resultEntryInsertResult = self._insertResultEntry(result, auth.jwt)
            
            # if result.isAnomaly:
            #   alertEntryInsertResult = self._insertAlertEntry(result.type, anomalyMessage, auth.jwt)


            """ return
            testSuiteId
            testType
            executionId
            executedOn
            isAnomolous
            modified z score
            deviation
            """


            return Result.ok(result)

        except Exception as e:
            logger.error(e)
            return Result.fail(e)
