from dataclasses import dataclass
from enum import Enum
import json
from typing import Any, Union
from unittest import case
import jwt
from numpy import integer
from domain.services.models.cito_data_query import CitoTableType, getHistoryQuery, getInsertQuery, getTestQuery
from domain.services.models.new_materialization_data_query import MaterializationType, getRowCountQuery
from domain.value_types.statistical_model import ResultDto, RowCountModel
from src.domain.integration_api.snowflake.query_snowflake import QuerySnowflake, QuerySnowflakeAuthDto, QuerySnowflakeRequestDto, QuerySnowflakeResponseDto
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
    targetOrganizationId: str


@dataclass
class ExecuteTestAuthDto:
    jwt: str
    organizationId: str


ExecuteTestResponseDto = Result[ResultDto]


class ExecuteTest(IUseCase):

    _MIN_HISTORICAL_DATA_NUMBER_TEST_CONDITION = 5
    _testSuiteId: str
    _targetOrganizationId: str
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

      executionQuery = getInsertQuery(
          valueSets, CitoTableType.Executions)
      return self._querySnowflake.execute(
          QuerySnowflakeRequestDto(executionQuery, self._targetOrganizationId), QuerySnowflakeAuthDto(self._jwt))

    def _insertHistoryEntry(self, testType: TestType, value: str, isAnomaly: bool):
      valueSets = [
        {'name': 'id', 'type': 'string', 'value': str(uuid.uuid4())},
        {'name': 'test_type', 'type': 'string', 'value': testType.value},
        {'name': 'value', 'type': 'float', 'value': value},
        {'name': 'is_anomaly', 'type': 'boolean', 'value': isAnomaly},
        {'name': 'user_feedback_is_anomaly', 'type': 'integer', 'value': -1},
        {'name': 'test_id', 'type': 'string', 'value': self._testSuiteId},
        {'name': 'execution_id', 'type': 'string', 'value': self._executionId},
      ]

      testHistoryQuery = getInsertQuery(
          valueSets, CitoTableType.TestHistory)
      return self._querySnowflake.execute(
          QuerySnowflakeRequestDto(testHistoryQuery, self._targetOrganizationId), QuerySnowflakeAuthDto(self._jwt))

    def _insertResultEntry(self, testResult: ResultDto):
      valueSets = [
        {'name': 'id', 'type': 'string', 'value': str(uuid.uuid4())},
        {'name': 'test_type', 'type': 'string', 'value': testResult.type},
        {'name': 'mean_ad', 'type': 'float',
            'value': testResult.meanAbsoluteDeviation},
        {'name': 'median_ad', 'type': 'float',
            'value': testResult.medianAbsoluteDeviation},
        {'name': 'modified_z_score', 'type': 'float',
            'value': testResult.modifiedZScore},
        {'name': 'expected_value', 'type': 'float',
            'value': testResult.expectedValue},
        {'name': 'expected_value_upper_bound', 'type': 'float',
            'value': testResult.expectedValueUpperBound},
        {'name': 'expected_value_lower_bound', 'type': 'float',
            'value': testResult.expectedValueLowerBound},
        {'name': 'deviation', 'type': 'float', 'value': testResult.deviation},
        {'name': 'is_anomalous', 'type': 'boolean', 'value': testResult.isAnomaly},
        {'name': 'test_id', 'type': 'string', 'value': self._testSuiteId},
        {'name': 'execution_id', 'type': 'string', 'value': self._executionId},
      ]

      testResultQuery = getInsertQuery(
          valueSets, CitoTableType.TestResults)
      return self._querySnowflake.execute(
          QuerySnowflakeRequestDto(testResultQuery, self._targetOrganizationId), QuerySnowflakeAuthDto(self._jwt))

    def _insertAlertEntry(self, testType: TestType, message: str):
      valueSets = [
        {'name': 'id', 'type': 'string', 'value': str(uuid.uuid4())},
        {'name': 'test_type', 'type': 'string', 'value': testType.value},
        {'name': 'message', 'type': 'string', 'value': message},
        {'name': 'test_id', 'type': 'string', 'value': self._testSuiteId},
        {'name': 'execution_id', 'type': 'string', 'value': self._executionId},
      ]

      testAlertQuery = getInsertQuery(
          valueSets, CitoTableType.Alerts)
      return self._querySnowflake.execute(
          QuerySnowflakeRequestDto(testAlertQuery, self._targetOrganizationId), QuerySnowflakeAuthDto(self._jwt))

    def _getTestEntry(self) -> QuerySnowflakeResponseDto:
      query = getTestQuery(self._testSuiteId)

      return self._querySnowflake.execute(QuerySnowflakeRequestDto(query, self._targetOrganizationId), QuerySnowflakeAuthDto(self._jwt))

    def _getHistoricalData(self) -> QuerySnowflakeResponseDto:
      query = getHistoryQuery(self._testSuiteId)
      return self._querySnowflake.execute(
        QuerySnowflakeRequestDto(query, self._targetOrganizationId), QuerySnowflakeAuthDto(self._jwt))

    def _runTest(self, testType: TestType, threshold: integer, newData: list[Any], historicalData: list[float]) -> ResultDto:
      if testType == TestType.MaterializationRowCount:
        return RowCountModel(newData, historicalData, threshold).run()
      else:
          raise Exception('Test type mismatch')
     
    def _runMaterializationRowCountTest(self, testDefinition: dict[str: Any]):
      newDataQuery = getRowCountQuery(
        testDefinition['DATABASE_NAME'], testDefinition['SCHEMA_NAME'], testDefinition['MATERIALIZATION_NAME'], testDefinition['MATERIALIZATION_TYPE'])
      getNewDataResult = self._querySnowflake.execute(
          QuerySnowflakeRequestDto(newDataQuery, self._targetOrganizationId), QuerySnowflakeAuthDto(self._jwt))

      if not getNewDataResult.success:
        raise Exception(getNewDataResult.error)
      if not getNewDataResult.value:
        raise Exception('Sf query error - operation: new row count data')
    
      newData = getNewDataResult.value.content[self._targetOrganizationId]
      if(len(newData) != 1):
        raise Exception('More than one or no matching row_count test found')

      getHistoricalDataResult = self._getHistoricalData()

      if not getHistoricalDataResult.success:
          raise Exception(getHistoricalDataResult.error)
      if not getHistoricalDataResult.value:
          raise Exception('Sf query error - operation: history row count data')

      historicalData = getHistoricalDataResult.value.content[self._targetOrganizationId]
      if(len(historicalData) <= self._MIN_HISTORICAL_DATA_NUMBER_TEST_CONDITION):
        historyEntryInsertResult = self._insertHistoryEntry(newData, False, self._jwt)
              
        if not historyEntryInsertResult.success:
          raise Exception(historyEntryInsertResult.error)
        if not historyEntryInsertResult.value:
          raise Exception('Sf query error - operation: inserting new history data')

        return something
      
      testResult = self._runTest(testDefinition['TEST_TYPE'], testDefinition['THRESHOLD'], newData, historicalData)

      executionEntryInsertResult = self._insertExecutionEntry(self._jwt)
      historyEntryInsertResult = self._insertHistoryEntry(testResult.newDatapoint, testResult.isAnomaly, self._jwt)
      resultEntryInsertResult = self._insertResultEntry(testResult, self._jwt)
      
      if result.isAnomaly:
        alertEntryInsertResult = self._insertAlertEntry(result.type, AnomalyMessage.MaterializationRowCount, self._jwt)

      return something

    def _runMaterializationColumnCountTest(self):
      pass

    def _runMaterializationFreshnessTest(self):
      pass

    def _runColumnCardinalityTest(self):
      pass

    def _runColumnDistributionTest(self):
      pass

    def _runColumnFreshnessTest(self):
      pass

    def _runColumnNullnessTest(self):
      pass

    def _runColumnSortednessDecreasingTest(self):
      pass

    def _runColumnSortednessIncreasingTest(self):
      pass

    def _runColumnUniquenessTest(self):
      pass

    def _getTestDefinition(self):
      getTestEntryResult = self._getTestEntry()

      if not getTestEntryResult.success:
        raise Exception(getTestEntryResult.error)
      if not getTestEntryResult.value:
        raise Exception(f'Sf query error - operation: test entry')
      
      organizationResult = getTestEntryResult.value.content[self._targetOrganizationId]
      if not len(organizationResult) == 1:
        raise Exception('More than one or no test found')

      return organizationResult[0]

    def execute(self, request: ExecuteTestRequestDto, auth: ExecuteTestAuthDto) -> ExecuteTestResponseDto:
        try:
            self._testSuiteId = request.testSuiteId
            self._targetOrganizationId = request.targetOrganizationId
            self._executionId = str(uuid.uuid4())
            self._jwt = auth.jwt

            testDefinition = self._getTestDefinition()

            testType = testDefinition['TEST_TYPE']

            if testType == TestType.MaterializationRowCount.value:
              testResult = self._runMaterializationRowCountTest()
            elif testType == TestType.MaterializationColumnCount.value:
              testResult = self._runMaterializationColumnCountTest()
            elif testType == TestType.MaterializationFreshness.value:
              testResult = self._runMaterializationFreshnessTest()
            elif testType == TestType.ColumnCardinality.value:
              testResult = self._runColumnCardinalityTest()
            elif testType == TestType.ColumnDistribution.value:
              testResult = self._runColumnDistributionTest()
            elif testType == TestType.ColumnFreshness.value:
              testResult = self._runColumnFreshnessTest()
            elif testType == TestType.ColumnNullness.value:
              testResult = self._runColumnNullnessTest()
            elif testType == TestType.ColumnSortednessDecreasing.value:
              testResult = self._runColumnSortednessDecreasingTest()
            elif testType == TestType.ColumnSortednessIncreasing.value:
              testResult = self._runColumnSortednessIncreasingTest()
            elif testType == TestType.ColumnUniqueness.value:
              testResult = self._runColumnUniquenessTest()
            else:
              raise Exception('Test type mismatch')




            """ return
            testSuiteId
            testType
            executionId
            executedOn
            isAnomolous
            modified z score
            deviation
            """


            # return Result.ok(result)
            return Result.ok('result')

        except Exception as e:
            logger.error(e)
            return Result.fail(e)
