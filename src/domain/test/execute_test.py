from dataclasses import dataclass
from enum import Enum
import json
from typing import Any, Union
from unittest import TestSuite, case
import jwt
from numpy import integer
from domain.services.models.cito_data_query import CitoTableType, getHistoryQuery, getInsertQuery, getTestQuery
from domain.services.models.new_column_data_query import getCardinalityQuery, getDistributionQuery, getNullnessQuery, getUniquenessQuery, getFreshnessQuery as getColumnFreshnessQuery
from domain.services.models.new_materialization_data_query import MaterializationType, getColumnCountQuery, getFreshnessQuery, getRowCountQuery
from domain.value_types.statistical_model import ResultDto, CommonModel
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
    ColumnDistribution = 'ColumnDistribution'
    MaterializationRowCount = 'MaterializationRowCount'
    MaterializationColumnCount = 'MaterializationColumnCount'
    MaterializationFreshness = 'MaterializationFreshness'


def getAnomalyMessage(targetResourceId: str, databaseName: str, schemaName: str, materializationName: str, columnName: Union[str, None], testType: TestType):
    targetResourceUrlTemplate = f'__base_url__?targetResourceId={targetResourceId}&ampisColumn={not not columnName}'

    if(testType == TestType.ColumnFreshness):
        return f"Freshness deviation for column <{targetResourceUrlTemplate}|{databaseName}.{schemaName}.{materializationName}{f'.{columnName}' if columnName else ''}> detected"
    elif(testType == TestType.ColumnDistribution):
        return f"Distribution deviation for column <{targetResourceUrlTemplate}|{databaseName}.{schemaName}.{materializationName}{f'.{columnName}' if columnName else ''}> detected"
    elif(testType == TestType.ColumnCardinality):
        return f"Cardinality deviation for column <{targetResourceUrlTemplate}|{databaseName}.{schemaName}.{materializationName}{f'.{columnName}' if columnName else ''}> detected"
    elif(testType == TestType.ColumnNullness):
        return f"Nullness deviation for column <{targetResourceUrlTemplate}|{databaseName}.{schemaName}.{materializationName}{f'.{columnName}' if columnName else ''}> detected"
    elif(testType == TestType.ColumnUniqueness):
        return f"Uniqueness deviation for column <{targetResourceUrlTemplate}|{databaseName}.{schemaName}.{materializationName}{f'.{columnName}' if columnName else ''}> detected"
    elif(testType == TestType.MaterializationColumnCount):
        return f"Column count deviation for materialization <{targetResourceUrlTemplate}|{databaseName}.{schemaName}.{materializationName}{f'.{columnName}' if columnName else ''}> detected"
    elif(testType == TestType.MaterializationRowCount):
        return f"Row count deviation for materialization <{targetResourceUrlTemplate}|{databaseName}.{schemaName}.{materializationName}{f'.{columnName}' if columnName else ''}> detected"
    elif(testType == TestType.MaterializationFreshness):
        return f"Freshness deviation for materialization <{targetResourceUrlTemplate}|{databaseName}.{schemaName}.{materializationName}{f'.{columnName}' if columnName else ''}> detected"

@dataclass
class TestSpecificData:
    executedOn: str
    isAnomolous: bool
    modifiedZScore: float
    deviation: float


@dataclass
class AlertSpecificData:
    alertId: str
    message: str
    value: float
    expectedUpperBound: float
    expectedLowerBound: float
    databaseName: str
    schemaName: str
    materializationName: str
    materializationType: str
    columnName: Union[str, None]


@dataclass
class TestExecutionResult:
    testSuiteId: str
    testType: str
    threshold: integer
    executionFrequency: integer
    executionId: str
    isWarmup: bool
    testSpecificData: Union[TestSpecificData, None]
    alertSpecificData: Union[AlertSpecificData, None]
    targetResourceId: str
    organizationId: str


@dataclass
class ExecuteTestRequestDto:
    testSuiteId: str
    targetOrganizationId: str


@dataclass
class ExecuteTestAuthDto:
    jwt: str

ExecuteTestResponseDto = Result[TestExecutionResult]


class ExecuteTest(IUseCase):

    _MIN_HISTORICAL_DATA_NUMBER_TEST_CONDITION = 5

    _testSuiteId: str
    _testDefinition: dict[str, Any]

    _targetOrganizationId: str
    _executionId: str
    _jwt: str

    def __init__(self, integrationApiRepo: IIntegrationApiRepo, querySnowflake: QuerySnowflake) -> None:
        self._integrationApiRepo = integrationApiRepo
        self._querySnowflake = querySnowflake

    def _insertExecutionEntry(self, executedOn: str):
        valueSets = [
            {'name': 'id', 'value': self._executionId, 'type': 'string'},
            {'name': 'executedOn', 'value': executedOn, 'type': 'timestamp_ntz'},
            {'name': 'testSuiteId', 'value': self._testSuiteId, 'type': 'string'},
        ]

        executionQuery = getInsertQuery(
            valueSets, CitoTableType.Executions)
        executionEntryInsertResult = self._querySnowflake.execute(
            QuerySnowflakeRequestDto(executionQuery, self._targetOrganizationId), QuerySnowflakeAuthDto(self._jwt))

        if not executionEntryInsertResult.success:
            raise Exception(executionEntryInsertResult.error)

    def _insertHistoryEntry(self, value: str, isAnomaly: bool, alertId: Union[str, None]):
        valueSets = [
            {'name': 'id', 'type': 'string', 'value': str(uuid.uuid4())},
            {'name': 'test_type', 'type': 'string',
                'value': self._testDefinition['TEST_TYPE']},
            {'name': 'value', 'type': 'float', 'value': value},
            {'name': 'is_anomaly', 'type': 'boolean',
                'value': 'true' if isAnomaly else 'false'},
            {'name': 'user_feedback_is_anomaly', 'type': 'integer', 'value': -1},
            {'name': 'test_id', 'type': 'string', 'value': self._testSuiteId},
            {'name': 'execution_id', 'type': 'string', 'value': self._executionId},
            {'name': 'alert_id', 'type': 'string', 'value': alertId},
        ]

        testHistoryQuery = getInsertQuery(
            valueSets, CitoTableType.TestHistory)
        historyEntryInsertResult = self._querySnowflake.execute(
            QuerySnowflakeRequestDto(testHistoryQuery, self._targetOrganizationId), QuerySnowflakeAuthDto(self._jwt))

        if not historyEntryInsertResult.success:
            raise Exception(historyEntryInsertResult.error)

    def _insertResultEntry(self, testResult: ResultDto):
        valueSets = [
            {'name': 'id', 'type': 'string', 'value': str(uuid.uuid4())},
            {'name': 'test_type', 'type': 'string',
                'value': self._testDefinition['TEST_TYPE']},
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
            {'name': 'is_anomalous', 'type': 'boolean',
                'value': testResult.isAnomaly},
            {'name': 'test_id', 'type': 'string', 'value': self._testSuiteId},
            {'name': 'execution_id', 'type': 'string', 'value': self._executionId},
        ]

        testResultQuery = getInsertQuery(
            valueSets, CitoTableType.TestResults)
        resultEntryInsertResult = self._querySnowflake.execute(
            QuerySnowflakeRequestDto(testResultQuery, self._targetOrganizationId), QuerySnowflakeAuthDto(self._jwt))

        if not resultEntryInsertResult.success:
            raise Exception(resultEntryInsertResult.error)

    def _insertAlertEntry(self, id, message: str):
        valueSets = [
            {'name': 'id', 'type': 'string', 'value': id},
            {'name': 'test_type', 'type': 'string',
                'value': self._testDefinition['TEST_TYPE']},
            {'name': 'message', 'type': 'string', 'value': message},
            {'name': 'test_id', 'type': 'string', 'value': self._testSuiteId},
            {'name': 'execution_id', 'type': 'string', 'value': self._executionId},
        ]

        testAlertQuery = getInsertQuery(
            valueSets, CitoTableType.Alerts)
        alertEntryInsertResult = self._querySnowflake.execute(
            QuerySnowflakeRequestDto(testAlertQuery, self._targetOrganizationId), QuerySnowflakeAuthDto(self._jwt))

        if not alertEntryInsertResult.success:
            raise Exception(alertEntryInsertResult.error)

    def _getTestEntry(self) -> QuerySnowflakeResponseDto:
        query = getTestQuery(self._testSuiteId)

        return self._querySnowflake.execute(QuerySnowflakeRequestDto(query, self._targetOrganizationId), QuerySnowflakeAuthDto(self._jwt))

    def _getHistoricalData(self) -> QuerySnowflakeResponseDto:
        query = getHistoryQuery(self._testSuiteId)
        getHistoricalDataResult = self._querySnowflake.execute(
            QuerySnowflakeRequestDto(query, self._targetOrganizationId), QuerySnowflakeAuthDto(self._jwt))

        if not getHistoricalDataResult.success:
            raise Exception(getHistoricalDataResult.error)
        if not getHistoricalDataResult.value:
            raise Exception(
                'Sf query error - operation: history data')

        return [element['VALUE']
                for element in getHistoricalDataResult.value.content[self._targetOrganizationId]]

    def _getNewData(self, query):
        getNewDataResult = self._querySnowflake.execute(
            QuerySnowflakeRequestDto(query, self._targetOrganizationId), QuerySnowflakeAuthDto(self._jwt))

        if not getNewDataResult.success:
            raise Exception(getNewDataResult.error)
        if not getNewDataResult.value:
            raise Exception('Sf query error - operation: new data')

        newData = getNewDataResult.value.content[self._targetOrganizationId]
        if(len(newData) != 1):
            raise Exception(
                'More than one or no matching new data entries found')

        return newData

    def _runModel(self, threshold: integer, newData: float, historicalData: list[float]) -> ResultDto:
        return CommonModel(newData, historicalData, threshold).run()

    def _runTest(self, newDataPoint, historicalData: list[float]):
        databaseName = self._testDefinition['DATABASE_NAME']
        schemaName = self._testDefinition['SCHEMA_NAME']
        materializationName = self._testDefinition['MATERIALIZATION_NAME']
        materializationType = self._testDefinition['MATERIALIZATION_TYPE']
        columnName = self._testDefinition['COLUMN_NAME']
        testSuiteId = self._testDefinition['ID']
        threshold = self._testDefinition['THRESHOLD']
        executionFrequency = self._testDefinition['EXECUTION_FREQUENCY']
        targetResourceId = self._testDefinition['TARGET_RESOURCE_ID']

        if(len(historicalData) <= self._MIN_HISTORICAL_DATA_NUMBER_TEST_CONDITION):
            self._insertHistoryEntry(
                newDataPoint, False)

            return TestExecutionResult(testSuiteId, self._testDefinition['TEST_TYPE'], threshold, executionFrequency, self._executionId, True, None, None, self._targetOrganizationId)

        testResult = self._runModel(
            threshold, newDataPoint, historicalData)

        self._insertExecutionEntry(
            testResult.executedOn)

        self._insertResultEntry(testResult)

        anomalyMessage = getAnomalyMessage(targetResourceId, databaseName, schemaName, materializationName, columnName, self._testDefinition['TEST_TYPE'])

        alertSpecificData = None
        alertId = None
        if testResult.isAnomaly:
            alertId = str(uuid.uuid4())
            self._insertAlertEntry(
                alertId, anomalyMessage)

            alertSpecificData = AlertSpecificData(alertId, anomalyMessage, newDataPoint, testResult.expectedValueUpperBound,
                                                  testResult.expectedValueLowerBound, databaseName, schemaName, materializationName, materializationType, columnName)

        testSpecificData = TestSpecificData(
            testResult.executedOn, testResult.isAnomaly, testResult.modifiedZScore, testResult.deviation)

        self._insertHistoryEntry(
            newDataPoint, testResult.isAnomaly, alertId)

        return TestExecutionResult(testSuiteId, self._testDefinition['TEST_TYPE'], threshold, executionFrequency, self._executionId, False, testSpecificData, alertSpecificData, targetResourceId, self._targetOrganizationId)

    def _runMaterializationRowCountTest(self) -> TestExecutionResult:
        databaseName = self._testDefinition['DATABASE_NAME']
        schemaName = self._testDefinition['SCHEMA_NAME']
        materializationName = self._testDefinition['MATERIALIZATION_NAME']
        materializationType = self._testDefinition['MATERIALIZATION_TYPE']

        newDataQuery = getRowCountQuery(
            databaseName, schemaName, materializationName, MaterializationType[materializationType])

        newData = self._getNewData(newDataQuery)

        newDataPoint = newData[0]['ROW_COUNT']

        historicalData = self._getHistoricalData()

        testResult = self._runTest(
            newDataPoint, historicalData)

        return testResult

    def _runMaterializationColumnCountTest(self) -> TestExecutionResult:
        databaseName = self._testDefinition['DATABASE_NAME']
        schemaName = self._testDefinition['SCHEMA_NAME']
        materializationName = self._testDefinition['MATERIALIZATION_NAME']

        newDataQuery = getColumnCountQuery(
            databaseName, schemaName, materializationName)

        newData = self._getNewData(newDataQuery)

        newDataPoint = newData[0]['COLUMN_COUNT']

        historicalData = self._getHistoricalData()

        testResult = self._runTest(
            newDataPoint, historicalData)

        return testResult

    def _runMaterializationFreshnessTest(self) -> TestExecutionResult:
        databaseName = self._testDefinition['DATABASE_NAME']
        schemaName = self._testDefinition['SCHEMA_NAME']
        materializationName = self._testDefinition['MATERIALIZATION_NAME']
        materializationType = self._testDefinition['MATERIALIZATION_TYPE']

        newDataQuery = getFreshnessQuery(
            databaseName, schemaName, materializationName, materializationType)

        newData = self._getNewData(newDataQuery)

        newDataPoint = newData[0]['TIME_DIFF']

        historicalData = self._getHistoricalData()

        testResult = self._runTest(
            newDataPoint, historicalData)

        return testResult

    def _runColumnCardinalityTest(self) -> TestExecutionResult:
        databaseName = self._testDefinition['DATABASE_NAME']
        schemaName = self._testDefinition['SCHEMA_NAME']
        materializationName = self._testDefinition['MATERIALIZATION_NAME']
        columnName = self._testDefinition['COLUMN_NAME']

        newDataQuery = getCardinalityQuery(
            databaseName, schemaName, materializationName, columnName)

        newData = self._getNewData(newDataQuery)

        newDataPoint = newData[0]['DISTINCT_VALUE_COUNT']

        historicalData = self._getHistoricalData()

        testResult = self._runTest(
            newDataPoint, historicalData)

        return testResult

    def _runColumnDistributionTest(self) -> TestExecutionResult:
        databaseName = self._testDefinition['DATABASE_NAME']
        schemaName = self._testDefinition['SCHEMA_NAME']
        materializationName = self._testDefinition['MATERIALIZATION_NAME']
        columnName = self._testDefinition['COLUMN_NAME']

        newDataQuery = getDistributionQuery(
            databaseName, schemaName, materializationName, columnName)

        newData = self._getNewData(newDataQuery)

        newDataPoint = newData[0]['MEDIAN']

        historicalData = self._getHistoricalData()

        testResult = self._runTest(
            newDataPoint, historicalData)

        return testResult

    def _runColumnFreshnessTest(self) -> TestExecutionResult:
        databaseName = self._testDefinition['DATABASE_NAME']
        schemaName = self._testDefinition['SCHEMA_NAME']
        materializationName = self._testDefinition['MATERIALIZATION_NAME']
        columnName = self._testDefinition['COLUMN_NAME']

        newDataQuery = getColumnFreshnessQuery(
            databaseName, schemaName, materializationName, columnName)

        newData = self._getNewData(newDataQuery)

        newDataPoint = newData[0]['TIME_DIFF']

        historicalData = self._getHistoricalData()

        testResult = self._runTest(
            newDataPoint, historicalData)

        return testResult

    def _runColumnNullnessTest(self) -> TestExecutionResult:
        databaseName = self._testDefinition['DATABASE_NAME']
        schemaName = self._testDefinition['SCHEMA_NAME']
        materializationName = self._testDefinition['MATERIALIZATION_NAME']
        columnName = self._testDefinition['COLUMN_NAME']

        newDataQuery = getNullnessQuery(
            databaseName, schemaName, materializationName, columnName)

        newData = self._getNewData(newDataQuery)

        newDataPoint = newData[0]['NULLNESS_RATE']

        historicalData = self._getHistoricalData()

        testResult = self._runTest(
            newDataPoint, historicalData)

        return testResult

    def _runColumnUniquenessTest(self) -> TestExecutionResult:
        databaseName = self._testDefinition['DATABASE_NAME']
        schemaName = self._testDefinition['SCHEMA_NAME']
        materializationName = self._testDefinition['MATERIALIZATION_NAME']
        columnName = self._testDefinition['COLUMN_NAME']

        newDataQuery = getUniquenessQuery(
            databaseName, schemaName, materializationName, columnName)

        newData = self._getNewData(newDataQuery)

        newDataPoint = newData[0]['UNIQUENESS_RATE']

        historicalData = self._getHistoricalData()

        testResult = self._runTest(
            newDataPoint, historicalData)

        return testResult

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
            self._testDefinition = self._getTestDefinition()

            testTypeKey = 'TEST_TYPE'

            if self._testDefinition[testTypeKey] == TestType.MaterializationRowCount.value:
                testResult = self._runMaterializationRowCountTest()
            elif self._testDefinition[testTypeKey] == TestType.MaterializationColumnCount.value:
                testResult = self._runMaterializationColumnCountTest()
            elif self._testDefinition[testTypeKey] == TestType.MaterializationFreshness.value:
                testResult = self._runMaterializationFreshnessTest()
            elif self._testDefinition[testTypeKey] == TestType.ColumnCardinality.value:
                testResult = self._runColumnCardinalityTest()
            elif self._testDefinition[testTypeKey] == TestType.ColumnDistribution.value:
                testResult = self._runColumnDistributionTest()
            elif self._testDefinition[testTypeKey] == TestType.ColumnFreshness.value:
                testResult = self._runColumnFreshnessTest()
            elif self._testDefinition[testTypeKey] == TestType.ColumnNullness.value:
                testResult = self._runColumnNullnessTest()
            elif self._testDefinition[testTypeKey] == TestType.ColumnUniqueness.value:
                testResult = self._runColumnUniquenessTest()
            else:
                raise Exception('Test type mismatch')

            return Result.ok(testResult)

        except Exception as e:
            logger.error(e)
            return Result.fail(e)
