from dataclasses import dataclass, asdict
from datetime import datetime
import json
from typing import Any, Union
from cito_data_query import CitoTableType, getHistoryQuery, getInsertQuery, getTestQuery, getLastMatSchemaQuery
from new_column_data_query import getCardinalityQuery, getDistributionQuery, getNullnessQuery, getUniquenessQuery, getFreshnessQuery as getColumnFreshnessQuery
from new_materialization_data_query import MaterializationType, getColumnCountQuery, getFreshnessQuery, getRowCountQuery, getSchemaChangeQuery
from qual_model import ColumnDefinition, SchemaChangeModel, ResultDto as QualResultDto
from quant_model import ResultDto as QuantTestResultDto, CommonModel
from query_snowflake import QuerySnowflake, QuerySnowflakeAuthDto, QuerySnowflakeRequestDto, QuerySnowflakeResponseDto
from test_execution_result import QualTestAlertData, QualTestData, QualTestExecutionResult, QuantTestAlertData, QuantTestData, QuantTestExecutionResult, AnomalyData
from test_type import QuantColumnTest, QuantMatTest, QualMatTest
from use_case import IUseCase
import logging
import uuid

from result import Result

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def getAnomalyMessage(targetResourceId: str, databaseName: str, schemaName: str, materializationName: str, columnName: Union[str, None], testType: str):
    targetResourceUrlTemplate = f'__base_url__?targetResourceId={targetResourceId}&ampisColumn={not not columnName}'

    if (testType == QuantColumnTest.ColumnFreshness.value):
        return f"<{targetResourceUrlTemplate}|{databaseName}.{schemaName}.{materializationName}{f'.{columnName}' if columnName else ''}>"
    elif (testType == QuantColumnTest.ColumnDistribution.value):
        return f"<{targetResourceUrlTemplate}|{databaseName}.{schemaName}.{materializationName}{f'.{columnName}' if columnName else ''}>"
    elif (testType == QuantColumnTest.ColumnCardinality.value):
        return f"<{targetResourceUrlTemplate}|{databaseName}.{schemaName}.{materializationName}{f'.{columnName}' if columnName else ''}>"
    elif (testType == QuantColumnTest.ColumnNullness.value):
        return f"<{targetResourceUrlTemplate}|{databaseName}.{schemaName}.{materializationName}{f'.{columnName}' if columnName else ''}>"
    elif (testType == QuantColumnTest.ColumnUniqueness.value):
        return f"<{targetResourceUrlTemplate}|{databaseName}.{schemaName}.{materializationName}{f'.{columnName}' if columnName else ''}>"
    elif (testType == QuantMatTest.MaterializationColumnCount.value):
        return f"<{targetResourceUrlTemplate}|{databaseName}.{schemaName}.{materializationName}{f'.{columnName}' if columnName else ''}>"
    elif (testType == QuantMatTest.MaterializationRowCount.value):
        return f"<{targetResourceUrlTemplate}|{databaseName}.{schemaName}.{materializationName}{f'.{columnName}' if columnName else ''}>"
    elif (testType == QuantMatTest.MaterializationFreshness.value):
        return f"<{targetResourceUrlTemplate}|{databaseName}.{schemaName}.{materializationName}{f'.{columnName}' if columnName else ''}>"
    elif (testType == QualMatTest.MaterializationSchemaChange.value):
        return f"<{targetResourceUrlTemplate}|{databaseName}.{schemaName}.{materializationName}{f'.{columnName}' if columnName else ''}>"
    else:
        raise Exception('Unhandled anomaly message test type')


@dataclass
class ExecuteTestRequestDto:
    testSuiteId: str
    testType: Union[QuantColumnTest, QuantMatTest, QualMatTest]
    targetOrgId: Union[str, None]


@dataclass
class ExecuteTestAuthDto:
    jwt: str
    callerOrgId: Union[str, None]
    isSystemInternal: bool


@dataclass
class CustomThreshold:
    value: float
    mode: str


ExecuteTestResponseDto = Result[Union[QuantTestExecutionResult,
                                      QualTestExecutionResult]]


class ExecuteTest(IUseCase):

    _MIN_HISTORICAL_DATA_TEST_NUMBER_CONDITION = 10
    _MIN_HISTORICAL_DATA_DAY_NUMBER_CONDITION = 7

    _testSuiteId: str
    _testType: Union[QuantColumnTest, QuantMatTest, QualMatTest]
    _testDefinition: "dict[str, Any]"

    _targetOrgId: Union[str, None]
    _organizationId: str

    _executionId: str
    _jwt: str

    _requestLoggingInfo: str

    _querySnowflake: QuerySnowflake

    def __init__(self, querySnowflake: QuerySnowflake) -> None:
        self._querySnowflake = querySnowflake

    def _insertExecutionEntry(self, executedOn: str, tableType: CitoTableType):
        valueSets = [
            {'name': 'id', 'value': self._executionId, 'type': 'string'},
            {'name': 'executedOn', 'value': executedOn, 'type': 'timestamp_ntz'},
            {'name': 'testSuiteId', 'value': self._testSuiteId, 'type': 'string'},
        ]

        executionQuery = getInsertQuery(
            valueSets, tableType)
        executionEntryInsertResult = self._querySnowflake.execute(
            QuerySnowflakeRequestDto(executionQuery, self._targetOrgId), QuerySnowflakeAuthDto(self._jwt))

        if not executionEntryInsertResult.success:
            raise Exception(executionEntryInsertResult.error)

    def _insertQualHistoryEntry(self, value: "dict[str, ColumnDefinition]", isIdentical: bool, alertId: Union[str, None]):
        valueSets = [
            {'name': 'id', 'type': 'string', 'value': str(uuid.uuid4())},
            {'name': 'test_type', 'type': 'string',
             'value': self._testDefinition['TEST_TYPE']},
            {'name': 'value', 'type': 'string', 'value':  json.dumps(value)},
            {'name': 'is_identical', 'type': 'boolean',
             'value': 'true' if isIdentical else 'false'},
            {'name': 'test_suite_id', 'type': 'string', 'value': self._testSuiteId},
            {'name': 'execution_id', 'type': 'string', 'value':  self._executionId},
            {'name': 'alert_id', 'type': 'string', 'value':  alertId},
        ]

        testHistoryQuery = getInsertQuery(
            valueSets, CitoTableType.TestHistoryQual)
        historyEntryInsertResult = self._querySnowflake.execute(
            QuerySnowflakeRequestDto(testHistoryQuery, self._targetOrgId), QuerySnowflakeAuthDto(self._jwt))

        if not historyEntryInsertResult.success:
            raise Exception(historyEntryInsertResult.error)

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
            QuerySnowflakeRequestDto(testHistoryQuery, self._targetOrgId), QuerySnowflakeAuthDto(self._jwt))

        if not historyEntryInsertResult.success:
            raise Exception(historyEntryInsertResult.error)

    def _insertQualTestResultEntry(self, testResult: QualResultDto):
        valueSets = [
            {'name': 'id', 'type': 'string', 'value': str(uuid.uuid4())},
            {'name': 'test_type', 'type': 'string',
                'value': self._testDefinition['TEST_TYPE']},
            {'name': 'expected_value', 'type': 'string',
             'value': json.dumps(testResult.expectedValue) if testResult.expectedValue else None},
            {'name': 'deviation', 'type': 'string',
                'value': json.dumps([asdict(el) for el in testResult.deviations])},
            {'name': 'is_identical', 'type': 'boolean',
                'value': testResult.isIdentical},
            {'name': 'test_suite_id', 'type': 'string', 'value': self._testSuiteId},
            {'name': 'execution_id', 'type': 'string', 'value': self._executionId},
        ]

        testResultQuery = getInsertQuery(
            valueSets, CitoTableType.TestResultsQual)
        resultEntryInsertResult = self._querySnowflake.execute(
            QuerySnowflakeRequestDto(testResultQuery, self._targetOrgId), QuerySnowflakeAuthDto(self._jwt))

        if not resultEntryInsertResult.success:
            raise Exception(resultEntryInsertResult.error)

    def _insertResultEntry(self, testResult: QuantTestResultDto):
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
             'value': testResult.expectedValueUpper},
            {'name': 'expected_value_lower_bound', 'type': 'float',
             'value': testResult.expectedValueLower},
            {'name': 'deviation', 'type': 'float', 'value': testResult.deviation},
            {'name': 'is_anomalous', 'type': 'boolean',
                'value': testResult.anomaly.isAnomaly},
            {'name': 'test_id', 'type': 'string', 'value': self._testSuiteId},
            {'name': 'execution_id', 'type': 'string', 'value': self._executionId},
            {'name': 'importance', 'type': 'float',
                'value': testResult.anomaly.importance},
            {'name': 'bounds_interval_relative', 'type': 'float',
                'value': testResult.anomaly.boundsIntervalRelative},
        ]

        testResultQuery = getInsertQuery(
            valueSets, CitoTableType.TestResults)
        resultEntryInsertResult = self._querySnowflake.execute(
            QuerySnowflakeRequestDto(testResultQuery, self._targetOrgId), QuerySnowflakeAuthDto(self._jwt))

        if not resultEntryInsertResult.success:
            raise Exception(resultEntryInsertResult.error)

    def _insertAlertEntry(self, id, message: str, tableType: CitoTableType):
        valueSets = [
            {'name': 'id', 'type': 'string', 'value': id},
            {'name': 'test_type', 'type': 'string',
                'value': self._testDefinition['TEST_TYPE']},
            {'name': 'message', 'type': 'string', 'value': message},
            {'name': 'test_id', 'type': 'string', 'value': self._testSuiteId},
            {'name': 'execution_id', 'type': 'string', 'value': self._executionId},
        ]

        testAlertQuery = getInsertQuery(
            valueSets, tableType)
        alertEntryInsertResult = self._querySnowflake.execute(
            QuerySnowflakeRequestDto(testAlertQuery, self._targetOrgId), QuerySnowflakeAuthDto(self._jwt))

        if not alertEntryInsertResult.success:
            raise Exception(alertEntryInsertResult.error)

    def _getTestEntry(self) -> QuerySnowflakeResponseDto:
        query = getTestQuery(self._testSuiteId, self._testType)

        return self._querySnowflake.execute(QuerySnowflakeRequestDto(query, self._targetOrgId), QuerySnowflakeAuthDto(self._jwt))

    def _getHistoricalData(self) -> "list[tuple[str, float]]":
        query = getHistoryQuery(self._testSuiteId)
        getHistoricalDataResult = self._querySnowflake.execute(
            QuerySnowflakeRequestDto(query, self._targetOrgId), QuerySnowflakeAuthDto(self._jwt))

        if not getHistoricalDataResult.success:
            raise Exception(getHistoricalDataResult.error)
        if not getHistoricalDataResult.value:
            raise Exception(
                'Sf query error - operation: history data')

        return sorted([(element['EXECUTED_ON'], element['VALUE'])
                       for element in getHistoricalDataResult.value.content[self._organizationId]])

    def _getLastMatSchema(self) -> Union["dict[str, ColumnDefinition]", None]:
        query = getLastMatSchemaQuery(self._testSuiteId)
        queryResult = self._querySnowflake.execute(QuerySnowflakeRequestDto(
            query, self._targetOrgId), QuerySnowflakeAuthDto(self._jwt))

        if not queryResult.success:
            raise Exception(queryResult.error)
        if not queryResult.value:
            raise Exception(
                'Sf query error - operation: last mat schema')

        return (json.loads(queryResult.value.content[self._organizationId][0]['VALUE']) if len(queryResult.value.content[self._organizationId]) else None)

    def _getNewData(self, query) -> "list[dict[str, Any]]":
        getNewDataResult = self._querySnowflake.execute(
            QuerySnowflakeRequestDto(query, self._targetOrgId), QuerySnowflakeAuthDto(self._jwt))

        if not getNewDataResult.success:
            raise Exception(getNewDataResult.error)
        if not getNewDataResult.value:
            raise Exception('Sf query error - operation: new data')

        newData = getNewDataResult.value.content[self._organizationId]

        return newData

    def _runModel(self, newData: "tuple[str, float]", historicalData: "list[tuple[str, float]]", testType: Union[QuantMatTest, QuantColumnTest], importanceThreshold: float, boundsIntervalRelative: float, customLowerThreshold: "Union[CustomThreshold, None]", customUpperThreshold: "Union[CustomThreshold, None]") -> QuantTestResultDto:
        return CommonModel(newData, historicalData, testType, importanceThreshold, boundsIntervalRelative, customLowerThreshold, customUpperThreshold).run()

    def _runTest(self, newDataPoint, historicalData: "list[tuple[str,float]]") -> QuantTestExecutionResult:
        databaseName = self._testDefinition['DATABASE_NAME']
        schemaName = self._testDefinition['SCHEMA_NAME']
        materializationName = self._testDefinition['MATERIALIZATION_NAME']
        materializationType = self._testDefinition['MATERIALIZATION_TYPE']
        columnName = self._testDefinition['COLUMN_NAME']
        testSuiteId = self._testDefinition['ID']
        customLowerThreshold = self._testDefinition['CUSTOM_LOWER_THRESHOLD']
        customLowerThresholdMode = self._testDefinition['CUSTOM_LOWER_THRESHOLD_MODE']
        customUpperThreshold = self._testDefinition['CUSTOM_UPPER_THRESHOLD']
        customUpperThresholdMode = self._testDefinition['CUSTOM_UPPER_THRESHOLD_MODE']
        targetResourceId = self._testDefinition['TARGET_RESOURCE_ID']
        testType = self._testDefinition['TEST_TYPE']
        importanceThreshold = self._testDefinition['IMPORTANCE_THRESHOLD']
        boundsIntervalRelative = self._testDefinition['BOUNDS_INTERVAL_RELATIVE']

        executedOn = datetime.utcnow()
        executedOnISOFormat = executedOn.isoformat()

        self._insertExecutionEntry(
            executedOnISOFormat, CitoTableType.TestExecutions)

        historicalDataLength = len(historicalData)
        belowDayBoundary = True if historicalDataLength == 0 else (executedOn - datetime.fromisoformat(
            historicalData[0][0].replace('Z', ''))).days <= self._MIN_HISTORICAL_DATA_DAY_NUMBER_CONDITION
        if (historicalDataLength <= self._MIN_HISTORICAL_DATA_TEST_NUMBER_CONDITION or belowDayBoundary):
            self._insertHistoryEntry(
                newDataPoint, False, None)

            return QuantTestExecutionResult(testSuiteId, testType, self._executionId, targetResourceId, self._organizationId, True, None, None)

        lowerThreshold = None if customLowerThreshold is None else CustomThreshold(
            customLowerThreshold, customLowerThresholdMode)
        upperThreshold = None if customUpperThreshold is None else CustomThreshold(
            customUpperThreshold, customUpperThresholdMode)

        testResult = self._runModel(
            (executedOnISOFormat, newDataPoint), historicalData, testType, importanceThreshold, boundsIntervalRelative, lowerThreshold, upperThreshold)

        self._insertResultEntry(testResult)

        alertData = None
        alertId = None
        if testResult.anomaly.isAnomaly:
            anomalyMessage = getAnomalyMessage(
                targetResourceId, databaseName, schemaName, materializationName, columnName, testType)
            alertId = str(uuid.uuid4())
            self._insertAlertEntry(
                alertId, anomalyMessage, CitoTableType.TestAlerts)

            alertData = QuantTestAlertData(alertId, anomalyMessage, databaseName, schemaName, materializationName, materializationType, testResult.expectedValue, testResult.expectedValueUpper,
                                           testResult.expectedValueLower, columnName, newDataPoint)

        testData = QuantTestData(
            executedOnISOFormat, testResult.modifiedZScore, testResult.deviation, AnomalyData(testResult.anomaly.isAnomaly, testResult.anomaly.importance, testResult.anomaly.boundsIntervalRelative))

        self._insertHistoryEntry(
            newDataPoint, testResult.anomaly.isAnomaly, alertId)

        return QuantTestExecutionResult(testSuiteId, testType, self._executionId, targetResourceId, self._organizationId, False, testData, alertData)

    def _runSchemaChangeModel(self, oldSchema: "dict[str, ColumnDefinition]", newSchema: "dict[str, ColumnDefinition]") -> QualResultDto:
        return SchemaChangeModel(newSchema, oldSchema).run()

    def _runSchemaChangeTest(self, oldSchema: "dict[str, ColumnDefinition]", newSchema: "dict[str, ColumnDefinition]") -> QualTestExecutionResult:
        databaseName = self._testDefinition['DATABASE_NAME']
        schemaName = self._testDefinition['SCHEMA_NAME']
        materializationName = self._testDefinition['MATERIALIZATION_NAME']
        materializationType = self._testDefinition['MATERIALIZATION_TYPE']
        columnName = self._testDefinition['COLUMN_NAME']
        testSuiteId = self._testDefinition['ID']
        testType = self._testDefinition['TEST_TYPE']
        targetResourceId = self._testDefinition['TARGET_RESOURCE_ID']

        executedOn = datetime.utcnow().isoformat()

        testResult = self._runSchemaChangeModel(
            oldSchema, newSchema)

        self._insertExecutionEntry(
            executedOn, CitoTableType.TestExecutionsQual)

        self._insertQualTestResultEntry(testResult)

        alertData = None
        alertId = None
        if not testResult.isIdentical:
            anomalyMessage = getAnomalyMessage(
                targetResourceId, databaseName, schemaName, materializationName, columnName, self._testDefinition['TEST_TYPE'])
            alertId = str(uuid.uuid4())
            self._insertAlertEntry(
                alertId, anomalyMessage, CitoTableType.TestAlertsQual)

            alertData = QualTestAlertData(alertId, anomalyMessage, databaseName, schemaName,
                                          materializationName, materializationType, testResult.deviations)

        self._insertQualHistoryEntry(
            newSchema, testResult.isIdentical, alertId)

        testData = QualTestData(
            executedOn, testResult.deviations, testResult.isIdentical)
        return QualTestExecutionResult(testSuiteId, testType, self._executionId, targetResourceId, self._organizationId, testData, alertData)

    def _runMaterializationRowCountTest(self) -> QuantTestExecutionResult:
        databaseName = self._testDefinition['DATABASE_NAME']
        schemaName = self._testDefinition['SCHEMA_NAME']
        materializationName = self._testDefinition['MATERIALIZATION_NAME']
        materializationType = self._testDefinition['MATERIALIZATION_TYPE']

        newDataQuery = getRowCountQuery(
            databaseName, schemaName, materializationName, MaterializationType[materializationType])

        newData = self._getNewData(newDataQuery)

        if (len(newData) != 1):
            raise Exception(
                f'Mat row count - More than one or no matching new data entries found')

        newDataPoint = newData[0]['ROW_COUNT']

        historicalData = self._getHistoricalData()

        historicalData

        testResult = self._runTest(
            newDataPoint, historicalData)

        return testResult

    def _runMaterializationColumnCountTest(self) -> QuantTestExecutionResult:
        databaseName = self._testDefinition['DATABASE_NAME']
        schemaName = self._testDefinition['SCHEMA_NAME']
        materializationName = self._testDefinition['MATERIALIZATION_NAME']

        newDataQuery = getColumnCountQuery(
            databaseName, schemaName, materializationName)

        newData = self._getNewData(newDataQuery)

        if (len(newData) != 1):
            raise Exception(
                'Mat column count - More than one or no matching new data entries found')

        newDataPoint = newData[0]['COLUMN_COUNT']

        historicalData = self._getHistoricalData()

        testResult = self._runTest(
            newDataPoint, historicalData)

        return testResult

    def _runMaterializationFreshnessTest(self) -> QuantTestExecutionResult:
        databaseName = self._testDefinition['DATABASE_NAME']
        schemaName = self._testDefinition['SCHEMA_NAME']
        materializationName = self._testDefinition['MATERIALIZATION_NAME']
        materializationType = self._testDefinition['MATERIALIZATION_TYPE']

        newDataQuery = getFreshnessQuery(
            databaseName, schemaName, materializationName, materializationType)

        newData = self._getNewData(newDataQuery)

        if (len(newData) != 1):
            raise Exception(
                'Mat freshness - More than one or no matching new data entries found')

        newDataPoint = newData[0]['TIME_DIFF']

        historicalData = self._getHistoricalData()

        testResult = self._runTest(
            newDataPoint, historicalData)

        return testResult

    def _runMaterializationSchemaChangeTest(self) -> QualTestExecutionResult:
        databaseName = self._testDefinition['DATABASE_NAME']
        schemaName = self._testDefinition['SCHEMA_NAME']
        materializationName = self._testDefinition['MATERIALIZATION_NAME']

        newDataQuery = getSchemaChangeQuery(
            databaseName, schemaName, materializationName)

        newData = self._getNewData(newDataQuery)

        newSchema: dict[str, ColumnDefinition] = {}
        for el in newData:
            columnDefinition = el['COLUMN_DEFINITION']
            ordinalPosition = columnDefinition['ORDINAL_POSITION']
            newSchema[str(ordinalPosition)] = ColumnDefinition(columnDefinition['COLUMN_NAME'],  columnDefinition['DATA_TYPE'],
                                                               columnDefinition['IS_IDENTITY'],  columnDefinition['IS_NULLABLE'],  columnDefinition['ORDINAL_POSITION'])

        oldSchema = self._getLastMatSchema()

        if not oldSchema:
            raise Exception(
                'Cannot test for schema change. Missing old schema.')

        testResult = self._runSchemaChangeTest(oldSchema, newSchema)

        return testResult

    def _runColumnCardinalityTest(self) -> QuantTestExecutionResult:
        databaseName = self._testDefinition['DATABASE_NAME']
        schemaName = self._testDefinition['SCHEMA_NAME']
        materializationName = self._testDefinition['MATERIALIZATION_NAME']
        columnName = self._testDefinition['COLUMN_NAME']

        newDataQuery = getCardinalityQuery(
            databaseName, schemaName, materializationName, columnName)

        newData = self._getNewData(newDataQuery)

        if (len(newData) != 1):
            raise Exception(
                'Col cardinality - More than one or no matching new data entries found')

        newDataPoint = newData[0]['DISTINCT_VALUE_COUNT']

        historicalData = self._getHistoricalData()

        testResult = self._runTest(
            newDataPoint, historicalData)

        return testResult

    def _runColumnDistributionTest(self) -> QuantTestExecutionResult:
        databaseName = self._testDefinition['DATABASE_NAME']
        schemaName = self._testDefinition['SCHEMA_NAME']
        materializationName = self._testDefinition['MATERIALIZATION_NAME']
        columnName = self._testDefinition['COLUMN_NAME']

        newDataQuery = getDistributionQuery(
            databaseName, schemaName, materializationName, columnName)

        newData = self._getNewData(newDataQuery)

        if (len(newData) != 1):
            raise Exception(
                'Col Distribution - More than one or no matching new data entries found')

        newDataPoint = newData[0]['MEDIAN']

        historicalData = self._getHistoricalData()

        testResult = self._runTest(
            newDataPoint, historicalData)

        return testResult

    def _runColumnFreshnessTest(self) -> QuantTestExecutionResult:
        databaseName = self._testDefinition['DATABASE_NAME']
        schemaName = self._testDefinition['SCHEMA_NAME']
        materializationName = self._testDefinition['MATERIALIZATION_NAME']
        columnName = self._testDefinition['COLUMN_NAME']

        newDataQuery = getColumnFreshnessQuery(
            databaseName, schemaName, materializationName, columnName)

        newData = self._getNewData(newDataQuery)

        if (len(newData) != 1):
            raise Exception(
                'Col Freshness - More than one or no matching new data entries found')

        newDataPoint = newData[0]['TIME_DIFF']

        historicalData = self._getHistoricalData()

        testResult = self._runTest(
            newDataPoint, historicalData)

        return testResult

    def _runColumnNullnessTest(self) -> QuantTestExecutionResult:
        databaseName = self._testDefinition['DATABASE_NAME']
        schemaName = self._testDefinition['SCHEMA_NAME']
        materializationName = self._testDefinition['MATERIALIZATION_NAME']
        columnName = self._testDefinition['COLUMN_NAME']

        newDataQuery = getNullnessQuery(
            databaseName, schemaName, materializationName, columnName)

        newData = self._getNewData(newDataQuery)

        if (len(newData) != 1):
            raise Exception(
                'Col Nullness - More than one or no matching new data entries found')

        newDataPoint = newData[0]['NULLNESS_RATE']

        historicalData = self._getHistoricalData()

        testResult = self._runTest(
            newDataPoint, historicalData)

        return testResult

    def _runColumnUniquenessTest(self) -> QuantTestExecutionResult:
        databaseName = self._testDefinition['DATABASE_NAME']
        schemaName = self._testDefinition['SCHEMA_NAME']
        materializationName = self._testDefinition['MATERIALIZATION_NAME']
        columnName = self._testDefinition['COLUMN_NAME']

        newDataQuery = getUniquenessQuery(
            databaseName, schemaName, materializationName, columnName)

        newData = self._getNewData(newDataQuery)

        if (len(newData) != 1):
            raise Exception(
                'Col Uniqueness - More than one or no matching new data entries found')

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

        organizationResult = getTestEntryResult.value.content[self._organizationId]
        if not len(organizationResult) == 1:
            raise Exception('Test Definition - More than one or no test found')

        return organizationResult[0]

    def execute(self, request: ExecuteTestRequestDto, auth: ExecuteTestAuthDto) -> ExecuteTestResponseDto:
        try:
            if not request.targetOrgId and not auth.callerOrgId:
                raise Exception('No organization Id instance provided')
            if request.targetOrgId and auth.callerOrgId:
                raise Exception(
                    'callerOrgId and targetOrgId provided. Not allowed')

            if auth.isSystemInternal:
                if not request.targetOrgId:
                    raise Exception('Target organization id missing')
                orgId = request.targetOrgId
            else:
                if not auth.callerOrgId:
                    raise Exception('Caller organization id missing')
                orgId = auth.callerOrgId

            self._testSuiteId = request.testSuiteId
            self._testType = request.testType
            self._targetOrgId = request.targetOrgId
            self._organizationId = orgId
            self._requestLoggingInfo = f'(organizationId: {self._organizationId}, testSuiteId: {self._testSuiteId}, testType: {self._testType})'
            self._executionId = str(uuid.uuid4())
            self._jwt = auth.jwt
            self._testDefinition = self._getTestDefinition()

            testTypeKey = 'TEST_TYPE'

            if self._testDefinition[testTypeKey] == QuantMatTest.MaterializationRowCount.value:
                testResult = self._runMaterializationRowCountTest()
            elif self._testDefinition[testTypeKey] == QuantMatTest.MaterializationColumnCount.value:
                testResult = self._runMaterializationColumnCountTest()
            elif self._testDefinition[testTypeKey] == QuantMatTest.MaterializationFreshness.value:
                testResult = self._runMaterializationFreshnessTest()
            elif self._testDefinition[testTypeKey] == QuantColumnTest.ColumnCardinality.value:
                testResult = self._runColumnCardinalityTest()
            elif self._testDefinition[testTypeKey] == QuantColumnTest.ColumnDistribution.value:
                testResult = self._runColumnDistributionTest()
            elif self._testDefinition[testTypeKey] == QuantColumnTest.ColumnFreshness.value:
                testResult = self._runColumnFreshnessTest()
            elif self._testDefinition[testTypeKey] == QuantColumnTest.ColumnNullness.value:
                testResult = self._runColumnNullnessTest()
            elif self._testDefinition[testTypeKey] == QuantColumnTest.ColumnUniqueness.value:
                testResult = self._runColumnUniquenessTest()
            elif self._testDefinition[testTypeKey] == QualMatTest.MaterializationSchemaChange.value:
                testResult = self._runMaterializationSchemaChangeTest()
            else:
                raise Exception('Test type mismatch')

            return Result.ok(testResult)

        except Exception as e:
            logger.exception(
                f'error: {e}' if e.args[0] else f'error: unknown - {self._requestLoggingInfo}')
            return Result.fail('')
